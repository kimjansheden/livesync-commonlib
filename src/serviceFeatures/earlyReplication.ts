import { serialized } from "octagonal-wheels/concurrency/lock";
import { withConcurrency } from "octagonal-wheels/iterable/map";
import {
    LOG_LEVEL_INFO,
    REMOTE_MINIO,
    isMetaEntry,
    type AnyEntry,
    type FilePathWithPrefix,
    type MetaEntry,
} from "@lib/common/types";
import type { NecessaryServices } from "@lib/interfaces/ServiceModule";
import type { LogFunction } from "@lib/services/lib/logUtils";
import { shouldBeIgnored, stripAllPrefixes } from "@lib/string_and_binary/path";
import { UnknownFileWriteStateError } from "@lib/serviceModules/FilePublicationCoordinator";

type Host = NecessaryServices<
    "appLifecycle" | "setting" | "vault" | "path" | "database" | "keyValueDB" | "replication" | "replicator",
    "fileHandler" | "storageAccess"
>;
type PathReceipt = { version: 1; path: FilePathWithPrefix; prepared: boolean; absent: boolean };
type ScanResult = "completed" | "failed" | "skipped";
type LocalActivityOwner = {
    runBoundedLocalApplicationActivity?<T>(task: () => Promise<T>, options: { label: string }): Promise<T>;
};
export const EARLY_PATH_RECEIPT_PREFIX = "offline-startup-path-v1:";
export const FULL_SCAN_RECEIPT_KEY = "offline-full-scan-v1";
const sessions = new WeakMap<object, EarlyReplication>();
export const EARLY_CAPTURE_CONCURRENCY = 8;
export class EarlyDatabaseReadError extends Error {
    constructor(cause: unknown) {
        super("Den lokala databasen kunde inte läsas säkert", { cause });
    }
}
export function isFatalEarlyDatabaseError(error: unknown): boolean {
    if (error instanceof EarlyDatabaseReadError) return true;
    if (!error || typeof error !== "object") return false;
    const e = error as { name?: string; message?: string; status?: number };
    return (
        (e.status ?? 0) >= 500 ||
        /indexed_db_went_bad|indexeddb|pouch|invalidstateerror|transactioninactiveerror|unknownerror|aborterror|databaseclosederror/i.test(
            `${e.name ?? ""} ${e.message ?? ""}`
        )
    );
}
export class EarlyScanInterruptedError extends Error {
    constructor() {
        super("Full avstämning pausades av livscykeln");
    }
}

/** Device-local path preparation; no remote or document format is changed. */
export class EarlyReplication {
    private active = false;
    private stopped = false;
    private readonly pendingPaths = new Set<string>();
    constructor(
        private readonly host: Host,
        private readonly log: LogFunction,
        private readonly wasSeen: (path: FilePathWithPrefix) => boolean
    ) {}

    get isActive(): boolean {
        return this.active;
    }
    get mayContinue(): boolean {
        return this.canRun();
    }
    private canRun(): boolean {
        return (
            !this.stopped &&
            !this.host.services.appLifecycle.hasUnloaded() &&
            !this.host.services.appLifecycle.isSuspended()
        );
    }
    async begin(): Promise<boolean> {
        if (!this.canRun() || this.host.services.setting.currentSettings().remoteType !== REMOTE_MINIO) return false;
        if (!this.host.services.database.isDatabaseReady()) return false;
        this.host.serviceModules.fileHandler.requireOfflinePreparation();
        await this.host.services.keyValueDB.simpleStore.set(FULL_SCAN_RECEIPT_KEY, { version: 1, complete: false });
        if (!this.canRun()) return false;
        this.active = true;
        return true;
    }
    stop(): void {
        this.stopped = true;
        this.active = false;
        this.host.serviceModules.fileHandler.requireOfflinePreparation(false);
    }
    private key(doc: MetaEntry): string {
        return EARLY_PATH_RECEIPT_PREFIX + doc._id;
    }
    private async selected(doc: MetaEntry): Promise<boolean> {
        const path = this.host.services.path.getPath(doc);
        if (shouldBeIgnored(path) || !this.host.services.vault.isValidPath(path)) return false;
        if ((await this.host.services.path.path2id(path)) !== doc._id) return false;
        return await this.host.services.vault.isTargetFile(path);
    }
    private async lock<T>(doc: MetaEntry, task: () => Promise<T>): Promise<T> {
        return await serialized(`processFileEvent-${doc._id}`, task);
    }
    private async readMetadata(id: MetaEntry["_id"], conflicts = false) {
        try {
            return await this.host.services.database.localDatabase.allDocsRaw({
                keys: [id],
                include_docs: true,
                conflicts,
            });
        } catch (error) {
            throw new EarlyDatabaseReadError(error);
        }
    }

    private async canPrepare(path: FilePathWithPrefix, absent: boolean): Promise<boolean> {
        try {
            return await this.host.serviceModules.fileHandler.canPrepareOfflinePath(path, absent);
        } catch (error) {
            if (!(error instanceof UnknownFileWriteStateError)) throw error;
            return false;
        }
    }

    /** Runs before bulk insertion, so missing DB state cannot be confused with a new remote winner. */
    async capture(docs: MetaEntry[]): Promise<void> {
        if (!this.active) return;
        const started = Date.now();
        let prepared = 0;
        let blocked = 0;
        const capturePath = async (doc: MetaEntry): Promise<void> => {
            if (!this.canRun()) throw new Error("Tidig mottagning avbröts av livscykeln");
            if (!(await this.selected(doc))) return;
            await this.lock(doc, async () => {
                const store = this.host.services.keyValueDB.simpleStore;
                const path = this.host.services.path.getPath(doc);
                const previous = (await store.get(this.key(doc))) as PathReceipt | undefined;
                // An interrupted receive must retain its original baseline, even after a restart.
                if (previous) {
                    this.pendingPaths.add(doc._id);
                    this.host.serviceModules.fileHandler.requireOfflinePreparationForPath(path, true);
                    return;
                }
                const result = await this.readMetadata(doc._id);
                const row = result.rows[0];
                const absent = Boolean(row && "error" in row && row.error === "not_found" && !this.wasSeen(path));
                if (!this.canRun()) throw new Error("Tidig mottagning avbröts av livscykeln");
                const ready = await this.canPrepare(path, absent);
                if (!this.canRun()) throw new Error("Tidig mottagning avbröts av livscykeln");
                await store.set(this.key(doc), { version: 1, path, prepared: ready, absent } satisfies PathReceipt);
                this.pendingPaths.add(doc._id);
                this.host.serviceModules.fileHandler.requireOfflinePreparationForPath(path, true);
                if (ready) prepared++;
                else blocked++;
            });
        };
        for await (const _ of withConcurrency(docs, capturePath, EARLY_CAPTURE_CONCURRENCY)) {
            /* Drain bounded workers before the ordered journal insertion. */
        }
        this.log(
            `Tidig sökvägsförberedelse: ${Date.now() - started} ms, ${prepared} klara, ${blocked} väntar`,
            LOG_LEVEL_INFO
        );
    }

    /** The lock covers both local preservation and reflection; a receipt alone never authorises a write. */
    async apply(doc: MetaEntry, apply: () => Promise<boolean>): Promise<boolean> {
        if (!this.canRun()) return false;
        if (!this.active && !this.pendingPaths.has(doc._id)) return await apply();
        if (!(await this.selected(doc))) return this.canRun() ? await apply() : false;
        return await this.lock(doc, async () => {
            const store = this.host.services.keyValueDB.simpleStore;
            const path = this.host.services.path.getPath(doc);
            this.pendingPaths.add(doc._id);
            this.host.serviceModules.fileHandler.requireOfflinePreparationForPath(path, true);
            const receipt = (await store.get(this.key(doc))) as PathReceipt | undefined;
            if (receipt && (receipt.version !== 1 || receipt.path !== path)) return false;
            // A previously unknown baseline can become available after a scan or a normal reflection.
            // Recheck it under the same file-event lock; the original absence observation stays unchanged.
            if (receipt && !receipt.prepared && !(await this.canPrepare(path, receipt.absent))) return false;
            if (!this.canRun()) return false;
            if (
                !(await this.host.serviceModules.fileHandler.prepareOfflinePath(
                    path,
                    receipt?.absent ?? false,
                    false,
                    () => this.canRun()
                ))
            )
                return false;
            if (!this.canRun()) return false;
            const absent = (await this.host.serviceModules.storageAccess.statHidden(path)) === null;
            if (!this.canRun()) return false;
            await store.set(this.key(doc), { version: 1, path, prepared: true, absent } satisfies PathReceipt);
            if (!this.canRun()) return false;
            const applied = await apply();
            if (applied && this.canRun()) {
                await store.delete(this.key(doc));
                this.pendingPaths.delete(doc._id);
                this.host.serviceModules.fileHandler.requireOfflinePreparationForPath(path, false);
            }
            return applied;
        });
    }

    /** Full scans re-read each path after enumeration, under the same lock as early reflection and local edits. */
    async scan(path: FilePathWithPrefix): Promise<ScanResult> {
        const id = await this.host.services.path.path2id(path);
        const run = async (): Promise<ScanResult> =>
            await serialized(`processFileEvent-${id}`, async () => {
                if (!this.canRun()) return "failed";
                this.pendingPaths.add(id);
                this.host.serviceModules.fileHandler.requireOfflinePreparationForPath(path, true);
                const rows = await this.readMetadata(id, true);
                const row = rows.rows[0];
                const doc =
                    row && "doc" in row && row.doc && isMetaEntry(row.doc as AnyEntry)
                        ? (row.doc as MetaEntry)
                        : undefined;
                if (doc && !(await this.selected(doc))) return "failed";
                const absent = Boolean(row && "error" in row && row.error === "not_found" && !this.wasSeen(path));
                if (
                    !(await this.host.serviceModules.fileHandler.prepareOfflinePath(path, absent, true, () =>
                        this.canRun()
                    ))
                ) {
                    this.pendingPaths.add(id);
                    return "failed";
                }
                if (!this.canRun()) return "failed";
                const latest = await this.readMetadata(id, true);
                if (!this.canRun()) return "failed";
                const latestRow = latest.rows[0];
                const current =
                    latestRow && "doc" in latestRow && latestRow.doc && isMetaEntry(latestRow.doc as AnyEntry)
                        ? (latestRow.doc as MetaEntry)
                        : undefined;
                if (!current) {
                    await this.host.services.keyValueDB.simpleStore.delete(EARLY_PATH_RECEIPT_PREFIX + id);
                    this.pendingPaths.delete(id);
                    this.host.serviceModules.fileHandler.requireOfflinePreparationForPath(path, false);
                    return "completed";
                }
                if ((current._conflicts?.length ?? 0) > 0) {
                    const absent = (await this.host.serviceModules.storageAccess.statHidden(path)) === null;
                    if (!this.canRun()) return "failed";
                    await this.host.services.keyValueDB.simpleStore.set(this.key(current), {
                        version: 1,
                        path,
                        prepared: true,
                        absent,
                    } satisfies PathReceipt);
                    return "skipped";
                }
                if (!(await this.host.serviceModules.fileHandler.dbToStorage(current, stripAllPrefixes(path))))
                    return "failed";
                if (!this.canRun()) return "failed";
                await this.host.services.keyValueDB.simpleStore.delete(this.key(current));
                this.pendingPaths.delete(id);
                this.host.serviceModules.fileHandler.requireOfflinePreparationForPath(path, false);
                return "completed";
            });
        const owner = this.host.services.replicator as LocalActivityOwner;
        return owner?.runBoundedLocalApplicationActivity
            ? await owner.runBoundedLocalApplicationActivity(run, { label: "offline-startup-path" })
            : await run();
    }

    async completeScan(succeeded: boolean): Promise<boolean> {
        if (!succeeded || !this.canRun()) return false;
        await this.host.services.keyValueDB.simpleStore.set(FULL_SCAN_RECEIPT_KEY, { version: 1, complete: true });
        this.active = false;
        this.host.serviceModules.fileHandler.requireOfflinePreparation(false);
        if (this.canRun()) this.log("Full avstämning klar; hållbart avslutskvitto sparat", LOG_LEVEL_INFO);
        return true;
    }
}

export function getEarlyReplication(host: { services: object }): EarlyReplication | undefined {
    const session = sessions.get(host.services);
    return session?.isActive ? session : undefined;
}
export function useEarlyReplication(
    host: Host,
    log: LogFunction,
    wasSeen: (path: FilePathWithPrefix) => boolean
): EarlyReplication {
    const session = new EarlyReplication(host, log, wasSeen);
    sessions.set(host.services, session);
    host.services.vault.withPreparedIncomingPath = (doc, apply) => session.apply(doc, apply);
    host.services.replication.prepareIncomingDocuments = (docs) => session.capture(docs);
    host.services.appLifecycle.onBeforeUnload.addHandler(async () => {
        session.stop();
        return true;
    });
    return session;
}
