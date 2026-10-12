import {
    type DocumentID,
    type EntryDoc,
    LOG_LEVEL_INFO,
    LOG_LEVEL_NOTICE,
    LOG_LEVEL_VERBOSE,
    LOG_LEVEL_DEBUG,
    type EntryLeaf,
    type SyncParameters,
    DEFAULT_SYNC_PARAMETERS,
    ProtocolVersions,
    DOCID_JOURNAL_SYNC_PARAMETERS,
    type BucketSyncSetting,
    E2EEAlgorithms,
    type RemoteDBSettings,
    type LOG_LEVEL,
} from "@lib/common/types.ts";
import { Logger } from "@lib/common/logger.ts";
import type { ReplicationCallback, ReplicationStat } from "@lib/replication/LiveSyncAbstractReplicator.ts";
import {
    type SimpleStore,
    concatUInt8Array,
    escapeNewLineFromString,
    setAllItems,
    unescapeNewLineFromString,
} from "@lib/common/utils.ts";
import { serialized, shareRunningResult } from "octagonal-wheels/concurrency/lock";
import { wrappedDeflate, wrappedInflate } from "@lib/pouchdb/compress.ts";
import { type CheckPointInfo, createCheckPointInfoDefault } from "./JournalSyncTypes.ts";
import type { LiveSyncJournalReplicatorEnv } from "./LiveSyncJournalReplicatorEnv.ts";
import {
    JournalStorageReadStatuses,
    type IJournalStorage,
    type JournalStorageObject,
    type JournalStorageReadResult,
} from "./objectstore/JournalStorageAdapter.ts";

import {
    clearHandlers,
    createSyncParamsHanderForServer,
    SyncParamsFetchError,
    SyncParamsNotFoundError,
    SyncParamsUpdateError,
} from "@lib/replication/SyncParamsHandler.ts";
import { REMOTE_CHUNK_FETCHED } from "@lib/pouchdb/LiveSyncLocalDB.ts";
import { decryptBinary, encryptBinary } from "octagonal-wheels/encryption/encryption";
import {
    encryptBinary as encryptBinaryHKDF,
    decryptBinary as decryptBinaryHKDF,
} from "octagonal-wheels/encryption/hkdf";

const CHECKPOINT_HISTORY_KEYS = ["knownIDs", "sentIDs", "receivedFiles", "sentFiles"] as const;

/** A send stopped because the journal history it started from was reset while it ran. */
class JournalCheckpointResetError extends Error {
    constructor() {
        super("The journal checkpoint was reset while sending");
        this.name = "JournalCheckpointResetError";
    }
}

const RECORD_SPLIT = `\n`;
const UNIT_SPLIT = `\u001f`;
type ProcessingEntry = PouchDB.Core.PutDocument<EntryDoc> & PouchDB.Core.GetMeta;

const JOURNAL_SUFFIX = "-docs.jsonl.gz";
/**
 * The name suffix of a journal which holds nothing but large chunks.
 *
 * Chunks are immutable and named after their content, so the order in which they arrive does not matter. A receiver
 * may therefore fetch these journals after every other journal, while a journal which can hold a document is always
 * applied in upload order. The format is that of any other journal, so a client which does not know the suffix reads
 * such a journal like the others.
 */
export const BULK_JOURNAL_SUFFIX = "-chunks.jsonl.gz";
/** A chunk whose serialised size is at least this is sent in a journal of large chunks. Text chunks are normally smaller. */
export const BULK_CHUNK_MIN_BYTES = 16 * 1024;
/** How long a synchronisation goes on starting journals of large chunks after it has sent the local changes. */
export const BULK_RECEIVE_BUDGET_MS = 15_000;
/** A receive of large chunks ends after this many journals in a row failed, as the connection is probably lost. */
const BULK_RECEIVE_MAX_FAILURES_IN_A_ROW = 2;

/** Entries collected for one journal. */
type SendBuffer = { rows: Uint8Array[]; binarySize: number; sentIDs: string[] };
/** One journal to upload, with the sequence the checkpoint may advance to once it is stored. */
type SendUnit = { bin: Uint8Array<ArrayBuffer>; packLastSeq: number; sentIDs: string[]; isBulk: boolean };
/** The journals of large chunks which a receive of the other journals left to fetch. */
type DeferredReceive = { resetGeneration: number; journals: string[] };

const te = new TextEncoder();
async function sha256Hex(value: string): Promise<string> {
    const digest = await globalThis.crypto.subtle.digest("SHA-256", te.encode(value));
    return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

function isChunkEntry(entry: { _id: string }): boolean {
    return entry._id.startsWith("h:");
}

function serializeDoc(doc: EntryDoc): Uint8Array {
    if (isChunkEntry(doc)) {
        const data = (doc as EntryLeaf).data;
        const writeData = escapeNewLineFromString(data);
        return te.encode(`~${doc._id}${UNIT_SPLIT}${writeData}${RECORD_SPLIT}`);
    }
    return te.encode(JSON.stringify(doc) + RECORD_SPLIT);
}

export class JournalSyncCore {
    _settings: BucketSyncSetting;
    storage: IJournalStorage;
    /** The host already refreshed the security seed for the next cycle. */
    private syncParametersRefreshedForNextCycle = false;

    get db() {
        return this.env.services.database.localDatabase.localDatabase;
    }

    get currentSettings() {
        return this.env.services.setting.currentSettings();
    }

    hash = "";
    processReplication: ReplicationCallback;
    batchSize = 100;
    /** How long a synchronisation goes on starting journals of large chunks after it has sent the local changes. */
    bulkReceiveBudgetMs = BULK_RECEIVE_BUDGET_MS;
    /** The clock which that time is measured with. A test replaces it instead of waiting. */
    now: () => number = () => Date.now();
    /**
     * Whether a synchronisation may fetch large chunks now. The owner replaces this to hold that work back, for
     * example while a mobile application is hidden and may be stopped at any time. It is asked before each journal,
     * so a round which is running ends after the journal in hand. A receive of everything is not held back.
     */
    mayReceiveBulkJournals: () => boolean = () => true;
    env: LiveSyncJournalReplicatorEnv;
    store: SimpleStore<CheckPointInfo>;
    requestedStop = false;

    getInitialSyncParameters(): Promise<SyncParameters> {
        return Promise.resolve({
            ...DEFAULT_SYNC_PARAMETERS,
            protocolVersion: ProtocolVersions.ADVANCED_E2EE,
            pbkdf2salt: "",
        } satisfies SyncParameters);
    }

    /**
     * Read the sync parameters of the remote.
     *
     * Only a remote which answers that the object is absent may be treated as having no parameters: that answer lets
     * the caller create a new security seed, which makes every journal written under the previous seed unreadable.
     * Any other outcome (a failed request, an empty response, unparsable content) is a read error, so it fails closed
     * and stops the synchronisation instead of silently replacing the seed.
     */
    async getSyncParameters(): Promise<SyncParameters> {
        let result: JournalStorageReadResult<SyncParameters>;
        try {
            result = await this.downloadJsonWithResult<SyncParameters>(DOCID_JOURNAL_SYNC_PARAMETERS);
        } catch (ex) {
            result = { status: JournalStorageReadStatuses.UNAVAILABLE, error: ex };
        }
        if (result.status === JournalStorageReadStatuses.NOT_FOUND) {
            Logger(`Remote sync parameters do not exist yet`, LOG_LEVEL_INFO);
            throw new SyncParamsNotFoundError(`Missing sync parameters`);
        }
        if (result.status !== JournalStorageReadStatuses.AVAILABLE) {
            Logger(`Could not retrieve remote sync parameters`, LOG_LEVEL_INFO);
            Logger(result.error, LOG_LEVEL_VERBOSE);
            throw new SyncParamsFetchError(`Could not read remote sync parameters`, { cause: result.error });
        }
        return result.value;
    }

    async putSyncParameters(params: SyncParameters): Promise<boolean> {
        try {
            const data = new TextEncoder().encode(JSON.stringify(params));
            if (await this.storage.upload(DOCID_JOURNAL_SYNC_PARAMETERS, data, "application/json")) {
                return true;
            }
            throw new SyncParamsUpdateError(`Could not store remote sync parameters`);
        } catch (ex) {
            Logger(`Could not upload sync parameters`, LOG_LEVEL_INFO);
            Logger(ex, LOG_LEVEL_VERBOSE);
            throw SyncParamsUpdateError.fromError(ex);
        }
    }

    getHash(settings: BucketSyncSetting) {
        return btoa(
            encodeURI([settings.endpoint, `${settings.bucket}${settings.bucketPrefix}`, settings.region].join())
        );
    }

    constructor(
        settings: BucketSyncSetting,
        store: SimpleStore<CheckPointInfo>,
        env: LiveSyncJournalReplicatorEnv,
        storage: IJournalStorage
    ) {
        this._settings = settings;
        this.env = env;
        this.processReplication = async (docs: PouchDB.Core.ExistingDocument<EntryDoc>[]) =>
            await env.services.replication.parseSynchroniseResult(docs);
        this.store = store;
        this.hash = this.getHash(settings);
        this.storage = storage;
        clearHandlers();
    }

    async downloadJson<T>(key: string): Promise<T | false> {
        try {
            const data = await this.storage.download(key, true);
            if (!data) return false;
            return JSON.parse(new TextDecoder().decode(data)) as T;
        } catch (ex) {
            Logger(`Could not download json ${key}`);
            Logger(ex, LOG_LEVEL_VERBOSE);
            return false;
        }
    }

    async downloadJsonWithResult<T>(key: string): Promise<JournalStorageReadResult<T>> {
        const result = await this.storage.downloadWithResult(key, true);
        if (result.status !== JournalStorageReadStatuses.AVAILABLE) return result;
        try {
            return {
                status: JournalStorageReadStatuses.AVAILABLE,
                value: JSON.parse(new TextDecoder().decode(result.value)) as T,
            };
        } catch (ex) {
            Logger(`Could not parse downloaded json ${key}`);
            Logger(ex, LOG_LEVEL_VERBOSE);
            return {
                status: JournalStorageReadStatuses.UNAVAILABLE,
                error: ex,
            };
        }
    }

    async uploadJson<T>(key: string, body: T): Promise<boolean> {
        try {
            const data = new TextEncoder().encode(JSON.stringify(body));
            return await this.storage.upload(key, data, "application/json");
        } catch (ex) {
            Logger(`Could not upload json ${key}`);
            Logger(ex, LOG_LEVEL_VERBOSE);
            return false;
        }
    }

    applyNewConfig(settings: BucketSyncSetting, store: SimpleStore<CheckPointInfo>, env: LiveSyncJournalReplicatorEnv) {
        const hash = this.getHash(settings);
        this._settings = settings;
        this.env = env;
        this.processReplication = async (docs: PouchDB.Core.ExistingDocument<EntryDoc>[]) =>
            await env.services.replication.parseSynchroniseResult(docs);
        this.store = store;
        this.storage.applyNewConfig(settings);
        // The replicator applies the configuration each time it uses this client. Sync parameters read in this cycle
        // stay valid for the same remote, so reading them again is left to the refresh of the next cycle.
        if (hash !== this.hash) {
            clearHandlers();
            this.syncParametersRefreshedForNextCycle = false;
            this._forgetBulkJournals();
        }
        this.hash = hash;
    }

    updateInfo(info: Partial<ReplicationStat>) {
        const old = this.env.services.replicator.replicationStatics.value;
        this.env.services.replicator.replicationStatics.value = {
            sent: info.sent ?? old.sent,
            arrived: info.arrived ?? old.arrived,
            maxPullSeq: info.maxPullSeq ?? old.maxPullSeq,
            maxPushSeq: info.maxPushSeq ?? old.maxPushSeq,
            lastSyncPullSeq: info.lastSyncPullSeq ?? old.lastSyncPullSeq,
            lastSyncPushSeq: info.lastSyncPushSeq ?? old.lastSyncPushSeq,
            syncStatus: info.syncStatus ?? old.syncStatus,
        };
    }

    /**
     * Update the checkpoint. Return a new object: an update which returns the checkpoint it was given, even after
     * changing it in place, is treated as no change and not stored. An update which removes history increments
     * the reset generation, which the update itself cannot set.
     */
    async updateCheckPointInfo(func: (infoFrom: CheckPointInfo) => CheckPointInfo) {
        return await this._updateCheckPointInfo(func, false);
    }

    private async _updateCheckPointInfo(func: (infoFrom: CheckPointInfo) => CheckPointInfo, isReset: boolean) {
        const checkPointKey = `bucketsync-checkpoint-${this.hash}` as DocumentID;
        // Other clients share this store, for example the maintenance pane which resets the history while
        // replication runs. Serialising the read-modify-write keeps one update from discarding another.
        return await serialized(checkPointKey, async () => {
            const old = await this.getCheckpointInfo();
            // Updates may add to the sets in place, so measure them before applying the update.
            const oldSeq = Number(old.lastLocalSeq);
            const oldSizes = CHECKPOINT_HISTORY_KEYS.map((key) => old[key].size);
            const oldGeneration = old.resetGeneration;
            const updated = func(old);
            // An update which returns the checkpoint it was given changes nothing, so a cycle without progress
            // does not write the whole history again.
            if (updated === old) return old;
            // An explicit reset always counts, so it also stops a transfer which has not recorded anything yet.
            const removesHistory =
                isReset ||
                Number(updated.lastLocalSeq) < oldSeq ||
                CHECKPOINT_HISTORY_KEYS.some((key, index) => updated[key].size < oldSizes[index]);
            const newInfo: CheckPointInfo = {
                ...updated,
                resetGeneration: removesHistory ? oldGeneration + 1 : oldGeneration,
            };
            this._currentCheckPointInfo = newInfo;
            await this.store.set(checkPointKey, newInfo);
            return newInfo;
        });
    }

    _currentCheckPointInfo = createCheckPointInfoDefault();
    async getCheckpointInfo(): Promise<CheckPointInfo> {
        const checkPointKey = `bucketsync-checkpoint-${this.hash}` as DocumentID;
        const old: Record<string, unknown> = (await this.store.get(checkPointKey)) || {};
        const items = ["knownIDs", "sentIDs", "receivedFiles", "sentFiles"];
        for (const key of items) {
            if (!(key in old)) {
                continue;
            }
            const value = old[key];
            if (value instanceof Set) {
                continue;
            }
            if (Array.isArray(value)) {
                old[key] = new Set(value);
                continue;
            }
            if (value && typeof value === "object") {
                old[key] = new Set(Object.keys(value));
                continue;
            }
            old[key] = new Set<string>();
        }
        if (!Number.isSafeInteger(old.resetGeneration)) {
            delete old.resetGeneration;
        }
        this._currentCheckPointInfo = { ...createCheckPointInfoDefault(), ...old };
        return this._currentCheckPointInfo;
    }

    resetAllCaches(): void {
        clearHandlers();
        this.syncParametersRefreshedForNextCycle = false;
    }

    async resetCheckpointInfo() {
        await this._updateCheckPointInfo(() => createCheckPointInfoDefault(), true);
        clearHandlers();
    }

    private getJournalEpochFromSyncParams(params: SyncParameters): string {
        return `${params.protocolVersion}:${params.pbkdf2salt}`;
    }

    /**
     * Reset the journal caches when the remote was wiped since this device last synchronised with it.
     *
     * The host ordinarily refreshes the security seed before the cycle. Direct replication entry points have no
     * such preflight, so they refresh it here. Either route reads the remote once per cycle.
     */
    async ensureCheckpointCachesAreFresh(): Promise<void> {
        const alreadyRefreshed = this.syncParametersRefreshedForNextCycle;
        this.syncParametersRefreshedForNextCycle = false;
        const params = await this.getSyncParamsHandler().fetch(!alreadyRefreshed);
        if (!params) throw new SyncParamsFetchError("Could not read remote sync parameters");
        const journalEpoch = this.getJournalEpochFromSyncParams(params);

        const current = await this.getCheckpointInfo();
        if (current.journalEpoch === journalEpoch) {
            return;
        }

        const lastSentFile = [...current.sentFiles].sort().pop();

        // Epoch changed (or first observed on migrated devices).
        // Use sentFiles to determine whether the remote was wiped:
        //   - No sent history          → fresh device or empty state; save epoch, keep caches.
        //   - File still on remote     → epoch changed without a wipe (e.g. protocol bump or
        //                                first run after upgrade); save epoch, keep caches.
        //   - File gone from remote    → wipe confirmed; save epoch, reset caches.
        // sentFiles names are opaque SHA-256 operation identities, so the sorted last name is any
        // sent file rather than the newest. Its presence still shows whether the remote was wiped.
        if (!lastSentFile) {
            // No send history: cannot confirm wipe; just record the epoch.
            await this.updateCheckPointInfo((info) => ({ ...info, journalEpoch }));
            return;
        }

        let remoteWipeConfirmed: boolean;
        try {
            // listFiles uses S3 StartAfter (exclusive), so slice off the last char to land
            // just before the target key, then check if the returned entry matches exactly.
            const probe = await this.storage.listFiles(lastSentFile.slice(0, -1), 1);
            remoteWipeConfirmed = probe[0] !== lastSentFile;
        } catch {
            remoteWipeConfirmed = true;
        }

        if (!remoteWipeConfirmed) {
            // Remote files are intact: no wipe occurred. Save epoch and preserve caches.
            await this.updateCheckPointInfo((info) => ({ ...info, journalEpoch }));
            Logger(`Journal epoch changed (remote files still present). Epoch updated; caches kept.`, LOG_LEVEL_NOTICE);
            return;
        }

        Logger(
            `Journal epoch changed and remote wipe confirmed. Clearing dedupe caches and the sent sequence.`,
            LOG_LEVEL_NOTICE
        );
        // The new remote only holds what the wiping device had. Changes this device sent to the old remote may
        // be missing from it, so scan the local database from the start again. A sync receives the new remote
        // first, which marks everything it already holds as known, so only the missing changes are sent.
        await this.updateCheckPointInfo((info) => ({
            ...info,
            journalEpoch,
            lastLocalSeq: 0,
            knownIDs: new Set<string>(),
            sentIDs: new Set<string>(),
            receivedFiles: new Set<string>(),
            sentFiles: new Set<string>(),
        }));
        clearHandlers();
    }

    async isAvailable(): Promise<boolean> {
        return await this.storage.isAvailable();
    }

    async resetBucket(): Promise<boolean> {
        let files = [] as string[];
        try {
            do {
                files = await this.storage.listFiles("", 100);
                if (files.length == 0) {
                    break;
                }
                await this.storage.deleteFiles(files);
            } while (files.length != 0);
            clearHandlers();
        } catch (ex) {
            Logger(`WARNING! Could not delete files.`, LOG_LEVEL_NOTICE, "reset-bucket");
            Logger(ex, LOG_LEVEL_VERBOSE);
        }

        const journals = (await this._getRemoteJournals()).flat();
        if (journals.length == 0) {
            Logger("Nothing to delete!", LOG_LEVEL_NOTICE);
        } else {
            await this.storage.deleteFiles(journals);
            Logger(`${journals.length} items has been deleted!`, LOG_LEVEL_NOTICE);
        }
        // Reset after deleting, even when nothing remained. A transfer which started during the deletion could
        // otherwise record journals of the cleared remote as received or known, and a later send would skip them.
        await this.resetCheckpointInfo();
        return true;
    }

    getRemoteKey(): string {
        return this.getHash(this._settings);
    }

    /** The handler which keeps the sync parameters of this remote until they are read again. */
    private getSyncParamsHandler() {
        return createSyncParamsHanderForServer(this.getRemoteKey(), {
            put: (params: SyncParameters) => this.putSyncParameters(params),
            get: () => this.getSyncParameters(),
            create: () => this.getInitialSyncParameters(),
        });
    }

    async getReplicationPBKDF2Salt(refresh?: boolean): Promise<Uint8Array<ArrayBuffer>> {
        const salt = await this.getSyncParamsHandler().getPBKDF2Salt(refresh);
        if (refresh) this.syncParametersRefreshedForNextCycle = true;
        return salt;
    }

    /**
     * Whether the local database has changed since its changes were last looked through for sending.
     *
     * Decided locally, without a request to the remote. Sequences which cannot be compared count as changed, so a
     * send is never skipped on their account.
     */
    async hasUnsentLocalChanges(): Promise<boolean> {
        const [info, checkpoint] = await Promise.all([this.db.info(), this.getCheckpointInfo()]);
        const current = Number(info.update_seq);
        const scanned = Number(checkpoint.lastLocalSeq);
        if (!Number.isFinite(current) || !Number.isFinite(scanned)) return true;
        return current > scanned;
    }

    isEncryptionPrevented(fileName: string): boolean {
        if (fileName.endsWith(DOCID_JOURNAL_SYNC_PARAMETERS)) return true;
        return false;
    }

    private async decryptDataV2(
        encrypted: Uint8Array<ArrayBuffer>,
        set: RemoteDBSettings
    ): Promise<Uint8Array<ArrayBuffer>> {
        const salt = await this.getReplicationPBKDF2Salt();
        return await decryptBinaryHKDF(encrypted, set.passphrase, salt);
    }

    private async decryptDataV1(
        encrypted: Uint8Array<ArrayBuffer>,
        set: RemoteDBSettings
    ): Promise<Uint8Array<ArrayBuffer>> {
        return (await decryptBinary(
            encrypted,
            set.passphrase,
            set.useDynamicIterationCount
        )) as Uint8Array<ArrayBuffer>;
    }

    async decryptDownloaded(
        key: string,
        encrypted: Uint8Array<ArrayBuffer>,
        set: RemoteDBSettings
    ): Promise<Uint8Array<ArrayBuffer>> {
        const u = new Uint8Array(encrypted);
        try {
            if (!set.encrypt || set.passphrase == "" || this.isEncryptionPrevented(key)) {
                return u;
            }
            if (set.E2EEAlgorithm === E2EEAlgorithms.ForceV1) {
                return await this.decryptDataV1(u, set);
            }
            const decrypted = await this.decryptDataV2(u, set);
            return decrypted;
        } catch (ex) {
            Logger(`Failed to decrypt in v2. Falling back to v1: ${key}`, LOG_LEVEL_INFO);
            try {
                const r = await this.decryptDataV1(u, set);
                Logger(`Decrypted in v1: ${key}`, LOG_LEVEL_VERBOSE);
                return r;
            } catch (ex2) {
                Logger(`Could not decrypt in v1: ${key}`, LOG_LEVEL_VERBOSE);
                Logger(ex, LOG_LEVEL_VERBOSE);
                Logger(ex2, LOG_LEVEL_VERBOSE);
                throw ex2;
            }
        }
    }

    async encryptForUpload(
        key: string,
        data: Uint8Array<ArrayBuffer>,
        set: RemoteDBSettings
    ): Promise<Uint8Array<ArrayBuffer>> {
        if (!set.encrypt || set.passphrase == "" || this.isEncryptionPrevented(key)) {
            return data;
        }

        if (set.E2EEAlgorithm === E2EEAlgorithms.V2) {
            const salt = await this.getReplicationPBKDF2Salt();
            return await encryptBinaryHKDF(data, set.passphrase, salt);
        } else {
            return await encryptBinary(data, set.passphrase, set.useDynamicIterationCount);
        }
    }

    getDocKey(doc: EntryDoc) {
        if (doc && doc._id.startsWith("h:")) {
            return doc._id;
        }
        return doc._id + "-" + doc._rev;
    }

    async _createJournalPack(override?: number | string) {
        const checkPointInfo = await this.getCheckpointInfo();
        const from = override || checkPointInfo.lastLocalSeq;
        Logger(`Journal reading from seq:${from}`, LOG_LEVEL_VERBOSE);
        let knownKeyCount = 0;
        const allChangesTask = this.db.changes({
            live: false,
            since: override || from,
            conflicts: true,
            limit: this.batchSize,
            return_docs: true,
            attachments: false,
            style: "all_docs",
            // NOTE: Do NOT add a filter function here that tests the winning-revision doc.
            // With style:"all_docs", each change entry can carry multiple leaf revisions
            // (e.g. the winner plus a newly-created tombstone for a resolved conflict).
            // A filter based on the winner's key would incorrectly suppress the entire entry
            // even when one of the other leaf revisions (e.g. the tombstone) has never been
            // sent.  Per-revision deduplication is handled correctly by the second filter
            // applied after bulkGet below.
        });
        const allChanges = await allChangesTask;
        if (allChanges.results.length == 0) {
            return { changes: [], hasNext: false, packLastSeq: allChanges.last_seq };
        }
        const bd = await this.db.bulkGet({
            docs: allChanges.results.map((e) => e.changes.map((change) => ({ id: e.id, rev: change.rev }))).flat(),
            revs: true,
        });
        const packLastSeq = allChanges.last_seq;
        const dbInfo = await this.db.info();
        const hasNext = packLastSeq < dbInfo.update_seq;
        const docs = bd.results.map((e) => e.docs).flat();

        const docChanges = docs
            .filter((e) => "ok" in e)
            .map((e) => e.ok as EntryDoc)
            .filter((doc) => {
                const key = this.getDocKey(doc);
                if (this._currentCheckPointInfo.knownIDs.has(key)) {
                    knownKeyCount++;
                    return false;
                }
                if (this._currentCheckPointInfo.sentIDs.has(key)) {
                    knownKeyCount++;
                    return false;
                }
                return true;
            });
        Logger(
            `Checked ${allChanges.results.length} changed entries, selected ${docChanges.length} docs (${knownKeyCount} keys already known)`,
            LOG_LEVEL_DEBUG
        );
        return { changes: docChanges, hasNext, packLastSeq };
    }

    /**
     * Records a transfer's progress only while the checkpoint still has the reset generation the transfer started
     * from. Progress from before a reset would otherwise mark changes as sent or known which the reset asked to
     * send again, for example to a wiped remote.
     * @returns whether the progress was recorded.
     */
    private async _updateTransferCheckpoint(
        resetGeneration: number,
        func: (infoFrom: CheckPointInfo) => CheckPointInfo
    ): Promise<boolean> {
        let recorded = false;
        await this.updateCheckPointInfo((info) => {
            if (info.resetGeneration !== resetGeneration) return info;
            recorded = true;
            return func(info);
        });
        return recorded;
    }

    private _createSendReadableStream(
        startSeq: number,
        resetGeneration: number,
        logLevel: LOG_LEVEL,
        MSG_KEY: string,
        scan: { lastScannedSeq: number }
    ) {
        let currentLastSeq = startSeq;
        return new ReadableStream({
            pull: async (controller) => {
                while (true) {
                    if (this.requestedStop) {
                        Logger("Packing Journal : Stop requested", logLevel, MSG_KEY);
                        controller.close();
                        return;
                    }
                    const { changes, hasNext, packLastSeq } = await this._createJournalPack(currentLastSeq);
                    // Reading the pack refreshed the checkpoint. Stop scanning once its history has been reset.
                    if (this._currentCheckPointInfo.resetGeneration !== resetGeneration) {
                        controller.error(new JournalCheckpointResetError());
                        return;
                    }
                    currentLastSeq = packLastSeq as number;
                    scan.lastScannedSeq = currentLastSeq;
                    if (changes.length > 0) {
                        controller.enqueue({ changes, packLastSeq });
                        return;
                    }
                    if (!hasNext) {
                        controller.close();
                        return;
                    }
                }
            },
        });
    }

    private _createSendCompressTransformStream(
        startSeq: number,
        seqToProcess: number,
        logLevel: LOG_LEVEL,
        MSG_KEY: string,
        stats: { packedDocs: number }
    ) {
        const maxOutBufLength = 250;
        const maxBinarySize = 1024 * 1024 * 10;
        const createBuffer = (): SendBuffer => ({ rows: [], binarySize: 0, sentIDs: [] });
        const isFull = (buffer: SendBuffer) =>
            buffer.rows.length > maxOutBufLength || buffer.binarySize > maxBinarySize;
        // Large chunks are collected apart from everything else, so that they are sent in journals which hold
        // nothing but chunks. A receiver can then fetch those after the journals which hold documents.
        let ordinary = createBuffer();
        let bulk = createBuffer();
        let lastProcessedSeq = startSeq;
        // A batch can end inside a pack. It may only advance the checkpoint past the packs it contains completely;
        // otherwise a failed upload of the remainder would leave that pack's later documents unsent for good.
        let lastCompletedPackSeq = startSeq;
        // The last sequence at which both buffers were empty, so that nothing before it waits to be uploaded.
        let safeSeq = startSeq;

        /**
         * Close both buffers together, the large chunks first, so that a document never precedes its chunks.
         *
         * Only the last journal carries `packLastSeq`. An earlier one carries the sequence before the batch, because
         * the checkpoint is recorded per uploaded journal and the entries of the journal after it are not stored yet.
         */
        const closeBatch = async (controller: TransformStreamDefaultController<SendUnit>, packLastSeq: number) => {
            const journals = [
                { buffer: bulk, isBulk: true },
                { buffer: ordinary, isBulk: false },
            ].filter(({ buffer }) => buffer.rows.length > 0);
            for (const [index, { buffer, isBulk }] of journals.entries()) {
                const bin = await wrappedDeflate(concatUInt8Array(buffer.rows), { consume: true, level: 8 });
                const isLast = index === journals.length - 1;
                controller.enqueue({
                    bin,
                    packLastSeq: isLast ? packLastSeq : safeSeq,
                    sentIDs: buffer.sentIDs,
                    isBulk,
                });
            }
            ordinary = createBuffer();
            bulk = createBuffer();
            safeSeq = packLastSeq;
        };

        return new TransformStream<{ changes: EntryDoc[]; packLastSeq: number | string }, SendUnit>({
            transform: async (chunk, controller) => {
                lastProcessedSeq = chunk.packLastSeq as number;
                const currentSeq = lastProcessedSeq - startSeq;
                Logger(`Packing Journal: ${currentSeq} / ${seqToProcess}`, logLevel, MSG_KEY);

                for (const [index, row] of chunk.changes.entries()) {
                    const serialized = serializeDoc(row);
                    const buffer = isChunkEntry(row) && serialized.length >= BULK_CHUNK_MIN_BYTES ? bulk : ordinary;
                    buffer.sentIDs.push(this.getDocKey(row));
                    buffer.binarySize += serialized.length;
                    buffer.rows.push(serialized);
                    stats.packedDocs++;

                    if (isFull(buffer)) {
                        // A batch closed on the last row of a pack contains that whole pack.
                        const packLastSeq =
                            index === chunk.changes.length - 1 ? (chunk.packLastSeq as number) : lastCompletedPackSeq;
                        await closeBatch(controller, packLastSeq);
                    }
                }
                lastCompletedPackSeq = chunk.packLastSeq as number;
            },
            flush: async (controller) => {
                await closeBatch(controller, lastProcessedSeq);
            },
        });
    }

    private _createSendUploadWritableStream(
        max: number,
        startSeq: number,
        resetGeneration: number,
        writerId: string,
        logLevel: LOG_LEVEL,
        MSG_KEY: string,
        stats: { uploadedFiles: number }
    ) {
        let sentFilesCount = 0;
        let partIndex = 0;
        return new WritableStream<SendUnit>({
            write: async (chunk) => {
                // Batches inside one pack share its sequence tag, and a retry filters out what was already sent.
                // Binding the name to the batch's documents keeps a retry of the same batch on the same object,
                // while a batch with other documents can never overwrite a journal file other devices may have read.
                const contentId = await sha256Hex(chunk.sentIDs.join("\u0000"));
                const operationId = await sha256Hex(
                    `${writerId}\u0000${startSeq}\u0000${String(chunk.packLastSeq)}\u0000${partIndex++}\u0000${contentId}`
                );
                const filename = `${operationId}${chunk.isBulk ? BULK_JOURNAL_SUFFIX : JOURNAL_SUFFIX}`;
                const mime = "application/octet-stream";

                const encryptedBin = await this.encryptForUpload(filename, chunk.bin, this.currentSettings);

                // A retry finds the journal of its earlier attempt under the same name. Replacing it would move the
                // time the storage reports for it past journals which other devices made on it meanwhile.
                const ret = await this.storage.upload(filename, encryptedBin, mime, { keepExisting: true });
                if (!ret) {
                    throw new Error(`Could not send journalPack to the bucket (${filename})`);
                }

                sentFilesCount++;
                stats.uploadedFiles++;
                this.updateInfo({
                    sent: sentFilesCount,
                    maxPushSeq: max,
                    lastSyncPushSeq: chunk.packLastSeq as number,
                });

                const recorded = await this._updateTransferCheckpoint(resetGeneration, (info) => ({
                    ...info,
                    lastLocalSeq: chunk.packLastSeq,
                    sentIDs: setAllItems(info.sentIDs, chunk.sentIDs),
                    sentFiles: info.sentFiles.add(filename),
                }));
                if (!recorded) {
                    throw new JournalCheckpointResetError();
                }

                Logger(`Uploading journal: ${sentFilesCount} / ...`, logLevel, MSG_KEY);
            },
        });
    }

    private _runningTransfers = new Set<Promise<unknown>>();

    private async _trackTransfer<T>(transfer: Promise<T>): Promise<T> {
        this._runningTransfers.add(transfer);
        try {
            return await transfer;
        } finally {
            this._runningTransfers.delete(transfer);
        }
    }

    /**
     * Wait until every journal send and receive which this client started has finished. Callers request a stop
     * first, so that a transfer does not record progress after they change the remote or the checkpoint.
     */
    async waitForTransfersToSettle(): Promise<void> {
        while (this._runningTransfers.size > 0) {
            await Promise.allSettled([...this._runningTransfers]);
        }
    }

    async sendLocalJournal(showMessage = false) {
        this.updateInfo({ syncStatus: "JOURNAL_SEND" });
        return await this._trackTransfer(this._sendLocalJournal(showMessage));
    }

    private async _sendLocalJournal(showMessage: boolean) {
        return await shareRunningResult("send_journal_stream", async () => {
            this.requestedStop = false;
            const logLevel = showMessage ? LOG_LEVEL_NOTICE : LOG_LEVEL_INFO;
            const MSG_KEY = "pack_journal";

            const max = (await this.db.info()).update_seq as number;
            const checkPointInfo = await this.getCheckpointInfo();
            const startSeq = checkPointInfo.lastLocalSeq as number;
            const resetGeneration = checkPointInfo.resetGeneration;
            const seqToProcess = max - startSeq;
            const deviceAndVaultName = this.env.services.setting.getDeviceAndVaultName();
            if (!deviceAndVaultName) {
                throw new Error("A device-local synchronisation identity is required before journal upload.");
            }
            const writerId = await sha256Hex(deviceAndVaultName);

            Logger(`Packing Journal: Start sending`, logLevel, MSG_KEY);

            const stats = { packedDocs: 0, uploadedFiles: 0 };
            const scan = { lastScannedSeq: startSeq };
            const readable = this._createSendReadableStream(startSeq, resetGeneration, logLevel, MSG_KEY, scan);
            const transform = this._createSendCompressTransformStream(startSeq, seqToProcess, logLevel, MSG_KEY, stats);
            const writable = this._createSendUploadWritableStream(
                max,
                startSeq,
                resetGeneration,
                writerId,
                logLevel,
                `${MSG_KEY}_upload`,
                stats
            );

            try {
                await readable.pipeThrough(transform).pipeTo(writable);
                // The pipe only resolves after every read pack was uploaded, so each change up to the
                // scanned sequence is either sent or already known. Persisting it keeps a device which
                // has only received changes from scanning the same entries again on every cycle.
                const recorded = await this._updateTransferCheckpoint(resetGeneration, (info) =>
                    typeof info.lastLocalSeq === "number" && scan.lastScannedSeq > info.lastLocalSeq
                        ? { ...info, lastLocalSeq: scan.lastScannedSeq }
                        : info
                );
                if (!recorded) {
                    throw new JournalCheckpointResetError();
                }
                if (seqToProcess != 0) {
                    Logger(
                        `Packing Journal: Finished. Processed ${stats.packedDocs} doc(s) into ${stats.uploadedFiles} chunk(s)`,
                        logLevel,
                        MSG_KEY
                    );
                    if (stats.uploadedFiles > 0) {
                        Logger(
                            `Uploading journal: All ${stats.uploadedFiles} chunk(s) uploaded`,
                            logLevel,
                            `${MSG_KEY}_upload`
                        );
                    }
                } else {
                    Logger(`Packing Journal: No journals to be packed!`, logLevel, MSG_KEY);
                }
                this.updateInfo({ syncStatus: "COMPLETED" });
                return true;
            } catch (ex) {
                if (ex instanceof JournalCheckpointResetError) {
                    Logger(`Packing Journal: The journal history was reset, so the next send starts again`, logLevel);
                } else {
                    Logger(`Packing Journal Error`, logLevel);
                }
                Logger(ex, LOG_LEVEL_VERBOSE);
                this.updateInfo({ syncStatus: "ERRORED" });
                return false;
            }
        });
    }

    /**
     * The remote journals which this device has neither sent nor received, in the order in which they are applied.
     *
     * That is the order in which they were uploaded. A journal carries the chunks of its documents or follows the
     * journals which do, and a revision follows the revisions it was made on. In any other order a document can
     * arrive before its chunks, and an old revision after a history which no longer reaches back to it, where it
     * stays as a conflict. The name of a journal says nothing about that order, so journals are applied by name only
     * when the storage cannot report it.
     *
     * Journals whose order the storage cannot tell apart are returned as one group, which is applied as a whole.
     */
    async _getRemoteJournals(): Promise<string[][]> {
        const checkPointInfo = await this.getCheckpointInfo();
        const groups =
            (await this.storage.listFilesInUploadOrder?.()) ??
            (await this.storage.listFiles(""))
                .sort((a, b) => a.localeCompare(b, undefined, { numeric: true }))
                .map((key) => [key]);
        return groups
            .map((keys) => keys.filter((key) => this._isUnprocessedJournal(key, checkPointInfo)))
            .filter((keys) => keys.length > 0);
    }

    private _isUnprocessedJournal(key: string, checkPointInfo: CheckPointInfo): boolean {
        return !key.startsWith("_") && !checkPointInfo.sentFiles.has(key) && !checkPointInfo.receivedFiles.has(key);
    }

    /**
     * The remote journals to receive, split into those which are applied in upload order and those which hold only
     * large chunks.
     *
     * The journals of large chunks are told by their name and returned smallest first, so that the content which
     * takes least to fetch arrives first; those of one size stay in upload order. Storage which cannot list sizes
     * leaves every journal in upload order.
     */
    private async _getRemoteJournalsByKind(): Promise<{ ordinary: string[][]; bulk: string[] }> {
        if (!this.storage.listObjectsInUploadOrder) {
            return { ordinary: await this._getRemoteJournals(), bulk: [] };
        }
        const checkPointInfo = await this.getCheckpointInfo();
        const groups = (await this.storage.listObjectsInUploadOrder()).map((objects) =>
            objects.filter(({ key }) => this._isUnprocessedJournal(key, checkPointInfo))
        );
        const isBulk = ({ key }: JournalStorageObject) => key.endsWith(BULK_JOURNAL_SUFFIX);
        // A size which the storage did not report sorts last, as the journal may be large.
        const sizeOf = ({ size }: JournalStorageObject) => size ?? Number.MAX_SAFE_INTEGER;
        return {
            ordinary: groups
                .map((objects) => objects.filter((object) => !isBulk(object)).map(({ key }) => key))
                .filter((keys) => keys.length > 0),
            bulk: groups
                .flat()
                .filter((object) => isBulk(object) && !this._refusedBulkJournals.has(object.key))
                .sort((a, b) => sizeOf(a) - sizeOf(b))
                .map(({ key }) => key),
        };
    }

    /**
     * @param resetGeneration when given, the documents are only recorded as known while the checkpoint still has
     * this reset generation.
     */
    async processDocuments(allDocs: ProcessingEntry[], resetGeneration?: number) {
        let applyTotal = 0;
        let wholeItems = 0;
        const recordKnown = async (ids: string[]) => {
            const update = (info: CheckPointInfo) => ({ ...info, knownIDs: setAllItems(info.knownIDs, ids) });
            if (resetGeneration === undefined) {
                await this.updateCheckPointInfo(update);
                return true;
            }
            if (await this._updateTransferCheckpoint(resetGeneration, update)) return true;
            Logger(`The journal history was reset while receiving`, LOG_LEVEL_INFO);
            return false;
        };
        try {
            // Sort transferred into chunks and docs.
            const chunks = [] as typeof allDocs;
            const docs = [] as typeof allDocs;
            allDocs.forEach((e) => {
                if (e._id.startsWith("h:")) {
                    chunks.push(e);
                } else {
                    docs.push(e);
                }
            });

            // Chunk saving.
            // Chunks always have the same content, hence revision comparisons are unnecessary
            try {
                const e1 = (await this.db.allDocs({ include_docs: true, keys: [...chunks.map((e) => e._id)] })).rows;
                const e2 = e1.map((e) => (e as { id?: string }).id ?? undefined);
                const existChunks = new Set(e2.filter((e) => e !== undefined));
                const saveChunks = chunks
                    .filter((e) => !existChunks.has(e._id))
                    .map((e) => ({ ...e, _rev: undefined as string | undefined }));
                const ret = await this.db.bulkDocs<EntryDoc>(saveChunks, { new_edits: true });
                const saveError = ret.filter((e) => "error" in e).map((e) => e.id);

                saveChunks
                    .filter((e) => saveError.indexOf(e._id) === -1)
                    .forEach((doc) =>
                        this.env.services.context.events.emitEvent(REMOTE_CHUNK_FETCHED, doc as EntryLeaf)
                    );

                if (!(await recordKnown(chunks.map((e) => this.getDocKey(e))))) return false;
            } catch (ex) {
                Logger(`Applying chunks failed`, LOG_LEVEL_INFO);
                Logger(ex, LOG_LEVEL_VERBOSE);
                return false;
            }

            // Docs saving.
            // Docs have different revisions, hence revision comparisons and merging are necessary.
            const params = docs.map((e) => [e._id, [e._rev]] as const);
            const docsRevs = params.reduce(
                (acc, [id, revs]) => {
                    return { ...acc, [id]: [...(acc[id] ?? []), ...revs] };
                },
                {} as { [key: string]: string[] }
            );
            const diffRevs = await this.db.revsDiff(docsRevs);
            const saveDocs = docs.filter(
                (e) =>
                    e._id in diffRevs &&
                    "missing" in diffRevs[e._id] &&
                    (diffRevs[e._id].missing?.indexOf(e._rev) ?? 0) !== -1
            );

            await this.db.bulkDocs<EntryDoc>(saveDocs, { new_edits: false });

            const writeDoc = !this.env.services.setting.currentSettings().suspendParseReplicationResult;
            if (writeDoc) {
                await this.processReplication(saveDocs satisfies PouchDB.Core.ExistingDocument<EntryDoc>[]);
            }

            if (!(await recordKnown(docs.map((e) => this.getDocKey(e))))) return false;

            applyTotal += saveDocs.length;
            wholeItems += docs.length;
            Logger(
                `Applied ${applyTotal} of ${wholeItems} docs (${wholeItems - applyTotal} skipped)`,
                LOG_LEVEL_VERBOSE
            );
            return true;
        } catch (ex) {
            Logger(`Applying journal failed`, LOG_LEVEL_INFO);
            Logger(ex, LOG_LEVEL_VERBOSE);
            return false;
        }
    }

    private _createReceiveReadableStream(groups: string[][]) {
        return new ReadableStream({
            pull: (controller) => {
                if (this.requestedStop || groups.length === 0) {
                    controller.close();
                    return;
                }
                const group = groups.shift();
                if (group) {
                    controller.enqueue(group);
                }
            },
        });
    }

    /** Download one journal and parse the chunks and documents it carries. */
    private async _readRemoteJournal(key: string): Promise<ProcessingEntry[]> {
        const encryptedData = await this.storage.download(key, true);
        if (encryptedData === false) {
            throw new Error("Download Error");
        }
        return await this._parseRemoteJournal(key, encryptedData);
    }

    /** Decrypt a downloaded journal and parse the chunks and documents it carries. */
    private async _parseRemoteJournal(key: string, encryptedData: Uint8Array): Promise<ProcessingEntry[]> {
        const data = await this.decryptDownloaded(key, encryptedData as Uint8Array<ArrayBuffer>, this.currentSettings);

        const decompressed = await wrappedInflate(new Uint8Array(data), { consume: true });
        let idxFrom = 0;
        let idxTo = 0;
        const d = new TextDecoder();
        const result = [] as ProcessingEntry[];
        do {
            idxTo = decompressed.indexOf(0x0a, idxFrom);
            if (idxTo == -1) break;
            const piece = decompressed.slice(idxFrom, idxTo);
            const strPiece = d.decode(piece);
            if (strPiece.startsWith("~")) {
                const [idPart, dataPart] = strPiece.substring(1).split(UNIT_SPLIT);
                result.push({
                    _id: idPart as DocumentID,
                    data: unescapeNewLineFromString(dataPart),
                    type: "leaf",
                    _rev: "",
                });
            } else {
                result.push(JSON.parse(strPiece));
            }
            idxFrom = idxTo + 1;
        } while (idxTo > 0);
        return result;
    }

    private _createReceiveTransformStream(logLevel: LOG_LEVEL, resetGeneration: number) {
        let count = 0;
        return new TransformStream({
            // The journals of one group are applied together, because their order is not known. Applied together,
            // their chunks are stored before any of their documents.
            transform: async (group: string[], controller) => {
                const keys: string[] = [];
                const docs: ProcessingEntry[] = [];
                for (const key of group) {
                    count++;
                    Logger(`Receiving Journal: ${count}`, logLevel, "receivejournal");

                    const checkPointInfo = await this.getCheckpointInfo();
                    if (checkPointInfo.sentFiles.has(key) || checkPointInfo.receivedFiles.has(key)) {
                        Logger(`Receiving Journal: ${key} is already processed`, LOG_LEVEL_VERBOSE);
                        const recorded = await this._updateTransferCheckpoint(resetGeneration, (info) => ({
                            ...info,
                            receivedFiles: info.receivedFiles.add(key),
                        }));
                        if (!recorded) {
                            controller.error(new JournalCheckpointResetError());
                            return;
                        }
                        continue; // Skip
                    }

                    try {
                        docs.push(...(await this._readRemoteJournal(key)));
                        keys.push(key);
                    } catch (ex) {
                        controller.error(ex);
                        return;
                    }
                }
                if (keys.length > 0) {
                    controller.enqueue({ keys, docs });
                }
            },
        });
    }

    private _createReceiveWritableStream(resetGeneration: number) {
        let downloaded = 0;
        return new WritableStream({
            write: async (chunk: { keys: string[]; docs: ProcessingEntry[] }) => {
                const { keys, docs } = chunk;
                if (docs.length > 0) {
                    const success = await this.processDocuments(docs, resetGeneration);
                    if (!success) {
                        throw new Error(`Could not process downloaded journals for ${keys.join(", ")}`);
                    }
                }

                const recorded = await this._updateTransferCheckpoint(resetGeneration, (info) => ({
                    ...info,
                    receivedFiles: setAllItems(info.receivedFiles, keys),
                }));
                if (!recorded) {
                    throw new JournalCheckpointResetError();
                }
                downloaded += keys.length;
                this.updateInfo({ arrived: downloaded, maxPullSeq: downloaded, lastSyncPullSeq: downloaded });
                for (const key of keys) {
                    Logger(`Processing journal: ${key} has been processed`, LOG_LEVEL_INFO);
                }
            },
        });
    }

    /**
     * Receive every remote journal: first those which are applied in upload order, then all journals of large
     * chunks. A caller which restores a database needs all of it, so success means that everything was received.
     */
    async receiveRemoteJournal(showMessage = false) {
        return await this._trackTransfer(
            (async () => {
                const deferred = await this._receiveOrdinaryJournals(showMessage);
                if (!deferred) return false;
                const { complete } = await this._receiveBulkJournals(deferred, showMessage);
                this.updateInfo({ syncStatus: complete ? "COMPLETED" : "ERRORED" });
                return complete;
            })()
        );
    }

    /**
     * Receive the journals which are applied in upload order.
     * @returns the journals of large chunks which are left to fetch, or false when the receive failed.
     */
    private async _receiveOrdinaryJournals(showMessage: boolean): Promise<DeferredReceive | false> {
        this.updateInfo({ syncStatus: "JOURNAL_RECEIVE" });
        return await shareRunningResult("receive_journal_stream", async () => {
            this.requestedStop = false;
            const logLevel = showMessage ? LOG_LEVEL_NOTICE : LOG_LEVEL_INFO;

            Logger("Receiving Journal: Getting list of remote journal", logLevel, "receivejournal");
            try {
                const resetGeneration = (await this.getCheckpointInfo()).resetGeneration;
                // A failed or aborted listing is a failed receive, like a failed download, not a thrown cycle.
                const { ordinary: files, bulk } = await this._getRemoteJournalsByKind();
                this._unreceivedBulkJournals = new Set(bulk);
                const deferred: DeferredReceive = { resetGeneration, journals: bulk };
                if (files.length == 0) {
                    Logger(`Receiving Journal: No journals needs to be downloaded`, logLevel, "receivejournal");
                    this.updateInfo({ syncStatus: "COMPLETED" });
                    return deferred;
                }

                const readable = this._createReceiveReadableStream(files);
                const transform = this._createReceiveTransformStream(logLevel, resetGeneration);
                const writable = this._createReceiveWritableStream(resetGeneration);
                await readable.pipeThrough(transform).pipeTo(writable);
                this.updateInfo({ syncStatus: "COMPLETED" });
                return deferred;
            } catch (ex) {
                Logger(`Receive Journal Error`, logLevel);
                Logger(ex, LOG_LEVEL_VERBOSE);
                this.updateInfo({ syncStatus: "ERRORED" });
                return false;
            }
        });
    }

    /**
     * Receive journals of large chunks, in the given order.
     *
     * Each journal is recorded as received once its chunks are stored, so a receive which stops early leaves the
     * rest to the next one. A journal which fails is left to be tried again, and the receive goes on with the next
     * one, so that one broken journal does not keep the larger ones behind it from ever being reached. Two failures
     * in a row end the receive, so that a lost connection does not go through the whole list.
     *
     * @param budgetMs when given, the receive is one round of a synchronisation: no further journal is started once
     * this time has passed, or once the host no longer allows it. Both are checked between journals, so a journal
     * which was started is always finished, and a round which may run fetches at least one.
     * @returns whether every journal was received, and how many were.
     */
    private async _receiveBulkJournals(
        deferred: DeferredReceive,
        showMessage: boolean,
        budgetMs?: number
    ): Promise<{ complete: boolean; received: number }> {
        const total = deferred.journals.length;
        if (total === 0) return { complete: true, received: 0 };
        // A synchronisation and a receive of everything can hold the same journals. One after the other, the
        // second finds what the first recorded instead of fetching it again.
        return await serialized("receive_bulk_journal_stream", async () => {
            const logLevel = showMessage ? LOG_LEVEL_NOTICE : LOG_LEVEL_INFO;
            const MSG_KEY = "receivebulkjournal";
            // Journals which failed are tried after the others, those which failed least often first, so that
            // journals which stay broken cannot end every receive before the others were reached.
            const failuresOf = (key: string) => this._failedBulkJournals.get(key) ?? 0;
            const journals = [...deferred.journals].sort((a, b) => failuresOf(a) - failuresOf(b));
            const startedAt = this.now();
            let received = 0;
            let complete = true;
            let failuresInARow = 0;
            for (const [index, key] of journals.entries()) {
                const mustEndRound =
                    budgetMs !== undefined &&
                    (!this.mayReceiveBulkJournals() || (index > 0 && this.now() - startedAt >= budgetMs));
                if (this.requestedStop || mustEndRound || failuresInARow >= BULK_RECEIVE_MAX_FAILURES_IN_A_ROW) {
                    Logger(`Receiving large chunks: ${total - index} journal(s) left for later`, logLevel, MSG_KEY);
                    return { complete: false, received };
                }
                this.updateInfo({ syncStatus: "JOURNAL_RECEIVE" });
                Logger(`Receiving large chunks: ${index + 1} / ${total}`, logLevel, MSG_KEY);
                try {
                    const outcome = await this._receiveBulkJournal(key, deferred.resetGeneration);
                    failuresInARow = 0;
                    this._failedBulkJournals.delete(key);
                    if (outcome === "refused") complete = false;
                    if (outcome === "received") received++;
                } catch (ex) {
                    // Trying to record progress read the checkpoint again. A history which was reset meanwhile
                    // stops the receive, and says nothing against the journal.
                    if (this._currentCheckPointInfo.resetGeneration !== deferred.resetGeneration) {
                        Logger(
                            `Receiving large chunks: The journal history was reset, so the rest is left for later`,
                            logLevel
                        );
                        Logger(ex, LOG_LEVEL_VERBOSE);
                        return { complete: false, received };
                    }
                    failuresInARow++;
                    complete = false;
                    const failures = failuresOf(key) + 1;
                    this._failedBulkJournals.set(key, failures);
                    // A first failure may pass. A second one in a row is worth telling, once.
                    Logger(
                        `Journal ${key} of large chunks could not be received; it is tried again later`,
                        failures === 2 ? LOG_LEVEL_NOTICE : logLevel
                    );
                    Logger(ex, LOG_LEVEL_VERBOSE);
                }
            }
            return { complete, received };
        });
    }

    /**
     * Receive one journal of large chunks.
     * @returns `received` once its chunks are stored and the journal is recorded; `absent` when nothing was left to
     * receive; `refused` when it holds something other than chunks. Any failure throws.
     */
    private async _receiveBulkJournal(
        key: string,
        resetGeneration: number
    ): Promise<"received" | "absent" | "refused"> {
        const checkPointInfo = await this.getCheckpointInfo();
        if (!this._isUnprocessedJournal(key, checkPointInfo)) {
            Logger(`Receiving Journal: ${key} is already processed`, LOG_LEVEL_VERBOSE);
            this._unreceivedBulkJournals?.delete(key);
            return "absent";
        }
        const downloaded = await this.storage.downloadWithResult(key, true);
        if (downloaded.status === JournalStorageReadStatuses.NOT_FOUND) {
            // A journal of large chunks may be removed once no document needs its chunks, also after it was listed.
            Logger(`Receiving Journal: ${key} is no longer stored`, LOG_LEVEL_VERBOSE);
            this._unreceivedBulkJournals?.delete(key);
            return "absent";
        }
        if (downloaded.status !== JournalStorageReadStatuses.AVAILABLE) {
            throw new Error("Download Error", { cause: downloaded.error });
        }
        const entries = await this._parseRemoteJournal(key, downloaded.value);
        if (!entries.every(isChunkEntry)) {
            // Applied now, its documents would follow revisions which were uploaded after them, and could stay as
            // conflicts. Nothing sends such a journal, so it is left alone and not fetched again by this client.
            this._refusedBulkJournals.add(key);
            this._unreceivedBulkJournals?.delete(key);
            Logger(
                `Journal ${key} is named as holding only chunks but holds a document, so it was not applied`,
                LOG_LEVEL_NOTICE
            );
            return "refused";
        }
        if (entries.length > 0 && !(await this.processDocuments(entries, resetGeneration))) {
            throw new Error(`Could not process downloaded journal ${key}`);
        }
        const recorded = await this._updateTransferCheckpoint(resetGeneration, (info) => ({
            ...info,
            receivedFiles: info.receivedFiles.add(key),
        }));
        if (!recorded) {
            throw new JournalCheckpointResetError();
        }
        this._unreceivedBulkJournals?.delete(key);
        Logger(`Processing journal: ${key} has been processed`, LOG_LEVEL_INFO);
        return "received";
    }

    /**
     * Journals of large chunks which the last listing showed and which are not received yet. There is none until
     * this client has listed its remote.
     */
    private _unreceivedBulkJournals: Set<string> | undefined;
    /** Journals named as holding only chunks which held a document. */
    private _refusedBulkJournals = new Set<string>();
    /** Journals of large chunks whose latest receive failed, with the number of failures in a row. */
    private _failedBulkJournals = new Map<string, number>();
    private _receivedBulkJournalInLastSync = false;

    /** Forget what this client learnt about the large chunks of a remote, when it is pointed at another one. */
    private _forgetBulkJournals(): void {
        this._unreceivedBulkJournals = undefined;
        this._refusedBulkJournals.clear();
        this._failedBulkJournals.clear();
        this._receivedBulkJournalInLastSync = false;
    }

    /**
     * Whether large chunks are still to come: the remote holds journals of them which this client has listed, has
     * not received, and expects to receive.
     *
     * A journal whose latest receive failed is tried again but not expected, so that a document which needs it is
     * reported as incomplete instead of waiting silently. Before this client has listed the remote it cannot tell,
     * and answers that there may be: a document which waited for such chunks when the application was closed is
     * then not reported as incomplete before the first listing.
     */
    hasDeferredRemoteContent(): boolean {
        if (!this._unreceivedBulkJournals) return true;
        for (const key of this._unreceivedBulkJournals) {
            if (!this._failedBulkJournals.has(key)) return true;
        }
        return false;
    }

    /**
     * Whether the last synchronisation received large chunks. Asking clears the answer.
     *
     * Documents which waited for those chunks can be applied now, and more chunks may remain, so a host starts the
     * next synchronisation at once instead of at its next interval.
     */
    takeDeferredReceiveContinuation(): boolean {
        const received = this._receivedBulkJournalInLastSync;
        this._receivedBulkJournalInLastSync = false;
        return received;
    }

    /**
     * Receive the remote journals which hold documents, send the local changes, and then fetch large chunks for a
     * limited time. Neither the documents of the remote nor the local changes then wait for large content, and the
     * next synchronisation goes on fetching where this one stopped.
     */
    async sync(showResult = false) {
        return (
            (await shareRunningResult("replicate", async () => {
                this.requestedStop = false;
                this._receivedBulkJournalInLastSync = false;
                const deferred = await this._trackTransfer(this._receiveOrdinaryJournals(showResult));
                if (this.requestedStop) return;
                if (!deferred) {
                    const logLevel = showResult ? LOG_LEVEL_NOTICE : LOG_LEVEL_INFO;
                    Logger(
                        `Could not receive remote journal, so we prevent sending local journals to prevent unwanted mass transfers`,
                        logLevel
                    );
                    return;
                }
                const sent = await this.sendLocalJournal(showResult);
                const { received } = await this._trackTransfer(
                    this._receiveBulkJournals(deferred, showResult, this.bulkReceiveBudgetMs)
                );
                this._receivedBulkJournalInLastSync = received > 0;
                // Large chunks which remain or failed are fetched later, so the result is that of the send.
                this.updateInfo({ syncStatus: sent ? "COMPLETED" : "ERRORED" });
                return sent;
            })) ?? false
        );
    }

    requestStop() {
        this.requestedStop = true;
    }

    /**
     * Abort Object Storage requests which started before `startedBefore` and are still waiting.
     * A host calls this when it knows such requests were suspended, for example while a mobile app was in the
     * background. The interrupted operation fails normally and its durable work remains pending.
     * @returns the number of requests which were aborted.
     */
    abortStaleRemoteRequests(startedBefore: number): number {
        return this.storage.abortRequestsStartedBefore?.(startedBefore) ?? 0;
    }
}
