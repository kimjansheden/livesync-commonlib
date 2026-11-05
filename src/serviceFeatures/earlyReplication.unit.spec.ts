import { describe, expect, it, vi } from "vitest";
import { serialized } from "octagonal-wheels/concurrency/lock";
import {
    EarlyReplication,
    EarlyDatabaseReadError,
    EARLY_PATH_RECEIPT_PREFIX,
    FULL_SCAN_RECEIPT_KEY,
    EARLY_CAPTURE_CONCURRENCY,
    getEarlyReplication,
} from "./earlyReplication";
import {
    useOfflineScanner,
    performFullScan,
    FullScanModes,
    ExtraOnRemote,
    collectDatabaseFiles,
} from "./offlineScanner";
import { handlers } from "@lib/services/lib/HandlerUtils";
import { createServiceContext } from "@lib/services/base/ServiceBase";
import { EVEN } from "@lib/common/models/shared.const.symbols";
import { REMOTE_MINIO, type MetaEntry } from "@lib/common/types";
import { UnknownFileWriteStateError } from "@lib/serviceModules/FilePublicationCoordinator";

const note = (path = "note.md"): MetaEntry =>
    ({
        _id: path,
        _rev: "2-remote",
        path,
        type: "plain",
        datatype: "plain",
        ctime: 1,
        mtime: 2,
        size: 4,
        children: [],
        eden: {},
    }) as MetaEntry;
function setup(entries = new Map<string, unknown>()) {
    let unloaded = false;
    let suspended = false;
    let exists = false;
    const store = {
        get: vi.fn(async (key: string) => entries.get(key)),
        set: vi.fn(async (key: string, value: unknown) => {
            entries.set(key, structuredClone(value));
        }),
        delete: vi.fn(async (key: string) => {
            entries.delete(key);
        }),
    };
    const prepareOfflinePath = vi.fn(async () => true);
    const host = {
        services: {
            context: createServiceContext(),
            API: { addLog: vi.fn() },
            appLifecycle: {
                hasUnloaded: () => unloaded,
                isSuspended: () => suspended,
                onBeforeUnload: handlers<{ event(): Promise<boolean> }>().bailFirstFailure("event"),
                onFirstInitialise: handlers<{ event(): Promise<boolean> }>().bailFirstFailure("event"),
                getUnresolvedMessages: handlers<{ event(): Promise<boolean> }>().bailFirstFailure("event"),
            },
            setting: {
                currentSettings: () => ({
                    remoteType: REMOTE_MINIO,
                    isConfigured: true,
                    handleFilenameCaseSensitive: true,
                    suspendFileWatching: false,
                    maxMTimeForReflectEvents: 0,
                    automaticallyDeleteMetadataOfDeletedFiles: 0,
                }),
            },
            database: {
                isDatabaseReady: () => true,
                localDatabase: {
                    allDocsRaw: vi.fn(async ({ keys }: { keys: string[] }) => ({
                        rows: keys.map((key) => (exists ? { id: key, doc: note(key) } : { key, error: "not_found" })),
                    })),
                    findAllNormalDocs: vi.fn(async function* () {
                        throw new Error("metadata enumeration must not run");
                    }),
                    findAllDocs: vi.fn(async function* () {}),
                },
            },
            vault: {
                isValidPath: vi.fn(() => true),
                isTargetFile: vi.fn(async () => true),
                isFileSizeTooLarge: () => false,
                scanVault: handlers<{
                    scan(
                        show?: boolean,
                        ignore?: boolean,
                        outcome?: import("@lib/services/base/IService").VaultScanOutcome
                    ): Promise<boolean>;
                }>().bailFirstFailure("scan"),
            },
            path: {
                getPath: (doc: MetaEntry) => doc.path,
                path2id: vi.fn(async (path: string) => path),
                compareFileFreshness: () => EVEN,
            },
            keyValueDB: {
                simpleStore: store,
                kvDB: { get: vi.fn(async () => undefined), set: vi.fn(async () => undefined) },
            },
            replication: { parseSynchroniseResult: vi.fn(async () => true) },
            fileProcessing: {},
            databaseEvents: {},
            replicator: {},
        },
        serviceModules: {
            fileHandler: {
                canPrepareOfflinePath: vi.fn(async () => true),
                requireOfflinePreparation: vi.fn(),
                requireOfflinePreparationForPath: vi.fn(),
                prepareOfflinePath,
                dbToStorage: vi.fn(async () => true),
            },
            storageAccess: {
                statHidden: vi.fn(async () => null),
                getFiles: vi.fn(async () => []),
                restoreState: vi.fn(async () => undefined),
                appendStorageEvents: vi.fn(async () => undefined),
            },
        },
    };
    const log = vi.fn();
    const session = new EarlyReplication(
        host as unknown as ConstructorParameters<typeof EarlyReplication>[0],
        log,
        () => false
    );
    return {
        session,
        host,
        entries,
        store,
        log,
        prepareOfflinePath,
        unload: () => {
            unloaded = true;
        },
        hide: () => {
            suspended = true;
        },
        resume: () => {
            suspended = false;
        },
        receive: () => {
            exists = true;
        },
    };
}

describe("path-protected early Object Storage reception", () => {
    it("receives a new small note without enumerating unrelated metadata", async () => {
        const h = setup();
        expect(await h.session.begin()).toBe(true);
        await h.session.capture([note()]);
        expect(h.prepareOfflinePath).not.toHaveBeenCalled();
        h.receive();
        const apply = vi.fn(async () => true);
        expect(await h.session.apply(note(), apply)).toBe(true);
        expect(apply).toHaveBeenCalledOnce();
        expect(h.host.services.database.localDatabase.findAllNormalDocs).not.toHaveBeenCalled();
        expect(h.entries.has(EARLY_PATH_RECEIPT_PREFIX + "note.md")).toBe(false);
        expect(h.entries.get(FULL_SCAN_RECEIPT_KEY)).toEqual({ version: 1, complete: false });
    });

    it("blocks only an uncertain path and keeps its original receipt across process death", async () => {
        const first = setup();
        await first.session.begin();
        first.host.serviceModules.fileHandler.canPrepareOfflinePath.mockResolvedValueOnce(false);
        await first.session.capture([note("uncertain.md"), note("safe.md")]);
        const next = setup(first.entries);
        await next.session.begin();
        next.receive();
        await next.session.capture([note("uncertain.md")]);
        const blocked = vi.fn(async () => true);
        const safe = vi.fn(async () => true);
        expect(await next.session.apply(note("uncertain.md"), blocked)).toBe(false);
        expect(await next.session.apply(note("safe.md"), safe)).toBe(true);
        expect(blocked).not.toHaveBeenCalled();
        expect(safe).toHaveBeenCalledOnce();
    });

    it("keeps the storage event lock through the applying callback", async () => {
        const h = setup();
        await h.session.begin();
        await h.session.capture([note()]);
        const order: string[] = [];
        let event: Promise<unknown> | undefined;
        await h.session.apply(note(), async () => {
            event = serialized("processFileEvent-note.md", async () => {
                order.push("event");
            });
            await Promise.resolve();
            expect(order).toEqual([]);
            order.push("reflection");
            return true;
        });
        await event;
        expect(order).toEqual(["reflection", "event"]);
    });

    it.each(["hide", "unload"] as const)(
        "does not apply or record completion after %s interrupts preparation",
        async (interrupt) => {
            const h = setup();
            await h.session.begin();
            await h.session.capture([note()]);
            h.prepareOfflinePath.mockImplementationOnce(async () => {
                h[interrupt]();
                return true;
            });
            const apply = vi.fn(async () => true);
            expect(await h.session.apply(note(), apply)).toBe(false);
            await h.session.completeScan(true);
            expect(apply).not.toHaveBeenCalled();
            expect(h.entries.get(FULL_SCAN_RECEIPT_KEY)).toEqual({ version: 1, complete: false });
            expect(h.entries.has(EARLY_PATH_RECEIPT_PREFIX + "note.md")).toBe(true);
        }
    );

    it("rejects an unreadable IndexedDB baseline before an incoming document can be saved", async () => {
        const h = setup();
        await h.session.begin();
        h.host.services.database.localDatabase.allDocsRaw.mockRejectedValueOnce(new Error("indexed_db_went_bad"));
        await expect(h.session.capture([note()])).rejects.toBeInstanceOf(EarlyDatabaseReadError);
        expect(h.prepareOfflinePath).not.toHaveBeenCalled();
    });

    it("quarantines unreadable publication provenance without delaying another path", async () => {
        const h = setup();
        await h.session.begin();
        h.host.serviceModules.fileHandler.canPrepareOfflinePath.mockRejectedValueOnce(new UnknownFileWriteStateError());
        await h.session.capture([note("unknown.md"), note("safe.md")]);
        expect(await h.session.apply(note("unknown.md"), async () => true)).toBe(false);
        expect(await h.session.apply(note("safe.md"), async () => true)).toBe(true);
    });

    it("lets fullscan resolve a failed path only after current local preparation succeeds", async () => {
        const h = setup();
        await h.session.begin();
        h.host.serviceModules.fileHandler.canPrepareOfflinePath.mockResolvedValueOnce(false);
        await h.session.capture([note()]);
        h.receive();
        h.prepareOfflinePath.mockResolvedValueOnce(false);
        expect(await h.session.scan("note.md" as MetaEntry["path"])).toBe("failed");
        expect(h.host.serviceModules.fileHandler.dbToStorage).not.toHaveBeenCalled();
        expect(await h.session.scan("note.md" as MetaEntry["path"])).toBe("completed");
        expect(h.host.serviceModules.fileHandler.dbToStorage).toHaveBeenCalledWith(
            expect.objectContaining({ _rev: "2-remote" }),
            "note.md"
        );
        expect(h.entries.has(EARLY_PATH_RECEIPT_PREFIX + "note.md")).toBe(false);
    });

    it("does not release an unsafe or mismatched receipt", async () => {
        const h = setup();
        await h.session.begin();
        h.entries.set(EARLY_PATH_RECEIPT_PREFIX + "note.md", {
            version: 1,
            prepared: true,
            path: "other.md",
            absent: true,
        });
        const apply = vi.fn(async () => true);
        expect(await h.session.apply(note(), apply)).toBe(false);
        expect(apply).not.toHaveBeenCalled();
    });

    it("retains a preserved deletion receipt when a process dies before reflection finishes", async () => {
        const first = setup();
        await first.session.begin();
        await first.session.capture([note()]);
        expect(await first.session.apply(note(), async () => false)).toBe(false);
        const next = setup(first.entries);
        await next.session.begin();
        next.receive();
        expect(await next.session.apply(note(), async () => true)).toBe(true);
        expect(next.prepareOfflinePath).toHaveBeenCalledWith("note.md", true, false, expect.any(Function));
    });

    it("writes the full-scan receipt only after successful complete processing", async () => {
        const h = setup();
        await h.session.begin();
        await h.session.completeScan(false);
        expect(h.entries.get(FULL_SCAN_RECEIPT_KEY)).toEqual({ version: 1, complete: false });
        await h.session.completeScan(true);
        expect(h.entries.get(FULL_SCAN_RECEIPT_KEY)).toEqual({ version: 1, complete: true });
        expect(h.log.mock.calls.every(([message]) => !String(message).includes("note.md"))).toBe(true);
    });

    it.each(["ignored", "non-target", "identity mismatch"])(
        "retains normal settlement for %s outside feature scope",
        async (reason) => {
            const h = setup();
            await h.session.begin();
            const doc = reason === "ignored" ? note("redflag.md") : note();
            if (reason === "non-target") h.host.services.vault.isTargetFile.mockResolvedValue(false);
            if (reason === "identity mismatch") h.host.services.path.path2id.mockResolvedValue("other");
            const apply = vi.fn(async () => true);
            expect(await h.session.apply(doc, apply)).toBe(true);
            expect(apply).toHaveBeenCalledOnce();
            h.unload();
            expect(await h.session.apply(doc, apply)).toBe(false);
            expect(apply).toHaveBeenCalledOnce();
        }
    );

    it("deactivates startup policy after completion while retaining isolated pending paths", async () => {
        const h = setup();
        await h.session.begin();
        h.host.serviceModules.fileHandler.canPrepareOfflinePath.mockResolvedValueOnce(false);
        await h.session.capture([note("blocked.md")]);
        await h.session.completeScan(true);
        expect(h.session.isActive).toBe(false);
        expect(h.host.serviceModules.fileHandler.requireOfflinePreparation).toHaveBeenLastCalledWith(false);
        const apply = vi.fn(async () => true);
        expect(await h.session.apply(note("later.md"), apply)).toBe(true);
        expect(await h.session.apply(note("blocked.md"), apply)).toBe(false);
        expect(h.prepareOfflinePath).not.toHaveBeenCalled();
    });

    it("retains a restored waiting path that was not part of the current journal capture", async () => {
        const h = setup();
        h.entries.set(EARLY_PATH_RECEIPT_PREFIX + "restored.md", {
            version: 1,
            path: "restored.md",
            prepared: false,
            absent: false,
        });
        await h.session.begin();
        const apply = vi.fn(async () => true);
        expect(await h.session.apply(note("restored.md"), apply)).toBe(false);
        await h.session.completeScan(true);
        expect(await h.session.apply(note("restored.md"), apply)).toBe(false);
        expect(apply).not.toHaveBeenCalled();
    });

    it("deactivates after a committed completion receipt even when hide arrives during persistence", async () => {
        const h = setup();
        await h.session.begin();
        h.store.set.mockImplementationOnce(async (key, value) => {
            h.entries.set(key, value);
            h.hide();
        });
        expect(await h.session.completeScan(true)).toBe(true);
        expect(h.session.isActive).toBe(false);
        expect(h.host.serviceModules.fileHandler.requireOfflinePreparation).toHaveBeenLastCalledWith(false);
    });

    it("removes a stale receipt when the latest document disappeared", async () => {
        const h = setup();
        await h.session.begin();
        await h.session.capture([note()]);
        expect(await h.session.scan("note.md" as MetaEntry["path"])).toBe("completed");
        expect(h.entries.has(EARLY_PATH_RECEIPT_PREFIX + "note.md")).toBe(false);
    });

    it.each(["hide", "unload"] as const)(
        "does not write a prepared conflict receipt after %s during stat",
        async (interrupt) => {
            const h = setup();
            await h.session.begin();
            h.receive();
            h.host.services.database.localDatabase.allDocsRaw.mockImplementation(async () => ({
                rows: [{ id: "note.md", doc: { ...note(), _conflicts: ["2-other"] } }],
            }));
            h.host.serviceModules.storageAccess.statHidden.mockImplementation(async () => {
                h[interrupt]();
                return null;
            });
            const before = h.store.set.mock.calls.length;
            expect(await h.session.scan("note.md" as MetaEntry["path"])).toBe("failed");
            expect(h.store.set.mock.calls.length).toBe(before);
        }
    );

    it("prepares a large journal with bounded parallel reads and retains each path lock", async () => {
        vi.useFakeTimers();
        try {
            const h = setup();
            await h.session.begin();
            let reads = 0;
            let maximum = 0;
            const prepared: string[] = [];
            h.host.services.database.localDatabase.allDocsRaw.mockImplementation(async ({ keys }) => {
                maximum = Math.max(maximum, ++reads);
                await new Promise((resolve) => setTimeout(resolve, 10));
                reads--;
                return { rows: keys.map((key) => ({ key, error: "not_found" })) };
            });
            h.host.serviceModules.fileHandler.canPrepareOfflinePath.mockImplementation(async (path?: unknown) => {
                prepared.push(String(path));
                return true;
            });
            const docs = Array.from({ length: 32 }, (_, index) => note(index === 7 ? "safe.md" : `other-${index}.md`));
            const started = Date.now();
            const capture = h.session.capture(docs);
            await vi.advanceTimersByTimeAsync(10);
            expect(prepared).toContain("safe.md");
            await vi.runAllTimersAsync();
            await capture;
            expect(maximum).toBe(EARLY_CAPTURE_CONCURRENCY);
            expect(Date.now() - started).toBeLessThanOrEqual(40);
            expect(docs.map((doc) => doc._id)).toEqual(
                Array.from({ length: 32 }, (_, index) => (index === 7 ? "safe.md" : `other-${index}.md`))
            );
            let release: (() => void) | undefined;
            const held = serialized(
                "processFileEvent-held.md",
                async () =>
                    await new Promise<void>((resolve) => {
                        release = resolve;
                    })
            );
            await Promise.resolve();
            const alongside = h.session.capture([note("held.md"), note("unrelated.md")]);
            await vi.advanceTimersByTimeAsync(10);
            expect(prepared).toContain("unrelated.md");
            expect(prepared).not.toContain("held.md");
            release?.();
            await vi.runAllTimersAsync();
            await alongside;
            await held;
            expect(prepared).toContain("held.md");
        } finally {
            vi.useRealTimers();
        }
    });
});

describe("Offline Scanner service composition", () => {
    function composed() {
        const h = setup();
        const host = h.host as unknown as Parameters<typeof useOfflineScanner>[0];
        useOfflineScanner(host);
        return { ...h, host };
    }

    it("installs all three early hooks on one host and restores normal scanner/apply semantics after completion", async () => {
        const h = composed();
        expect(await h.host.services.vault.prepareForEarlyReplication?.()).toBe(true);
        const session = getEarlyReplication(h.host);
        expect(session).toBeDefined();
        await h.host.services.replication.prepareIncomingDocuments?.([note()]);
        expect(h.entries.has(EARLY_PATH_RECEIPT_PREFIX + "note.md")).toBe(true);
        expect(await h.host.services.vault.withPreparedIncomingPath?.(note(), async () => true)).toBe(true);
        expect(h.entries.has(EARLY_PATH_RECEIPT_PREFIX + "note.md")).toBe(false);
        await session?.completeScan(true);
        expect(getEarlyReplication(h.host)).toBeUndefined();
        h.host.services.database.localDatabase.findAllNormalDocs = vi.fn(async function* () {
            yield note();
        }) as never;
        h.prepareOfflinePath.mockClear();
        expect(
            await performFullScan(h.host, h.log, { showError: vi.fn(), clearError: vi.fn() } as never, {
                mode: FullScanModes.DB_APPLY,
            })
        ).toBe(true);
        expect(h.prepareOfflinePath).not.toHaveBeenCalled();
        expect(await h.host.services.vault.withPreparedIncomingPath?.(note("new.md"), async () => true)).toBe(true);
    });

    it("finishes startup when a normal file failure is durably queued, but preserves its per-path protection", async () => {
        const h = composed();
        await h.host.services.vault.prepareForEarlyReplication?.();
        h.receive();
        h.host.services.database.localDatabase.findAllNormalDocs = vi.fn(async function* () {
            yield note();
        }) as never;
        h.prepareOfflinePath.mockRejectedValueOnce(new Error("synthetic file read failure"));
        const outcome = {};
        expect(await h.host.services.vault.scanVault(false, false, outcome)).toBe(false);
        expect(outcome).toMatchObject({ failedPairs: 1, queuedForReflection: 1 });
        expect(getEarlyReplication(h.host)).toBeUndefined();
        expect(h.host.services.replication.parseSynchroniseResult).toHaveBeenCalledOnce();
        expect(h.host.serviceModules.fileHandler.requireOfflinePreparation).toHaveBeenLastCalledWith(false);
    });

    it("does not downgrade a non-string-matched fatal database read into a file retry", async () => {
        const h = composed();
        await h.host.services.vault.prepareForEarlyReplication?.();
        h.receive();
        h.host.services.database.localDatabase.findAllNormalDocs = vi.fn(async function* () {
            yield note();
        }) as never;
        h.host.services.database.localDatabase.allDocsRaw.mockRejectedValueOnce(
            new Error("synthetic page backend read failure")
        );
        const outcome = {};
        expect(await h.host.services.vault.scanVault(false, false, outcome)).toBe(false);
        expect(outcome).toEqual({});
        expect(getEarlyReplication(h.host)).toBeDefined();
        expect(h.entries.get(FULL_SCAN_RECEIPT_KEY)).toEqual({ version: 1, complete: false });
        expect(h.host.services.replication.parseSynchroniseResult).not.toHaveBeenCalled();
    });

    it("samples identity timing and aggregates progress for a large synthetic metadata listing", async () => {
        const h = setup();
        const now = vi.spyOn(Date, "now");
        h.host.services.database.localDatabase.findAllNormalDocs.mockImplementation(async function* () {
            for (let i = 0; i < 4096; i++) yield note(`synthetic-${i}.md`);
        });
        try {
            const before = now.mock.calls.length;
            await collectDatabaseFiles(
                h.host as never,
                h.host.services.setting.currentSettings() as never,
                h.log,
                false
            );
            expect(now.mock.calls.length - before).toBeLessThan(100);
            expect(h.log.mock.calls.length).toBeLessThan(10);
        } finally {
            now.mockRestore();
        }
    });
});
