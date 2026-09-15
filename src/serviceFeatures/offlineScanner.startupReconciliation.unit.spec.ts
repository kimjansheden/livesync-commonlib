import { describe, it, expect, vi } from "vitest";
import {
    ExtraOnRemote,
    FullScanModes,
    queueStorageChangesSinceLastScan,
    synchroniseAllFilesBetweenDBandStorage,
    useOfflineScanner,
} from "./offlineScanner";
import { handlers } from "@lib/services/lib/HandlerUtils";
import { createServiceContext } from "@lib/services/base/ServiceBase";
import { createInstanceLogFunction } from "@lib/services/lib/logUtils";
import { EVEN } from "@lib/common/models/shared.const.symbols";
import type { FileEvent } from "@lib/interfaces/StorageEventManager";

type StoredFile = { path: string; mtime: number; size: number };

const stub = (file: StoredFile) => ({
    name: file.path.split("/").pop() ?? file.path,
    path: file.path,
    stat: { size: file.size, mtime: file.mtime, ctime: file.mtime, type: "file" as const },
});

const note = (path: string, mtime: number, size: number) => ({
    _id: path,
    path,
    mtime,
    size,
    type: "newnote",
    children: [],
});

/**
 * Compose the start-up order used by the hosts: the full scan lists storage and reconciles it, then
 * `onFirstInitialise` registers the Vault watcher (priority 0, as ServiceFileAccessBase does). The
 * simulated Vault reports a change to the watcher only once it has been registered.
 */
function createStartup(
    storage: StoredFile[],
    database: ReturnType<typeof note>[],
    options: { keepsWrittenMTime?: boolean } = {}
) {
    const files = new Map(storage.map((file) => [file.path, { ...file }]));
    let watching = false;
    let afterListing: (() => void) | undefined;
    let failNextListing = false;
    const queued: FileEvent[] = [];
    const vault = {
        write(path: string, mtime: number, size: number) {
            const existed = files.has(path);
            files.set(path, { path, mtime, size });
            if (watching) queued.push({ type: existed ? "CHANGED" : "CREATE", file: stub(files.get(path)!) });
        },
        remove(path: string) {
            const removed = files.get(path)!;
            files.delete(path);
            if (watching) queued.push({ type: "DELETE", file: { ...stub(removed), deleted: true } });
        },
    };
    const onFirstInitialise = handlers<{ onFirstInitialise: () => Promise<boolean> }>().bailFirstFailure(
        "onFirstInitialise"
    );
    const settings = {
        isConfigured: true,
        suspendFileWatching: false,
        maxMTimeForReflectEvents: 0,
        handleFilenameCaseSensitive: true,
        automaticallyDeleteMetadataOfDeletedFiles: 0,
    };
    const scanVault = handlers<{
        scanVault: (showNotice?: boolean, ignoreSuspending?: boolean) => Promise<boolean>;
    }>().bailFirstFailure("scanVault");
    const host = {
        services: {
            context: createServiceContext(),
            API: { addLog: vi.fn() },
            appLifecycle: { onFirstInitialise, getUnresolvedMessages: { addHandler: vi.fn() } },
            setting: { currentSettings: () => settings },
            keyValueDB: { kvDB: { get: vi.fn().mockResolvedValue(undefined), set: vi.fn() } },
            vault: {
                scanVault,
                isTargetFile: vi.fn().mockResolvedValue(true),
                isValidPath: vi.fn().mockReturnValue(true),
                isFileSizeTooLarge: vi.fn().mockReturnValue(false),
            },
            database: {
                localDatabase: {
                    findAllDocs: vi.fn(async function* () {}),
                    findAllNormalDocs: vi.fn(async function* () {
                        yield* database;
                    }),
                },
            },
            path: {
                getPath: vi.fn((doc: { path: string }) => doc.path),
                path2id: vi.fn(async (path: string) => path),
                compareFileFreshness: vi.fn(() => EVEN),
            },
            fileProcessing: {},
        },
        serviceModules: {
            storageAccess: {
                restoreState: vi.fn(),
                getFiles: vi.fn(async () => {
                    if (failNextListing) throw new Error("synthetic listing failure");
                    const listed = [...files.values()].map(stub);
                    const change = afterListing;
                    afterListing = undefined;
                    change?.();
                    return listed;
                }),
                delete: vi.fn(async (path: string) => {
                    files.delete(path);
                }),
                appendStorageEvents: vi.fn(async (events: FileEvent[]) => {
                    queued.push(...events);
                }),
            },
            fileHandler: {
                storeFileToDB: vi.fn().mockResolvedValue(true),
                dbToStorage: vi.fn(async (path: string) => {
                    const doc = database.find((entry) => entry.path === path)!;
                    const mtime = options.keepsWrittenMTime === false ? 90_000 : doc.mtime;
                    files.set(path, { path, mtime, size: doc.size });
                    return true;
                }),
            },
        },
    };
    useOfflineScanner(host as never);
    // Registered after the scanner, so only the handler priority puts the watcher first.
    onFirstInitialise.addHandler(() => {
        watching = true;
        return Promise.resolve(true);
    });
    return {
        host,
        settings,
        vault,
        afterListing: (change: () => void) => (afterListing = change),
        failNextListing: () => (failNextListing = true),
        start: async () => {
            await host.services.vault.scanVault(false, false);
            return await host.services.appLifecycle.onFirstInitialise();
        },
        queued: () =>
            queued.map((event) => ({ type: event.type, path: event.file.path, revalidate: event.revalidate === true })),
    };
}

describe("start-up reconciliation after the Vault watcher begins", () => {
    it("queues files created or changed after the scan listed storage but before the watcher began", async () => {
        const startup = createStartup(
            [
                { path: "stable.md", mtime: 10_000, size: 5 },
                { path: "edited.md", mtime: 10_000, size: 5 },
            ],
            [note("stable.md", 10_000, 5), note("edited.md", 10_000, 5)]
        );
        startup.afterListing(() => {
            startup.vault.write("created.md", 40_000, 3);
            startup.vault.write("edited.md", 40_000, 9);
        });

        await expect(startup.start()).resolves.toBe(true);

        expect(startup.queued()).toEqual([
            { type: "CHANGED", path: "edited.md", revalidate: true },
            { type: "CREATE", path: "created.md", revalidate: true },
        ]);
    });

    it("queues deletions and renames in the same window as revalidated intent", async () => {
        const startup = createStartup(
            [
                { path: "removed.md", mtime: 10_000, size: 5 },
                { path: "old-name.md", mtime: 10_000, size: 6 },
            ],
            [note("removed.md", 10_000, 5), note("old-name.md", 10_000, 6)]
        );
        startup.afterListing(() => {
            startup.vault.remove("removed.md");
            startup.vault.remove("old-name.md");
            startup.vault.write("new-name.md", 10_000, 6);
        });

        await expect(startup.start()).resolves.toBe(true);

        expect(startup.queued()).toEqual([
            { type: "CREATE", path: "new-name.md", revalidate: true },
            { type: "DELETE", path: "removed.md", revalidate: true },
            { type: "DELETE", path: "old-name.md", revalidate: true },
        ]);
    });

    it("does not queue unchanged files or files the scan itself wrote from the database", async () => {
        const startup = createStartup(
            [{ path: "stable.md", mtime: 10_000, size: 5 }],
            [note("stable.md", 10_000, 5), note("remote.md", 30_000, 7)]
        );

        await expect(startup.start()).resolves.toBe(true);

        expect(startup.queued()).toEqual([]);
    });

    it("does not queue a deletion for a path the scan removed itself", async () => {
        const startup = createStartup([{ path: "local-only.md", mtime: 10_000, size: 5 }], []);
        const log = createInstanceLogFunction("test", startup.host.services.API as never);

        await synchroniseAllFilesBetweenDBandStorage(startup.host as never, log, {} as never, {
            mode: FullScanModes.DB_APPLY,
            extraOnRemote: ExtraOnRemote.DELETE_LOCAL_MISSING,
        });
        expect(startup.host.serviceModules.storageAccess.delete).toHaveBeenCalledWith("local-only.md", true);

        await expect(queueStorageChangesSinceLastScan(startup.host as never, log)).resolves.toBe(true);

        expect(startup.queued()).toEqual([]);
    });

    it("does not stop start-up when the reconciliation cannot list storage", async () => {
        const startup = createStartup([{ path: "stable.md", mtime: 10_000, size: 5 }], [note("stable.md", 10_000, 5)]);
        startup.afterListing(() => {
            startup.failNextListing();
        });

        await expect(startup.start()).resolves.toBe(true);

        expect(startup.queued()).toEqual([]);
    });

    it("leaves changes made after the watcher began to the watcher alone", async () => {
        const startup = createStartup([{ path: "stable.md", mtime: 10_000, size: 5 }], [note("stable.md", 10_000, 5)]);

        await expect(startup.start()).resolves.toBe(true);
        startup.vault.write("later.md", 50_000, 4);

        expect(startup.queued()).toEqual([{ type: "CREATE", path: "later.md", revalidate: false }]);
    });

    it("does not lose a change made while the reconciliation lists storage", async () => {
        const startup = createStartup([{ path: "stable.md", mtime: 10_000, size: 5 }], [note("stable.md", 10_000, 5)]);
        startup.afterListing(() => {
            startup.afterListing(() => startup.vault.write("during.md", 60_000, 2));
        });

        await expect(startup.start()).resolves.toBe(true);

        expect(startup.queued()).toEqual([{ type: "CREATE", path: "during.md", revalidate: false }]);
    });

    it("does not queue a listed file which the scan left alone and nobody changed", async () => {
        // A document whose identity does not match its path is quarantined, so the scan records nothing for it.
        const startup = createStartup(
            [{ path: "quarantined.md", mtime: 10_000, size: 5 }],
            [{ ...note("quarantined.md", 20_000, 5), _id: "stale-id" }]
        );

        await expect(startup.start()).resolves.toBe(true);

        expect(startup.queued()).toEqual([]);
    });

    it("leaves the changes to the next scan when the case setting changed after the listing", async () => {
        const startup = createStartup([{ path: "stable.md", mtime: 10_000, size: 5 }], [note("stable.md", 10_000, 5)]);
        startup.afterListing(() => startup.vault.write("Created.md", 40_000, 3));

        await startup.host.services.vault.scanVault(false, false);
        startup.settings.handleFilenameCaseSensitive = false;
        await expect(startup.host.services.appLifecycle.onFirstInitialise()).resolves.toBe(true);

        expect(startup.queued()).toEqual([]);
        expect(startup.host.serviceModules.storageAccess.getFiles).toHaveBeenCalledTimes(1);
    });

    it("queues a size change which kept the modification time", async () => {
        const startup = createStartup([{ path: "stable.md", mtime: 10_000, size: 5 }], [note("stable.md", 10_000, 5)]);
        startup.afterListing(() => startup.vault.write("stable.md", 10_000, 9));

        await expect(startup.start()).resolves.toBe(true);

        expect(startup.queued()).toEqual([{ type: "CHANGED", path: "stable.md", revalidate: true }]);
    });

    it("compares only the path the scan chose when paths differ only in case", async () => {
        const startup = createStartup(
            [
                { path: "Note.md", mtime: 10_000, size: 5 },
                { path: "note.md", mtime: 20_000, size: 6 },
            ],
            [note("note.md", 20_000, 6)]
        );
        startup.settings.handleFilenameCaseSensitive = false;

        await expect(startup.start()).resolves.toBe(true);

        expect(startup.queued()).toEqual([]);
    });

    it("does not keep the listing of a scan after start-up", async () => {
        const startup = createStartup([{ path: "stable.md", mtime: 10_000, size: 5 }], [note("stable.md", 10_000, 5)]);
        await expect(startup.start()).resolves.toBe(true);
        const log = createInstanceLogFunction("test", startup.host.services.API as never);

        startup.afterListing(() => startup.vault.write("later.md", 70_000, 8));
        await synchroniseAllFilesBetweenDBandStorage(startup.host as never, log, {} as never, {
            mode: FullScanModes.DB_APPLY,
        });
        await expect(queueStorageChangesSinceLastScan(startup.host as never, log)).resolves.toBe(true);

        expect(startup.queued()).toEqual([{ type: "CREATE", path: "later.md", revalidate: false }]);
    });

    it("queues a file the scan wrote for revalidation when the host does not keep its modification time", async () => {
        const startup = createStartup([], [note("remote.md", 30_000, 7)], { keepsWrittenMTime: false });

        await expect(startup.start()).resolves.toBe(true);

        // The file handler finds the reflected revision in storage and does not store it again.
        expect(startup.queued()).toEqual([{ type: "CREATE", path: "remote.md", revalidate: true }]);
    });
});
