import { describe, expect, it, vi } from "vitest";
import { BASE_IS_NEW, EVEN, TARGET_IS_NEW } from "@lib/common/models/shared.const.symbols";
import type {
    FileEventItem,
    FilePath,
    FilePathWithPrefix,
    MetaEntry,
    UXFileInfo,
    UXFileInfoStub,
} from "@lib/common/types";
import { createTextBlob } from "@lib/common/utils";
import { ServiceFileHandlerBase, type ServiceFileHandlerDependencies } from "./ServiceFileHandlerBase";
import { BinaryContentSizeMismatchError } from "@lib/interfaces/DatabaseFileAccess";
import { createLiveSyncEventHub } from "@lib/hub/hub";
import { serialized } from "octagonal-wheels/concurrency/lock";
import type { BinaryPublication } from "@lib/interfaces/StorageAccess";
import type { FileReflectionProvenanceRecord } from "@lib/interfaces/FileReflectionProvenance";
import { UnknownFileWriteStateError } from "./FilePublicationCoordinator";
import { EVENT_PLUGIN_UNLOADED } from "@lib/events/coreEvents";
import { LOG_LEVEL_NOTICE } from "octagonal-wheels/common/logger";

class TestFileHandler extends ServiceFileHandlerBase {}

function byteLength(text: string) {
    return new Blob([text]).size;
}

function createMeta(path: string, body: string, rev = "2-remote"): MetaEntry {
    return {
        _id: "doc-id",
        _rev: rev,
        path,
        ctime: 1,
        mtime: 2,
        size: byteLength(body),
        children: [],
        datatype: "plain",
        type: "plain",
        eden: {},
    } as unknown as MetaEntry;
}

function createStorageFile(path: string, body: string): UXFileInfo {
    return {
        name: path.split("/").pop() || path,
        path,
        stat: {
            ctime: 1,
            mtime: 3,
            size: byteLength(body),
            type: "file",
        },
        body: createTextBlob(body),
    } as UXFileInfo;
}

function createStorageStub(path: string, body: string): UXFileInfoStub {
    const file = createStorageFile(path, body);
    delete (file as Partial<UXFileInfo>).body;
    return file;
}

function createHandler(
    localBody: string,
    remoteBody: string,
    localContentIsKnown: boolean,
    freshness: typeof BASE_IS_NEW | typeof TARGET_IS_NEW | typeof EVEN = TARGET_IS_NEW,
    trackProvenance: boolean = false
) {
    const path = "note.md";
    const remoteMeta = createMeta(path, remoteBody);
    const remoteEntry = {
        ...remoteMeta,
        data: remoteBody,
    };
    const storageFile = createStorageFile(path, localBody);
    const storageStub = { ...storageFile };
    delete (storageStub as Partial<UXFileInfo>).body;

    const databaseFileAccess = {
        fetchEntryMeta: vi.fn().mockResolvedValue(remoteMeta),
        getConflictedRevs: vi.fn().mockResolvedValue([]),
        fetchEntryFromMeta: vi.fn().mockResolvedValue(remoteEntry),
        hasContentInRevisionHistory: vi.fn().mockResolvedValue(localContentIsKnown),
        storeAsConflictedRevision: vi.fn().mockResolvedValue(true),
        storeAsConflictedRevisionWithResult: vi.fn().mockResolvedValue("3-local-preserved"),
    };
    // Paths the harness has actually removed, so the adapter-level existence read answers like a real one.
    const removedFromStorage = new Set<string>();
    const storageAccess = {
        normalisePath: (path: string) => path,
        // Existence is confirmed against the adapter, not the editor's index, so the harness answers those
        // reads too. A removal makes the file absent, exactly as the adapter would report it.
        isExistsIncludeHidden: vi.fn(async (path: string) => !removedFromStorage.has(path)),
        statHidden: vi.fn().mockResolvedValue(storageFile.stat),
        removeHidden: vi.fn(async (path: string) => {
            removedFromStorage.add(path);
            return true;
        }),
        getFileStub: vi.fn().mockResolvedValue(storageStub),
        getStub: vi.fn().mockResolvedValue(storageStub),
        readStubContent: vi.fn().mockResolvedValue(storageFile),
        ensureDir: vi.fn().mockResolvedValue(undefined),
        writeFileAuto: vi.fn().mockResolvedValue(true),
        stat: vi.fn().mockResolvedValue(storageFile.stat),
        touched: vi.fn().mockResolvedValue(undefined),
        triggerFileEvent: vi.fn(),
        renameFile: vi.fn(),
    };
    const conflict = {
        queueCheckFor: vi.fn().mockResolvedValue(undefined),
        queueCheckForIfOpen: vi.fn().mockResolvedValue(undefined),
    };
    const pathService = {
        getPath: vi.fn().mockImplementation((entry: MetaEntry) => entry.path),
        path2id: vi.fn().mockImplementation(async (path: string) => path.toLowerCase()),
        compareFileFreshness: vi.fn().mockReturnValue(freshness),
        markChangesAreSame: vi.fn(),
    };
    // The store reads back what it wrote, like the real one, so a test exercises the same sequence of records
    // as a device does. A test which needs a specific record still overrides `get`.
    const records = new Map<string, FileReflectionProvenanceRecord>();
    const setting = { currentSettings: vi.fn().mockReturnValue({ writeDocumentsIfConflicted: false }) };
    const provenance = {
        get: vi.fn(async (path: string) => records.get(path)),
        set: vi.fn(async (path: string, record: FileReflectionProvenanceRecord) => {
            records.set(path, record);
        }),
        delete: vi.fn(async (path: string) => {
            records.delete(path);
        }),
        move: vi.fn(async (from: string, to: string) => {
            const record = records.get(from);
            if (record) records.set(to, record);
            records.delete(from);
        }),
    };
    const deps = {
        events: createLiveSyncEventHub(),
        API: { addLog: vi.fn() },
        databaseFileAccess,
        storageAccess,
        fileProcessing: { processFileEvent: { addHandler: vi.fn() } },
        replication: { processSynchroniseResult: { addHandler: vi.fn() } },
        conflict,
        path: pathService,
        setting,
        vault: {},
        fileReflectionProvenance: trackProvenance ? provenance : undefined,
    } as unknown as ServiceFileHandlerDependencies;

    return {
        handler: new TestFileHandler(deps),
        remoteMeta,
        storageStub: storageStub as UXFileInfoStub,
        databaseFileAccess,
        storageAccess,
        conflict,
        pathService,
        provenance,
        setting,
        removedFromStorage,
        deps,
        records,
    };
}

function createRenameHandler(caseInsensitive: boolean, oldEntry: MetaEntry | false = createMeta("old.md", "body")) {
    let processFileEvent: ((item: FileEventItem) => Promise<boolean>) | undefined;
    const databaseFileAccess = {
        fetchEntryMeta: vi.fn().mockImplementation(async (path: UXFileInfoStub | FilePathWithPrefix) => {
            const filePath = typeof path === "string" ? path : path.path;
            return filePath === "new.md" ? false : oldEntry;
        }),
        getConflictedRevs: vi.fn().mockResolvedValue([]),
        fetchEntry: vi.fn().mockResolvedValue(oldEntry),
        delete: vi.fn().mockResolvedValue(true),
        storeWithBaseRevision: vi.fn().mockResolvedValue("4-renamed"),
    };
    const pathService = {
        path2id: vi.fn().mockImplementation(async (path: string) => (caseInsensitive ? path.toLowerCase() : path)),
    };
    const deps = {
        events: createLiveSyncEventHub(),
        API: { addLog: vi.fn() },
        databaseFileAccess,
        storageAccess: {},
        fileProcessing: {
            processFileEvent: {
                addHandler: vi.fn((handler: (item: FileEventItem) => Promise<boolean>) => {
                    processFileEvent = handler;
                }),
            },
        },
        replication: { processSynchroniseResult: { addHandler: vi.fn() } },
        conflict: {},
        path: pathService,
        setting: { currentSettings: vi.fn().mockReturnValue({}) },
        vault: { isTargetFile: vi.fn().mockResolvedValue(true) },
    } as unknown as ServiceFileHandlerDependencies;
    const handler = new TestFileHandler(deps);
    if (!processFileEvent) throw new Error("File event handler was not registered");
    return { handler, processFileEvent, databaseFileAccess, pathService };
}

function createRestoredEvent(type: FileEventItem["type"], file: UXFileInfoStub, oldPath?: string): FileEventItem {
    return {
        type,
        key: `${type}-${file.path}`,
        args: { file, oldPath },
        restoredFromPreviousRuntime: true,
    };
}

function createRestoredEventHandler(
    options: {
        currentItems?: Record<string, UXFileInfoStub | { path: FilePath; isFolder: true } | null>;
        caseInsensitiveIds?: boolean;
        isTargetFile?: (path: string) => boolean;
        isFileSizeTooLarge?: (size: number) => boolean;
        /** Provenance records by path; the handler has no provenance store when omitted. */
        records?: Record<string, FileReflectionProvenanceRecord>;
        /** Content of each database revision of the file. */
        revisions?: Record<string, string>;
        /** The current winning revision; the last of `revisions` when omitted. */
        winner?: string;
        winnerDeleted?: boolean;
        /** Content in storage by path. */
        storageBodies?: Record<string, string>;
    } = {}
) {
    let processFileEvent: ((item: FileEventItem) => Promise<boolean>) | undefined;
    const currentItems = options.currentItems ?? {};
    const revisions = options.revisions ?? {};
    const winner = options.winner ?? Object.keys(revisions).at(-1);
    const storageAccess = {
        normalisePath: vi.fn((path: string) => path.replaceAll("\\", "/")),
        getStub: vi.fn(async (path: string) => currentItems[path] ?? null),
        stat: vi.fn(async (path: string) => {
            const item = currentItems[path];
            return item && "stat" in item ? item.stat : null;
        }),
        readStubContent: vi.fn(async (file: UXFileInfoStub) =>
            createStorageFile(file.path, options.storageBodies?.[file.path] ?? "")
        ),
        statHidden: vi.fn().mockResolvedValue(null),
    };
    const metaOf = (path: string, rev: string | undefined) => {
        const revision = rev ?? winner;
        if (revision === undefined || !(revision in revisions)) return false;
        const meta = createMeta(path, revisions[revision], revision);
        return rev === undefined && options.winnerDeleted ? { ...meta, deleted: true } : meta;
    };
    const databaseFileAccess = {
        fetchEntryMeta: vi.fn(async (file: UXFileInfoStub | string, rev?: string) =>
            metaOf(typeof file === "string" ? file : file.path, rev)
        ),
        fetchEntry: vi.fn(async (file: UXFileInfoStub, rev?: string) => {
            const meta = metaOf(file.path, rev);
            return meta && { ...meta, data: revisions[meta._rev] };
        }),
        storeDeletionWithBaseRevision: vi.fn().mockResolvedValue("4-deleted"),
    };
    const conflict = { queueCheckFor: vi.fn().mockResolvedValue(undefined) };
    const records = options.records;
    const provenance = records && {
        get: vi.fn(async (path: string) => records[path]),
        set: vi.fn(),
        delete: vi.fn(),
        move: vi.fn(),
    };
    const pathService = {
        path2id: vi.fn(async (path: string) => (options.caseInsensitiveIds ? path.toLowerCase() : path)),
    };
    const vault = {
        isTargetFile: vi.fn(async (path: string) => options.isTargetFile?.(path) ?? true),
        isFileSizeTooLarge: vi.fn((size: number) => options.isFileSizeTooLarge?.(size) ?? false),
    };
    const dependencies = {
        events: createLiveSyncEventHub(),
        API: { addLog: vi.fn() },
        databaseFileAccess,
        storageAccess,
        fileProcessing: {
            processFileEvent: {
                addHandler: vi.fn((handler: (item: FileEventItem) => Promise<boolean>) => {
                    processFileEvent = handler;
                }),
            },
        },
        replication: { processSynchroniseResult: { addHandler: vi.fn() } },
        conflict,
        path: pathService,
        setting: { currentSettings: vi.fn().mockReturnValue({}) },
        vault,
        fileReflectionProvenance: provenance,
    } as unknown as ServiceFileHandlerDependencies;
    const handler = new TestFileHandler(dependencies);
    if (!processFileEvent) throw new Error("File event handler was not registered");
    const storeFileToDB = vi.spyOn(handler, "storeFileToDB").mockResolvedValue(true);
    const deleteFileFromDB = vi.spyOn(handler, "deleteFileFromDB").mockResolvedValue(true);
    const renameFileInDB = vi.spyOn(handler, "renameFileInDB").mockResolvedValue(true);
    return {
        handler,
        processFileEvent,
        storageAccess,
        databaseFileAccess,
        conflict,
        provenance,
        vault,
        storeFileToDB,
        deleteFileFromDB,
        renameFileInDB,
    };
}

function createConflictedOperationHandler() {
    const displayedRevision = "3-displayed";
    const winner = {
        ...createMeta("note.md", "winner", "3-winner"),
        data: "winner",
    };
    const storageFile = createStorageFile("note.md", "edited displayed content");
    const databaseFileAccess = {
        fetchEntry: vi.fn().mockImplementation(async (file: UXFileInfoStub | FilePathWithPrefix) => {
            const path = typeof file === "string" ? file : file.path;
            return path === "new.md" ? false : winner;
        }),
        fetchEntryMeta: vi.fn().mockImplementation(async (file: UXFileInfoStub | FilePathWithPrefix) => {
            const path = typeof file === "string" ? file : file.path;
            return path === "new.md" ? false : winner;
        }),
        getConflictedRevs: vi.fn().mockImplementation(async (file: UXFileInfoStub | FilePathWithPrefix) => {
            const path = typeof file === "string" ? file : file.path;
            return path === "new.md" ? [] : [displayedRevision];
        }),
        store: vi.fn().mockResolvedValue(true),
        delete: vi.fn().mockResolvedValue(true),
        storeWithBaseRevision: vi.fn().mockResolvedValue("4-local-edit"),
        storeAsConflictedRevisionWithResult: vi.fn().mockResolvedValue("4-unknown-edit"),
        storeDeletionWithBaseRevision: vi.fn().mockResolvedValue("4-local-delete"),
        findContentRevisions: vi.fn().mockResolvedValue([]),
    };
    const provenance = {
        get: vi
            .fn()
            .mockImplementation(async (path: FilePathWithPrefix) =>
                path === "note.md" || path === "old.md"
                    ? { revision: displayedRevision, observedStorageMtime: 2 }
                    : undefined
            ),
        set: vi.fn().mockResolvedValue(undefined),
        delete: vi.fn().mockResolvedValue(undefined),
        move: vi.fn().mockResolvedValue(undefined),
    };
    const storageAccess = {
        normalisePath: (path: string) => path,
        statHidden: vi.fn(async () => storageFile.stat),
        isExistsIncludeHidden: vi.fn(async () => true),
        getFileStub: vi.fn().mockResolvedValue(storageFile),
        readStubContent: vi
            .fn()
            .mockImplementation(async (file: UXFileInfoStub) => ({ ...storageFile, path: file.path })),
        stat: vi.fn().mockImplementation(async () => storageFile.stat),
    };
    const conflict = {
        queueCheckFor: vi.fn().mockResolvedValue(undefined),
        queueCheckForIfOpen: vi.fn().mockResolvedValue(undefined),
    };
    const pathService = {
        path2id: vi.fn().mockImplementation(async (path: string) => path.toLowerCase()),
        compareFileFreshness: vi.fn().mockReturnValue(TARGET_IS_NEW),
        markChangesAreSame: vi.fn(),
    };
    const deps = {
        events: createLiveSyncEventHub(),
        API: { addLog: vi.fn() },
        databaseFileAccess,
        storageAccess,
        fileProcessing: { processFileEvent: { addHandler: vi.fn() } },
        replication: { processSynchroniseResult: { addHandler: vi.fn() } },
        conflict,
        path: pathService,
        setting: { currentSettings: vi.fn().mockReturnValue({}) },
        vault: {},
        fileReflectionProvenance: provenance,
    } as unknown as ServiceFileHandlerDependencies;
    return {
        handler: new TestFileHandler(deps),
        databaseFileAccess,
        provenance,
        conflict,
        storageFile,
        displayedRevision,
    };
}

describe("ServiceFileHandlerBase.renameFileInDB", () => {
    it("updates one document without deleting it for a case-only rename", async () => {
        const { handler, databaseFileAccess, pathService } = createRenameHandler(true);
        const deleteSpy = vi.spyOn(handler, "deleteFileFromDB").mockResolvedValue(true);
        const file = createStorageFile("calculus.md", "body");

        await expect(handler.renameFileInDB(file, "Calculus.md" as FilePath)).resolves.toBe(true);

        expect(pathService.path2id).toHaveBeenNthCalledWith(1, "Calculus.md");
        expect(pathService.path2id).toHaveBeenNthCalledWith(2, "calculus.md");
        expect(databaseFileAccess.storeWithBaseRevision).toHaveBeenCalledWith(file, "2-remote", true);
        expect(deleteSpy).not.toHaveBeenCalled();
    });

    it("stores the target before deleting the source for an ordinary rename", async () => {
        const { handler, databaseFileAccess } = createRenameHandler(false);
        const storeSpy = vi.spyOn(handler, "storeFileToDB").mockResolvedValue(true);
        const file = createStorageFile("new.md", "body");

        await expect(handler.renameFileInDB(file, "old.md" as FilePath)).resolves.toBe(true);

        expect(databaseFileAccess.fetchEntryMeta).toHaveBeenCalledWith("old.md", undefined, true);
        expect(storeSpy.mock.invocationCallOrder[0]).toBeLessThan(
            databaseFileAccess.delete.mock.invocationCallOrder[0]
        );
        expect(databaseFileAccess.delete).toHaveBeenCalledWith("old.md");
    });

    it("preserves the source when storing the rename target fails", async () => {
        const { handler } = createRenameHandler(false);
        vi.spyOn(handler, "storeFileToDB").mockResolvedValue(false);
        const deleteSpy = vi.spyOn(handler, "deleteFileFromDB").mockResolvedValue(true);
        const file = createStorageFile("new.md", "body");

        await expect(handler.renameFileInDB(file, "old.md" as FilePath)).resolves.toBe(false);

        expect(deleteSpy).not.toHaveBeenCalled();
    });

    it("does not fail when the rename source is already absent", async () => {
        const { handler } = createRenameHandler(false, false);
        vi.spyOn(handler, "storeFileToDB").mockResolvedValue(true);
        const deleteSpy = vi.spyOn(handler, "deleteFileFromDB").mockResolvedValue(true);
        const file = createStorageFile("new.md", "body");

        await expect(handler.renameFileInDB(file, "old.md" as FilePath)).resolves.toBe(true);

        expect(deleteSpy).not.toHaveBeenCalled();
    });

    it("dispatches a rename event to the atomic rename handler", async () => {
        const { handler, processFileEvent } = createRenameHandler(true);
        const renameSpy = vi.spyOn(handler, "renameFileInDB").mockResolvedValue(true);
        const file = createStorageFile("calculus.md", "body");
        const event: FileEventItem = {
            type: "RENAME",
            args: { file, oldPath: "Calculus.md" },
            key: "rename",
        };

        await expect(processFileEvent(event)).resolves.toBe(true);

        expect(renameSpy).toHaveBeenCalledWith(file, "Calculus.md");
    });

    it("serialises case variants by their canonical document ID", async () => {
        const { handler, processFileEvent } = createRenameHandler(true);
        let notifyDeleteStarted: (() => void) | undefined;
        let releaseDelete: (() => void) | undefined;
        const deleteStarted = new Promise<void>((resolve) => {
            notifyDeleteStarted = resolve;
        });
        const deleteGate = new Promise<void>((resolve) => {
            releaseDelete = resolve;
        });
        vi.spyOn(handler, "deleteFileFromDB").mockImplementation(async () => {
            notifyDeleteStarted?.();
            await deleteGate;
            return true;
        });
        const storeSpy = vi.spyOn(handler, "storeFileToDB").mockResolvedValue(true);
        const oldFile = createStorageFile("Calculus.md", "body");
        const newFile = createStorageFile("calculus.md", "body");

        const deletePromise = processFileEvent({ type: "DELETE", args: { file: oldFile }, key: "delete" });
        await deleteStarted;
        const createPromise = processFileEvent({ type: "CREATE", args: { file: newFile }, key: "create" });
        await new Promise((resolve) => setTimeout(resolve, 10));

        expect(storeSpy).not.toHaveBeenCalled();
        releaseDelete?.();
        await Promise.all([deletePromise, createPromise]);
        expect(storeSpy).toHaveBeenCalledTimes(1);
    });
});

describe("ServiceFileHandlerBase restored storage events", () => {
    it.each(["CREATE", "CHANGED"] as const)("uses the current storage stub for a restored %s event", async (type) => {
        const saved = createStorageStub("note.md", "saved");
        const current = createStorageStub("note.md", "current");
        const { processFileEvent, storeFileToDB } = createRestoredEventHandler({
            currentItems: { "note.md": current },
        });

        await expect(processFileEvent(createRestoredEvent(type, saved))).resolves.toBe(true);

        expect(storeFileToDB).toHaveBeenCalledWith(current);
    });

    it.each(["CREATE", "CHANGED"] as const)(
        "does not store a revalidated %s event when storage holds the revision this device recorded",
        async (type) => {
            // A newer revision is already current in the database; storing the recorded content would replace it.
            const current = createStorageStub("note.md", "reflected");
            const { processFileEvent, storeFileToDB, provenance } = createRestoredEventHandler({
                currentItems: { "note.md": current },
                records: { "note.md": { revision: "2-reflected", reflectedFromDatabase: true } },
                revisions: { "2-reflected": "reflected", "3-newer": "newer content" },
                storageBodies: { "note.md": "reflected" },
            });

            await expect(processFileEvent(createRestoredEvent(type, current))).resolves.toBe(true);

            expect(storeFileToDB).not.toHaveBeenCalled();
            expect(provenance!.delete).not.toHaveBeenCalled();
        }
    );

    it.each([
        ["a different size", "edited in the window"],
        ["the same size", "reflectes"],
    ])("stores a revalidated event when storage differs from the recorded revision with %s", async (_, body) => {
        const current = createStorageStub("note.md", body);
        const { processFileEvent, storeFileToDB } = createRestoredEventHandler({
            currentItems: { "note.md": current },
            records: { "note.md": { revision: "2-stored" } },
            revisions: { "2-stored": "reflected" },
            storageBodies: { "note.md": body },
        });

        await expect(processFileEvent(createRestoredEvent("CHANGED", current))).resolves.toBe(true);

        expect(storeFileToDB).toHaveBeenCalledWith(current);
    });

    it.each([
        ["no record exists", {}, { "2-reflected": "reflected" }],
        ["the record cannot be read", undefined, { "2-reflected": "reflected" }],
        [
            "the file is being published",
            { "note.md": { revision: "2-reflected", pendingPublication: { revision: "3-next", token: "t" } } },
            { "2-reflected": "reflected" },
        ],
    ] as const)("stores a revalidated event as before when %s", async (_, records, revisions) => {
        const current = createStorageStub("note.md", "reflected");
        const { processFileEvent, provenance, storeFileToDB } = createRestoredEventHandler({
            currentItems: { "note.md": current },
            records: records ?? {},
            revisions,
            storageBodies: { "note.md": "reflected" },
        });
        if (records === undefined) provenance!.get.mockRejectedValueOnce(new Error("store unavailable"));

        await expect(processFileEvent(createRestoredEvent("CHANGED", current))).resolves.toBe(true);

        expect(storeFileToDB).toHaveBeenCalledWith(current);
    });

    it("discards a record whose revision is gone before storing the revalidated file", async () => {
        const current = createStorageStub("note.md", "reflected");
        const { processFileEvent, provenance, storeFileToDB } = createRestoredEventHandler({
            currentItems: { "note.md": current },
            records: { "note.md": { revision: "2-reflected" } },
            revisions: { "3-other": "reflected" },
            storageBodies: { "note.md": "reflected" },
        });

        await expect(processFileEvent(createRestoredEvent("CHANGED", current))).resolves.toBe(true);

        expect(provenance!.delete).toHaveBeenCalledWith("note.md");
        expect(storeFileToDB).toHaveBeenCalledWith(current);
    });

    it("stores a file restored with the recorded content after the database deleted it", async () => {
        const current = createStorageStub("note.md", "reflected");
        const { processFileEvent, storeFileToDB } = createRestoredEventHandler({
            currentItems: { "note.md": current },
            records: { "note.md": { revision: "2-reflected", reflectedFromDatabase: true } },
            revisions: { "2-reflected": "reflected", "3-deleted": "" },
            winnerDeleted: true,
            storageBodies: { "note.md": "reflected" },
        });

        await expect(processFileEvent(createRestoredEvent("CREATE", current))).resolves.toBe(true);

        expect(storeFileToDB).toHaveBeenCalledWith(current);
    });

    it.each([
        ["recognises", 7, false],
        ["does not read", 6, true],
    ] as const)(
        "%s a large file by its recorded modification time without loading its content",
        async (_, observedStorageMtime, stored) => {
            const size = 2 * 1024 * 1024;
            const current = { ...createStorageStub("video.mp4", ""), stat: { ctime: 1, mtime: 7, size, type: "file" } };
            const { processFileEvent, storeFileToDB, storageAccess, databaseFileAccess } = createRestoredEventHandler({
                currentItems: { "video.mp4": current as UXFileInfoStub },
                records: { "video.mp4": { revision: "2-large", observedStorageMtime } },
                revisions: { "2-large": "", "3-newer": "" },
            });
            databaseFileAccess.fetchEntryMeta.mockImplementation(async (_file: unknown, rev?: string) => ({
                ...createMeta("video.mp4", "", rev ?? "3-newer"),
                size,
            }));
            const fetchBinaryContentFromMeta = vi.fn();
            Object.assign(databaseFileAccess, { fetchBinaryContentFromMeta });

            await expect(processFileEvent(createRestoredEvent("CHANGED", current as UXFileInfoStub))).resolves.toBe(
                true
            );

            expect(storeFileToDB).toHaveBeenCalledTimes(stored ? 1 : 0);
            expect(fetchBinaryContentFromMeta).not.toHaveBeenCalled();
            expect(databaseFileAccess.fetchEntry).not.toHaveBeenCalled();
            expect(storageAccess.readStubContent).not.toHaveBeenCalled();
        }
    );

    it.each([
        ["does not store", { status: "ok" as const }, false],
        ["stores", { status: "unsupported" as const }, true],
    ])("%s a binary file when its recorded content loads with status %o", async (_, binary, stored) => {
        const bytes = new Uint8Array([1, 2, 3, 4]);
        const current = { ...createStorageStub("image.png", ""), stat: { ctime: 1, mtime: 3, size: 4, type: "file" } };
        const { processFileEvent, storeFileToDB, storageAccess, databaseFileAccess } = createRestoredEventHandler({
            currentItems: { "image.png": current as UXFileInfoStub },
            records: { "image.png": { revision: "2-image" } },
            revisions: { "2-image": "AQIDBA==" },
        });
        databaseFileAccess.fetchEntryMeta.mockImplementation(async (_file: unknown, rev?: string) => ({
            ...createMeta("image.png", "", rev ?? "2-image"),
            type: "newnote",
            datatype: "newnote",
            size: 4,
        }));
        Object.assign(databaseFileAccess, {
            fetchBinaryContentFromMeta: vi
                .fn()
                .mockResolvedValue(binary.status === "ok" ? { status: "ok", data: bytes.buffer } : binary),
        });
        storageAccess.readStubContent.mockResolvedValue({
            ...(current as UXFileInfoStub),
            body: new Blob([bytes]),
        } as UXFileInfo);

        await expect(processFileEvent(createRestoredEvent("CHANGED", current as UXFileInfoStub))).resolves.toBe(true);

        expect(storeFileToDB).toHaveBeenCalledTimes(stored ? 1 : 0);
    });

    it("deletes on the displayed revision when a newer revision became current after storage last showed the file", async () => {
        const saved = createStorageStub("note.md", "reflected");
        const { processFileEvent, deleteFileFromDB, databaseFileAccess, conflict, provenance } =
            createRestoredEventHandler({
                records: { "note.md": { revision: "2-reflected", reflectedFromDatabase: true } },
                revisions: { "2-reflected": "reflected", "3-newer": "newer content" },
            });

        await expect(processFileEvent(createRestoredEvent("DELETE", saved))).resolves.toBe(true);

        expect(databaseFileAccess.storeDeletionWithBaseRevision).toHaveBeenCalledWith("note.md", "2-reflected");
        expect(provenance!.delete).toHaveBeenCalledWith("note.md");
        expect(conflict.queueCheckFor).toHaveBeenCalledWith("note.md");
        expect(deleteFileFromDB).not.toHaveBeenCalled();
    });

    it("deletes by path when storage last showed the current revision", async () => {
        const saved = createStorageStub("note.md", "reflected");
        const { processFileEvent, deleteFileFromDB, databaseFileAccess } = createRestoredEventHandler({
            records: { "note.md": { revision: "2-reflected", reflectedFromDatabase: true } },
            revisions: { "2-reflected": "reflected" },
        });

        await expect(processFileEvent(createRestoredEvent("DELETE", saved))).resolves.toBe(true);

        expect(deleteFileFromDB).toHaveBeenCalledWith("note.md");
        expect(databaseFileAccess.storeDeletionWithBaseRevision).not.toHaveBeenCalled();
    });

    it.each([
        ["deletes by path when the database already deleted the file", {}, true, 3, true],
        ["deletes by path without a record when the file as last seen is not older", {}, false, 3, true],
        [
            "leaves a deletion without a record to the next scan when a newer revision is current",
            {},
            false,
            60_000,
            false,
        ],
    ] as const)("%s", async (_, records, winnerDeleted, winnerMTime, deleted) => {
        const saved = createStorageStub("note.md", "reflected");
        const { processFileEvent, deleteFileFromDB, databaseFileAccess } = createRestoredEventHandler({
            records,
            revisions: { "3-current": "current" },
            winnerDeleted,
        });
        const metaOf = databaseFileAccess.fetchEntryMeta.getMockImplementation()!;
        databaseFileAccess.fetchEntryMeta.mockImplementation(async (file: UXFileInfoStub | string, rev?: string) => {
            const meta = await metaOf(file, rev);
            return meta && { ...meta, mtime: winnerMTime };
        });

        await expect(processFileEvent(createRestoredEvent("DELETE", saved))).resolves.toBe(true);

        expect(deleteFileFromDB).toHaveBeenCalledTimes(deleted ? 1 : 0);
        expect(databaseFileAccess.storeDeletionWithBaseRevision).not.toHaveBeenCalled();
    });

    it("omits a restored inclusion when its exact path no longer contains that file", async () => {
        const saved = createStorageStub("Note.md", "saved");
        const current = createStorageStub("note.md", "current");
        const { processFileEvent, storeFileToDB, deleteFileFromDB, renameFileInDB } = createRestoredEventHandler({
            currentItems: { "Note.md": current },
        });

        await expect(processFileEvent(createRestoredEvent("CHANGED", saved))).resolves.toBe(true);

        expect(storeFileToDB).not.toHaveBeenCalled();
        expect(deleteFileFromDB).not.toHaveBeenCalled();
        expect(renameFileInDB).not.toHaveBeenCalled();
    });

    it("suppresses a restored deletion when the path is occupied now", async () => {
        const saved = createStorageStub("note.md", "saved");
        const current = createStorageStub("note.md", "current");
        const { processFileEvent, deleteFileFromDB } = createRestoredEventHandler({
            currentItems: { "note.md": current },
        });

        await expect(processFileEvent(createRestoredEvent("DELETE", saved))).resolves.toBe(true);

        expect(deleteFileFromDB).not.toHaveBeenCalled();
    });

    it("applies a restored deletion by path only after confirming current absence", async () => {
        const saved = createStorageStub("note.md", "saved");
        const { processFileEvent, deleteFileFromDB } = createRestoredEventHandler();

        await expect(processFileEvent(createRestoredEvent("DELETE", saved))).resolves.toBe(true);

        expect(deleteFileFromDB).toHaveBeenCalledWith("note.md");
    });

    it("suppresses a restored deletion when current storage inspection fails", async () => {
        const saved = createStorageStub("note.md", "saved");
        const { processFileEvent, storageAccess, deleteFileFromDB } = createRestoredEventHandler();
        storageAccess.getStub.mockRejectedValueOnce(new Error("storage unavailable"));

        await expect(processFileEvent(createRestoredEvent("DELETE", saved))).resolves.toBe(true);

        expect(deleteFileFromDB).not.toHaveBeenCalled();
    });

    it.each(["CHANGED", "DELETE"] as const)(
        "does not apply a restored %s operation after the path is deselected",
        async (type) => {
            const saved = createStorageStub("note.md", "saved");
            const currentItems = type === "CHANGED" ? { "note.md": createStorageStub("note.md", "current") } : {};
            const { processFileEvent, storeFileToDB, deleteFileFromDB } = createRestoredEventHandler({
                currentItems,
                isTargetFile: () => false,
            });

            await expect(processFileEvent(createRestoredEvent(type, saved))).resolves.toBe(true);

            expect(storeFileToDB).not.toHaveBeenCalled();
            expect(deleteFileFromDB).not.toHaveBeenCalled();
        }
    );

    it("uses the current target for a restored cross-document rename", async () => {
        const saved = createStorageStub("new.md", "saved");
        const current = createStorageStub("new.md", "current");
        const { processFileEvent, renameFileInDB } = createRestoredEventHandler({
            currentItems: { "new.md": current, "old.md": null },
        });

        await expect(processFileEvent(createRestoredEvent("RENAME", saved, "old.md"))).resolves.toBe(true);

        expect(renameFileInDB).toHaveBeenCalledWith(current, "old.md");
    });

    it("includes the current rename target without deleting a source which still exists", async () => {
        const saved = createStorageStub("new.md", "saved");
        const currentNew = createStorageStub("new.md", "current new");
        const currentOld = createStorageStub("old.md", "current old");
        const { processFileEvent, storeFileToDB, deleteFileFromDB, renameFileInDB } = createRestoredEventHandler({
            currentItems: { "new.md": currentNew, "old.md": currentOld },
        });

        await expect(processFileEvent(createRestoredEvent("RENAME", saved, "old.md"))).resolves.toBe(true);

        expect(storeFileToDB).toHaveBeenCalledWith(currentNew);
        expect(deleteFileFromDB).not.toHaveBeenCalled();
        expect(renameFileInDB).not.toHaveBeenCalled();
    });

    it("deletes an absent rename source when the target is also absent", async () => {
        const saved = createStorageStub("new.md", "saved");
        const { processFileEvent, storeFileToDB, deleteFileFromDB, renameFileInDB } = createRestoredEventHandler();

        await expect(processFileEvent(createRestoredEvent("RENAME", saved, "old.md"))).resolves.toBe(true);

        expect(deleteFileFromDB).toHaveBeenCalledWith("old.md");
        expect(storeFileToDB).not.toHaveBeenCalled();
        expect(renameFileInDB).not.toHaveBeenCalled();
    });

    it("preserves the rename source when a current target cannot be included", async () => {
        const saved = createStorageStub("new.md", "saved");
        const current = createStorageStub("new.md", "current");
        const { processFileEvent, storeFileToDB, deleteFileFromDB, renameFileInDB } = createRestoredEventHandler({
            currentItems: { "new.md": current, "old.md": null },
            isFileSizeTooLarge: () => true,
        });

        await expect(processFileEvent(createRestoredEvent("RENAME", saved, "old.md"))).resolves.toBe(true);

        expect(storeFileToDB).not.toHaveBeenCalled();
        expect(deleteFileFromDB).not.toHaveBeenCalled();
        expect(renameFileInDB).not.toHaveBeenCalled();
    });

    it("updates one document from the current target for a restored case-only rename", async () => {
        const saved = createStorageStub("note.md", "saved");
        const current = createStorageStub("note.md", "current");
        const { processFileEvent, renameFileInDB } = createRestoredEventHandler({
            currentItems: { "note.md": current, "Note.md": current },
            caseInsensitiveIds: true,
        });

        await expect(processFileEvent(createRestoredEvent("RENAME", saved, "Note.md"))).resolves.toBe(true);

        expect(renameFileInDB).toHaveBeenCalledWith(current, "Note.md");
    });

    it("does not replay a case-only rename whose current target is absent", async () => {
        const saved = createStorageStub("note.md", "saved");
        const { processFileEvent, storeFileToDB, deleteFileFromDB, renameFileInDB } = createRestoredEventHandler({
            caseInsensitiveIds: true,
        });

        await expect(processFileEvent(createRestoredEvent("RENAME", saved, "Note.md"))).resolves.toBe(true);

        expect(storeFileToDB).not.toHaveBeenCalled();
        expect(deleteFileFromDB).not.toHaveBeenCalled();
        expect(renameFileInDB).not.toHaveBeenCalled();
    });

    it("still includes a current rename target when source inspection fails", async () => {
        const saved = createStorageStub("new.md", "saved");
        const current = createStorageStub("new.md", "current");
        const { processFileEvent, storageAccess, storeFileToDB, deleteFileFromDB, renameFileInDB } =
            createRestoredEventHandler({ currentItems: { "new.md": current } });
        storageAccess.getStub.mockImplementation(async (path: string) => {
            if (path === "old.md") throw new Error("source unavailable");
            return current;
        });

        await expect(processFileEvent(createRestoredEvent("RENAME", saved, "old.md"))).resolves.toBe(true);

        expect(storeFileToDB).toHaveBeenCalledWith(current);
        expect(deleteFileFromDB).not.toHaveBeenCalled();
        expect(renameFileInDB).not.toHaveBeenCalled();
    });

    it("does not delete a rename source when target inspection fails", async () => {
        const saved = createStorageStub("new.md", "saved");
        const { processFileEvent, storageAccess, deleteFileFromDB } = createRestoredEventHandler();
        storageAccess.getStub.mockRejectedValueOnce(new Error("target unavailable"));

        await expect(processFileEvent(createRestoredEvent("RENAME", saved, "old.md"))).resolves.toBe(true);

        expect(deleteFileFromDB).not.toHaveBeenCalled();
    });

    it("reports a failure from an admitted restored operation", async () => {
        const saved = createStorageStub("note.md", "saved");
        const current = createStorageStub("note.md", "current");
        const { processFileEvent, storeFileToDB } = createRestoredEventHandler({
            currentItems: { "note.md": current },
        });
        storeFileToDB.mockRejectedValueOnce(new Error("database unavailable"));

        await expect(processFileEvent(createRestoredEvent("CHANGED", saved))).rejects.toThrow("database unavailable");
    });
});

describe("ServiceFileHandlerBase.dbToStorage", () => {
    it("applies a canonical filename case change before comparing content", async () => {
        const { handler, storageStub, databaseFileAccess, storageAccess, pathService } = createHandler(
            "same body",
            "same body",
            false,
            EVEN
        );
        const remoteMeta = createMeta("calculus.md", "same body");
        const existingFile = {
            ...storageStub,
            name: "Calculus.md",
            path: "Calculus.md" as FilePath,
        };
        const renamedFile = {
            ...existingFile,
            name: "calculus.md",
            path: "calculus.md" as FilePath,
        };
        databaseFileAccess.fetchEntryMeta.mockResolvedValue(remoteMeta);
        databaseFileAccess.fetchEntryFromMeta.mockResolvedValue({ ...remoteMeta, data: "same body" });
        storageAccess.getStub.mockResolvedValue(existingFile);
        storageAccess.renameFile.mockResolvedValue(renamedFile);
        storageAccess.readStubContent.mockResolvedValue(createStorageFile("calculus.md", "same body"));

        await expect(handler.dbToStorage(remoteMeta, existingFile)).resolves.toBe(true);

        expect(pathService.path2id).toHaveBeenCalledWith("Calculus.md");
        expect(pathService.path2id).toHaveBeenCalledWith("calculus.md");
        expect(storageAccess.renameFile).toHaveBeenCalledWith(existingFile, "calculus.md");
        expect(storageAccess.writeFileAuto).not.toHaveBeenCalled();
    });

    it("preserves a file when the canonical path change also changes parent directory case", async () => {
        const { handler, storageStub, databaseFileAccess, storageAccess } = createHandler(
            "same body",
            "same body",
            false,
            EVEN
        );
        const remoteMeta = createMeta("renamed/calculus.md", "same body");
        const existingFile = {
            ...storageStub,
            name: "Calculus.md",
            path: "Renamed/Calculus.md" as FilePath,
        };
        databaseFileAccess.fetchEntryMeta.mockResolvedValue(remoteMeta);
        storageAccess.getStub.mockResolvedValue(existingFile);

        await expect(handler.dbToStorage(remoteMeta, existingFile)).resolves.toBe(false);

        expect(storageAccess.renameFile).not.toHaveBeenCalled();
        expect(databaseFileAccess.fetchEntryFromMeta).not.toHaveBeenCalled();
        expect(storageAccess.writeFileAuto).not.toHaveBeenCalled();
    });

    it("stops remote reflection when the canonical filename case cannot be applied", async () => {
        const { handler, storageStub, databaseFileAccess, storageAccess } = createHandler(
            "same body",
            "same body",
            false,
            EVEN
        );
        const remoteMeta = createMeta("calculus.md", "same body");
        const existingFile = {
            ...storageStub,
            name: "Calculus.md",
            path: "Calculus.md" as FilePath,
        };
        databaseFileAccess.fetchEntryMeta.mockResolvedValue(remoteMeta);
        storageAccess.getStub.mockResolvedValue(existingFile);
        storageAccess.renameFile.mockResolvedValue(null);

        await expect(handler.dbToStorage(remoteMeta, existingFile)).resolves.toBe(false);

        expect(databaseFileAccess.fetchEntryFromMeta).not.toHaveBeenCalled();
        expect(storageAccess.writeFileAuto).not.toHaveBeenCalled();
    });

    it("preserves unknown local storage content as a conflict before applying a remote revision", async () => {
        const { handler, remoteMeta, storageStub, databaseFileAccess, storageAccess, conflict } = createHandler(
            "local unsynced",
            "remote update",
            false,
            BASE_IS_NEW
        );

        await expect(handler.dbToStorage(remoteMeta, storageStub)).resolves.toBe(true);

        expect(databaseFileAccess.storeAsConflictedRevisionWithResult).toHaveBeenCalledWith(
            expect.objectContaining({ path: "note.md" }),
            "2-remote",
            true
        );
        expect(conflict.queueCheckFor).toHaveBeenCalledWith("note.md");
        expect(storageAccess.writeFileAuto).not.toHaveBeenCalled();
    });

    it("records the exact revision created while preserving unknown local storage content", async () => {
        const { handler, remoteMeta, storageStub, databaseFileAccess, provenance } = createHandler(
            "local unsynchronised edit",
            "remote update",
            false,
            BASE_IS_NEW,
            true
        );

        await expect(handler.dbToStorage(remoteMeta, storageStub)).resolves.toBe(true);

        expect(databaseFileAccess.storeAsConflictedRevisionWithResult).toHaveBeenCalledWith(
            expect.objectContaining({ path: "note.md" }),
            "2-remote",
            true
        );
        expect(provenance.set).toHaveBeenCalledWith("note.md", {
            revision: "3-local-preserved",
            observedStorageMtime: storageStub.stat.mtime,
        });
    });

    it("applies a remote addition without conflict when local storage is an unmodified older copy (#994)", async () => {
        const { handler, remoteMeta, storageStub, databaseFileAccess, storageAccess, conflict } = createHandler(
            "existing synced content\n",
            "existing synced content\nnew desktop paragraph\n",
            false,
            TARGET_IS_NEW
        );

        await expect(handler.dbToStorage(remoteMeta, storageStub)).resolves.toBe(true);

        expect(databaseFileAccess.storeAsConflictedRevisionWithResult).not.toHaveBeenCalled();
        expect(conflict.queueCheckFor).not.toHaveBeenCalled();
        expect(storageAccess.writeFileAuto).toHaveBeenCalledWith(
            "note.md",
            "existing synced content\nnew desktop paragraph\n",
            {
                ctime: 1,
                mtime: 2,
            }
        );
    });

    it("preserves unknown local storage content even when the incoming entry is newer", async () => {
        const { handler, remoteMeta, storageStub, databaseFileAccess, storageAccess, conflict } = createHandler(
            "mobile-only local edit\n",
            "desktop-only remote edit\n",
            false,
            TARGET_IS_NEW
        );

        await expect(handler.dbToStorage(remoteMeta, storageStub)).resolves.toBe(true);

        expect(databaseFileAccess.storeAsConflictedRevisionWithResult).toHaveBeenCalledWith(
            expect.objectContaining({ path: "note.md" }),
            "2-remote",
            true
        );
        expect(conflict.queueCheckFor).toHaveBeenCalledWith("note.md");
        expect(storageAccess.writeFileAuto).not.toHaveBeenCalled();
    });

    it("preserves unknown local storage content when freshness is ambiguous", async () => {
        const { handler, remoteMeta, storageStub, databaseFileAccess, storageAccess, conflict } = createHandler(
            "local edit in same timestamp window",
            "remote update in same timestamp window",
            false,
            EVEN
        );

        await expect(handler.dbToStorage(remoteMeta, storageStub)).resolves.toBe(true);

        expect(databaseFileAccess.storeAsConflictedRevisionWithResult).toHaveBeenCalledWith(
            expect.objectContaining({ path: "note.md" }),
            "2-remote",
            true
        );
        expect(conflict.queueCheckFor).toHaveBeenCalledWith("note.md");
        expect(storageAccess.writeFileAuto).not.toHaveBeenCalled();
    });

    it("applies the remote revision when local storage content is already in database history", async () => {
        const { handler, remoteMeta, storageStub, databaseFileAccess, storageAccess, conflict } = createHandler(
            "known old revision",
            "remote update",
            true,
            EVEN
        );

        await expect(handler.dbToStorage(remoteMeta, storageStub)).resolves.toBe(true);

        expect(databaseFileAccess.storeAsConflictedRevisionWithResult).not.toHaveBeenCalled();
        expect(conflict.queueCheckFor).not.toHaveBeenCalled();
        expect(storageAccess.writeFileAuto).toHaveBeenCalledWith("note.md", "remote update", {
            ctime: 1,
            mtime: 2,
        });
    });

    it("does not run the protection path when the remote content matches storage", async () => {
        const { handler, remoteMeta, storageStub, databaseFileAccess, storageAccess, pathService } = createHandler(
            "same body",
            "same body",
            false
        );
        pathService.compareFileFreshness.mockReturnValue(EVEN);

        await expect(handler.dbToStorage(remoteMeta, storageStub)).resolves.toBe(true);

        expect(databaseFileAccess.hasContentInRevisionHistory).not.toHaveBeenCalled();
        expect(storageAccess.writeFileAuto).not.toHaveBeenCalled();
    });

    it("rebinds provenance to the surviving revision when duplicate content already matches storage", async () => {
        const { handler, remoteMeta, storageStub, databaseFileAccess, storageAccess, pathService, provenance } =
            createHandler("same body", "same body", false, EVEN, true);
        provenance.get.mockResolvedValue({
            revision: "1-deleted-duplicate",
            observedStorageMtime: storageStub.stat.mtime,
        });
        pathService.compareFileFreshness.mockReturnValue(EVEN);

        await expect(handler.dbToStorage(remoteMeta, storageStub)).resolves.toBe(true);

        expect(databaseFileAccess.storeAsConflictedRevisionWithResult).not.toHaveBeenCalled();
        expect(storageAccess.writeFileAuto).not.toHaveBeenCalled();
        expect(provenance.set).toHaveBeenCalledWith("note.md", {
            revision: remoteMeta._rev,
            observedStorageMtime: storageStub.stat.mtime,
            reflectedFromDatabase: true,
        });
    });

    it("reflects the explicitly selected revision instead of refetching the winner", async () => {
        const { handler, storageStub, databaseFileAccess, storageAccess } = createHandler(
            "old storage",
            "unused",
            false
        );
        const selected = createMeta("note.md", "selected content", "2-selected");
        const winner = createMeta("note.md", "winner content", "3-winner");
        databaseFileAccess.getConflictedRevs.mockResolvedValue([selected._rev]);
        databaseFileAccess.fetchEntryMeta.mockReset();
        databaseFileAccess.fetchEntryMeta.mockResolvedValueOnce(selected).mockResolvedValueOnce(winner);
        databaseFileAccess.fetchEntryFromMeta.mockImplementation(async (meta: MetaEntry) => ({
            ...meta,
            data: meta._rev === selected._rev ? "selected content" : "winner content",
        }));

        await expect(handler.dbToStorageWithSpecificRev(storageStub, selected._rev, true)).resolves.toBe(true);

        expect(storageAccess.writeFileAuto).toHaveBeenCalledWith("note.md", "selected content", {
            ctime: 1,
            mtime: 2,
        });
        expect(databaseFileAccess.fetchEntryMeta).toHaveBeenCalledTimes(2);
    });

    it("refuses to reflect an explicitly selected revision which is no longer live", async () => {
        const { handler, storageStub, databaseFileAccess, storageAccess, provenance } = createHandler(
            "old storage",
            "unused",
            false
        );
        const selected = createMeta("note.md", "selected content", "2-obsolete");
        const winner = createMeta("note.md", "winner content", "3-winner");
        databaseFileAccess.fetchEntryMeta.mockReset();
        databaseFileAccess.fetchEntryMeta.mockResolvedValueOnce(selected).mockResolvedValueOnce(winner);
        databaseFileAccess.getConflictedRevs.mockResolvedValue([]);

        await expect(handler.dbToStorageWithSpecificRev(storageStub, selected._rev, true)).resolves.toBe(false);

        expect(storageAccess.writeFileAuto).not.toHaveBeenCalled();
        expect(provenance.set).not.toHaveBeenCalled();
    });

    it("reflects an explicitly selected conflict revision while other conflicts remain", async () => {
        const { handler, storageStub, databaseFileAccess, storageAccess, provenance } = createHandler(
            "old storage",
            "unused",
            false,
            TARGET_IS_NEW,
            true
        );
        const selected = createMeta("note.md", "selected content", "2-selected");
        databaseFileAccess.getConflictedRevs.mockResolvedValue(["3-other"]);
        databaseFileAccess.fetchEntryMeta.mockResolvedValue(selected);
        databaseFileAccess.fetchEntryFromMeta.mockResolvedValue({
            ...selected,
            data: "selected content",
        });

        await expect(handler.dbToStorageWithSpecificRev(storageStub, selected._rev, true)).resolves.toBe(true);

        expect(storageAccess.writeFileAuto).toHaveBeenCalledWith("note.md", "selected content", {
            ctime: 1,
            mtime: 2,
        });
        expect(provenance.set).toHaveBeenCalledWith("note.md", {
            revision: selected._rev,
            observedStorageMtime: storageStub.stat.mtime,
            reflectedFromDatabase: true,
        });
    });

    it("restores an explicitly selected revision when the Vault file is missing", async () => {
        const { handler, databaseFileAccess, storageAccess, provenance } = createHandler(
            "unused",
            "unused",
            false,
            TARGET_IS_NEW,
            true
        );
        const selected = createMeta("note.md", "selected content", "2-selected");
        databaseFileAccess.getConflictedRevs.mockResolvedValue(["3-other"]);
        databaseFileAccess.fetchEntryMeta.mockResolvedValue(selected);
        databaseFileAccess.fetchEntryFromMeta.mockResolvedValue({
            ...selected,
            data: "selected content",
        });
        storageAccess.getFileStub.mockResolvedValue(null);
        storageAccess.getStub.mockResolvedValue(null);
        storageAccess.stat.mockResolvedValue({ ctime: 1, mtime: 22, size: 16, type: "file" });

        await expect(handler.dbToStorageWithSpecificRev("note.md" as FilePath, selected._rev, true)).resolves.toBe(
            true
        );

        expect(storageAccess.writeFileAuto).toHaveBeenCalledWith("note.md", "selected content", {
            ctime: 1,
            mtime: 2,
        });
        expect(provenance.set).toHaveBeenCalledWith("note.md", {
            revision: selected._rev,
            observedStorageMtime: 22,
            reflectedFromDatabase: true,
        });
    });
});

describe("ServiceFileHandlerBase conflicted storage operations", () => {
    it("clears matching Vault provenance when an exact live branch is discarded", async () => {
        const { handler, databaseFileAccess, storageStub, provenance } = createHandler(
            "Vault content",
            "winner content",
            false,
            TARGET_IS_NEW,
            true
        );
        const selectedRevision = "2-selected";
        provenance.get.mockResolvedValue({
            revision: selectedRevision,
            observedStorageMtime: storageStub.stat.mtime,
        });
        Object.assign(databaseFileAccess, {
            delete: vi.fn().mockResolvedValue(true),
        });

        await expect(handler.deleteRevisionFromDB(storageStub, selectedRevision)).resolves.toBe(true);

        expect(databaseFileAccess.delete).toHaveBeenCalledWith(storageStub, selectedRevision);
        expect(provenance.delete).toHaveBeenCalledWith("note.md");
    });

    it("keeps Vault provenance which names another live branch", async () => {
        const { handler, databaseFileAccess, storageStub, provenance } = createHandler(
            "Vault content",
            "winner content",
            false,
            TARGET_IS_NEW,
            true
        );
        provenance.get.mockResolvedValue({
            revision: "3-other",
            observedStorageMtime: storageStub.stat.mtime,
        });
        Object.assign(databaseFileAccess, {
            delete: vi.fn().mockResolvedValue(true),
        });

        await expect(handler.deleteRevisionFromDB(storageStub, "2-selected")).resolves.toBe(true);

        expect(provenance.delete).not.toHaveBeenCalled();
    });

    it("keeps matching Vault provenance when exact branch deletion fails", async () => {
        const { handler, databaseFileAccess, storageStub, provenance } = createHandler(
            "Vault content",
            "winner content",
            false,
            TARGET_IS_NEW,
            true
        );
        provenance.get.mockResolvedValue({
            revision: "2-selected",
            observedStorageMtime: storageStub.stat.mtime,
        });
        Object.assign(databaseFileAccess, {
            delete: vi.fn().mockResolvedValue(false),
        });

        await expect(handler.deleteRevisionFromDB(storageStub, "2-selected")).resolves.toBe(false);

        expect(provenance.delete).not.toHaveBeenCalled();
    });

    it("applies a discarded conflict branch after removing it from the live revision tree", async () => {
        const { handler, databaseFileAccess, storageAccess, storageStub, provenance } = createHandler(
            "Vault content",
            "winner content",
            false,
            TARGET_IS_NEW,
            true
        );
        const discardedRevision = "2-discarded";
        const discarded = createMeta("note.md", "discarded content", discardedRevision);
        const winner = createMeta("note.md", "winner content", "3-winner");
        let deleted = false;
        Object.assign(databaseFileAccess, {
            delete: vi.fn().mockImplementation(async () => {
                deleted = true;
                return true;
            }),
        });
        databaseFileAccess.fetchEntryMeta.mockImplementation(
            async (_file: UXFileInfoStub | FilePathWithPrefix, revision?: string) =>
                revision === discardedRevision ? discarded : winner
        );
        databaseFileAccess.getConflictedRevs.mockImplementation(async () => (deleted ? [] : [discardedRevision]));
        databaseFileAccess.fetchEntryFromMeta.mockImplementation(async (meta: MetaEntry) => ({
            ...meta,
            data: meta._rev === discardedRevision ? "discarded content" : "winner content",
        }));

        await expect(
            handler.resolveConflictedByDeletingRevision(storageStub, discardedRevision)
        ).resolves.toBeUndefined();

        expect(databaseFileAccess.delete).toHaveBeenCalledWith(storageStub, discardedRevision);
        expect(storageAccess.writeFileAuto).toHaveBeenCalledWith("note.md", "discarded content", {
            ctime: 1,
            mtime: 2,
        });
        expect(provenance.delete).toHaveBeenCalledWith("note.md");
    });

    it("stores Vault content as a child of an explicitly selected live revision", async () => {
        const { handler, databaseFileAccess, provenance, conflict, storageFile } = createConflictedOperationHandler();
        const selectedRevision = "3-winner";

        await expect(handler.storeFileToDBWithBaseRevision(storageFile, selectedRevision)).resolves.toBe(true);

        expect(databaseFileAccess.storeWithBaseRevision).toHaveBeenCalledWith(storageFile, selectedRevision, true);
        expect(provenance.set).toHaveBeenCalledWith("note.md", {
            revision: "4-local-edit",
            observedStorageMtime: storageFile.stat.mtime,
        });
        expect(conflict.queueCheckFor).toHaveBeenCalledWith("note.md");
    });

    it("refuses to extend a revision which is no longer live", async () => {
        const { handler, databaseFileAccess, provenance, conflict, storageFile } = createConflictedOperationHandler();
        const selectedRevision = "2-obsolete";
        const obsolete = createMeta("note.md", "obsolete", selectedRevision);
        const winner = createMeta("note.md", "winner", "3-winner");
        databaseFileAccess.fetchEntryMeta.mockImplementation(
            async (_file: UXFileInfoStub | FilePathWithPrefix, revision?: string) =>
                revision === selectedRevision ? obsolete : winner
        );

        await expect(handler.storeFileToDBWithBaseRevision(storageFile, selectedRevision)).resolves.toBe(false);

        expect(databaseFileAccess.storeWithBaseRevision).not.toHaveBeenCalled();
        expect(provenance.set).not.toHaveBeenCalled();
        expect(conflict.queueCheckFor).not.toHaveBeenCalled();
    });

    it("refuses to store an unfinished copy on a base revision", async () => {
        const { handler, databaseFileAccess, provenance, storageFile } = createConflictedOperationHandler();
        provenance.get.mockResolvedValue({ revision: "3-displayed", incompleteWriteRevision: "4-remote" });

        await expect(handler.storeFileToDBWithBaseRevision(storageFile, "3-winner")).rejects.toBeInstanceOf(
            UnknownFileWriteStateError
        );

        // Resolving a conflict while a part write runs would publish the partial content as the resolution.
        expect(databaseFileAccess.storeWithBaseRevision).not.toHaveBeenCalled();
    });

    it("records the selected revision without creating a child when its content already matches the Vault", async () => {
        const { handler, databaseFileAccess, provenance, conflict, storageFile } = createConflictedOperationHandler();
        const selectedRevision = "3-winner";
        databaseFileAccess.fetchEntry.mockResolvedValue({
            ...createMeta("note.md", storageFile.body, selectedRevision),
            data: storageFile.body,
        });

        await expect(handler.storeFileToDBWithBaseRevision(storageFile, selectedRevision, false)).resolves.toBe(true);

        expect(databaseFileAccess.storeWithBaseRevision).not.toHaveBeenCalled();
        expect(provenance.set).toHaveBeenCalledWith("note.md", {
            revision: selectedRevision,
            observedStorageMtime: storageFile.stat.mtime,
        });
        expect(conflict.queueCheckFor).toHaveBeenCalledWith("note.md");
    });

    it("does not create a child when asked only to mark a selected revision which differs from the Vault", async () => {
        const { handler, databaseFileAccess, provenance, conflict, storageFile } = createConflictedOperationHandler();
        const selectedRevision = "3-winner";

        await expect(handler.storeFileToDBWithBaseRevision(storageFile, selectedRevision, false)).resolves.toBe(false);

        expect(databaseFileAccess.storeWithBaseRevision).not.toHaveBeenCalled();
        expect(provenance.set).not.toHaveBeenCalled();
        expect(conflict.queueCheckFor).not.toHaveBeenCalled();
    });

    it("extends the revision displayed in storage when a conflicted file is edited", async () => {
        const { handler, databaseFileAccess, provenance, storageFile, displayedRevision } =
            createConflictedOperationHandler();

        await expect(handler.storeFileToDB(storageFile)).resolves.toBe(true);

        expect(databaseFileAccess.storeWithBaseRevision).toHaveBeenCalledWith(storageFile, displayedRevision, true);
        expect(databaseFileAccess.store).not.toHaveBeenCalled();
        expect(provenance.set).toHaveBeenCalledWith("note.md", {
            revision: "4-local-edit",
            observedStorageMtime: storageFile.stat.mtime,
        });
    });

    it("keeps the recorded displayed branch when edited content also matches another branch", async () => {
        const { handler, databaseFileAccess, storageFile, displayedRevision } = createConflictedOperationHandler();
        databaseFileAccess.findContentRevisions.mockResolvedValue(["3-other-branch"]);

        await expect(handler.storeFileToDB(storageFile)).resolves.toBe(true);

        expect(databaseFileAccess.storeWithBaseRevision).toHaveBeenCalledWith(storageFile, displayedRevision, true);
    });

    it("reconstructs a missing displayed revision only from a unique exact content match", async () => {
        const { handler, databaseFileAccess, provenance, storageFile } = createConflictedOperationHandler();
        provenance.get.mockResolvedValue(undefined);
        databaseFileAccess.findContentRevisions.mockResolvedValue(["3-reconstructed"]);

        await expect(handler.storeFileToDB(storageFile)).resolves.toBe(true);

        expect(databaseFileAccess.storeWithBaseRevision).toHaveBeenCalledWith(storageFile, "3-reconstructed", true);
        expect(databaseFileAccess.storeAsConflictedRevisionWithResult).not.toHaveBeenCalled();
    });

    it("preserves an edit as a new conflict when the displayed revision cannot be proved", async () => {
        const { handler, databaseFileAccess, provenance, conflict, storageFile } = createConflictedOperationHandler();
        provenance.get.mockResolvedValue(undefined);
        databaseFileAccess.findContentRevisions.mockResolvedValue(["3-first-match", "3-second-match"]);

        await expect(handler.storeFileToDB(storageFile)).resolves.toBe(true);

        expect(databaseFileAccess.storeWithBaseRevision).not.toHaveBeenCalled();
        expect(databaseFileAccess.storeAsConflictedRevisionWithResult).toHaveBeenCalledWith(
            storageFile,
            "3-winner",
            true
        );
        expect(conflict.queueCheckFor).toHaveBeenCalledWith("note.md");
    });

    it("preserves an edit when conflicted winner content is unavailable but its metadata remains", async () => {
        const { handler, databaseFileAccess, provenance, conflict, storageFile } = createConflictedOperationHandler();
        provenance.get.mockResolvedValue(undefined);
        databaseFileAccess.findContentRevisions.mockResolvedValue([]);
        databaseFileAccess.fetchEntry.mockResolvedValue(false);

        await expect(handler.storeFileToDB(storageFile)).resolves.toBe(true);

        expect(databaseFileAccess.fetchEntryMeta).toHaveBeenCalledWith(storageFile, undefined, true);
        expect(databaseFileAccess.storeAsConflictedRevisionWithResult).toHaveBeenCalledWith(
            storageFile,
            "3-winner",
            true
        );
        expect(conflict.queueCheckFor).toHaveBeenCalledWith("note.md");
    });

    it("keeps a generation-one unreadable winner unresolved when no sibling base can exist", async () => {
        const { handler, databaseFileAccess, provenance, conflict, storageFile } = createConflictedOperationHandler();
        provenance.get.mockResolvedValue(undefined);
        databaseFileAccess.findContentRevisions.mockResolvedValue([]);
        databaseFileAccess.fetchEntry.mockResolvedValue(false);
        databaseFileAccess.fetchEntryMeta.mockResolvedValue({
            _id: "note.md",
            _rev: "1-root",
            path: "note.md",
        });
        databaseFileAccess.storeAsConflictedRevisionWithResult.mockResolvedValue(false);

        await expect(handler.storeFileToDB(storageFile)).resolves.toBe(false);

        expect(databaseFileAccess.storeAsConflictedRevisionWithResult).toHaveBeenCalledWith(
            storageFile,
            "1-root",
            true
        );
        expect(provenance.set).not.toHaveBeenCalled();
        expect(conflict.queueCheckFor).toHaveBeenCalledWith("note.md");
    });

    it("stores a soft-delete child of the displayed revision instead of deleting the winner", async () => {
        const { handler, databaseFileAccess, provenance, conflict, storageFile, displayedRevision } =
            createConflictedOperationHandler();

        await expect(handler.deleteFileFromDB(storageFile)).resolves.toBe(true);

        expect(databaseFileAccess.storeDeletionWithBaseRevision).toHaveBeenCalledWith("note.md", displayedRevision);
        expect(databaseFileAccess.delete).not.toHaveBeenCalled();
        expect(provenance.delete).toHaveBeenCalledWith("note.md");
        expect(conflict.queueCheckFor).toHaveBeenCalledWith("note.md");
    });

    it("preserves every branch when a deleted file has no provable displayed revision", async () => {
        const { handler, databaseFileAccess, provenance, conflict } = createConflictedOperationHandler();
        provenance.get.mockResolvedValue(undefined);

        await expect(handler.deleteFileFromDB("note.md" as FilePath)).resolves.toBe(true);

        expect(databaseFileAccess.storeDeletionWithBaseRevision).not.toHaveBeenCalled();
        expect(databaseFileAccess.delete).not.toHaveBeenCalled();
        expect(conflict.queueCheckFor).toHaveBeenCalledWith("note.md");
    });

    it("extends the displayed revision for a case-only rename", async () => {
        const { handler, databaseFileAccess, provenance, displayedRevision } = createConflictedOperationHandler();
        const renamedFile = createStorageFile("note.md", "renamed case content");

        await expect(handler.renameFileInDB(renamedFile, "Note.md" as FilePath)).resolves.toBe(true);

        expect(databaseFileAccess.storeWithBaseRevision).toHaveBeenCalledWith(renamedFile, displayedRevision, true);
        expect(databaseFileAccess.delete).not.toHaveBeenCalled();
        expect(provenance.delete).toHaveBeenCalledWith("Note.md");
        expect(provenance.set).toHaveBeenCalledWith("note.md", {
            revision: "4-local-edit",
            observedStorageMtime: renamedFile.stat.mtime,
        });
    });

    it("soft-deletes only the displayed source branch for a cross-path rename", async () => {
        const { handler, databaseFileAccess, provenance, conflict, displayedRevision } =
            createConflictedOperationHandler();
        const renamedFile = createStorageFile("new.md", "renamed content");

        await expect(handler.renameFileInDB(renamedFile, "old.md" as FilePath)).resolves.toBe(true);

        expect(databaseFileAccess.storeDeletionWithBaseRevision).toHaveBeenCalledWith("old.md", displayedRevision);
        expect(databaseFileAccess.delete).not.toHaveBeenCalled();
        expect(provenance.delete).toHaveBeenCalledWith("old.md");
        expect(conflict.queueCheckFor).toHaveBeenCalledWith("old.md");
    });

    it("preserves every source branch when a cross-path rename has no provable displayed revision", async () => {
        const { handler, databaseFileAccess, provenance, conflict } = createConflictedOperationHandler();
        provenance.get.mockResolvedValue(undefined);
        databaseFileAccess.findContentRevisions.mockResolvedValue([]);
        vi.spyOn(handler, "storeFileToDB").mockResolvedValue(true);
        const renamedFile = createStorageFile("new.md", "renamed content");

        await expect(handler.renameFileInDB(renamedFile, "old.md" as FilePath)).resolves.toBe(true);

        expect(databaseFileAccess.storeDeletionWithBaseRevision).not.toHaveBeenCalled();
        expect(databaseFileAccess.delete).not.toHaveBeenCalled();
        expect(conflict.queueCheckFor).toHaveBeenCalledWith("old.md");
    });
});

describe("ServiceFileHandlerBase large binary reflection", () => {
    function createBinaryMeta(size: number) {
        return { ...createMeta("image.bin", ""), type: "newnote", datatype: "newnote", size } as unknown as MetaEntry;
    }

    it("writes binary content assembled in batches without loading the whole entry", async () => {
        const { handler, databaseFileAccess, storageAccess } = createHandler("", "", false);
        const meta = createBinaryMeta(4);
        const data = new Uint8Array([1, 2, 3, 4]).buffer;
        databaseFileAccess.fetchEntryMeta.mockResolvedValue(meta);
        storageAccess.getStub.mockResolvedValue(null);
        const fetchBinaryContentFromMeta = vi.fn().mockResolvedValue({ status: "ok", data });
        Object.assign(databaseFileAccess, { fetchBinaryContentFromMeta });

        await expect(handler.dbToStorage(meta, null)).resolves.toBe(true);

        expect(fetchBinaryContentFromMeta).toHaveBeenCalledWith(meta);
        expect(databaseFileAccess.fetchEntryFromMeta).not.toHaveBeenCalled();
        expect(storageAccess.writeFileAuto).toHaveBeenCalledWith("image.bin", data, { ctime: 1, mtime: 2 });
    });

    it("refuses to write binary content whose decoded size differs from the record", async () => {
        const { handler, databaseFileAccess, storageAccess } = createHandler("", "", false);
        const meta = createBinaryMeta(4);
        databaseFileAccess.fetchEntryMeta.mockResolvedValue(meta);
        storageAccess.getStub.mockResolvedValue(null);
        Object.assign(databaseFileAccess, {
            fetchBinaryContentFromMeta: vi.fn().mockResolvedValue({ status: "size-mismatch", decodedSize: 3 }),
        });

        await expect(handler.dbToStorage(meta, null)).resolves.toBe(false);

        expect(databaseFileAccess.fetchEntryFromMeta).not.toHaveBeenCalled();
        expect(storageAccess.writeFileAuto).not.toHaveBeenCalled();
    });

    it("uses the general loading path for binary content it cannot assemble in batches", async () => {
        const { handler, databaseFileAccess, storageAccess } = createHandler("", "", false);
        const meta = createBinaryMeta(0);
        databaseFileAccess.fetchEntryMeta.mockResolvedValue(meta);
        databaseFileAccess.fetchEntryFromMeta.mockResolvedValue({ ...meta, data: [] });
        storageAccess.getStub.mockResolvedValue(null);
        Object.assign(databaseFileAccess, {
            fetchBinaryContentFromMeta: vi.fn().mockResolvedValue({ status: "unsupported" }),
        });

        await expect(handler.dbToStorage(meta, null)).resolves.toBe(true);

        expect(databaseFileAccess.fetchEntryFromMeta).toHaveBeenCalledWith(meta);
        expect(storageAccess.writeFileAuto).toHaveBeenCalled();
    });

    // The shortcut only applies to files large enough that reading them twice would matter.
    const REFLECTED_SIZE = 2 * 1024 * 1024;

    function createReflectedHandler(observedStorageMtime: number) {
        const handlerParts = createHandler("same body", "same body", false, EVEN, true);
        const { databaseFileAccess, storageAccess, provenance, remoteMeta, storageStub } = handlerParts;
        const largeMeta = { ...remoteMeta, size: REFLECTED_SIZE };
        storageStub.stat.size = REFLECTED_SIZE;
        provenance.get.mockResolvedValue({ revision: remoteMeta._rev, observedStorageMtime });
        storageAccess.stat.mockResolvedValue({ ...storageStub.stat, size: REFLECTED_SIZE, mtime: 3 });
        databaseFileAccess.fetchEntryMeta.mockResolvedValue(largeMeta);
        Object.assign(databaseFileAccess, {
            fetchEntry: vi.fn().mockResolvedValue({ ...largeMeta, data: "same body" }),
            storeWithBaseRevision: vi.fn().mockResolvedValue("3-local"),
        });
        return { ...handlerParts, remoteMeta: largeMeta };
    }

    it("does not read storage again when it still holds the revision this device reflected", async () => {
        const { handler, storageStub, storageAccess } = createReflectedHandler(3);

        await expect(handler.storeFileToDB(storageStub)).resolves.toBe(true);

        expect(storageAccess.readStubContent).not.toHaveBeenCalled();
    });

    it("reads storage when its modification time differs from the reflected one", async () => {
        const { handler, storageStub, storageAccess } = createReflectedHandler(2);

        await expect(handler.storeFileToDB(storageStub)).resolves.toBe(true);

        expect(storageAccess.readStubContent).toHaveBeenCalled();
    });

    it("does not load content to reflect a revision which storage already holds", async () => {
        const { handler, storageStub, storageAccess, databaseFileAccess, remoteMeta } = createReflectedHandler(3);
        const fetchBinaryContentFromMeta = vi.fn();
        Object.assign(databaseFileAccess, { fetchBinaryContentFromMeta });
        storageAccess.getStub.mockResolvedValue(storageStub);

        await expect(handler.dbToStorage(remoteMeta, storageStub)).resolves.toBe(true);

        expect(fetchBinaryContentFromMeta).not.toHaveBeenCalled();
        expect(databaseFileAccess.fetchEntryFromMeta).not.toHaveBeenCalled();
        expect(storageAccess.readStubContent).not.toHaveBeenCalled();
        expect(storageAccess.writeFileAuto).not.toHaveBeenCalled();
    });

    function createBinaryStoreHandler(storageBytes: Uint8Array, databaseBytes: Uint8Array) {
        const handlerParts = createHandler("", "", false, EVEN);
        const { databaseFileAccess, storageAccess } = handlerParts;
        const meta = createBinaryMeta(databaseBytes.byteLength);
        const storageStub = {
            name: "image.bin",
            path: "image.bin",
            stat: { ctime: 1, mtime: 2, size: storageBytes.byteLength, type: "file" },
        } as UXFileInfoStub;
        storageAccess.readStubContent.mockResolvedValue({ ...storageStub, body: new Blob([storageBytes]) });
        databaseFileAccess.fetchEntryMeta.mockResolvedValue(meta);
        const fetchEntry = vi.fn();
        const storeWithBaseRevision = vi.fn().mockResolvedValue("3-local");
        Object.assign(databaseFileAccess, {
            fetchEntry,
            storeWithBaseRevision,
            fetchBinaryContentFromMeta: vi.fn().mockResolvedValue({ status: "ok", data: databaseBytes.slice().buffer }),
        });
        return { handler: handlerParts.handler, storageStub, fetchEntry, storeWithBaseRevision };
    }

    it("recognises an unchanged binary file by comparing bytes instead of chunk text", async () => {
        const bytes = new Uint8Array([1, 2, 3, 4]);
        const { handler, storageStub, fetchEntry, storeWithBaseRevision } = createBinaryStoreHandler(bytes, bytes);

        await expect(handler.storeFileToDB(storageStub)).resolves.toBe(true);

        expect(fetchEntry).not.toHaveBeenCalled();
        expect(storeWithBaseRevision).not.toHaveBeenCalled();
    });

    it("stores a binary file whose bytes differ under the same modification time", async () => {
        const { handler, storageStub, storeWithBaseRevision } = createBinaryStoreHandler(
            new Uint8Array([1, 2, 3, 5]),
            new Uint8Array([1, 2, 3, 4])
        );

        await expect(handler.storeFileToDB(storageStub)).resolves.toBe(true);

        expect(storeWithBaseRevision).toHaveBeenCalledWith(
            expect.objectContaining({ path: "image.bin" }),
            "2-remote",
            true
        );
    });
});

describe("ServiceFileHandlerBase staged binary writes", () => {
    function fixture() {
        const f = createHandler("", "", false, EVEN, true);
        const meta = {
            ...createMeta("image.bin", ""),
            type: "newnote",
            datatype: "newnote",
            size: 12,
            children: Array.from({ length: 128 }, (_, i) => "chunk-" + i),
        } as unknown as MetaEntry;
        let disk: Uint8Array | undefined;
        const stat = () => (disk ? { size: disk.length, mtime: meta.mtime, ctime: 1, type: "file" as const } : null);
        const source = vi.fn(async function* () {
            yield new Uint8Array(8).fill(1);
            yield new Uint8Array(4).fill(2);
        });
        let failPublication = false;
        const write = vi.fn(async (_path: string, parts: AsyncIterable<Uint8Array>, publication: BinaryPublication) => {
            const staged: number[] = [];
            for await (const part of parts) staged.push(...part);
            if (staged.length !== publication.size) throw new Error("wrong size");
            await publication.beforePublish();
            if (failPublication) {
                disk = undefined;
                throw new Error("native predelete interruption");
            }
            disk = new Uint8Array(staged);
            await publication.afterPublish(stat()!);
            return true;
        });
        const fullRead = vi.fn().mockResolvedValue({ status: "ok", data: new ArrayBuffer(12) });
        const store = vi.fn().mockResolvedValue("3-local");
        f.databaseFileAccess.fetchEntryMeta.mockResolvedValue(meta);
        Object.assign(f.databaseFileAccess, {
            iterateBinaryContentFromMeta: source,
            canStreamBinaryContentFromMeta: vi.fn().mockResolvedValue(true),
            fetchBinaryContentFromMeta: fullRead,
            storeWithBaseRevision: store,
            delete: vi.fn(),
        });
        f.storageAccess.getStub.mockResolvedValue(null);
        f.storageAccess.getFileStub.mockResolvedValue(null);
        f.storageAccess.statHidden.mockImplementation(async () => stat());
        f.storageAccess.stat.mockImplementation(async () => stat());
        f.storageAccess.isExistsIncludeHidden.mockImplementation(async () => Boolean(disk));
        Object.assign(f.storageAccess, { writeBinaryFileInParts: write, supportsBinaryPartWrites: () => true });
        return {
            ...f,
            meta,
            source,
            write,
            fullRead,
            store,
            disk: () => disk,
            setDisk: (value: Uint8Array | undefined) => {
                disk = value;
            },
            failPublication: (fail: boolean) => {
                failPublication = fail;
            },
            restart: () => new TestFileHandler(f.deps),
        };
    }
    it("publishes complete staged content and completes its durable publication mark", async () => {
        const f = fixture();
        expect(await f.handler.dbToStorage(f.meta, null)).toBe(true);
        expect([...f.disk()!]).toEqual([...Array(8).fill(1), ...Array(4).fill(2)]);
        expect(f.records.get("image.bin")).toEqual({
            revision: "2-remote",
            observedStorageMtime: f.meta.mtime,
            reflectedFromDatabase: true,
        });
        expect(f.fullRead).not.toHaveBeenCalled();
        expect(f.storageAccess.writeFileAuto).not.toHaveBeenCalled();
    });
    it("repeated missing chunks never expose partial target bytes or use a full buffer", async () => {
        const f = fixture();
        f.source.mockImplementation(async function* () {
            yield new Uint8Array(8);
            throw new Error("missing chunk");
        });
        for (let attempt = 0; attempt < 6; attempt++) expect(await f.handler.dbToStorage(f.meta, null)).toBe(false);
        expect(f.disk()).toBeUndefined();
        expect(f.records.get("image.bin")).toBeUndefined();
        expect(f.fullRead).not.toHaveBeenCalled();
        expect(f.storageAccess.writeFileAuto).not.toHaveBeenCalled();
    });
    it("recovers a publication gap on restart instead of syncing its absence as deletion", async () => {
        const f = fixture();
        f.failPublication(true);
        expect(await f.handler.dbToStorage(f.meta, null)).toBe(false);
        expect(f.records.get("image.bin")?.pendingPublication?.revision).toBe("2-remote");
        f.failPublication(false);
        expect(await f.restart().deleteFileFromDB("image.bin" as FilePath)).toBe(true);
        expect(f.disk()?.length).toBe(12);
        expect(f.databaseFileAccess.delete).not.toHaveBeenCalled();
        expect(f.store).not.toHaveBeenCalled();
        expect(f.records.get("image.bin")?.pendingPublication).toBeUndefined();
    });
    it("terminates a post-rename mark before accepting a later genuine local deletion", async () => {
        const f = fixture();
        await f.handler.dbToStorage(f.meta, null);
        f.records.set("image.bin", {
            revision: "1-previous",
            pendingPublication: { revision: "2-remote", token: "killed" },
        });
        const fetchEntry = vi.fn().mockResolvedValue(false);
        const deleteEntry = vi.fn().mockResolvedValue(true);
        Object.assign(f.databaseFileAccess, { fetchEntry, delete: deleteEntry });
        const file = {
            name: "image.bin",
            path: "image.bin",
            stat: { type: "file", size: 12, mtime: 2, ctime: 1 },
        } as UXFileInfoStub;
        expect(await f.restart().storeFileToDBWithBaseRevision(file, "2-remote", true)).toBe(true);
        expect(f.records.get("image.bin")?.pendingPublication).toBeUndefined();
        f.setDisk(undefined);
        fetchEntry.mockResolvedValue(f.meta);
        expect(await f.restart().deleteFileFromDB("image.bin" as FilePath)).toBe(true);
        expect(deleteEntry).toHaveBeenCalledOnce();
        expect(f.write).toHaveBeenCalledTimes(1);
        expect(f.disk()).toBeUndefined();
    });

    it("honours a later database deletion instead of reviving the earlier publication", async () => {
        const f = fixture();
        f.failPublication(true);
        await f.handler.dbToStorage(f.meta, null);
        f.databaseFileAccess.fetchEntryMeta.mockResolvedValue({ ...f.meta, deleted: true });
        expect(await f.restart().deleteFileFromDB("image.bin" as FilePath)).toBe(true);
        expect(f.disk()).toBeUndefined();
        expect(f.records.get("image.bin")).toBeUndefined();
        expect(f.write).toHaveBeenCalledTimes(1);
    });
    it("refuses staging on unknown provenance and refuses publication on failed mark persistence", async () => {
        const f = fixture();
        f.provenance.get.mockRejectedValue(new Error("unreadable"));
        await expect(f.handler.dbToStorage(f.meta, null)).rejects.toBeInstanceOf(UnknownFileWriteStateError);
        expect(f.write).not.toHaveBeenCalled();
        f.provenance.get.mockResolvedValue(undefined);
        f.provenance.set.mockRejectedValue(new Error("unwritable"));
        expect(await f.handler.dbToStorage(f.meta, null)).toBe(false);
        expect(f.disk()).toBeUndefined();
    });
    it("does not sync a deletion if its gap record is unreadable after restart", async () => {
        const f = fixture();
        f.failPublication(true);
        await f.handler.dbToStorage(f.meta, null);
        f.provenance.get.mockRejectedValue(new Error("unreadable"));
        await expect(f.restart().deleteFileFromDB("image.bin" as FilePath)).rejects.toBeInstanceOf(
            UnknownFileWriteStateError
        );
        expect(f.databaseFileAccess.delete).not.toHaveBeenCalled();
    });
    it("retains a complete target when journal completion fails", async () => {
        const f = fixture();
        const set = f.provenance.set.getMockImplementation()!;
        f.provenance.set.mockImplementation(async (path, record) => {
            if (!record.pendingPublication) throw new Error("finish failed");
            await set(path, record);
        });
        expect(await f.handler.dbToStorage(f.meta, null)).toBe(false);
        expect(f.disk()?.length).toBe(12);
        expect(f.records.get("image.bin")?.pendingPublication).toBeDefined();
        expect(f.store).not.toHaveBeenCalled();
    });
});

describe("ServiceFileHandlerBase empty store recheck", () => {
    function createNewFileHandler(body: string, laterStats: ({ size: number } | null)[]) {
        const storageFile = createStorageFile("note.md", body);
        const storageStub = createStorageStub("note.md", body);
        const stat = vi.fn();
        for (const next of laterStats) {
            stat.mockResolvedValueOnce(next && { ...storageFile.stat, size: next.size });
        }
        const deps = {
            events: createLiveSyncEventHub(),
            API: { addLog: vi.fn() },
            databaseFileAccess: {
                fetchEntry: vi.fn().mockResolvedValue(false),
                getConflictedRevs: vi.fn().mockResolvedValue([]),
                storeWithBaseRevision: vi.fn().mockResolvedValue("1-new"),
            },
            storageAccess: {
                getFileStub: vi.fn().mockResolvedValue(storageStub),
                readStubContent: vi.fn().mockResolvedValue(storageFile),
                stat,
            },
            fileProcessing: { processFileEvent: { addHandler: vi.fn() } },
            replication: { processSynchroniseResult: { addHandler: vi.fn() } },
            conflict: {},
            path: { compareFileFreshness: vi.fn().mockReturnValue(EVEN) },
            setting: { currentSettings: vi.fn().mockReturnValue({}) },
            vault: {},
        } as unknown as ServiceFileHandlerDependencies;
        return { handler: new TestFileHandler(deps), storageStub, stat };
    }

    it("stores a file again when content reaches storage after it was stored while empty", async () => {
        vi.useFakeTimers();
        try {
            const { handler, storageStub, stat } = createNewFileHandler("", [{ size: 0 }, { size: 83 }]);
            await expect(handler.storeFileToDB(storageStub)).resolves.toBe(true);
            const storeAgain = vi.spyOn(handler, "storeFileToDB").mockResolvedValue(true);

            await vi.advanceTimersByTimeAsync(3_000);
            expect(stat).toHaveBeenCalledTimes(1);
            expect(storeAgain).not.toHaveBeenCalled();

            await vi.advanceTimersByTimeAsync(15_000);
            expect(stat).toHaveBeenCalledTimes(2);
            expect(storeAgain).toHaveBeenCalledExactlyOnceWith("note.md");
        } finally {
            vi.useRealTimers();
        }
    });

    it("does not check a non-empty stored file again", async () => {
        vi.useFakeTimers();
        try {
            const { handler, storageStub, stat } = createNewFileHandler("content", []);
            await expect(handler.storeFileToDB(storageStub)).resolves.toBe(true);

            await vi.advanceTimersByTimeAsync(20_000);
            expect(stat).not.toHaveBeenCalled();
        } finally {
            vi.useRealTimers();
        }
    });

    it("stops checking an empty stored file which was removed", async () => {
        vi.useFakeTimers();
        try {
            const { handler, storageStub, stat } = createNewFileHandler("", [null]);
            await expect(handler.storeFileToDB(storageStub)).resolves.toBe(true);
            const storeAgain = vi.spyOn(handler, "storeFileToDB").mockResolvedValue(true);

            await vi.advanceTimersByTimeAsync(20_000);
            expect(stat).toHaveBeenCalledTimes(1);
            expect(storeAgain).not.toHaveBeenCalled();
        } finally {
            vi.useRealTimers();
        }
    });
});

describe("ServiceFileHandlerBase empty reads on Android", () => {
    const ANDROID = "android-app";
    /** The whole confirmation window: 3 s, 15 s, 1 min, 5 min and 15 min. */
    const CONFIRMATION_WINDOW_MS = 3_000 + 15_000 + 60_000 + 300_000 + 900_000;
    const OTHER_PLATFORMS = ["ios", "macos", "linux", undefined];

    /**
     * A file handler over one storage file whose content a test changes, as Android's shared storage does when
     * a stale empty size is replaced by the content which was on disk all along.
     */
    function createStorageHarness(
        platform: string | undefined,
        localBody: string,
        databaseBody?: string,
        conflictedRevisions: string[] = []
    ) {
        let body = localBody;
        const stat = () => ({ ctime: 1, mtime: 3, size: byteLength(body), type: "file" as const });
        const stub = () => ({ name: "note.md", path: "note.md", stat: stat() }) as UXFileInfoStub;
        const meta = databaseBody === undefined ? false : createMeta("note.md", databaseBody);
        const storageAccess = {
            getFileStub: vi.fn(async () => stub()),
            readStubContent: vi.fn(async () => ({ ...stub(), body: createTextBlob(body) }) as UXFileInfo),
            stat: vi.fn(async () => stat()),
        };
        const databaseFileAccess = {
            fetchEntry: vi.fn().mockResolvedValue(meta && { ...meta, data: databaseBody }),
            fetchEntryMeta: vi.fn().mockResolvedValue(meta),
            findContentRevisions: vi.fn().mockResolvedValue([]),
            getConflictedRevs: vi.fn().mockResolvedValue(conflictedRevisions),
            storeWithBaseRevision: vi.fn().mockResolvedValue("3-stored"),
            storeAsConflictedRevisionWithResult: vi.fn().mockResolvedValue("3-preserved"),
        };
        let processFileEvent: ((item: FileEventItem) => Promise<boolean>) | undefined;
        const deps = {
            events: createLiveSyncEventHub(),
            API: { addLog: vi.fn(), getPlatform: () => platform },
            databaseFileAccess,
            storageAccess,
            fileProcessing: {
                processFileEvent: {
                    addHandler: vi.fn((handler: (item: FileEventItem) => Promise<boolean>) => {
                        processFileEvent = handler;
                    }),
                },
            },
            replication: { processSynchroniseResult: { addHandler: vi.fn() } },
            conflict: { queueCheckFor: vi.fn().mockResolvedValue(undefined) },
            path: {
                path2id: vi.fn(async (path: string) => path),
                compareFileFreshness: vi.fn().mockReturnValue(BASE_IS_NEW),
                markChangesAreSame: vi.fn(),
            },
            setting: { currentSettings: vi.fn().mockReturnValue({}) },
            vault: { isTargetFile: vi.fn().mockResolvedValue(true) },
        } as unknown as ServiceFileHandlerDependencies;
        const handler = new TestFileHandler(deps);
        return {
            handler,
            deps,
            stub,
            storageAccess,
            databaseFileAccess,
            setStorageBody: (next: string) => {
                body = next;
            },
            /** Dispatches a storage change event through the handler's registered event path. */
            dispatchChange: () => {
                if (!processFileEvent) throw new Error("File event handler was not registered");
                return processFileEvent({ type: "CHANGED", args: { file: stub() } } as FileEventItem);
            },
        };
    }

    function notices(deps: ServiceFileHandlerDependencies): unknown[] {
        const addLog = deps.API.addLog as unknown as ReturnType<typeof vi.fn>;
        return addLog.mock.calls.filter(([, level]) => level === LOG_LEVEL_NOTICE).map(([message]) => message);
    }

    function settleLogLines(deps: ServiceFileHandlerDependencies): number {
        const addLog = deps.API.addLog as unknown as ReturnType<typeof vi.fn>;
        return addLog.mock.calls.filter(([message]) => String(message).includes("waiting operations")).length;
    }

    async function storedBodies(store: ReturnType<typeof vi.fn>): Promise<string[]> {
        return await Promise.all(store.mock.calls.map(([file]) => (file as UXFileInfo).body.text()));
    }

    async function withFakeTimers(test: () => Promise<void>) {
        vi.useFakeTimers();
        try {
            await test();
        } finally {
            vi.useRealTimers();
        }
    }

    function confirmationLogLines(deps: ServiceFileHandlerDependencies): number {
        const addLog = deps.API.addLog as unknown as ReturnType<typeof vi.fn>;
        return addLog.mock.calls.filter(([message]) => String(message).includes("reads as empty; it is published"))
            .length;
    }

    describe("storing storage into the database", () => {
        it.each([
            ["missing from", undefined],
            ["empty in", ""],
        ])(
            "does not store a new file %s the database which reads as empty, and stores content which appears",
            async (_, databaseBody) => {
                await withFakeTimers(async () => {
                    const { handler, stub, databaseFileAccess, setStorageBody } = createStorageHarness(
                        ANDROID,
                        "",
                        databaseBody
                    );

                    await expect(handler.storeFileToDB(stub())).resolves.toBe(true);
                    expect(databaseFileAccess.storeWithBaseRevision).not.toHaveBeenCalled();

                    setStorageBody("written on the phone");
                    await vi.advanceTimersByTimeAsync(3_000);

                    expect(await storedBodies(databaseFileAccess.storeWithBaseRevision)).toEqual([
                        "written on the phone",
                    ]);
                });
            }
        );

        it.each([
            ["missing from", undefined],
            ["empty in", ""],
        ])(
            "stores a new file %s the database which stays empty through the whole window, and only then",
            async (_, databaseBody) => {
                await withFakeTimers(async () => {
                    const { handler, stub, databaseFileAccess } = createStorageHarness(ANDROID, "", databaseBody);

                    await expect(handler.storeFileToDB(stub())).resolves.toBe(true);
                    await vi.advanceTimersByTimeAsync(CONFIRMATION_WINDOW_MS - 1);
                    expect(databaseFileAccess.storeWithBaseRevision).not.toHaveBeenCalled();

                    await vi.advanceTimersByTimeAsync(1);
                    expect(await storedBodies(databaseFileAccess.storeWithBaseRevision)).toEqual([""]);
                });
            }
        );

        it("never stores an empty read over content in the database, and stores content which appears", async () => {
            await withFakeTimers(async () => {
                const { handler, stub, databaseFileAccess, setStorageBody } = createStorageHarness(
                    ANDROID,
                    "",
                    "synchronised body"
                );

                await expect(handler.storeFileToDB(stub())).resolves.toBe(true);
                await vi.advanceTimersByTimeAsync(15_000);
                expect(databaseFileAccess.storeWithBaseRevision).not.toHaveBeenCalled();

                setStorageBody("synchronised body, edited");
                await vi.advanceTimersByTimeAsync(60_000);

                expect(databaseFileAccess.storeWithBaseRevision).toHaveBeenCalledExactlyOnceWith(
                    expect.objectContaining({ path: "note.md" }),
                    "2-remote",
                    true
                );
                expect(await storedBodies(databaseFileAccess.storeWithBaseRevision)).toEqual([
                    "synchronised body, edited",
                ]);
            });
        });

        it("never stores an empty read over content in the database, however long it lasts", async () => {
            await withFakeTimers(async () => {
                const { handler, stub, storageAccess, databaseFileAccess } = createStorageHarness(
                    ANDROID,
                    "",
                    "synchronised body"
                );

                await expect(handler.storeFileToDB(stub())).resolves.toBe(true);
                await vi.advanceTimersByTimeAsync(CONFIRMATION_WINDOW_MS);
                const checks = storageAccess.stat.mock.calls.length;
                await vi.advanceTimersByTimeAsync(CONFIRMATION_WINDOW_MS);

                expect(databaseFileAccess.storeWithBaseRevision).not.toHaveBeenCalled();
                expect(databaseFileAccess.storeAsConflictedRevisionWithResult).not.toHaveBeenCalled();
                // The confirmation has ended; nothing keeps checking the file.
                expect(storageAccess.stat).toHaveBeenCalledTimes(checks);
            });
        });

        it("does not preserve an empty read of a conflicted file as a conflicted revision", async () => {
            await withFakeTimers(async () => {
                const { handler, stub, databaseFileAccess } = createStorageHarness(ANDROID, "", "synchronised body", [
                    "2-other",
                ]);

                await expect(handler.storeFileToDB(stub())).resolves.toBe(true);
                await vi.advanceTimersByTimeAsync(2 * CONFIRMATION_WINDOW_MS);

                expect(databaseFileAccess.storeAsConflictedRevisionWithResult).not.toHaveBeenCalled();
                expect(databaseFileAccess.storeWithBaseRevision).not.toHaveBeenCalled();
            });
        });

        it("confirms again when the file reads as empty while its appeared content is being stored", async () => {
            await withFakeTimers(async () => {
                const { handler, stub, storageAccess, databaseFileAccess, setStorageBody } = createStorageHarness(
                    ANDROID,
                    ""
                );

                await expect(handler.storeFileToDB(stub())).resolves.toBe(true);
                // The size flickers: the check sees content, but the store which follows still reads nothing.
                storageAccess.stat.mockResolvedValueOnce({ ctime: 1, mtime: 3, size: 20, type: "file" });
                await vi.advanceTimersByTimeAsync(3_000);
                expect(databaseFileAccess.storeWithBaseRevision).not.toHaveBeenCalled();

                setStorageBody("written on the phone");
                await vi.advanceTimersByTimeAsync(3_000);

                expect(await storedBodies(databaseFileAccess.storeWithBaseRevision)).toEqual(["written on the phone"]);
            });
        });

        it("keeps every store which arrives while the read is confirmed, with one log line", async () => {
            await withFakeTimers(async () => {
                const { handler, deps, stub, databaseFileAccess, setStorageBody } = createStorageHarness(ANDROID, "");

                await expect(handler.storeFileToDB(stub())).resolves.toBe(true);
                await expect(handler.storeFileToDB(stub(), true)).resolves.toBe(true);
                expect(confirmationLogLines(deps)).toBe(1);

                setStorageBody("written on the phone");
                await vi.advanceTimersByTimeAsync(3_000);

                expect(await storedBodies(databaseFileAccess.storeWithBaseRevision)).toEqual([
                    "written on the phone",
                    "written on the phone",
                ]);
            });
        });

        it("stores a file with content at once, without checking it again", async () => {
            await withFakeTimers(async () => {
                const { handler, stub, storageAccess, databaseFileAccess } = createStorageHarness(ANDROID, "content");

                await expect(handler.storeFileToDB(stub())).resolves.toBe(true);

                expect(await storedBodies(databaseFileAccess.storeWithBaseRevision)).toEqual(["content"]);
                await vi.advanceTimersByTimeAsync(CONFIRMATION_WINDOW_MS);
                expect(storageAccess.stat).not.toHaveBeenCalled();
                expect(databaseFileAccess.storeWithBaseRevision).toHaveBeenCalledTimes(1);
            });
        });

        it("stores content on a selected revision once it appears, and never an empty read over it", async () => {
            await withFakeTimers(async () => {
                const { handler, stub, databaseFileAccess, setStorageBody } = createStorageHarness(
                    ANDROID,
                    "",
                    "selected body"
                );

                await expect(handler.storeFileToDBWithBaseRevision(stub(), "2-remote")).resolves.toBe(true);
                expect(databaseFileAccess.storeWithBaseRevision).not.toHaveBeenCalled();

                setStorageBody("resolved on the phone");
                await vi.advanceTimersByTimeAsync(3_000);
                expect(databaseFileAccess.storeWithBaseRevision).toHaveBeenCalledExactlyOnceWith(
                    expect.objectContaining({ path: "note.md" }),
                    "2-remote",
                    true
                );
                expect(await storedBodies(databaseFileAccess.storeWithBaseRevision)).toEqual(["resolved on the phone"]);
            });
        });

        it("does not store an empty read on a selected revision with content after the window", async () => {
            await withFakeTimers(async () => {
                const { handler, stub, databaseFileAccess } = createStorageHarness(ANDROID, "", "selected body");

                await expect(handler.storeFileToDBWithBaseRevision(stub(), "2-remote")).resolves.toBe(true);
                await vi.advanceTimersByTimeAsync(2 * CONFIRMATION_WINDOW_MS);

                expect(databaseFileAccess.storeWithBaseRevision).not.toHaveBeenCalled();
            });
        });

        it("stops confirming when the host unloads", async () => {
            await withFakeTimers(async () => {
                const { handler, deps, stub, storageAccess, databaseFileAccess, setStorageBody } = createStorageHarness(
                    ANDROID,
                    ""
                );

                await expect(handler.storeFileToDB(stub())).resolves.toBe(true);
                deps.events.emitEvent(EVENT_PLUGIN_UNLOADED);
                setStorageBody("written on the phone");
                await vi.advanceTimersByTimeAsync(2 * CONFIRMATION_WINDOW_MS);

                expect(storageAccess.stat).not.toHaveBeenCalled();
                expect(databaseFileAccess.storeWithBaseRevision).not.toHaveBeenCalled();
                expect(vi.getTimerCount()).toBe(0);
            });
        });

        it("starts no confirmation after the host has unloaded, and accepts no empty read", async () => {
            await withFakeTimers(async () => {
                const { handler, deps, stub, storageAccess, databaseFileAccess } = createStorageHarness(ANDROID, "");
                // The host unloads before any confirmation has ever started.
                deps.events.emitEvent(EVENT_PLUGIN_UNLOADED);

                await expect(handler.storeFileToDB(stub())).resolves.toBe(false);
                await vi.advanceTimersByTimeAsync(2 * CONFIRMATION_WINDOW_MS);

                expect(vi.getTimerCount()).toBe(0);
                expect(storageAccess.stat).not.toHaveBeenCalled();
                expect(confirmationLogLines(deps)).toBe(0);
                expect(databaseFileAccess.storeWithBaseRevision).not.toHaveBeenCalled();
            });
        });

        it("does not accept an empty read on a selected revision after the host has unloaded", async () => {
            await withFakeTimers(async () => {
                const { handler, deps, stub, databaseFileAccess } = createStorageHarness(ANDROID, "", "selected body");
                deps.events.emitEvent(EVENT_PLUGIN_UNLOADED);

                await expect(handler.storeFileToDBWithBaseRevision(stub(), "2-remote")).resolves.toBe(false);
                await vi.advanceTimersByTimeAsync(2 * CONFIRMATION_WINDOW_MS);

                expect(vi.getTimerCount()).toBe(0);
                expect(databaseFileAccess.storeWithBaseRevision).not.toHaveBeenCalled();
            });
        });

        it("goes on checking when a check fails, and settles once after the window", async () => {
            await withFakeTimers(async () => {
                const { handler, deps, stub, storageAccess, databaseFileAccess } = createStorageHarness(
                    ANDROID,
                    "",
                    "synchronised body"
                );
                storageAccess.stat.mockRejectedValue(new Error("stat failed"));

                await expect(handler.storeFileToDB(stub())).resolves.toBe(true);
                await vi.advanceTimersByTimeAsync(CONFIRMATION_WINDOW_MS - 1);
                expect(storageAccess.stat).toHaveBeenCalledTimes(4);
                expect(settleLogLines(deps)).toBe(0);

                await vi.advanceTimersByTimeAsync(1);
                expect(storageAccess.stat).toHaveBeenCalledTimes(5);
                expect(settleLogLines(deps)).toBe(1);
                expect(notices(deps)).toEqual([]);
                expect(databaseFileAccess.storeWithBaseRevision).not.toHaveBeenCalled();
                expect(databaseFileAccess.storeAsConflictedRevisionWithResult).not.toHaveBeenCalled();
            });
        });

        it("runs a timer settle and a storage event on the same path one after the other", async () => {
            await withFakeTimers(async () => {
                const { handler, stub, databaseFileAccess, setStorageBody, dispatchChange } = createStorageHarness(
                    ANDROID,
                    ""
                );
                await expect(handler.storeFileToDB(stub())).resolves.toBe(true);
                setStorageBody("written on the phone");
                let release: () => void = () => undefined;
                const blocked = new Promise<void>((resolve) => {
                    release = resolve;
                });
                databaseFileAccess.storeWithBaseRevision.mockImplementationOnce(async () => {
                    await blocked;
                    return "3-stored";
                });

                // The timer settles the confirmation, and its store is still running.
                await vi.advanceTimersByTimeAsync(3_000);
                expect(databaseFileAccess.storeWithBaseRevision).toHaveBeenCalledTimes(1);

                // A storage event on the same path waits until that store has finished.
                const event = dispatchChange();
                await vi.advanceTimersByTimeAsync(0);
                expect(databaseFileAccess.storeWithBaseRevision).toHaveBeenCalledTimes(1);

                release();
                await expect(event).resolves.toBe(true);
                expect(databaseFileAccess.storeWithBaseRevision).toHaveBeenCalledTimes(2);
            });
        });

        it.each(OTHER_PLATFORMS)("stores an empty read at once on platform %s", async (platform) => {
            const { handler, stub, databaseFileAccess } = createStorageHarness(platform, "");

            await expect(handler.storeFileToDB(stub())).resolves.toBe(true);

            expect(await storedBodies(databaseFileAccess.storeWithBaseRevision)).toEqual([""]);
        });

        it.each(OTHER_PLATFORMS)(
            "stores an empty read on a selected revision at once on platform %s",
            async (platform) => {
                const { handler, stub, databaseFileAccess } = createStorageHarness(platform, "", "selected body");

                await expect(handler.storeFileToDBWithBaseRevision(stub(), "2-remote")).resolves.toBe(true);

                expect(await storedBodies(databaseFileAccess.storeWithBaseRevision)).toEqual([""]);
            }
        );

        it.each(OTHER_PLATFORMS)(
            "keeps the recheck of an empty stored file held through its store again on platform %s",
            async (platform) => {
                await withFakeTimers(async () => {
                    const { handler, stub, storageAccess, databaseFileAccess, setStorageBody } = createStorageHarness(
                        platform,
                        ""
                    );

                    await expect(handler.storeFileToDB(stub())).resolves.toBe(true);
                    expect(databaseFileAccess.storeWithBaseRevision).toHaveBeenCalledTimes(1);

                    // The first check sees content, but the store it starts still reads the file as empty. That
                    // store runs while the check holds the path, so it does not start a check of its own.
                    storageAccess.stat.mockResolvedValueOnce({ ctime: 1, mtime: 3, size: 20, type: "file" });
                    await vi.advanceTimersByTimeAsync(3_000);
                    expect(databaseFileAccess.storeWithBaseRevision).toHaveBeenCalledTimes(2);

                    setStorageBody("content which arrived later");
                    await vi.advanceTimersByTimeAsync(CONFIRMATION_WINDOW_MS);
                    expect(storageAccess.stat).toHaveBeenCalledTimes(1);
                    expect(await storedBodies(databaseFileAccess.storeWithBaseRevision)).toEqual(["", ""]);
                });
            }
        );
    });

    describe("renaming", () => {
        /**
         * A rename of `old.md` to `new.md`. The target's reported size and its read content can differ, as they do
         * while Android's shared storage flickers between a stale empty size and the content.
         */
        function createRenameOnAndroidHarness(platform: string | undefined, sourceBody = "moved body") {
            let body = "";
            let reportedSize: number | undefined;
            let targetStored = false;
            const stat = () => ({
                ctime: 1,
                mtime: 3,
                size: reportedSize ?? byteLength(body),
                type: "file" as const,
            });
            const stub = () => ({ name: "new.md", path: "new.md", stat: stat() }) as UXFileInfoStub;
            const oldEntry = createMeta("old.md", sourceBody, "2-old");
            const storeWithBaseRevision = vi.fn(async (): Promise<string | false> => {
                targetStored = true;
                return "1-new";
            });
            const databaseFileAccess = {
                fetchEntryMeta: vi.fn(async (path: UXFileInfoStub | FilePathWithPrefix) => {
                    const filePath = typeof path === "string" ? path : path.path;
                    if (filePath === "old.md") return oldEntry;
                    return targetStored ? createMeta("new.md", body, "1-new") : false;
                }),
                fetchEntry: vi.fn().mockResolvedValue(false),
                getConflictedRevs: vi.fn().mockResolvedValue([]),
                storeWithBaseRevision,
                delete: vi.fn().mockResolvedValue(true),
            };
            const deps = {
                events: createLiveSyncEventHub(),
                API: { addLog: vi.fn(), getPlatform: () => platform },
                databaseFileAccess,
                storageAccess: {
                    getFileStub: vi.fn(async () => stub()),
                    readStubContent: vi.fn(async () => ({ ...stub(), body: createTextBlob(body) }) as UXFileInfo),
                    stat: vi.fn(async () => stat()),
                },
                fileProcessing: { processFileEvent: { addHandler: vi.fn() } },
                replication: { processSynchroniseResult: { addHandler: vi.fn() } },
                conflict: {},
                path: {
                    path2id: vi.fn(async (path: string) => path),
                    compareFileFreshness: vi.fn().mockReturnValue(BASE_IS_NEW),
                },
                setting: { currentSettings: vi.fn().mockReturnValue({}) },
                vault: {},
            } as unknown as ServiceFileHandlerDependencies;
            return {
                handler: new TestFileHandler(deps),
                deps,
                stub,
                databaseFileAccess,
                setStorageBody: (next: string) => {
                    body = next;
                },
                /** Overrides the size storage reports, independently of the content it reads. */
                setReportedSize: (size: number | undefined) => {
                    reportedSize = size;
                },
            };
        }

        it("does not delete the source before the target which reads as empty is stored", async () => {
            await withFakeTimers(async () => {
                const { handler, stub, databaseFileAccess, setStorageBody } = createRenameOnAndroidHarness(ANDROID);

                await expect(handler.renameFileInDB(stub(), "old.md" as FilePath)).resolves.toBe(true);
                expect(databaseFileAccess.storeWithBaseRevision).not.toHaveBeenCalled();
                expect(databaseFileAccess.delete).not.toHaveBeenCalled();

                setStorageBody("moved body");
                await vi.advanceTimersByTimeAsync(3_000);

                expect(await storedBodies(databaseFileAccess.storeWithBaseRevision)).toEqual(["moved body"]);
                expect(databaseFileAccess.delete).toHaveBeenCalledExactlyOnceWith("old.md");
                expect(databaseFileAccess.storeWithBaseRevision.mock.invocationCallOrder[0]).toBeLessThan(
                    databaseFileAccess.delete.mock.invocationCallOrder[0]
                );
            });
        });

        it("never stores a target which stays empty over a source with content, and keeps the source", async () => {
            await withFakeTimers(async () => {
                const { handler, stub, databaseFileAccess } = createRenameOnAndroidHarness(ANDROID);

                await expect(handler.renameFileInDB(stub(), "old.md" as FilePath)).resolves.toBe(true);
                await vi.advanceTimersByTimeAsync(2 * CONFIRMATION_WINDOW_MS);

                expect(databaseFileAccess.storeWithBaseRevision).not.toHaveBeenCalled();
                expect(databaseFileAccess.delete).not.toHaveBeenCalled();
            });
        });

        it("deletes an empty source after a target which stays empty is stored", async () => {
            await withFakeTimers(async () => {
                const { handler, stub, databaseFileAccess } = createRenameOnAndroidHarness(ANDROID, "");

                await expect(handler.renameFileInDB(stub(), "old.md" as FilePath)).resolves.toBe(true);
                await vi.advanceTimersByTimeAsync(CONFIRMATION_WINDOW_MS - 1);
                expect(databaseFileAccess.delete).not.toHaveBeenCalled();

                await vi.advanceTimersByTimeAsync(1);
                expect(await storedBodies(databaseFileAccess.storeWithBaseRevision)).toEqual([""]);
                expect(databaseFileAccess.delete).toHaveBeenCalledExactlyOnceWith("old.md");
            });
        });

        it("keeps the source while the target flickers back to empty, and deletes it once the target is stored", async () => {
            await withFakeTimers(async () => {
                const { handler, stub, databaseFileAccess, setStorageBody, setReportedSize } =
                    createRenameOnAndroidHarness(ANDROID);

                await expect(handler.renameFileInDB(stub(), "old.md" as FilePath)).resolves.toBe(true);
                // The check sees a size, but the store which follows still reads nothing.
                setReportedSize(10);
                await vi.advanceTimersByTimeAsync(3_000);
                setReportedSize(undefined);
                expect(databaseFileAccess.storeWithBaseRevision).not.toHaveBeenCalled();
                expect(databaseFileAccess.delete).not.toHaveBeenCalled();

                setStorageBody("moved body");
                await vi.advanceTimersByTimeAsync(3_000);

                expect(await storedBodies(databaseFileAccess.storeWithBaseRevision)).toEqual(["moved body"]);
                expect(databaseFileAccess.delete).toHaveBeenCalledExactlyOnceWith("old.md");
                expect(databaseFileAccess.storeWithBaseRevision.mock.invocationCallOrder[0]).toBeLessThan(
                    databaseFileAccess.delete.mock.invocationCallOrder[0]
                );
            });
        });

        it("keeps the source when the waiting target store is refused", async () => {
            await withFakeTimers(async () => {
                const { handler, stub, databaseFileAccess, setStorageBody } = createRenameOnAndroidHarness(ANDROID);
                databaseFileAccess.storeWithBaseRevision.mockResolvedValue(false);

                await expect(handler.renameFileInDB(stub(), "old.md" as FilePath)).resolves.toBe(true);
                setStorageBody("moved body");
                await vi.advanceTimersByTimeAsync(3_000);

                expect(databaseFileAccess.storeWithBaseRevision).toHaveBeenCalledTimes(1);
                expect(databaseFileAccess.delete).not.toHaveBeenCalled();
            });
        });

        it("keeps the source when the host unloads before the target is stored", async () => {
            await withFakeTimers(async () => {
                const { handler, deps, stub, databaseFileAccess } = createRenameOnAndroidHarness(ANDROID);

                await expect(handler.renameFileInDB(stub(), "old.md" as FilePath)).resolves.toBe(true);
                deps.events.emitEvent(EVENT_PLUGIN_UNLOADED);
                await vi.advanceTimersByTimeAsync(2 * CONFIRMATION_WINDOW_MS);

                expect(databaseFileAccess.storeWithBaseRevision).not.toHaveBeenCalled();
                expect(databaseFileAccess.delete).not.toHaveBeenCalled();
            });
        });

        it("fails a rename after unloading when its target reads as empty, and keeps the source", async () => {
            await withFakeTimers(async () => {
                const { handler, deps, stub, databaseFileAccess } = createRenameOnAndroidHarness(ANDROID);
                deps.events.emitEvent(EVENT_PLUGIN_UNLOADED);

                await expect(handler.renameFileInDB(stub(), "old.md" as FilePath)).resolves.toBe(false);
                await vi.advanceTimersByTimeAsync(2 * CONFIRMATION_WINDOW_MS);

                expect(databaseFileAccess.storeWithBaseRevision).not.toHaveBeenCalled();
                expect(databaseFileAccess.delete).not.toHaveBeenCalled();
                expect(vi.getTimerCount()).toBe(0);
            });
        });

        it.each(OTHER_PLATFORMS)("stores an empty target and deletes the source at once on platform %s", async (p) => {
            const { handler, stub, databaseFileAccess } = createRenameOnAndroidHarness(p);

            await expect(handler.renameFileInDB(stub(), "old.md" as FilePath)).resolves.toBe(true);

            expect(await storedBodies(databaseFileAccess.storeWithBaseRevision)).toEqual([""]);
            expect(databaseFileAccess.delete).toHaveBeenCalledExactlyOnceWith("old.md");
        });
    });

    describe("receiving", () => {
        function createReceivingHandler(platform: string | undefined, reportedSizes: number[], body = "received body") {
            const parts = createHandler("", body, false, TARGET_IS_NEW, true);
            const { storageAccess, deps } = parts;
            Object.assign(deps.API, { getPlatform: () => platform });
            storageAccess.getStub.mockResolvedValue(null);
            const writtenSize = byteLength(body);
            const reported = [...reportedSizes];
            storageAccess.stat.mockImplementation(async () => ({
                ctime: 1,
                mtime: 5,
                size: reported.length > 0 ? reported.shift()! : writtenSize,
                type: "file",
            }));
            return parts;
        }

        it("writes a received file again until it reads with content", async () => {
            await withFakeTimers(async () => {
                const { handler, remoteMeta, storageAccess, provenance } = createReceivingHandler(ANDROID, [0]);

                const reflected = handler.dbToStorage(remoteMeta, null);
                await vi.advanceTimersByTimeAsync(1_000);
                await expect(reflected).resolves.toBe(true);

                expect(storageAccess.writeFileAuto).toHaveBeenCalledTimes(2);
                expect(storageAccess.writeFileAuto).toHaveBeenLastCalledWith("note.md", "received body", {
                    ctime: 1,
                    mtime: 2,
                });
                expect(storageAccess.touched).toHaveBeenCalledTimes(2);
                expect(provenance.set).toHaveBeenLastCalledWith(
                    "note.md",
                    expect.objectContaining({ revision: "2-remote", observedStorageMtime: 5 })
                );
            });
        });

        it("gives up after three rewrites, and the empty read is not published", async () => {
            await withFakeTimers(async () => {
                const { handler, remoteMeta, storageAccess, storageStub, databaseFileAccess } = createReceivingHandler(
                    ANDROID,
                    Array<number>(100).fill(0)
                );
                const storeWithBaseRevision = vi.fn().mockResolvedValue("3-local");
                Object.assign(databaseFileAccess, {
                    storeWithBaseRevision,
                    fetchEntry: vi.fn().mockResolvedValue({ ...remoteMeta, data: "received body" }),
                });

                const reflected = handler.dbToStorage(remoteMeta, null);
                await vi.advanceTimersByTimeAsync(1_000);
                await expect(reflected).resolves.toBe(true);
                expect(storageAccess.writeFileAuto).toHaveBeenCalledTimes(4);

                // The storage event of the write reads the file as empty, through the whole window and after it.
                await expect(handler.storeFileToDB(storageStub)).resolves.toBe(true);
                await vi.advanceTimersByTimeAsync(2 * CONFIRMATION_WINDOW_MS);
                expect(storeWithBaseRevision).not.toHaveBeenCalled();
                expect(databaseFileAccess.storeAsConflictedRevisionWithResult).not.toHaveBeenCalled();
            });
        });

        it("does not write a received file again when it reads with another size than zero", async () => {
            const { handler, remoteMeta, storageAccess } = createReceivingHandler(ANDROID, [5]);

            await expect(handler.dbToStorage(remoteMeta, null)).resolves.toBe(true);

            expect(storageAccess.writeFileAuto).toHaveBeenCalledTimes(1);
        });

        it("neither rewrites nor waits for an empty file it received", async () => {
            await withFakeTimers(async () => {
                const { handler, deps, remoteMeta, storageAccess, storageStub, databaseFileAccess, pathService } =
                    createReceivingHandler(ANDROID, [], "");
                const storeWithBaseRevision = vi.fn();
                Object.assign(databaseFileAccess, {
                    storeWithBaseRevision,
                    fetchEntry: vi.fn().mockResolvedValue({ ...remoteMeta, data: "" }),
                });
                pathService.compareFileFreshness.mockReturnValue(EVEN);

                await expect(handler.dbToStorage(remoteMeta, null)).resolves.toBe(true);
                expect(storageAccess.writeFileAuto).toHaveBeenCalledTimes(1);

                // The storage event of that write is recognised as the expected emptiness and handled at once.
                storageAccess.readStubContent.mockResolvedValue({
                    ...storageStub,
                    stat: { ...storageStub.stat, mtime: 5 },
                    body: createTextBlob(""),
                });
                const checksBefore = storageAccess.stat.mock.calls.length;
                await expect(handler.storeFileToDB(storageStub)).resolves.toBe(true);
                expect(pathService.markChangesAreSame).toHaveBeenCalled();
                await vi.advanceTimersByTimeAsync(CONFIRMATION_WINDOW_MS);
                expect(confirmationLogLines(deps)).toBe(0);
                expect(storageAccess.stat).toHaveBeenCalledTimes(checksBefore);
                expect(storeWithBaseRevision).not.toHaveBeenCalled();
            });
        });

        it.each(OTHER_PLATFORMS)("does not write a received file again on platform %s", async (platform) => {
            const { handler, remoteMeta, storageAccess } = createReceivingHandler(platform, [0]);

            await expect(handler.dbToStorage(remoteMeta, null)).resolves.toBe(true);

            expect(storageAccess.writeFileAuto).toHaveBeenCalledTimes(1);
        });
    });

    describe("incoming changes to a file which reads as empty", () => {
        function createIncomingHandler(platform: string | undefined, localContentIsKnown = false) {
            const parts = createHandler("", "remote update", localContentIsKnown, TARGET_IS_NEW);
            Object.assign(parts.deps.API, { getPlatform: () => platform });
            const setStorageBody = (body: string) => {
                const file = createStorageFile("note.md", body);
                const stub = createStorageStub("note.md", body);
                parts.storageAccess.readStubContent.mockResolvedValue(file);
                parts.storageAccess.getFileStub.mockResolvedValue(stub);
                parts.storageAccess.getStub.mockResolvedValue(stub);
                parts.storageAccess.stat.mockResolvedValue(file.stat);
            };
            // Storage holds what was written, so a written file reads back with its content.
            parts.storageAccess.writeFileAuto.mockImplementation(async (_path: string, data: string) => {
                setStorageBody(data);
                return true;
            });
            return { ...parts, setStorageBody };
        }

        it("holds an incoming revision while the local file reads as empty", async () => {
            await withFakeTimers(async () => {
                const { handler, remoteMeta, storageStub, storageAccess, databaseFileAccess } =
                    createIncomingHandler(ANDROID);

                await expect(handler.dbToStorage(remoteMeta, storageStub)).resolves.toBe(true);
                await vi.advanceTimersByTimeAsync(60_000);

                expect(storageAccess.writeFileAuto).not.toHaveBeenCalled();
                expect(databaseFileAccess.storeAsConflictedRevisionWithResult).not.toHaveBeenCalled();
            });
        });

        it("preserves local content which appears as a conflict before the revision replaces it", async () => {
            await withFakeTimers(async () => {
                const { handler, remoteMeta, storageStub, storageAccess, databaseFileAccess, setStorageBody } =
                    createIncomingHandler(ANDROID);

                await expect(handler.dbToStorage(remoteMeta, storageStub)).resolves.toBe(true);
                setStorageBody("local unsynchronised edit");
                await vi.advanceTimersByTimeAsync(3_000);

                expect(await storedBodies(databaseFileAccess.storeAsConflictedRevisionWithResult)).toEqual([
                    "local unsynchronised edit",
                ]);
                expect(storageAccess.writeFileAuto).not.toHaveBeenCalled();
            });
        });

        it("applies the revision over appeared content which the database already holds", async () => {
            await withFakeTimers(async () => {
                const { handler, remoteMeta, storageStub, storageAccess, databaseFileAccess, setStorageBody } =
                    createIncomingHandler(ANDROID, true);

                await expect(handler.dbToStorage(remoteMeta, storageStub)).resolves.toBe(true);
                setStorageBody("an older synchronised body");
                await vi.advanceTimersByTimeAsync(3_000);

                expect(databaseFileAccess.storeAsConflictedRevisionWithResult).not.toHaveBeenCalled();
                expect(storageAccess.writeFileAuto).toHaveBeenCalledWith("note.md", "remote update", expect.anything());
            });
        });

        it("applies the revision after the window without preserving the empty read", async () => {
            await withFakeTimers(async () => {
                const { handler, remoteMeta, storageStub, storageAccess, databaseFileAccess } =
                    createIncomingHandler(ANDROID);

                await expect(handler.dbToStorage(remoteMeta, storageStub)).resolves.toBe(true);
                await vi.advanceTimersByTimeAsync(CONFIRMATION_WINDOW_MS - 1);
                expect(storageAccess.writeFileAuto).not.toHaveBeenCalled();

                await vi.advanceTimersByTimeAsync(1);
                expect(storageAccess.writeFileAuto).toHaveBeenCalledWith("note.md", "remote update", expect.anything());
                expect(databaseFileAccess.storeAsConflictedRevisionWithResult).not.toHaveBeenCalled();
            });
        });

        it("applies a revision at once over emptiness this device reflected itself", async () => {
            const { handler, remoteMeta, storageStub, storageAccess, databaseFileAccess, deps, records } = (() => {
                const parts = createHandler("", "remote update", false, TARGET_IS_NEW, true);
                Object.assign(parts.deps.API, { getPlatform: () => ANDROID });
                return parts;
            })();
            records.set("note.md", {
                revision: "1-reflected",
                observedStorageMtime: storageStub.stat.mtime,
                reflectedFromDatabase: true,
            } as FileReflectionProvenanceRecord);

            await expect(handler.dbToStorage(remoteMeta, storageStub)).resolves.toBe(true);

            expect(storageAccess.writeFileAuto).toHaveBeenCalledWith("note.md", "remote update", expect.anything());
            expect(databaseFileAccess.storeAsConflictedRevisionWithResult).not.toHaveBeenCalled();
            expect(confirmationLogLines(deps)).toBe(0);
        });

        it("holds a deletion while the local file reads as empty, and applies it after the window", async () => {
            await withFakeTimers(async () => {
                const { handler, remoteMeta, storageStub, storageAccess, databaseFileAccess } =
                    createIncomingHandler(ANDROID);
                const deletion = { ...remoteMeta, deleted: true, size: 0 };
                databaseFileAccess.fetchEntryMeta.mockResolvedValue(deletion);
                const deleteVaultItem = vi.fn(async () => {
                    storageAccess.isExistsIncludeHidden.mockResolvedValue(false);
                });
                Object.assign(storageAccess, { deleteVaultItem });

                await expect(handler.dbToStorage(deletion, storageStub)).resolves.toBe(true);
                await vi.advanceTimersByTimeAsync(60_000);
                expect(deleteVaultItem).not.toHaveBeenCalled();

                await vi.advanceTimersByTimeAsync(CONFIRMATION_WINDOW_MS);
                expect(deleteVaultItem).toHaveBeenCalledExactlyOnceWith("note.md");
                expect(databaseFileAccess.storeAsConflictedRevisionWithResult).not.toHaveBeenCalled();
            });
        });

        it("applies an incoming revision which arrived while a local store waited, and stores nothing empty", async () => {
            await withFakeTimers(async () => {
                const {
                    handler,
                    remoteMeta,
                    storageStub,
                    storageAccess,
                    databaseFileAccess,
                    pathService,
                    setStorageBody,
                } = createIncomingHandler(ANDROID);
                const storeWithBaseRevision = vi.fn().mockResolvedValue("3-local");
                Object.assign(databaseFileAccess, {
                    storeWithBaseRevision,
                    fetchEntry: vi.fn().mockResolvedValue({ ...remoteMeta, data: "remote update" }),
                });
                // Storage holds what was written, and times compare as equal, as they do once a write is reflected.
                storageAccess.writeFileAuto.mockImplementation(async (_path: string, data: string) => {
                    setStorageBody(data);
                    return true;
                });
                pathService.compareFileFreshness.mockReturnValue(EVEN);

                // A local event reads the file as empty, and a revision arrives while that is being confirmed.
                await expect(handler.storeFileToDB(storageStub)).resolves.toBe(true);
                await vi.advanceTimersByTimeAsync(15_000);
                await expect(handler.dbToStorage(remoteMeta, storageStub)).resolves.toBe(true);
                await vi.advanceTimersByTimeAsync(CONFIRMATION_WINDOW_MS);

                expect(storageAccess.writeFileAuto).toHaveBeenCalledExactlyOnceWith(
                    "note.md",
                    "remote update",
                    expect.anything()
                );
                expect(storeWithBaseRevision).not.toHaveBeenCalled();
                expect(databaseFileAccess.storeAsConflictedRevisionWithResult).not.toHaveBeenCalled();
            });
        });

        it("reflects an incoming revision which waited before local content which appears is stored", async () => {
            await withFakeTimers(async () => {
                const { handler, remoteMeta, storageStub, databaseFileAccess, setStorageBody } =
                    createIncomingHandler(ANDROID);
                const preserved = "3-local-preserved";
                const storeWithBaseRevision = vi.fn().mockResolvedValue("4-local");
                Object.assign(databaseFileAccess, {
                    storeWithBaseRevision,
                    findContentRevisions: vi.fn().mockResolvedValue([preserved]),
                    fetchEntry: vi.fn(async (_file: unknown, revision?: string) =>
                        revision === preserved
                            ? {
                                  ...createMeta("note.md", "edited on the phone", preserved),
                                  data: "edited on the phone",
                              }
                            : { ...remoteMeta, data: "remote update" }
                    ),
                });
                databaseFileAccess.storeAsConflictedRevisionWithResult.mockImplementation(async () => {
                    databaseFileAccess.getConflictedRevs.mockResolvedValue([preserved]);
                    return preserved;
                });

                await expect(handler.storeFileToDB(storageStub)).resolves.toBe(true);
                await expect(handler.dbToStorage(remoteMeta, storageStub)).resolves.toBe(true);
                // The user types into the file; its storage event carries content and runs what waited first.
                setStorageBody("edited on the phone");
                await expect(handler.storeFileToDB(createStorageStub("note.md", "edited on the phone"))).resolves.toBe(
                    true
                );

                expect(await storedBodies(databaseFileAccess.storeAsConflictedRevisionWithResult)).toEqual([
                    "edited on the phone",
                ]);
                expect(storeWithBaseRevision).not.toHaveBeenCalled();
            });
        });

        it("stores what storage holds after the revision which waited was reflected, not the earlier read", async () => {
            await withFakeTimers(async () => {
                const {
                    handler,
                    remoteMeta,
                    storageStub,
                    storageAccess,
                    databaseFileAccess,
                    pathService,
                    setStorageBody,
                } = createIncomingHandler(ANDROID, true);
                const storeWithBaseRevision = vi.fn().mockResolvedValue("3-local");
                Object.assign(databaseFileAccess, {
                    storeWithBaseRevision,
                    fetchEntry: vi.fn().mockResolvedValue({ ...remoteMeta, data: "remote update" }),
                });
                pathService.compareFileFreshness.mockReturnValue(EVEN);

                await expect(handler.dbToStorage(remoteMeta, storageStub)).resolves.toBe(true);
                // The file then shows content the database already knew, and its storage event arrives.
                setStorageBody("an older synchronised body");
                await expect(
                    handler.storeFileToDB(createStorageStub("note.md", "an older synchronised body"))
                ).resolves.toBe(true);

                expect(storageAccess.writeFileAuto).toHaveBeenCalledExactlyOnceWith(
                    "note.md",
                    "remote update",
                    expect.anything()
                );
                // The revision is not reverted by storing the older content read before it was reflected.
                expect(storeWithBaseRevision).not.toHaveBeenCalled();
                expect(databaseFileAccess.storeAsConflictedRevisionWithResult).not.toHaveBeenCalled();
            });
        });

        it("does not apply an incoming revision over an empty read after the host has unloaded", async () => {
            await withFakeTimers(async () => {
                const { handler, deps, remoteMeta, storageStub, storageAccess, databaseFileAccess } =
                    createIncomingHandler(ANDROID);
                deps.events.emitEvent(EVENT_PLUGIN_UNLOADED);

                await expect(handler.dbToStorage(remoteMeta, storageStub)).resolves.toBe(false);
                await vi.advanceTimersByTimeAsync(2 * CONFIRMATION_WINDOW_MS);

                expect(vi.getTimerCount()).toBe(0);
                expect(storageAccess.writeFileAuto).not.toHaveBeenCalled();
                expect(databaseFileAccess.storeAsConflictedRevisionWithResult).not.toHaveBeenCalled();
            });
        });

        describe("an explicitly selected revision", () => {
            function createSelectedRevisionHandler(localContentIsKnown: boolean) {
                const parts = createIncomingHandler(ANDROID, localContentIsKnown);
                const { databaseFileAccess, remoteMeta } = parts;
                const selectedMeta = createMeta("note.md", "selected body", "2-selected");
                databaseFileAccess.fetchEntryMeta.mockImplementation(
                    async (_file: UXFileInfoStub | FilePathWithPrefix, revision?: string) =>
                        revision === "2-selected" ? selectedMeta : remoteMeta
                );
                databaseFileAccess.getConflictedRevs.mockResolvedValue(["2-selected"]);
                databaseFileAccess.fetchEntryFromMeta.mockImplementation(async (meta: MetaEntry) => ({
                    ...meta,
                    data: meta._rev === "2-selected" ? "selected body" : "remote update",
                }));
                return parts;
            }

            it("reflects the selected revision over known content which appears, not the winner", async () => {
                await withFakeTimers(async () => {
                    const { handler, storageStub, storageAccess, setStorageBody } = createSelectedRevisionHandler(true);

                    await expect(handler.dbToStorageWithSpecificRev(storageStub, "2-selected")).resolves.toBe(true);
                    expect(storageAccess.writeFileAuto).not.toHaveBeenCalled();

                    setStorageBody("an older synchronised body");
                    await vi.advanceTimersByTimeAsync(3_000);

                    expect(storageAccess.writeFileAuto).toHaveBeenCalledExactlyOnceWith(
                        "note.md",
                        "selected body",
                        expect.anything()
                    );
                });
            });

            it("reflects the selected revision after the window, not the winner", async () => {
                await withFakeTimers(async () => {
                    const { handler, storageStub, storageAccess, databaseFileAccess } =
                        createSelectedRevisionHandler(false);

                    await expect(handler.dbToStorageWithSpecificRev(storageStub, "2-selected")).resolves.toBe(true);
                    await vi.advanceTimersByTimeAsync(CONFIRMATION_WINDOW_MS);

                    expect(storageAccess.writeFileAuto).toHaveBeenCalledExactlyOnceWith(
                        "note.md",
                        "selected body",
                        expect.anything()
                    );
                    expect(databaseFileAccess.storeAsConflictedRevisionWithResult).not.toHaveBeenCalled();
                });
            });
        });

        it.each(OTHER_PLATFORMS)("preserves an emptied local file as a conflict at once on platform %s", async (p) => {
            const { handler, remoteMeta, storageStub, databaseFileAccess } = createIncomingHandler(p);

            await expect(handler.dbToStorage(remoteMeta, storageStub)).resolves.toBe(true);

            expect(await storedBodies(databaseFileAccess.storeAsConflictedRevisionWithResult)).toEqual([""]);
        });
    });
});
