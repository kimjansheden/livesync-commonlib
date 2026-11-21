import { describe, expect, it, vi } from "vitest";
import type { FileEventItem, FilePathWithPrefix, MetaEntry, UXFileInfo, UXFileInfoStub } from "@lib/common/types";
import { REMOTE_MINIO } from "@lib/common/types";
import { compareMTime, createTextBlob } from "@lib/common/utils";
import type { SimpleStore } from "@lib/common/utils";
import { StoredFileReflectionProvenance } from "@lib/interfaces/FileReflectionProvenance";
import { createLiveSyncEventHub } from "@lib/hub/hub";
import { syncFileBetweenDBandStorage } from "@lib/serviceFeatures/offlineScanner";
import { ServiceFileHandlerBase, type ServiceFileHandlerDependencies } from "./ServiceFileHandlerBase";

class TestFileHandler extends ServiceFileHandlerBase {}

const PATH = "notes/packing.md" as FilePathWithPrefix;

type Revision = { rev: string; parent?: string; body: string; mtime: number; deleted?: boolean };

function byteLength(text: string) {
    return new Blob([text]).size;
}

/**
 * A revision tree of one document which behaves like the local database for the operations the handler uses:
 * a conditional store is refused once its base is no longer a leaf, and a forced store always adds a child.
 */
function createRevisionTree() {
    const revisions = new Map<string, Revision>();
    let created = 0;
    const generation = (rev: string) => Number(rev.split("-")[0]);
    const leaves = () => {
        const parents = new Set([...revisions.values()].map((revision) => revision.parent));
        return [...revisions.values()].filter((revision) => !parents.has(revision.rev));
    };
    const liveLeaves = () => leaves().filter((revision) => !revision.deleted);
    const winner = () =>
        liveLeaves().sort((a, b) => generation(b.rev) - generation(a.rev) || (a.rev < b.rev ? 1 : -1))[0];
    const add = (parent: string | undefined, body: string, mtime: number) => {
        const rev = `${parent ? generation(parent) + 1 : 1}-r${++created}`;
        revisions.set(rev, { rev, parent, body, mtime });
        return rev;
    };
    const meta = (revision: Revision): MetaEntry =>
        ({
            _id: "notes/packing.md",
            _rev: revision.rev,
            path: PATH,
            ctime: 1,
            mtime: revision.mtime,
            size: byteLength(revision.body),
            children: [],
            datatype: "plain",
            type: "plain",
            eden: {},
            ...(revision.deleted ? { deleted: true } : {}),
        }) as unknown as MetaEntry;
    const resolve = (rev?: string) => (rev === undefined ? winner() : revisions.get(rev));
    const isInHistory = (rev: string, branch: string) => {
        for (let current: string | undefined = branch; current; current = revisions.get(current)?.parent) {
            if (current === rev) return true;
        }
        return false;
    };
    const bodyOf = async (file: UXFileInfo) => await file.body.text();
    const databaseFileAccess = {
        fetchEntryMeta: vi.fn(async (_file: unknown, rev?: string) => {
            const revision = resolve(rev);
            return revision ? meta(revision) : false;
        }),
        fetchEntry: vi.fn(async (_file: unknown, rev?: string) => {
            const revision = resolve(rev);
            return revision ? { ...meta(revision), data: revision.body } : false;
        }),
        fetchEntryFromMeta: vi.fn(async (entry: MetaEntry) => {
            const revision = revisions.get(entry._rev!);
            return revision ? { ...meta(revision), data: revision.body } : false;
        }),
        getConflictedRevs: vi.fn(async () => {
            const current = winner();
            return liveLeaves()
                .filter((revision) => revision !== current)
                .map((revision) => revision.rev);
        }),
        storeWithLiveBaseRevision: vi.fn(async (file: UXFileInfo, base: string | undefined) => {
            if (base === undefined) return winner() ? false : add(undefined, await bodyOf(file), file.stat.mtime);
            if (!leaves().some((revision) => revision.rev === base)) return false;
            return add(base, await bodyOf(file), file.stat.mtime);
        }),
        storeWithBaseRevision: vi.fn(async (file: UXFileInfo, base: string | undefined) =>
            add(base, await bodyOf(file), file.stat.mtime)
        ),
        storeAsConflictedRevisionWithResult: vi.fn(async (file: UXFileInfo, current: string) => {
            const parent = revisions.get(current)?.parent;
            return parent ? add(parent, await bodyOf(file), file.stat.mtime) : false;
        }),
        hasContentInRevisionHistory: vi.fn(async (_file: unknown, content: Blob | string, current?: string) => {
            const text = typeof content === "string" ? content : await content.text();
            for (let rev = current; rev; rev = revisions.get(rev)?.parent) {
                if (revisions.get(rev)?.body === text) return true;
            }
            return false;
        }),
        isRevisionInHistory: vi.fn(async (_file: unknown, rev: string, branch: string) => isInHistory(rev, branch)),
        findContentRevisions: vi.fn(async (_file: unknown, content: Blob) => {
            const text = await content.text();
            return liveLeaves()
                .filter((revision) => revision.body === text)
                .map((revision) => revision.rev);
        }),
    };
    return {
        databaseFileAccess,
        add,
        leaves: liveLeaves,
        winner,
        parentOf: (rev: string) => revisions.get(rev)?.parent,
        metaOf: (rev: string) => meta(revisions.get(rev)!),
    };
}

/** One file in storage, which the user edits by `save`. */
function createStorage() {
    let content: string | undefined;
    let mtime = 0;
    const stat = () => (content === undefined ? null : { ctime: 1, mtime, size: byteLength(content), type: "file" });
    const stub = (): UXFileInfoStub | null =>
        content === undefined ? null : ({ name: "packing.md", path: PATH, stat: stat()! } as UXFileInfoStub);
    // When set, the next content read takes its snapshot and then waits, as a slow read of a busy device does.
    let pausedRead: Promise<void> | undefined;
    const storageAccess = {
        normalisePath: (path: string) => path,
        isExistsIncludeHidden: vi.fn(async () => content !== undefined),
        statHidden: vi.fn(async () => stat()),
        stat: vi.fn(async () => stat()),
        getStub: vi.fn(async () => stub()),
        getFileStub: vi.fn(async () => stub()),
        readStubContent: vi.fn(async () => {
            const snapshot = stub() && ({ ...stub()!, body: createTextBlob(content!) } as UXFileInfo);
            const pause = pausedRead;
            pausedRead = undefined;
            if (pause) await pause;
            return snapshot;
        }),
        ensureDir: vi.fn(async () => undefined),
        writeFileAuto: vi.fn(async (_path: string, data: string, options: { mtime: number }) => {
            content = data;
            mtime = options.mtime;
            return true;
        }),
        touched: vi.fn(async () => undefined),
        triggerFileEvent: vi.fn(),
        renameFile: vi.fn(),
    };
    return {
        storageAccess,
        save: (text: string, at: number) => {
            content = text;
            mtime = at;
        },
        content: () => content,
        stub: () => stub()!,
        pauseNextRead: () => {
            let release!: () => void;
            pausedRead = new Promise<void>((resolve) => (release = resolve));
            return release;
        },
    };
}

function createMapStore() {
    const values = new Map<string, unknown>();
    const store = {
        get: async (key: string) => values.get(key),
        set: async (key: string, value: unknown) => {
            values.set(key, value);
        },
        delete: async (key: string) => {
            values.delete(key);
        },
        keys: async () => [...values.keys()],
        db: undefined,
    } as unknown as SimpleStore<never>;
    return { store, values };
}

function createDevice(provenanceStore = createMapStore().store) {
    const tree = createRevisionTree();
    const storage = createStorage();
    const conflict = { queueCheckFor: vi.fn(async () => undefined), queueCheckForIfOpen: vi.fn(async () => undefined) };
    const mtimeOf = (item: { stat?: { mtime: number }; mtime?: number }) => item.stat?.mtime ?? item.mtime ?? 0;
    const path = {
        getPath: (entry: MetaEntry) => entry.path,
        path2id: async (value: string) => value.toLowerCase(),
        compareFileFreshness: (base: never, target: never) => compareMTime(mtimeOf(base), mtimeOf(target)),
        markChangesAreSame: vi.fn(),
    };
    const vault = { isTargetFile: async () => true, isFileSizeTooLarge: () => false, isValidPath: () => true };
    const setting = { currentSettings: () => ({ remoteType: REMOTE_MINIO, writeDocumentsIfConflicted: false }) };
    const addLog = vi.fn();
    const deps = {
        events: createLiveSyncEventHub(),
        API: { addLog, getPlatform: () => "android-app" },
        databaseFileAccess: tree.databaseFileAccess,
        storageAccess: storage.storageAccess,
        fileProcessing: { processFileEvent: { addHandler: vi.fn() } },
        replication: { processSynchroniseResult: { addHandler: vi.fn() } },
        conflict,
        path,
        setting,
        vault,
        fileReflectionProvenance: new StoredFileReflectionProvenance(provenanceStore),
    } as unknown as ServiceFileHandlerDependencies;
    const handler = new TestFileHandler(deps);
    const registered = <T>(service: { addHandler: unknown }) =>
        vi.mocked(service.addHandler as (handler: T) => void).mock.calls[0][0];
    const processFileEvent = registered<(item: FileEventItem) => Promise<boolean>>(
        deps.fileProcessing.processFileEvent
    );
    const processReplicatedDoc = registered<(entry: MetaEntry) => Promise<boolean>>(
        deps.replication.processSynchroniseResult
    );
    /** The storage event of the latest save, handled as the host's storage event queue does. */
    const storageEvent = () =>
        processFileEvent({ type: "CHANGED", args: { file: storage.stub() } } as unknown as FileEventItem);
    const scannerHost = {
        services: { path, setting, vault, context: { events: { emitEvent: vi.fn() } } },
        serviceModules: { fileHandler: handler, storageAccess: storage.storageAccess },
    } as unknown as Parameters<typeof syncFileBetweenDBandStorage>[0];
    return { handler, tree, storage, conflict, addLog, storageEvent, processReplicatedDoc, scannerHost };
}

/** Fixture text typed into one note in four quick saves, each continuing the previous one. */
const TYPED = [
    "# Packing\n\n- tent\n",
    "# Packing\n\n- tent\n- stove\n",
    "# Packing\n\n- tent\n- stove\n- map of the ridge\n",
    "# Packing\n\n- tent\n- stove\n- map of the ridge\n- two litres of water\n",
    "# Packing\n\n- tent\n- stove\n- map of the ridge\n- two litres of water each\n",
];

/** Type the first four states with a storage event each; the device's own chain ends at the returned revision. */
async function typeOwnChain(device: ReturnType<typeof createDevice>) {
    const times = [24_000, 34_000, 40_000, 42_000];
    for (let i = 0; i < times.length; i++) {
        device.storage.save(TYPED[i], times[i]);
        await expect(device.storageEvent()).resolves.toBe(true);
    }
    expect(device.tree.leaves()).toHaveLength(1);
    return device.tree.winner().rev;
}

/** The device's saves form one chain: a single live leaf holding `body`, made on `parent`, and no forced write. */
async function expectSingleChainEndingIn(device: ReturnType<typeof createDevice>, body: string, parent: string) {
    const leaves = device.tree.leaves();
    expect(leaves.map((leaf) => leaf.body)).toEqual([body]);
    expect(device.tree.parentOf(leaves[0].rev)).toBe(parent);
    expect(device.conflict.queueCheckFor).not.toHaveBeenCalled();
    await expect(device.handler.getForcedSiblingWrites()).resolves.toEqual([]);
}

describe("ServiceFileHandlerBase own consecutive saves of one file", () => {
    it("extends its own latest revision when the offline scanner reflects it while storage is a save ahead", async () => {
        const device = createDevice();
        const own = await typeOwnChain(device);
        // The scan listed storage while it still showed an earlier save, and the database after the latest store.
        const listedFile = { ...device.storage.stub(), stat: { ...device.storage.stub().stat, mtime: 38_000 } };
        const listedDoc = device.tree.metaOf(own);
        // The next save reaches storage; its storage event is still queued.
        device.storage.save(TYPED[4], 45_000);

        await syncFileBetweenDBandStorage(device.scannerHost, vi.fn(), listedFile, listedDoc);
        await expect(device.storageEvent()).resolves.toBe(true);

        expect(device.storage.content()).toBe(TYPED[4]);
        await expectSingleChainEndingIn(device, TYPED[4], own);
    });

    it("extends its own latest revision when a queued reflection of it runs while storage is a save ahead", async () => {
        const device = createDevice();
        const own = await typeOwnChain(device);
        device.storage.save(TYPED[4], 45_000);

        // A reflection which a scan queued for another attempt arrives through the replication result queue.
        await expect(device.processReplicatedDoc(device.tree.metaOf(own))).resolves.toBe(true);
        await expect(device.storageEvent()).resolves.toBe(true);

        expect(device.storage.content()).toBe(TYPED[4]);
        await expectSingleChainEndingIn(device, TYPED[4], own);
    });

    it("keeps its record of a store made while a reflection of the previous revision was reading storage", async () => {
        const device = createDevice();
        device.storage.save(TYPED[0], 24_000);
        await device.storageEvent();
        device.storage.save(TYPED[1], 40_000);
        await device.storageEvent();
        const previous = device.tree.winner().rev;

        // A reflection of the current revision reads storage, which still shows it, and is held up there.
        const release = device.storage.pauseNextRead();
        const reflecting = device.handler.dbToStorage(PATH, PATH as never);
        await vi.waitFor(() => expect(device.storage.storageAccess.readStubContent).toHaveBeenCalledTimes(3));
        // Meanwhile the user saves, and that save is stored before the reflection goes on.
        device.storage.save(TYPED[2], 42_000);
        await expect(device.storageEvent()).resolves.toBe(true);
        const own = device.tree.winner().rev;
        expect(device.tree.parentOf(own)).toBe(previous);
        release();
        await expect(reflecting).resolves.toBe(true);
        device.storage.save(TYPED[3], 45_000);
        await expect(device.storageEvent()).resolves.toBe(true);

        await expectSingleChainEndingIn(device, TYPED[3], own);
    });

    it("stores nothing beside a newer own revision when a reflection of an older one arrives late", async () => {
        const device = createDevice();
        const own = await typeOwnChain(device);
        const older = device.tree.parentOf(own)!;
        // The reflection read the older revision before the latest store; storage already holds the latest one.
        device.tree.databaseFileAccess.fetchEntryMeta.mockImplementationOnce(async () => device.tree.metaOf(older));

        await expect(device.handler.dbToStorage(PATH, PATH as never)).resolves.toBe(true);

        expect(device.storage.content()).toBe(TYPED[3]);
        expect(device.tree.leaves().map((leaf) => leaf.rev)).toEqual([own]);
        expect(device.conflict.queueCheckFor).not.toHaveBeenCalled();
        await expect(device.handler.getForcedSiblingWrites()).resolves.toEqual([]);
    });
});

describe("ServiceFileHandlerBase revisions from another device", () => {
    it("still keeps a local change as a conflict beside a foreign revision which is reflected", async () => {
        const device = createDevice();
        const own = await typeOwnChain(device);
        device.storage.save(TYPED[4], 45_000);
        // Another device extended the same revision before this device stored its latest save.
        const foreign = device.tree.add(own, `${TYPED[3]}- matches\n`, 44_000);

        await expect(device.processReplicatedDoc(device.tree.metaOf(foreign))).resolves.toBe(true);

        expect(device.storage.content()).toBe(TYPED[4]);
        const leaves = device.tree.leaves();
        expect(leaves.map((leaf) => leaf.body).sort()).toEqual([TYPED[4], `${TYPED[3]}- matches\n`].sort());
        expect(leaves.every((leaf) => device.tree.parentOf(leaf.rev) === own)).toBe(true);
        expect(device.conflict.queueCheckFor).toHaveBeenCalledWith(PATH);
        const writes = await device.handler.getForcedSiblingWrites();
        expect(writes).toEqual([
            expect.objectContaining({
                path: PATH,
                reason: "reflection-preserve-local-change",
                siblingOf: foreign,
                winnerRevision: foreign,
                storedRevision: leaves.find((leaf) => leaf.body === TYPED[4])!.rev,
            }),
        ]);
    });

    it("still keeps a local change as a conflict beside a foreign revision which storage does not show yet", async () => {
        const device = createDevice();
        const own = await typeOwnChain(device);
        const foreign = device.tree.add(own, `${TYPED[3]}- matches\n`, 44_000);
        device.storage.save(TYPED[4], 45_000);

        await expect(device.storageEvent()).resolves.toBe(true);

        const leaves = device.tree.leaves();
        expect(leaves.map((leaf) => leaf.body).sort()).toEqual([TYPED[4], `${TYPED[3]}- matches\n`].sort());
        expect(leaves.every((leaf) => device.tree.parentOf(leaf.rev) === own)).toBe(true);
        expect(device.conflict.queueCheckFor).toHaveBeenCalledWith(PATH);
        await expect(device.handler.getForcedSiblingWrites()).resolves.toEqual([
            expect.objectContaining({
                reason: "beside-unreflected-winner",
                baseRevision: own,
                winnerRevision: foreign,
            }),
        ]);
    });
});

describe("ServiceFileHandlerBase record of forced sibling writes", () => {
    it("records each forced write once, logs it, and keeps it across a restart of the handler", async () => {
        const { store } = createMapStore();
        const device = createDevice(store);
        const own = await typeOwnChain(device);
        const foreign = device.tree.add(own, `${TYPED[3]}- matches\n`, 44_000);
        device.storage.save(TYPED[4], 45_000);
        await device.storageEvent();

        const written = await device.handler.getForcedSiblingWrites();
        expect(written).toHaveLength(1);
        expect(written[0]).toEqual({
            path: PATH,
            reason: "beside-unreflected-winner",
            baseRevision: own,
            winnerRevision: foreign,
            storedRevision: expect.stringMatching(/^5-/),
            at: expect.any(Number),
        });
        const logged = device.addLog.mock.calls.filter(([message]) => String(message).includes("Forced sibling write"));
        expect(logged).toHaveLength(1);

        const restarted = createDevice(store);
        await expect(restarted.handler.getForcedSiblingWrites()).resolves.toEqual(written);
    });
});
