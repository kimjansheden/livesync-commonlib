import { afterEach, describe, expect, it, vi } from "vitest";
import PouchDB from "pouchdb-core";
import MemoryAdapter from "pouchdb-adapter-memory";
import replication from "pouchdb-replication";
import type { DocumentID, EntryDoc, FilePathWithPrefix, LoadedEntry, MetaEntry, UXFileInfo } from "@lib/common/types";
import type { BinaryContentAvailability } from "@lib/interfaces/DatabaseFileAccess";
import { createLiveSyncEventHub } from "@lib/hub/hub";
import {
    ServiceDatabaseFileAccessBase,
    type ServiceDatabaseFileAccessDependencies,
} from "./ServiceDatabaseFileAccessBase";
import { storeDeletionByPathAtRevision } from "@lib/managers/EntryManager/EntryManagerImpls";
import { EVENT_FILE_SAVED } from "@lib/events/coreEvents";

PouchDB.plugin(MemoryAdapter);
PouchDB.plugin(replication);

let databaseSequence = 0;

function createEntry(id: DocumentID, path: FilePathWithPrefix, content: string): EntryDoc {
    return {
        _id: id,
        path,
        type: "plain",
        datatype: "plain",
        data: [content],
        ctime: 1,
        mtime: 1,
        size: content.length,
        children: [],
        eden: {},
    } as unknown as EntryDoc;
}

type RevisionReader = (
    path: FilePathWithPrefix,
    options?: PouchDB.Core.GetOptions,
    dump?: boolean,
    waitForReady?: boolean,
    includeDeleted?: boolean
) => Promise<LoadedEntry | false>;

function createService(database: PouchDB.Database<EntryDoc>, id: DocumentID, readRevision?: RevisionReader) {
    const getEntry = async (_path: FilePathWithPrefix, options?: PouchDB.Core.GetOptions) => {
        try {
            return (await database.get(id, options)) as LoadedEntry;
        } catch {
            return false;
        }
    };
    const dependencies = {
        events: createLiveSyncEventHub(),
        API: { addLog: vi.fn() },
        vault: { isTargetFile: vi.fn().mockResolvedValue(true) },
        storageAccess: {},
        path: {},
        database: {
            localDatabase: {
                getDBEntryMeta: getEntry,
                getDBEntry: readRevision ?? getEntry,
                getRaw: (documentId: DocumentID, options?: PouchDB.Core.GetOptions) =>
                    database.get(documentId, options),
            },
        },
    } as unknown as ServiceDatabaseFileAccessDependencies;
    return new ServiceDatabaseFileAccessBase(dependencies);
}

describe("ServiceDatabaseFileAccessBase.hasContentInRevisionHistory", () => {
    const databases: PouchDB.Database<EntryDoc>[] = [];

    afterEach(async () => {
        await Promise.all(databases.splice(0).map((database) => database.destroy()));
    });

    it("recognises content from a resolved losing branch as synchronised history", async () => {
        databaseSequence += 1;
        const source = new PouchDB<EntryDoc>(`revision-history-source-${databaseSequence}`, { adapter: "memory" });
        const target = new PouchDB<EntryDoc>(`revision-history-target-${databaseSequence}`, { adapter: "memory" });
        databases.push(source, target);

        const path = "note.md" as FilePathWithPrefix;
        const id = "note.md" as DocumentID;
        await source.put(createEntry(id, path, "base"));
        await source.replicate.to(target);

        const sourceBase = await source.get(id);
        const targetBase = await target.get(id);
        await source.put({ ...sourceBase, data: ["edited on source"], mtime: 2 });
        await target.put({ ...targetBase, data: ["edited on target"], mtime: 3 });
        await source.replicate.to(target);

        const conflicted = await target.get(id, { conflicts: true });
        expect(conflicted._conflicts).toHaveLength(1);
        const losingRev = conflicted._conflicts![0];
        const losingEntry = await target.get(id, { rev: losingRev });
        const losingContent = Array.isArray(losingEntry.data) ? losingEntry.data.join("") : losingEntry.data;

        await target.remove(id, losingRev);
        const resolved = await target.get(id, { conflicts: true });
        expect(resolved._conflicts).toBeUndefined();

        const service = createService(target, id);
        await expect(service.hasContentInRevisionHistory(path, losingContent, resolved._rev)).resolves.toBe(true);
        const winningContent = Array.isArray(resolved.data) ? resolved.data.join("") : resolved.data;
        await expect(service.hasContentInRevisionHistory(path, winningContent, resolved._rev)).resolves.toBe(true);
        await expect(
            service.hasContentInRevisionHistory(path, "a new unsynchronised local edit", resolved._rev)
        ).resolves.toBe(false);
    });

    it("stops after matching current content before an unreadable older revision", async () => {
        databaseSequence += 1;
        const database = new PouchDB<EntryDoc>(`revision-short-circuit-${databaseSequence}`, {
            adapter: "memory",
        });
        databases.push(database);

        const path = "short-circuit.md" as FilePathWithPrefix;
        const id = "short-circuit.md" as DocumentID;
        await database.put(createEntry(id, path, "older content"));
        const base = await database.get(id);
        const current = await database.put({ ...base, data: ["current content"], mtime: 2 });
        const readRevision = vi.fn<RevisionReader>(async (_path, options) => {
            if (options?.rev !== current.rev) {
                return false;
            }
            return (await database.get(id, { rev: current.rev })) as LoadedEntry;
        });
        const service = createService(database, id, readRevision);

        await expect(service.hasContentInRevisionHistory(path, "current content", current.rev)).resolves.toBe(true);

        expect(readRevision).toHaveBeenCalledTimes(1);
        expect(readRevision).toHaveBeenCalledWith(path, { rev: current.rev }, false, true, true);
    });

    it("returns every available revision with exactly matching content", async () => {
        databaseSequence += 1;
        const source = new PouchDB<EntryDoc>(`revision-match-source-${databaseSequence}`, { adapter: "memory" });
        const target = new PouchDB<EntryDoc>(`revision-match-target-${databaseSequence}`, { adapter: "memory" });
        databases.push(source, target);

        const path = "same.md" as FilePathWithPrefix;
        const id = "same.md" as DocumentID;
        await source.put(createEntry(id, path, "base"));
        await source.replicate.to(target);
        const sourceBase = await source.get(id);
        const targetBase = await target.get(id);
        await source.put({ ...sourceBase, data: ["same content"], mtime: 2 });
        await target.put({ ...targetBase, data: ["same content"], mtime: 3 });
        await source.replicate.to(target);

        const conflicted = await target.get(id, { conflicts: true });
        expect(conflicted._conflicts).toHaveLength(1);
        const service = createService(target, id);

        await expect(service.findContentRevisions(path, "same content", conflicted._rev)).resolves.toEqual(
            expect.arrayContaining([conflicted._rev, conflicted._conflicts![0]])
        );
    });

    it("stores a conflict-time deletion as a visible logical child of the selected revision", async () => {
        databaseSequence += 1;
        const source = new PouchDB<EntryDoc>(`revision-delete-source-${databaseSequence}`, { adapter: "memory" });
        const target = new PouchDB<EntryDoc>(`revision-delete-target-${databaseSequence}`, { adapter: "memory" });
        databases.push(source, target);

        const path = "deleted.md" as FilePathWithPrefix;
        const id = "deleted.md" as DocumentID;
        await source.put(createEntry(id, path, "base"));
        await source.replicate.to(target);
        const sourceBase = await source.get(id);
        const targetBase = await target.get(id);
        await source.put({ ...sourceBase, data: ["source edit"], mtime: 2 });
        await target.put({ ...targetBase, data: ["displayed edit"], mtime: 3 });
        await source.replicate.to(target);

        const conflicted = await target.get(id, { conflicts: true });
        const displayedRevision = conflicted._conflicts![0];
        const host = {
            services: {
                path: { path2id: vi.fn().mockResolvedValue(id) },
                setting: {
                    currentSettings: vi.fn().mockReturnValue({
                        syncInternalFiles: true,
                        syncOnlyRegEx: "",
                        syncIgnoreRegEx: "",
                    }),
                },
            },
            serviceModules: {},
        };

        const result = await storeDeletionByPathAtRevision(
            host as never,
            { localDatabase: target },
            path,
            displayedRevision
        );

        expect(result).not.toBe(false);
        const response = result as PouchDB.Core.Response;
        const deletion = await target.get(id, { rev: response.rev, revs: true });
        expect(deletion.deleted).toBe(true);
        expect(deletion._deleted).not.toBe(true);
        expect(deletion._revisions?.ids[1]).toBe(displayedRevision.split("-")[1]);
        const after = await target.get(id, { conflicts: true });
        expect([after._rev, ...(after._conflicts ?? [])]).toContain(response.rev);
        expect(after._conflicts).toHaveLength(1);
        const service = createService(target, id);
        await expect(service.findContentRevisions(path, "displayed edit", response.rev)).resolves.not.toContain(
            response.rev
        );
    });

    it("does not invent a sibling base for a generation-one revision", async () => {
        databaseSequence += 1;
        const database = new PouchDB<EntryDoc>(`revision-generation-one-${databaseSequence}`, {
            adapter: "memory",
        });
        databases.push(database);

        const path = "root.md" as FilePathWithPrefix;
        const id = "root.md" as DocumentID;
        const root = await database.put({
            ...createEntry(id, path, ""),
            children: ["h:missing"],
        });
        const service = createService(database, id);
        const storeWithBaseRevision = vi.spyOn(service, "storeWithBaseRevision");
        const storageFile = {
            path,
            name: "root.md",
            stat: {
                ctime: 2,
                mtime: 2,
                size: 12,
                type: "file",
            },
            body: new Blob(["local bytes"]),
        } as UXFileInfo;

        await expect(service.storeAsConflictedRevisionWithResult(storageFile, root.rev, true)).resolves.toBe(false);

        expect(storeWithBaseRevision).not.toHaveBeenCalled();
        const after = await database.get(id, { conflicts: true });
        expect(after._rev).toBe(root.rev);
        expect(after._conflicts).toBeUndefined();
    });
});

describe("ServiceDatabaseFileAccessBase.isRevisionInHistory", () => {
    const databases: PouchDB.Database<EntryDoc>[] = [];

    afterEach(async () => {
        await Promise.all(databases.splice(0).map((database) => database.destroy()));
    });

    it("recognises the revisions a deletion was made on, and no revision of another branch", async () => {
        databaseSequence += 1;
        const local = new PouchDB<EntryDoc>(`revision-ancestry-local-${databaseSequence}`, { adapter: "memory" });
        const other = new PouchDB<EntryDoc>(`revision-ancestry-other-${databaseSequence}`, { adapter: "memory" });
        databases.push(local, other);

        const path = "archive.zip" as FilePathWithPrefix;
        const id = "archive.zip" as DocumentID;
        const base = await local.put(createEntry(id, path, "base"));
        await local.replicate.to(other);
        const otherBase = await other.get(id);
        const sibling = await other.put({ ...otherBase, data: ["edited elsewhere"], mtime: 2 });
        const localBase = await local.get(id);
        const own = await local.put({ ...localBase, data: ["written by this device"], mtime: 3 });
        const ownEntry = await local.get(id);
        const deletion = await local.put({ ...ownEntry, deleted: true, mtime: 4 } as EntryDoc);
        await other.replicate.to(local);
        const service = createService(local, id);

        await expect(service.isRevisionInHistory(path, own.rev, deletion.rev)).resolves.toBe(true);
        await expect(service.isRevisionInHistory(path, base.rev, deletion.rev)).resolves.toBe(true);
        await expect(service.isRevisionInHistory(path, deletion.rev, deletion.rev)).resolves.toBe(true);
        await expect(service.isRevisionInHistory(path, sibling.rev, deletion.rev)).resolves.toBe(false);
        await expect(service.isRevisionInHistory(path, own.rev, sibling.rev)).resolves.toBe(false);
        await expect(service.isRevisionInHistory(path, own.rev, "9-unknown")).resolves.toBe(false);
    });

    it("recognises the revision a tombstone was made on", async () => {
        databaseSequence += 1;
        const database = new PouchDB<EntryDoc>(`revision-ancestry-tombstone-${databaseSequence}`, {
            adapter: "memory",
        });
        databases.push(database);

        const path = "archive.zip" as FilePathWithPrefix;
        const id = "archive.zip" as DocumentID;
        const base = await database.put(createEntry(id, path, "base"));
        const baseEntry = await database.get(id);
        const own = await database.put({ ...baseEntry, data: ["written by this device"], mtime: 3 });
        const tombstone = await database.remove(id, own.rev);
        await expect(database.get(id)).rejects.toMatchObject({ status: 404 });
        const service = createService(database, id);

        await expect(service.isRevisionInHistory(path, own.rev, tombstone.rev)).resolves.toBe(true);
        await expect(service.isRevisionInHistory(path, base.rev, tombstone.rev)).resolves.toBe(true);
        await expect(service.isRevisionInHistory(path, "2-other", tombstone.rev)).resolves.toBe(false);
    });
});

describe("ServiceDatabaseFileAccessBase.inspectBinaryContentFromMeta", () => {
    it("reports what the local database finds of an entry's chunks, waiting for their delivery unless told not to", async () => {
        const inspectDBEntryBinaryContent = vi.fn(async (): Promise<BinaryContentAvailability> => "missing");
        const service = new ServiceDatabaseFileAccessBase({
            events: createLiveSyncEventHub(),
            API: { addLog: vi.fn() },
            vault: {},
            storageAccess: {},
            path: {},
            database: { localDatabase: { inspectDBEntryBinaryContent } },
        } as unknown as ServiceDatabaseFileAccessDependencies);
        const meta = {
            _id: "archive.zip",
            path: "archive.zip",
            children: ["h:archive"],
            size: 700_000_000,
            type: "newnote",
        } as unknown as MetaEntry;

        await expect(service.inspectBinaryContentFromMeta(meta)).resolves.toBe("missing");
        await expect(service.inspectBinaryContentFromMeta(meta, false)).resolves.toBe("missing");

        expect(inspectDBEntryBinaryContent.mock.calls).toEqual([
            [meta, true],
            [meta, false],
        ]);
    });
});

describe("ServiceDatabaseFileAccessBase.storeWithLiveBaseRevision", () => {
    it("returns the exact child revision and emits the saved event only after a successful conditional write", async () => {
        const path = "restored.md" as FilePathWithPrefix;
        const id = "restored.md" as DocumentID;
        const events = createLiveSyncEventHub();
        const emitEvent = vi.spyOn(events, "emitEvent");
        const putDBEntryWithLiveBaseRevision = vi.fn().mockResolvedValue({
            ok: true,
            id,
            rev: "3-restored",
        });
        const service = new ServiceDatabaseFileAccessBase({
            events,
            API: { addLog: vi.fn() },
            vault: {},
            storageAccess: {},
            path: { path2id: vi.fn().mockResolvedValue(id) },
            database: {
                localDatabase: { putDBEntryWithLiveBaseRevision },
            },
        } as unknown as ServiceDatabaseFileAccessDependencies);
        const file = {
            path,
            name: "restored.md",
            stat: {
                ctime: 1,
                mtime: 2,
                size: 8,
                type: "file",
            },
            body: new Blob(["restored"]),
        } as UXFileInfo;

        await expect(service.storeWithLiveBaseRevision(file, "2-deleted", true)).resolves.toBe("3-restored");

        expect(putDBEntryWithLiveBaseRevision).toHaveBeenCalledWith(
            expect.objectContaining({ _id: id, path }),
            "2-deleted",
            false
        );
        expect(emitEvent).toHaveBeenCalledWith(EVENT_FILE_SAVED);

        putDBEntryWithLiveBaseRevision.mockResolvedValueOnce(false);
        emitEvent.mockClear();
        await expect(service.storeWithLiveBaseRevision(file, "2-deleted", true)).resolves.toBe(false);
        expect(emitEvent).not.toHaveBeenCalled();
    });
});

describe("ServiceDatabaseFileAccessBase.storeWithBaseRevision", () => {
    it("routes an independent creation to the root-sibling writer", async () => {
        const path = "independent.md" as FilePathWithPrefix;
        const putDBEntryWithBaseRevision = vi.fn().mockResolvedValue({
            ok: true,
            id: path,
            rev: "1-independent",
        });
        const putDBEntry = vi.fn();
        const service = new ServiceDatabaseFileAccessBase({
            events: createLiveSyncEventHub(),
            API: { addLog: vi.fn() },
            vault: {},
            storageAccess: {},
            path: { path2id: vi.fn().mockResolvedValue(path) },
            database: { localDatabase: { putDBEntryWithBaseRevision, putDBEntry } },
        } as unknown as ServiceDatabaseFileAccessDependencies);
        const file = {
            path,
            name: "independent.md",
            stat: { ctime: 1, mtime: 2, size: 5, type: "file" },
            body: new Blob(["local"]),
        } as UXFileInfo;

        await expect(service.storeWithBaseRevision(file, undefined, true)).resolves.toBe("1-independent");

        expect(putDBEntryWithBaseRevision).toHaveBeenCalledWith(expect.objectContaining({ path }), undefined, false);
        expect(putDBEntry).not.toHaveBeenCalled();
    });
});

describe("ServiceDatabaseFileAccessBase.storeContent", () => {
    const path = "merged.md" as FilePathWithPrefix;

    function createService() {
        const putDBEntry = vi.fn().mockResolvedValue({ ok: true, id: path, rev: "3-current" });
        const putDBEntryWithLiveBaseRevision = vi.fn().mockResolvedValue({ ok: true, id: path, rev: "3-merged" });
        const service = new ServiceDatabaseFileAccessBase({
            events: createLiveSyncEventHub(),
            API: { addLog: vi.fn() },
            vault: { isTargetFile: vi.fn().mockResolvedValue(true) },
            storageAccess: {},
            path: { path2id: vi.fn().mockResolvedValue(path) },
            database: { localDatabase: { putDBEntry, putDBEntryWithLiveBaseRevision } },
        } as unknown as ServiceDatabaseFileAccessDependencies);
        return { service, putDBEntry, putDBEntryWithLiveBaseRevision };
    }

    it("stores content as a child of an exact revision with the given times", async () => {
        const { service, putDBEntry, putDBEntryWithLiveBaseRevision } = createService();

        await expect(
            service.storeContent(path, "merged content", { revision: "2-winner", ctime: 5, mtime: 7 })
        ).resolves.toBe(true);

        expect(putDBEntryWithLiveBaseRevision).toHaveBeenCalledWith(
            expect.objectContaining({ path, ctime: 5, mtime: 7, size: 14 }),
            "2-winner",
            false
        );
        expect(putDBEntry).not.toHaveBeenCalled();
    });

    it("reports content which could not be stored on an exact revision any more", async () => {
        const { service, putDBEntryWithLiveBaseRevision } = createService();
        putDBEntryWithLiveBaseRevision.mockResolvedValue(false);

        await expect(
            service.storeContent(path, "merged content", { revision: "2-superseded", ctime: 5, mtime: 7 })
        ).resolves.toBe(false);
    });

    it("stores content on the current revision with the current time otherwise", async () => {
        const { service, putDBEntry, putDBEntryWithLiveBaseRevision } = createService();
        const before = Date.now();

        await expect(service.storeContent(path, "edited content")).resolves.toBe(true);

        const [stored] = putDBEntry.mock.calls[0] as [{ ctime: number; mtime: number }];
        expect(stored.mtime).toBeGreaterThanOrEqual(before);
        expect(stored.ctime).toBeGreaterThanOrEqual(before);
        expect(putDBEntryWithLiveBaseRevision).not.toHaveBeenCalled();
    });
});
