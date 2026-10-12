/**
 * What the journal specs share: a synthetic object store and synthetic devices which synchronise through it.
 *
 * This file holds no tests. Its name keeps it out of the package build, like the specs which use it.
 */
import { vi } from "vitest";
import PouchDB from "pouchdb-core";
import MemoryAdapter from "pouchdb-adapter-memory";
import { BULK_JOURNAL_SUFFIX, JournalSyncCore } from "./JournalSyncCore.ts";
import {
    JournalStorageReadStatuses,
    type IJournalStorage,
    type JournalStorageObject,
    type JournalStorageReadResult,
    type JournalUploadOptions,
} from "./objectstore/JournalStorageAdapter.ts";
import type { LiveSyncJournalReplicatorEnv } from "./LiveSyncJournalReplicatorEnv.ts";
import {
    DEFAULT_SETTINGS,
    type DocumentID,
    type EntryDoc,
    type EntryLeaf,
    type FilePathWithPrefix,
    type ObsidianLiveSyncSettings,
    type PlainEntry,
} from "@lib/common/types.ts";
import { defaultLogger, setGlobalLogFunction } from "@lib/common/logger.ts";
import { type SimpleStore, pickBucketSyncSettings } from "@lib/common/utils.ts";
import { createCheckPointInfoDefault, type CheckPointInfo } from "./JournalSyncTypes.ts";
import { wrappedDeflate, wrappedInflate } from "@lib/pouchdb/compress.ts";
import { createServiceContext } from "@lib/services/base/ServiceBase.ts";

PouchDB.plugin(MemoryAdapter);

export const DOCS_SUFFIX = "-docs.jsonl.gz";
export const UNIT_SPLIT = "\u001f";
export const encoder = new TextEncoder();
export const decoder = new TextDecoder();

/** Whether an object of the remote is a journal. Other objects have names which begin with an underscore. */
const isJournal = (key: string) => !key.startsWith("_");

/**
 * A synthetic object store which, like an S3 listing, reports when each object was stored and how large it is.
 *
 * `timeOf` turns the number of an upload into the place the store gives it among the uploads, so that a test
 * can let uploads share a place as a store with a coarse clock does.
 *
 * `now` is the clock of the store. With one, a listing reports when each object was stored; without one it
 * reports no time, like storage which cannot.
 */
export function createRemote(timeOf: (upload: number) => number = (upload) => upload, now?: () => number) {
    /** What a listing shows. An object can be listed although it is no longer stored. */
    const listed = new Map<string, { place: number; size: number; storedAt?: number }>();
    const stored = new Map<string, Uint8Array>();
    /** Every upload and download, in the order in which they began. */
    const transfers: string[] = [];
    /** Every content each key was written with. */
    const written = new Map<string, Set<string>>();
    const unavailable = new Set<string>();
    /** Every object which was deleted, in order. */
    const deleted: string[] = [];
    let uploads = 0;

    const put = (key: string, data: Uint8Array, size = data.length) => {
        listed.set(key, { place: timeOf(uploads++), size, storedAt: now?.() });
        stored.set(key, data);
    };
    const groups = (): JournalStorageObject[][] => {
        const objects = [...listed].map(([key, object]) => ({ key, ...object }));
        objects.sort((a, b) => a.place - b.place || (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
        const result: JournalStorageObject[][] = [];
        let groupPlace: number | undefined;
        for (const { key, place, size, storedAt } of objects) {
            const object = storedAt === undefined ? { key, size } : { key, size, storedAt };
            if (place === groupPlace) result[result.length - 1].push(object);
            else result.push([object]);
            groupPlace = place;
        }
        return result;
    };
    const objectKeys = () =>
        groups()
            .flat()
            .map(({ key }) => key);
    const downloadWithResult = vi.fn(async (key: string): Promise<JournalStorageReadResult<Uint8Array>> => {
        transfers.push(`download ${key}`);
        if (unavailable.has(key)) {
            return { status: JournalStorageReadStatuses.UNAVAILABLE, error: new Error("synthetic failure") };
        }
        const data = stored.get(key);
        if (data === undefined) return { status: JournalStorageReadStatuses.NOT_FOUND };
        return { status: JournalStorageReadStatuses.AVAILABLE, value: data };
    });
    const store = async (key: string, data: Uint8Array, _mime: string, options?: JournalUploadOptions) => {
        transfers.push(`upload ${key}`);
        if (options?.keepExisting && stored.has(key)) return true;
        const contents = written.get(key) ?? new Set<string>();
        contents.add(Buffer.from(data).toString("base64"));
        written.set(key, contents);
        put(key, data);
        return true;
    };
    /** The uploads of journals. */
    const upload = vi.fn(store);
    /** The uploads of what is stored beside the journals: their indexes, and the removal records. */
    const uploadBeside = vi.fn(store);
    const deleteFiles = vi.fn(async (keys: string[]) => {
        for (const key of keys) {
            listed.delete(key);
            stored.delete(key);
            deleted.push(key);
        }
        return true;
    });
    const storage = {
        upload: (key: string, data: Uint8Array, mime: string, options?: JournalUploadOptions) =>
            (isJournal(key) ? upload : uploadBeside)(key, data, mime, options),
        downloadWithResult,
        download: vi.fn(async (key: string) => {
            const result = await downloadWithResult(key);
            return result.status === JournalStorageReadStatuses.AVAILABLE ? result.value : false;
        }),
        listFiles: vi.fn(async () => [...listed.keys()]),
        listFilesInUploadOrder: vi.fn(async () => groups().map((group) => group.map(({ key }) => key))),
        listObjectsInUploadOrder: vi.fn(async () => groups()),
        deleteFiles,
        isAvailable: vi.fn(async () => true),
        getUsage: vi.fn(async () => false as const),
        applyNewConfig: vi.fn(),
    } satisfies IJournalStorage;

    return {
        storage,
        upload,
        uploadBeside,
        downloadWithResult,
        deleteFiles,
        transfers,
        written,
        unavailable,
        deleted,
        put,
        groups,
        /** Remove an object which a listing still shows, as when it is deleted after it was listed. */
        removeButKeepListed: (key: string) => stored.delete(key),
        /** Every object, in upload order. */
        objectKeys,
        /** Every journal, in upload order. */
        keys: () => objectKeys().filter(isJournal),
        dataOf: (key: string) => stored.get(key)!,
        /** When the store received the object, by its own clock. */
        storedAtOf: (key: string) => listed.get(key)?.storedAt,
        sizeOf: (key: string) => listed.get(key)?.size,
    };
}

export type Remote = ReturnType<typeof createRemote>;

const openDatabases: PouchDB.Database<EntryDoc>[] = [];
let databaseCounter = 0;

/** A synthetic device with its own database and its own key-value store, on the given storage. */
export function createDevice(name: string, storage: IJournalStorage) {
    // A device keeps the last hundred revisions of a document, as the application does.
    const db = new PouchDB<EntryDoc>(`journal_spec_${name}_${++databaseCounter}`, {
        adapter: "memory",
        revs_limit: 100,
    });
    openDatabases.push(db);
    const context = createServiceContext();
    /** What the device keeps between synchronisations, by key. */
    const entries = new Map<string, unknown>();
    const store = {
        get: vi.fn(async (key: string) => structuredClone(entries.get(key))),
        set: vi.fn(async (key: string, value: unknown) => {
            entries.set(key, structuredClone(value));
        }),
        delete: vi.fn(async (key: string) => {
            entries.delete(key);
        }),
        keys: vi.fn(async (from: string | undefined, to: string | undefined) =>
            [...entries.keys()]
                .filter((key) => (from === undefined || key >= from) && (to === undefined || key <= to))
                .sort()
        ),
    } as unknown as SimpleStore<CheckPointInfo>;
    const statics = {
        value: {
            sent: 0,
            arrived: 0,
            maxPullSeq: 0,
            maxPushSeq: 0,
            lastSyncPullSeq: 0,
            lastSyncPushSeq: 0,
            syncStatus: "NOT_CONNECTED",
        },
    };
    /** What the test changes about the device: its settings, and whether it is a mobile device. */
    const host = { settings: {} as Partial<ObsidianLiveSyncSettings>, isMobile: false };
    /** Every revision handed on to be applied to storage, in order. */
    const handedOn: string[] = [];
    const env = {
        services: {
            context,
            API: { isMobile: () => host.isMobile },
            database: { localDatabase: { localDatabase: db } },
            setting: {
                currentSettings: () => ({ ...DEFAULT_SETTINGS, ...host.settings }),
                getDeviceAndVaultName: () => `synthetic-device-${name}`,
            },
            replicator: { replicationStatics: statics },
            replication: {
                parseSynchroniseResult: async (docs: PouchDB.Core.ExistingDocument<EntryDoc>[]) => {
                    handedOn.push(...docs.map((doc) => `${doc._id}@${doc._rev}`));
                    return true;
                },
            },
            keyValueDB: { simpleStore: store },
        },
    } as unknown as LiveSyncJournalReplicatorEnv;
    const settings = pickBucketSyncSettings(DEFAULT_SETTINGS);
    const core = new JournalSyncCore(settings, store, env, storage);
    // Time stands still unless a test moves it, so no result depends on how fast the test runs.
    core.now = () => 0;

    /** Every document revision saved from a journal, in order. */
    const saved: string[] = [];
    const bulkDocs = db.bulkDocs.bind(db) as (...args: unknown[]) => unknown;
    // The database also calls this itself, with other arguments, so every argument is passed on as it came.
    vi.spyOn(db, "bulkDocs").mockImplementation(((...args: unknown[]) => {
        const [docs, options] = args as [unknown, { new_edits?: boolean } | undefined];
        if (Array.isArray(docs) && options?.new_edits === false) {
            saved.push(...(docs as { _id: string; _rev?: string }[]).map((doc) => `${doc._id}@${doc._rev}`));
        }
        return bulkDocs(...args);
    }) as never);

    return {
        db,
        core,
        env,
        settings,
        context,
        store,
        entries,
        host,
        statics,
        handedOn,
        saved,
        get checkpoint(): CheckPointInfo {
            return (
                (entries.get(`bucketsync-checkpoint-${core.hash}`) as CheckPointInfo) ?? createCheckPointInfoDefault()
            );
        },
        /** Another client on the same store, such as the maintenance pane. */
        createPaneClient: () => new JournalSyncCore(settings, store, env, storage),
    };
}

export type Device = ReturnType<typeof createDevice>;

/** Close what the devices of a test opened. Each spec calls this after every test. */
export async function closeDevices(): Promise<void> {
    setGlobalLogFunction(defaultLogger);
    await Promise.all(openDatabases.splice(0).map((db) => db.destroy()));
}

export const chunkId = (name: string) => `h:${name}` as DocumentID;
export const chunk = (name: string, data: string) => ({ _id: chunkId(name), type: "leaf", data }) as EntryLeaf;
/** Content of `bytes` bytes which differs per name. */
export const contentOf = (name: string, bytes: number) => name.padEnd(bytes, "x");
export const largeChunk = (name: string, bytes = 20 * 1024) => chunk(name, contentOf(name, bytes));
export const note = (id: string, children: string[] = [], mtime = 1) =>
    ({
        _id: id as DocumentID,
        type: "plain",
        path: id as FilePathWithPrefix,
        children: children.map(chunkId),
        ctime: 1,
        mtime,
        size: 0,
        eden: {},
    }) as PlainEntry;

/** The entries of a stored journal: the IDs of its chunks and of its documents. */
export async function entriesOf(remote: Remote, key: string) {
    const text = decoder.decode(await wrappedInflate(remote.dataOf(key) as Uint8Array<ArrayBuffer>, {}));
    const lines = text.split("\n").filter((line) => line !== "");
    return {
        chunks: lines.filter((line) => line.startsWith("~")).map((line) => line.slice(1).split(UNIT_SPLIT)[0]),
        documents: lines
            .filter((line) => !line.startsWith("~"))
            .map((line) => (JSON.parse(line) as { _id: string })._id),
    };
}

/** Every entry stored in the journals of the remote, with the number of journals which hold it. */
export async function storedEntries(remote: Remote) {
    const counts = new Map<string, number>();
    for (const key of remote.keys()) {
        const { chunks, documents } = await entriesOf(remote, key);
        for (const id of [...chunks, ...documents]) counts.set(id, (counts.get(id) ?? 0) + 1);
    }
    return counts;
}

export const isBulkKey = (key: string) => key.endsWith(BULK_JOURNAL_SUFFIX);

export const chunkLine = (name: string, data: string) => `~${chunkId(name)}${UNIT_SPLIT}${data}\n`;
export const documentLine = (id: string, children: string[] = [], generation = 1) =>
    `${JSON.stringify({
        ...note(id, children),
        _rev: `${generation}-rev${generation}`,
        _revisions: { start: generation, ids: [`rev${generation}`] },
    })}\n`;
export const journalOf = async (...lines: string[]) => await wrappedDeflate(encoder.encode(lines.join("")), {});
/** Journal names are opaque, so tests pick names whose order differs from the order of their uploads. */
export const docsKey = (letter: string) => `${letter.repeat(64)}${DOCS_SUFFIX}`;
export const bulkKey = (letter: string) => `${letter.repeat(64)}${BULK_JOURNAL_SUFFIX}`;
export const hasChunk = async (device: Device, name: string) =>
    await device.db.get(chunkId(name)).then(
        () => true,
        () => false
    );
