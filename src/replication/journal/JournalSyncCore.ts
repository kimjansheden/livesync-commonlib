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
import {
    ASSESSMENT_MARKER_PREFIX,
    type CheckPointInfo,
    createCheckPointInfoDefault,
    BULK_JOURNAL_SUFFIX,
    chunksInUseBy,
    isChunkEntry,
    isRemovalInTime,
    JOURNAL_INDEX_PREFIX,
    journalIndexKey,
    type JournalIndex,
    type JournalReliance,
    parseJournalReliance,
    parseRemovalRecord,
    parseRemoteChangeMarker,
    REMOTE_CHANGE_MARKER_KEY,
    REMOTE_FULL_LIST_INTERVAL_MS,
    type RelianceDocument,
    REMOVAL_RECORD_PREFIX,
    REMOVAL_RECORD_SUFFIX,
    type RemovalRecord,
    sha256Hex,
} from "./JournalSyncTypes.ts";
import { JournalCleaner } from "./JournalCleaner.ts";
import type { LiveSyncJournalReplicatorEnv } from "./LiveSyncJournalReplicatorEnv.ts";
import {
    JournalStorageReadStatuses,
    type IJournalStorage,
    type JournalStorageObject,
    type JournalStorageReadResult,
    type JournalUploadOptions,
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
export { BULK_JOURNAL_SUFFIX };
const te = new TextEncoder();
/** A chunk whose serialised size is at least this is sent in a journal of large chunks. Text chunks are normally smaller. */
export const BULK_CHUNK_MIN_BYTES = 16 * 1024;
/** How long a synchronisation goes on starting journals of large chunks after it has sent the local changes. */
export const BULK_RECEIVE_BUDGET_MS = 15_000;
/** A receive of large chunks ends after this many journals in a row failed, as the connection is probably lost. */
const BULK_RECEIVE_MAX_FAILURES_IN_A_ROW = 2;

/** How long a sending device remembers a document which it sent in reliance on chunks in the remote. */
export const RELIANCE_MEMORY_MS = 60 * 60 * 1000;

/** The most chunks which a send reads from the local database at once, to send them along with a document. */
export const LOCAL_CHUNK_READ_MAX_COUNT = 50;
/**
 * About how much content such a read may bring into memory. The size of a chunk is not known before it is read,
 * so a send reads one chunk first, and then as many at once as stay within this at the size of the largest chunk
 * it has read so far.
 */
export const LOCAL_CHUNK_READ_MAX_BYTES = 4 * 1024 * 1024;
/**
 * The least time between two writes of the checkpoint which nothing else calls for: those which only store how
 * far a send has looked, and those which only register what was received.
 *
 * The checkpoint is one record, which grows with everything a device has received and sent, and every write
 * stores all of it. What is left unwritten meanwhile is done again after the application was cut off: the same
 * known changes are passed again, and the same journals are fetched and applied again.
 */
export const CHECKPOINT_WRITE_INTERVAL_MS = 30 * 1000;

/** Whether `interval` has passed since `since`. A clock which went backwards counts as passed. */
function hasPassed(since: number, now: number, interval: number): boolean {
    const elapsed = now - since;
    return elapsed < 0 || elapsed >= interval;
}

/**
 * How often the checkpoint under a key was written by any client in this process.
 *
 * Several clients can share one checkpoint, for example the replicator and the maintenance pane, which resets the
 * history while replication runs. A client which holds the checkpoint in memory compares this number to tell
 * whether another client has written since, without reading the record. It is kept by the name of the record, as
 * the lock which serialises its updates is.
 */
const checkpointWrites = new Map<string, number>();

/** What a receive has registered in memory and not yet written, and since when. */
type UnwrittenProgress = { knownIDs: string[]; receivedFiles: string[]; since: number };
/** The checkpoint as a client holds it in memory while a transfer runs. */
type HeldCheckpoint = { key: string; info: CheckPointInfo; writes: number; unwritten?: UnwrittenProgress };

/**
 * The key under which the checkpoint counts a revision as sent or received: the ID and the revision, and for a
 * chunk the ID alone, as a chunk is named after its content.
 */
function docKeyOf(id: string, rev: string | undefined): string {
    return id.startsWith("h:") ? id : `${id}-${rev}`;
}

/** What a send has learnt of the size of the chunks it reads. */
type ChunkReadState = { largestBytes?: number };
/** Chunks read from the local database in one go, with the IDs which were asked for. */
type ChunkSlice = { ids: string[]; chunks: EntryLeaf[] };
/** Reads the next slice of a list of chunks, or nothing once the list is read. */
type ChunkSliceReader = () => Promise<ChunkSlice | undefined>;
/**
 * Changes on their way into journals, with the sequence up to which they are complete. A pack which asks for the
 * position to be stored holds no change: everything up to its sequence is known or was handed on before it.
 */
type SendPack = { changes: EntryDoc[]; packLastSeq: number | string; storesPosition?: true };

/** A document in a journal which was sent without some of its chunks, because the remote was taken to hold them. */
type ReliedDocument = Pick<RelianceDocument, "id" | "rev">;
/** Entries collected for one journal. */
type SendBuffer = { rows: Uint8Array[]; binarySize: number; sentIDs: string[]; relied: ReliedDocument[] };
/** One journal to upload, with the sequence the checkpoint may advance to once it is stored. */
type JournalUnit = {
    bin: Uint8Array<ArrayBuffer>;
    packLastSeq: number;
    sentIDs: string[];
    isBulk: boolean;
    relied: ReliedDocument[];
};
/** A journal to upload, or only a sequence to store as the position once every journal before it is stored. */
type SendUnit = JournalUnit | { packLastSeq: number; positionOnly: true };
/**
 * What a listing of the remote shows.
 *
 * `journals` holds every journal, in the groups and order in which they were uploaded. Objects whose names begin
 * with an underscore are not journals: among them are the indexes of journals of large chunks and the removal
 * records.
 */
export type RemoteListing = {
    journals: JournalStorageObject[][];
    /** Whether the storage reported sizes, without which no journal is fetched out of upload order. */
    hasSizes: boolean;
    /** The journals which have an index object. */
    indexed: Set<string>;
    /** The removal records, each with the time at which the storage received it when the storage reports that. */
    removalRecords: JournalStorageObject[];
    /** The assessment markers, likewise. */
    assessmentMarkers: JournalStorageObject[];
};

type CachedRemoteListing = {
    listing: RemoteListing;
    /** `undefined` means that the marker could not be read when this listing was made. */
    markerToken: string | null | undefined;
    listedAt: number;
};
/** What a receive of the journals with documents leaves to do: the journals of large chunks, and its listing. */
type DeferredReceive = { resetGeneration: number; journals: string[]; listing: RemoteListing };

/** Whether an object of the remote is a journal. Other objects have names which begin with an underscore. */
function isJournalKey(key: string): boolean {
    return !key.startsWith("_");
}

/**
 * Whether a revision of a document can have left large chunks without a use: a deletion, or a version of a file
 * which is large enough to have such chunks, and may have replaced another.
 */
function mayLeaveChunksUnused(doc: object): boolean {
    const entry = doc as { _deleted?: boolean; deleted?: boolean; size?: unknown };
    if (entry._deleted === true || entry.deleted === true) return true;
    return typeof entry.size === "number" && entry.size >= BULK_CHUNK_MIN_BYTES;
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
    /** Removes journals of large chunks which nothing uses any more, when the device is set to. */
    private readonly cleaner = new JournalCleaner(this);
    env: LiveSyncJournalReplicatorEnv;
    store: SimpleStore<CheckPointInfo>;
    requestedStop = false;
    /** The last full listing and the marker which was read immediately before it. */
    private cachedRemoteListing: CachedRemoteListing | undefined;

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

    /** Upload a small object as JSON, deflated and encrypted like a journal. */
    async uploadPackedJson(key: string, body: unknown, options?: JournalUploadOptions): Promise<boolean> {
        try {
            const bin = await wrappedDeflate(te.encode(JSON.stringify(body)), { consume: true, level: 8 });
            const encrypted = await this.encryptForUpload(key, bin, this.currentSettings);
            return await this.storage.upload(key, encrypted, "application/octet-stream", options);
        } catch (ex) {
            Logger(`Could not upload ${key}`, LOG_LEVEL_VERBOSE);
            Logger(ex, LOG_LEVEL_VERBOSE);
            return false;
        }
    }

    /** Download an object which {@link uploadPackedJson} stored. Content which cannot be read is unavailable. */
    async downloadPackedJson(key: string): Promise<JournalStorageReadResult<unknown>> {
        try {
            const result = await this.storage.downloadWithResult(key, true);
            if (result.status !== JournalStorageReadStatuses.AVAILABLE) return result;
            const data = await this.decryptDownloaded(
                key,
                result.value as Uint8Array<ArrayBuffer>,
                this.currentSettings
            );
            const text = new TextDecoder().decode(await wrappedInflate(new Uint8Array(data), { consume: true }));
            return { status: JournalStorageReadStatuses.AVAILABLE, value: JSON.parse(text) as unknown };
        } catch (ex) {
            return { status: JournalStorageReadStatuses.UNAVAILABLE, error: ex };
        }
    }

    /**
     * The store of this client, for what it keeps beside the checkpoint. Each kind of entry has a key of its own,
     * which names the remote.
     */
    keyValueStore<T>(): SimpleStore<T> {
        return this.store as unknown as SimpleStore<T>;
    }

    private get relianceKey(): string {
        return `bucketsync-reliance-${this.hash}`;
    }

    private async _getReliance(): Promise<JournalReliance> {
        return parseJournalReliance(await this.keyValueStore<unknown>().get(this.relianceKey));
    }

    /** Change what the sending side remembers. An update which returns what it was given stores nothing. */
    private async _updateReliance(update: (old: JournalReliance) => JournalReliance): Promise<void> {
        const key = this.relianceKey;
        await serialized(key, async () => {
            const old = await this._getReliance();
            const updated = update(old);
            if (updated !== old) await this.keyValueStore<JournalReliance>().set(key, updated);
        });
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
            this.cachedRemoteListing = undefined;
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
        const checkPointKey = this.checkPointKey;
        // Other clients share this store, for example the maintenance pane which resets the history while
        // replication runs. Serialising the read-modify-write keeps one update from discarding another.
        return await serialized(checkPointKey, async () => {
            const old = await this._loadCheckpoint();
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
            await this._writeCheckpoint(checkPointKey, newInfo);
            return newInfo;
        });
    }

    private get checkPointKey(): string {
        return `bucketsync-checkpoint-${this.hash}`;
    }

    /** The checkpoint in memory, while a transfer of this client runs or is about to. */
    private _heldCheckpoint: HeldCheckpoint | undefined;
    /** How many running transfers hold the checkpoint in memory. */
    private _checkpointHolders = 0;
    /** When the check before a cycle read the checkpoint for the transfer which follows it. */
    private _checkpointKeptSince: number | undefined;
    /** The local-change check has read the checkpoint for the connectivity check of this cycle. */
    private _checkpointPreflightPending = false;

    /**
     * Whether a copy of the checkpoint in memory may be used: while a transfer runs, and for a short while after
     * the check before a cycle has read it, until the transfer of that cycle begins.
     */
    private get _keepsCheckpoint(): boolean {
        if (this._checkpointHolders > 0) return true;
        const since = this._checkpointKeptSince;
        return since !== undefined && !hasPassed(since, this.now(), CHECKPOINT_WRITE_INTERVAL_MS);
    }

    /**
     * Run a transfer which reads the checkpoint from the store at most once, however much it transfers.
     *
     * The store is the source of truth: another client can change the record, and a reset of the local database
     * empties the store beneath this client. Outside a cycle every access therefore reads the record. Inside one
     * the record is read once and then held in memory, which is safe because every client in this process counts
     * its writes in {@link checkpointWrites}: a write by another client is seen at the next access, and the
     * record is read again. When the last transfer ends, what was only registered in memory is written, and the
     * copy is given up, so that the next cycle begins with what the store holds.
     */
    private async _holdingCheckpoint<T>(task: () => Promise<T>): Promise<T> {
        // A copy which has waited too long for its transfer is not the one to go on with.
        if (!this._keepsCheckpoint) this._heldCheckpoint = undefined;
        this._checkpointHolders++;
        try {
            return await task();
        } finally {
            await this._releaseCheckpoint();
        }
    }

    /**
     * End one transfer's hold on the checkpoint. The last transfer to end writes what was only registered in
     * memory, while it still holds the copy, and then gives the copy up.
     */
    private async _releaseCheckpoint(): Promise<void> {
        const checkPointKey = this.checkPointKey;
        // Transfers can end in the same moment. Each leaves as it decides, under the lock, so that one of them
        // finds itself the last.
        let hasLeft = false;
        const leave = () => {
            if (!hasLeft) this._checkpointHolders--;
            hasLeft = true;
        };
        try {
            await serialized(checkPointKey, async () => {
                // Another transfer goes on with the copy, and writes what is left when it ends.
                if (this._checkpointHolders > 1) return leave();
                if (this._heldCheckpoint?.unwritten) {
                    // Reading through the copy drops what was registered before a reset by another client.
                    const info = await this._loadCheckpoint();
                    if (this._heldCheckpoint?.unwritten) await this._writeCheckpoint(checkPointKey, info);
                }
                // The copy can be as large as everything the device has received, and is not kept in memory.
                this._heldCheckpoint = undefined;
                this._checkpointKeptSince = undefined;
                this._checkpointPreflightPending = false;
                leave();
            });
        } catch (ex) {
            // What was not written is received again, so this is no failure of the transfer.
            this._heldCheckpoint = undefined;
            this._checkpointKeptSince = undefined;
            this._checkpointPreflightPending = false;
            Logger(`The journal checkpoint could not be written; what was received is registered again later`);
            Logger(ex, LOG_LEVEL_VERBOSE);
        } finally {
            leave();
        }
    }

    /**
     * The checkpoint, for a caller which holds the lock of its record: the copy in memory when no client has
     * written since it was read, and otherwise the record of the store.
     *
     * What this client registered in memory and has not written is kept across a write by another client, unless
     * that write was a reset of the history: nothing from before a reset is recorded after it.
     */
    private async _loadCheckpoint(): Promise<CheckPointInfo> {
        const key = this.checkPointKey;
        const keeps = this._keepsCheckpoint;
        const held = keeps && this._heldCheckpoint?.key === key ? this._heldCheckpoint : undefined;
        // The count is taken before the record is read. A write which comes in between is then seen as a change at
        // the next access, never missed.
        const writes = checkpointWrites.get(key) ?? 0;
        if (held && held.writes === writes) return held.info;
        const info = await this._readCheckpointRecord(key);
        const unwritten =
            held?.unwritten && held.info.resetGeneration === info.resetGeneration ? held.unwritten : undefined;
        if (unwritten) {
            setAllItems(info.knownIDs, unwritten.knownIDs);
            setAllItems(info.receivedFiles, unwritten.receivedFiles);
        }
        this._currentCheckPointInfo = info;
        this._heldCheckpoint = keeps ? { key, info, writes, unwritten } : undefined;
        return info;
    }

    /** Write the whole record. Counting the write first makes every copy in memory read again, also if it fails. */
    private async _writeCheckpoint(key: string, info: CheckPointInfo): Promise<void> {
        const writes = (checkpointWrites.get(key) ?? 0) + 1;
        checkpointWrites.set(key, writes);
        this._currentCheckPointInfo = info;
        await this.store.set(key, info);
        if (this._heldCheckpoint?.key === key) this._heldCheckpoint = { key, info, writes };
    }

    /**
     * Register what a receive has stored: the journals it received and the entries which came with them.
     *
     * This only adds to the checkpoint, so it need not be written at once: while a transfer holds the checkpoint
     * in memory it is written at most every {@link CHECKPOINT_WRITE_INTERVAL_MS}, when the transfer ends, and
     * with any other write of the record. What is on disk can therefore lag behind what was received, and never
     * runs ahead of it. A journal whose registration was not written is fetched and applied again, which stores
     * nothing twice.
     *
     * @param resetGeneration when given, the progress is only registered while the checkpoint still has this
     * reset generation.
     * @returns whether it was registered.
     */
    private async _registerReceived(
        resetGeneration: number | undefined,
        { knownIDs = [], receivedFiles = [] }: { knownIDs?: string[]; receivedFiles?: string[] }
    ): Promise<boolean> {
        const checkPointKey = this.checkPointKey;
        return await serialized(checkPointKey, async () => {
            const info = await this._loadCheckpoint();
            if (resetGeneration !== undefined && info.resetGeneration !== resetGeneration) return false;
            if (knownIDs.length === 0 && receivedFiles.length === 0) return true;
            setAllItems(info.knownIDs, knownIDs);
            setAllItems(info.receivedFiles, receivedFiles);
            // Only a running transfer writes later: its end is when the rest is written.
            const held = this._checkpointHolders > 0 ? this._heldCheckpoint : undefined;
            if (!held) {
                await this._writeCheckpoint(checkPointKey, info);
                return true;
            }
            const unwritten = (held.unwritten ??= { knownIDs: [], receivedFiles: [], since: this.now() });
            for (const id of knownIDs) unwritten.knownIDs.push(id);
            for (const file of receivedFiles) unwritten.receivedFiles.push(file);
            if (hasPassed(unwritten.since, this.now(), CHECKPOINT_WRITE_INTERVAL_MS)) {
                await this._writeCheckpoint(checkPointKey, info);
            }
            return true;
        });
    }

    _currentCheckPointInfo = createCheckPointInfoDefault();

    /**
     * The checkpoint. During a cycle of this client it is read from the store once and then answered from memory;
     * see {@link _holdingCheckpoint}.
     */
    async getCheckpointInfo(): Promise<CheckPointInfo> {
        const key = this.checkPointKey;
        const held = this._keepsCheckpoint ? this._heldCheckpoint : undefined;
        if (held && held.key === key && held.writes === (checkpointWrites.get(key) ?? 0)) return held.info;
        return await serialized(key, async () => await this._loadCheckpoint());
    }

    /** Read the whole record from the store. */
    private async _readCheckpointRecord(checkPointKey: string): Promise<CheckPointInfo> {
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
        return { ...createCheckPointInfoDefault(), ...old };
    }

    resetAllCaches(): void {
        clearHandlers();
        this.syncParametersRefreshedForNextCycle = false;
    }

    async resetCheckpointInfo() {
        await this._updateCheckPointInfo(() => createCheckPointInfoDefault(), true);
        // With an empty history this device receives every journal again, so what kept it from removing content
        // from the remote no longer holds.
        await this.cleaner.forgetWhyDatabaseIsNotTrusted();
        this._forgetBulkJournals();
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
     *
     * A host calls this before the transfer of a cycle, so this is also where the checkpoint is read for the
     * cycle: from the store, whatever this client held before. The copy is kept for the transfer which follows,
     * so that the check and the transfer read the record once between them.
     */
    async ensureCheckpointCachesAreFresh(): Promise<void> {
        const alreadyRefreshed = this.syncParametersRefreshedForNextCycle;
        this.syncParametersRefreshedForNextCycle = false;
        const params = await this.getSyncParamsHandler().fetch(!alreadyRefreshed);
        if (!params) throw new SyncParamsFetchError("Could not read remote sync parameters");
        const journalEpoch = this.getJournalEpochFromSyncParams(params);

        if (this._checkpointHolders === 0) {
            // A local-change check belongs to this same preflight. Its fresh read can be reused; a second
            // connectivity check starts a new cycle and must read the store again. Writes by another client
            // invalidate the held copy through checkpointWrites, including a history reset between these checks.
            if (!this._checkpointPreflightPending || !this._keepsCheckpoint) this._heldCheckpoint = undefined;
            this._checkpointKeptSince = this.now();
            this._checkpointPreflightPending = false;
        }

        const current = await this.getCheckpointInfo();
        if (current.journalEpoch === journalEpoch) {
            return;
        }
        // An epoch names the identity of the journal set. Even when an old sent file remains, or there is no sent
        // history with which to prove a wipe, a listing cached for the earlier epoch cannot describe this one.
        this.cachedRemoteListing = undefined;

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
            if (remoteWipeConfirmed) {
                // A journal which only held chunks nothing used any more may have been removed alone, by a device
                // whose removal record this device has not read yet. The remote was wiped only when none of the
                // journals this device sent is left.
                const listed = new Set(await this.storage.listFiles(""));
                remoteWipeConfirmed = ![...current.sentFiles].some((key) => listed.has(key));
            }
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
        this._forgetBulkJournals();
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
        this.cachedRemoteListing = undefined;
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
        // Chunks which must be sent again are no change of the local database, but they wait to be sent.
        if ((await this._getReliance()).resend.length > 0) return true;
        if (this._checkpointHolders === 0) {
            // Begin the local preflight from the store, even if the previous check did not start a transfer.
            this._heldCheckpoint = undefined;
            this._checkpointKeptSince = this.now();
            this._checkpointPreflightPending = true;
        }
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
        return docKeyOf(doc._id, doc._rev);
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
            // sent.  Per-revision deduplication is handled correctly by the filter below.
        });
        const allChanges = await allChangesTask;
        if (allChanges.results.length == 0) {
            return { changes: [], hasNext: false, packLastSeq: allChanges.last_seq };
        }
        // Each leaf revision is looked up by its key before any document is read. A device which has received
        // very much passes all of that here, and reading a document for every known revision would make what it
        // costs to find its own changes grow with everything it has received.
        const revisions = allChanges.results.flatMap((e) => e.changes.map((change) => ({ id: e.id, rev: change.rev })));
        const unknown = revisions.filter(({ id, rev }) => {
            const key = docKeyOf(id, rev);
            return !checkPointInfo.knownIDs.has(key) && !checkPointInfo.sentIDs.has(key);
        });
        knownKeyCount = revisions.length - unknown.length;
        const packLastSeq = allChanges.last_seq;
        const dbInfo = await this.db.info();
        const hasNext = packLastSeq < dbInfo.update_seq;
        if (unknown.length === 0) return { changes: [], hasNext, packLastSeq };
        const bd = await this.db.bulkGet({ docs: unknown, revs: true });
        const docChanges = bd.results
            .flatMap((e) => e.docs)
            .filter((e) => "ok" in e)
            .map((e) => e.ok as EntryDoc);
        // A per-revision read error is not evidence that the remote has the revision. Advancing the pack's
        // sequence after dropping it would permanently skip an unsent change, so retry this batch instead.
        const loadedRevisions = new Set(docChanges.map((doc) => docKeyOf(doc._id, doc._rev)));
        if (unknown.some(({ id, rev }) => !loadedRevisions.has(docKeyOf(id, rev)))) {
            throw new Error("A local journal revision could not be read");
        }
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
        scan: { lastScannedSeq: number },
        /** Chunks to send before the local changes. They carry no sequence, so they do not move the checkpoint. */
        resendIDs: string[]
    ) {
        let currentLastSeq = startSeq;
        // A long run of changes which are all known uploads nothing, and so would store no position before the
        // pass ends. A pass which the operating system cuts off would then begin at the same place every time.
        let positionStoredAt = this.now();
        const readState: ChunkReadState = {};
        // The chunks which this send reads from the local database: those to send again, and those which a
        // document brings along. A chunk is read once.
        const brought = new Set<string>(resendIDs);
        const isInRemote = (id: string) =>
            this._currentCheckPointInfo.knownIDs.has(id) || this._currentCheckPointInfo.sentIDs.has(id);
        /**
         * The chunks which the documents of a pack use and the remote lacks.
         *
         * A chunk which is already in the local database gets no new sequence when another document begins to use
         * it, so it is not among the changes. It may never have been sent, or it may have been removed from the
         * remote after nothing used it any more.
         */
        const chunksToBringAlong = (changes: EntryDoc[]) => {
            const absent: string[] = [];
            for (const row of changes) {
                if (isChunkEntry(row)) {
                    brought.add(row._id);
                    continue;
                }
                for (const id of chunksInUseBy(row)) {
                    if (brought.has(id) || isInRemote(id)) continue;
                    brought.add(id);
                    absent.push(id);
                }
            }
            return absent;
        };
        // Chunks are read a slice at a time, and each slice is handed on by itself. The stream asks for the next
        // one only when the one before is on its way to the remote, so the content of a large file is never in
        // memory as a whole. The pack whose documents use the chunks follows its last slice.
        let readSlice: ChunkSliceReader | undefined =
            resendIDs.length > 0 ? this._readLocalChunksInSlices(resendIDs, readState) : undefined;
        let slicesAreResent = readSlice !== undefined;
        let packAfterSlices: SendPack | undefined;
        return new ReadableStream<SendPack>({
            pull: async (controller) => {
                while (true) {
                    if (this.requestedStop) {
                        Logger("Packing Journal : Stop requested", logLevel, MSG_KEY);
                        controller.close();
                        return;
                    }
                    if (readSlice) {
                        const slice = await readSlice();
                        if (slice) {
                            const { ids, chunks } = slice;
                            if (slicesAreResent && chunks.length < ids.length) {
                                // What the local database lacks cannot be sent again, so it leaves the queue.
                                const readable = new Set(chunks.map((leaf) => leaf._id as string));
                                const lacking = new Set(ids.filter((id) => !readable.has(id)));
                                await this._updateReliance((old) => ({
                                    ...old,
                                    resend: old.resend.filter((id) => !lacking.has(id)),
                                }));
                            }
                            if (chunks.length === 0) continue;
                            // The chunks carry the sequence before their documents, so the position never
                            // passes a document whose pack is still to come.
                            controller.enqueue({ changes: chunks, packLastSeq: currentLastSeq });
                            return;
                        }
                        readSlice = undefined;
                        slicesAreResent = false;
                        if (!packAfterSlices) continue;
                        const pack = packAfterSlices;
                        packAfterSlices = undefined;
                        currentLastSeq = pack.packLastSeq as number;
                        scan.lastScannedSeq = currentLastSeq;
                        controller.enqueue(pack);
                        return;
                    }
                    const { changes, hasNext, packLastSeq } = await this._createJournalPack(currentLastSeq);
                    // Reading the pack looked at the checkpoint: at its copy in memory, or at the record when
                    // another client has written it. Stop scanning once its history has been reset.
                    if (this._currentCheckPointInfo.resetGeneration !== resetGeneration) {
                        controller.error(new JournalCheckpointResetError());
                        return;
                    }
                    if (changes.length > 0) {
                        // The pack counts as looked through only once it is handed on, after its chunks.
                        packAfterSlices = { changes, packLastSeq };
                        readSlice = this._readLocalChunksInSlices(chunksToBringAlong(changes), readState);
                        continue;
                    }
                    currentLastSeq = packLastSeq as number;
                    scan.lastScannedSeq = currentLastSeq;
                    if (!hasNext) {
                        controller.close();
                        return;
                    }
                    if (hasPassed(positionStoredAt, this.now(), CHECKPOINT_WRITE_INTERVAL_MS)) {
                        // The request travels the stream like a pack, behind everything which was handed on
                        // before it, so the position is stored only once all of that is uploaded.
                        positionStoredAt = this.now();
                        controller.enqueue({ changes: [], packLastSeq: currentLastSeq, storesPosition: true });
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
        const createBuffer = (): SendBuffer => ({ rows: [], binarySize: 0, sentIDs: [], relied: [] });
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
                    relied: buffer.relied,
                });
            }
            ordinary = createBuffer();
            bulk = createBuffer();
            safeSeq = packLastSeq;
        };

        // The chunks which this send has put into a journal. A chunk goes into one journal only.
        const packedChunks = new Set<string>();
        /**
         * Put an entry into its journal, and close the batch when that fills the journal.
         * @param closingSeq the sequence the batch may carry when it closes here.
         */
        const pack = async (
            controller: TransformStreamDefaultController<SendUnit>,
            row: EntryDoc,
            closingSeq: number
        ) => {
            const serialized = serializeDoc(row);
            const isChunk = isChunkEntry(row);
            const buffer = isChunk && serialized.length >= BULK_CHUNK_MIN_BYTES ? bulk : ordinary;
            if (isChunk) packedChunks.add(row._id);
            buffer.sentIDs.push(this.getDocKey(row));
            buffer.binarySize += serialized.length;
            buffer.rows.push(serialized);
            stats.packedDocs++;
            if (isFull(buffer)) await closeBatch(controller, closingSeq);
        };

        return new TransformStream<SendPack, SendUnit>({
            transform: async (chunk, controller) => {
                lastProcessedSeq = chunk.packLastSeq as number;
                const currentSeq = lastProcessedSeq - startSeq;
                Logger(`Packing Journal: ${currentSeq} / ${seqToProcess}`, logLevel, MSG_KEY);
                if (chunk.storesPosition) {
                    // Every change up to here is known or lies in a buffer. Closing the batch uploads what the
                    // buffers hold, and its last journal carries the position; with empty buffers the position
                    // travels by itself. Either way it is the position at which nothing waits to be uploaded.
                    lastCompletedPackSeq = lastProcessedSeq;
                    const isWaiting = ordinary.rows.length > 0 || bulk.rows.length > 0;
                    await closeBatch(controller, lastProcessedSeq);
                    if (!isWaiting) controller.enqueue({ packLastSeq: lastProcessedSeq, positionOnly: true });
                    return;
                }

                for (const [index, row] of chunk.changes.entries()) {
                    if (isChunkEntry(row)) {
                        // A document has already brought this chunk along.
                        if (packedChunks.has(row._id)) continue;
                    } else {
                        if (mayLeaveChunksUnused(row)) this.noteContentMayHaveDied();
                        // The chunks which the remote lacked came before this document. What is not in this send
                        // is taken to be in the remote. Should it be removed from there meanwhile, the document
                        // is remembered so that it can be sent after all.
                        if (chunksInUseBy(row).some((id) => !packedChunks.has(id))) {
                            ordinary.relied.push({ id: row._id, rev: row._rev ?? "" });
                        }
                    }
                    // A batch closed on the last row of a pack contains that whole pack.
                    const isLastRow = index === chunk.changes.length - 1;
                    await pack(controller, row, isLastRow ? (chunk.packLastSeq as number) : lastCompletedPackSeq);
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
        stats: { uploadedFiles: number },
        /** Whether this send began with chunks which had to be sent again. */
        hasResendChunks: boolean
    ) {
        let sentFilesCount = 0;
        let partIndex = 0;
        return new WritableStream<SendUnit>({
            write: async (chunk) => {
                if ("positionOnly" in chunk) {
                    // Every journal before this is stored and recorded, and nothing between them and this
                    // sequence was left to send.
                    // A history which was reset meanwhile keeps its own position. The pass then ends at its next
                    // look at the checkpoint.
                    await this._updateTransferCheckpoint(resetGeneration, (info) => ({
                        ...info,
                        lastLocalSeq: chunk.packLastSeq,
                    }));
                    return;
                }
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
                // The index tells a device which has not fetched this journal which chunks it holds. The journal
                // is stored whether its index is or not. Without one it is like a journal of an earlier version:
                // it stays, and a desktop device reads it to learn what it holds.
                if (chunk.isBulk) {
                    const index: JournalIndex = { version: 1, chunkIDs: chunk.sentIDs };
                    if (!(await this.uploadPackedJson(journalIndexKey(filename), index, { keepExisting: true }))) {
                        Logger(`The index of ${filename} could not be stored`, LOG_LEVEL_VERBOSE);
                    }
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
                if (chunk.relied.length > 0 || hasResendChunks) {
                    const sentAt = this.now();
                    const sent = new Set(chunk.sentIDs);
                    await this._updateReliance((old) => ({
                        documents: [...old.documents, ...chunk.relied.map((doc) => ({ ...doc, sentAt }))],
                        resend: old.resend.filter((id) => !sent.has(id)),
                    }));
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
        return await this._trackTransfer(this._holdingCheckpoint(() => this._sendLocalJournal(showMessage)));
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
            const resendIDs = await this._takeChunksToSendAgain(checkPointInfo);
            const readable = this._createSendReadableStream(
                startSeq,
                resetGeneration,
                logLevel,
                MSG_KEY,
                scan,
                resendIDs
            );
            const transform = this._createSendCompressTransformStream(startSeq, seqToProcess, logLevel, MSG_KEY, stats);
            const writable = this._createSendUploadWritableStream(
                max,
                startSeq,
                resetGeneration,
                writerId,
                logLevel,
                `${MSG_KEY}_upload`,
                stats,
                resendIDs.length > 0
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
            } finally {
                // One marker covers every journal in this send. It is best-effort: if it cannot be stored, the
                // receiver's periodic full listing still discovers the journals within ten minutes.
                if (stats.uploadedFiles > 0) await this.publishRemoteChangeMarker();
            }
        });
    }

    /**
     * Read chunks from the local database, a bounded slice at a time.
     *
     * A file of several hundred megabytes has thousands of chunks, and reading them at once would exhaust the
     * memory of a mobile device. A slice holds at most {@link LOCAL_CHUNK_READ_MAX_COUNT} chunks, and fewer when
     * chunks of the largest size read so far would exceed {@link LOCAL_CHUNK_READ_MAX_BYTES}.
     *
     * Each slice comes with the IDs which were asked for. A chunk which is not there is left out, and logged.
     */
    private _readLocalChunksInSlices(ids: string[], state: ChunkReadState): ChunkSliceReader {
        let from = 0;
        return async () => {
            if (from >= ids.length) return undefined;
            const count =
                state.largestBytes === undefined
                    ? 1
                    : Math.floor(LOCAL_CHUNK_READ_MAX_BYTES / Math.max(1, state.largestBytes));
            const slice = ids.slice(from, from + Math.min(LOCAL_CHUNK_READ_MAX_COUNT, Math.max(1, count)));
            from += slice.length;
            const rows = (await this.db.allDocs({ keys: slice, include_docs: true })).rows;
            const chunks: EntryLeaf[] = [];
            for (const [index, row] of rows.entries()) {
                const doc = "doc" in row ? row.doc : undefined;
                if (doc && !("_deleted" in doc && doc._deleted)) {
                    const leaf = doc as unknown as EntryLeaf;
                    const bytes = typeof leaf.data === "string" ? leaf.data.length : 0;
                    state.largestBytes = Math.max(state.largestBytes ?? 0, bytes);
                    chunks.push(leaf);
                } else {
                    Logger(
                        `Chunk ${slice[index]} is not in the local database, so it cannot be sent`,
                        LOG_LEVEL_VERBOSE
                    );
                }
            }
            return { ids: slice, chunks };
        };
    }

    /**
     * The chunks which a removal took from the remote while a document this device had just sent relied on them.
     *
     * One which the remote holds again is dropped from the queue. The others stay queued until the journal which
     * carries them is recorded, or until the local database turns out to lack them.
     */
    private async _takeChunksToSendAgain(checkPointInfo: CheckPointInfo): Promise<string[]> {
        const queued = (await this._getReliance()).resend;
        if (queued.length === 0) return [];
        const absent = queued.filter((id) => !checkPointInfo.knownIDs.has(id) && !checkPointInfo.sentIDs.has(id));
        if (absent.length !== queued.length) {
            const stillAbsent = new Set(absent);
            await this._updateReliance((old) => ({ ...old, resend: old.resend.filter((id) => stillAbsent.has(id)) }));
        }
        return absent;
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
        return isJournalKey(key) && !checkPointInfo.sentFiles.has(key) && !checkPointInfo.receivedFiles.has(key);
    }

    /**
     * List the remote once: its journals in upload order, which of them have an index, and the removal records.
     *
     * Storage which cannot report sizes is listed in upload order, and storage which cannot report upload order
     * either is listed by name.
     */
    async listRemote(): Promise<RemoteListing> {
        const sized = await this.storage.listObjectsInUploadOrder?.();
        const groups: JournalStorageObject[][] =
            sized ??
            (
                (await this.storage.listFilesInUploadOrder?.()) ??
                (await this.storage.listFiles(""))
                    .sort((a, b) => a.localeCompare(b, undefined, { numeric: true }))
                    .map((key) => [key])
            ).map((keys) => keys.map((key) => ({ key })));
        const others = groups.flat().filter(({ key }) => !isJournalKey(key));
        return {
            journals: groups
                .map((objects) => objects.filter(({ key }) => isJournalKey(key)))
                .filter((objects) => objects.length > 0),
            hasSizes: sized !== undefined,
            indexed: new Set(
                others
                    .filter(({ key }) => key.startsWith(JOURNAL_INDEX_PREFIX))
                    .map(({ key }) => key.slice(JOURNAL_INDEX_PREFIX.length))
            ),
            removalRecords: others.filter(
                ({ key }) => key.startsWith(REMOVAL_RECORD_PREFIX) && key.endsWith(REMOVAL_RECORD_SUFFIX)
            ),
            assessmentMarkers: others.filter(({ key }) => key.startsWith(ASSESSMENT_MARKER_PREFIX)),
        };
    }

    /** Publish a new opaque marker after changing the journal objects which receivers list. */
    async publishRemoteChangeMarker(): Promise<boolean> {
        try {
            const bytes = globalThis.crypto.getRandomValues(new Uint8Array(16));
            const token = [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");
            const marker = new TextEncoder().encode(JSON.stringify({ version: 1, token }));
            const stored = await this.storage.upload(REMOTE_CHANGE_MARKER_KEY, marker, "application/json");
            if (!stored) Logger(`The remote change marker could not be stored`, LOG_LEVEL_VERBOSE);
            return stored;
        } catch (ex) {
            Logger(`The remote change marker could not be stored`, LOG_LEVEL_VERBOSE);
            Logger(ex, LOG_LEVEL_VERBOSE);
            return false;
        }
    }

    /**
     * Read the cheap change marker and reuse the previous full listing while it is unchanged and younger than ten
     * minutes. An absent marker is a valid legacy state. A failed or malformed marker read falls back to listing.
     */
    private async listRemoteWhenChanged(): Promise<RemoteListing> {
        let markerToken: string | null | undefined;
        try {
            const result = await this.downloadJsonWithResult<unknown>(REMOTE_CHANGE_MARKER_KEY);
            if (result.status === JournalStorageReadStatuses.AVAILABLE) {
                markerToken = parseRemoteChangeMarker(result.value).token;
            } else if (result.status === JournalStorageReadStatuses.NOT_FOUND) {
                markerToken = null;
            }
        } catch {
            markerToken = undefined;
        }

        const cached = this.cachedRemoteListing;
        const markerIsKnownAndUnchanged =
            markerToken !== undefined && cached?.markerToken !== undefined && markerToken === cached.markerToken;
        if (
            cached &&
            markerIsKnownAndUnchanged &&
            !hasPassed(cached.listedAt, this.now(), REMOTE_FULL_LIST_INTERVAL_MS)
        ) {
            return cached.listing;
        }

        const listing = await this.listRemote();
        this.cachedRemoteListing = { listing, markerToken, listedAt: this.now() };
        return listing;
    }

    /**
     * The listed journals which this device has still to receive, split into those which are applied in upload
     * order and those which hold only large chunks.
     *
     * The journals of large chunks are told by their name and returned smallest first, so that the content which
     * takes least to fetch arrives first; those of one size stay in upload order. Storage which cannot list sizes
     * leaves every journal in upload order.
     */
    private _splitJournalsToReceive(
        listing: RemoteListing,
        checkPointInfo: CheckPointInfo
    ): { ordinary: string[][]; bulk: string[] } {
        const groups = listing.journals.map((objects) =>
            objects.filter(({ key }) => this._isUnprocessedJournal(key, checkPointInfo))
        );
        const isBulk = ({ key }: JournalStorageObject) => listing.hasSizes && key.endsWith(BULK_JOURNAL_SUFFIX);
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
     * Read the removal records which this device has not read yet, and act on them.
     *
     * This is done before anything is received or sent. A record which cannot be read fails the receive, so that
     * nothing is sent on what this device believed the remote to hold.
     *
     * @param listedAt when the listing which showed the records was taken. A document which was sent an hour or
     * more before that is forgotten afterwards: no removal which is still to come can have missed it.
     * @returns the checkpoint as it is afterwards.
     */
    private async _processRemovalRecords(
        listed: JournalStorageObject[],
        checkPointInfo: CheckPointInfo,
        listedAt: number
    ): Promise<CheckPointInfo> {
        const unread = listed.filter(({ key }) => !checkPointInfo.receivedFiles.has(key));
        let afterwards = checkPointInfo;
        if (unread.length > 0) {
            const { inTime, ignored } = await this._readRemovalRecords(unread);
            afterwards = await this.applyRemovalRecords(inTime, checkPointInfo.resetGeneration, ignored);
        }
        this._unreadableRemovalRecordCycles = 0;
        await this._updateReliance((old) => {
            const documents = old.documents.filter((doc) => listedAt - doc.sentAt < RELIANCE_MEMORY_MS);
            return documents.length === old.documents.length ? old : { ...old, documents };
        });
        return afterwards;
    }

    /** How many receives in a row ended because a removal record could not be read. */
    private _unreadableRemovalRecordCycles = 0;
    private _unreadableRemovalRecordNoticed = false;

    /**
     * Download removal records, and tell those to act on from those which reached the storage too late: see
     * {@link isRemovalInTime}. The device which made a record decides by the same rule on the same two times, so a
     * record is acted on by every device or by none. Storage which does not report when it received an object
     * leaves nothing to compare, so its records are all acted on; no device removes anything on such storage.
     *
     * A record which cannot be read throws. One such receive may pass; the second in a row is worth telling, once.
     */
    private async _readRemovalRecords(
        unread: JournalStorageObject[]
    ): Promise<{ inTime: { key: string; record: RemovalRecord }[]; ignored: string[] }> {
        const inTime: { key: string; record: RemovalRecord }[] = [];
        const ignored: string[] = [];
        try {
            for (const { key, storedAt } of unread) {
                const read = await this.downloadPackedJson(key);
                if (read.status !== JournalStorageReadStatuses.AVAILABLE) {
                    throw new Error(`Could not read the removal record ${key}`, {
                        cause: "error" in read ? read.error : undefined,
                    });
                }
                const record = parseRemovalRecord(read.value);
                if (storedAt !== undefined && !isRemovalInTime(record.assessmentStoredAt, storedAt)) {
                    Logger(`The removal record ${key} reached the remote too late, and is ignored`, LOG_LEVEL_INFO);
                    ignored.push(key);
                } else {
                    inTime.push({ key, record });
                }
            }
        } catch (ex) {
            this._unreadableRemovalRecordCycles++;
            if (this._unreadableRemovalRecordCycles >= 2 && !this._unreadableRemovalRecordNoticed) {
                this._unreadableRemovalRecordNoticed = true;
                Logger(
                    `A record of content which was removed from the remote cannot be read. Nothing is received or sent until it can be.`,
                    LOG_LEVEL_NOTICE
                );
            }
            throw ex;
        }
        return { inTime, ignored };
    }

    /**
     * Act on removal records: their chunks are no longer in the remote, and their journals must never be fetched.
     *
     * - The chunks leave the sets of what the remote holds, so that a document which uses one again brings it
     *   along. Chunks which a document this device sent a short while ago still uses are queued to be sent again.
     * - The journals are marked as received. A journal can outlive its record, when the device which removes it
     *   is stopped in between. A device which fetched it then would count its chunks as present again, and nothing
     *   would tell it once the journal is deleted after all.
     * - The records are marked as received.
     *
     * @param resetGeneration when given, the records are only applied while the checkpoint still has this reset
     * generation.
     * @param ignoredRecords records which are not acted on. They are only marked as received.
     * @returns the checkpoint as it is afterwards.
     */
    async applyRemovalRecords(
        records: { key: string; record: RemovalRecord }[],
        resetGeneration?: number,
        ignoredRecords: string[] = []
    ): Promise<CheckPointInfo> {
        const chunkIDs = new Set(records.flatMap(({ record }) => record.chunkIDs));
        const journals = records.flatMap(({ record }) => record.journals);
        // The queue is stored first. Should the device stop before the records are marked, they are read again.
        await this._queueChunksStillInUse(chunkIDs);
        let applied = false;
        const updated = await this.updateCheckPointInfo((info) => {
            if (resetGeneration !== undefined && info.resetGeneration !== resetGeneration) return info;
            applied = true;
            for (const id of chunkIDs) {
                info.knownIDs.delete(id);
                info.sentIDs.delete(id);
            }
            for (const journal of journals) {
                info.sentFiles.delete(journal);
                info.receivedFiles.add(journal);
            }
            for (const { key } of records) info.receivedFiles.add(key);
            for (const key of ignoredRecords) info.receivedFiles.add(key);
            return { ...info };
        });
        if (!applied) throw new JournalCheckpointResetError();
        // What this session still expected to fetch of them is no longer to come.
        for (const journal of journals) this._unreceivedBulkJournals?.delete(journal);
        return updated;
    }

    /**
     * Queue the removed chunks which a remembered document still uses, to be sent again.
     *
     * A document counts while the revision which was sent is still a live one: the winning revision or a
     * conflicting one, and not deleted.
     */
    private async _queueChunksStillInUse(removed: Set<string>): Promise<void> {
        const remembered = (await this._getReliance()).documents;
        if (removed.size === 0 || remembered.length === 0) return;
        const needed = new Set<string>();
        for (const { id, rev } of remembered) {
            for (const chunk of await this._chunksInUseByRevision(id, rev)) {
                if (removed.has(chunk)) needed.add(chunk);
            }
        }
        if (needed.size === 0) return;
        await this._updateReliance((old) => ({ ...old, resend: [...new Set([...old.resend, ...needed])] }));
    }

    /** The chunks which a revision of a document uses, when that revision is still a live one. */
    private async _chunksInUseByRevision(id: string, rev: string): Promise<string[]> {
        try {
            const winner = await this.db.get(id, { conflicts: true });
            if (winner._rev === rev) return chunksInUseBy(winner);
            if (!(winner._conflicts ?? []).includes(rev)) return [];
            return chunksInUseBy(await this.db.get(id, { rev }));
        } catch (ex) {
            // A document which is gone uses nothing. Any other failure must not pass for that.
            if ((ex as { status?: number }).status === 404) return [];
            throw ex;
        }
    }

    /**
     * @param resetGeneration when given, the documents are only recorded as known while the checkpoint still has
     * this reset generation.
     */
    async processDocuments(allDocs: ProcessingEntry[], resetGeneration?: number) {
        let applyTotal = 0;
        let wholeItems = 0;
        const recordKnown = async (ids: string[]) => {
            if (await this._registerReceived(resetGeneration, { knownIDs: ids })) return true;
            Logger(`The journal history was reset while receiving`, LOG_LEVEL_INFO);
            return false;
        };
        // What the receive records stays as it is: the journal was received. But the local database may now lack
        // a document which the remote holds, so it can no longer show which content is in use.
        const noteNotStored = async (count: number) => {
            if (count > 0) await this.cleaner.noteDocumentsNotStored(count);
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
                const failed = ret.filter((e): e is PouchDB.Core.Error => "error" in e);
                const saveError = failed.map((e) => e.id);
                // A conflict says that the chunk was stored meanwhile, which is as good as storing it here.
                await noteNotStored(failed.filter((e) => e.status !== 409).length);

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

            const refused = await this.db.bulkDocs<EntryDoc>(saveDocs, { new_edits: false });
            await noteNotStored(refused.filter((e) => "error" in e).length);

            if (saveDocs.some(mayLeaveChunksUnused)) this.noteContentMayHaveDied();
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
    async readRemoteJournal(key: string): Promise<ProcessingEntry[]> {
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
                        if (!(await this._registerReceived(resetGeneration, { receivedFiles: [key] }))) {
                            controller.error(new JournalCheckpointResetError());
                            return;
                        }
                        continue; // Skip
                    }

                    try {
                        docs.push(...(await this.readRemoteJournal(key)));
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

                if (!(await this._registerReceived(resetGeneration, { receivedFiles: keys }))) {
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
            this._holdingCheckpoint(async () => {
                const deferred = await this._receiveOrdinaryJournals(showMessage);
                if (!deferred) return false;
                const { complete } = await this._receiveBulkJournals(deferred, showMessage);
                this.updateInfo({ syncStatus: complete ? "COMPLETED" : "ERRORED" });
                return complete;
            })
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
            this._receivingDocumentJournals = true;
            try {
                // The checkpoint is read before the listing: a reset of the history while the remote is listed
                // must stop this receive, like one which comes later.
                const listedAt = this.now();
                const beforeListing = await this.getCheckpointInfo();
                // A failed or aborted listing is a failed receive, like a failed download, not a thrown cycle.
                const listing = await this.listRemoteWhenChanged();
                // Removal records come first. Acting on them takes chunks out of the history, which is a reset
                // to any transfer that started before; this receive starts after it.
                const checkPointInfo = await this._processRemovalRecords(
                    listing.removalRecords,
                    beforeListing,
                    listedAt
                );
                const resetGeneration = checkPointInfo.resetGeneration;
                const { ordinary: files, bulk } = this._splitJournalsToReceive(listing, checkPointInfo);
                this._unreceivedBulkJournals = new Set(bulk);
                const deferred: DeferredReceive = { resetGeneration, journals: bulk, listing };
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
            } finally {
                this._receivingDocumentJournals = false;
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
        if (!(await this._registerReceived(resetGeneration, { receivedFiles: [key] }))) {
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
    /** Whether a receive of the journals which can hold documents is running. */
    private _receivingDocumentJournals = false;
    /** Journals named as holding only chunks which held a document. */
    private _refusedBulkJournals = new Set<string>();

    /** Whether this session refused to apply the journal, because it is named as holding only chunks but held a document. */
    isRefusedBulkJournal(key: string): boolean {
        return this._refusedBulkJournals.has(key);
    }
    /** Journals of large chunks whose latest receive failed, with the number of failures in a row. */
    private _failedBulkJournals = new Map<string, number>();
    private _receivedBulkJournalInLastSync = false;

    /** Whether a document was received or sent since this was last asked which can have left chunks unused. */
    private _contentMayHaveDied = false;

    noteContentMayHaveDied(): void {
        this._contentMayHaveDied = true;
    }

    takeContentMayHaveDied(): boolean {
        const mayHaveDied = this._contentMayHaveDied;
        this._contentMayHaveDied = false;
        return mayHaveDied;
    }

    /**
     * Forget what this client learnt about the journals of large chunks of a remote: when it is pointed at another
     * one, and when its journal history starts again. A remote which was built again can hold another journal
     * under the name of one which was refused or failed.
     */
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
     *
     * While the journals which can hold documents are being received it cannot tell either. A document is handed
     * on before the journals after it are read, and one of those may hold a later revision of it, such as the
     * deletion of a file whose content was since removed from the remote.
     */
    hasDeferredRemoteContent(): boolean {
        if (this._receivingDocumentJournals) return true;
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
        return await this._holdingCheckpoint(() => this._sync(showResult));
    }

    private async _sync(showResult: boolean) {
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
                let sent = await this.sendLocalJournal(showResult);
                const { received } = await this._trackTransfer(
                    this._receiveBulkJournals(deferred, showResult, this.bulkReceiveBudgetMs)
                );
                this._receivedBulkJournalInLastSync = received > 0;
                // The chunks which were just stored are changes of the local database after the position of the
                // send. Looked through now, while they count as held by the remote, they are passed. Left to the
                // next synchronisation, they would be sent again if a removal record first took them out of what
                // the remote is taken to hold.
                if (sent && received > 0 && !this.requestedStop) sent = await this.sendLocalJournal(showResult);
                // Only a synchronisation which received every journal with documents and sent its own changes
                // knows what is in use, so only such a one may remove anything from the remote.
                if (sent) await this._trackTransfer(this.cleaner.run(deferred.listing));
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
