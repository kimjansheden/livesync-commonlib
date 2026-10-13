import { LOG_LEVEL_INFO, LOG_LEVEL_NOTICE, LOG_LEVEL_VERBOSE } from "@lib/common/types.ts";
import { Logger } from "@lib/common/logger.ts";
import type { JournalSyncCore, RemoteListing } from "./JournalSyncCore.ts";
import {
    ASSESSMENT_MARKER_PREFIX,
    BULK_JOURNAL_SUFFIX,
    chunksInUseBy,
    isChunkEntry,
    isRemovalInTime,
    journalIndexKey,
    parseJournalIndex,
    REMOVAL_FRESHNESS_MS,
    REMOVAL_RECORD_PREFIX,
    REMOVAL_RECORD_SUFFIX,
    type RemovalRecord,
    sha256Hex,
} from "./JournalSyncTypes.ts";
import { JournalStorageReadStatuses, type JournalStorageObject } from "./objectstore/JournalStorageAdapter.ts";

/** The least time between two cleaning rounds of a device, and between the two assessments a removal needs. */
export const CLEANING_INTERVAL_MS = 10 * 60 * 1000;
/** How long a mobile device may look through its documents in one round. */
export const MOBILE_SCAN_LIMIT_MS = 10 * 1000;
/** How long a mobile device which could not look through its documents in that time waits before it tries again. */
export const MOBILE_SCAN_RETRY_MS = 24 * 60 * 60 * 1000;
/**
 * How long a session relies on what it was told before it looks through the documents anyway.
 *
 * What tells a device to look is the revision it sends or receives: a deletion, or a version of a file which is
 * large enough to have large chunks. A file which was large and is replaced by a small version tells it nothing,
 * so the chunks it left are found by this look at the latest.
 */
export const FULL_SCAN_INTERVAL_MS = 24 * 60 * 60 * 1000;
/** A journal without an index is only read to learn its content when it has at least this size. */
export const UNINDEXED_JOURNAL_MIN_BYTES = 1024 * 1024;
/** How much a desktop device downloads in one round to learn the content of journals without an index. */
export const UNINDEXED_READ_BUDGET_BYTES = 256 * 1024 * 1024;
const SCAN_PAGE_SIZE = 200;
/** The most keys one request to delete may name. */
const DELETE_BATCH_SIZE = 1000;
/**
 * Where the identity of the local database is kept: in the database itself, as a document which is never
 * synchronised, and in the key-value store which holds the checkpoint.
 */
const DATABASE_IDENTITY_DOC = "_local/bucketsync-database-identity";
const DATABASE_IDENTITY_KEY = "bucketsync-database-identity";

/** What this device knows of the content of a journal: whether it holds nothing but chunks, and which. */
type JournalContent = { pure: boolean; chunkIDs: string[] };
/** The journals none of whose chunks was in use at an assessment. */
type Assessment = { at: number; dead: string[] };
type CleaningState = { lastRoundAt?: number; assessment?: Assessment; scanTimedOutAt?: number };
/** What a look through the documents found: the chunks in use, and how many documents can use chunks at all. */
type DocumentScan = { inUse: Set<string>; updateSeq: number | string; documents: number };
type DatabaseIdentity = { identity: string };

/** Whether `now` is less than `span` after `since`. A clock which was set back does not count, so it cannot hold a device up. */
function isWithin(since: number | undefined, now: number, span: number): boolean {
    if (since === undefined) return false;
    const elapsed = now - since;
    return elapsed >= 0 && elapsed < span;
}

function randomIdentity(): string {
    const bytes = globalThis.crypto.getRandomValues(new Uint8Array(16));
    return [...bytes].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

/**
 * Removes from the remote the journals whose chunks nothing uses any more.
 *
 * The remote only grows otherwise: the content of a deleted file stays in it for good. A journal is removed only
 * when it holds nothing but chunks, and no live revision of any document uses any of them: neither a winning
 * revision nor a conflicting one. A journal which can hold a document is never removed.
 *
 * Every device does this, with the same code, while it synchronises. To judge a journal which it has not fetched,
 * a device reads the journal's index. Before anything is deleted, the chunks which go are published in a removal
 * record, so that every device stops counting them as present in the remote.
 *
 * What is in use is read from the local database, so a device only removes anything while that database can be
 * trusted to hold every document of the remote.
 */
export class JournalCleaner {
    /** Whether the documents must be looked through whatever else is known: after a start, or an unfinished look. */
    private scanIsDue = true;
    /** When this session last looked through the documents to the end. */
    private lastScanAt: number | undefined;
    /** Whether this session has told the user that it removes nothing. */
    private refusalNoticed = false;

    constructor(private readonly core: JournalSyncCore) {}

    private get stateKey(): string {
        return `bucketsync-cleaning-${this.core.hash}`;
    }

    private get contentKeyPrefix(): string {
        return `bucketsync-journal-content-${this.core.hash}-`;
    }

    private get incompleteKey(): string {
        return `bucketsync-incomplete-${this.core.hash}`;
    }

    private get markersKey(): string {
        return `bucketsync-assessment-markers-${this.core.hash}`;
    }

    private async readState(): Promise<CleaningState> {
        const stored = (await this.core.keyValueStore<Partial<CleaningState>>().get(this.stateKey)) ?? {};
        const assessment = stored.assessment;
        return {
            lastRoundAt: typeof stored.lastRoundAt === "number" ? stored.lastRoundAt : undefined,
            assessment:
                assessment && typeof assessment.at === "number" && Array.isArray(assessment.dead)
                    ? { at: assessment.at, dead: assessment.dead.filter((key) => typeof key === "string") }
                    : undefined,
            scanTimedOutAt: typeof stored.scanTimedOutAt === "number" ? stored.scanTimedOutAt : undefined,
        };
    }

    private async writeState(state: CleaningState): Promise<void> {
        await this.core.keyValueStore<CleaningState>().set(this.stateKey, state);
    }

    /**
     * Run a cleaning round when the device is set to clean and its last round was long enough ago.
     *
     * @param listing the listing of the synchronisation which has just received every journal with documents.
     * A failure ends the round and is logged; it is no failure of the synchronisation.
     */
    async run(listing: RemoteListing): Promise<void> {
        const core = this.core;
        if (!core.currentSettings.removeUnreferencedJournalData || core.requestedStop) return;
        try {
            const stored = await this.readState();
            const startedAt = core.now();
            if (isWithin(stored.lastRoundAt, startedAt, CLEANING_INTERVAL_MS)) return;
            const state = { ...stored, lastRoundAt: startedAt };
            await this.writeState(state);
            // A mobile device whose documents were too many to look through leaves the cleaning to the others,
            // and does not spend that time again in every round.
            if (isWithin(state.scanTimedOutAt, startedAt, MOBILE_SCAN_RETRY_MS)) return;
            if (!(await this.mayTrustLocalDatabase())) return;
            await this.runRound(listing, state);
        } catch (ex) {
            Logger(`Cleaning the remote: the round ended with an error and is tried again later`, LOG_LEVEL_INFO);
            Logger(ex, LOG_LEVEL_VERBOSE);
        }
    }

    /**
     * Note that received documents could not be stored in the local database.
     *
     * The journal which held them counts as received all the same, so nothing brings them again. From then on the
     * local database may lack a document which uses content of the remote, and this device removes nothing.
     */
    async noteDocumentsNotStored(count: number): Promise<void> {
        Logger(`${count} received document(s) could not be stored in the local database`, LOG_LEVEL_NOTICE);
        await this.core.keyValueStore<{ at: number }>().set(this.incompleteKey, { at: this.core.now() });
    }

    /**
     * Forget what kept this device from removing content, when the journal history is reset.
     *
     * The device then receives every journal again, which makes its local database complete, and a new identity
     * is given to the database and the key-value store together. A failure here leaves the device removing
     * nothing, and is no failure of the reset.
     */
    async forgetWhyDatabaseIsNotTrusted(): Promise<void> {
        try {
            const store = this.core.keyValueStore<unknown>();
            await store.delete(this.incompleteKey);
            await store.delete(DATABASE_IDENTITY_KEY);
            const db = this.core.db as unknown as PouchDB.Database<DatabaseIdentity>;
            await db.remove(await db.get(DATABASE_IDENTITY_DOC));
        } catch (ex) {
            if ((ex as { status?: number }).status === 404) return;
            Logger(`Cleaning the remote: could not forget the state of the local database`, LOG_LEVEL_VERBOSE);
            Logger(ex, LOG_LEVEL_VERBOSE);
        }
    }

    /**
     * Whether the local database can show which content is in use.
     *
     * It cannot once a received document could not be stored. It cannot either when it is not the database which
     * the checkpoint describes: the checkpoint lives in the key-value store, and the two can part, for example when
     * the application opens another local database while the key-value store stays. A database which lacks
     * documents would make their content look unused.
     *
     * The two are first paired as they are found, so a database which had parted before that is not told by its
     * identity. One sign of it is: the checkpoint has looked through more changes than the database has had.
     */
    private async mayTrustLocalDatabase(): Promise<boolean> {
        const store = this.core.keyValueStore<unknown>();
        if ((await store.get(this.incompleteKey)) !== undefined) {
            this.refuse(`a received document could not be stored in the local database`);
            return false;
        }
        const lookedThrough = Number((await this.core.getCheckpointInfo()).lastLocalSeq);
        if (lookedThrough > Number((await this.core.db.info()).update_seq)) {
            this.refuse(`the journal history is ahead of the local database`);
            return false;
        }
        if (!(await this.localDatabaseBelongsToStore())) {
            this.refuse(`the local database is not the one which the journal history describes`);
            return false;
        }
        return true;
    }

    /** Say that nothing is removed, and why. The user is told once in a session. */
    private refuse(reason: string): void {
        Logger(
            `Content is not removed from the remote by this device, because ${reason}. Resetting the journal history lets it start again.`,
            this.refusalNoticed ? LOG_LEVEL_VERBOSE : LOG_LEVEL_NOTICE
        );
        this.refusalNoticed = true;
    }

    /**
     * Whether the local database and the key-value store carry the same identity.
     *
     * The identity is a random value which is written to both when neither has one. Afterwards a database
     * without it, or with another one, is not the database whose documents the checkpoint counts as received.
     */
    private async localDatabaseBelongsToStore(): Promise<boolean> {
        const store = this.core.keyValueStore<string>();
        const db = this.core.db as unknown as PouchDB.Database<DatabaseIdentity>;
        const inStore = (await store.get(DATABASE_IDENTITY_KEY)) ?? undefined;
        const inDatabase = await db.get(DATABASE_IDENTITY_DOC).then(
            (doc) => doc.identity,
            (ex: { status?: number }): undefined => {
                if (ex.status !== 404) throw ex;
            }
        );
        if (inStore === undefined && inDatabase === undefined) {
            const identity = randomIdentity();
            await db.put({ _id: DATABASE_IDENTITY_DOC, identity });
            await store.set(DATABASE_IDENTITY_KEY, identity);
            return true;
        }
        return inStore === inDatabase;
    }

    private async runRound(listing: RemoteListing, state: CleaningState): Promise<void> {
        const core = this.core;
        await this.deleteAssessmentMarkers();
        const journals = listing.journals.flat();
        const { contents, learntOfPureJournal } = await this.learnJournalContents(journals, listing.indexed);
        await this.forgetUnlistedJournals(new Set(journals.map(({ key }) => key)));
        // Only a journal which is known to hold chunks, and nothing else, can be removed. A journal which this
        // session refused to apply because it held a document is no such journal, whatever its index says.
        const candidates = [...contents].filter(
            ([key, content]) => content.pure && content.chunkIDs.length > 0 && !core.isRefusedBulkJournal(key)
        );
        if (candidates.length === 0) return;

        // Looking through every document is only worth it when something may have gone out of use.
        const previous = state.assessment;
        const mayHaveDied = core.takeContentMayHaveDied();
        const hadDeadJournals = (previous?.dead.length ?? 0) > 0;
        // Not every revision which leaves chunks unused says so, so a look is also due when the last one is old.
        const lastScanIsOld = !isWithin(this.lastScanAt, core.now(), FULL_SCAN_INTERVAL_MS);
        if (!this.scanIsDue && !lastScanIsOld && !mayHaveDied && !learntOfPureJournal && !hadDeadJournals) return;
        this.scanIsDue = true;
        const scan = await this.findChunksInUse();
        if (scan === "stopped") return;
        if (scan === "timed-out") {
            await this.writeState({ ...state, scanTimedOutAt: core.now() });
            return;
        }
        // A database without any document which could use a chunk says nothing about what is in use: it is new,
        // or it is not the one this device synchronised. It never justifies a removal.
        if (scan.documents === 0) {
            Logger(`Cleaning the remote: the local database holds no file, so nothing is removed`, LOG_LEVEL_VERBOSE);
            return;
        }
        this.scanIsDue = false;
        this.lastScanAt = core.now();

        const assessedAt = core.now();
        const dead = candidates
            .filter(([, content]) => content.chunkIDs.every((id) => !scan.inUse.has(id)))
            .map(([key]) => key);
        const round: CleaningState = { lastRoundAt: state.lastRoundAt };
        await this.writeState({ ...round, assessment: { at: assessedAt, dead } });
        if (dead.length > 0) {
            Logger(`Cleaning the remote: ${dead.length} journal(s) hold nothing which is in use`, LOG_LEVEL_VERBOSE);
        }

        // A file which is deleted and put back, or moved in two steps, must not have its content removed and
        // sent again. So a journal goes only when two assessments, some time apart, both found it unused.
        const wasDead = new Set(previous && assessedAt - previous.at >= CLEANING_INTERVAL_MS ? previous.dead : []);
        const removable = dead.filter((key) => wasDead.has(key));
        if (removable.length === 0) return;

        const removed = await this.remove(removable, contents, scan.updateSeq);
        if (removed.length === 0) return;
        for (const key of removed) await this.core.keyValueStore<JournalContent>().delete(this.contentKeyPrefix + key);
        const gone = new Set(removed);
        await this.writeState({ ...round, assessment: { at: assessedAt, dead: dead.filter((key) => !gone.has(key)) } });
        Logger(`Cleaning the remote: removed ${removed.length} journal(s) whose content nothing used`, LOG_LEVEL_INFO);
    }

    /**
     * What this device knows of the content of the listed journals, learning what it can of those it does not know.
     *
     * - A journal of large chunks is known by its index object, which is small.
     * - A journal without an index was made by an earlier version, or its index could not be stored. A desktop
     *   device reads such a journal once, when it is large enough to matter, and only so much of them in one
     *   round. A mobile device never downloads a journal for this.
     *
     * A journal whose content cannot be learnt now is left for a later round.
     */
    private async learnJournalContents(
        journals: JournalStorageObject[],
        indexed: Set<string>
    ): Promise<{ contents: Map<string, JournalContent>; learntOfPureJournal: boolean }> {
        const core = this.core;
        const store = core.keyValueStore<JournalContent>();
        const mayReadJournals = !core.env.services.API.isMobile();
        const contents = new Map<string, JournalContent>();
        let learntOfPureJournal = false;
        let readBudget = UNINDEXED_READ_BUDGET_BYTES;
        for (const { key, size } of journals) {
            if (core.requestedStop) break;
            let content = await store.get(this.contentKeyPrefix + key);
            if (!content) {
                if (indexed.has(key)) {
                    // Only a journal which is named as holding nothing but chunks is taken at the word of an index.
                    if (key.endsWith(BULK_JOURNAL_SUFFIX)) content = await this.readIndex(key);
                } else if (mayReadJournals && size !== undefined && size >= UNINDEXED_JOURNAL_MIN_BYTES) {
                    if (size > readBudget) continue;
                    readBudget -= size;
                    content = await this.readJournal(key);
                }
                if (!content) continue;
                await store.set(this.contentKeyPrefix + key, content);
                learntOfPureJournal ||= content.pure;
            }
            contents.set(key, content);
        }
        return { contents, learntOfPureJournal };
    }

    private async readIndex(journalKey: string): Promise<JournalContent | undefined> {
        const read = await this.core.downloadPackedJson(journalIndexKey(journalKey));
        if (read.status !== JournalStorageReadStatuses.AVAILABLE) return undefined;
        try {
            return { pure: true, chunkIDs: parseJournalIndex(read.value).chunkIDs };
        } catch (ex) {
            Logger(`Cleaning the remote: the index of ${journalKey} could not be read`, LOG_LEVEL_VERBOSE);
            Logger(ex, LOG_LEVEL_VERBOSE);
            return undefined;
        }
    }

    private async readJournal(journalKey: string): Promise<JournalContent | undefined> {
        try {
            const entries = await this.core.readRemoteJournal(journalKey);
            const pure = entries.every(isChunkEntry);
            // The chunks of a journal which also holds a document are of no interest: it is never removed.
            return { pure, chunkIDs: pure ? entries.map((entry) => entry._id as string) : [] };
        } catch (ex) {
            Logger(`Cleaning the remote: ${journalKey} could not be read, and is tried again later`, LOG_LEVEL_VERBOSE);
            Logger(ex, LOG_LEVEL_VERBOSE);
            return undefined;
        }
    }

    /** Drop what is known of journals which are no longer in the remote. */
    private async forgetUnlistedJournals(listed: Set<string>): Promise<void> {
        const store = this.core.keyValueStore<JournalContent>();
        const prefix = this.contentKeyPrefix;
        for (const storeKey of await store.keys(prefix, `${prefix}\uffff`)) {
            if (!listed.has(storeKey.slice(prefix.length))) await store.delete(storeKey);
        }
    }

    /**
     * Look through every document of the local database for the chunks which are in use.
     *
     * A chunk is in use when a winning revision or a conflicting revision uses it, and that revision is not
     * deleted. The sequence of the database is noted first: a removal which follows is only valid while the
     * database has not changed since.
     *
     * @returns `stopped` when a stop was requested, and `timed-out` when a mobile device reached its time limit,
     * so that a large database does not keep a phone busy. Such a device leaves the cleaning to the others.
     */
    private async findChunksInUse(): Promise<DocumentScan | "stopped" | "timed-out"> {
        const core = this.core;
        const db = core.db;
        const updateSeq = (await db.info()).update_seq;
        const timeLimit = core.env.services.API.isMobile() ? MOBILE_SCAN_LIMIT_MS : undefined;
        const startedAt = core.now();
        const interruption = (): "stopped" | "timed-out" | undefined => {
            if (core.requestedStop) return "stopped";
            if (timeLimit !== undefined && core.now() - startedAt >= timeLimit) {
                Logger(`Cleaning the remote: the documents are too many to look through here`, LOG_LEVEL_VERBOSE);
                return "timed-out";
            }
        };
        const inUse = new Set<string>();
        let documents = 0;
        // The IDs of chunks begin with "h:". Every other document lies before or after them.
        const ranges = [{ endkey: "h:", inclusive_end: false }, { startkey: "h;" }];
        for (const range of ranges) {
            let from: { startkey: string; skip: number } | undefined;
            while (true) {
                const beforePage = interruption();
                if (beforePage) return beforePage;
                const page = await db.allDocs({
                    ...range,
                    ...from,
                    limit: SCAN_PAGE_SIZE,
                    include_docs: true,
                    conflicts: true,
                });
                const afterPage = interruption();
                if (afterPage) return afterPage;
                for (const { doc } of page.rows) {
                    const beforeDocument = interruption();
                    if (beforeDocument) return beforeDocument;
                    if (!doc) continue;
                    // A file keeps its list of chunks when it is deleted, so a deleted file counts as well.
                    if (Array.isArray((doc as { children?: unknown }).children)) documents++;
                    for (const id of chunksInUseBy(doc)) inUse.add(id);
                    for (const rev of doc._conflicts ?? []) {
                        const conflict = await db.get(doc._id, { rev });
                        const afterConflict = interruption();
                        if (afterConflict) return afterConflict;
                        for (const id of chunksInUseBy(conflict)) inUse.add(id);
                    }
                }
                if (page.rows.length < SCAN_PAGE_SIZE) break;
                from = { startkey: page.rows[page.rows.length - 1].key, skip: 1 };
            }
        }
        return interruption() ?? { inUse, updateSeq, documents };
    }

    /**
     * Store an assessment marker, and remember it so that it is deleted even when this round does not get to it.
     * Whether the storage has it is for the listing which follows to show.
     * @returns its name.
     */
    private async storeAssessmentMarker(): Promise<string> {
        const store = this.core.keyValueStore<string[]>();
        const key = `${ASSESSMENT_MARKER_PREFIX}${randomIdentity()}`;
        await store.set(this.markersKey, [...((await store.get(this.markersKey)) ?? []), key]);
        await this.core.uploadPackedJson(key, { version: 1 });
        return key;
    }

    /**
     * Delete the assessment markers of this device. One which stays is harmless, and goes in a later round.
     *
     * The names come from the key-value store of the device. Only a name of a marker is ever deleted on its word,
     * so that nothing which is kept there by mistake can cost the remote a journal.
     */
    private async deleteAssessmentMarkers(): Promise<void> {
        try {
            const store = this.core.keyValueStore<string[]>();
            const remembered = (await store.get(this.markersKey)) ?? [];
            const markers = remembered.filter((key) => key.startsWith(ASSESSMENT_MARKER_PREFIX));
            if (markers.length === 0 || (await this.core.storage.deleteFiles(markers))) {
                await store.delete(this.markersKey);
            }
        } catch (ex) {
            Logger(
                `Cleaning the remote: an assessment marker could not be deleted, and is left for later`,
                LOG_LEVEL_VERBOSE
            );
            Logger(ex, LOG_LEVEL_VERBOSE);
        }
    }

    /**
     * Remove journals which two assessments found unused, when the assessment still holds.
     *
     * It holds only while the local database has not changed since it was looked through, and the remote shows no
     * journal with documents which this device has not received: such a journal may hold a document which uses the
     * chunks.
     *
     * @returns the journals which were deleted.
     */
    private async remove(
        removable: string[],
        contents: Map<string, JournalContent>,
        assessedSeq: number | string
    ): Promise<string[]> {
        if ((await this.core.db.info()).update_seq !== assessedSeq) return [];
        const marker = await this.storeAssessmentMarker();
        try {
            return await this.removeVerified(marker, removable, contents);
        } finally {
            await this.deleteAssessmentMarkers();
        }
    }

    /**
     * List the remote to verify a removal, publish the removal, and carry it out.
     *
     * A device can be suspended at any point and go on hours later, and its clock can be wrong. So whether the
     * removal is carried out is decided by two times of the storage: when it received the marker, which was
     * before the listing, and when it received the record: see {@link isRemovalInTime}. Every device which reads
     * the record decides by the same two times. The clock of this device is only asked how long it has itself
     * been since the listing, before the record is stored and before the journals are deleted.
     *
     * @param marker the assessment marker, which must be in the listing.
     */
    private async removeVerified(
        marker: string,
        removable: string[],
        contents: Map<string, JournalContent>
    ): Promise<string[]> {
        const core = this.core;
        const listedAt = core.now();
        const listing = await core.listRemote();
        // Storage which does not report when it received an object gives a removal nothing to go by.
        const assessmentStoredAt = listing.assessmentMarkers.find(({ key }) => key === marker)?.storedAt;
        if (assessmentStoredAt === undefined) return [];
        const checkPointInfo = await core.getCheckpointInfo();
        const listed = listing.journals.flat().map(({ key }) => key);
        const isUnseenJournalWithDocuments = (key: string) =>
            !key.endsWith(BULK_JOURNAL_SUFFIX) &&
            !checkPointInfo.receivedFiles.has(key) &&
            !checkPointInfo.sentFiles.has(key);
        if (listed.some(isUnseenJournalWithDocuments)) return [];
        const stillListed = new Set(listed);
        const journals = removable.filter((key) => stillListed.has(key));
        if (journals.length === 0) return [];
        // A clock which went backwards tells nothing of how long ago the listing was, so it counts as too long.
        const listingAge = () => core.now() - listedAt;
        const listingIsFresh = () => listingAge() >= 0 && listingAge() <= REMOVAL_FRESHNESS_MS;

        if (core.requestedStop || !listingIsFresh()) return [];
        const record: RemovalRecord = {
            version: 1,
            at: core.now(),
            assessmentStoredAt,
            chunkIDs: [...new Set(journals.flatMap((key) => contents.get(key)?.chunkIDs ?? []))],
            journals,
        };
        const recordKey = `${REMOVAL_RECORD_PREFIX}${await sha256Hex(JSON.stringify(record))}${REMOVAL_RECORD_SUFFIX}`;
        // Nothing is deleted unless the record is stored: without it no device would send the chunks again. The
        // upload is given up when the listing would be too old by the time it ends. A record which is already
        // stored is kept, so that an upload which is tried again cannot move the time the storage reports for it.
        const stored = await core.uploadPackedJson(recordKey, record, {
            keepExisting: true,
            abortAfterMs: REMOVAL_FRESHNESS_MS - listingAge(),
        });
        // An upload which did not report success may still be under way: an attempt which is tried again can reach
        // the storage after this device has looked, and on storage which replaces a stored object it moves the
        // time of the record. So this device acts only on an upload which has ended with the record stored. A
        // record which was stored without that being reported is met in the next listing, like any other.
        if (!stored) return [];
        // The storage is asked when it received the record. A record which came too late stays where it is: this
        // device does not act on it, and neither does any device which reads it.
        const recordStoredAt = (await core.listRemote()).removalRecords.find(({ key }) => key === recordKey)?.storedAt;
        if (recordStoredAt === undefined || !isRemovalInTime(assessmentStoredAt, recordStoredAt)) return [];
        await core.applyRemovalRecords([{ key: recordKey, record }]);

        // The record stands whatever happens now. Journals which outlive it are removed by a later round, with a
        // record of its own, and no device which has read this one fetches them.
        if (core.requestedStop || !listingIsFresh()) return [];
        const objects = journals.flatMap((key) => (listing.indexed.has(key) ? [key, journalIndexKey(key)] : [key]));
        for (let index = 0; index < objects.length; index += DELETE_BATCH_SIZE) {
            if (!(await core.storage.deleteFiles(objects.slice(index, index + DELETE_BATCH_SIZE)))) return [];
        }
        return journals;
    }
}
