export type CheckPointInfo = {
    lastLocalSeq: number | string;
    journalEpoch: string;
    knownIDs: Set<string>;
    sentIDs: Set<string>;
    receivedFiles: Set<string>;
    sentFiles: Set<string>;
    /**
     * Incremented whenever an update removes exchange history, such as a reset of the sent history or a
     * confirmed remote wipe. A send only records its progress while this value is the one it started with.
     */
    resetGeneration: number;
};
/**
 * Creates an empty checkpoint. Updates add to its sets in place, so each checkpoint needs its own sets
 * instead of sharing those of {@link CheckPointInfoDefault}.
 */
export function createCheckPointInfoDefault(): CheckPointInfo {
    return {
        lastLocalSeq: 0,
        journalEpoch: "",
        knownIDs: new Set<string>(),
        sentIDs: new Set<string>(),
        receivedFiles: new Set<string>(),
        sentFiles: new Set<string>(),
        resetGeneration: 0,
    };
}
export const CheckPointInfoDefault: CheckPointInfo = createCheckPointInfoDefault();

/** The name prefix of the index of a journal of large chunks. The journal's name follows it. */
export const JOURNAL_INDEX_PREFIX = "_index-";
/** The name prefix and suffix of a removal record. Names which begin with an underscore are never journals. */
export const REMOVAL_RECORD_PREFIX = "_removed-";
export const REMOVAL_RECORD_SUFFIX = ".json.gz";
/**
 * The name prefix of an assessment marker: a small object which a device stores before it lists the remote to
 * verify a removal. The time at which the storage received it is the storage's own time for that listing.
 */
export const ASSESSMENT_MARKER_PREFIX = "_assessment-";

/**
 * A small object which changes after a device stores journals or a removal record.
 *
 * Reading this fixed key is cheaper than listing the object store. A missing or stale marker never becomes the
 * only way to discover changes: every client still lists the whole remote periodically.
 */
export const REMOTE_CHANGE_MARKER_KEY = "_remote-change.json";

/** The longest time a client reuses a remote listing without listing the storage again. */
export const REMOTE_FULL_LIST_INTERVAL_MS = 10 * 60 * 1000;

export type RemoteChangeMarker = { version: 1; token: string };

/** Accept only the marker format this version writes. */
export function parseRemoteChangeMarker(value: unknown): RemoteChangeMarker {
    const marker = value as Partial<RemoteChangeMarker> | null;
    if (!marker || marker.version !== 1 || typeof marker.token !== "string" || !/^[a-f0-9]{32}$/u.test(marker.token)) {
        throw new Error("The remote change marker is not valid");
    }
    return { version: 1, token: marker.token };
}

/**
 * The index of a journal of large chunks: the IDs of the chunks it holds.
 *
 * It lets a device which has not fetched the journal tell whether anything still uses its chunks.
 */
export type JournalIndex = { version: 1; chunkIDs: string[] };

/**
 * A removal record: the chunks which were removed from the remote, and the journals which held them.
 *
 * It is stored before the journals are deleted. A device which reads it no longer counts those chunks as present
 * in the remote, so it sends them again when a document uses them, and never fetches those journals.
 *
 * - `at` is when the record was made, by the clock of the device. It only gives every removal a record of its
 *   own, so that a journal which outlived one removal is covered by the next.
 * - `assessmentStoredAt` is when the storage received the assessment marker of the removal, by the storage's
 *   own clock. The remote was listed to verify the removal after that.
 */
export type RemovalRecord = {
    version: 1;
    at: number;
    assessmentStoredAt: number;
    chunkIDs: string[];
    journals: string[];
};

/** How long after the remote was listed to verify a removal the removal may still be carried out. */
export const REMOVAL_FRESHNESS_MS = 60 * 1000;

/**
 * Whether a removal record reached the storage soon enough after the removal was verified to be acted on.
 *
 * Both times are the storage's own: when it received the assessment marker, before the verifying listing, and
 * when it received the record. No clock of a device is involved, so the device which removes and every device
 * which reads the record decide alike, however wrong their clocks are. A device which was suspended in between
 * stores its record late; by then a file may use the chunks again, and nobody acts on such a record.
 */
export function isRemovalInTime(assessmentStoredAt: number, recordStoredAt: number): boolean {
    const delay = recordStoredAt - assessmentStoredAt;
    return delay >= 0 && delay <= REMOVAL_FRESHNESS_MS;
}

/** A document which was sent without some of its chunks, because the remote was taken to hold them. */
export type RelianceDocument = { id: string; rev: string; sentAt: number };

/**
 * What a sending device keeps beside its checkpoint to mend a send which crossed a removal: the documents it
 * sent in reliance on chunks in the remote, and the chunks it must send again.
 */
export type JournalReliance = { documents: RelianceDocument[]; resend: string[] };

function isStringArray(value: unknown): value is string[] {
    return Array.isArray(value) && value.every((item) => typeof item === "string");
}

/** Check a downloaded journal index. Anything else than a known version is refused. */
export function parseJournalIndex(value: unknown): JournalIndex {
    const index = value as Partial<JournalIndex> | null;
    if (!index || index.version !== 1 || !isStringArray(index.chunkIDs)) {
        throw new Error("The journal index is not valid");
    }
    return { version: 1, chunkIDs: index.chunkIDs };
}

/** Check a downloaded removal record. Anything else than a known version is refused. */
export function parseRemovalRecord(value: unknown): RemovalRecord {
    const record = value as Partial<RemovalRecord> | null;
    if (
        !record ||
        record.version !== 1 ||
        typeof record.at !== "number" ||
        typeof record.assessmentStoredAt !== "number" ||
        !isStringArray(record.chunkIDs) ||
        !isStringArray(record.journals)
    ) {
        throw new Error("The removal record is not valid");
    }
    return {
        version: 1,
        at: record.at,
        assessmentStoredAt: record.assessmentStoredAt,
        chunkIDs: record.chunkIDs,
        journals: record.journals,
    };
}

/** Read what a sending device kept, tolerating a store which holds nothing or something else under the key. */
export function parseJournalReliance(value: unknown): JournalReliance {
    const reliance = value as Partial<JournalReliance> | null | undefined;
    const documents = Array.isArray(reliance?.documents)
        ? reliance.documents.filter(
              (doc): doc is RelianceDocument =>
                  !!doc && typeof doc.id === "string" && typeof doc.rev === "string" && typeof doc.sentAt === "number"
          )
        : [];
    return { documents, resend: isStringArray(reliance?.resend) ? reliance.resend : [] };
}

/**
 * The name suffix of a journal which holds nothing but large chunks.
 *
 * Chunks are immutable and named after their content, so the order in which they arrive does not matter. A receiver
 * may therefore fetch these journals after every other journal, while a journal which can hold a document is always
 * applied in upload order. The format is that of any other journal, so a client which does not know the suffix reads
 * such a journal like the others.
 */
export const BULK_JOURNAL_SUFFIX = "-chunks.jsonl.gz";

const textEncoder = new TextEncoder();
export async function sha256Hex(value: string): Promise<string> {
    const digest = await globalThis.crypto.subtle.digest("SHA-256", textEncoder.encode(value));
    return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}

export function isChunkEntry(entry: { _id: string }): boolean {
    return entry._id.startsWith("h:");
}

/** The name of the index object of a journal. */
export function journalIndexKey(journalKey: string): string {
    return `${JOURNAL_INDEX_PREFIX}${journalKey}`;
}

/**
 * The chunks which a revision of a document keeps in use. A deleted revision keeps its list of chunks, but uses
 * none of them.
 */
export function chunksInUseBy(doc: object): string[] {
    const entry = doc as { _deleted?: boolean; deleted?: boolean; children?: unknown };
    if (entry._deleted === true || entry.deleted === true || !Array.isArray(entry.children)) return [];
    return entry.children.filter((id): id is string => typeof id === "string");
}
