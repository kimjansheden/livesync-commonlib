import type { FilePathWithPrefix, LoadedEntry, MetaEntry, UXFileInfo, UXFileInfoStub } from "@lib/common/types";

/**
 * Binary entry content assembled into one buffer.
 *
 * `unsupported` means the entry needs the general loading path, for example a legacy encoding.
 * `size-mismatch` reports the decoded size, which is at least the recorded size plus one chunk when it overflows.
 */
/** Thrown when an entry cannot be read as successive binary parts and needs the general loading path. */
export class UnsupportedBinaryContentError extends Error {}

/** Thrown when the decoded chunks do not add up to the size recorded in the metadata. */
export class BinaryContentSizeMismatchError extends Error {
    constructor(readonly decodedSize: number) {
        super(`The decoded size ${decodedSize} does not match the recorded size`);
    }
}

/** Whether an error means the entry itself cannot be written in parts, rather than a transient failure. */
export function isPermanentBinaryContentError(error: unknown): boolean {
    return error instanceof UnsupportedBinaryContentError || error instanceof BinaryContentSizeMismatchError;
}

export type BinaryEntryContent =
    | { status: "ok"; data: ArrayBuffer }
    | { status: "size-mismatch"; decodedSize: number }
    | { status: "unsupported" };

/**
 * Whether an entry's content can be written as successive parts, found without holding or decoding it.
 *
 * `missing` means a chunk is not available yet. `unsupported` means the entry needs the general loading path.
 */
export type BinaryContentAvailability = "streamable" | "missing" | "unsupported";

export interface DatabaseFileAccess {
    delete: (file: UXFileInfoStub | FilePathWithPrefix, rev?: string) => Promise<boolean>;
    store: (file: UXFileInfo, force?: boolean, skipCheck?: boolean) => Promise<boolean>;
    /** Store a file as a child of an exact revision and return the created revision. */
    storeWithBaseRevision: (
        file: UXFileInfo,
        baseRevision: string | undefined,
        skipCheck?: boolean
    ) => Promise<string | false>;
    /**
     * Store a file as a child of an exact revision only while that revision remains a live leaf.
     *
     * Unlike {@link storeWithBaseRevision}, this method does not force a branch below an obsolete
     * revision. 'Live leaf' means any current revision-tree leaf, including a non-winning conflict
     * leaf or a logical-deletion leaf. In particular, it returns `false` when another writer has
     * already advanced the supplied base; ordinary target and Chunk validation can also refuse the
     * write without creating a Metadata successor.
     */
    storeWithLiveBaseRevision: (file: UXFileInfo, baseRevision: string, skipCheck?: boolean) => Promise<string | false>;
    storeAsConflictedRevision: (file: UXFileInfo, currentRev: string, skipCheck?: boolean) => Promise<boolean>;
    /** Preserve unknown storage content as a conflict and return its exact revision. */
    storeAsConflictedRevisionWithResult: (
        file: UXFileInfo,
        currentRev: string,
        skipCheck?: boolean
    ) => Promise<string | false>;
    /** Store a user deletion as a visible logical-deletion child of an exact revision. */
    storeDeletionWithBaseRevision: (
        file: UXFileInfoStub | FilePathWithPrefix,
        baseRevision: string
    ) => Promise<string | false>;
    storeContent(path: FilePathWithPrefix, content: string): Promise<boolean>;
    createChunks: (file: UXFileInfo, force?: boolean, skipCheck?: boolean) => Promise<boolean>;
    hasContentInRevisionHistory: (
        file: UXFileInfoStub | FilePathWithPrefix,
        content: string | string[] | Blob | ArrayBuffer,
        currentRev?: string
    ) => Promise<boolean>;
    /**
     * Whether `revision` is in the history of `branchRevision`, which counts as its own history.
     *
     * Decided from the revision tree, so no content is loaded. Optional for compatibility hosts; callers then compare
     * content with {@link hasContentInRevisionHistory} instead.
     */
    isRevisionInHistory?: (
        file: UXFileInfoStub | FilePathWithPrefix,
        revision: string,
        branchRevision: string
    ) => Promise<boolean>;
    /** Return every available revision whose content exactly matches the supplied bytes. */
    findContentRevisions: (
        file: UXFileInfoStub | FilePathWithPrefix,
        content: string | string[] | Blob | ArrayBuffer,
        currentRev?: string
    ) => Promise<string[]>;
    fetch: (
        file: UXFileInfoStub | FilePathWithPrefix,
        rev?: string,
        waitForReady?: boolean,
        skipCheck?: boolean
    ) => Promise<UXFileInfo | false>;
    fetchEntryFromMeta: (meta: MetaEntry, waitForReady?: boolean, skipCheck?: boolean) => Promise<LoadedEntry | false>;
    /**
     * Assemble a binary entry's content while holding only one decoded copy and a small batch of chunks.
     * Optional for compatibility hosts; callers fall back to {@link fetchEntryFromMeta}.
     */
    fetchBinaryContentFromMeta?: (meta: MetaEntry, waitForReady?: boolean) => Promise<BinaryEntryContent | false>;
    /**
     * Yield a binary entry's content as successive parts, holding only one part at a time.
     *
     * Throws when the entry cannot be streamed, so a caller must begin iterating before it starts writing.
     * Optional for compatibility hosts.
     */
    iterateBinaryContentFromMeta?: (meta: MetaEntry, waitForReady?: boolean) => AsyncGenerator<Uint8Array>;
    /**
     * Whether the entry can be written as successive parts, decided without decoding its content.
     *
     * A caller which is about to replace a complete file asks first, so a refusal cannot truncate that file.
     */
    canStreamBinaryContentFromMeta?: (meta: MetaEntry, waitForReady?: boolean) => Promise<boolean>;
    /**
     * Whether a binary entry's chunks are all available locally, one of them is missing, or the entry needs the
     * general loading path, found in small batches without holding or decoding its content.
     *
     * Optional for compatibility hosts; callers fall back to {@link fetchEntry}.
     */
    inspectBinaryContentFromMeta?: (meta: MetaEntry, waitForReady?: boolean) => Promise<BinaryContentAvailability>;
    fetchEntryMeta: (
        file: UXFileInfoStub | FilePathWithPrefix,
        rev?: string,
        skipCheck?: boolean
    ) => Promise<MetaEntry | false>;
    fetchEntry: (
        file: UXFileInfoStub | FilePathWithPrefix,
        rev?: string,
        waitForReady?: boolean,
        skipCheck?: boolean
    ) => Promise<LoadedEntry | false>;
    getConflictedRevs: (file: UXFileInfoStub | FilePathWithPrefix) => Promise<string[]>;
}
