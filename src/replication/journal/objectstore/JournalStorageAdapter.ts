import type { RemoteDBStatus } from "@lib/replication/LiveSyncAbstractReplicator.ts";
import type { BucketSyncSetting } from "@lib/common/types.ts";

export const JournalStorageReadStatuses = {
    AVAILABLE: "available",
    NOT_FOUND: "not-found",
    UNAVAILABLE: "unavailable",
} as const;

export type JournalStorageReadResult<T> =
    | { status: typeof JournalStorageReadStatuses.AVAILABLE; value: T }
    | { status: typeof JournalStorageReadStatuses.NOT_FOUND }
    | { status: typeof JournalStorageReadStatuses.UNAVAILABLE; error: unknown };

export type JournalUploadOptions = {
    /**
     * Leave an object which is already stored under the key as it is, and report the upload as done.
     * The time at which the storage first received the object then stays the time it reports.
     */
    keepExisting?: boolean;
    /**
     * Give the upload up when it has not finished after this many milliseconds, and report it as failed.
     * The object may still have been stored. Storage which cannot abort a request may ignore this.
     */
    abortAfterMs?: number;
};

/** A stored object as a listing reports it. */
export type JournalStorageObject = {
    key: string;
    /** The stored size in bytes, left out when the storage does not report it. */
    size?: number;
    /** When the storage received the object, by its own clock, left out when the storage does not report it. */
    storedAt?: number;
};

export interface IJournalStorage {
    upload(key: string, data: Uint8Array, mime: string, options?: JournalUploadOptions): Promise<boolean>;
    download(key: string, ignoreCache?: boolean): Promise<Uint8Array | false>;
    downloadWithResult(key: string, ignoreCache?: boolean): Promise<JournalStorageReadResult<Uint8Array>>;
    listFiles(from: string, limit?: number): Promise<string[]>;
    /**
     * List every stored key in the order in which the storage received the objects, oldest first.
     * Keys which the storage received at the same time form one group, because their order cannot be told.
     * Storage which cannot tell when an object was stored may omit this.
     */
    listFilesInUploadOrder?(): Promise<string[][]>;
    /**
     * List every stored object in the groups and order of {@link listFilesInUploadOrder}, each with its size.
     * A caller which also needs the sizes asks for this instead, so that it lists the storage once.
     * Storage which cannot tell when an object was stored may omit this.
     */
    listObjectsInUploadOrder?(): Promise<JournalStorageObject[][]>;
    deleteFiles(keys: string[]): Promise<boolean>;
    isAvailable(): Promise<boolean>;
    getUsage(): Promise<false | RemoteDBStatus>;
    applyNewConfig(settings: BucketSyncSetting): void;
    /**
     * Abort requests which started before `startedBefore` and are still in flight.
     * Storage without abortable requests may omit this.
     * @returns the number of requests which were aborted.
     */
    abortRequestsStartedBefore?(startedBefore: number): number;
}
import type { LiveSyncJournalReplicatorEnv } from "@lib/replication/journal/LiveSyncJournalReplicatorEnv.ts";

export interface IJournalStorageAdapterClass {
    new (settings: BucketSyncSetting, env: LiveSyncJournalReplicatorEnv): IJournalStorage;
}
