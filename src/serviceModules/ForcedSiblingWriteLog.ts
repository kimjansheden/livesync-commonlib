import { serialized } from "octagonal-wheels/concurrency/lock";
import type { FilePathWithPrefix } from "@lib/common/types";
import type { FileReflectionProvenance } from "@lib/interfaces/FileReflectionProvenance";

/**
 * The branch of the file handler which forced a revision onto an explicit base.
 *
 * Each code names one call site, so a record read from a device tells which decision created a branch.
 */
export type ForcedSiblingWriteReason =
    | "offline-edit-on-recorded-revision"
    | "offline-unrecorded-beside-winner"
    | "selected-base-revision"
    | "store-unrecorded-during-offline-preparation"
    | "conflicted-displayed-branch"
    | "conflicted-unknown-branch"
    | "retry-beside-concurrent-revision"
    | "beside-unreflected-winner"
    | "reflection-own-revision-advanced"
    | "reflection-preserve-emptied"
    | "reflection-preserve-local-change"
    | "deletion-on-recorded-revision"
    | "deletion-on-displayed-branch"
    | "rename-source-deletion-on-displayed-branch";

/** One forced write of a revision which can become a sibling of the winner. */
export type ForcedSiblingWrite = {
    path: FilePathWithPrefix;
    reason: ForcedSiblingWriteReason;
    /** The exact revision the write was stored on, when the handler chose it. */
    baseRevision?: string;
    /** The revision whose parent became the base, for a write preserved as a conflict of that revision. */
    siblingOf?: string;
    /** The winning revision just before the write. */
    winnerRevision?: string;
    storedRevision: string;
    /** Milliseconds since the epoch. */
    at: number;
};

/** Name of the ring among the device-local diagnostics of the provenance store. */
export const FORCED_SIBLING_WRITES_KEY = "forced-sibling-writes-v1";

/** Entries the ring keeps; the oldest are dropped first. */
export const FORCED_SIBLING_WRITE_LIMIT = 50;

/** Lock shared by every handler of a process, because they share the backing store. */
const RING_LOCK = "forced-sibling-writes";

function isEntry(value: unknown): value is ForcedSiblingWrite {
    const entry = value as Partial<ForcedSiblingWrite> | null;
    return (
        typeof entry === "object" &&
        entry !== null &&
        typeof entry.path === "string" &&
        typeof entry.reason === "string" &&
        typeof entry.storedRevision === "string" &&
        typeof entry.at === "number"
    );
}

/**
 * A small persistent ring of the forced writes this device made, kept beside the reflection provenance.
 *
 * It lives in the host's key-value store, so it survives log rotation and restarts. A host whose provenance cannot
 * keep diagnostics keeps the ring in memory for the life of the handler instead. Neither reading nor writing it ever
 * fails a store; the caller logs a failure and goes on.
 */
export class ForcedSiblingWriteLog {
    private memory: ForcedSiblingWrite[] = [];

    constructor(private readonly provenance: FileReflectionProvenance | undefined) {}

    private get persistent() {
        const provenance = this.provenance;
        return provenance?.readDiagnostic && provenance.writeDiagnostic ? provenance : undefined;
    }

    async append(entry: ForcedSiblingWrite): Promise<void> {
        await serialized(RING_LOCK, async () => {
            const next = [...(await this.readUnlocked()), entry].slice(-FORCED_SIBLING_WRITE_LIMIT);
            this.memory = next;
            await this.persistent?.writeDiagnostic!(FORCED_SIBLING_WRITES_KEY, next);
        });
    }

    /** The recorded writes, oldest first. */
    async read(): Promise<ForcedSiblingWrite[]> {
        return await serialized(RING_LOCK, () => this.readUnlocked());
    }

    private async readUnlocked(): Promise<ForcedSiblingWrite[]> {
        const persistent = this.persistent;
        if (!persistent) return [...this.memory];
        const stored = await persistent.readDiagnostic!(FORCED_SIBLING_WRITES_KEY);
        return Array.isArray(stored) ? stored.filter(isEntry) : [];
    }
}
