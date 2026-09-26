import type { FilePathWithPrefix } from "@lib/common/types";
import type { SimpleStore } from "@lib/common/utils";

export type FileReflectionProvenanceRecord = {
    /** Exact database revision which most recently produced the storage state. */
    revision: string;
    /**
     * Raw modification time observed from this device's storage after reflection.
     *
     * It never identifies a branch. Together with `revision` and the size of that revision, an unchanged modification
     * time is accepted as proof that storage still holds that revision where reading the file would cost too much: for
     * files of at least one mebibyte, the storage event of this device's own write, and for files of at least
     * `LARGE_FILE_BYTES` (50 MiB), an incoming deletion or revision made on that revision, whose preservation check
     * then does not read the file. That check accepts only a record written by a reflection, and compares with a stat
     * of the file system itself. Below those sizes the content is compared as before.
     */
    observedStorageMtime?: number;
    /**
     * Whether this record was written because the database content was reflected into storage.
     *
     * A record written while storing storage into the database describes a file the device merely read, which
     * says nothing about how that file came to hold its content. Only a reflection proves that storage holds
     * what the database produced.
     */
    reflectedFromDatabase?: boolean;
    /** Database revision being published from a complete staged file; absence is recoverable, not deletion. */
    pendingPublication?: { revision: string; token: string };
    /** Unreleased in-place candidates are recognised only to fail closed, never to infer file ownership. */
    incompleteWriteRevision?: string;
};

export interface FileReflectionProvenance {
    get(path: FilePathWithPrefix): Promise<FileReflectionProvenanceRecord | undefined>;
    set(path: FilePathWithPrefix, record: FileReflectionProvenanceRecord): Promise<void>;
    delete(path: FilePathWithPrefix): Promise<void>;
    move(from: FilePathWithPrefix, to: FilePathWithPrefix): Promise<void>;
}

/**
 * Device-local provenance backed by an existing host-owned key-value store.
 *
 * The revision is authoritative. The raw storage mtime never proves branch
 * identity. It proves content only together with the recorded revision and its
 * size, for files large enough that reading them costs too much, as described
 * for `observedStorageMtime`; otherwise it is a fast change hint.
 * The host may construct this object before opening its store, but it must not
 * invoke provenance operations until its normal storage lifecycle is ready.
 * Store failures are reported to the caller rather than hidden by readiness
 * waits, so lifecycle violations and reset races cannot hang file processing.
 */
export class StoredFileReflectionProvenance implements FileReflectionProvenance {
    constructor(private readonly store: SimpleStore<FileReflectionProvenanceRecord>) {}

    async get(path: FilePathWithPrefix): Promise<FileReflectionProvenanceRecord | undefined> {
        return (await this.store.get(path)) ?? undefined;
    }

    async set(path: FilePathWithPrefix, record: FileReflectionProvenanceRecord): Promise<void> {
        await this.store.set(path, record);
    }

    async delete(path: FilePathWithPrefix): Promise<void> {
        await this.store.delete(path);
    }

    async move(from: FilePathWithPrefix, to: FilePathWithPrefix): Promise<void> {
        const record = await this.get(from);
        if (record) {
            await this.set(to, record);
        }
        await this.delete(from);
    }
}
