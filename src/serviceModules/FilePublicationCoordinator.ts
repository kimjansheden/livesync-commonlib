import { serialized } from "octagonal-wheels/concurrency/lock";
import type { FilePathWithPrefix, UXStat } from "@lib/common/types";
import type {
    FileReflectionProvenance,
    FileReflectionProvenanceRecord,
} from "@lib/interfaces/FileReflectionProvenance";

export class UnknownFileWriteStateError extends Error {
    constructor() {
        super("File publication state is unavailable; the operation must be retried");
    }
}

type Dependencies = {
    store: FileReflectionProvenance;
    normalise: (path: string) => string;
    stat: (path: FilePathWithPrefix) => Promise<UXStat | null>;
};

/** One owner for the existing provenance record and the short, recoverable publication gap. */
export class FilePublicationCoordinator {
    constructor(private readonly dependencies: Dependencies) {}

    async run<T>(path: FilePathWithPrefix, operation: () => Promise<T>): Promise<T> {
        return serialized(`write-in-parts:${this.dependencies.normalise(path)}`, operation);
    }

    async get(path: FilePathWithPrefix): Promise<FileReflectionProvenanceRecord | undefined> {
        try {
            const record = await this.dependencies.store.get(path);
            if (record?.incompleteWriteRevision) throw new UnknownFileWriteStateError();
            return record;
        } catch {
            throw new UnknownFileWriteStateError();
        }
    }

    private async edit(
        path: FilePathWithPrefix,
        transform: (record: FileReflectionProvenanceRecord | undefined) => FileReflectionProvenanceRecord | undefined
    ): Promise<void> {
        return serialized(`file-provenance:${this.dependencies.normalise(path)}`, async () => {
            try {
                const next = transform(await this.get(path));
                if (next) await this.dependencies.store.set(path, next);
                else await this.dependencies.store.delete(path);
            } catch {
                throw new UnknownFileWriteStateError();
            }
        });
    }

    async reflect(
        path: FilePathWithPrefix,
        revision: string,
        mtime?: number,
        reflected = false,
        token?: string
    ): Promise<void> {
        // Storage which is recorded again as the revision and time a reflection wrote still holds what the database
        // produced. The storage event of that write records it this way, and dropping the mark there would make
        // a reflected empty file look like one which was emptied on this device.
        const stillReflected = (current: FileReflectionProvenanceRecord | undefined) =>
            current?.reflectedFromDatabase === true &&
            current.revision === revision &&
            current.observedStorageMtime === mtime;
        await this.edit(path, (current) => ({
            revision,
            observedStorageMtime: mtime,
            ...(reflected || stillReflected(current) ? { reflectedFromDatabase: true } : {}),
            ...(current?.pendingPublication && current.pendingPublication.token !== token
                ? { pendingPublication: current.pendingPublication }
                : {}),
        }));
    }

    async delete(path: FilePathWithPrefix): Promise<void> {
        await this.edit(path, (current) => {
            if (current?.pendingPublication) throw new UnknownFileWriteStateError();
            return undefined;
        });
    }

    /** Called inside the host queue immediately before native publication, without re-entering the adapter. */
    async begin(path: FilePathWithPrefix, revision: string): Promise<string> {
        const token = crypto.randomUUID();
        await this.edit(path, (current) => ({
            ...current,
            revision: current?.revision ?? revision,
            pendingPublication: { revision, token },
        }));
        return token;
    }

    /** After publication has left its lifecycle lock, every existing target is a complete version. */
    async missingPublication(path: FilePathWithPrefix): Promise<string | undefined> {
        return this.run(path, async () => {
            const stat = await this.dependencies.stat(path);
            const record = await this.get(path);
            if (!stat) return record?.pendingPublication?.revision;
            if (record?.pendingPublication) {
                // Completion was interrupted. Drop uncertain branch provenance, not the complete local file.
                // The mark must terminate here or a later real user deletion would be mistaken for our gap.
                await this.edit(path, (current) => {
                    if (current?.pendingPublication?.token !== record.pendingPublication?.token)
                        throw new UnknownFileWriteStateError();
                    return undefined;
                });
            }
            return undefined;
        });
    }

    async discardForDatabaseDeletion(path: FilePathWithPrefix): Promise<void> {
        await this.edit(path, () => undefined);
    }

    async move(from: FilePathWithPrefix, to: FilePathWithPrefix): Promise<void> {
        if (from === to) return;
        const keys = [...new Set([from, to].map((p) => this.dependencies.normalise(p)))].sort();
        const lock = async (i: number): Promise<void> => {
            if (i < keys.length) return serialized(`write-in-parts:${keys[i]}`, () => lock(i + 1));
            const record = await this.get(from);
            if (!record) return;
            // A missing publication target is not a renameable user file.
            if (record.pendingPublication && !(await this.dependencies.stat(to)))
                throw new UnknownFileWriteStateError();
            await this.edit(to, (current) => {
                if (current?.pendingPublication) throw new UnknownFileWriteStateError();
                return record;
            });
            await this.edit(from, (current) => {
                if (current?.pendingPublication?.token !== record.pendingPublication?.token)
                    throw new UnknownFileWriteStateError();
                return undefined;
            });
        };
        await lock(0);
    }
}
