import type { EntryLeaf } from "@lib/common/types";
import { Semaphore } from "octagonal-wheels/concurrency/semaphore";
import { parseJournalIndex } from "./JournalSyncTypes";
import { JournalStorageReadStatuses, type JournalStorageReadResult } from "./objectstore/JournalStorageAdapter";

export interface ChunkJournalLocator {
    version: 1;
    journal: string;
}
export interface JournalOnDemandDependencies {
    list(): Promise<string[]>;
    readIndex(journal: string): Promise<JournalStorageReadResult<unknown>>;
    readJournal(journal: string): Promise<EntryLeaf[]>;
    writeIndex(journal: string, ids: string[]): Promise<boolean>;
    getLocator(id: string): Promise<unknown>;
    putLocator(id: string, value: ChunkJournalLocator): Promise<void>;
    mayRun(): boolean;
}

/** Locate immutable chunks without applying document revisions or advancing transfer checkpoints. */
export class JournalOnDemandChunks {
    private readonly reading = new Map<string, Promise<EntryLeaf[]>>();
    private readonly journalReads = Semaphore(4);
    constructor(private readonly deps: JournalOnDemandDependencies) {}

    private checkRunning(): void {
        if (!this.deps.mayRun()) throw new Error("On-demand journal reading stopped");
    }

    private async read(journal: string): Promise<EntryLeaf[]> {
        this.checkRunning();
        const pending = this.reading.get(journal);
        if (pending) return await pending;
        const reading = (async () => {
            const release = await this.journalReads.acquire();
            try {
                this.checkRunning();
                return await this.deps.readJournal(journal);
            } finally {
                release();
            }
        })();
        this.reading.set(journal, reading);
        try {
            const chunks = await reading;
            this.checkRunning();
            return chunks;
        } finally {
            this.reading.delete(journal);
        }
    }

    private async ids(journal: string, allowLegacyRead = true): Promise<{ ids: string[]; missingIndex: boolean }> {
        this.checkRunning();
        const index = await this.deps.readIndex(journal);
        this.checkRunning();
        if (index.status === JournalStorageReadStatuses.AVAILABLE)
            return { ids: parseJournalIndex(index.value).chunkIDs, missingIndex: false };
        if (index.status !== JournalStorageReadStatuses.NOT_FOUND) throw new Error("Journal index is unavailable");
        if (!allowLegacyRead) return { ids: [], missingIndex: true };
        return { ids: (await this.read(journal)).map((chunk) => chunk._id), missingIndex: true };
    }

    private async remember(journal: string, ids: readonly string[]): Promise<void> {
        for (const id of ids) {
            this.checkRunning();
            if (!id.startsWith("h:")) throw new Error("Journal index contains a non-chunk identifier");
            await this.deps.putLocator(id, { version: 1, journal });
        }
    }

    /** Explicit preparation indexes legacy journals once; opening a file publishes no remote objects. */
    async prepare(): Promise<{ journals: number; indexed: number; chunks: number }> {
        this.checkRunning();
        const journals = [...new Set(await this.deps.list())];
        this.checkRunning();
        let indexed = 0,
            chunks = 0;
        for (const journal of journals) {
            const result = await this.ids(journal);
            await this.remember(journal, result.ids);
            if (result.missingIndex) {
                this.checkRunning();
                if (!(await this.deps.writeIndex(journal, result.ids)))
                    throw new Error("Journal index could not be stored");
                indexed++;
            }
            chunks += result.ids.length;
        }
        return { journals: journals.length, indexed, chunks };
    }

    async fetch(ids: readonly string[]): Promise<EntryLeaf[]> {
        this.checkRunning();
        if (ids.length > 1000 || ids.some((id) => !id.startsWith("h:") || id.length < 3 || id.length > 512))
            throw new Error("Invalid on-demand chunk request");
        const wanted = new Set(ids);
        if (!wanted.size) return [];
        const journals = new Set(await this.deps.list());
        this.checkRunning();
        const locations = new Map<string, Set<string>>(),
            located = new Set<string>();
        const add = (journal: string, id: string) => {
            if (located.has(id)) return;
            const group = locations.get(journal) ?? new Set<string>();
            group.add(id);
            locations.set(journal, group);
            located.add(id);
        };
        for (const id of wanted) {
            const locator = (await this.deps.getLocator(id)) as Partial<ChunkJournalLocator> | null;
            if (locator?.version === 1 && typeof locator.journal === "string" && journals.has(locator.journal))
                add(locator.journal, id);
        }
        if (located.size < wanted.size) {
            const legacy: string[] = [];
            const search = async (candidates: string[], legacyRead: boolean) => {
                // Bound remote work and settle each batch before returning or propagating a failure.
                for (let offset = 0; offset < candidates.length; offset += 32) {
                    this.checkRunning();
                    const batch = candidates.slice(offset, offset + 32);
                    const results = await Promise.allSettled(
                        batch.map(async (journal) =>
                            legacyRead
                                ? { ids: (await this.read(journal)).map((chunk) => chunk._id), missingIndex: true }
                                : await this.ids(journal, false)
                        )
                    );
                    this.checkRunning();
                    for (const result of results) if (result.status === "rejected") throw result.reason;
                    for (let i = 0; i < results.length; i++) {
                        const result = results[i];
                        if (result.status !== "fulfilled") continue;
                        if (!legacyRead && result.value.missingIndex) legacy.push(batch[i]);
                        const matching = result.value.ids.filter((id) => {
                            if (!id.startsWith("h:")) throw new Error("Journal index contains a non-chunk identifier");
                            return wanted.has(id) && !located.has(id);
                        });
                        await this.remember(batch[i], matching);
                        for (const id of matching) add(batch[i], id);
                    }
                    if (located.size === wanted.size) break;
                }
            };
            await search([...journals], false);
            if (located.size < wanted.size) await search(legacy, true);
        }
        const found = new Map<string, EntryLeaf>();
        for (const [journal, requested] of locations) {
            for (const chunk of await this.read(journal)) {
                if (!requested.has(chunk._id)) continue;
                if (chunk.type !== "leaf" || typeof chunk.data !== "string") throw new Error("Invalid requested chunk");
                found.set(chunk._id, chunk);
            }
        }
        this.checkRunning();
        return [...wanted].flatMap((id) => {
            const chunk = found.get(id);
            return chunk ? [chunk] : [];
        });
    }
}
