import { describe, expect, it, vi } from "vitest";
import type { DocumentID, EntryLeaf } from "@lib/common/types";
import { JournalOnDemandChunks, type JournalOnDemandDependencies } from "./JournalOnDemandChunks";

const leaf = (id: string): EntryLeaf => ({ _id: id as DocumentID, _rev: "", type: "leaf", data: `synthetic ${id}` });

function fixture() {
    const locators = new Map<string, unknown>();
    const deps: JournalOnDemandDependencies = {
        list: vi.fn(async () => ["a-docs.jsonl.gz", "b-chunks.jsonl.gz"]),
        readIndex: vi.fn(async (journal) => ({
            status: "available",
            value: { version: 1, chunkIDs: journal.startsWith("a") ? ["h:a"] : ["h:b", "h:c"] },
        })),
        readJournal: vi.fn(async (journal) => (journal.startsWith("a") ? [leaf("h:a")] : [leaf("h:b"), leaf("h:c")])),
        writeIndex: vi.fn(async () => true),
        getLocator: vi.fn(async (id) => locators.get(id)),
        putLocator: vi.fn(async (id, value) => {
            locators.set(id, value);
        }),
        mayRun: () => true,
    };
    return { deps, locators, reader: new JournalOnDemandChunks(deps) };
}

describe("on-demand R2 journal chunks", () => {
    it("prepares locator records and then fetches only the requested journal and chunks", async () => {
        const h = fixture();
        expect(await h.reader.prepare()).toEqual({ journals: 2, indexed: 0, chunks: 3 });
        vi.mocked(h.deps.readIndex).mockClear();
        expect(await h.reader.fetch(["h:c", "h:c"])).toEqual([leaf("h:c")]);
        expect(h.deps.readJournal).toHaveBeenCalledExactlyOnceWith("b-chunks.jsonl.gz");
        expect(h.deps.readIndex).not.toHaveBeenCalled();
        expect(h.deps.writeIndex).not.toHaveBeenCalled();
    });

    it("reads an unindexed legacy journal once for preparation and publishes its chunk-only index", async () => {
        const h = fixture();
        vi.mocked(h.deps.readIndex).mockResolvedValue({ status: "not-found" });
        expect((await h.reader.prepare()).indexed).toBe(2);
        expect(h.deps.writeIndex).toHaveBeenCalledWith("a-docs.jsonl.gz", ["h:a"]);
        expect(h.deps.writeIndex).toHaveBeenCalledWith("b-chunks.jsonl.gz", ["h:b", "h:c"]);
    });

    it("never turns an unavailable or malformed index into a legacy fallback", async () => {
        for (const value of [
            { status: "unavailable" as const, error: Error("offline") },
            { status: "available" as const, value: { version: 99, chunkIDs: [] } },
        ]) {
            const h = fixture();
            vi.mocked(h.deps.readIndex).mockResolvedValue(value);
            await expect(h.reader.fetch(["h:a"])).rejects.toThrow();
            expect(h.deps.readJournal).not.toHaveBeenCalled();
            expect(h.deps.writeIndex).not.toHaveBeenCalled();
        }
    });

    it("discards stale locators whose journal is absent from the fresh listing", async () => {
        const h = fixture();
        h.locators.set("h:c", { version: 1, journal: "retired-chunks.jsonl.gz" });
        expect(await h.reader.fetch(["h:c"])).toEqual([leaf("h:c")]);
        expect(h.deps.readJournal).not.toHaveBeenCalledWith("retired-chunks.jsonl.gz");
    });

    it("does not publish a remote index while opening a legacy attachment", async () => {
        const h = fixture();
        vi.mocked(h.deps.readIndex).mockResolvedValue({ status: "not-found" });
        expect(await h.reader.fetch(["h:a"])).toEqual([leaf("h:a")]);
        expect(h.deps.writeIndex).not.toHaveBeenCalled();
    });

    it("stops a request when its owner unloads or changes remote", async () => {
        const h = fixture();
        let active = true;
        h.deps.mayRun = () => active;
        vi.mocked(h.deps.list).mockImplementation(async () => {
            active = false;
            return ["a-docs.jsonl.gz"];
        });
        await expect(h.reader.fetch(["h:a"])).rejects.toThrow("stopped");
        expect(h.deps.readJournal).not.toHaveBeenCalled();
    });

    it("returns no unrelated chunk when an authenticated index claims content the journal does not hold", async () => {
        const h = fixture();
        h.locators.set("h:a", { version: 1, journal: "b-chunks.jsonl.gz" });
        expect(await h.reader.fetch(["h:a"])).toEqual([]);
    });

    it("rejects requests for metadata documents and unbounded batches", async () => {
        const h = fixture();
        await expect(h.reader.fetch(["note.md"])).rejects.toThrow("Invalid");
        await expect(h.reader.fetch(Array(1001).fill("h:a"))).rejects.toThrow("Invalid");
        expect(h.deps.list).not.toHaveBeenCalled();
    });
});
