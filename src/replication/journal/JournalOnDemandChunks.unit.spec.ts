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
    it("cancels queued legacy downloads and waits for the active downloads to settle", async () => {
        const h = fixture();
        vi.mocked(h.deps.list).mockResolvedValue(Array.from({ length: 32 }, (_, i) => `journal-${i}`));
        vi.mocked(h.deps.readIndex).mockResolvedValue({ status: "not-found" });
        let active = true;
        let entered!: () => void;
        let release!: () => void;
        const running = new Promise<void>((resolve) => {
            entered = resolve;
        });
        const waiting = new Promise<void>((resolve) => {
            release = resolve;
        });
        let downloads = 0;
        h.deps.mayRun = () => active;
        vi.mocked(h.deps.readJournal).mockImplementation(async () => {
            if (++downloads === 4) entered();
            await waiting;
            return [leaf("h:a")];
        });
        let settled = false;
        const reading = h.reader.fetch(["h:a"]).finally(() => {
            settled = true;
        });
        void reading.catch(() => undefined);
        await running;
        active = false;
        await Promise.resolve();
        expect(settled).toBe(false);
        release();
        await expect(reading).rejects.toThrow("stopped");
        expect(downloads).toBe(4);
        expect(h.deps.putLocator).not.toHaveBeenCalled();
    });

    it("searches compact indexes before downloading unrelated legacy journals", async () => {
        const h = fixture();
        vi.mocked(h.deps.list).mockResolvedValue([...Array.from({ length: 70 }, (_, i) => `legacy-${i}`), "wanted"]);
        vi.mocked(h.deps.readIndex).mockImplementation(async (journal) =>
            journal === "wanted"
                ? { status: "available", value: { version: 1, chunkIDs: ["h:a"] } }
                : { status: "not-found" }
        );
        vi.mocked(h.deps.readJournal).mockResolvedValue([leaf("h:a")]);
        expect(await h.reader.fetch(["h:a"])).toEqual([leaf("h:a")]);
        expect(h.deps.readJournal).toHaveBeenCalledExactlyOnceWith("wanted");
        expect(h.deps.putLocator).toHaveBeenCalledTimes(1);
    });

    it("bounds legacy content downloads separately from compact index reads", async () => {
        const h = fixture();
        vi.mocked(h.deps.list).mockResolvedValue(Array.from({ length: 32 }, (_, i) => `journal-${i}`));
        vi.mocked(h.deps.readIndex).mockResolvedValue({ status: "not-found" });
        let active = 0;
        let maximum = 0;
        vi.mocked(h.deps.readJournal).mockImplementation(async (journal) => {
            active++;
            maximum = Math.max(maximum, active);
            await Promise.resolve();
            active--;
            return [leaf(`h:${journal}`)];
        });
        expect(await h.reader.fetch(["h:journal-31"])).toEqual([leaf("h:journal-31")]);
        expect(maximum).toBe(4);
        expect(active).toBe(0);
        expect(h.deps.putLocator).toHaveBeenCalledTimes(1);
    });

    it("locates requested chunks in a large catalogue without persisting unrelated locators", async () => {
        const h = fixture();
        const journals = Array.from({ length: 7000 }, (_, i) => `journal-${i}`);
        vi.mocked(h.deps.list).mockResolvedValue(journals);
        let active = 0;
        let maximum = 0;
        vi.mocked(h.deps.readIndex).mockImplementation(async (journal) => {
            active++;
            maximum = Math.max(maximum, active);
            await Promise.resolve();
            active--;
            return {
                status: "available",
                value: { version: 1, chunkIDs: Array.from({ length: 250 }, (_, i) => `h:${journal}:${i}`) },
            };
        });
        const requested = "h:journal-6999:249";
        vi.mocked(h.deps.readJournal).mockResolvedValue([leaf(requested)]);
        expect(await h.reader.fetch([requested])).toEqual([leaf(requested)]);
        expect(maximum).toBe(32);
        expect(active).toBe(0);
        expect(h.deps.putLocator).toHaveBeenCalledExactlyOnceWith(requested, { version: 1, journal: journals[6999] });
        expect(h.deps.writeIndex).not.toHaveBeenCalled();
    });

    it("settles already started index reads before rejecting a failed batch", async () => {
        const h = fixture();
        let release!: () => void;
        const waiting = new Promise<void>((resolve) => {
            release = resolve;
        });
        vi.mocked(h.deps.readIndex).mockImplementation(async (journal) => {
            if (journal.startsWith("a")) throw Error("unavailable");
            await waiting;
            return { status: "available", value: { version: 1, chunkIDs: ["h:b"] } };
        });
        let settled = false;
        const fetching = h.reader.fetch(["h:b"]);
        const observed = fetching.finally(() => {
            settled = true;
        });
        void observed.catch(() => undefined);
        await new Promise((resolve) => setTimeout(resolve, 0));
        expect(settled).toBe(false);
        release();
        await expect(observed).rejects.toThrow("unavailable");
        expect(h.deps.putLocator).not.toHaveBeenCalled();
        expect(h.deps.readJournal).not.toHaveBeenCalled();
    });

    it("stops after outstanding reads settle without starting another batch or persisting locators", async () => {
        const h = fixture();
        vi.mocked(h.deps.list).mockResolvedValue(Array.from({ length: 20 }, (_, i) => `journal-${i}`));
        let active = true;
        h.deps.mayRun = () => active;
        vi.mocked(h.deps.readIndex).mockImplementation(async () => {
            active = false;
            return { status: "available", value: { version: 1, chunkIDs: ["h:a"] } };
        });
        await expect(h.reader.fetch(["h:a"])).rejects.toThrow("stopped");
        expect(h.deps.readIndex).toHaveBeenCalledTimes(1);
        expect(h.deps.putLocator).not.toHaveBeenCalled();
    });

    it("rejects non-chunk index entries even when they are not requested", async () => {
        const h = fixture();
        vi.mocked(h.deps.readIndex).mockResolvedValue({
            status: "available",
            value: { version: 1, chunkIDs: ["note.md"] },
        });
        await expect(h.reader.fetch(["h:a"])).rejects.toThrow("non-chunk");
        expect(h.deps.putLocator).not.toHaveBeenCalled();
    });

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
