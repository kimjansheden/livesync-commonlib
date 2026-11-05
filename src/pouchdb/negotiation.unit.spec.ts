import { describe, expect, it, vi } from "vitest";
import PouchDB from "pouchdb-core";
import MemoryAdapter from "pouchdb-adapter-memory";
import Find from "pouchdb-find";
import { countCompromisedChunksSinceReceipt, COMPROMISED_CHUNK_SCAN_RECEIPT_KEY } from "./negotiation";
PouchDB.plugin(MemoryAdapter).plugin(Find);

function setup() {
    const state = { sequence: 10 as number | string, identity: "synthetic-db-a" };
    const records = new Map<string, { _id: string; type: string }>();
    const changes: { id: string; seq: number; deleted?: boolean }[] = [];
    const receipts = new Map<string, unknown>();
    const db = {
        id: vi.fn(async () => state.identity),
        info: vi.fn(async () => ({ update_seq: state.sequence })),
        find: vi.fn(async (query: { selector: { _id: { $lt?: string; $gt?: string } } }) => ({
            docs: [...records.values()].filter(
                (doc) =>
                    doc.type === "leaf" &&
                    (query.selector._id.$lt
                        ? doc._id < query.selector._id.$lt
                        : doc._id > (query.selector._id.$gt ?? ""))
            ),
        })),
        changes: vi.fn(async ({ since, limit }: { since: number; limit: number }) => {
            const results = changes.filter((change) => change.seq > since).slice(0, limit);
            return { results, last_seq: results.at(-1)?.seq ?? state.sequence };
        }),
        allDocs: vi.fn(async ({ keys }: { keys: string[] }) => ({
            rows: keys.map((id) => (records.has(id) ? { id, doc: records.get(id) } : { key: id, error: "not_found" })),
        })),
    };
    const store = {
        get: vi.fn(async (key: string) => receipts.get(key)),
        set: vi.fn(async (key: string, value: unknown) => {
            receipts.set(key, structuredClone(value));
        }),
    };
    const check = () => countCompromisedChunksSinceReceipt(db as never, store);
    const add = (id: string, type: string) => {
        if (typeof state.sequence !== "number") throw Error("Numeric sequence required by fixture");
        records.set(id, { _id: id, type });
        changes.push({ id, seq: ++state.sequence });
    };
    return { state, db, records, changes, receipts, store, check, add };
}

describe("durable Object Storage local compromised-chunk gate", () => {
    it("performs the first full clean scan before recording a versioned database-bound sequence", async () => {
        const h = setup();
        expect(await h.check()).toBe(0);
        expect(h.db.find).toHaveBeenCalledTimes(2);
        expect(h.receipts.get(COMPROMISED_CHUNK_SCAN_RECEIPT_KEY)).toEqual({
            version: 1,
            policy: "leaf-h-namespace-v1",
            databaseId: "synthetic-db-a",
            verifiedSequence: 10,
        });
    });

    it("keeps an unchanged sequence O(1) without historical document reads", async () => {
        const h = setup();
        await h.check();
        h.db.find.mockClear();
        h.db.changes.mockClear();
        h.db.allDocs.mockClear();
        h.store.set.mockClear();
        h.db.find.mockImplementation(async () => await new Promise<never>(() => undefined));
        expect(await h.check()).toBe(0);
        expect(h.db.find).not.toHaveBeenCalled();
        expect(h.db.changes).not.toHaveBeenCalled();
        expect(h.db.allDocs).not.toHaveBeenCalled();
        expect(h.store.set).not.toHaveBeenCalled();
    });

    it("inspects only new changes and never reads modern chunk bodies", async () => {
        const h = setup();
        await h.check();
        h.db.find.mockClear();
        h.add("h:modern-chunk", "leaf");
        h.add("f:new-note", "plain");
        expect(await h.check()).toBe(0);
        expect(h.db.changes).toHaveBeenCalledWith(
            expect.objectContaining({ since: 10, include_docs: false, limit: 256 })
        );
        expect(h.db.allDocs).toHaveBeenCalledWith({ keys: ["f:new-note"], include_docs: true });
        expect(h.db.find).not.toHaveBeenCalled();
        expect(h.receipts.get(COMPROMISED_CHUNK_SCAN_RECEIPT_KEY)).toMatchObject({ verifiedSequence: 12 });
    });

    it("detects a newly introduced legacy leaf without advancing the clean receipt", async () => {
        const h = setup();
        await h.check();
        h.add("legacy-chunk", "leaf");
        expect(await h.check()).toBe(1);
        expect(h.receipts.get(COMPROMISED_CHUNK_SCAN_RECEIPT_KEY)).toMatchObject({ verifiedSequence: 10 });
    });

    it("includes the exact h; namespace boundary in the first full local proof", async () => {
        const h = setup();
        h.records.set("h;", { _id: "h;", type: "leaf" });
        expect(await h.check()).toBe(1);
        expect(h.store.set).not.toHaveBeenCalled();
    });

    it("checks changes which arrived during the initial full scan before recording success", async () => {
        const h = setup();
        h.db.find.mockImplementation(async () => {
            if (h.state.sequence === 10) h.add("legacy-new", "leaf");
            return { docs: [] };
        });
        expect(await h.check()).toBe(1);
        expect(h.receipts.has(COMPROMISED_CHUNK_SCAN_RECEIPT_KEY)).toBe(false);
    });

    it("does not advance a receipt past an observed but uninspected concurrent write", async () => {
        const h = setup();
        await h.check();
        h.add("f:changed-note", "plain");
        const read = h.db.allDocs.getMockImplementation()!;
        h.db.allDocs.mockImplementationOnce(async (options) => {
            const result = await read(options);
            h.add("legacy-late", "leaf");
            return result;
        });
        expect(await h.check()).toBe(false);
        expect(h.receipts.get(COMPROMISED_CHUNK_SCAN_RECEIPT_KEY)).toMatchObject({ verifiedSequence: 10 });
    });

    it.each(["doctor-only", "different database", "different policy", "sequence rollback"])(
        "requires a full scan for unsafe upgrade proof: %s",
        async (proof) => {
            const h = setup();
            await h.check();
            h.db.find.mockClear();
            if (proof === "doctor-only")
                h.receipts.set(COMPROMISED_CHUNK_SCAN_RECEIPT_KEY, {
                    doctorProcessedVersion: "approved-release-20",
                    initialized: true,
                    checkIncompleteDocs: true,
                });
            if (proof === "different database") h.state.identity = "synthetic-db-recreated";
            if (proof === "different policy")
                h.receipts.set(COMPROMISED_CHUNK_SCAN_RECEIPT_KEY, {
                    ...(h.receipts.get(COMPROMISED_CHUNK_SCAN_RECEIPT_KEY) as object),
                    version: 0,
                });
            if (proof === "sequence rollback") h.state.sequence = 2;
            expect(await h.check()).toBe(0);
            expect(h.db.find).toHaveBeenCalledTimes(2);
        }
    );

    it("uses the full check without issuing a comparable receipt for opaque sequences", async () => {
        const h = setup();
        h.state.sequence = "10-opaque";
        expect(await h.check()).toBe(0);
        expect(h.db.find).toHaveBeenCalledTimes(2);
        expect(h.store.set).not.toHaveBeenCalled();
    });

    it.each(["info", "changes", "allDocs", "receipt"])(
        "fails closed on %s failure and never moves the receipt",
        async (failure) => {
            const h = setup();
            await h.check();
            h.add("new-note", "plain");
            const error = new Error("synthetic database failure");
            if (failure === "info") h.db.info.mockRejectedValueOnce(error);
            if (failure === "changes") h.db.changes.mockRejectedValueOnce(error);
            if (failure === "allDocs") h.db.allDocs.mockRejectedValueOnce(error);
            if (failure === "receipt") h.store.set.mockRejectedValueOnce(error);
            expect(await h.check()).toBe(false);
            expect(h.receipts.get(COMPROMISED_CHUNK_SCAN_RECEIPT_KEY)).toMatchObject({ verifiedSequence: 10 });
        }
    );

    it("does not create a receipt when the first historical check fails", async () => {
        const h = setup();
        h.db.find.mockRejectedValueOnce(new Error("synthetic Mango read error"));
        expect(await h.check()).toBe(false);
        expect(h.store.set).not.toHaveBeenCalled();
    });

    it("pages only the new interval when more than one change batch arrived", async () => {
        const h = setup();
        await h.check();
        h.db.find.mockClear();
        for (let index = 0; index < 600; index++) h.add(`f:new-${index}`, "plain");
        expect(await h.check()).toBe(0);
        expect(h.db.changes).toHaveBeenCalledTimes(3);
        expect(h.db.find).not.toHaveBeenCalled();
        expect(h.receipts.get(COMPROMISED_CHUNK_SCAN_RECEIPT_KEY)).toMatchObject({ verifiedSequence: 610 });
    });

    it("qualifies and resumes a real PouchDB database by its physical identity and sequence", async () => {
        const db = new PouchDB("synthetic-chunk-safety-receipt", { adapter: "memory" });
        const h = setup();
        try {
            await db.bulkDocs([
                { _id: "f:existing-note", type: "plain" },
                { _id: "h:modern", type: "leaf", data: "synthetic" },
            ]);
            expect(await countCompromisedChunksSinceReceipt(db, h.store)).toBe(0);
            const first = h.receipts.get(COMPROMISED_CHUNK_SCAN_RECEIPT_KEY);
            const find = vi.spyOn(db, "find");
            expect(await countCompromisedChunksSinceReceipt(db, h.store)).toBe(0);
            expect(find).not.toHaveBeenCalled();
            await db.put({ _id: "legacy-new", type: "leaf", data: "synthetic-bad" });
            expect(await countCompromisedChunksSinceReceipt(db, h.store)).toBe(1);
            expect(h.receipts.get(COMPROMISED_CHUNK_SCAN_RECEIPT_KEY)).toEqual(first);
        } finally {
            await db.destroy();
        }
    });
});
