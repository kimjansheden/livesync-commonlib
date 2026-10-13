import { afterEach, describe, expect, it, vi } from "vitest";
import {
    DOCID_JOURNAL_SYNC_PARAMETERS,
    ProtocolVersions,
    type EntryDoc,
    type EntryLeaf,
    type PlainEntry,
} from "@lib/common/types.ts";
import { wrappedInflate } from "@lib/pouchdb/compress.ts";
import { CHECKPOINT_WRITE_INTERVAL_MS } from "./JournalSyncCore.ts";
import { createCheckPointInfoDefault, type CheckPointInfo } from "./JournalSyncTypes.ts";
import {
    chunk,
    closeDevices,
    createDevice,
    createRemote,
    decoder,
    type Device,
    documentLine,
    DOCS_SUFFIX,
    encoder,
    journalOf,
    note,
    type Remote,
    storedEntries,
} from "./journalSpecHarness.spec.ts";

afterEach(closeDevices);

const SECOND = 1000;
const BATCH = 100;

const checkpointKey = (device: Device) => `bucketsync-checkpoint-${device.core.hash}`;
/** The key under which the checkpoint counts a revision as sent or received. */
const keyOf = (row: { id: string; rev: string }) => `${row.id}-${row.rev}`;

/** A device and its remote, with a clock which only the test moves. */
function createSender(name = "device") {
    const remote = createRemote();
    const device = createDevice(name, remote.storage);
    const clock = { now: 1_700_000_000_000 };
    device.core.now = () => clock.now;
    return { remote, device, clock };
}

/** Count the whole reads and whole writes of the checkpoint record from here on. */
function recordAccessOf(device: Device) {
    const key = checkpointKey(device);
    const get = vi.mocked(device.store.get);
    const set = vi.mocked(device.store.set);
    const from = { get: get.mock.calls.length, set: set.mock.calls.length };
    return () => ({
        reads: get.mock.calls.slice(from.get).filter(([read]) => read === key).length,
        writes: set.mock.calls.slice(from.set).filter(([written]) => written === key).length,
    });
}

/** Every position which is written to the store from here on, in order. */
function positionsWrittenBy(device: Device) {
    const key = checkpointKey(device);
    const set = vi.mocked(device.store.set);
    const from = set.mock.calls.length;
    return () =>
        set.mock.calls
            .slice(from)
            .filter(([written]) => written === key)
            .map(([, value]) => Number((value as CheckPointInfo).lastLocalSeq));
}

/** Change the checkpoint in the store as a client would, without any client: the device holds no copy now. */
function changeStoredCheckpoint(device: Device, change: (info: CheckPointInfo) => void) {
    const info = structuredClone(device.checkpoint);
    change(info);
    device.entries.set(checkpointKey(device), info);
}

/**
 * Give the device local changes which its checkpoint knows as received: `count` small files, and `filler`
 * further entries which the checkpoint knows and the local database has no change for.
 */
async function receiveEarlier(device: Device, count: number, filler = 0, prefix = "received") {
    const known: string[] = [];
    for (let from = 0; from < count; from += 5_000) {
        const docs = Array.from({ length: Math.min(5_000, count - from) }, (_, index) =>
            note(`${prefix}-${from + index}`)
        );
        const saved = (await device.db.bulkDocs(docs as EntryDoc[])) as { id: string; rev: string }[];
        for (const row of saved) known.push(keyOf(row));
    }
    for (let index = 0; index < filler; index++) known.push(`filler-${index}-1-0123456789abcdef`);
    changeStoredCheckpoint(device, (info) => {
        for (const key of known) info.knownIDs.add(key);
    });
    return known;
}

/** Watch the batches in which a send reads the local changes, and act before each of them is read. */
function eachBatch(device: Device, act: (batch: number) => void | Promise<void> = () => {}) {
    // An earlier watch of the same device ends here.
    if (vi.isMockFunction(device.db.changes)) vi.mocked(device.db.changes).mockRestore();
    const changes = device.db.changes.bind(device.db) as (options: unknown) => Promise<unknown>;
    const read: { since: number; limit: number }[] = [];
    vi.spyOn(device.db, "changes").mockImplementation((async (options: { since: number; limit: number }) => {
        read.push({ since: Number(options.since), limit: options.limit });
        await act(read.length);
        return await changes(options);
    }) as never);
    return read;
}

/** The revisions whose documents a send reads from the local database from here on. */
function documentsReadBy(device: Device) {
    const bulkGet = vi.spyOn(device.db, "bulkGet");
    return () => bulkGet.mock.calls.flatMap(([options]) => (options as { docs: { id: string; rev: string }[] }).docs);
}

const sequenceOf = async (device: Device) => Number((await device.db.info()).update_seq);
const send = async (device: Device) => await device.core.sendLocalJournal();

describe("JournalSyncCore send pass over changes which the device received", () => {
    it("reads the checkpoint once and no document, whatever the size of the checkpoint", async () => {
        const passOver = async (filler: number) => {
            const { device, remote } = createSender(`sender-${filler}`);
            const known = await receiveEarlier(device, 10 * BATCH, filler);
            const batches = eachBatch(device);
            const documentsRead = documentsReadBy(device);
            const access = recordAccessOf(device);

            await expect(send(device)).resolves.toBe(true);

            expect(device.db.bulkGet).not.toHaveBeenCalled();
            // Nothing which the checkpoint knew was taken out of it.
            expect(device.checkpoint.knownIDs.size).toBe(known.length);
            expect(device.checkpoint.lastLocalSeq).toBe(await sequenceOf(device));
            expect(remote.keys()).toEqual([]);
            return { ...access(), documentsRead: documentsRead().length, batches: batches.length };
        };

        const small = await passOver(0);
        const large = await passOver(50_000);

        // One read, and one write: the position at the end of the pass.
        expect(small).toEqual({ reads: 1, writes: 1, documentsRead: 0, batches: 10 });
        expect(large).toEqual(small);
    });

    it("reads a hundred changes at a time", async () => {
        const { device } = createSender();
        expect(device.core.batchSize).toBe(BATCH);
        await receiveEarlier(device, 3 * BATCH);
        const batches = eachBatch(device);

        await send(device);

        expect(batches).toEqual([
            { since: 0, limit: BATCH },
            { since: BATCH, limit: BATCH },
            { since: 2 * BATCH, limit: BATCH },
        ]);
    });

    it("uploads a change of its own behind two hundred thousand received ones in the same pass", async () => {
        const { device, remote } = createSender();
        const known = await receiveEarlier(device, 200_000);
        await device.db.put(note("written-here"));
        const own = await device.db.get("written-here");
        const documentsRead = documentsReadBy(device);
        const access = recordAccessOf(device);

        await expect(send(device)).resolves.toBe(true);

        expect([...(await storedEntries(remote)).keys()]).toEqual(["written-here"]);
        expect(documentsRead()).toEqual([{ id: "written-here", rev: own._rev }]);
        expect(access().reads).toBe(1);
        expect(device.checkpoint.lastLocalSeq).toBe(await sequenceOf(device));
        expect(device.checkpoint.knownIDs.size).toBe(known.length);
        expect(device.checkpoint.sentIDs.has(`written-here-${own._rev}`)).toBe(true);
    }, 120_000);

    it("does not move the position past a change of its own whose upload fails", async () => {
        const { device, remote, clock } = createSender();
        await receiveEarlier(device, 3 * BATCH);
        await device.db.put(note("written-here"));
        const ownSequence = await sequenceOf(device);
        // Time passes, so the position is stored while the received changes are passed.
        eachBatch(device, () => void (clock.now += CHECKPOINT_WRITE_INTERVAL_MS));
        remote.upload.mockImplementationOnce(async () => false);

        await expect(send(device)).resolves.toBe(false);

        expect(remote.keys()).toEqual([]);
        expect(Number(device.checkpoint.lastLocalSeq)).toBeGreaterThan(0);
        expect(Number(device.checkpoint.lastLocalSeq)).toBeLessThan(ownSequence);

        await expect(send(device)).resolves.toBe(true);
        expect([...(await storedEntries(remote)).keys()]).toEqual(["written-here"]);
        expect(device.checkpoint.lastLocalSeq).toBe(ownSequence);
    });

    it("uploads a change of its own between two long runs of received ones before the position passes it", async () => {
        const { device, remote, clock } = createSender();
        await receiveEarlier(device, 3 * BATCH, 0, "before");
        await device.db.put(note("written-here"));
        const ownSequence = await sequenceOf(device);
        await receiveEarlier(device, 5 * BATCH, 0, "after");
        const batches = eachBatch(device, () => void (clock.now += CHECKPOINT_WRITE_INTERVAL_MS));
        // What the remote holds each time a position is written.
        const key = checkpointKey(device);
        const written: { position: number; ownIsUploaded: boolean; batchesRead: number }[] = [];
        const set = vi.mocked(device.store.set);
        const store = set.getMockImplementation()!;
        set.mockImplementation(async (storeKey, value) => {
            if (storeKey === key) {
                written.push({
                    position: Number((value as CheckPointInfo).lastLocalSeq),
                    ownIsUploaded: (await storedEntries(remote)).has("written-here"),
                    batchesRead: batches.length,
                });
            }
            return await store(storeKey, value);
        });

        await expect(send(device)).resolves.toBe(true);

        expect(written.length).toBeGreaterThan(2);
        for (const { position, ownIsUploaded } of written) {
            if (position >= ownSequence) expect(ownIsUploaded).toBe(true);
        }
        // No position is written a second time.
        const positions = written.map(({ position }) => position);
        expect(positions).toEqual([...new Set(positions)].sort((a, b) => a - b));
        // It was uploaded when the next position was due, not when the pass ended.
        const firstPast = written.find(({ position }) => position >= ownSequence)!;
        expect(firstPast.batchesRead).toBeLessThan(batches.length);
        expect(device.checkpoint.lastLocalSeq).toBe(await sequenceOf(device));
        expect([...(await storedEntries(remote)).values()]).toEqual([1]);
    });

    it("stores the position about every half minute while it passes received changes, and once when the pass ends", async () => {
        const { device, clock } = createSender();
        await receiveEarlier(device, 30 * BATCH);
        const positions = positionsWrittenBy(device);
        eachBatch(device, () => void (clock.now += 10 * SECOND));

        await expect(send(device)).resolves.toBe(true);

        // Three batches take half a minute here.
        expect(positions()).toEqual([300, 600, 900, 1200, 1500, 1800, 2100, 2400, 2700, 3000]);
    });

    it("stores the position once, at the end, when the pass takes less than half a minute", async () => {
        const { device, clock } = createSender();
        await receiveEarlier(device, 30 * BATCH);
        const positions = positionsWrittenBy(device);
        eachBatch(device, () => void (clock.now += (CHECKPOINT_WRITE_INTERVAL_MS - 1) / 30));

        await expect(send(device)).resolves.toBe(true);

        expect(positions()).toEqual([3000]);
    });

    it("stores the position when the clock has gone backwards, as nothing tells how long it has been", async () => {
        const { device, clock } = createSender();
        await receiveEarlier(device, 3 * BATCH);
        const positions = positionsWrittenBy(device);
        eachBatch(device, (batch) => {
            if (batch === 2) clock.now -= 60 * 60 * SECOND;
        });

        await expect(send(device)).resolves.toBe(true);

        expect(positions()).toEqual([200, 300]);
    });

    it("does not move the position back when a journal fills up in the batch after a stored position", async () => {
        const { device, remote, clock } = createSender();
        await receiveEarlier(device, 2 * BATCH);
        // Three chunks of about 3.5 MB fill a journal before their batch of changes has ended.
        const payload = "x".repeat(3_500_000);
        await device.db.bulkDocs([
            ...[1, 2, 3].map((index) => chunk(`binary-${index}`, `${index}${payload}`) as EntryDoc),
            note("after-the-chunks"),
        ]);
        const positions = positionsWrittenBy(device);
        eachBatch(device, () => void (clock.now += CHECKPOINT_WRITE_INTERVAL_MS));

        await expect(send(device)).resolves.toBe(true);

        const written = positions();
        expect(written[0]).toBe(100);
        expect(written).toEqual([...written].sort((a, b) => a - b));
        expect(device.checkpoint.resetGeneration).toBe(0);
        expect(device.checkpoint.lastLocalSeq).toBe(await sequenceOf(device));
        expect((await storedEntries(remote)).size).toBe(4);
    });

    describe("when the pass is cut off without an orderly stop", () => {
        /** A pass over six batches of received changes and a change of its own, which dies as it reads `batch`. */
        async function cutOffAt(batch: number, timePerBatch: number) {
            const { device, remote, clock } = createSender();
            await receiveEarlier(device, 6 * BATCH);
            await device.db.put(note("written-here"));
            const positions = positionsWrittenBy(device);
            const batches = eachBatch(device, (read) => {
                clock.now += timePerBatch;
                if (read === batch) throw new Error("the application was closed");
            });

            await expect(send(device)).resolves.toBe(false);

            vi.mocked(device.db.changes).mockRestore();
            return { device, remote, positions: positions(), batchesRead: batches.length };
        }

        it("begins the next pass at the stored position when one was stored", async () => {
            const { device, remote, positions } = await cutOffAt(5, CHECKPOINT_WRITE_INTERVAL_MS);
            // The position was stored after each batch, and nothing was stored when the pass died.
            expect(positions).toEqual([100, 200, 300, 400]);
            expect(device.checkpoint.lastLocalSeq).toBe(400);
            const batches = eachBatch(device);

            await expect(send(device)).resolves.toBe(true);

            expect(batches[0].since).toBe(400);
            expect([...(await storedEntries(remote)).keys()]).toEqual(["written-here"]);
            expect(device.checkpoint.lastLocalSeq).toBe(await sequenceOf(device));
        });

        it("begins the next pass where this one began when no position was stored yet", async () => {
            const { device, remote, positions } = await cutOffAt(5, 0);
            expect(positions).toEqual([]);
            expect(device.checkpoint.lastLocalSeq).toBe(0);
            const batches = eachBatch(device);

            await expect(send(device)).resolves.toBe(true);

            expect(batches[0].since).toBe(0);
            expect([...(await storedEntries(remote)).keys()]).toEqual(["written-here"]);
        });

        it("has uploaded a change of its own before any position past it was stored", async () => {
            const { device, remote, clock } = createSender();
            await device.db.put(note("written-here"));
            const ownSequence = await sequenceOf(device);
            await receiveEarlier(device, 6 * BATCH);
            eachBatch(device, (read) => {
                clock.now += CHECKPOINT_WRITE_INTERVAL_MS;
                if (read === 5) throw new Error("the application was closed");
            });

            await expect(send(device)).resolves.toBe(false);

            expect(Number(device.checkpoint.lastLocalSeq)).toBeGreaterThan(ownSequence);
            expect([...(await storedEntries(remote)).keys()]).toEqual(["written-here"]);
        });
    });

    it("stores the position at what it has looked through when a stop is requested", async () => {
        const { device } = createSender();
        await receiveEarlier(device, 6 * BATCH);
        const positions = positionsWrittenBy(device);
        eachBatch(device, (batch) => {
            if (batch === 4) device.core.requestStop();
        });

        await expect(send(device)).resolves.toBe(true);

        expect(positions()).toEqual([400]);
    });

    it("sends the revision which it does not know of a change with several, and reads only that one", async () => {
        const { device, remote } = createSender();
        const base = note("resolved");
        // An edit and a deletion were made on the same revision. The edit was received; the deletion, which a
        // resolved conflict leaves behind, was made here.
        await device.db.bulkDocs(
            [
                { ...base, mtime: 2, _rev: "2-edit", _revisions: { start: 2, ids: ["edit", "base"] } },
                { ...base, _deleted: true, _rev: "2-deletion", _revisions: { start: 2, ids: ["deletion", "base"] } },
            ] as EntryDoc[],
            { new_edits: false }
        );
        changeStoredCheckpoint(device, (info) => void info.knownIDs.add("resolved-2-edit"));
        const documentsRead = documentsReadBy(device);

        await expect(send(device)).resolves.toBe(true);

        expect(documentsRead()).toEqual([{ id: "resolved", rev: "2-deletion" }]);
        const [journal] = remote.keys();
        const text = decoder.decode(await wrappedInflate(remote.dataOf(journal) as Uint8Array<ArrayBuffer>, {}));
        expect(text).toContain(`"_rev":"2-deletion"`);
        expect(text).not.toContain(`"_rev":"2-edit"`);
        expect(device.checkpoint.sentIDs.has("resolved-2-deletion")).toBe(true);
    });

    it.each(["knownIDs", "sentIDs"] as const)(
        "passes a chunk on its ID alone when %s holds it, without reading it",
        async (history) => {
            const { device, remote } = createSender();
            await device.db.put(chunk("content", "the content of a file") as EntryDoc);
            changeStoredCheckpoint(device, (info) => void info[history].add("h:content"));
            const documentsRead = documentsReadBy(device);

            await expect(send(device)).resolves.toBe(true);

            expect(documentsRead()).toEqual([]);
            expect(remote.keys()).toEqual([]);
            expect(device.checkpoint.lastLocalSeq).toBe(await sequenceOf(device));
        }
    );

    it("does not pass a chunk for a document revision of the same name, nor a revision for another one", async () => {
        const { device, remote } = createSender();
        await device.db.put(chunk("content", "the content of a file") as EntryDoc);
        await device.db.put(note("file"));
        const { _rev } = await device.db.get("file");
        // The checkpoint knows a revision of the chunk's ID, which is no key of a chunk, and another revision of
        // the file.
        changeStoredCheckpoint(device, (info) => {
            info.knownIDs.add(`h:content-${(_rev as string).replace(/^1/u, "9")}`);
            info.knownIDs.add("file-1-another");
        });

        await expect(send(device)).resolves.toBe(true);

        expect([...(await storedEntries(remote)).keys()].sort()).toEqual(["file", "h:content"]);
    });
});

describe("JournalSyncCore reads of the checkpoint in a cycle", () => {
    /** A device which has synchronised before, on a remote which has what a cycle checks first. */
    async function settled(filler: number) {
        const { device, remote, clock } = createSender(`idle-${filler}`);
        remote.put(
            DOCID_JOURNAL_SYNC_PARAMETERS,
            encoder.encode(
                JSON.stringify({ protocolVersion: ProtocolVersions.ADVANCED_E2EE, pbkdf2salt: btoa("synthetic") })
            )
        );
        await receiveEarlier(device, BATCH, filler);
        await device.db.put(note("written-here"));
        await cycle(device);
        return { device, remote, clock };
    }
    /** What a host does in one cycle: its check of the remote, which reads the checkpoint twice, and the transfer. */
    async function cycle(device: Device) {
        await device.core.ensureCheckpointCachesAreFresh();
        // The replicator reads the checkpoint here, for what it tells the remote of this device's progress.
        await device.core.getCheckpointInfo();
        return await device.core.sync();
    }

    it("reads the record once in a cycle with nothing to do, whatever its size, and writes nothing", async () => {
        const idleCycle = async (filler: number) => {
            const { device } = await settled(filler);
            const access = recordAccessOf(device);
            await expect(cycle(device)).resolves.toBe(true);
            return access();
        };

        const small = await idleCycle(0);
        const large = await idleCycle(50_000);

        expect(small).toEqual({ reads: 1, writes: 0 });
        expect(large).toEqual(small);
    });

    it("reads the record once in a synchronisation which the host starts by itself", async () => {
        const { device } = await settled(0);
        const access = recordAccessOf(device);

        await expect(device.core.sync()).resolves.toBe(true);

        expect(access()).toEqual({ reads: 1, writes: 0 });
    });

    it("reads the record from the store in the check before a cycle, whatever it held", async () => {
        const { device } = await settled(0);
        await device.core.ensureCheckpointCachesAreFresh();
        // The record changes in the store with no client of this process writing it.
        changeStoredCheckpoint(device, (info) => void info.receivedFiles.add("changed-beneath-the-client"));
        const access = recordAccessOf(device);

        await device.core.ensureCheckpointCachesAreFresh();

        expect(access().reads).toBe(1);
        expect((await device.core.getCheckpointInfo()).receivedFiles.has("changed-beneath-the-client")).toBe(true);
        expect(access().reads).toBe(1);
    });

    it.each([
        ["goes on with the copy of the check when its transfer begins within half a minute", -1, 1],
        ["reads the record again when the transfer begins half a minute after the check", 0, 2],
    ])("%s", async (_case, beyond, reads) => {
        const { device, clock } = await settled(0);
        const access = recordAccessOf(device);
        await device.core.ensureCheckpointCachesAreFresh();
        changeStoredCheckpoint(device, (info) => void info.receivedFiles.add("changed-beneath-the-client"));
        clock.now += CHECKPOINT_WRITE_INTERVAL_MS + beyond;

        await expect(device.core.sync()).resolves.toBe(true);

        expect(access().reads).toBe(reads);
    });

    it("reads the record again for an access after half a minute when no transfer began", async () => {
        const { device, clock } = await settled(0);
        await device.core.ensureCheckpointCachesAreFresh();
        changeStoredCheckpoint(device, (info) => void info.receivedFiles.add("changed-beneath-the-client"));
        clock.now += CHECKPOINT_WRITE_INTERVAL_MS;

        expect((await device.core.getCheckpointInfo()).receivedFiles.has("changed-beneath-the-client")).toBe(true);
    });

    it("reads the record at every access outside a cycle, as the store is the source of truth", async () => {
        const { device } = await settled(0);
        const access = recordAccessOf(device);

        await device.core.getCheckpointInfo();
        changeStoredCheckpoint(device, (info) => void info.receivedFiles.add("changed-beneath-the-client"));
        const info = await device.core.getCheckpointInfo();
        await device.core.hasUnsentLocalChanges();

        expect(info.receivedFiles.has("changed-beneath-the-client")).toBe(true);
        expect(access().reads).toBe(3);
    });

    it("keeps no copy of the record in memory when no cycle runs", async () => {
        const { device } = await settled(0);
        // The copy can be as large as everything the device has received.
        const copyInMemory = () => (device.core as unknown as { _heldCheckpoint?: object })._heldCheckpoint;

        await device.core.sync();
        expect(copyInMemory()).toBeUndefined();
        await device.core.getCheckpointInfo();
        expect(copyInMemory()).toBeUndefined();
        await device.core.updateCheckPointInfo((info) => ({ ...info, journalEpoch: "another" }));
        expect(copyInMemory()).toBeUndefined();
    });

    it("gives its copy up when the transfer ends, so that the next cycle begins with what the store holds", async () => {
        const { device } = await settled(0);
        await device.core.sync();
        changeStoredCheckpoint(device, (info) => void (info.lastLocalSeq = 0));
        const batches = eachBatch(device);

        await expect(device.core.sync()).resolves.toBe(true);

        expect(batches[0].since).toBe(0);
    });
});

describe("JournalSyncCore registration of what a receive stored", () => {
    const journalName = (index: number) => `${index.toString(16).padStart(64, "0")}${DOCS_SUFFIX}`;
    /** A remote with `count` journals, each with a small file. */
    async function remoteWithJournals(count: number) {
        const remote = createRemote();
        for (let index = 0; index < count; index++) {
            remote.put(journalName(index), await journalOf(documentLine(`file-${index}`)));
        }
        return remote;
    }
    function receiverOn(remote: Remote, name = "receiver") {
        const device = createDevice(name, remote.storage);
        const clock = { now: 1_700_000_000_000 };
        device.core.now = () => clock.now;
        return { device, clock };
    }
    /** Act as each journal is downloaded, before it is applied. */
    function eachDownload(remote: Remote, act: (journal: number) => void | Promise<void>) {
        const download = remote.downloadWithResult.getMockImplementation()!;
        let journals = 0;
        remote.downloadWithResult.mockImplementation(async (key) => {
            await act(++journals);
            return await download(key);
        });
    }

    it("reads the record once and not per journal, and writes it once, however many journals arrive", async () => {
        const receive = async (count: number) => {
            const { device } = receiverOn(await remoteWithJournals(count), `receiver-${count}`);
            const access = recordAccessOf(device);
            await expect(device.core.sync()).resolves.toBe(true);
            expect(device.checkpoint.receivedFiles.size).toBe(count);
            expect(device.checkpoint.knownIDs.size).toBe(count);
            return access();
        };

        const few = await receive(5);
        const many = await receive(40);

        expect(few).toEqual({ reads: 1, writes: 1 });
        expect(many).toEqual(few);
    });

    it("writes the record about every half minute during a long receive, and never ahead of what is stored", async () => {
        const remote = await remoteWithJournals(12);
        const { device, clock } = receiverOn(remote);
        eachDownload(remote, () => void (clock.now += 10 * SECOND));
        // What each write of the record holds, and what the local database held then.
        const key = checkpointKey(device);
        const set = vi.mocked(device.store.set);
        const store = set.getMockImplementation()!;
        const written: { journals: number; files: number }[] = [];
        set.mockImplementation(async (storeKey, value) => {
            if (storeKey === key) {
                const info = value as CheckPointInfo;
                const stored = (await device.db.allDocs()).rows.map((row) => `${row.id}-${row.value.rev}`);
                for (const known of info.knownIDs) expect(stored).toContain(known);
                written.push({ journals: info.receivedFiles.size, files: stored.length });
            }
            return await store(storeKey, value);
        });

        await expect(device.core.receiveRemoteJournal()).resolves.toBe(true);

        // Three journals take half a minute here. The last write is the one at the end of the receive.
        expect(written.map(({ journals }) => journals)).toEqual([3, 6, 9, 12]);
        for (const { journals, files } of written) expect(journals).toBeLessThanOrEqual(files);
    });

    it("fetches the journals again whose registration was not written when the application was closed, and stores nothing twice", async () => {
        const remote = await remoteWithJournals(8);
        const { device } = receiverOn(remote);
        // The application is closed while the fifth journal is fetched: what the store holds then is all that is
        // left of the checkpoint afterwards.
        let storeWhenClosed = new Map<string, unknown>();
        eachDownload(remote, (journal) => {
            if (journal === 5) storeWhenClosed = structuredClone(device.entries);
        });
        await expect(device.core.receiveRemoteJournal()).resolves.toBe(true);
        expect(device.saved).toHaveLength(8);
        device.entries.clear();
        for (const [key, value] of storeWhenClosed) device.entries.set(key, value);
        // Nothing was written yet, so nothing is registered.
        expect(device.checkpoint.receivedFiles.size).toBe(0);
        remote.transfers.length = 0;

        await expect(device.core.sync()).resolves.toBe(true);

        expect(remote.transfers.filter((entry) => entry.startsWith("download "))).toHaveLength(8);
        // Each file is in the local database once, in its one revision, and none was saved a second time.
        expect(device.saved).toHaveLength(8);
        expect((await device.db.allDocs()).rows).toHaveLength(8);
        expect(device.checkpoint.receivedFiles.size).toBe(8);
        expect(device.checkpoint.knownIDs.size).toBe(8);
        // What was received is known, so none of it is sent back.
        expect(remote.keys()).toHaveLength(8);
    });

    it("keeps what it registered in memory when the check before the next cycle runs during the receive", async () => {
        const remote = await remoteWithJournals(6);
        remote.put(
            DOCID_JOURNAL_SYNC_PARAMETERS,
            encoder.encode(
                JSON.stringify({ protocolVersion: ProtocolVersions.ADVANCED_E2EE, pbkdf2salt: btoa("synthetic") })
            )
        );
        const { device } = receiverOn(remote);
        eachDownload(remote, async (journal) => {
            if (journal === 4) await device.core.ensureCheckpointCachesAreFresh();
        });
        const access = recordAccessOf(device);

        await expect(device.core.receiveRemoteJournal()).resolves.toBe(true);

        expect(device.checkpoint.receivedFiles.size).toBe(6);
        expect(device.checkpoint.knownIDs.size).toBe(6);
        expect(access().reads).toBe(1);
    });

    it("keeps its copy while another transfer of the same client still runs", async () => {
        const remote = await remoteWithJournals(6);
        const { device } = receiverOn(remote);
        await device.db.put(note("written-here"));
        // A send begins and ends while the receive is under way.
        eachDownload(remote, async (journal) => {
            if (journal === 3) await expect(device.core.sendLocalJournal()).resolves.toBe(true);
        });
        const access = recordAccessOf(device);

        await expect(device.core.receiveRemoteJournal()).resolves.toBe(true);

        expect(access().reads).toBe(1);
        expect(device.checkpoint.receivedFiles.size).toBe(6);
        expect(device.checkpoint.sentFiles.size).toBe(1);
    });

    it("writes what is left when two transfers end in the same moment", async () => {
        const remote = await remoteWithJournals(6);
        const { device } = receiverOn(remote);

        // Both receives wait for the same journals, and end together.
        await expect(
            Promise.all([device.core.receiveRemoteJournal(), device.core.receiveRemoteJournal()])
        ).resolves.toEqual([true, true]);

        expect(device.checkpoint.receivedFiles.size).toBe(6);
        expect(device.checkpoint.knownIDs.size).toBe(6);
        // The next cycle reads the record once, as after any other.
        const access = recordAccessOf(device);
        await expect(device.core.sync()).resolves.toBe(true);
        expect(access().reads).toBe(1);
    });

    it("does not fail a transfer when what is left cannot be written, and receives it again", async () => {
        const remote = await remoteWithJournals(3);
        const { device } = receiverOn(remote);
        const key = checkpointKey(device);
        const set = vi.mocked(device.store.set);
        const store = set.getMockImplementation()!;
        let failures = 0;
        set.mockImplementation(async (storeKey, value) => {
            if (storeKey === key && failures++ === 0) throw new Error("the store is closed");
            return await store(storeKey, value);
        });

        await expect(device.core.receiveRemoteJournal()).resolves.toBe(true);
        expect(device.checkpoint.receivedFiles.size).toBe(0);

        await expect(device.core.receiveRemoteJournal()).resolves.toBe(true);
        expect(device.checkpoint.receivedFiles.size).toBe(3);
        expect(device.saved).toHaveLength(3);
    });

    it("writes a registration at once when no transfer is running", async () => {
        const remote = await remoteWithJournals(1);
        const { device } = receiverOn(remote);
        remote.put(
            DOCID_JOURNAL_SYNC_PARAMETERS,
            encoder.encode(
                JSON.stringify({ protocolVersion: ProtocolVersions.ADVANCED_E2EE, pbkdf2salt: btoa("synthetic") })
            )
        );
        // The check before a cycle leaves a copy in memory, and no transfer follows it.
        await device.core.ensureCheckpointCachesAreFresh();
        const entries = await device.core.readRemoteJournal(journalName(0));

        await expect(device.core.processDocuments(entries)).resolves.toBe(true);

        expect([...device.checkpoint.knownIDs]).toEqual(["file-0-1-rev1"]);
    });

    it("writes what is registered with the position, so that a position never stands for something unregistered", async () => {
        const remote = await remoteWithJournals(6);
        const { device } = receiverOn(remote);
        const key = checkpointKey(device);
        const set = vi.mocked(device.store.set);
        const store = set.getMockImplementation()!;
        const written: CheckPointInfo[] = [];
        set.mockImplementation(async (storeKey, value) => {
            if (storeKey === key) written.push(structuredClone(value as CheckPointInfo));
            return await store(storeKey, value);
        });

        await expect(device.core.sync()).resolves.toBe(true);

        // The files which were received are changes of the local database. The send passes them as known.
        const sequence = await sequenceOf(device);
        expect(written.at(-1)!.lastLocalSeq).toBe(sequence);
        for (const info of written) {
            if (Number(info.lastLocalSeq) > 0) expect(info.knownIDs.size).toBe(6);
        }
    });
});

describe("JournalSyncCore with another client over the same store", () => {
    const journalName = (index: number) => `${index.toString(16).padStart(64, "0")}${DOCS_SUFFIX}`;

    it("stops a send pass when the other client resets the history, and reads the record once more to learn of it", async () => {
        const { device, remote } = createSender();
        await receiveEarlier(device, 6 * BATCH);
        await device.db.put(note("written-here"));
        const pane = device.createPaneClient();
        eachBatch(device, async (batch) => {
            if (batch === 4) await pane.resetCheckpointInfo();
        });
        const access = recordAccessOf(device);

        await expect(send(device)).resolves.toBe(false);

        // The sending client read the record at its start, and again when it saw that another client had written.
        // The other client read it once for its reset, and wrote it once.
        expect(access()).toEqual({ reads: 3, writes: 1 });
        expect(device.checkpoint).toMatchObject({ lastLocalSeq: 0, resetGeneration: 1 });
        expect(device.checkpoint.knownIDs.size).toBe(0);
        expect(remote.keys()).toEqual([]);

        // The next send starts again, and sends everything, as the reset asked.
        const batches = eachBatch(device);
        await expect(send(device)).resolves.toBe(true);
        expect(batches[0].since).toBe(0);
        expect((await storedEntries(remote)).size).toBe(6 * BATCH + 1);
    });

    it("records nothing which was received before the other client reset the history", async () => {
        const remote = createRemote();
        for (let index = 0; index < 6; index++) {
            remote.put(journalName(index), await journalOf(documentLine(`file-${index}`)));
        }
        const device = createDevice("receiver", remote.storage);
        const pane = device.createPaneClient();
        const download = remote.downloadWithResult.getMockImplementation()!;
        let journals = 0;
        remote.downloadWithResult.mockImplementation(async (key) => {
            // Three journals are received and registered in memory when the history is reset.
            if (++journals === 4) await pane.resetCheckpointInfo();
            return await download(key);
        });

        await expect(device.core.sync()).resolves.toBe(false);

        // The fourth journal is not applied: the reset is seen before anything of it is stored.
        expect(device.saved).toHaveLength(3);
        expect(device.checkpoint.resetGeneration).toBe(1);
        expect([...device.checkpoint.receivedFiles]).toEqual([]);
        expect([...device.checkpoint.knownIDs]).toEqual([]);
    });

    it("records nothing which was received before a reset that comes after the last journal", async () => {
        const remote = createRemote();
        for (let index = 0; index < 3; index++) {
            remote.put(journalName(index), await journalOf(documentLine(`file-${index}`)));
        }
        const device = createDevice("receiver", remote.storage);
        const pane = device.createPaneClient();
        // The journals are received and registered in memory. The history is reset as the send begins.
        eachBatch(device, async (batch) => {
            if (batch === 1) await pane.resetCheckpointInfo();
        });

        await expect(device.core.sync()).resolves.toBe(false);

        expect(device.saved).toHaveLength(3);
        expect(device.checkpoint.resetGeneration).toBe(1);
        expect([...device.checkpoint.receivedFiles]).toEqual([]);
        expect([...device.checkpoint.knownIDs]).toEqual([]);
        expect(device.checkpoint.lastLocalSeq).toBe(0);
    });

    it("records nothing which was received before a reset that comes as the receive ends", async () => {
        const remote = createRemote();
        for (let index = 0; index < 3; index++) {
            remote.put(journalName(index), await journalOf(documentLine(`file-${index}`)));
        }
        const device = createDevice("receiver", remote.storage);
        const pane = device.createPaneClient();
        // The journals are received and registered in memory, and nothing of it is written yet. The history is
        // reset before the receive writes what is left.
        let reset: Promise<void> | undefined;
        const updateInfo = device.core.updateInfo.bind(device.core);
        vi.spyOn(device.core, "updateInfo").mockImplementation((info) => {
            updateInfo(info);
            if (info.syncStatus === "COMPLETED") reset ??= pane.resetCheckpointInfo();
        });

        await device.core.receiveRemoteJournal();
        await reset;

        expect(device.saved).toHaveLength(3);
        expect(device.checkpoint.resetGeneration).toBe(1);
        expect([...device.checkpoint.receivedFiles]).toEqual([]);
        expect([...device.checkpoint.knownIDs]).toEqual([]);
    });

    it("does not store the position of a pass in a history which the other client has reset meanwhile", async () => {
        const { device, clock } = createSender();
        await receiveEarlier(device, 6 * BATCH);
        const pane = device.createPaneClient();
        let looksAtTheClock = 0;
        eachBatch(device, () => {
            clock.now += CHECKPOINT_WRITE_INTERVAL_MS;
            looksAtTheClock = 0;
        });
        // The history is reset as the pass asks for its position to be stored: after its look at the checkpoint
        // for that batch, and before the position is written.
        let reset: Promise<void> | undefined;
        let positionsWritten = 0;
        const set = vi.mocked(device.store.set);
        const store = set.getMockImplementation()!;
        const key = checkpointKey(device);
        const now = device.core.now;
        device.core.now = () => {
            // After a batch the pass looks at the clock once to see whether a position is due, and a second
            // time as it hands the request on.
            if (++looksAtTheClock === 2) reset ??= pane.resetCheckpointInfo();
            return now();
        };
        set.mockImplementation(async (storeKey, value) => {
            if (storeKey === key && Number((value as CheckPointInfo).lastLocalSeq) > 0) positionsWritten++;
            return await store(storeKey, value);
        });

        await expect(send(device)).resolves.toBe(false);
        await reset;

        expect(reset).toBeDefined();
        expect(positionsWritten).toBe(0);
        expect(device.checkpoint).toMatchObject({ lastLocalSeq: 0, resetGeneration: 1 });
    });

    it("keeps what it registered in memory when the other client writes something which is no reset", async () => {
        const remote = createRemote();
        for (let index = 0; index < 6; index++) {
            remote.put(journalName(index), await journalOf(documentLine(`file-${index}`)));
        }
        const device = createDevice("receiver", remote.storage);
        const pane = device.createPaneClient();
        const download = remote.downloadWithResult.getMockImplementation()!;
        let journals = 0;
        remote.downloadWithResult.mockImplementation(async (key) => {
            if (++journals === 4) await pane.updateCheckPointInfo((info) => ({ ...info, journalEpoch: "another" }));
            return await download(key);
        });

        await expect(device.core.sync()).resolves.toBe(true);

        expect(device.checkpoint.journalEpoch).toBe("another");
        expect(device.checkpoint.receivedFiles.size).toBe(6);
        expect(device.checkpoint.knownIDs.size).toBe(6);
        expect(device.checkpoint.resetGeneration).toBe(0);
        // Nothing which was received is sent back.
        expect(remote.keys()).toHaveLength(6);
    });

    it("sees a change by the other client between the check before a cycle and its transfer", async () => {
        const { device, remote } = createSender();
        remote.put(
            DOCID_JOURNAL_SYNC_PARAMETERS,
            encoder.encode(
                JSON.stringify({ protocolVersion: ProtocolVersions.ADVANCED_E2EE, pbkdf2salt: btoa("synthetic") })
            )
        );
        await device.db.put(note("written-here"));
        await device.core.sync();
        await device.core.ensureCheckpointCachesAreFresh();

        // The sent history is reset, as the maintenance pane does.
        await device.createPaneClient().updateCheckPointInfo((info) => ({
            ...info,
            lastLocalSeq: 0,
            sentIDs: new Set<string>(),
            sentFiles: new Set<string>(),
        }));
        const batches = eachBatch(device);
        await expect(device.core.sync()).resolves.toBe(true);

        expect(batches[0].since).toBe(0);
        expect(device.checkpoint.resetGeneration).toBe(1);
    });
});

describe("JournalSyncCore checkpoint record", () => {
    it("is stored as before, under its one key, so that an earlier version reads it", async () => {
        const { device, remote, clock } = createSender();
        const receiver = createDevice("receiver", remote.storage);
        await device.db.bulkDocs([chunk("content", "the content of a file") as EntryLeaf, note("file", ["content"])]);
        await receiveEarlier(device, 3 * BATCH);
        eachBatch(device, () => void (clock.now += CHECKPOINT_WRITE_INTERVAL_MS));
        await device.core.sync();
        await receiver.core.sync();

        for (const client of [device, receiver]) {
            const stored = client.entries.get(checkpointKey(client)) as Record<string, unknown>;
            expect(Object.keys(stored).sort()).toEqual(Object.keys(createCheckPointInfoDefault()).sort());
            for (const history of ["knownIDs", "sentIDs", "receivedFiles", "sentFiles"]) {
                expect(stored[history]).toBeInstanceOf(Set);
            }
            expect(typeof stored.lastLocalSeq).toBe("number");
            // Nothing else is kept in the store for the checkpoint.
            expect([...client.entries.keys()].filter((key) => key.includes("checkpoint"))).toEqual([
                checkpointKey(client),
            ]);
        }
        expect((receiver.checkpoint.knownIDs as Set<string>).has("h:content")).toBe(true);
        expect(((await receiver.db.get("file")) as PlainEntry).children).toEqual(["h:content"]);
    });
});
