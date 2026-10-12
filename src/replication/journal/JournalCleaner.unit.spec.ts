import { afterEach, describe, expect, it, vi } from "vitest";
import {
    DEFAULT_SETTINGS,
    KeyIndexOfSettings,
    DOCID_JOURNAL_SYNC_PARAMETERS,
    E2EEAlgorithms,
    LOG_LEVEL_NOTICE,
    LOG_LEVEL_VERBOSE,
    ProtocolVersions,
    type EntryDoc,
    type EntryLeaf,
    type PlainEntry,
} from "@lib/common/types.ts";
import { setGlobalLogFunction } from "@lib/common/logger.ts";
import { wrappedInflate } from "@lib/pouchdb/compress.ts";
import {
    CLEANING_INTERVAL_MS,
    FULL_SCAN_INTERVAL_MS,
    MOBILE_SCAN_LIMIT_MS,
    MOBILE_SCAN_RETRY_MS,
    UNINDEXED_JOURNAL_MIN_BYTES,
    UNINDEXED_READ_BUDGET_BYTES,
} from "./JournalCleaner.ts";
import { LOCAL_CHUNK_READ_MAX_BYTES, LOCAL_CHUNK_READ_MAX_COUNT, RELIANCE_MEMORY_MS } from "./JournalSyncCore.ts";
import {
    ASSESSMENT_MARKER_PREFIX,
    JOURNAL_INDEX_PREFIX,
    REMOVAL_FRESHNESS_MS,
    REMOVAL_RECORD_PREFIX,
    type CheckPointInfo,
    type JournalReliance,
    type RemovalRecord,
} from "./JournalSyncTypes.ts";
import {
    bulkKey,
    chunk,
    chunkId,
    chunkLine,
    closeDevices,
    createDevice,
    createRemote,
    type Device,
    docsKey,
    documentLine,
    encoder,
    entriesOf,
    hasChunk,
    isBulkKey,
    journalOf,
    largeChunk,
    note,
    type Remote,
    storedEntries,
} from "./journalSpecHarness.spec.ts";

afterEach(closeDevices);

const MINUTE = 60 * 1000;
/** The size from which a file has large chunks. */
const LARGE_FILE_BYTES = 40 * 1024;

type DeviceOptions = { mobile?: boolean; cleans?: boolean; clockOffset?: number };
/**
 * Devices on one remote. Time passes only when the test moves `clock`. The store tells that time. Each device
 * has a clock of its own, which can be off by any amount and can be set while the test runs.
 */
function createWorld() {
    const clock = { now: 1_700_000_000_000 };
    const remote = createRemote(undefined, () => clock.now);
    const offsets = new Map<Device, { ms: number }>();
    const device = (name: string, options: DeviceOptions = {}) => {
        const created = createDevice(name, remote.storage);
        const offset = { ms: options.clockOffset ?? 0 };
        offsets.set(created, offset);
        created.core.now = () => clock.now + offset.ms;
        created.host.isMobile = options.mobile ?? false;
        if (options.cleans) created.host.settings.removeUnreferencedJournalData = true;
        return created;
    };
    /** How far the clock of a device is off. Changing it sets the clock of that device alone. */
    const clockOffsetOf = (created: Device) => offsets.get(created)!;
    return { remote, clock, device, clockOffsetOf };
}
type World = ReturnType<typeof createWorld>;

/** Write a file with large chunks on a device. */
async function writeFile(device: Device, id: string, chunkNames: string[]) {
    await device.db.bulkDocs([
        ...chunkNames.map((name) => largeChunk(name)),
        { ...note(id, chunkNames), size: LARGE_FILE_BYTES },
    ]);
}
/** Write a file on a device which already holds its chunks. */
async function writeFileFromChunksHere(device: Device, id: string, chunkNames: string[]) {
    await device.db.put({ ...note(id, chunkNames), size: LARGE_FILE_BYTES });
}
/** Delete a file as the application does: the document stays, marked as deleted, with its list of chunks. */
async function deleteFile(device: Device, id: string) {
    const doc = (await device.db.get(id)) as PlainEntry;
    await device.db.put({ ...doc, deleted: true });
}
const synchronise = async (device: Device) => await expect(device.core.sync()).resolves.toBe(true);
/**
 * Let a device receive and send while the synchronisation of another device is under way.
 *
 * In an application one client synchronises at a time, and a synchronisation which is asked for meanwhile joins
 * the running one. The devices of a test live in one process, so a device which is to act in the middle of
 * another's synchronisation receives and sends in two steps instead.
 */
async function synchroniseBeside(device: Device) {
    await expect(device.core.receiveRemoteJournal()).resolves.toBe(true);
    await expect(device.core.sendLocalJournal()).resolves.toBe(true);
}
/** Synchronise once the least time between two cleaning rounds has passed. */
async function synchroniseAfterInterval(world: World, device: Device) {
    world.clock.now += CLEANING_INTERVAL_MS + MINUTE;
    await synchronise(device);
}
const bulkJournals = (remote: Remote) => remote.keys().filter(isBulkKey);
const indexObjects = (remote: Remote) => remote.objectKeys().filter((key) => key.startsWith(JOURNAL_INDEX_PREFIX));
const removalRecords = (remote: Remote) => remote.objectKeys().filter((key) => key.startsWith(REMOVAL_RECORD_PREFIX));
const isMarker = (key: string) => key.startsWith(ASSESSMENT_MARKER_PREFIX);
const markersOf = (remote: Remote) => remote.objectKeys().filter(isMarker);
/** The journals, indexes and records which were deleted, in order. */
const deletedOf = (remote: Remote) => remote.deleted.filter((key) => !isMarker(key));
const downloadsOf = (remote: Remote, key: string) => remote.transfers.filter((entry) => entry === `download ${key}`);
async function readRecord(device: Device, key: string): Promise<RemovalRecord> {
    const read = await device.core.downloadPackedJson(key);
    if (read.status !== "available") throw new Error(`The record ${key} could not be read`);
    return read.value as RemovalRecord;
}
const relianceOf = (device: Device) =>
    (device.entries.get(`bucketsync-reliance-${device.core.hash}`) as JournalReliance | undefined) ?? {
        documents: [],
        resend: [],
    };
const cleaningStateOf = (device: Device) =>
    device.entries.get(`bucketsync-cleaning-${device.core.hash}`) as
        | { lastRoundAt?: number; assessment?: { at: number; dead: string[] }; scanTimedOutAt?: number }
        | undefined;
const knownContentOf = (device: Device) =>
    [...device.entries.keys()].filter((key) => key.startsWith("bucketsync-journal-content-"));
const chunkData = async (device: Device, name: string) => ((await device.db.get(chunkId(name))) as EntryLeaf).data;
/** Collect what is logged as a notice from here on. */
function noticesFromNowOn() {
    const notices: string[] = [];
    setGlobalLogFunction((message, level) => {
        if (level === LOG_LEVEL_NOTICE) notices.push(String(message));
    });
    return notices;
}
const setCheckpoint = (device: Device, change: Partial<CheckPointInfo>) =>
    device.entries.set(`bucketsync-checkpoint-${device.core.hash}`, {
        ...device.checkpoint,
        ...change,
    } satisfies CheckPointInfo);

/**
 * A sender writes a file with two large chunks and synchronises, a receiver takes it, and the sender deletes it.
 * Both devices then know of the deletion.
 */
async function fileSentReceivedAndDeleted(world: World, receiverOptions: DeviceOptions = {}) {
    const sender = world.device("sender");
    const receiver = world.device("receiver", receiverOptions);
    await writeFile(sender, "recording", ["part-1", "part-2"]);
    await synchronise(sender);
    await synchronise(receiver);
    await deleteFile(sender, "recording");
    await synchronise(sender);
    await synchronise(receiver);
    const [journal] = bulkJournals(world.remote);
    return { sender, receiver, journal };
}

describe("JournalSyncCore index of a journal of large chunks", () => {
    it("stores the IDs of the chunks beside the journal, whatever the setting", async () => {
        const world = createWorld();
        const sender = world.device("sender");
        await writeFile(sender, "recording", ["part-1", "part-2"]);

        await synchronise(sender);

        const [journal] = bulkJournals(world.remote);
        expect(indexObjects(world.remote)).toEqual([`_index-${journal}`]);
        await expect(sender.core.downloadPackedJson(`_index-${journal}`)).resolves.toEqual({
            status: "available",
            value: { version: 1, chunkIDs: ["h:part-1", "h:part-2"] },
        });
        // The journal comes first, then its index, and only then the documents.
        expect(world.remote.objectKeys().map((key) => key.replace(/[a-f0-9]{64}/u, "<id>"))).toEqual([
            "<id>-chunks.jsonl.gz",
            "_index-<id>-chunks.jsonl.gz",
            "<id>-docs.jsonl.gz",
        ]);
    });

    it("stores a journal of large chunks once when its index cannot be stored, and goes on", async () => {
        const world = createWorld();
        const sender = world.device("sender");
        await writeFile(sender, "recording", ["part-1", "part-2"]);
        world.remote.uploadBeside.mockImplementationOnce(async () => false);

        await synchronise(sender);

        expect(bulkJournals(world.remote)).toHaveLength(1);
        expect(indexObjects(world.remote)).toEqual([]);
        expect(world.remote.keys()).toHaveLength(2);
        expect(sender.checkpoint.lastLocalSeq).toBe((await sender.db.info()).update_seq);

        // Nothing is sent again, under this name or another, and the journal stays without an index.
        await synchronise(sender);
        expect([...(await storedEntries(world.remote)).values()]).toEqual([1, 1, 1]);
        expect(indexObjects(world.remote)).toEqual([]);
    });

    it("keeps an index which is already stored when a journal is sent again", async () => {
        const world = createWorld();
        const sender = world.device("sender");
        await writeFile(sender, "recording", ["part-1", "part-2"]);

        await synchronise(sender);

        const indexUploads = world.remote.uploadBeside.mock.calls.map(([key, , , options]) => [key, options]);
        expect(indexUploads).toEqual([[indexObjects(world.remote)[0], { keepExisting: true }]]);
    });

    it("does not treat an index or a removal record as a journal", async () => {
        const world = createWorld();
        const receiver = world.device("receiver");
        world.remote.put(bulkKey("a"), await journalOf(chunkLine("large", "L")));
        world.remote.put(`_index-${bulkKey("a")}`, encoder.encode("not a journal"));
        world.remote.put(docsKey("b"), await journalOf(documentLine("note")));
        // Storage which cannot report sizes or upload order lists by name, as for a client of an earlier version.
        const byName = createDevice("by-name", {
            ...world.remote.storage,
            listObjectsInUploadOrder: undefined,
            listFilesInUploadOrder: undefined,
        });

        await synchronise(receiver);
        await synchronise(byName);

        expect(downloadsOf(world.remote, `_index-${bulkKey("a")}`)).toEqual([]);
        expect(receiver.handedOn).toEqual(["note@1-rev1"]);
        expect(byName.handedOn).toEqual(["note@1-rev1"]);
    });

    it("encrypts the index and the removal record like a journal", async () => {
        const world = createWorld();
        const encrypted = {
            encrypt: true,
            passphrase: "synthetic passphrase",
            E2EEAlgorithm: E2EEAlgorithms.ForceV1,
            usePathObfuscation: false,
        };
        const sender = world.device("sender", { cleans: true });
        const receiver = world.device("receiver");
        Object.assign(sender.host.settings, encrypted);
        Object.assign(receiver.host.settings, encrypted);
        await writeFile(sender, "recording", ["part-1", "part-2"]);
        await synchronise(sender);
        await deleteFile(sender, "recording");
        await synchronise(sender);
        await synchroniseAfterInterval(world, sender);
        await synchroniseAfterInterval(world, sender);

        const [record] = removalRecords(world.remote);
        expect(record).toBeDefined();
        const besideJournals = world.remote.uploadBeside.mock.calls;
        const [index] = besideJournals.map(([key]) => key).filter((key) => key.startsWith("_index-"));
        for (const key of [record, index]) {
            const stored = besideJournals.find(([uploaded]) => uploaded === key)![1];
            // What is stored is neither the text nor its plain compression.
            expect(new TextDecoder().decode(stored)).not.toContain("part-1");
            await expect(wrappedInflate(stored as Uint8Array<ArrayBuffer>, {})).rejects.toBeDefined();
        }
        // A device with the same passphrase reads the record.
        await synchronise(receiver);
        expect(receiver.checkpoint.receivedFiles.has(record)).toBe(true);
    });
});

describe("JournalCleaner removal of journals whose content nothing uses", () => {
    it("removes the journals of a deleted file in the second round which finds them unused, record first", async () => {
        const world = createWorld();
        const { receiver, journal } = await fileSentReceivedAndDeleted(world, { cleans: true });

        // The first round only notes what it found.
        await synchroniseAfterInterval(world, receiver);
        expect(cleaningStateOf(receiver)?.assessment?.dead).toEqual([journal]);
        expect(bulkJournals(world.remote)).toEqual([journal]);
        expect(removalRecords(world.remote)).toEqual([]);

        await synchroniseAfterInterval(world, receiver);

        expect(bulkJournals(world.remote)).toEqual([]);
        expect(indexObjects(world.remote)).toEqual([]);
        const [recordKey] = removalRecords(world.remote);
        expect(recordKey).toMatch(/^_removed-[a-f0-9]{64}\.json\.gz$/u);
        // The record was stored, and then the journal and its index were deleted.
        expect(world.remote.transfers.at(-1)).toBe(`upload ${recordKey}`);
        expect(deletedOf(world.remote)).toEqual([journal, `_index-${journal}`]);
        await expect(readRecord(receiver, recordKey)).resolves.toEqual({
            version: 1,
            at: world.clock.now,
            assessmentStoredAt: world.clock.now,
            chunkIDs: ["h:part-1", "h:part-2"],
            journals: [journal],
        });
        // The device which removed them acts on its own record as every other device does.
        expect(receiver.checkpoint.knownIDs.has("h:part-1")).toBe(false);
        expect(receiver.checkpoint.receivedFiles.has(recordKey)).toBe(true);
        expect(receiver.checkpoint.receivedFiles.has(journal)).toBe(true);
        expect(knownContentOf(receiver)).toEqual([]);
        expect(cleaningStateOf(receiver)?.assessment?.dead).toEqual([]);
        // Its local database keeps the chunks.
        await expect(hasChunk(receiver, "part-1")).resolves.toBe(true);
    });

    it("lets a device which comes later apply the file and its deletion without fetching the content", async () => {
        const world = createWorld();
        const { receiver, journal } = await fileSentReceivedAndDeleted(world, { cleans: true });
        await synchroniseAfterInterval(world, receiver);
        await synchroniseAfterInterval(world, receiver);
        const late = world.device("late");
        world.remote.transfers.length = 0;

        await synchronise(late);

        expect(late.handedOn.map((revision) => revision.split("@")[0])).toEqual(["recording", "recording"]);
        expect(downloadsOf(world.remote, journal)).toEqual([]);
        await expect(hasChunk(late, "part-1")).resolves.toBe(false);
        expect(late.statics.value.syncStatus).toBe("COMPLETED");
        expect(late.core.hasDeferredRemoteContent()).toBe(false);
    });

    it("removes them on a device which never fetched the content, by means of the index", async () => {
        const world = createWorld();
        const sender = world.device("sender");
        const phone = world.device("phone", { mobile: true, cleans: true });
        // The phone puts large content off, and never gets to it.
        phone.core.mayReceiveBulkJournals = () => false;
        await writeFile(sender, "recording", ["part-1", "part-2"]);
        await synchronise(sender);
        await synchronise(phone);
        await deleteFile(sender, "recording");
        await synchronise(sender);
        const [journal] = bulkJournals(world.remote);
        expect(phone.core.hasDeferredRemoteContent()).toBe(true);

        await synchroniseAfterInterval(world, phone);
        await synchroniseAfterInterval(world, phone);

        expect(bulkJournals(world.remote)).toEqual([]);
        expect(downloadsOf(world.remote, journal)).toEqual([]);
        expect(downloadsOf(world.remote, `_index-${journal}`)).toHaveLength(1);
        await expect(hasChunk(phone, "part-1")).resolves.toBe(false);
        // Nothing is left for the phone to fetch.
        expect(phone.core.hasDeferredRemoteContent()).toBe(false);
    });

    it("needs two assessments at least ten minutes apart, and rounds at least ten minutes apart", async () => {
        const world = createWorld();
        const { receiver, journal } = await fileSentReceivedAndDeleted(world, { cleans: true });
        await synchroniseAfterInterval(world, receiver);
        const firstRoundAt = world.clock.now;
        expect(cleaningStateOf(receiver)).toEqual({
            lastRoundAt: firstRoundAt,
            assessment: { at: firstRoundAt, dead: [journal] },
        });

        // Nine minutes later no round runs, however often the device synchronises.
        world.clock.now += CLEANING_INTERVAL_MS - MINUTE;
        await synchronise(receiver);
        await synchronise(receiver);
        expect(cleaningStateOf(receiver)?.lastRoundAt).toBe(firstRoundAt);
        expect(bulkJournals(world.remote)).toEqual([journal]);

        // Neither after a restart, as the time of the last round is kept.
        const restarted = receiver.createPaneClient();
        restarted.now = () => world.clock.now;
        await expect(restarted.sync()).resolves.toBe(true);
        expect(cleaningStateOf(receiver)?.lastRoundAt).toBe(firstRoundAt);

        // Ten minutes after the first round the second one runs, and removes what both found unused.
        world.clock.now += MINUTE;
        await synchronise(receiver);
        expect(cleaningStateOf(receiver)?.lastRoundAt).toBe(firstRoundAt + CLEANING_INTERVAL_MS);
        expect(bulkJournals(world.remote)).toEqual([]);
    });

    it("runs a round when the clock was set back behind the last one", async () => {
        const world = createWorld();
        const { receiver } = await fileSentReceivedAndDeleted(world, { cleans: true });
        await synchroniseAfterInterval(world, receiver);

        world.clock.now -= 24 * 60 * MINUTE;
        await synchronise(receiver);

        expect(cleaningStateOf(receiver)?.lastRoundAt).toBe(world.clock.now);
    });

    it("does not remove a journal which was found unused only in the latest round", async () => {
        const world = createWorld();
        const sender = world.device("sender");
        const cleaner = world.device("cleaner", { cleans: true });
        await writeFile(sender, "first", ["first-1"]);
        await synchronise(sender);
        await writeFile(sender, "second", ["second-1"]);
        await synchronise(sender);
        const [firstJournal, secondJournal] = bulkJournals(world.remote);
        await deleteFile(sender, "first");
        await synchronise(sender);
        await synchronise(cleaner);

        await deleteFile(sender, "second");
        await synchronise(sender);
        await synchroniseAfterInterval(world, cleaner);

        expect(bulkJournals(world.remote)).toEqual([secondJournal]);
        expect(deletedOf(world.remote)).toContain(firstJournal);

        await synchroniseAfterInterval(world, cleaner);
        expect(bulkJournals(world.remote)).toEqual([]);
    });

    it("keeps the content of a file which is deleted and put back between two rounds", async () => {
        const world = createWorld();
        const { sender, receiver, journal } = await fileSentReceivedAndDeleted(world, { cleans: true });
        await synchroniseAfterInterval(world, receiver);

        await writeFileFromChunksHere(sender, "recording-again", ["part-1", "part-2"]);
        await synchronise(sender);
        for (let round = 0; round < 3; round++) await synchroniseAfterInterval(world, receiver);

        expect(bulkJournals(world.remote)).toEqual([journal]);
        expect(removalRecords(world.remote)).toEqual([]);
    });

    it("keeps a journal of which a single chunk is in use, in any number of rounds on any device", async () => {
        const world = createWorld();
        const sender = world.device("sender", { cleans: true });
        const desktop = world.device("desktop", { cleans: true });
        const phone = world.device("phone", { mobile: true, cleans: true });
        // Two files whose chunks travel in one journal.
        await sender.db.bulkDocs([
            largeChunk("deleted-1"),
            largeChunk("shared"),
            { ...note("deleted", ["deleted-1", "shared"]), size: LARGE_FILE_BYTES },
            { ...note("kept", ["shared"]), size: LARGE_FILE_BYTES },
        ]);
        await synchronise(sender);
        await deleteFile(sender, "deleted");
        await synchronise(sender);
        const [journal] = bulkJournals(world.remote);

        for (let round = 0; round < 4; round++) {
            for (const device of [sender, desktop, phone]) await synchroniseAfterInterval(world, device);
        }

        expect(bulkJournals(world.remote)).toEqual([journal]);
        expect(removalRecords(world.remote)).toEqual([]);
        expect(deletedOf(world.remote)).toEqual([]);
    });

    it("counts the chunks of an unresolved conflict as in use, although the winning revision is deleted", async () => {
        const world = createWorld();
        const sender = world.device("sender", { cleans: true });
        const other = world.device("other", { cleans: true });
        await sender.db.bulkDocs([largeChunk("part-1")]);
        const base = { ...note("recording", ["part-1"]), size: LARGE_FILE_BYTES };
        // Two revisions were made on the first one: a deletion, which wins, and an edit which still uses the chunk.
        await sender.db.bulkDocs(
            [
                { ...base, _rev: "1-base", _revisions: { start: 1, ids: ["base"] } },
                { ...base, deleted: true, _rev: "2-zzz", _revisions: { start: 2, ids: ["zzz", "base"] } },
                { ...base, mtime: 2, _rev: "2-aaa", _revisions: { start: 2, ids: ["aaa", "base"] } },
            ] as EntryDoc[],
            { new_edits: false }
        );
        expect(await sender.db.get("recording", { conflicts: true })).toMatchObject({
            _rev: "2-zzz",
            deleted: true,
            _conflicts: ["2-aaa"],
        });
        await synchronise(sender);
        const [journal] = bulkJournals(world.remote);

        for (let round = 0; round < 3; round++) {
            for (const device of [sender, other]) await synchroniseAfterInterval(world, device);
        }
        expect(bulkJournals(world.remote)).toEqual([journal]);

        // Once the conflict is resolved in favour of the deletion, nothing uses the chunk.
        await sender.db.remove("recording", "2-aaa");
        await synchronise(sender);
        await synchroniseAfterInterval(world, other);
        await synchroniseAfterInterval(world, other);
        expect(bulkJournals(world.remote)).toEqual([]);
    });

    it("removes nothing with the setting off, but still stores indexes, reads records and sends chunks again", async () => {
        const world = createWorld();
        const { sender, receiver, journal } = await fileSentReceivedAndDeleted(world);
        for (let round = 0; round < 3; round++) {
            for (const device of [sender, receiver]) await synchroniseAfterInterval(world, device);
        }
        expect(bulkJournals(world.remote)).toEqual([journal]);
        expect(indexObjects(world.remote)).toHaveLength(1);
        expect(cleaningStateOf(receiver)).toBeUndefined();

        // Another device cleans. The devices with the setting off act on its record.
        const cleaner = world.device("cleaner", { cleans: true });
        await synchronise(cleaner);
        await synchroniseAfterInterval(world, cleaner);
        const [recordKey] = removalRecords(world.remote);
        await synchronise(sender);
        expect(sender.checkpoint.receivedFiles.has(recordKey)).toBe(true);
        expect(sender.checkpoint.sentIDs.has("h:part-1")).toBe(false);

        await writeFileFromChunksHere(sender, "recording-again", ["part-1", "part-2"]);
        await synchronise(sender);
        expect(bulkJournals(world.remote)).toHaveLength(1);
        expect(indexObjects(world.remote)).toHaveLength(1);
    });

    it("only cleans in a synchronisation, not when journals are only received or only sent", async () => {
        const world = createWorld();
        const { receiver, journal } = await fileSentReceivedAndDeleted(world, { cleans: true });
        receiver.entries.delete(`bucketsync-cleaning-${receiver.core.hash}`);

        for (let round = 0; round < 3; round++) {
            world.clock.now += CLEANING_INTERVAL_MS + MINUTE;
            await expect(receiver.core.receiveRemoteJournal()).resolves.toBe(true);
            await expect(receiver.core.sendLocalJournal()).resolves.toBe(true);
        }

        expect(cleaningStateOf(receiver)).toBeUndefined();
        expect(bulkJournals(world.remote)).toEqual([journal]);
    });

    it.each<[string, (world: World, receiver: Device) => Promise<void> | void, boolean]>([
        [
            "its receive of journals with documents failed",
            (world) => {
                world.remote.put(docsKey("9"), encoder.encode("not a compressed journal"));
            },
            false,
        ],
        [
            "its send failed",
            async (world, receiver) => {
                await receiver.db.put(note("written-here"));
                world.remote.upload.mockImplementation(async () => false);
            },
            false,
        ],
        [
            "a stop was requested",
            (_world, receiver) => {
                // The stop arrives once everything is sent, so the synchronisation itself succeeds.
                const send = receiver.core.sendLocalJournal.bind(receiver.core);
                vi.spyOn(receiver.core, "sendLocalJournal").mockImplementationOnce(async (showMessage) => {
                    const sent = await send(showMessage);
                    receiver.core.requestStop();
                    return sent;
                });
            },
            true,
        ],
    ])("does not clean in a synchronisation when %s", async (_case, disturb, result) => {
        const world = createWorld();
        const { receiver, journal } = await fileSentReceivedAndDeleted(world, { cleans: true });
        await synchroniseAfterInterval(world, receiver);
        const stateBefore = cleaningStateOf(receiver);
        world.clock.now += CLEANING_INTERVAL_MS + MINUTE;
        await disturb(world, receiver);

        await expect(receiver.core.sync()).resolves.toBe(result);

        expect(cleaningStateOf(receiver)).toEqual(stateBefore);
        expect(bulkJournals(world.remote)).toEqual([journal]);
    });

    it("does not remove when the earlier assessment is less than ten minutes old", async () => {
        const world = createWorld();
        const { receiver, journal } = await fileSentReceivedAndDeleted(world, { cleans: true });
        // The first round takes eight minutes to look through the documents.
        const allDocs = receiver.db.allDocs.bind(receiver.db) as (options: unknown) => Promise<unknown>;
        vi.spyOn(receiver.db, "allDocs").mockImplementationOnce((async (options: unknown) => {
            world.clock.now += 8 * MINUTE;
            return await allDocs(options);
        }) as never);
        world.clock.now += CLEANING_INTERVAL_MS;
        await synchronise(receiver);
        const firstRoundAt = cleaningStateOf(receiver)!.lastRoundAt!;
        expect(cleaningStateOf(receiver)?.assessment).toEqual({ at: firstRoundAt + 8 * MINUTE, dead: [journal] });

        // The next round starts ten minutes after the first one started, which is two after its assessment.
        world.clock.now = firstRoundAt + CLEANING_INTERVAL_MS;
        await synchronise(receiver);
        expect(cleaningStateOf(receiver)?.lastRoundAt).toBe(firstRoundAt + CLEANING_INTERVAL_MS);
        expect(bulkJournals(world.remote)).toEqual([journal]);

        await synchroniseAfterInterval(world, receiver);
        expect(bulkJournals(world.remote)).toEqual([]);
    });

    it("stores no record for journals which another device has removed meanwhile", async () => {
        const world = createWorld();
        const { receiver, journal } = await fileSentReceivedAndDeleted(world, { cleans: true });
        await synchroniseAfterInterval(world, receiver);
        const listRemote = receiver.core.listRemote.bind(receiver.core);
        let listings = 0;
        vi.spyOn(receiver.core, "listRemote").mockImplementation(async () => {
            if (++listings === 2) await world.remote.storage.deleteFiles([journal, `_index-${journal}`]);
            return await listRemote();
        });

        await synchroniseAfterInterval(world, receiver);

        expect(listings).toBe(2);
        expect(removalRecords(world.remote)).toEqual([]);
        expect(deletedOf(world.remote)).toEqual([journal, `_index-${journal}`]);
    });

    it("stores no record when a stop is requested before the removal", async () => {
        const world = createWorld();
        const { receiver, journal } = await fileSentReceivedAndDeleted(world, { cleans: true });
        await synchroniseAfterInterval(world, receiver);
        const listRemote = receiver.core.listRemote.bind(receiver.core);
        let listings = 0;
        vi.spyOn(receiver.core, "listRemote").mockImplementation(async () => {
            if (++listings === 2) receiver.core.requestStop();
            return await listRemote();
        });

        await synchroniseAfterInterval(world, receiver);

        expect(removalRecords(world.remote)).toEqual([]);
        expect(bulkJournals(world.remote)).toEqual([journal]);
    });

    it("does not fail the synchronisation when the round fails, and tries again later", async () => {
        const world = createWorld();
        const { receiver, journal } = await fileSentReceivedAndDeleted(world, { cleans: true });
        await synchroniseAfterInterval(world, receiver);
        world.remote.deleteFiles.mockRejectedValueOnce(new Error("synthetic failure"));

        await synchroniseAfterInterval(world, receiver);
        expect(receiver.statics.value.syncStatus).toBe("COMPLETED");
        expect(bulkJournals(world.remote)).toEqual([journal]);

        await synchroniseAfterInterval(world, receiver);
        expect(bulkJournals(world.remote)).toEqual([]);
    });

    it("counts as a running transfer until the round has ended", async () => {
        const world = createWorld();
        const { receiver } = await fileSentReceivedAndDeleted(world, { cleans: true });
        await synchroniseAfterInterval(world, receiver);
        let releaseDelete!: () => void;
        const deleteFiles = world.remote.deleteFiles.getMockImplementation()!;
        const deleteStarted = new Promise<void>((started) => {
            world.remote.deleteFiles.mockImplementationOnce(async (keys) => {
                started();
                await new Promise<void>((release) => (releaseDelete = release));
                return await deleteFiles(keys);
            });
        });
        world.clock.now += CLEANING_INTERVAL_MS + MINUTE;
        const synchronising = receiver.core.sync();
        await deleteStarted;
        let settled = false;
        const waiting = receiver.core.waitForTransfersToSettle().then(() => (settled = true));

        await Promise.resolve();
        expect(settled).toBe(false);
        releaseDelete();
        await waiting;
        await expect(synchronising).resolves.toBe(true);
    });

    it("deletes nothing when the removal record cannot be stored", async () => {
        const world = createWorld();
        const { receiver, journal } = await fileSentReceivedAndDeleted(world, { cleans: true });
        await synchroniseAfterInterval(world, receiver);
        const storeUpload = world.remote.uploadBeside.getMockImplementation()!;
        world.remote.uploadBeside.mockImplementation(async (key, ...rest) =>
            key.startsWith(REMOVAL_RECORD_PREFIX) ? false : await storeUpload(key, ...rest)
        );

        await synchroniseAfterInterval(world, receiver);

        expect(bulkJournals(world.remote)).toEqual([journal]);
        expect(deletedOf(world.remote)).toEqual([]);
        // The device goes on counting the chunks as present, which they are.
        expect(receiver.checkpoint.knownIDs.has("h:part-1")).toBe(true);

        world.remote.uploadBeside.mockImplementation(storeUpload);
        await synchroniseAfterInterval(world, receiver);
        expect(bulkJournals(world.remote)).toEqual([]);
    });

    it("deletes nothing in the round when its record is stored but the upload reports failure, and removes later", async () => {
        const world = createWorld();
        const { receiver, journal } = await fileSentReceivedAndDeleted(world, { cleans: true });
        await synchroniseAfterInterval(world, receiver);
        // The record reaches the store in time, but the upload ends without an answer.
        const storeUpload = world.remote.uploadBeside.getMockImplementation()!;
        world.remote.uploadBeside.mockImplementation(async (key, ...rest) => {
            const stored = await storeUpload(key, ...rest);
            return key.startsWith(REMOVAL_RECORD_PREFIX) ? false : stored;
        });

        await synchroniseAfterInterval(world, receiver);

        const [recordKey] = removalRecords(world.remote);
        expect(world.remote.storedAtOf(recordKey)).toBe(world.clock.now);
        expect(bulkJournals(world.remote)).toEqual([journal]);
        expect(deletedOf(world.remote)).toEqual([]);
        expect(receiver.checkpoint.knownIDs.has("h:part-1")).toBe(true);
        expect(receiver.checkpoint.receivedFiles.has(recordKey)).toBe(false);

        // In its next synchronisation the device meets its record in the listing and acts on it, like any device
        // which reads it. The journal, which outlived that record, goes in a round of its own.
        world.remote.uploadBeside.mockImplementation(storeUpload);
        receiver.host.settings.removeUnreferencedJournalData = false;
        await synchronise(receiver);
        expect(receiver.checkpoint.receivedFiles.has(recordKey)).toBe(true);
        expect(receiver.checkpoint.knownIDs.has("h:part-1")).toBe(false);
        expect(bulkJournals(world.remote)).toEqual([journal]);

        receiver.host.settings.removeUnreferencedJournalData = true;
        await synchroniseAfterInterval(world, receiver);
        expect(bulkJournals(world.remote)).toEqual([]);
        expect(removalRecords(world.remote)).toHaveLength(2);
    });

    it("leaves journals which could not be deleted to a later round, which stores a record of its own", async () => {
        const world = createWorld();
        const { receiver, journal } = await fileSentReceivedAndDeleted(world, { cleans: true });
        await synchroniseAfterInterval(world, receiver);
        world.remote.deleteFiles.mockImplementationOnce(async () => false);

        await synchroniseAfterInterval(world, receiver);
        expect(bulkJournals(world.remote)).toEqual([journal]);
        expect(removalRecords(world.remote)).toHaveLength(1);

        await synchroniseAfterInterval(world, receiver);
        expect(bulkJournals(world.remote)).toEqual([]);
        expect(removalRecords(world.remote)).toHaveLength(2);
    });

    it("deletes many journals and their indexes in requests of at most a thousand objects", async () => {
        const world = createWorld();
        const cleaner = world.device("cleaner", { cleans: true });
        const journals = Array.from(
            { length: 501 },
            (_, index) => `${index.toString(16).padStart(64, "0")}-chunks.jsonl.gz`
        );
        // The device knows what the journals hold, as after earlier rounds, so that nothing has to be downloaded.
        for (const [index, key] of journals.entries()) {
            world.remote.put(key, encoder.encode("journal"));
            world.remote.put(`_index-${key}`, encoder.encode("index"));
            cleaner.entries.set(`bucketsync-journal-content-${cleaner.core.hash}-${key}`, {
                pure: true,
                chunkIDs: [`h:unused-${index}`],
            });
        }
        cleaner.core.mayReceiveBulkJournals = () => false;
        await cleaner.db.put(note("kept"));

        await synchronise(cleaner);
        await synchroniseAfterInterval(world, cleaner);

        // The last request deletes the assessment marker of the round.
        expect(world.remote.deleteFiles.mock.calls.map(([keys]) => keys.length)).toEqual([1000, 2, 1]);
        expect(bulkJournals(world.remote)).toEqual([]);
        expect(indexObjects(world.remote)).toEqual([]);
        const [recordKey] = removalRecords(world.remote);
        expect((await readRecord(cleaner, recordKey)).journals).toHaveLength(501);
    });

    describe("looking through the documents", () => {
        /** Count the looks through the documents: each begins with the documents before the chunks. */
        const scansOf = (device: Device) => {
            const allDocs = vi.spyOn(device.db, "allDocs");
            return () =>
                allDocs.mock.calls.filter(([options]) => (options as { endkey?: string })?.endkey === "h:").length;
        };

        it("looks only when something may have gone out of use since the last look", async () => {
            const world = createWorld();
            const sender = world.device("sender");
            const cleaner = world.device("cleaner", { cleans: true });
            await writeFile(sender, "kept", ["kept-1"]);
            await synchronise(sender);
            const scans = scansOf(cleaner);

            // The first round after a start looks, and finds everything in use.
            await synchronise(cleaner);
            expect(scans()).toBe(1);
            await synchroniseAfterInterval(world, cleaner);
            expect(scans()).toBe(1);

            // A note is no reason to look, as it has no large chunks.
            await sender.db.put(note("small-note"));
            await synchronise(sender);
            await synchroniseAfterInterval(world, cleaner);
            expect(scans()).toBe(1);

            // A journal of large chunks which this device learns of is: here one which no document came with.
            await sender.db.put(largeChunk("stray-1"));
            await synchronise(sender);
            await synchroniseAfterInterval(world, cleaner);
            expect(scans()).toBe(2);
            // So is a round which found journals unused: the next round removes the stray chunk.
            await synchroniseAfterInterval(world, cleaner);
            expect(scans()).toBe(3);
            expect(bulkJournals(world.remote)).toHaveLength(1);
            await synchroniseAfterInterval(world, cleaner);
            expect(scans()).toBe(3);

            // And so is a deletion.
            await deleteFile(sender, "kept");
            await synchronise(sender);
            await synchroniseAfterInterval(world, cleaner);
            expect(scans()).toBe(4);
            await synchroniseAfterInterval(world, cleaner);
            expect(scans()).toBe(5);
            expect(bulkJournals(world.remote)).toEqual([]);
            // With nothing which could be removed there is nothing to look for.
            await synchroniseAfterInterval(world, cleaner);
            expect(scans()).toBe(5);
        });

        it("looks after this device itself sent a deletion, or received a large file which replaces another", async () => {
            const world = createWorld();
            const sender = world.device("sender");
            const cleaner = world.device("cleaner", { cleans: true });
            await writeFile(cleaner, "mine", ["mine-1"]);
            await writeFile(sender, "theirs", ["theirs-1"]);
            await synchronise(sender);
            await synchronise(cleaner);
            await synchroniseAfterInterval(world, cleaner);
            const scans = scansOf(cleaner);
            await synchroniseAfterInterval(world, cleaner);
            expect(scans()).toBe(0);

            // This device deletes its own file. The deletion is sent, not received.
            await deleteFile(cleaner, "mine");
            await synchroniseAfterInterval(world, cleaner);
            expect(scans()).toBe(1);
            await synchroniseAfterInterval(world, cleaner);
            expect(scans()).toBe(2);
            expect(bulkJournals(world.remote)).toHaveLength(1);
            await synchroniseAfterInterval(world, cleaner);
            expect(scans()).toBe(2);

            // The other device replaces its large file by one which uses none of the chunks.
            const theirs = (await sender.db.get("theirs")) as PlainEntry;
            await sender.db.put({ ...theirs, children: [], mtime: 5 });
            await synchronise(sender);
            await synchroniseAfterInterval(world, cleaner);
            expect(scans()).toBe(3);
            await synchroniseAfterInterval(world, cleaner);
            expect(bulkJournals(world.remote)).toEqual([]);
        });

        it("looks once a day whatever it was told, and so finds the chunks of a large file which became small", async () => {
            const world = createWorld();
            const sender = world.device("sender");
            const cleaner = world.device("cleaner", { cleans: true });
            await writeFile(sender, "shrunk", ["shrunk-1"]);
            await synchronise(sender);
            await synchronise(cleaner);
            const scans = scansOf(cleaner);

            // The file is replaced by a small version without the chunk. A revision of that size tells no device to look.
            const shrunk = (await sender.db.get("shrunk")) as PlainEntry;
            await sender.db.put({ ...shrunk, children: [], size: 1, mtime: 5 });
            await synchronise(sender);
            await synchroniseAfterInterval(world, cleaner);
            expect(scans()).toBe(0);

            world.clock.now += FULL_SCAN_INTERVAL_MS;
            await synchronise(cleaner);
            expect(scans()).toBe(1);
            await synchroniseAfterInterval(world, cleaner);
            expect(bulkJournals(world.remote)).toEqual([]);
        });

        it("does not look while the remote has no journal which could be removed", async () => {
            const world = createWorld();
            const sender = world.device("sender");
            const cleaner = world.device("cleaner", { cleans: true });
            await sender.db.put(note("small-note"));
            await synchronise(sender);
            const scans = scansOf(cleaner);

            for (let round = 0; round < 3; round++) await synchroniseAfterInterval(world, cleaner);

            expect(scans()).toBe(0);
            // The time of the round is kept all the same, so that the next one waits its turn.
            expect(cleaningStateOf(cleaner)).toEqual({ lastRoundAt: world.clock.now });
        });

        it("looks again after a restart", async () => {
            const world = createWorld();
            const sender = world.device("sender");
            const cleaner = world.device("cleaner", { cleans: true });
            await writeFile(sender, "kept", ["kept-1"]);
            await synchronise(sender);
            const scans = scansOf(cleaner);
            await synchronise(cleaner);
            const restarted = cleaner.createPaneClient();
            restarted.now = () => world.clock.now;

            world.clock.now += CLEANING_INTERVAL_MS + MINUTE;
            await expect(restarted.sync()).resolves.toBe(true);

            expect(scans()).toBe(2);
        });

        it("counts the chunks of hidden files and of other documents beside the notes", async () => {
            const world = createWorld();
            const sender = world.device("sender", { cleans: true });
            await sender.db.bulkDocs([
                largeChunk("hidden-1"),
                largeChunk("plugin-1"),
                largeChunk("upper-1"),
                { ...note("i:.hidden/file", ["hidden-1"]), size: LARGE_FILE_BYTES },
                { ...note("ps:plugin/data", ["plugin-1"]), size: LARGE_FILE_BYTES },
                { ...note("Zeta", ["upper-1"]), size: LARGE_FILE_BYTES },
            ]);
            await synchronise(sender);

            for (let round = 0; round < 3; round++) await synchroniseAfterInterval(world, sender);

            expect(bulkJournals(world.remote)).toHaveLength(1);
            expect(removalRecords(world.remote)).toEqual([]);
        });

        it("looks through more documents than one page holds", async () => {
            const world = createWorld();
            const sender = world.device("sender", { cleans: true });
            // The file which uses the chunk sorts after two pages of other documents, before the chunks.
            await sender.db.bulkDocs(Array.from({ length: 450 }, (_, index) => note(`a-note-${index}`)));
            await writeFile(sender, "g-recording", ["part-1"]);
            await synchronise(sender);

            for (let round = 0; round < 3; round++) await synchroniseAfterInterval(world, sender);

            expect(bulkJournals(world.remote)).toHaveLength(1);
        });

        it.each([
            ["a mobile device gives up after ten seconds", true, MOBILE_SCAN_LIMIT_MS + 1, false],
            ["a mobile device finishes within ten seconds", true, MOBILE_SCAN_LIMIT_MS - 1, true],
            ["a desktop device takes the time it needs", false, MOBILE_SCAN_LIMIT_MS + 1, true],
        ])("%s", async (_case, mobile, timePerPage, removes) => {
            const world = createWorld();
            const { receiver, journal } = await fileSentReceivedAndDeleted(world, { mobile, cleans: true });
            const assessmentBefore = cleaningStateOf(receiver)?.assessment;
            const allDocs = receiver.db.allDocs.bind(receiver.db) as (options: unknown) => Promise<unknown>;
            vi.spyOn(receiver.db, "allDocs").mockImplementation((async (options: unknown) => {
                const page = await allDocs(options);
                world.clock.now += timePerPage;
                return page;
            }) as never);

            for (let round = 0; round < 3; round++) await synchroniseAfterInterval(world, receiver);

            expect(bulkJournals(world.remote)).toEqual(removes ? [] : [journal]);
            // A look which was given up leaves no assessment behind.
            if (!removes) expect(cleaningStateOf(receiver)?.assessment).toEqual(assessmentBefore);
        });

        it.each([
            ["does not look again within a day after a mobile device ran out of time", -1, 1],
            ["looks again a day after a mobile device ran out of time", 0, 2],
        ])("%s", async (_case, beforeOrAt, expectedLooks) => {
            const world = createWorld();
            const { receiver, journal } = await fileSentReceivedAndDeleted(world, { mobile: true, cleans: true });
            const allDocs = receiver.db.allDocs.bind(receiver.db) as (options: unknown) => Promise<unknown>;
            let looks = 0;
            let isSlow = true;
            vi.spyOn(receiver.db, "allDocs").mockImplementation((async (options: unknown) => {
                const page = await allDocs(options);
                if ((options as { endkey?: string } | undefined)?.endkey === "h:") {
                    looks++;
                    if (isSlow) world.clock.now += MOBILE_SCAN_LIMIT_MS + 1;
                }
                return page;
            }) as never);

            await synchroniseAfterInterval(world, receiver);
            const timedOutAt = cleaningStateOf(receiver)?.scanTimedOutAt;
            expect(timedOutAt).toBe(world.clock.now);
            expect(looks).toBe(1);

            // The following rounds do not look, and neither does one after a restart.
            isSlow = false;
            for (let round = 0; round < 3; round++) await synchroniseAfterInterval(world, receiver);
            const restarted = receiver.createPaneClient();
            restarted.now = () => world.clock.now;
            world.clock.now += CLEANING_INTERVAL_MS + MINUTE;
            await expect(restarted.sync()).resolves.toBe(true);
            expect(looks).toBe(1);
            expect(cleaningStateOf(receiver)?.scanTimedOutAt).toBe(timedOutAt);

            world.clock.now = timedOutAt! + MOBILE_SCAN_RETRY_MS + beforeOrAt;
            await synchronise(receiver);

            expect(looks).toBe(expectedLooks);
            if (expectedLooks === 2) {
                // The look which was finished leaves an assessment, and nothing of the earlier one which was not.
                expect(cleaningStateOf(receiver)).toEqual({
                    lastRoundAt: world.clock.now,
                    assessment: { at: world.clock.now, dead: [journal] },
                });
            }
        });

        it("ends on a stop request, and looks again in the next round", async () => {
            const world = createWorld();
            const { receiver, journal } = await fileSentReceivedAndDeleted(world, { cleans: true });
            const assessmentBefore = cleaningStateOf(receiver)?.assessment;
            const allDocs = receiver.db.allDocs.bind(receiver.db) as (options: unknown) => Promise<unknown>;
            vi.spyOn(receiver.db, "allDocs").mockImplementationOnce((async (options: unknown) => {
                receiver.core.requestStop();
                return await allDocs(options);
            }) as never);

            await synchroniseAfterInterval(world, receiver);
            expect(cleaningStateOf(receiver)?.assessment).toEqual(assessmentBefore);

            await synchroniseAfterInterval(world, receiver);
            expect(cleaningStateOf(receiver)?.assessment?.dead).toEqual([journal]);
        });
    });

    describe("when the assessment has gone stale", () => {
        it("removes nothing when the local database changed after it was looked through", async () => {
            const world = createWorld();
            const { receiver, journal } = await fileSentReceivedAndDeleted(world, { cleans: true });
            await synchroniseAfterInterval(world, receiver);
            // A file is put back on this device after the documents were looked through.
            const allDocs = receiver.db.allDocs.bind(receiver.db) as (options: unknown) => Promise<unknown>;
            let changed = false;
            vi.spyOn(receiver.db, "allDocs").mockImplementation((async (options: unknown) => {
                const page = await allDocs(options);
                if (!changed && (options as { startkey?: string }).startkey === "h;") {
                    changed = true;
                    await writeFileFromChunksHere(receiver, "recording-again", ["part-1", "part-2"]);
                }
                return page;
            }) as never);

            await synchroniseAfterInterval(world, receiver);

            expect(changed).toBe(true);
            expect(bulkJournals(world.remote)).toEqual([journal]);
            expect(removalRecords(world.remote)).toEqual([]);
            // The next rounds see the file, and keep its content.
            for (let round = 0; round < 3; round++) await synchroniseAfterInterval(world, receiver);
            expect(bulkJournals(world.remote)).toEqual([journal]);
        });

        it("removes nothing when a journal with documents has appeared which it has not received", async () => {
            const world = createWorld();
            const { sender, receiver, journal } = await fileSentReceivedAndDeleted(world, { cleans: true });
            await synchroniseAfterInterval(world, receiver);
            // Another device puts the file back and sends it while this device is in its round.
            await writeFileFromChunksHere(sender, "recording-again", ["part-1", "part-2"]);
            const listRemote = receiver.core.listRemote.bind(receiver.core);
            let listings = 0;
            vi.spyOn(receiver.core, "listRemote").mockImplementation(async () => {
                if (++listings === 2) await synchroniseBeside(sender);
                return await listRemote();
            });

            await synchroniseAfterInterval(world, receiver);

            expect(listings).toBe(2);
            expect(bulkJournals(world.remote)).toEqual([journal]);
            expect(removalRecords(world.remote)).toEqual([]);
            for (let round = 0; round < 3; round++) await synchroniseAfterInterval(world, receiver);
            expect(bulkJournals(world.remote)).toEqual([journal]);
            // A third device reads the file which was put back.
            const third = world.device("third");
            await synchronise(third);
            await expect(hasChunk(third, "part-1")).resolves.toBe(true);
        });

        /** Let time jump once, as when a device is suspended in its round and goes on later. */
        const jumpOnce = (world: World, by: number) => {
            let jumped = false;
            return () => {
                if (jumped) return;
                jumped = true;
                world.clock.now += by;
            };
        };
        /** Set the clock of one device back once, while time goes on as before. */
        const setBackOnce = (world: World, device: Device, by: number) => {
            let isSetBack = false;
            return () => {
                if (isSetBack) return;
                isSetBack = true;
                world.clockOffsetOf(device).ms -= by;
            };
        };
        /** Call `act` after the listing which a removal relies on: the second one of a synchronisation. */
        const afterVerifyingListing = (device: Device, act: () => void) => {
            const listRemote = device.core.listRemote.bind(device.core);
            let listings = 0;
            vi.spyOn(device.core, "listRemote").mockImplementation(async () => {
                const listing = await listRemote();
                if (++listings === 2) act();
                return listing;
            });
        };
        /** Call `act` once the device has acted on its own removal record, before it deletes anything. */
        const afterOwnRecordIsApplied = (device: Device, act: () => void) => {
            const apply = device.core.applyRemovalRecords.bind(device.core);
            vi.spyOn(device.core, "applyRemovalRecords").mockImplementationOnce(async (...args) => {
                const checkpoint = await apply(...args);
                act();
                return checkpoint;
            });
        };

        it.each<[string, (world: World, device: Device) => () => void]>([
            ["is more than a minute old", (world) => jumpOnce(world, REMOVAL_FRESHNESS_MS + 1)],
            [
                "lies in the future because the clock of the device was set back",
                (world, device) => setBackOnce(world, device, 5 * MINUTE),
            ],
        ])("stores no record when the listing %s", async (_case, disturbance) => {
            const world = createWorld();
            const { receiver, journal } = await fileSentReceivedAndDeleted(world, { cleans: true });
            await synchroniseAfterInterval(world, receiver);
            afterVerifyingListing(receiver, disturbance(world, receiver));

            await synchroniseAfterInterval(world, receiver);

            expect(bulkJournals(world.remote)).toEqual([journal]);
            expect(removalRecords(world.remote)).toEqual([]);
            expect(deletedOf(world.remote)).toEqual([]);

            // A later round removes the journal.
            world.clock.now += 6 * MINUTE;
            await synchroniseAfterInterval(world, receiver);
            expect(bulkJournals(world.remote)).toEqual([]);
        });

        it("gives the upload of the record up when the listing would be more than a minute old", async () => {
            const world = createWorld();
            const { receiver } = await fileSentReceivedAndDeleted(world, { cleans: true });
            await synchroniseAfterInterval(world, receiver);
            afterVerifyingListing(receiver, jumpOnce(world, 20_000));

            await synchroniseAfterInterval(world, receiver);

            const [recordKey, , , options] = world.remote.uploadBeside.mock.calls.find(([key]) =>
                key.startsWith(REMOVAL_RECORD_PREFIX)
            )!;
            expect(options).toEqual({ keepExisting: true, abortAfterMs: REMOVAL_FRESHNESS_MS - 20_000 });
            // The record names the time at which the store received the marker, before the listing.
            await expect(readRecord(receiver, recordKey)).resolves.toMatchObject({
                assessmentStoredAt: world.clock.now - 20_000,
                at: world.clock.now,
            });
        });

        it.each<[string, (world: World, device: Device) => () => void]>([
            ["the listing is more than a minute old", (world) => jumpOnce(world, REMOVAL_FRESHNESS_MS + 1)],
            ["the clock of the device was set back", (world, device) => setBackOnce(world, device, 5 * MINUTE)],
        ])("deletes nothing after it acted on its record when %s", async (_case, disturbance) => {
            const world = createWorld();
            const { receiver, journal } = await fileSentReceivedAndDeleted(world, { cleans: true });
            await synchroniseAfterInterval(world, receiver);
            afterOwnRecordIsApplied(receiver, disturbance(world, receiver));

            await synchroniseAfterInterval(world, receiver);

            // The record stands, and the device has acted on it like every other device.
            expect(bulkJournals(world.remote)).toEqual([journal]);
            expect(deletedOf(world.remote)).toEqual([]);
            const [recordKey] = removalRecords(world.remote);
            expect(receiver.checkpoint.receivedFiles.has(recordKey)).toBe(true);
            expect(receiver.checkpoint.knownIDs.has("h:part-1")).toBe(false);

            // A later round removes the journal, with a record of its own.
            world.clock.now += 6 * MINUTE;
            await synchroniseAfterInterval(world, receiver);
            expect(bulkJournals(world.remote)).toEqual([]);
            expect(removalRecords(world.remote)).toHaveLength(2);
        });

        it("deletes nothing when a stop is requested after it acted on its record", async () => {
            const world = createWorld();
            const { receiver, journal } = await fileSentReceivedAndDeleted(world, { cleans: true });
            await synchroniseAfterInterval(world, receiver);
            afterOwnRecordIsApplied(receiver, () => receiver.core.requestStop());

            await synchroniseAfterInterval(world, receiver);

            expect(bulkJournals(world.remote)).toEqual([journal]);
            expect(deletedOf(world.remote)).toEqual([]);
            expect(removalRecords(world.remote)).toHaveLength(1);
        });

        describe("and its assessment marker", () => {
            const markerUploads = (remote: Remote) => remote.transfers.filter((entry) => isMarker(entry.slice(7)));

            it("stores the marker before it lists the remote, and deletes it when the round ends", async () => {
                const world = createWorld();
                const { receiver } = await fileSentReceivedAndDeleted(world, { cleans: true });
                // A round which finds nothing to remove stores no marker.
                await synchroniseAfterInterval(world, receiver);
                expect(markerUploads(world.remote)).toEqual([]);
                expect(world.remote.deleteFiles).not.toHaveBeenCalled();
                const listRemote = receiver.core.listRemote.bind(receiver.core);
                const markersAtListing: number[] = [];
                vi.spyOn(receiver.core, "listRemote").mockImplementation(async () => {
                    markersAtListing.push(markersOf(world.remote).length);
                    return await listRemote();
                });

                await synchroniseAfterInterval(world, receiver);

                // The listing of the receive, the verifying listing, and the one which shows the stored record.
                expect(markersAtListing).toEqual([0, 1, 1]);
                expect(markerUploads(world.remote)).toHaveLength(1);
                expect(markersOf(world.remote)).toEqual([]);
                expect(bulkJournals(world.remote)).toEqual([]);
                // Nothing is left to delete in a later round.
                expect(receiver.entries.has(`bucketsync-assessment-markers-${receiver.core.hash}`)).toBe(false);
            });

            it.each<[string, (world: World, device: Device) => void]>([
                [
                    "the verifying listing does not show the marker",
                    (_world, device) => {
                        const listRemote = device.core.listRemote.bind(device.core);
                        vi.spyOn(device.core, "listRemote").mockImplementation(async () => ({
                            ...(await listRemote()),
                            assessmentMarkers: [],
                        }));
                    },
                ],
                [
                    "the storage does not report when it received the marker",
                    (world) => {
                        world.remote.storage.listObjectsInUploadOrder.mockImplementation(async () =>
                            world.remote.groups().map((group) => group.map(({ key, size }) => ({ key, size })))
                        );
                    },
                ],
                [
                    "the marker cannot be stored",
                    (world) => {
                        const storeUpload = world.remote.uploadBeside.getMockImplementation()!;
                        world.remote.uploadBeside.mockImplementation(async (key, ...rest) =>
                            isMarker(key) ? false : await storeUpload(key, ...rest)
                        );
                    },
                ],
            ])("stores no record and removes nothing when %s", async (_case, disturb) => {
                const world = createWorld();
                const { receiver, journal } = await fileSentReceivedAndDeleted(world, { cleans: true });
                await synchroniseAfterInterval(world, receiver);
                disturb(world, receiver);

                for (let round = 0; round < 3; round++) await synchroniseAfterInterval(world, receiver);

                expect(removalRecords(world.remote)).toEqual([]);
                expect(bulkJournals(world.remote)).toEqual([journal]);
                expect(deletedOf(world.remote)).toEqual([]);
                expect(markersOf(world.remote)).toEqual([]);
                expect(receiver.checkpoint.knownIDs.has("h:part-1")).toBe(true);
            });

            it("acts on nothing when the storage does not show the stored record afterwards", async () => {
                const world = createWorld();
                const { receiver, journal } = await fileSentReceivedAndDeleted(world, { cleans: true });
                await synchroniseAfterInterval(world, receiver);
                const listRemote = receiver.core.listRemote.bind(receiver.core);
                let listings = 0;
                vi.spyOn(receiver.core, "listRemote").mockImplementation(async () => {
                    const listing = await listRemote();
                    return ++listings === 3 ? { ...listing, removalRecords: [] } : listing;
                });

                await synchroniseAfterInterval(world, receiver);

                expect(listings).toBe(3);
                expect(removalRecords(world.remote)).toHaveLength(1);
                expect(bulkJournals(world.remote)).toEqual([journal]);
                expect(receiver.checkpoint.knownIDs.has("h:part-1")).toBe(true);
            });

            it.each<[string, () => boolean]>([
                ["is refused", () => false],
                [
                    "fails",
                    () => {
                        throw new Error("synthetic failure");
                    },
                ],
            ])("removes a marker in a later round when its deletion %s", async (_case, fail) => {
                const world = createWorld();
                const { receiver } = await fileSentReceivedAndDeleted(world, { cleans: true });
                await synchroniseAfterInterval(world, receiver);
                const deleteFiles = world.remote.deleteFiles.getMockImplementation()!;
                let failures = 0;
                world.remote.deleteFiles.mockImplementation(async (keys) => {
                    if (isMarker(keys[0]) && failures++ === 0) return fail();
                    return await deleteFiles(keys);
                });

                await synchroniseAfterInterval(world, receiver);
                // The removal itself went through to its end, and the synchronisation did not fail.
                expect(receiver.statics.value.syncStatus).toBe("COMPLETED");
                expect(bulkJournals(world.remote)).toEqual([]);
                expect(knownContentOf(receiver)).toEqual([]);
                expect(markersOf(world.remote)).toHaveLength(1);

                await synchroniseAfterInterval(world, receiver);
                expect(markersOf(world.remote)).toEqual([]);
            });

            it("deletes only markers on the word of what it remembers", async () => {
                const world = createWorld();
                const sender = world.device("sender");
                const cleaner = world.device("cleaner", { cleans: true });
                await writeFile(sender, "kept", ["kept-1"]);
                await synchronise(sender);
                await synchronise(cleaner);
                const [journal] = bulkJournals(world.remote);
                const leftover = `${ASSESSMENT_MARKER_PREFIX}left-by-an-earlier-round`;
                world.remote.put(leftover, encoder.encode("marker"));
                // The list of markers holds the name of a journal, which does not belong there.
                const markersKey = `bucketsync-assessment-markers-${cleaner.core.hash}`;
                cleaner.entries.set(markersKey, [journal, leftover]);

                await synchroniseAfterInterval(world, cleaner);

                expect(world.remote.deleted).toEqual([leftover]);
                expect(bulkJournals(world.remote)).toEqual([journal]);
                expect(cleaner.entries.has(markersKey)).toBe(false);
            });

            it("does not take a marker for a journal", async () => {
                const world = createWorld();
                const { receiver } = await fileSentReceivedAndDeleted(world, { cleans: true });
                await synchroniseAfterInterval(world, receiver);
                const other = world.device("other");
                const listRemote = receiver.core.listRemote.bind(receiver.core);
                let listings = 0;
                vi.spyOn(receiver.core, "listRemote").mockImplementation(async () => {
                    // Another device synchronises while the marker is in the remote.
                    if (++listings === 2) await synchroniseBeside(other);
                    return await listRemote();
                });
                world.remote.transfers.length = 0;

                await synchroniseAfterInterval(world, receiver);

                expect(world.remote.transfers.filter((entry) => isMarker(entry.slice(9)))).toEqual([]);
                expect(other.statics.value.syncStatus).toBe("COMPLETED");
            });
        });

        it("removes exactly at the bound", async () => {
            const world = createWorld();
            const { receiver } = await fileSentReceivedAndDeleted(world, { cleans: true });
            await synchroniseAfterInterval(world, receiver);
            afterVerifyingListing(receiver, jumpOnce(world, REMOVAL_FRESHNESS_MS));

            await synchroniseAfterInterval(world, receiver);

            expect(bulkJournals(world.remote)).toEqual([]);
        });
    });
});

describe("JournalSyncCore removal records", () => {
    /** A removal by one device which another device has still to learn of. */
    async function removedByAnotherDevice(world: World) {
        const { sender, receiver, journal } = await fileSentReceivedAndDeleted(world, { cleans: true });
        await synchroniseAfterInterval(world, receiver);
        await synchroniseAfterInterval(world, receiver);
        const [recordKey] = removalRecords(world.remote);
        return { sender, cleaner: receiver, journal, recordKey };
    }

    it("takes the removed chunks out of what the remote is taken to hold, and marks the journals", async () => {
        const world = createWorld();
        const { sender, journal, recordKey } = await removedByAnotherDevice(world);
        expect(sender.checkpoint.sentIDs.has("h:part-1")).toBe(true);
        expect(sender.checkpoint.sentFiles.has(journal)).toBe(true);
        const generation = sender.checkpoint.resetGeneration;

        await synchronise(sender);

        expect(sender.checkpoint.sentIDs.has("h:part-1")).toBe(false);
        expect(sender.checkpoint.sentIDs.has("h:part-2")).toBe(false);
        // The journal no longer counts as one this device sent, and is never fetched.
        expect(sender.checkpoint.sentFiles.has(journal)).toBe(false);
        expect(sender.checkpoint.receivedFiles.has(journal)).toBe(true);
        expect(sender.checkpoint.receivedFiles.has(recordKey)).toBe(true);
        // Taking chunks out of the history is a reset to any transfer which started before.
        expect(sender.checkpoint.resetGeneration).toBe(generation + 1);

        // The record is read once.
        await synchronise(sender);
        expect(downloadsOf(world.remote, recordKey)).toHaveLength(1);
        expect(sender.checkpoint.resetGeneration).toBe(generation + 1);
    });

    it("receives the journals of the same cycle after the record took effect", async () => {
        const world = createWorld();
        const { sender, cleaner, recordKey } = await removedByAnotherDevice(world);
        await cleaner.db.put(note("later-note"));
        await synchronise(cleaner);
        const handedOnBefore = sender.handedOn.length;

        await synchronise(sender);

        expect(sender.checkpoint.receivedFiles.has(recordKey)).toBe(true);
        expect(sender.handedOn.slice(handedOnBefore).map((revision) => revision.split("@")[0])).toEqual(["later-note"]);
    });

    it("is read by a device without any history too, so that it never fetches a journal which is going", async () => {
        const world = createWorld();
        const { cleaner, journal, recordKey } = await removedByAnotherDevice(world);
        // The journal is still listed: the device which removes it was stopped before it deleted it.
        world.remote.put(journal, await journalOf(chunkLine("part-1", "L")), 100);
        void cleaner;
        const fresh = world.device("fresh");
        world.remote.transfers.length = 0;

        await synchronise(fresh);

        expect(downloadsOf(world.remote, recordKey)).toHaveLength(1);
        expect(downloadsOf(world.remote, journal)).toEqual([]);
        expect(fresh.checkpoint.receivedFiles.has(journal)).toBe(true);
        expect(fresh.checkpoint.knownIDs.has("h:part-1")).toBe(false);
        expect(fresh.core.hasDeferredRemoteContent()).toBe(false);
    });

    it("leaves nothing to wait for on a device which puts large chunks off", async () => {
        const world = createWorld();
        const { journal } = await removedByAnotherDevice(world);
        // The journal is still listed: the device which removes it was stopped before it deleted it.
        world.remote.put(journal, await journalOf(chunkLine("part-1", "L")), 100);
        const phone = world.device("phone", { mobile: true });
        phone.core.mayReceiveBulkJournals = () => false;

        await synchronise(phone);

        expect(phone.checkpoint.receivedFiles.has(journal)).toBe(true);
        expect(phone.core.hasDeferredRemoteContent()).toBe(false);
    });

    it.each<[string, (world: World, recordKey: string) => Promise<void> | void]>([
        ["cannot be downloaded", (world, recordKey) => void world.remote.unavailable.add(recordKey)],
        ["is listed but no longer stored", (world, recordKey) => void world.remote.removeButKeepListed(recordKey)],
        ["cannot be decoded", (world, recordKey) => world.remote.put(recordKey, encoder.encode("not a record"))],
        [
            "is of a version this device does not know",
            async (world, recordKey) => {
                const device = world.device("writer");
                await device.core.uploadPackedJson(recordKey, {
                    version: 2,
                    at: 1,
                    assessmentStoredAt: 1,
                    chunkIDs: [],
                    journals: [],
                });
            },
        ],
    ])("fails the receive before anything is sent when a record %s", async (_case, breakRecord) => {
        const world = createWorld();
        const { sender, recordKey } = await removedByAnotherDevice(world);
        const intact = world.remote.dataOf(recordKey);
        await breakRecord(world, recordKey);
        await writeFileFromChunksHere(sender, "recording-again", ["part-1", "part-2"]);
        const uploadsBefore = world.remote.upload.mock.calls.length;

        await expect(sender.core.sync()).resolves.toBe(false);

        expect(world.remote.upload.mock.calls.length).toBe(uploadsBefore);
        expect(sender.statics.value.syncStatus).toBe("ERRORED");
        expect(sender.checkpoint.receivedFiles.has(recordKey)).toBe(false);
        await expect(sender.core.receiveRemoteJournal()).resolves.toBe(false);

        // Once the record can be read, the file is sent with its content.
        world.remote.unavailable.clear();
        world.remote.put(recordKey, intact);
        await synchronise(sender);
        expect(bulkJournals(world.remote)).toHaveLength(1);
    });

    it("tells the user once when a record cannot be read in two synchronisations in a row", async () => {
        const world = createWorld();
        const { sender, recordKey } = await removedByAnotherDevice(world);
        world.remote.unavailable.add(recordKey);
        const notices = noticesFromNowOn();
        const aboutRecord = () => notices.filter((message) => message.includes("cannot be read"));

        await expect(sender.core.sync()).resolves.toBe(false);
        expect(aboutRecord()).toEqual([]);
        await expect(sender.core.sync()).resolves.toBe(false);
        expect(aboutRecord()).toHaveLength(1);

        for (let cycle = 0; cycle < 3; cycle++) await expect(sender.core.sync()).resolves.toBe(false);
        expect(aboutRecord()).toHaveLength(1);
    });

    it("does not tell the user of two failures with a synchronisation between them which read every record", async () => {
        const world = createWorld();
        const { sender, recordKey } = await removedByAnotherDevice(world);
        const intact = world.remote.dataOf(recordKey);
        world.remote.unavailable.add(recordKey);
        const notices = noticesFromNowOn();

        await expect(sender.core.sync()).resolves.toBe(false);
        // The record is gone from the remote for a while, so there is none to read.
        await world.remote.storage.deleteFiles([recordKey]);
        await synchronise(sender);
        world.remote.put(recordKey, intact);
        await expect(sender.core.sync()).resolves.toBe(false);

        expect(notices.filter((message) => message.includes("cannot be read"))).toEqual([]);
    });

    describe("which reached the remote after the removal was verified", () => {
        /**
         * The removing device is suspended while it stores its record, which the store receives `late` after the
         * assessment marker of that removal.
         */
        async function recordStoredLate(world: World, late: number, cleanerOptions: DeviceOptions = {}) {
            const {
                sender,
                receiver: cleaner,
                journal,
            } = await fileSentReceivedAndDeleted(world, {
                ...cleanerOptions,
                cleans: true,
            });
            await synchroniseAfterInterval(world, cleaner);
            const storeUpload = world.remote.uploadBeside.getMockImplementation()!;
            world.remote.uploadBeside.mockImplementation(async (key, ...rest) => {
                if (key.startsWith(REMOVAL_RECORD_PREFIX)) world.clock.now += late;
                return await storeUpload(key, ...rest);
            });
            await synchroniseAfterInterval(world, cleaner);
            world.remote.uploadBeside.mockImplementation(storeUpload);
            const [recordKey] = removalRecords(world.remote);
            world.remote.transfers.length = 0;
            return { sender, cleaner, journal, recordKey };
        }

        it.each([
            ["hours", 5 * 60 * MINUTE, 0],
            ["hours, by a device whose clock is half an hour behind,", 5 * 60 * MINUTE, -30 * MINUTE],
            ["hours, by a device whose clock is hours ahead,", 5 * 60 * MINUTE, 6 * 60 * MINUTE],
        ])("is acted on by nobody when it is stored %s late", async (_case, late, clockOffset) => {
            const world = createWorld();
            const { sender, cleaner, journal, recordKey } = await recordStoredLate(world, late, { clockOffset });
            const fresh = world.device("fresh");

            // The device which stored it leaves it where it is, deletes nothing, and does not act on it.
            expect(removalRecords(world.remote)).toEqual([recordKey]);
            expect(bulkJournals(world.remote)).toEqual([journal]);
            expect(deletedOf(world.remote)).toEqual([]);
            expect(cleaner.checkpoint.knownIDs.has("h:part-1")).toBe(true);
            expect(cleaner.checkpoint.receivedFiles.has(recordKey)).toBe(false);
            // When it meets its record in a listing, it ignores it like every other device. It is kept from
            // starting another removal here.
            cleaner.host.settings.removeUnreferencedJournalData = false;
            await synchronise(cleaner);
            expect(cleaner.checkpoint.receivedFiles.has(recordKey)).toBe(true);
            expect(cleaner.checkpoint.knownIDs.has("h:part-1")).toBe(true);
            expect(cleaner.checkpoint.receivedFiles.has(journal)).toBe(true);
            world.remote.transfers.length = 0;

            await synchronise(fresh);

            // The record is read, and marked as read, and changes nothing else.
            expect(downloadsOf(world.remote, recordKey)).toHaveLength(1);
            expect(fresh.checkpoint.receivedFiles.has(recordKey)).toBe(true);
            expect(downloadsOf(world.remote, journal)).toHaveLength(1);
            expect(await chunkData(fresh, "part-1")).toBe(await chunkData(sender, "part-1"));
            expect(fresh.checkpoint.knownIDs.has("h:part-1")).toBe(true);

            // The device which sent the journal goes on counting it, and its chunks, as held by the remote.
            const generation = sender.checkpoint.resetGeneration;
            await synchronise(sender);
            expect(sender.checkpoint.receivedFiles.has(recordKey)).toBe(true);
            expect(sender.checkpoint.sentFiles.has(journal)).toBe(true);
            expect(sender.checkpoint.sentIDs.has("h:part-1")).toBe(true);
            expect(sender.checkpoint.resetGeneration).toBe(generation);

            // It is read once by each device.
            await synchronise(fresh);
            await synchronise(sender);
            expect(downloadsOf(world.remote, recordKey)).toHaveLength(2);
        });

        it.each([
            ["is acted on by every device when the store received it a minute after the marker", 0, true],
            ["is acted on by no device when the store received it a millisecond later than that", 1, false],
        ])("%s", async (_case, beyondBound, actedOn) => {
            const world = createWorld();
            const { cleaner, journal, recordKey } = await recordStoredLate(world, REMOVAL_FRESHNESS_MS + beyondBound);
            const fresh = world.device("fresh");

            // The device which stored it.
            expect(cleaner.checkpoint.receivedFiles.has(recordKey)).toBe(actedOn);
            expect(cleaner.checkpoint.knownIDs.has("h:part-1")).toBe(!actedOn);
            expect(bulkJournals(world.remote)).toEqual(actedOn ? [] : [journal]);

            // A device which reads it.
            await synchronise(fresh);
            expect(fresh.checkpoint.receivedFiles.has(recordKey)).toBe(true);
            expect(fresh.checkpoint.knownIDs.has("h:part-1")).toBe(!actedOn);
            expect(downloadsOf(world.remote, journal)).toHaveLength(actedOn ? 0 : 1);
        });

        it("is not acted on when the store received it before the marker it names", async () => {
            const world = createWorld();
            const { journal, recordKey } = await recordStoredLate(world, 0);
            // A record which names a time after its own cannot come from a removal which was verified first.
            const writer = world.device("writer");
            const forged = `${REMOVAL_RECORD_PREFIX}${"0".repeat(64)}.json.gz`;
            world.remote.put(journal, await journalOf(chunkLine("part-1", "L")), 100);
            await writer.core.uploadPackedJson(forged, {
                ...(await readRecord(writer, recordKey)),
                assessmentStoredAt: world.clock.now + 1,
            });
            await world.remote.storage.deleteFiles([recordKey]);
            const fresh = world.device("fresh");

            await synchronise(fresh);

            expect(fresh.checkpoint.receivedFiles.has(forged)).toBe(true);
            expect(fresh.checkpoint.knownIDs.has("h:part-1")).toBe(true);
        });

        it.each([
            ["half an hour behind", -30 * MINUTE],
            ["hours ahead", 6 * 60 * MINUTE],
        ])(
            "is acted on by every device when the clock of the device which removes is %s",
            async (_case, clockOffset) => {
                const world = createWorld();
                const {
                    sender,
                    receiver: cleaner,
                    journal,
                } = await fileSentReceivedAndDeleted(world, {
                    cleans: true,
                    clockOffset,
                });
                const fresh = world.device("fresh");
                await synchronise(fresh);
                await synchronise(fresh);
                await synchroniseAfterInterval(world, cleaner);

                await synchroniseAfterInterval(world, cleaner);

                // The removal went through, by the times of the store alone.
                expect(bulkJournals(world.remote)).toEqual([]);
                const [recordKey] = removalRecords(world.remote);
                await expect(readRecord(cleaner, recordKey)).resolves.toMatchObject({
                    at: world.clock.now + clockOffset,
                    assessmentStoredAt: world.clock.now,
                });
                expect(world.remote.storedAtOf(recordKey)).toBe(world.clock.now);
                // Every other device acts on the record.
                for (const reader of [sender, fresh]) {
                    await synchronise(reader);
                    expect(reader.checkpoint.receivedFiles.has(recordKey)).toBe(true);
                    expect(reader.checkpoint.receivedFiles.has(journal)).toBe(true);
                }
                expect(sender.checkpoint.sentIDs.has("h:part-1")).toBe(false);
                expect(fresh.checkpoint.knownIDs.has("h:part-1")).toBe(false);
            }
        );

        it("keeps the time at which the store first received it when its upload is tried again", async () => {
            const world = createWorld();
            const { receiver: cleaner } = await fileSentReceivedAndDeleted(world, { cleans: true });
            await synchroniseAfterInterval(world, cleaner);
            const storeUpload = world.remote.uploadBeside.getMockImplementation()!;
            let firstStoredAt = 0;
            world.remote.uploadBeside.mockImplementation(async (key, ...rest) => {
                if (!key.startsWith(REMOVAL_RECORD_PREFIX)) return await storeUpload(key, ...rest);
                // The answer to the first attempt is lost, so the upload is tried again half a minute later.
                await storeUpload(key, ...rest);
                firstStoredAt = world.clock.now;
                world.clock.now += 30_000;
                return await storeUpload(key, ...rest);
            });

            await synchroniseAfterInterval(world, cleaner);

            const [recordKey] = removalRecords(world.remote);
            expect(world.remote.storedAtOf(recordKey)).toBe(firstStoredAt);
            expect(bulkJournals(world.remote)).toEqual([]);
        });

        it("is acted on, however late, with storage which does not report when an object was stored", async () => {
            const world = createWorld();
            const { journal, recordKey } = await recordStoredLate(world, 5 * 60 * MINUTE);
            world.remote.storage.listObjectsInUploadOrder.mockImplementation(async () =>
                world.remote.groups().map((group) => group.map(({ key, size }) => ({ key, size })))
            );
            const fresh = world.device("fresh");

            await synchronise(fresh);

            expect(fresh.checkpoint.receivedFiles.has(recordKey)).toBe(true);
            expect(downloadsOf(world.remote, journal)).toEqual([]);
            expect(fresh.checkpoint.knownIDs.has("h:part-1")).toBe(false);
        });
    });

    it("stops when the history is reset while the records are read", async () => {
        const world = createWorld();
        const { sender, recordKey } = await removedByAnotherDevice(world);
        const download = world.remote.downloadWithResult.getMockImplementation()!;
        world.remote.downloadWithResult.mockImplementation(async (key) => {
            if (key === recordKey) await sender.createPaneClient().resetCheckpointInfo();
            return await download(key);
        });

        await expect(sender.core.sync()).resolves.toBe(false);

        expect(sender.checkpoint.receivedFiles.has(recordKey)).toBe(false);
        world.remote.downloadWithResult.mockImplementation(download);
        await synchronise(sender);
        expect(sender.checkpoint.receivedFiles.has(recordKey)).toBe(true);
    });

    it("keeps a device which read the record from fetching a journal which outlived it (the removal was interrupted)", async () => {
        const world = createWorld();
        const { sender, receiver, journal } = await fileSentReceivedAndDeleted(world, { cleans: true });
        const other = world.device("other");
        await synchronise(other);
        await synchronise(other);
        await synchroniseAfterInterval(world, receiver);
        // The removing device is frozen in its request to delete. Meanwhile another device synchronises: it reads
        // the record while the journal is still there.
        const deleteFiles = world.remote.deleteFiles.getMockImplementation()!;
        world.remote.deleteFiles.mockImplementationOnce(async (keys) => {
            world.remote.transfers.length = 0;
            await synchroniseBeside(other);
            return await deleteFiles(keys);
        });

        await synchroniseAfterInterval(world, receiver);

        expect(bulkJournals(world.remote)).toEqual([]);
        // The other device did not fetch the journal, and does not take the remote to hold its chunks.
        expect(downloadsOf(world.remote, journal)).toEqual([]);
        expect(other.checkpoint.receivedFiles.has(journal)).toBe(true);
        expect(other.checkpoint.knownIDs.has("h:part-1")).toBe(false);

        // So when it uses the content again, it sends it, and a third device can read the file.
        await writeFileFromChunksHere(other, "recording-again", ["part-1", "part-2"]);
        await synchronise(other);
        const third = world.device("third");
        await synchronise(third);
        expect(await chunkData(third, "part-1")).toBe(await chunkData(sender, "part-1"));
        expect(await chunkData(third, "part-2")).toBe(await chunkData(sender, "part-2"));
    });

    it("gives a second removal of the same journals a record of its own, which every device reads", async () => {
        const world = createWorld();
        const { sender, receiver, journal } = await fileSentReceivedAndDeleted(world, { cleans: true });
        await synchroniseAfterInterval(world, receiver);
        // The first attempt stores its record and deletes nothing.
        world.remote.deleteFiles.mockImplementationOnce(async () => false);
        await synchroniseAfterInterval(world, receiver);
        const [firstRecord] = removalRecords(world.remote);
        await synchronise(sender);
        expect(sender.checkpoint.receivedFiles.has(firstRecord)).toBe(true);

        await synchroniseAfterInterval(world, receiver);

        expect(bulkJournals(world.remote)).toEqual([]);
        const secondRecord = removalRecords(world.remote).find((key) => key !== firstRecord)!;
        expect(secondRecord).toBeDefined();
        const [first, second] = [await readRecord(sender, firstRecord), await readRecord(sender, secondRecord)];
        expect({ ...second, at: first.at, assessmentStoredAt: first.assessmentStoredAt }).toEqual(first);
        expect(second.at).toBeGreaterThan(first.at);
        expect(second.assessmentStoredAt).toBeGreaterThan(first.assessmentStoredAt);
        expect(second.journals).toEqual([journal]);
        // The device which read the first record reads the second as well.
        world.remote.transfers.length = 0;
        await synchronise(sender);
        expect(downloadsOf(world.remote, secondRecord)).toHaveLength(1);
        expect(sender.checkpoint.receivedFiles.has(secondRecord)).toBe(true);
    });
});

describe("JournalSyncCore chunks which the remote no longer holds", () => {
    it("sends the chunks of a document again before the document, so that a third device reads the right content", async () => {
        const world = createWorld();
        const { sender, receiver } = await fileSentReceivedAndDeleted(world, { cleans: true });
        await synchroniseAfterInterval(world, receiver);
        await synchroniseAfterInterval(world, receiver);
        expect(bulkJournals(world.remote)).toEqual([]);

        // The same content is used again on a device which holds it. Its chunks get no new sequence.
        await writeFileFromChunksHere(sender, "recording-again", ["part-1", "part-2"]);
        world.remote.transfers.length = 0;
        await synchronise(sender);

        const [journal] = bulkJournals(world.remote);
        await expect(entriesOf(world.remote, journal)).resolves.toEqual({
            chunks: ["h:part-1", "h:part-2"],
            documents: [],
        });
        const uploads = world.remote.transfers.filter((entry) => entry.startsWith("upload "));
        expect(uploads.map((entry) => entry.replace(/[a-f0-9]{64}/u, "<id>"))).toEqual([
            "upload <id>-chunks.jsonl.gz",
            "upload _index-<id>-chunks.jsonl.gz",
            "upload <id>-docs.jsonl.gz",
        ]);
        const third = world.device("third");
        await synchronise(third);
        expect(third.handedOn.at(-1)).toMatch(/^recording-again@/u);
        expect(await chunkData(third, "part-1")).toBe(await chunkData(sender, "part-1"));
        expect(await chunkData(third, "part-2")).toBe(await chunkData(sender, "part-2"));
        // Nothing was relied on, so nothing is remembered.
        expect(relianceOf(sender).documents).toEqual([]);
    });

    it("sends a chunk once which several documents of one send use, and none which the remote holds", async () => {
        const world = createWorld();
        const sender = world.device("sender");
        await writeFile(sender, "first", ["shared", "first-1"]);
        await synchronise(sender);
        // The second file uses a chunk of the first, and two chunks which were never sent.
        await sender.db.bulkDocs([largeChunk("second-1"), largeChunk("second-2")]);
        await synchronise(sender);
        const before = bulkJournals(world.remote).length;
        sender.entries.set(`bucketsync-checkpoint-${sender.core.hash}`, {
            ...sender.checkpoint,
            sentIDs: new Set([...sender.checkpoint.sentIDs].filter((id) => !id.includes("second"))),
        } satisfies CheckPointInfo);
        await sender.db.bulkDocs([
            { ...note("second", ["shared", "second-1", "second-2"]), size: LARGE_FILE_BYTES },
            { ...note("third", ["second-1", "second-2"]), size: LARGE_FILE_BYTES },
        ]);

        await synchronise(sender);

        const [journal] = bulkJournals(world.remote).slice(before);
        expect(bulkJournals(world.remote)).toHaveLength(before + 1);
        await expect(entriesOf(world.remote, journal)).resolves.toEqual({
            chunks: ["h:second-1", "h:second-2"],
            documents: [],
        });
    });

    it("sends a chunk once which follows its document among the changes", async () => {
        const world = createWorld();
        const sender = world.device("sender");
        // The document was stored before its chunk, so the chunk comes after it among the changes.
        await sender.db.put({ ...note("recording", ["part-1"]), size: LARGE_FILE_BYTES });
        await sender.db.put(largeChunk("part-1"));

        await synchronise(sender);

        const [journal] = bulkJournals(world.remote);
        expect(bulkJournals(world.remote)).toHaveLength(1);
        await expect(entriesOf(world.remote, journal)).resolves.toEqual({ chunks: ["h:part-1"], documents: [] });
        expect(sender.checkpoint.lastLocalSeq).toBe((await sender.db.info()).update_seq);
    });

    it("does not move the position past a document whose chunks filled a journal before it", async () => {
        const world = createWorld();
        const sender = world.device("sender");
        // Three chunks of about 3.5 MB which the local database holds and the remote does not.
        const payload = "x".repeat(3_500_000);
        const names = ["binary-1", "binary-2", "binary-3"];
        await sender.db.bulkDocs(names.map((name) => ({ ...largeChunk(name), data: `${name}${payload}` })));
        await synchronise(sender);
        await world.remote.storage.deleteFiles(world.remote.objectKeys());
        sender.entries.set(`bucketsync-checkpoint-${sender.core.hash}`, {
            ...sender.checkpoint,
            sentIDs: new Set<string>(),
        } satisfies CheckPointInfo);
        const position = sender.checkpoint.lastLocalSeq;
        // The document is the only change, and so the last one of its pack. Its chunks fill a journal before it.
        await sender.db.put({ ...note("recording", names), size: LARGE_FILE_BYTES });
        const storeUpload = world.remote.upload.getMockImplementation()!;
        world.remote.upload.mockImplementationOnce(storeUpload).mockImplementationOnce(async () => false);

        await expect(sender.core.sync()).resolves.toBe(false);

        // The chunks are stored and the document is not, so the position has not passed the document.
        expect(bulkJournals(world.remote)).toHaveLength(1);
        expect(sender.checkpoint.lastLocalSeq).toBe(position);

        await synchronise(sender);
        const documents = world.remote.keys().filter((key) => !isBulkKey(key));
        expect(documents).toHaveLength(1);
        await expect(entriesOf(world.remote, documents[0])).resolves.toEqual({ chunks: [], documents: ["recording"] });
    });

    describe("when the chunks to read from the local database are many or large", () => {
        /** Make the local database hold chunks which the remote lacks and no send finds among the changes. */
        async function chunksOnlyHere(world: World, device: Device, chunks: EntryLeaf[]) {
            await device.db.bulkDocs(chunks);
            await synchronise(device);
            await world.remote.storage.deleteFiles(world.remote.objectKeys());
            setCheckpoint(device, { sentIDs: new Set<string>(), sentFiles: new Set<string>() });
        }
        /** The reads of chunks by ID from here on: how many each asked for, and how much content it returned. */
        function chunkReadsOf(device: Device, onRead: () => void = () => {}) {
            const reads: { count: number; bytes: number }[] = [];
            type Page = { rows: { doc?: { data?: string } }[] };
            const allDocs = device.db.allDocs.bind(device.db) as (options: unknown) => Promise<Page>;
            vi.spyOn(device.db, "allDocs").mockImplementation((async (options: unknown) => {
                const page = await allDocs(options);
                const keys = (options as { keys?: string[] } | undefined)?.keys;
                if (keys) {
                    reads.push({
                        count: keys.length,
                        bytes: page.rows.reduce((sum, row) => sum + (row.doc?.data?.length ?? 0), 0),
                    });
                    onRead();
                }
                return page;
            }) as never);
            return reads;
        }
        const total = (reads: { count: number }[]) => reads.reduce((sum, read) => sum + read.count, 0);
        const largest = (reads: { count: number }[]) => Math.max(...reads.map((read) => read.count));
        const MANY = 3_000;
        const manyNames = Array.from({ length: MANY }, (_, index) => `piece-${index}`);
        const manyChunks = () => manyNames.map((name) => chunk(name, `content of ${name}`));

        it("reads the chunks of a document fifty at a time, and sends all of them before the document", async () => {
            const world = createWorld();
            const sender = world.device("sender");
            await chunksOnlyHere(world, sender, manyChunks());
            // Two files use the same chunks, which are read once.
            await sender.db.bulkDocs([
                { ...note("recording", manyNames), size: LARGE_FILE_BYTES },
                { ...note("copy", manyNames), size: LARGE_FILE_BYTES },
            ]);
            const reads = chunkReadsOf(sender);

            await expect(sender.core.sendLocalJournal()).resolves.toBe(true);

            expect(reads[0].count).toBe(1);
            expect(largest(reads)).toBe(LOCAL_CHUNK_READ_MAX_COUNT);
            expect(total(reads)).toBe(MANY);
            // A journal takes as many entries as in any other send, so the chunks go into several.
            const journals = world.remote.keys();
            expect(journals.length).toBeGreaterThan(MANY / 251);
            for (const key of journals) {
                const { chunks, documents } = await entriesOf(world.remote, key);
                expect(chunks.length + documents.length).toBeLessThanOrEqual(251);
            }
            const entries = await storedEntries(world.remote);
            expect(entries.size).toBe(MANY + 2);
            expect([...entries.values()].every((journalCount) => journalCount === 1)).toBe(true);
            expect((await entriesOf(world.remote, journals.at(-1)!)).documents).toEqual(["recording", "copy"]);
            expect(sender.checkpoint.lastLocalSeq).toBe((await sender.db.info()).update_seq);
        });

        it("reads fewer at a time when they are large, so that no read brings more than four mebibytes", async () => {
            const world = createWorld();
            const sender = world.device("sender");
            // A large chunk, then small ones, then more large ones: the largest one read so far sets the bound.
            const isSmall = (index: number) => index >= 1 && index <= 6;
            const names = Array.from({ length: 27 }, (_, index) => `mixed-piece-${index}`);
            await chunksOnlyHere(
                world,
                sender,
                names.map((name, index) => largeChunk(name, isSmall(index) ? 100 : 600_000))
            );
            await sender.db.put({ ...note("recording", names), size: LARGE_FILE_BYTES });
            const reads = chunkReadsOf(sender);

            await expect(sender.core.sendLocalJournal()).resolves.toBe(true);

            expect(reads[0].count).toBe(1);
            expect(largest(reads)).toBe(Math.floor(LOCAL_CHUNK_READ_MAX_BYTES / 600_000));
            for (const read of reads) expect(read.bytes).toBeLessThanOrEqual(LOCAL_CHUNK_READ_MAX_BYTES);
            expect(total(reads)).toBe(names.length);
            expect((await storedEntries(world.remote)).size).toBe(names.length + 1);
        });

        it("does not read chunks which are among the changes before their document", async () => {
            const world = createWorld();
            const sender = world.device("sender");
            await writeFile(sender, "recording", ["part-1", "part-2"]);
            const reads = chunkReadsOf(sender);

            await expect(sender.core.sendLocalJournal()).resolves.toBe(true);

            expect(reads).toEqual([]);
            expect((await storedEntries(world.remote)).size).toBe(3);
        });

        it("reads the chunks which it must send again in the same way, and empties the queue", async () => {
            const world = createWorld();
            const sender = world.device("sender");
            await chunksOnlyHere(world, sender, manyChunks());
            sender.entries.set(`bucketsync-reliance-${sender.core.hash}`, {
                documents: [],
                resend: manyNames.map(chunkId),
            } satisfies JournalReliance);
            const reads = chunkReadsOf(sender);

            await expect(sender.core.sendLocalJournal()).resolves.toBe(true);

            expect(largest(reads)).toBe(LOCAL_CHUNK_READ_MAX_COUNT);
            expect(total(reads)).toBe(MANY);
            expect((await storedEntries(world.remote)).size).toBe(MANY);
            expect(relianceOf(sender).resend).toEqual([]);
        });

        it("reads a chunk once which it must send again and which a new document uses as well", async () => {
            const world = createWorld();
            const sender = world.device("sender");
            await chunksOnlyHere(world, sender, [chunk("piece", "content of a piece")]);
            sender.entries.set(`bucketsync-reliance-${sender.core.hash}`, {
                documents: [],
                resend: [chunkId("piece")],
            } satisfies JournalReliance);
            await sender.db.put({ ...note("recording", ["piece"]), size: LARGE_FILE_BYTES });
            const reads = chunkReadsOf(sender);

            await expect(sender.core.sendLocalJournal()).resolves.toBe(true);

            expect(total(reads)).toBe(1);
            expect([...(await storedEntries(world.remote)).entries()].sort()).toEqual([
                ["h:piece", 1],
                ["recording", 1],
            ]);
        });

        it("does not pass a document when a stop is requested while its chunks are read", async () => {
            const world = createWorld();
            const sender = world.device("sender");
            const names = manyNames.slice(0, 120);
            await chunksOnlyHere(world, sender, manyChunks().slice(0, 120));
            await sender.db.put({ ...note("recording", names), size: LARGE_FILE_BYTES });
            const position = sender.checkpoint.lastLocalSeq;
            let stopped = false;
            chunkReadsOf(sender, () => {
                if (stopped) return;
                stopped = true;
                sender.core.requestStop();
            });

            await expect(sender.core.sendLocalJournal()).resolves.toBe(true);

            // What was read is sent, the document is not, and the position has not moved past it.
            expect((await storedEntries(world.remote)).has("recording")).toBe(false);
            expect(sender.checkpoint.lastLocalSeq).toBe(position);

            await synchronise(sender);
            const entries = await storedEntries(world.remote);
            expect(entries.size).toBe(names.length + 1);
            expect([...entries.values()].every((journalCount) => journalCount === 1)).toBe(true);
        });
    });

    it("does not send chunks again which it received and the remote still holds", async () => {
        const world = createWorld();
        const sender = world.device("sender");
        const receiver = world.device("receiver");
        await writeFile(sender, "recording", ["part-1", "part-2"]);
        await synchronise(sender);
        await synchronise(receiver);
        await synchronise(receiver);

        await writeFileFromChunksHere(receiver, "copy", ["part-1", "part-2"]);
        await synchronise(receiver);

        expect(bulkJournals(world.remote)).toHaveLength(1);
        // The copy relies on the remote for its chunks, which is remembered for a while.
        expect(relianceOf(receiver).documents.map((doc) => doc.id)).toEqual(["copy"]);
    });

    it("sends a document as before when one of its chunks is not in the local database, and logs it", async () => {
        const world = createWorld();
        const sender = world.device("sender");
        const logged: string[] = [];
        setGlobalLogFunction((message, level) => {
            if (level === LOG_LEVEL_VERBOSE) logged.push(String(message));
        });
        await sender.db.put({ ...note("incomplete", ["missing-here"]), size: LARGE_FILE_BYTES });

        await synchronise(sender);

        const [journal] = world.remote.keys();
        await expect(entriesOf(world.remote, journal)).resolves.toEqual({ chunks: [], documents: ["incomplete"] });
        expect(logged.filter((message) => message.includes("h:missing-here"))).toHaveLength(1);
    });

    it("brings no chunks along for a deleted document", async () => {
        const world = createWorld();
        const sender = world.device("sender");
        await sender.db.bulkDocs([largeChunk("part-1")]);
        await synchronise(sender);
        sender.entries.set(`bucketsync-checkpoint-${sender.core.hash}`, {
            ...sender.checkpoint,
            sentIDs: new Set<string>(),
        } satisfies CheckPointInfo);
        await sender.db.put({ ...note("deleted", ["part-1"]), deleted: true });

        await synchronise(sender);

        expect(bulkJournals(world.remote)).toHaveLength(1);
        expect(relianceOf(sender).documents).toEqual([]);
    });

    describe("when a document is sent while another device removes its chunks", () => {
        /**
         * The sender puts a file back whose chunks it takes the remote to hold, and sends it while the cleaner is
         * between its last look at the remote and the removal. The cleaner cannot know of the file.
         */
        async function sendWhileRemoving(world: World, sendAfter = 0) {
            const { sender, receiver: cleaner } = await fileSentReceivedAndDeleted(world, { cleans: true });
            await synchroniseAfterInterval(world, cleaner);
            await writeFileFromChunksHere(sender, "recording-again", ["part-1", "part-2"]);
            const listRemote = cleaner.core.listRemote.bind(cleaner.core);
            let listings = 0;
            let sentAt = 0;
            vi.spyOn(cleaner.core, "listRemote").mockImplementation(async () => {
                const listing = await listRemote();
                if (++listings === 2) {
                    world.clock.now += sendAfter;
                    sentAt = world.clock.now;
                    await synchroniseBeside(sender);
                }
                return listing;
            });
            await synchroniseAfterInterval(world, cleaner);
            vi.mocked(cleaner.core.listRemote).mockRestore();
            return { sender, cleaner, sentAt: () => sentAt };
        }

        it("remembers the document it sent in reliance on the remote", async () => {
            const world = createWorld();
            const { sender, sentAt } = await sendWhileRemoving(world);

            expect(bulkJournals(world.remote)).toEqual([]);
            const revision = (await sender.db.get("recording-again"))._rev;
            expect(relianceOf(sender)).toEqual({
                documents: [{ id: "recording-again", rev: revision, sentAt: sentAt() }],
                resend: [],
            });
        });

        it.each([
            ["at once", 0],
            ["when it was sent just inside the bound of the removal", REMOVAL_FRESHNESS_MS],
        ])(
            "sends the chunks again in its next synchronisation %s, without any local change",
            async (_case, sendAfter) => {
                const world = createWorld();
                const { sender } = await sendWhileRemoving(world, sendAfter);
                await expect(sender.core.hasUnsentLocalChanges()).resolves.toBe(false);

                // Reading the record shows that the chunks are gone, and that the document still needs them.
                await expect(sender.core.receiveRemoteJournal()).resolves.toBe(true);
                expect(relianceOf(sender).resend).toEqual(["h:part-1", "h:part-2"]);
                await expect(sender.core.hasUnsentLocalChanges()).resolves.toBe(true);

                await expect(sender.core.sendLocalJournal()).resolves.toBe(true);

                const [journal] = bulkJournals(world.remote);
                await expect(entriesOf(world.remote, journal)).resolves.toEqual({
                    chunks: ["h:part-1", "h:part-2"],
                    documents: [],
                });
                expect(indexObjects(world.remote)).toEqual([`_index-${journal}`]);
                expect(relianceOf(sender).resend).toEqual([]);
                await expect(sender.core.hasUnsentLocalChanges()).resolves.toBe(false);
                const third = world.device("third");
                await synchronise(third);
                expect(await chunkData(third, "part-1")).toBe(await chunkData(sender, "part-1"));
                expect(await chunkData(third, "part-2")).toBe(await chunkData(sender, "part-2"));
            }
        );

        it("sends only the removed chunks again, not the others which the document uses", async () => {
            const world = createWorld();
            const sender = world.device("sender");
            const cleaner = world.device("cleaner", { cleans: true });
            await writeFile(sender, "recording", ["part-1"]);
            await synchronise(sender);
            await writeFile(sender, "kept", ["kept-1"]);
            await synchronise(sender);
            await synchronise(cleaner);
            await deleteFile(sender, "recording");
            await synchronise(sender);
            await synchroniseAfterInterval(world, cleaner);
            // The new file uses a chunk which is about to be removed and one which stays.
            await writeFileFromChunksHere(sender, "mixed", ["part-1", "kept-1"]);
            const listRemote = cleaner.core.listRemote.bind(cleaner.core);
            let listings = 0;
            vi.spyOn(cleaner.core, "listRemote").mockImplementation(async () => {
                const listing = await listRemote();
                if (++listings === 2) await synchroniseBeside(sender);
                return listing;
            });
            await synchroniseAfterInterval(world, cleaner);
            expect(bulkJournals(world.remote)).toHaveLength(1);

            await expect(sender.core.receiveRemoteJournal()).resolves.toBe(true);

            expect(relianceOf(sender).resend).toEqual(["h:part-1"]);
        });

        it("still sends them again when it synchronises hours later", async () => {
            const world = createWorld();
            const { sender } = await sendWhileRemoving(world);

            world.clock.now += 5 * RELIANCE_MEMORY_MS;
            await synchronise(sender);

            expect(bulkJournals(world.remote)).toHaveLength(1);
            // Having read every record of that time, it no longer remembers the document.
            expect(relianceOf(sender).documents).toEqual([]);
        });

        it("keeps the chunks queued when the send fails, and sends them the next time", async () => {
            const world = createWorld();
            const { sender } = await sendWhileRemoving(world);
            const storeUpload = world.remote.upload.getMockImplementation()!;
            world.remote.upload.mockImplementationOnce(async () => false);

            await expect(sender.core.sync()).resolves.toBe(false);
            expect(relianceOf(sender).resend).toEqual(["h:part-1", "h:part-2"]);

            world.remote.upload.mockImplementation(storeUpload);
            await synchronise(sender);
            expect(relianceOf(sender).resend).toEqual([]);
            expect(bulkJournals(world.remote)).toHaveLength(1);
        });

        it.each<[string, (sender: Device) => Promise<unknown>]>([
            ["was deleted since", async (sender) => await deleteFile(sender, "recording-again")],
            [
                "was replaced by a revision which uses other chunks",
                async (sender) => {
                    const doc = (await sender.db.get("recording-again")) as PlainEntry;
                    await sender.db.put({ ...doc, children: [], mtime: 3 });
                },
            ],
        ])("sends nothing again for a document which %s", async (_case, change) => {
            const world = createWorld();
            const { sender } = await sendWhileRemoving(world);
            await change(sender);

            await synchronise(sender);

            expect(bulkJournals(world.remote)).toEqual([]);
            expect(relianceOf(sender).resend).toEqual([]);
        });

        it("sends them again for a revision which is now one side of a conflict", async () => {
            const world = createWorld();
            const { sender } = await sendWhileRemoving(world);
            const doc = (await sender.db.get("recording-again")) as PlainEntry;
            // Another revision was made on the same base elsewhere, and wins.
            await sender.db.bulkDocs(
                [{ ...doc, children: [], _rev: "1-zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz", mtime: 4 }] as EntryDoc[],
                { new_edits: false }
            );
            expect((await sender.db.get("recording-again", { conflicts: true }))._conflicts).toEqual([doc._rev]);

            await synchronise(sender);

            expect(bulkJournals(world.remote)).toHaveLength(1);
        });
    });

    describe("when a remembered document cannot be looked up as a removal record is read", () => {
        async function removalWithRememberedDocument(world: World) {
            const { sender, receiver } = await fileSentReceivedAndDeleted(world, { cleans: true });
            await synchroniseAfterInterval(world, receiver);
            await synchroniseAfterInterval(world, receiver);
            sender.entries.set(`bucketsync-reliance-${sender.core.hash}`, {
                documents: [{ id: "recording-again", rev: "1-abc", sentAt: world.clock.now }],
                resend: [],
            } satisfies JournalReliance);
            return sender;
        }

        it("sends nothing again for a document which is gone", async () => {
            const world = createWorld();
            const sender = await removalWithRememberedDocument(world);

            await synchronise(sender);

            expect(relianceOf(sender).resend).toEqual([]);
            expect(bulkJournals(world.remote)).toEqual([]);
        });

        it("fails the receive when the local database cannot be read, and does not mark the record as read", async () => {
            const world = createWorld();
            const sender = await removalWithRememberedDocument(world);
            const [recordKey] = removalRecords(world.remote);
            const get = vi.spyOn(sender.db, "get").mockRejectedValueOnce(new Error("the database is busy"));

            await expect(sender.core.sync()).resolves.toBe(false);

            expect(get).toHaveBeenCalled();
            expect(sender.checkpoint.receivedFiles.has(recordKey)).toBe(false);
            await synchronise(sender);
            expect(sender.checkpoint.receivedFiles.has(recordKey)).toBe(true);
        });
    });

    it("forgets a document it relied on once it has read the records of an hour after the send", async () => {
        const world = createWorld();
        const sender = world.device("sender");
        await writeFile(sender, "recording", ["part-1"]);
        await synchronise(sender);
        await writeFileFromChunksHere(sender, "recording-again", ["part-1"]);
        await synchronise(sender);
        expect(relianceOf(sender).documents).toHaveLength(1);

        world.clock.now += RELIANCE_MEMORY_MS - 1;
        await synchronise(sender);
        expect(relianceOf(sender).documents).toHaveLength(1);

        world.clock.now += 1;
        await synchronise(sender);
        expect(relianceOf(sender).documents).toEqual([]);
    });

    it("does not send a queued chunk again which the remote holds by then, or which is not in the local database", async () => {
        const world = createWorld();
        const sender = world.device("sender");
        await writeFile(sender, "recording", ["part-1"]);
        await synchronise(sender);
        const journals = bulkJournals(world.remote).length;
        sender.entries.set(`bucketsync-reliance-${sender.core.hash}`, {
            documents: [],
            resend: ["h:part-1", "h:missing-here"],
        } satisfies JournalReliance);

        await synchronise(sender);

        expect(bulkJournals(world.remote)).toHaveLength(journals);
        expect(relianceOf(sender).resend).toEqual([]);
    });
});

describe("JournalCleaner journals without an index", () => {
    /** A journal as an earlier version made it: named like any other, and without an index. */
    async function putEarlierJournal(world: World, letter: string, size: number, ...lines: string[]) {
        world.remote.put(docsKey(letter), await journalOf(...lines), size);
        return docsKey(letter);
    }
    /** A journal with a small file, so that a device which receives it holds a file and may remove content. */
    const putJournalWithFile = async (world: World) => await putEarlierJournal(world, "f", 100, documentLine("kept"));

    it("is read once by a desktop device, and removed when it holds nothing but chunks which nothing uses", async () => {
        const world = createWorld();
        const unused = await putEarlierJournal(world, "a", UNINDEXED_JOURNAL_MIN_BYTES, chunkLine("old-1", "L"));
        const kept = await putJournalWithFile(world);
        const desktop = world.device("desktop", { cleans: true });

        await synchronise(desktop);
        await synchroniseAfterInterval(world, desktop);
        await synchroniseAfterInterval(world, desktop);

        expect(world.remote.keys()).toEqual([kept]);
        // Once to receive it, and once to learn what it holds.
        expect(downloadsOf(world.remote, unused)).toHaveLength(2);
        // There was no index to delete with it.
        expect(deletedOf(world.remote)).toEqual([unused]);
        const [recordKey] = removalRecords(world.remote);
        await expect(readRecord(desktop, recordKey)).resolves.toMatchObject({
            chunkIDs: ["h:old-1"],
            journals: [unused],
        });
    });

    it("never removes a journal which holds a document, on any device, in any number of rounds", async () => {
        const world = createWorld();
        // The document in it is deleted by a later journal, so that nothing uses its chunks.
        const mixed = await putEarlierJournal(
            world,
            "a",
            8 * UNINDEXED_JOURNAL_MIN_BYTES,
            chunkLine("old-1", "L"),
            documentLine("recording", ["old-1"])
        );
        const deletion = `${JSON.stringify({
            ...note("recording", ["old-1"]),
            deleted: true,
            _rev: "2-rev2",
            _revisions: { start: 2, ids: ["rev2", "rev1"] },
        })}\n`;
        const later = await putEarlierJournal(world, "b", 100, deletion);
        const desktop = world.device("desktop", { cleans: true });
        const other = world.device("other", { cleans: true });
        const phone = world.device("phone", { mobile: true, cleans: true });

        for (let round = 0; round < 4; round++) {
            for (const device of [desktop, other, phone]) await synchroniseAfterInterval(world, device);
        }

        expect(world.remote.keys()).toEqual([mixed, later]);
        expect(deletedOf(world.remote)).toEqual([]);
        expect(removalRecords(world.remote)).toEqual([]);
        // Each desktop device read it once for this, beside receiving it; the phone only received it.
        expect(downloadsOf(world.remote, mixed)).toHaveLength(5);
    });

    it("is never downloaded by a mobile device to learn its content, nor removed by one", async () => {
        const world = createWorld();
        const unused = await putEarlierJournal(world, "a", UNINDEXED_JOURNAL_MIN_BYTES, chunkLine("old-1", "L"));
        const kept = await putJournalWithFile(world);
        const phone = world.device("phone", { mobile: true, cleans: true });

        for (let round = 0; round < 3; round++) await synchroniseAfterInterval(world, phone);

        expect(world.remote.keys()).toEqual([unused, kept]);
        expect(downloadsOf(world.remote, unused)).toHaveLength(1);
        expect(knownContentOf(phone)).toEqual([]);
    });

    it("is not read when it is smaller than 1 MiB, or of unknown size", async () => {
        const world = createWorld();
        const small = await putEarlierJournal(world, "a", UNINDEXED_JOURNAL_MIN_BYTES - 1, chunkLine("old-1", "L"));
        const unsized = await putEarlierJournal(world, "b", UNINDEXED_JOURNAL_MIN_BYTES, chunkLine("old-2", "L"));
        const kept = await putJournalWithFile(world);
        world.remote.storage.listObjectsInUploadOrder.mockImplementation(async () =>
            world.remote
                .groups()
                .map((group) => group.map(({ key, size }) => (key === unsized ? { key } : { key, size })))
        );
        const desktop = world.device("desktop", { cleans: true });

        for (let round = 0; round < 3; round++) await synchroniseAfterInterval(world, desktop);

        expect(world.remote.keys()).toEqual([small, unsized, kept]);
        expect(downloadsOf(world.remote, small)).toHaveLength(1);
        expect(downloadsOf(world.remote, unsized)).toHaveLength(1);
    });

    it("reads at most 256 MiB of them in one round", async () => {
        const world = createWorld();
        const MIB = 1024 * 1024;
        expect(UNINDEXED_READ_BUDGET_BYTES).toBe(256 * MIB);
        const first = await putEarlierJournal(world, "a", 200 * MIB, chunkLine("old-1", "L"));
        const second = await putEarlierJournal(world, "b", 100 * MIB, chunkLine("old-2", "L"));
        const third = await putEarlierJournal(world, "c", 56 * MIB, chunkLine("old-3", "L"));
        await putEarlierJournal(world, "d", 100, documentLine("kept", ["old-1", "old-2", "old-3"]));
        const desktop = world.device("desktop", { cleans: true });
        const contentKeys = () => knownContentOf(desktop).map((key) => key.slice(-docsKey("a").length));

        await synchronise(desktop);
        // The second does not fit beside the first; the third does, exactly.
        expect(contentKeys().sort()).toEqual([first, third].sort());

        await synchroniseAfterInterval(world, desktop);
        expect(contentKeys().sort()).toEqual([first, second, third].sort());
    });

    it("leaves a journal which cannot be read for a later round", async () => {
        const world = createWorld();
        const unused = await putEarlierJournal(world, "a", UNINDEXED_JOURNAL_MIN_BYTES, chunkLine("old-1", "L"));
        const kept = await putJournalWithFile(world);
        const desktop = world.device("desktop", { cleans: true });
        desktop.host.settings.removeUnreferencedJournalData = false;
        await synchronise(desktop);
        desktop.host.settings.removeUnreferencedJournalData = true;
        world.remote.unavailable.add(unused);

        await synchroniseAfterInterval(world, desktop);
        expect(knownContentOf(desktop)).toEqual([]);

        world.remote.unavailable.clear();
        await synchroniseAfterInterval(world, desktop);
        await synchroniseAfterInterval(world, desktop);
        expect(world.remote.keys()).toEqual([kept]);
    });

    it("leaves a journal of large chunks alone whose index cannot be read, and forgets journals which are gone", async () => {
        const world = createWorld();
        const { journal } = await fileSentReceivedAndDeleted(world);
        const cleaner = world.device("cleaner", { cleans: true });
        world.remote.unavailable.add(`_index-${journal}`);
        for (let round = 0; round < 3; round++) await synchroniseAfterInterval(world, cleaner);
        expect(bulkJournals(world.remote)).toEqual([journal]);
        expect(knownContentOf(cleaner)).toEqual([]);

        world.remote.unavailable.clear();
        await synchroniseAfterInterval(world, cleaner);
        expect(knownContentOf(cleaner)).toHaveLength(1);

        // Another device removes the journal. This device forgets what it knew of it.
        await world.remote.storage.deleteFiles([journal, `_index-${journal}`]);
        await synchroniseAfterInterval(world, cleaner);
        expect(knownContentOf(cleaner)).toEqual([]);
    });

    it("does not take an index of a version it does not know at its word", async () => {
        const world = createWorld();
        const cleaner = world.device("cleaner", { cleans: true });
        world.remote.put(bulkKey("a"), await journalOf(chunkLine("large", "L")));
        await cleaner.core.uploadPackedJson(`_index-${bulkKey("a")}`, { version: 2, chunkIDs: ["h:large"] });
        const kept = await putJournalWithFile(world);

        for (let round = 0; round < 3; round++) await synchroniseAfterInterval(world, cleaner);

        expect(world.remote.keys()).toEqual([bulkKey("a"), kept]);
        expect(knownContentOf(cleaner)).toEqual([]);
    });

    it("stops learning what journals hold when a stop is requested", async () => {
        const world = createWorld();
        await putEarlierJournal(world, "a", UNINDEXED_JOURNAL_MIN_BYTES, chunkLine("old-1", "L"));
        await putEarlierJournal(world, "b", UNINDEXED_JOURNAL_MIN_BYTES, chunkLine("old-2", "L"));
        const desktop = world.device("desktop");
        await synchronise(desktop);
        desktop.host.settings.removeUnreferencedJournalData = true;
        const readRemoteJournal = desktop.core.readRemoteJournal.bind(desktop.core);
        vi.spyOn(desktop.core, "readRemoteJournal").mockImplementationOnce(async (key) => {
            desktop.core.requestStop();
            return await readRemoteJournal(key);
        });

        await synchroniseAfterInterval(world, desktop);
        expect(knownContentOf(desktop)).toHaveLength(1);

        await synchroniseAfterInterval(world, desktop);
        expect(knownContentOf(desktop)).toHaveLength(2);
    });

    it("never removes a journal which it refused to apply because it holds a document, whatever its index says", async () => {
        const world = createWorld();
        const desktop = world.device("desktop", { cleans: true });
        // It is named as holding only chunks, and its index names one chunk, which nothing uses.
        const named = bulkKey("a");
        world.remote.put(named, await journalOf(chunkLine("large", "L"), documentLine("hidden-note")), 100);
        await desktop.core.uploadPackedJson(`_index-${named}`, { version: 1, chunkIDs: ["h:large"] });
        const kept = await putJournalWithFile(world);

        for (let round = 0; round < 4; round++) await synchroniseAfterInterval(world, desktop);

        expect(world.remote.keys()).toEqual([named, kept]);
        expect(removalRecords(world.remote)).toEqual([]);
    });

    it("never removes a journal whose index names no chunk", async () => {
        const world = createWorld();
        const desktop = world.device("desktop", { cleans: true });
        world.remote.put(bulkKey("a"), await journalOf(chunkLine("large", "L")), 100);
        await desktop.core.uploadPackedJson(`_index-${bulkKey("a")}`, { version: 1, chunkIDs: [] });
        const kept = await putJournalWithFile(world);

        for (let round = 0; round < 4; round++) await synchroniseAfterInterval(world, desktop);

        expect(world.remote.keys()).toEqual([bulkKey("a"), kept]);
        expect(removalRecords(world.remote)).toEqual([]);
    });

    it("does not take a journal which can hold a document at the word of an index", async () => {
        const world = createWorld();
        const named = await putEarlierJournal(world, "a", UNINDEXED_JOURNAL_MIN_BYTES, documentLine("note"));
        const desktop = world.device("desktop", { cleans: true });
        await desktop.core.uploadPackedJson(`_index-${named}`, { version: 1, chunkIDs: ["h:unused"] });

        for (let round = 0; round < 3; round++) await synchroniseAfterInterval(world, desktop);

        expect(world.remote.keys()).toEqual([named]);
        expect(knownContentOf(desktop)).toEqual([]);
    });
});

describe("JournalCleaner trust in the local database", () => {
    const IDENTITY_DOC = "_local/bucketsync-database-identity";
    const IDENTITY_KEY = "bucketsync-database-identity";
    type IdentityDatabase = PouchDB.Database<{ identity: string }>;
    const identityInStore = (device: Device) => device.entries.get(IDENTITY_KEY) as string | undefined;
    const identityInDatabase = async (device: Device) =>
        await (device.db as unknown as IdentityDatabase).get(IDENTITY_DOC).then(
            (doc) => doc.identity,
            () => undefined
        );
    const isMarkedIncomplete = (device: Device) => device.entries.has(`bucketsync-incomplete-${device.core.hash}`);
    const refusals = (notices: string[]) =>
        notices.filter((message) => message.includes("is not removed from the remote"));
    /**
     * Let the local database answer with an error for the documents of a received journal which `isRefused` picks.
     * @returns a function which lets it store everything again.
     */
    function refuseToStore(
        device: Device,
        isRefused: (docs: { _id: string }[], options: { new_edits?: boolean } | undefined) => boolean,
        status = 500
    ) {
        const bulkDocs = vi.mocked(device.db.bulkDocs);
        const store = bulkDocs.getMockImplementation() as unknown as (...args: unknown[]) => Promise<unknown>;
        bulkDocs.mockImplementation(((...args: unknown[]) => {
            const [docs, options] = args as [{ _id: string }[], { new_edits?: boolean } | undefined];
            if (Array.isArray(docs) && docs.length > 0 && isRefused(docs, options)) {
                return Promise.resolve(docs.map((doc) => ({ error: true, id: doc._id, status, name: "synthetic" })));
            }
            return store(...args);
        }) as never);
        return () => bulkDocs.mockImplementation(store as never);
    }

    describe("when a received document could not be stored", () => {
        async function noteNotStored(world: World) {
            const { sender, receiver, journal } = await fileSentReceivedAndDeleted(world, { cleans: true });
            await sender.db.put(note("refused-note"));
            await synchronise(sender);
            const storeAgain = refuseToStore(
                receiver,
                (docs, options) => options?.new_edits === false && docs.some((doc) => doc._id === "refused-note")
            );
            return { receiver, journal, storeAgain, noteJournal: world.remote.keys().at(-1)! };
        }

        it("says so, records the journal as received all the same, and removes nothing from then on", async () => {
            const world = createWorld();
            const { receiver, journal, noteJournal } = await noteNotStored(world);
            const notices = noticesFromNowOn();

            await synchronise(receiver);

            expect(notices.filter((message) => message.includes("could not be stored"))).toHaveLength(1);
            expect(receiver.checkpoint.receivedFiles.has(noteJournal)).toBe(true);
            expect(receiver.checkpoint.knownIDs.has(`refused-note-${receiver.handedOn.at(-1)!.split("@")[1]}`)).toBe(
                true
            );
            expect(isMarkedIncomplete(receiver)).toBe(true);

            for (let round = 0; round < 3; round++) await synchroniseAfterInterval(world, receiver);
            expect(bulkJournals(world.remote)).toEqual([journal]);
            expect(removalRecords(world.remote)).toEqual([]);
            // The user is told once in a session that nothing is removed.
            expect(refusals(notices)).toHaveLength(1);
        });

        it("removes again after the journal history was reset", async () => {
            const world = createWorld();
            const { receiver, storeAgain } = await noteNotStored(world);
            await synchronise(receiver);
            storeAgain();

            await receiver.createPaneClient().resetCheckpointInfo();
            expect(isMarkedIncomplete(receiver)).toBe(false);
            for (let round = 0; round < 3; round++) await synchroniseAfterInterval(world, receiver);

            // It received everything again, the note among it, and removed what nothing uses.
            await expect(receiver.db.get("refused-note")).resolves.toBeDefined();
            expect(bulkJournals(world.remote)).toEqual([]);
        });

        it.each([
            ["counts a chunk which could not be stored", 500, true],
            ["does not count a chunk which was stored meanwhile", 409, false],
        ])("%s", async (_case, status, incomplete) => {
            const world = createWorld();
            const sender = world.device("sender");
            const receiver = world.device("receiver");
            await writeFile(sender, "recording", ["part-1", "part-2"]);
            await synchronise(sender);
            refuseToStore(
                receiver,
                (docs, options) => options?.new_edits === true && docs[0]._id.startsWith("h:"),
                status
            );

            await synchronise(receiver);

            expect(isMarkedIncomplete(receiver)).toBe(incomplete);
            // What the receive records is the same either way.
            expect(receiver.checkpoint.receivedFiles.has(bulkJournals(world.remote)[0])).toBe(true);
        });
    });

    describe("identity of the local database", () => {
        it("is given to the local database and to the key-value store together, in the first round", async () => {
            const world = createWorld();
            const { sender, receiver } = await fileSentReceivedAndDeleted(world, { cleans: true });

            const identity = identityInStore(receiver);
            expect(identity).toMatch(/^[a-f0-9]{32}$/u);
            await expect(identityInDatabase(receiver)).resolves.toBe(identity);
            // A device which does not clean has none.
            expect(identityInStore(sender)).toBeUndefined();
            await expect(identityInDatabase(sender)).resolves.toBeUndefined();

            // It stays, and it is no change of the local database: the removal goes through.
            await synchroniseAfterInterval(world, receiver);
            await synchroniseAfterInterval(world, receiver);
            expect(identityInStore(receiver)).toBe(identity);
            expect(bulkJournals(world.remote)).toEqual([]);
        });

        it.each<[string, (device: Device) => Promise<void> | void]>([
            [
                "the local database is another one, without an identity",
                async (device) => {
                    const db = device.db as unknown as IdentityDatabase;
                    await db.remove(await db.get(IDENTITY_DOC));
                },
            ],
            [
                "the local database carries another identity",
                async (device) => {
                    const db = device.db as unknown as IdentityDatabase;
                    await db.put({ ...(await db.get(IDENTITY_DOC)), identity: "of another database" });
                },
            ],
            ["the key-value store has no identity", (device) => void device.entries.delete(IDENTITY_KEY)],
        ])("removes nothing when %s, until the journal history is reset", async (_case, part) => {
            const world = createWorld();
            const { receiver, journal } = await fileSentReceivedAndDeleted(world, { cleans: true });
            await synchroniseAfterInterval(world, receiver);
            await part(receiver);
            const notices = noticesFromNowOn();

            for (let round = 0; round < 3; round++) await synchroniseAfterInterval(world, receiver);

            expect(bulkJournals(world.remote)).toEqual([journal]);
            expect(removalRecords(world.remote)).toEqual([]);
            expect(refusals(notices)).toHaveLength(1);

            // With an empty history the device receives everything again, and the two get one identity anew.
            await receiver.createPaneClient().resetCheckpointInfo();
            expect(identityInStore(receiver)).toBeUndefined();
            await expect(identityInDatabase(receiver)).resolves.toBeUndefined();
            for (let round = 0; round < 3; round++) await synchroniseAfterInterval(world, receiver);
            expect(bulkJournals(world.remote)).toEqual([]);
            expect(identityInStore(receiver)).toBeDefined();
            await expect(identityInDatabase(receiver)).resolves.toBe(identityInStore(receiver));
        });

        it("gives no identity while the local database cannot be asked for its own", async () => {
            const world = createWorld();
            const sender = world.device("sender");
            const cleaner = world.device("cleaner", { cleans: true });
            await writeFile(sender, "recording", ["part-1"]);
            await synchronise(sender);
            const get = cleaner.db.get.bind(cleaner.db) as (...args: unknown[]) => Promise<unknown>;
            let failures = 0;
            vi.spyOn(cleaner.db, "get").mockImplementation(((...args: unknown[]) => {
                if (args[0] === IDENTITY_DOC && failures++ === 0) {
                    return Promise.reject(new Error("the database is busy"));
                }
                return get(...args);
            }) as never);

            // A read which failed says nothing about whether the database has an identity.
            await synchronise(cleaner);
            expect(failures).toBe(1);
            expect(identityInStore(cleaner)).toBeUndefined();

            await synchroniseAfterInterval(world, cleaner);
            expect(identityInStore(cleaner)).toBeDefined();
            await expect(identityInDatabase(cleaner)).resolves.toBe(identityInStore(cleaner));
        });

        it("does not let a reset fail which cannot reach the local database", async () => {
            const world = createWorld();
            const { receiver } = await fileSentReceivedAndDeleted(world, { cleans: true });
            vi.spyOn(receiver.db, "get").mockRejectedValueOnce(new Error("the database is closed"));

            await expect(receiver.core.resetCheckpointInfo()).resolves.toBeUndefined();

            expect(receiver.checkpoint.receivedFiles.size).toBe(0);
        });
    });

    it("removes nothing when the journal history has looked through more changes than the local database has had", async () => {
        const world = createWorld();
        const { receiver, journal } = await fileSentReceivedAndDeleted(world, { cleans: true });
        await synchroniseAfterInterval(world, receiver);
        // The key-value store was paired with a database which is not the one it described.
        const sequence = Number((await receiver.db.info()).update_seq);
        setCheckpoint(receiver, { lastLocalSeq: sequence + 1 });
        const notices = noticesFromNowOn();

        for (let round = 0; round < 3; round++) await synchroniseAfterInterval(world, receiver);

        expect(bulkJournals(world.remote)).toEqual([journal]);
        expect(removalRecords(world.remote)).toEqual([]);
        expect(refusals(notices)).toHaveLength(1);

        // A history which has looked through exactly what the database has had is no such sign.
        setCheckpoint(receiver, { lastLocalSeq: sequence });
        for (let round = 0; round < 2; round++) await synchroniseAfterInterval(world, receiver);
        expect(bulkJournals(world.remote)).toEqual([]);
    });

    it.each<[string, (device: Device) => Promise<unknown>, PlainEntry]>([
        ["no document at all", async () => {}, note("kept")],
        [
            "only documents which are no files",
            async (device) => await device.db.put({ _id: "synthetic-version-info", type: "versioninfo" } as never),
            // A file keeps its place in the database when it is deleted, and counts as one.
            { ...note("gone"), deleted: true },
        ],
    ])("removes nothing while the local database holds %s", async (_case, fill, file) => {
        const world = createWorld();
        const cleaner = world.device("cleaner", { cleans: true });
        world.remote.put(bulkKey("a"), await journalOf(chunkLine("large", "L")), 100);
        await cleaner.core.uploadPackedJson(`_index-${bulkKey("a")}`, { version: 1, chunkIDs: ["h:large"] });
        await fill(cleaner);

        for (let round = 0; round < 4; round++) await synchroniseAfterInterval(world, cleaner);

        expect(world.remote.keys()).toContain(bulkKey("a"));
        expect(removalRecords(world.remote)).toEqual([]);
        expect(cleaningStateOf(cleaner)?.assessment).toBeUndefined();

        // Once it holds a file, it can tell what is in use.
        await cleaner.db.put(file);
        for (let round = 0; round < 3; round++) await synchroniseAfterInterval(world, cleaner);
        expect(world.remote.keys()).not.toContain(bulkKey("a"));
    });
});

describe("JournalSyncCore while the journals with documents are received", () => {
    it("answers that content is still to come, and no longer once they are received", async () => {
        const world = createWorld();
        const sender = world.device("sender");
        const receiver = world.device("receiver");
        await synchronise(receiver);
        expect(receiver.core.hasDeferredRemoteContent()).toBe(false);
        await sender.db.put(note("small-note"));
        await synchronise(sender);
        const whileReceiving: boolean[] = [];
        const processDocuments = receiver.core.processDocuments.bind(receiver.core);
        vi.spyOn(receiver.core, "processDocuments").mockImplementation(async (...args) => {
            whileReceiving.push(receiver.core.hasDeferredRemoteContent());
            return await processDocuments(...args);
        });

        await synchronise(receiver);

        expect(whileReceiving).toEqual([true]);
        expect(receiver.core.hasDeferredRemoteContent()).toBe(false);
    });

    it("no longer answers so once the receive has failed", async () => {
        const world = createWorld();
        const receiver = world.device("receiver");
        await synchronise(receiver);
        world.remote.put(docsKey("9"), encoder.encode("not a compressed journal"));

        await expect(receiver.core.sync()).resolves.toBe(false);

        expect(receiver.core.hasDeferredRemoteContent()).toBe(false);
    });
});

describe("JournalSyncCore local changes after a round of large chunks", () => {
    /** A receiver which has received a file except for its large chunks, which are still in the remote. */
    async function largeChunksStillToFetch(world: World, options: { cleans?: boolean } = {}) {
        const sender = world.device("sender");
        const receiver = world.device("receiver", options);
        await writeFile(sender, "recording", ["part-1", "part-2"]);
        await synchronise(sender);
        receiver.core.mayReceiveBulkJournals = () => false;
        await synchronise(receiver);
        receiver.core.mayReceiveBulkJournals = () => true;
        return { sender, receiver, sends: vi.spyOn(receiver.core, "sendLocalJournal") };
    }
    /** Call `act` once, when the receiver has downloaded a journal of large chunks. */
    function whenLargeChunksArrive(world: World, act: () => Promise<void> | void) {
        const download = world.remote.downloadWithResult.getMockImplementation()!;
        let acted = false;
        world.remote.downloadWithResult.mockImplementation(async (key) => {
            const result = await download(key);
            if (!acted && isBulkKey(key)) {
                acted = true;
                await act();
            }
            return result;
        });
    }

    it("looks through what the round stored, so that none of it is sent when a removal record follows", async () => {
        const world = createWorld();
        const { sender, receiver, sends } = await largeChunksStillToFetch(world);
        const cleaner = world.device("cleaner", { cleans: true });

        await synchronise(receiver);

        expect(sends).toHaveBeenCalledTimes(2);
        await expect(hasChunk(receiver, "part-1")).resolves.toBe(true);
        expect(receiver.checkpoint.lastLocalSeq).toBe((await receiver.db.info()).update_seq);

        // The file is deleted, and its content is removed from the remote.
        await deleteFile(sender, "recording");
        await synchronise(sender);
        await synchronise(cleaner);
        await synchroniseAfterInterval(world, cleaner);
        expect(bulkJournals(world.remote)).toEqual([]);

        // The record takes the chunks out of what the remote holds. The receiver has passed them, and sends none.
        await synchronise(receiver);
        expect(receiver.checkpoint.knownIDs.has("h:part-1")).toBe(false);
        expect(bulkJournals(world.remote)).toEqual([]);
    });

    it("sends what was written during the round in the same synchronisation", async () => {
        const world = createWorld();
        const { receiver } = await largeChunksStillToFetch(world);
        whenLargeChunksArrive(world, async () => void (await receiver.db.put(note("written-meanwhile"))));

        await synchronise(receiver);

        expect((await storedEntries(world.remote)).has("written-meanwhile")).toBe(true);
    });

    it("does not look again when the round received nothing", async () => {
        const world = createWorld();
        const { receiver, sends } = await largeChunksStillToFetch(world);
        receiver.core.mayReceiveBulkJournals = () => false;

        await synchronise(receiver);

        expect(sends).toHaveBeenCalledTimes(1);
    });

    it("does not look again when a stop was requested during the round", async () => {
        const world = createWorld();
        const { receiver, sends } = await largeChunksStillToFetch(world);
        whenLargeChunksArrive(world, () => receiver.core.requestStop());

        await synchronise(receiver);

        expect(sends).toHaveBeenCalledTimes(1);
        await expect(hasChunk(receiver, "part-1")).resolves.toBe(true);
        expect(receiver.core.requestedStop).toBe(true);
    });

    it("does not look again when the send before the round failed", async () => {
        const world = createWorld();
        const { receiver, sends } = await largeChunksStillToFetch(world);
        await receiver.db.put(note("written-here"));
        world.remote.upload.mockImplementationOnce(async () => false);

        await expect(receiver.core.sync()).resolves.toBe(false);

        expect(sends).toHaveBeenCalledTimes(1);
        await expect(hasChunk(receiver, "part-1")).resolves.toBe(true);
    });

    it("fails the synchronisation, and does not clean, when that send fails", async () => {
        const world = createWorld();
        const { receiver } = await largeChunksStillToFetch(world, { cleans: true });
        const stateBefore = cleaningStateOf(receiver);
        world.clock.now += CLEANING_INTERVAL_MS + MINUTE;
        whenLargeChunksArrive(world, async () => {
            await receiver.db.put(note("written-meanwhile"));
            world.remote.upload.mockImplementation(async () => false);
        });

        await expect(receiver.core.sync()).resolves.toBe(false);

        expect(receiver.statics.value.syncStatus).toBe("ERRORED");
        expect(cleaningStateOf(receiver)).toEqual(stateBefore);
    });
});

describe("JournalSyncCore check for a wiped remote", () => {
    const storeSyncParameters = (remote: Remote, seed: string) =>
        remote.put(
            DOCID_JOURNAL_SYNC_PARAMETERS,
            encoder.encode(JSON.stringify({ protocolVersion: ProtocolVersions.ADVANCED_E2EE, pbkdf2salt: btoa(seed) }))
        );
    /** A device which sent two journals, and then sees other sync parameters than it knew. */
    async function deviceWithSentJournals(world: World) {
        const device = world.device("sender");
        await device.db.put(note("first"));
        await synchronise(device);
        await device.db.put(note("second"));
        await synchronise(device);
        storeSyncParameters(world.remote, "synthetic-seed");
        const sent = [...device.checkpoint.sentFiles].sort();
        return { device, firstSent: sent[0], probed: sent[1] };
    }

    it("does not take the remote for wiped when only the journal it looks for has been removed", async () => {
        const world = createWorld();
        const { device, probed } = await deviceWithSentJournals(world);
        // Another device removed that journal alone, and this device has not read its record yet.
        await world.remote.storage.deleteFiles([probed]);
        const sequence = device.checkpoint.lastLocalSeq;

        await device.core.ensureCheckpointCachesAreFresh();

        expect(device.checkpoint.lastLocalSeq).toBe(sequence);
        expect(device.checkpoint.sentFiles.size).toBe(2);
        expect(device.checkpoint.journalEpoch).not.toBe("");
    });

    it("takes the remote for wiped when none of the journals it sent is left", async () => {
        const world = createWorld();
        const { device, firstSent, probed } = await deviceWithSentJournals(world);
        await world.remote.storage.deleteFiles([firstSent, probed]);

        await device.core.ensureCheckpointCachesAreFresh();

        expect(device.checkpoint.lastLocalSeq).toBe(0);
        expect(device.checkpoint.sentFiles.size).toBe(0);
    });

    it("forgets which journals of large chunks it refused when it takes the remote for wiped", async () => {
        const world = createWorld();
        const { device, firstSent, probed } = await deviceWithSentJournals(world);
        world.remote.put(bulkKey("a"), await journalOf(documentLine("old-revision")));
        await expect(device.core.receiveRemoteJournal()).resolves.toBe(false);
        expect(device.core.isRefusedBulkJournal(bulkKey("a"))).toBe(true);
        await world.remote.storage.deleteFiles([firstSent, probed]);

        await device.core.ensureCheckpointCachesAreFresh();

        expect(device.core.isRefusedBulkJournal(bulkKey("a"))).toBe(false);
    });

    it("lists the remote once more only when the journal it looks for is missing", async () => {
        const world = createWorld();
        const { device } = await deviceWithSentJournals(world);
        world.remote.storage.listFiles.mockClear();
        world.remote.storage.listFiles.mockImplementation(async (from?: string, limit?: number) =>
            world.remote
                .objectKeys()
                .filter((key) => key > (from ?? ""))
                .sort()
                .slice(0, limit)
        );

        await device.core.ensureCheckpointCachesAreFresh();

        expect(world.remote.storage.listFiles).toHaveBeenCalledOnce();
        expect(device.checkpoint.sentFiles.size).toBe(2);
    });
});

describe("JournalSyncCore default settings", () => {
    it("does not remove anything from the remote unless a device is set to", () => {
        expect(DEFAULT_SETTINGS.removeUnreferencedJournalData).toBe(false);
    });

    it("keeps the setting of a device out of the setup code which other devices read", () => {
        expect(KeyIndexOfSettings.removeUnreferencedJournalData).toBe(-1);
    });
});
