import { afterEach, describe, expect, it, vi } from "vitest";
import {
    BULK_CHUNK_MIN_BYTES,
    BULK_JOURNAL_SUFFIX,
    BULK_RECEIVE_BUDGET_MS,
    JournalSyncCore,
} from "./JournalSyncCore.ts";
import { JournalStorageReadStatuses, type IJournalStorage } from "./objectstore/JournalStorageAdapter.ts";
import { LiveSyncJournalReplicator } from "./LiveSyncJournalReplicator.ts";
import {
    DEFAULT_SETTINGS,
    LOG_LEVEL_INFO,
    LOG_LEVEL_NOTICE,
    type DocumentID,
    type EntryDoc,
    type EntryLeaf,
    type PlainEntry,
} from "@lib/common/types.ts";
import { setGlobalLogFunction } from "@lib/common/logger.ts";
import { wrappedInflate } from "@lib/pouchdb/compress.ts";
import { REMOTE_CHUNK_FETCHED } from "@lib/pouchdb/LiveSyncLocalDB.ts";
import {
    bulkKey,
    chunk,
    chunkId,
    chunkLine,
    closeDevices,
    contentOf,
    createDevice,
    createRemote,
    decoder,
    type Device,
    docsKey,
    documentLine,
    DOCS_SUFFIX,
    encoder,
    entriesOf,
    hasChunk,
    isBulkKey,
    journalOf,
    largeChunk,
    note,
    type Remote,
    storedEntries,
    UNIT_SPLIT,
} from "./journalSpecHarness.spec.ts";

afterEach(closeDevices);

describe("JournalSyncCore sending large chunks", () => {
    it("uploads large chunks in journals of their own, before the journal with the documents and small chunks", async () => {
        const remote = createRemote();
        const device = createDevice("sender", remote.storage);
        await device.db.bulkDocs([
            chunk("text", "a line of text"),
            largeChunk("binary-1", 1_500_000),
            largeChunk("binary-2", 1_500_000),
            note("attachment", ["binary-1", "binary-2"]),
            note("note", ["text"]),
        ]);

        await expect(device.core.sendLocalJournal()).resolves.toBe(true);

        const [bulkKey, docsKey] = remote.keys();
        expect(remote.keys()).toHaveLength(2);
        expect(bulkKey).toMatch(/^[a-f0-9]{64}-chunks\.jsonl\.gz$/u);
        expect(docsKey).toMatch(/^[a-f0-9]{64}-docs\.jsonl\.gz$/u);
        await expect(entriesOf(remote, bulkKey)).resolves.toEqual({
            chunks: ["h:binary-1", "h:binary-2"],
            documents: [],
        });
        await expect(entriesOf(remote, docsKey)).resolves.toEqual({
            chunks: ["h:text"],
            documents: ["attachment", "note"],
        });
        expect(remote.upload.mock.calls.map(([, , , options]) => options)).toEqual([
            { keepExisting: true },
            { keepExisting: true },
        ]);
        expect(device.checkpoint.lastLocalSeq).toBe((await device.db.info()).update_seq);
    });

    it("writes a journal of large chunks in the format of any other journal", async () => {
        const remote = createRemote();
        const device = createDevice("sender", remote.storage);
        const data = contentOf("binary\nwith a line break", 20 * 1024);
        await device.db.bulkDocs([chunk("binary", data)]);

        await expect(device.core.sendLocalJournal()).resolves.toBe(true);

        const [bulkKey] = remote.keys();
        expect(isBulkKey(bulkKey)).toBe(true);
        const text = decoder.decode(await wrappedInflate(remote.dataOf(bulkKey) as Uint8Array<ArrayBuffer>, {}));
        // A line break inside a chunk is escaped, which the marker before the data announces.
        expect(text).toBe(`~h:binary${UNIT_SPLIT}\\f${data.replaceAll("\n", "\\n")}\n`);
    });

    it("counts a chunk as large from 16 KiB of serialised size", async () => {
        expect(BULK_CHUNK_MIN_BYTES).toBe(16 * 1024);
        const remote = createRemote();
        const device = createDevice("sender", remote.storage);
        // A chunk is serialised as "~<id><separator><data><line break>".
        const dataBytes = (id: DocumentID, serialisedBytes: number) => serialisedBytes - id.length - 3;
        await device.db.bulkDocs([
            chunk("at-limit", "x".repeat(dataBytes(chunkId("at-limit"), BULK_CHUNK_MIN_BYTES))),
            chunk("below-limit", "x".repeat(dataBytes(chunkId("below-limit"), BULK_CHUNK_MIN_BYTES - 1))),
        ]);

        await expect(device.core.sendLocalJournal()).resolves.toBe(true);

        const [bulkKey, docsKey] = remote.keys();
        await expect(entriesOf(remote, bulkKey)).resolves.toEqual({ chunks: ["h:at-limit"], documents: [] });
        await expect(entriesOf(remote, docsKey)).resolves.toEqual({ chunks: ["h:below-limit"], documents: [] });
    });

    it("never puts a document into a journal of large chunks, however large the document is", async () => {
        const remote = createRemote();
        const device = createDevice("sender", remote.storage);
        const manyChildren = Array.from({ length: 2_000 }, (_, index) => `child-${index}`);
        await device.db.bulkDocs([note("long-document", manyChildren), largeChunk("binary")]);

        await expect(device.core.sendLocalJournal()).resolves.toBe(true);

        for (const key of remote.keys().filter(isBulkKey)) {
            expect((await entriesOf(remote, key)).documents).toEqual([]);
        }
        const docsKey = remote.keys().find((key) => key.endsWith(DOCS_SUFFIX))!;
        expect(remote.dataOf(docsKey).length).toBeGreaterThan(0);
        await expect(entriesOf(remote, docsKey)).resolves.toEqual({ chunks: [], documents: ["long-document"] });
    });

    describe("in batches which each close a journal of large chunks and a journal of documents", () => {
        // Packs of four changes: a note and three chunks of about 3.5 MB, which fill a batch on the last row.
        const fillTwoBatches = async (device: Device) => {
            device.core.batchSize = 4;
            const payload = "x".repeat(3_500_000);
            await device.db.bulkDocs(
                ["first", "second"].flatMap((batch) => [
                    note(`note-${batch}`),
                    ...[1, 2, 3].map((index) => chunk(`binary-${batch}-${index}`, `${batch}${index}${payload}`)),
                ])
            );
        };
        const ALL_ENTRIES = [
            "note-first",
            "h:binary-first-1",
            "h:binary-first-2",
            "h:binary-first-3",
            "note-second",
            "h:binary-second-1",
            "h:binary-second-2",
            "h:binary-second-3",
        ];

        it("advances the checkpoint only with the last journal of each batch", async () => {
            const remote = createRemote();
            const device = createDevice("sender", remote.storage);
            await fillTwoBatches(device);
            const positionAtUpload: (number | string)[] = [];
            const storeUpload = remote.upload.getMockImplementation()!;
            remote.upload.mockImplementation(async (...args) => {
                positionAtUpload.push(device.checkpoint.lastLocalSeq);
                return await storeUpload(...args);
            });

            await expect(device.core.sendLocalJournal()).resolves.toBe(true);

            expect(remote.keys().map(isBulkKey)).toEqual([true, false, true, false]);
            // The large chunks of a batch leave the checkpoint where it was; its documents then move it on.
            expect(positionAtUpload).toEqual([0, 0, 4, 4]);
            expect(device.checkpoint.lastLocalSeq).toBe(8);
        });

        it.each([
            ["the large chunks of the first batch", 1, 0],
            ["the documents of the first batch", 2, 0],
            ["the large chunks of the second batch", 3, 4],
            ["the documents of the second batch", 4, 4],
        ])(
            "loses nothing and replaces nothing when the upload of %s fails",
            async (_what, failingUpload, expectedPosition) => {
                const remote = createRemote();
                const device = createDevice("sender", remote.storage);
                await fillTwoBatches(device);
                const storeUpload = remote.upload.getMockImplementation()!;
                let attempts = 0;
                remote.upload.mockImplementation(async (...args) =>
                    ++attempts === failingUpload ? false : await storeUpload(...args)
                );

                await expect(device.core.sendLocalJournal()).resolves.toBe(false);

                // The checkpoint has not passed anything which is not stored.
                expect(device.checkpoint.lastLocalSeq).toBe(expectedPosition);
                expect(remote.keys()).toHaveLength(failingUpload - 1);
                const storedBeforeRetry = remote.keys();

                await expect(device.core.sendLocalJournal()).resolves.toBe(true);

                const entries = await storedEntries(remote);
                expect([...entries.keys()].sort()).toEqual([...ALL_ENTRIES].sort());
                // Nothing which was stored is sent a second time, under its own name or another.
                expect([...entries.values()]).toEqual(ALL_ENTRIES.map(() => 1));
                for (const [key, contents] of remote.written) expect(contents.size, key).toBe(1);
                const retried = remote.upload.mock.calls.slice(failingUpload).map(([key]) => key);
                for (const key of storedBeforeRetry) expect(retried).not.toContain(key);
                expect(device.checkpoint.lastLocalSeq).toBe(8);
            }
        );
    });

    describe("in batches which close in the middle of a pack while both journals hold entries", () => {
        // Packs of five changes. The third large chunk of each pack fills a batch while a note is waiting in the
        // other journal, and more changes of the same pack follow it.
        const ENTRIES = [
            "note-1",
            "h:binary-1",
            "h:binary-2",
            "h:binary-3",
            "note-2",
            "h:binary-4",
            "h:binary-5",
            "h:binary-6",
            "note-3",
            "h:text",
        ];
        const fillPacks = async (device: Device) => {
            device.core.batchSize = 5;
            const payload = "x".repeat(3_500_000);
            await device.db.bulkDocs(
                ENTRIES.map((id) => {
                    if (id === "h:text") return chunk("text", "a line of text");
                    if (id.startsWith("h:")) return chunk(id.slice(2), `${id}${payload}`);
                    return note(id);
                })
            );
        };
        /** The changes up to the recorded position which are not stored on the remote. Each change has its place as its sequence. */
        const passedButNotStored = async (device: Device, remote: Remote) => {
            const stored = await storedEntries(remote);
            return ENTRIES.slice(0, Number(device.checkpoint.lastLocalSeq)).filter((id) => !stored.has(id));
        };

        it("never records a position past a change which is not stored", async () => {
            const remote = createRemote();
            const device = createDevice("sender", remote.storage);
            await fillPacks(device);
            const positionAtUpload: (number | string)[] = [];
            const passedAtUpload: string[][] = [];
            const storeUpload = remote.upload.getMockImplementation()!;
            remote.upload.mockImplementation(async (...args) => {
                positionAtUpload.push(device.checkpoint.lastLocalSeq);
                passedAtUpload.push(await passedButNotStored(device, remote));
                return await storeUpload(...args);
            });

            await expect(device.core.sendLocalJournal()).resolves.toBe(true);

            expect(remote.keys().map(isBulkKey)).toEqual([true, false, true, false, false]);
            // The first batch closes inside the first pack and the second inside the second pack, which completes
            // only the first. The rest follows when the stream ends.
            expect(positionAtUpload).toEqual([0, 0, 0, 0, 5]);
            expect(passedAtUpload).toEqual([[], [], [], [], []]);
            expect(device.checkpoint.lastLocalSeq).toBe(10);
            await expect(passedButNotStored(device, remote)).resolves.toEqual([]);
        });

        it.each([1, 2, 3, 4, 5])("loses nothing and replaces nothing when upload %i fails", async (failingUpload) => {
            const remote = createRemote();
            const device = createDevice("sender", remote.storage);
            await fillPacks(device);
            const storeUpload = remote.upload.getMockImplementation()!;
            let attempts = 0;
            remote.upload.mockImplementation(async (...args) =>
                ++attempts === failingUpload ? false : await storeUpload(...args)
            );

            await expect(device.core.sendLocalJournal()).resolves.toBe(false);

            await expect(passedButNotStored(device, remote)).resolves.toEqual([]);
            expect(remote.keys()).toHaveLength(failingUpload - 1);

            await expect(device.core.sendLocalJournal()).resolves.toBe(true);

            const entries = await storedEntries(remote);
            expect([...entries.keys()].sort()).toEqual([...ENTRIES].sort());
            expect([...entries.values()]).toEqual(ENTRIES.map(() => 1));
            for (const [key, contents] of remote.written) expect(contents.size, key).toBe(1);
            expect(device.checkpoint.lastLocalSeq).toBe(10);
        });
    });

    it("sends the documents again when the send stops between the last two journals", async () => {
        const remote = createRemote();
        const device = createDevice("sender", remote.storage);
        await device.db.bulkDocs([largeChunk("binary"), note("attachment", ["binary"]), note("note")]);
        const storeUpload = remote.upload.getMockImplementation()!;
        remote.upload.mockImplementationOnce(storeUpload).mockImplementationOnce(async () => false);

        await expect(device.core.sendLocalJournal()).resolves.toBe(false);

        expect(device.checkpoint.lastLocalSeq).toBe(0);
        const [bulkKey] = remote.keys();
        expect(remote.keys()).toEqual([bulkKey]);
        expect(isBulkKey(bulkKey)).toBe(true);

        await expect(device.core.sendLocalJournal()).resolves.toBe(true);

        expect(remote.keys()).toHaveLength(2);
        await expect(entriesOf(remote, remote.keys()[1])).resolves.toEqual({
            chunks: [],
            documents: ["attachment", "note"],
        });
        expect(remote.upload.mock.calls.filter(([key]) => key === bulkKey)).toHaveLength(1);
        expect(device.checkpoint.lastLocalSeq).toBe((await device.db.info()).update_seq);
    });
});

describe("JournalSyncCore receiving journals of large chunks", () => {
    it("applies the journals with documents in upload order, sends, and then fetches large chunks smallest first", async () => {
        const remote = createRemote();
        const device = createDevice("receiver", remote.storage);
        // The journals of large chunks were uploaded before the documents which need them.
        remote.put(bulkKey("a"), await journalOf(chunkLine("large-a", "A")), 300);
        remote.put(docsKey("f"), await journalOf(documentLine("first", ["large-a"])));
        remote.put(bulkKey("b"), await journalOf(chunkLine("large-b", "B")), 100);
        remote.put(docsKey("0"), await journalOf(documentLine("second", ["large-b"])));
        // An object whose name begins with an underscore is not a journal, whatever its suffix.
        remote.put(`_${bulkKey("c")}`, encoder.encode("not a journal"));
        await device.db.put(note("written-here"));
        const chunkWasStoredWhenHandedOn: boolean[] = [];
        device.core.processReplication = async (docs) => {
            if (docs.length > 0) chunkWasStoredWhenHandedOn.push(await hasChunk(device, "large-a"));
            return true;
        };
        const fetched: string[] = [];
        device.context.events.onEvent(REMOTE_CHUNK_FETCHED, (leaf: EntryLeaf) => fetched.push(leaf._id));

        await expect(device.core.sync()).resolves.toBe(true);

        const sentKey = remote.keys().at(-1)!;
        expect(sentKey).toMatch(/^[a-f0-9]{64}-docs\.jsonl\.gz$/u);
        expect(remote.transfers).toEqual([
            `download ${docsKey("f")}`,
            `download ${docsKey("0")}`,
            `upload ${sentKey}`,
            `download ${bulkKey("b")}`,
            `download ${bulkKey("a")}`,
        ]);
        // The documents were handed on before their large chunks were here, and the chunks arrived afterwards.
        expect(chunkWasStoredWhenHandedOn).toEqual([false, false]);
        expect(fetched).toEqual(["h:large-b", "h:large-a"]);
        expect(device.checkpoint.knownIDs.has("h:large-a")).toBe(true);
        expect(device.checkpoint.knownIDs.has("h:large-b")).toBe(true);
        expect([...device.checkpoint.receivedFiles].sort()).toEqual(
            [docsKey("f"), docsKey("0"), bulkKey("a"), bulkKey("b")].sort()
        );
        expect(device.statics.value.syncStatus).toBe("COMPLETED");
        expect(device.core.hasDeferredRemoteContent()).toBe(false);
        expect(device.core.takeDeferredReceiveContinuation()).toBe(true);
        expect(device.core.takeDeferredReceiveContinuation()).toBe(false);
    });

    it("keeps journals of one size in upload order and fetches one of unknown size last", async () => {
        const remote = createRemote();
        const device = createDevice("receiver", remote.storage);
        remote.put(bulkKey("f"), await journalOf(chunkLine("first", "1")), 200);
        remote.put(bulkKey("e"), await journalOf(chunkLine("unknown", "2")));
        remote.put(bulkKey("0"), await journalOf(chunkLine("second", "3")), 200);
        remote.storage.listObjectsInUploadOrder.mockImplementation(async () =>
            remote
                .groups()
                .map((group) => group.map(({ key, size }) => (key === bulkKey("e") ? { key } : { key, size })))
        );

        await expect(device.core.receiveRemoteJournal()).resolves.toBe(true);

        expect(remote.transfers).toEqual([
            `download ${bulkKey("f")}`,
            `download ${bulkKey("0")}`,
            `download ${bulkKey("e")}`,
        ]);
    });

    it("applies journals without the suffix in upload order, however large they are", async () => {
        const remote = createRemote();
        const device = createDevice("receiver", remote.storage);
        // A journal of an earlier release holds its large chunks beside a revision of a document.
        remote.put(
            docsKey("f"),
            await journalOf(chunkLine("large", "L"), documentLine("note", ["large"], 1)),
            8_000_000
        );
        remote.put(docsKey("0"), await journalOf(documentLine("other")), 100);
        remote.put(bulkKey("a"), await journalOf(chunkLine("small-journal", "S")), 10);

        await expect(device.core.sync()).resolves.toBe(true);

        expect(remote.transfers).toEqual([
            `download ${docsKey("f")}`,
            `download ${docsKey("0")}`,
            `download ${bulkKey("a")}`,
        ]);
        expect(device.handedOn).toEqual(["note@1-rev1", "other@1-rev1"]);
    });

    it.each([
        [
            "only the order of uploads",
            (storage: Remote["storage"]) => ({ ...storage, listObjectsInUploadOrder: undefined }),
        ],
        [
            "neither sizes nor the order of uploads",
            (storage: Remote["storage"]) => ({
                ...storage,
                listObjectsInUploadOrder: undefined,
                listFilesInUploadOrder: undefined,
            }),
        ],
    ])("receives every journal before it sends when the storage reports %s", async (_what, withoutSizes) => {
        const remote = createRemote();
        const device = createDevice("receiver", withoutSizes(remote.storage) as IJournalStorage);
        remote.put(bulkKey("1"), await journalOf(chunkLine("large", "L")), 300);
        remote.put(docsKey("2"), await journalOf(documentLine("note", ["large"])), 100);
        await device.db.put(note("written-here"));
        const chunkWasStoredWhenHandedOn: boolean[] = [];
        device.core.processReplication = async () => {
            chunkWasStoredWhenHandedOn.push(await hasChunk(device, "large"));
            return true;
        };

        await expect(device.core.sync()).resolves.toBe(true);

        // This is also how a client of an earlier release reads the journals: all of them, in one order.
        expect(remote.transfers.slice(0, 2)).toEqual([`download ${bulkKey("1")}`, `download ${docsKey("2")}`]);
        expect(remote.transfers[2]).toMatch(/^upload /u);
        expect(chunkWasStoredWhenHandedOn).toEqual([true, true]);
        expect(device.core.hasDeferredRemoteContent()).toBe(false);
        expect(device.core.takeDeferredReceiveContinuation()).toBe(false);
    });

    it("sends nothing and fetches no large chunks when a journal with documents cannot be received", async () => {
        const remote = createRemote();
        const device = createDevice("receiver", remote.storage);
        remote.put(docsKey("1"), await journalOf(documentLine("note")));
        remote.put(bulkKey("2"), await journalOf(chunkLine("large", "L")));
        // For a journal which can hold documents, a missing object stays a failure.
        remote.removeButKeepListed(docsKey("1"));
        await device.db.put(note("written-here"));

        await expect(device.core.sync()).resolves.toBe(false);

        expect(remote.transfers).toEqual([`download ${docsKey("1")}`]);
        expect(device.statics.value.syncStatus).toBe("ERRORED");
        expect(device.checkpoint.receivedFiles.size).toBe(0);
        expect(device.core.takeDeferredReceiveContinuation()).toBe(false);
    });

    describe("within a time budget", () => {
        /** Four journals of large chunks, each of which takes ten seconds to download. */
        const prepareSlowRemote = async () => {
            const remote = createRemote();
            const device = createDevice("receiver", remote.storage);
            const keys = ["a", "b", "c", "d"].map(bulkKey);
            for (const [index, key] of keys.entries()) {
                remote.put(key, await journalOf(chunkLine(`large-${index}`, "L")), 100 + index);
            }
            let time = 1_000_000;
            device.core.now = () => time;
            const download = remote.downloadWithResult.getMockImplementation()!;
            remote.downloadWithResult.mockImplementation(async (key) => {
                time += 10_000;
                return await download(key);
            });
            return { remote, device, keys };
        };

        it("stops starting journals after fifteen seconds, and goes on in the next synchronisation", async () => {
            const { remote, device, keys } = await prepareSlowRemote();
            expect(device.core.bulkReceiveBudgetMs).toBe(BULK_RECEIVE_BUDGET_MS);
            expect(BULK_RECEIVE_BUDGET_MS).toBe(15_000);

            await expect(device.core.sync()).resolves.toBe(true);

            // The second journal was started after ten seconds and finished after twenty.
            expect(remote.transfers).toEqual([`download ${keys[0]}`, `download ${keys[1]}`]);
            expect(device.statics.value.syncStatus).toBe("COMPLETED");
            expect(device.core.hasDeferredRemoteContent()).toBe(true);
            expect(device.core.takeDeferredReceiveContinuation()).toBe(true);

            await expect(device.core.sync()).resolves.toBe(true);

            expect(remote.transfers.slice(2)).toEqual([`download ${keys[2]}`, `download ${keys[3]}`]);
            expect(device.core.hasDeferredRemoteContent()).toBe(false);
            expect(device.core.takeDeferredReceiveContinuation()).toBe(true);

            await expect(device.core.sync()).resolves.toBe(true);

            expect(remote.transfers).toHaveLength(4);
            expect(device.core.takeDeferredReceiveContinuation()).toBe(false);
        });

        it("receives one journal in each synchronisation even without any time", async () => {
            const { remote, device, keys } = await prepareSlowRemote();
            device.core.bulkReceiveBudgetMs = 0;

            await expect(device.core.sync()).resolves.toBe(true);

            expect(remote.transfers).toEqual([`download ${keys[0]}`]);
            expect(device.checkpoint.receivedFiles.has(keys[0])).toBe(true);
        });

        it("receives every journal without a budget when the receive is called on its own", async () => {
            const { remote, device, keys } = await prepareSlowRemote();
            remote.put(docsKey("e"), await journalOf(documentLine("note")));
            device.core.bulkReceiveBudgetMs = 0;

            await expect(device.core.receiveRemoteJournal()).resolves.toBe(true);

            expect(remote.transfers).toEqual([`download ${docsKey("e")}`, ...keys.map((key) => `download ${key}`)]);
            expect(device.statics.value.syncStatus).toBe("COMPLETED");
            expect(device.core.hasDeferredRemoteContent()).toBe(false);
            // Nothing is left for a next synchronisation to continue.
            expect(device.core.takeDeferredReceiveContinuation()).toBe(false);
        });

        it("stops between journals when a stop is requested", async () => {
            const { remote, device, keys } = await prepareSlowRemote();
            const download = remote.downloadWithResult.getMockImplementation()!;
            remote.downloadWithResult.mockImplementation(async (key) => {
                device.core.requestStop();
                return await download(key);
            });

            await expect(device.core.receiveRemoteJournal()).resolves.toBe(false);

            expect(remote.transfers).toEqual([`download ${keys[0]}`]);
            expect(device.checkpoint.receivedFiles.has(keys[0])).toBe(true);
        });
    });

    describe("when a journal of large chunks cannot be received", () => {
        const prepare = async () => {
            const remote = createRemote();
            const device = createDevice("receiver", remote.storage);
            const keys = ["a", "b", "c"].map(bulkKey);
            for (const [index, key] of keys.entries()) {
                remote.put(key, await journalOf(chunkLine(`large-${index}`, "L")), 100 + index);
            }
            return { remote, device, keys };
        };

        it.each([
            ["cannot be downloaded", (remote: Remote, key: string) => remote.unavailable.add(key)],
            [
                "cannot be read",
                (remote: Remote, key: string) => remote.put(key, encoder.encode("not a compressed journal"), 101),
            ],
        ])(
            "goes on with the other journals and tries the journal again next time when it %s",
            async (_what, breakJournal) => {
                const { remote, device, keys } = await prepare();
                const intact = remote.dataOf(keys[1]);
                breakJournal(remote, keys[1]);

                // The result of the synchronisation is that of its send.
                await expect(device.core.sync()).resolves.toBe(true);

                expect(remote.transfers).toEqual(keys.map((key) => `download ${key}`));
                expect(device.statics.value.syncStatus).toBe("COMPLETED");
                expect([...device.checkpoint.receivedFiles].sort()).toEqual([keys[0], keys[2]].sort());
                expect(device.core.takeDeferredReceiveContinuation()).toBe(true);

                remote.unavailable.clear();
                remote.put(keys[1], intact, 101);
                await expect(device.core.sync()).resolves.toBe(true);

                expect(remote.transfers.slice(3)).toEqual([`download ${keys[1]}`]);
                expect(device.checkpoint.receivedFiles.has(keys[1])).toBe(true);
                expect(device.core.hasDeferredRemoteContent()).toBe(false);
            }
        );

        it("reports a receive on its own as failed", async () => {
            const { remote, device, keys } = await prepare();
            remote.unavailable.add(keys[1]);

            await expect(device.core.receiveRemoteJournal()).resolves.toBe(false);

            expect(device.statics.value.syncStatus).toBe("ERRORED");
            expect([...device.checkpoint.receivedFiles].sort()).toEqual([keys[0], keys[2]].sort());
        });

        it("ends the fetching after two journals in a row failed, and then tries the others first", async () => {
            const { remote, device, keys } = await prepare();
            remote.unavailable.add(keys[0]);
            remote.unavailable.add(keys[1]);

            await expect(device.core.sync()).resolves.toBe(true);

            // A lost connection is not tried on every journal of a long list.
            expect(remote.transfers).toEqual([`download ${keys[0]}`, `download ${keys[1]}`]);
            expect(device.checkpoint.receivedFiles.size).toBe(0);
            expect(device.core.takeDeferredReceiveContinuation()).toBe(false);

            // The two smallest journals stay broken. The larger one behind them is still reached.
            await expect(device.core.sync()).resolves.toBe(true);

            expect(remote.transfers.slice(2)).toEqual([
                `download ${keys[2]}`,
                `download ${keys[0]}`,
                `download ${keys[1]}`,
            ]);
            expect([...device.checkpoint.receivedFiles]).toEqual([keys[2]]);
        });

        it("counts one failure after a received journal as the first of a row", async () => {
            const { remote, device, keys } = await prepare();
            remote.put(bulkKey("d"), await journalOf(chunkLine("large-3", "L")), 103);
            remote.unavailable.add(keys[0]);
            remote.unavailable.add(keys[2]);

            await expect(device.core.sync()).resolves.toBe(true);

            expect(remote.transfers).toEqual([...keys, bulkKey("d")].map((key) => `download ${key}`));
            expect([...device.checkpoint.receivedFiles].sort()).toEqual([keys[1], bulkKey("d")].sort());
        });

        it("does not count a journal which failed as content which is still to come, until it is received", async () => {
            const { remote, device, keys } = await prepare();
            remote.unavailable.add(keys[1]);
            // Each synchronisation fetches one journal, so the others are still to come after the first.
            device.core.bulkReceiveBudgetMs = 0;
            let time = 0;
            device.core.now = () => time++;

            await expect(device.core.sync()).resolves.toBe(true);
            expect(device.core.hasDeferredRemoteContent()).toBe(true);
            await expect(device.core.sync()).resolves.toBe(true);
            expect(remote.transfers).toEqual([`download ${keys[0]}`, `download ${keys[1]}`]);
            // The journal which failed is not expected, but the third one is.
            expect(device.core.hasDeferredRemoteContent()).toBe(true);

            await expect(device.core.sync()).resolves.toBe(true);
            expect(remote.transfers.slice(2)).toEqual([`download ${keys[2]}`]);
            // Only the journal which failed is left, so a document which needs it is no longer waiting for content
            // which is on its way.
            expect(device.core.hasDeferredRemoteContent()).toBe(false);

            remote.unavailable.clear();
            await expect(device.core.sync()).resolves.toBe(true);
            expect(device.checkpoint.receivedFiles.has(keys[1])).toBe(true);
            expect(device.core.hasDeferredRemoteContent()).toBe(false);
        });

        it("forgets a failure once the journal was received", async () => {
            const { remote, device, keys } = await prepare();
            remote.unavailable.add(keys[0]);
            await expect(device.core.sync()).resolves.toBe(true);
            remote.unavailable.clear();
            await expect(device.core.sync()).resolves.toBe(true);
            expect(device.checkpoint.receivedFiles.size).toBe(3);

            // The history of received journals is reset, so all of them are received again.
            await device.core.resetCheckpointInfo();
            remote.transfers.length = 0;
            await expect(device.core.sync()).resolves.toBe(true);

            // The journal which once failed has its place by size again, instead of being tried last. The reset
            // also makes the device send what it holds, which is not looked at here.
            expect(remote.transfers.filter((entry) => entry.startsWith("download "))).toEqual(
                keys.map((key) => `download ${key}`)
            );
        });

        it("reports a journal which fails in two synchronisations in a row once as a notice", async () => {
            const { remote, device, keys } = await prepare();
            remote.unavailable.add(keys[1]);
            const failureLevels: (number | undefined)[] = [];
            setGlobalLogFunction((message, level) => {
                if (String(message).includes(`${keys[1]} of large chunks could not be received`)) {
                    failureLevels.push(level);
                }
            });

            for (let cycle = 0; cycle < 4; cycle++) {
                await expect(device.core.sync()).resolves.toBe(true);
            }

            expect(failureLevels).toEqual([LOG_LEVEL_INFO, LOG_LEVEL_NOTICE, LOG_LEVEL_INFO, LOG_LEVEL_INFO]);
        });

        it("still fetches large chunks after a send which failed, and reports the failed send", async () => {
            const { remote, device, keys } = await prepare();
            await device.db.put(note("written-here"));
            remote.upload.mockImplementation(async () => false);

            await expect(device.core.sync()).resolves.toBe(false);

            expect(device.statics.value.syncStatus).toBe("ERRORED");
            expect([...device.checkpoint.receivedFiles].sort()).toEqual([...keys].sort());
        });

        it("passes over a journal which is listed but no longer stored, without an error", async () => {
            const { remote, device, keys } = await prepare();
            remote.removeButKeepListed(keys[1]);

            await expect(device.core.sync()).resolves.toBe(true);

            expect(remote.transfers).toEqual(keys.map((key) => `download ${key}`));
            expect([...device.checkpoint.receivedFiles].sort()).toEqual([keys[0], keys[2]].sort());
            expect(device.core.hasDeferredRemoteContent()).toBe(false);
            await expect(device.core.receiveRemoteJournal()).resolves.toBe(true);
        });

        it("does not record a journal as received when the history is reset while it is received", async () => {
            const { device, keys } = await prepare();
            device.context.events.onEvent(REMOTE_CHUNK_FETCHED, () => {
                void device.createPaneClient().resetCheckpointInfo();
            });

            await expect(device.core.receiveRemoteJournal()).resolves.toBe(false);

            for (const key of keys) expect(device.checkpoint.receivedFiles.has(key)).toBe(false);
            expect(device.checkpoint.knownIDs.size).toBe(0);
        });

        it("does not record a journal as received when the history is reset after its chunks were recorded", async () => {
            const { device, keys } = await prepare();
            vi.spyOn(device.core, "processDocuments").mockImplementationOnce(async (docs, resetGeneration) => {
                const processed = await JournalSyncCore.prototype.processDocuments.call(
                    device.core,
                    docs,
                    resetGeneration
                );
                await device.createPaneClient().resetCheckpointInfo();
                return processed;
            });

            await expect(device.core.receiveRemoteJournal()).resolves.toBe(false);

            for (const key of keys) expect(device.checkpoint.receivedFiles.has(key)).toBe(false);
        });

        it.each<[string, (device: Device) => void]>([
            [
                "while its chunks are recorded",
                (device) => {
                    let reset = false;
                    device.context.events.onEvent(REMOTE_CHUNK_FETCHED, () => {
                        if (reset) return;
                        reset = true;
                        void device.createPaneClient().resetCheckpointInfo();
                    });
                },
            ],
            [
                "after its chunks were recorded",
                (device) => {
                    vi.spyOn(device.core, "processDocuments").mockImplementationOnce(async (docs, resetGeneration) => {
                        const processed = await JournalSyncCore.prototype.processDocuments.call(
                            device.core,
                            docs,
                            resetGeneration
                        );
                        await device.createPaneClient().resetCheckpointInfo();
                        return processed;
                    });
                },
            ],
        ])(
            "ends the receive on a history reset %s, without holding it against any journal",
            async (_when, resetOnce) => {
                const { remote, device, keys } = await prepare();
                resetOnce(device);

                await expect(device.core.receiveRemoteJournal()).resolves.toBe(false);

                // The receive ended with the journal in hand, instead of trying the next one against the old history.
                expect(remote.transfers).toEqual([`download ${keys[0]}`]);
                expect(device.checkpoint.receivedFiles.size).toBe(0);

                remote.transfers.length = 0;
                await expect(device.core.receiveRemoteJournal()).resolves.toBe(true);

                // No journal counts as having failed, so each has its place by size and all are expected.
                expect(remote.transfers).toEqual(keys.map((key) => `download ${key}`));
                expect(device.checkpoint.receivedFiles.size).toBe(3);
            }
        );

        it("tries the journals which failed least often first, so that two broken ones do not shut out the rest", async () => {
            const { remote, device, keys } = await prepare();
            remote.put(bulkKey("d"), await journalOf(chunkLine("large-3", "L")), 103);
            const all = [...keys, bulkKey("d")];
            const downloadsOf = (cycle: () => Promise<boolean>) => async () => {
                remote.transfers.length = 0;
                await expect(cycle()).resolves.toBe(true);
                return remote.transfers.map((entry) => entry.replace("download ", ""));
            };
            const synchronise = downloadsOf(() => device.core.sync());

            // The connection is lost for three synchronisations. Each ends after two failures in a row.
            for (const key of all) remote.unavailable.add(key);
            await expect(synchronise()).resolves.toEqual([all[0], all[1]]);
            await expect(synchronise()).resolves.toEqual([all[2], all[3]]);
            await expect(synchronise()).resolves.toEqual([all[0], all[1]]);

            // The two smallest journals stay broken, and the others can be received again. They failed once and
            // the smallest twice, so they come first.
            remote.unavailable.delete(all[2]);
            remote.unavailable.delete(all[3]);
            await expect(synchronise()).resolves.toEqual([all[2], all[3], all[0], all[1]]);

            expect([...device.checkpoint.receivedFiles].sort()).toEqual([all[2], all[3]].sort());
        });

        it("asks for no continuation after a synchronisation which could not receive", async () => {
            const { remote, device } = await prepare();
            await expect(device.core.sync()).resolves.toBe(true);
            // Nothing asked whether the first synchronisation received large chunks.
            remote.storage.listObjectsInUploadOrder.mockRejectedValueOnce(new Error("synthetic listing failure"));

            await expect(device.core.sync()).resolves.toBe(false);

            expect(device.core.takeDeferredReceiveContinuation()).toBe(false);
        });
    });

    describe("before and after it has listed the remote", () => {
        it("cannot rule out large chunks which are still to come until it has listed the remote", async () => {
            const remote = createRemote();
            const device = createDevice("receiver", remote.storage);
            remote.put(docsKey("a"), await journalOf(documentLine("note")));

            expect(device.core.hasDeferredRemoteContent()).toBe(true);
            remote.storage.listObjectsInUploadOrder.mockRejectedValueOnce(new Error("synthetic listing failure"));
            await expect(device.core.sync()).resolves.toBe(false);
            expect(device.core.hasDeferredRemoteContent()).toBe(true);

            await expect(device.core.sync()).resolves.toBe(true);
            expect(device.core.hasDeferredRemoteContent()).toBe(false);
        });

        it("forgets what it learnt about large chunks when it is pointed at another remote", async () => {
            const remote = createRemote();
            const device = createDevice("receiver", remote.storage);
            const notices: string[] = [];
            setGlobalLogFunction((message, level) => {
                if (level === LOG_LEVEL_NOTICE) notices.push(String(message));
            });
            remote.put(bulkKey("a"), await journalOf(documentLine("old-revision")), 100);
            remote.put(bulkKey("b"), await journalOf(chunkLine("large-b", "B")), 200);
            remote.put(bulkKey("c"), await journalOf(chunkLine("large-c", "C")), 300);
            remote.unavailable.add(bulkKey("b"));
            await expect(device.core.sync()).resolves.toBe(true);
            // One journal was refused, one failed and one was received, which nothing has asked about yet.
            expect(device.core.hasDeferredRemoteContent()).toBe(false);
            const otherRemote = { ...device.settings, bucket: "synthetic-other-bucket" };

            device.core.applyNewConfig(otherRemote, device.store, device.env);

            // Nothing is known of the other remote, and no continuation is left over from the first.
            expect(device.core.hasDeferredRemoteContent()).toBe(true);
            expect(device.core.takeDeferredReceiveContinuation()).toBe(false);
            remote.transfers.length = 0;
            device.core.mayReceiveBulkJournals = () => false;
            await expect(device.core.sync()).resolves.toBe(true);
            // The journal which failed on the first remote is expected again on this one.
            expect(device.core.hasDeferredRemoteContent()).toBe(true);
            device.core.mayReceiveBulkJournals = () => true;
            await expect(device.core.sync()).resolves.toBe(true);
            // Nothing of the first remote is remembered, neither what was received nor the refusal nor the failure:
            // every journal is fetched, and the one which fails does so for the first time here, which is no notice
            // yet.
            expect(remote.transfers.filter((entry) => entry.startsWith("download "))).toEqual(
                ["a", "b", "c"].map((letter) => `download ${bulkKey(letter)}`)
            );
            expect(notices.filter((message) => message.includes("holds a document"))).toHaveLength(2);
            expect(notices.filter((message) => message.includes("could not be received"))).toEqual([]);
        });

        it("keeps what it learnt when the same remote is configured again", async () => {
            const remote = createRemote();
            const device = createDevice("receiver", remote.storage);
            await expect(device.core.sync()).resolves.toBe(true);

            device.core.applyNewConfig(device.settings, device.store, device.env);

            expect(device.core.hasDeferredRemoteContent()).toBe(false);
        });

        it("cannot tell about another remote, although the first one had nothing left to come", async () => {
            const remote = createRemote();
            const device = createDevice("receiver", remote.storage);
            await expect(device.core.sync()).resolves.toBe(true);
            expect(device.core.hasDeferredRemoteContent()).toBe(false);

            device.core.applyNewConfig(
                { ...device.settings, bucket: "synthetic-other-bucket" },
                device.store,
                device.env
            );

            expect(device.core.hasDeferredRemoteContent()).toBe(true);
        });
    });

    describe("when the host holds large chunks back", () => {
        const prepare = async () => {
            const remote = createRemote();
            const device = createDevice("receiver", remote.storage);
            remote.put(docsKey("f"), await journalOf(documentLine("note", ["large-0"])));
            const keys = ["a", "b", "c"].map(bulkKey);
            for (const [index, key] of keys.entries()) {
                remote.put(key, await journalOf(chunkLine(`large-${index}`, "L")), 100 + index);
            }
            await device.db.put(note("written-here"));
            return { remote, device, keys };
        };

        it("receives the documents and sends, but fetches no large chunks", async () => {
            const { remote, device } = await prepare();
            device.core.mayReceiveBulkJournals = () => false;

            await expect(device.core.sync()).resolves.toBe(true);

            expect(remote.transfers).toHaveLength(2);
            expect(remote.transfers[0]).toBe(`download ${docsKey("f")}`);
            expect(remote.transfers[1]).toMatch(/^upload [a-f0-9]{64}-docs\.jsonl\.gz$/u);
            expect(device.handedOn).toEqual(["note@1-rev1"]);
            expect(device.statics.value.syncStatus).toBe("COMPLETED");
            // The large chunks stay expected, and nothing asks for another synchronisation at once.
            expect(device.core.hasDeferredRemoteContent()).toBe(true);
            expect(device.core.takeDeferredReceiveContinuation()).toBe(false);
        });

        it("ends a round which is running after the journal in hand", async () => {
            const { remote, device, keys } = await prepare();
            let allowed = true;
            device.core.mayReceiveBulkJournals = () => allowed;
            const download = remote.downloadWithResult.getMockImplementation()!;
            remote.downloadWithResult.mockImplementation(async (key) => {
                // The host withdraws its permission while the first journal of large chunks is fetched.
                if (key === keys[0]) allowed = false;
                return await download(key);
            });

            await expect(device.core.sync()).resolves.toBe(true);

            expect(remote.transfers.filter((entry) => entry.startsWith("download ")).slice(1)).toEqual([
                `download ${keys[0]}`,
            ]);
            expect(device.checkpoint.receivedFiles.has(keys[0])).toBe(true);
            expect(device.core.hasDeferredRemoteContent()).toBe(true);

            allowed = true;
            await expect(device.core.sync()).resolves.toBe(true);
            expect(device.core.hasDeferredRemoteContent()).toBe(false);
        });

        it("still receives everything when the receive is called on its own", async () => {
            const { device, keys } = await prepare();
            device.core.mayReceiveBulkJournals = () => false;

            await expect(device.core.receiveRemoteJournal()).resolves.toBe(true);

            for (const key of keys) expect(device.checkpoint.receivedFiles.has(key)).toBe(true);
        });

        it("is asked through its replicator, as when a hidden application sends what was written", async () => {
            const { remote, device, keys } = await prepare();
            const replicator = new LiveSyncJournalReplicator(device.env);
            replicator._client = device.core;
            vi.spyOn(replicator, "checkReplicationConnectivity").mockResolvedValue(true);
            let hidden = true;
            replicator.mayReceiveDeferredContent = () => !hidden;

            // The application is hidden, and synchronises once to send what was written.
            await expect(replicator.openReplication(DEFAULT_SETTINGS, false, false)).resolves.toBe(true);

            expect(remote.transfers.filter((entry) => entry.startsWith("upload "))).toHaveLength(1);
            expect(remote.transfers.filter((entry) => keys.some((key) => entry.endsWith(key)))).toEqual([]);
            expect(replicator.hasDeferredRemoteContent()).toBe(true);
            expect(replicator.takeDeferredReceiveContinuation()).toBe(false);

            // It is shown again, and the next synchronisation fetches the large chunks.
            hidden = false;
            await expect(replicator.openReplication(DEFAULT_SETTINGS, false, false)).resolves.toBe(true);

            for (const key of keys) expect(device.checkpoint.receivedFiles.has(key)).toBe(true);
            expect(replicator.takeDeferredReceiveContinuation()).toBe(true);
        });
    });

    it("leaves the count of received journals to the journals with documents", async () => {
        const remote = createRemote();
        const device = createDevice("receiver", remote.storage);
        remote.put(docsKey("a"), await journalOf(documentLine("first")));
        remote.put(docsKey("b"), await journalOf(documentLine("second")));
        remote.put(docsKey("c"), await journalOf(documentLine("third")));
        remote.put(bulkKey("d"), await journalOf(chunkLine("large", "L")));

        await expect(device.core.sync()).resolves.toBe(true);

        expect(device.checkpoint.receivedFiles.size).toBe(4);
        expect(device.statics.value).toMatchObject({ arrived: 3, maxPullSeq: 3, lastSyncPullSeq: 3 });
    });

    it("applies journals with documents which arrive during a long queue of large chunks before it goes on with them", async () => {
        const remote = createRemote();
        const device = createDevice("receiver", remote.storage);
        const keys = ["a", "b", "c"].map(bulkKey);
        for (const [index, key] of keys.entries()) {
            remote.put(key, await journalOf(chunkLine(`large-${index}`, "L")), 100 + index);
        }
        // Each synchronisation fetches one journal of large chunks.
        device.core.bulkReceiveBudgetMs = 0;
        await expect(device.core.sync()).resolves.toBe(true);
        expect(remote.transfers).toEqual([`download ${keys[0]}`]);

        // Another device uploads two revisions of a note, whose journal names sort against their uploads, and a
        // journal of large chunks smaller than those which are left.
        remote.put(docsKey("f"), await journalOf(documentLine("note", [], 1)));
        remote.put(docsKey("0"), await journalOf(documentLine("note", [], 2)));
        remote.put(bulkKey("d"), await journalOf(chunkLine("large-new", "L")), 50);
        await expect(device.core.sync()).resolves.toBe(true);

        expect(remote.transfers.slice(1)).toEqual([
            `download ${docsKey("f")}`,
            `download ${docsKey("0")}`,
            `download ${bulkKey("d")}`,
        ]);
        expect(device.handedOn).toEqual(["note@1-rev1", "note@2-rev2"]);
        expect(device.core.hasDeferredRemoteContent()).toBe(true);

        await expect(device.core.sync()).resolves.toBe(true);
        await expect(device.core.sync()).resolves.toBe(true);
        expect(remote.transfers.slice(4)).toEqual([`download ${keys[1]}`, `download ${keys[2]}`]);
        expect(device.core.hasDeferredRemoteContent()).toBe(false);
    });

    it("applies nothing from a journal of large chunks which holds a document, and says so once", async () => {
        const remote = createRemote();
        const device = createDevice("receiver", remote.storage);
        const notices: string[] = [];
        setGlobalLogFunction((message, level) => {
            if (level === LOG_LEVEL_NOTICE) notices.push(String(message));
        });
        remote.put(bulkKey("a"), await journalOf(chunkLine("beside-document", "L"), documentLine("old-revision")), 100);
        remote.put(bulkKey("b"), await journalOf(chunkLine("large", "L")), 200);

        await expect(device.core.sync()).resolves.toBe(true);
        await expect(device.core.sync()).resolves.toBe(true);

        expect(device.saved).toEqual([]);
        expect(device.handedOn).toEqual([]);
        await expect(hasChunk(device, "beside-document")).resolves.toBe(false);
        await expect(hasChunk(device, "large")).resolves.toBe(true);
        expect([...device.checkpoint.receivedFiles]).toEqual([bulkKey("b")]);
        // The journal is fetched and reported once, and does not count as content which is still to arrive.
        expect(remote.transfers).toEqual([`download ${bulkKey("a")}`, `download ${bulkKey("b")}`]);
        expect(notices.filter((message) => message.includes(bulkKey("a")))).toHaveLength(1);
        expect(device.core.hasDeferredRemoteContent()).toBe(false);
    });

    it("fetches a journal which it refused again once the journal history is reset", async () => {
        const remote = createRemote();
        const device = createDevice("receiver", remote.storage);
        remote.put(bulkKey("a"), await journalOf(documentLine("old-revision")), 100);
        await expect(device.core.sync()).resolves.toBe(true);
        expect(device.core.isRefusedBulkJournal(bulkKey("a"))).toBe(true);

        // The remote was built again, and holds a journal of chunks under the name of the one which was refused.
        remote.put(bulkKey("a"), await journalOf(chunkLine("large", "L")), 200);
        await device.core.resetCheckpointInfo();
        expect(device.core.isRefusedBulkJournal(bulkKey("a"))).toBe(false);
        await expect(device.core.sync()).resolves.toBe(true);

        await expect(hasChunk(device, "large")).resolves.toBe(true);
        expect([...device.checkpoint.receivedFiles]).toEqual([bulkKey("a")]);
    });

    it("reports a receive on its own as incomplete when it refuses such a journal", async () => {
        const remote = createRemote();
        const device = createDevice("receiver", remote.storage);
        remote.put(bulkKey("a"), await journalOf(documentLine("old-revision")), 100);
        remote.put(bulkKey("b"), await journalOf(chunkLine("large", "L")), 200);

        await expect(device.core.receiveRemoteJournal()).resolves.toBe(false);

        await expect(hasChunk(device, "large")).resolves.toBe(true);
    });

    it("does not fetch a journal which another receive recorded after it was listed", async () => {
        const remote = createRemote();
        const device = createDevice("receiver", remote.storage);
        remote.put(bulkKey("a"), await journalOf(chunkLine("large-a", "A")), 100);
        remote.put(bulkKey("b"), await journalOf(chunkLine("large-b", "B")), 200);
        const download = remote.downloadWithResult.getMockImplementation()!;
        remote.downloadWithResult.mockImplementationOnce(async (key) => {
            await device.createPaneClient().updateCheckPointInfo((info) => ({
                ...info,
                receivedFiles: info.receivedFiles.add(bulkKey("b")),
            }));
            return await download(key);
        });

        await expect(device.core.receiveRemoteJournal()).resolves.toBe(true);

        expect(remote.transfers).toEqual([`download ${bulkKey("a")}`]);
        expect(device.core.hasDeferredRemoteContent()).toBe(false);
    });

    it("fetches each journal once when two receives run at the same time", async () => {
        const remote = createRemote();
        const device = createDevice("receiver", remote.storage);
        const keys = ["a", "b", "c"].map(bulkKey);
        for (const [index, key] of keys.entries()) {
            remote.put(key, await journalOf(chunkLine(`large-${index}`, "L")), 100 + index);
        }

        await expect(
            Promise.all([device.core.receiveRemoteJournal(), device.core.receiveRemoteJournal()])
        ).resolves.toEqual([true, true]);

        expect(remote.transfers).toEqual(keys.map((key) => `download ${key}`));
    });

    it("waits for the fetching of large chunks before transfers count as settled", async () => {
        const remote = createRemote();
        const device = createDevice("receiver", remote.storage);
        remote.put(bulkKey("a"), await journalOf(chunkLine("large", "L")));
        let releaseDownload!: () => void;
        const download = remote.downloadWithResult.getMockImplementation()!;
        const downloadStarted = new Promise<void>((started) => {
            remote.downloadWithResult.mockImplementationOnce(async (key) => {
                started();
                await new Promise<void>((release) => (releaseDownload = release));
                return await download(key);
            });
        });
        const synchronising = device.core.sync();
        await downloadStarted;
        let settled = false;
        const waiting = device.core.waitForTransfersToSettle().then(() => (settled = true));

        await Promise.resolve();
        expect(settled).toBe(false);
        releaseDownload();
        await waiting;
        await expect(synchronising).resolves.toBe(true);
    });
});

describe("JournalSyncCore order of document revisions with journals of large chunks", () => {
    /** Write a new revision of a document on a device. */
    const edit = async (device: Device, id: string, children: string[]) => {
        const current = await device.db.get(id);
        await device.db.put({ ...(current as PlainEntry), children: children.map(chunkId), mtime: Date.now() });
    };
    /** Edit a note `times` times, each time with a new small chunk. */
    const editNote = async (device: Device, name: string, id: string, times: number) => {
        for (let round = 0; round < times; round++) {
            const current = await device.db.get(id);
            const chunkName = `${name}-text-${current._rev.split("-")[0]}`;
            await device.db.put(chunk(chunkName, `text ${chunkName}`));
            await edit(device, id, [chunkName]);
        }
    };
    const generationOf = async (device: Device, id: string) => Number((await device.db.get(id))._rev.split("-")[0]);
    /** The revision trees of every document which is not a chunk. */
    const revisionTrees = async (device: Device) => {
        const rows = (await device.db.allDocs()).rows.filter((row) => !row.id.startsWith("h:"));
        return await Promise.all(
            rows.map(async (row) => {
                const doc = await device.db.get(row.id, { revs: true, conflicts: true });
                return { id: doc._id, rev: doc._rev, revs: doc._revisions, conflicts: doc._conflicts ?? [] };
            })
        );
    };
    const storedChunks = async (device: Device) =>
        (await device.db.allDocs()).rows.map((row) => row.id).filter((id) => id.startsWith("h:"));

    /**
     * The same objects as `remote`, with every journal of large chunks named like any other journal, so that a
     * receiver applies all of them strictly in upload order.
     */
    const inUploadOrderOnly = (remote: Remote): IJournalStorage => {
        const renamed = (key: string) => (isBulkKey(key) ? key.replace(BULK_JOURNAL_SUFFIX, DOCS_SUFFIX) : key);
        const original = new Map(remote.keys().map((key) => [renamed(key), key]));
        const downloadWithResult = async (key: string) => await remote.downloadWithResult(original.get(key) ?? key);
        return {
            ...remote.storage,
            downloadWithResult,
            download: async (key: string) => {
                const result = await downloadWithResult(key);
                return result.status === JournalStorageReadStatuses.AVAILABLE ? result.value : false;
            },
            listFiles: async () => [...original.keys()],
            listFilesInUploadOrder: async () => remote.groups().map((group) => group.map(({ key }) => renamed(key))),
            listObjectsInUploadOrder: async () =>
                remote.groups().map((group) => group.map(({ key, size }) => ({ key: renamed(key), size }))),
        };
    };

    it("saves and hands on the same revisions in the same order as a receive strictly in upload order", async () => {
        // The store reports a coarse time, so that consecutive uploads share it and form one group.
        const remote = createRemote((upload) => Math.floor(upload / 3));
        const first = createDevice("first", remote.storage);
        const second = createDevice("second", remote.storage);

        // The first device writes a note and an attachment.
        await first.db.bulkDocs([
            chunk("first-text-0", "text"),
            largeChunk("first-binary-1"),
            largeChunk("first-binary-2"),
            note("note", ["first-text-0"]),
            note("attachment", ["first-binary-1", "first-binary-2"]),
        ]);
        await expect(first.core.sync()).resolves.toBe(true);

        // Its second revision of the note is uploaded together with the large chunks of a picture.
        await editNote(first, "first", "note", 1);
        await first.db.bulkDocs([largeChunk("first-binary-3"), note("picture", ["first-binary-3"])]);
        const generationWithLargeChunks = await generationOf(first, "note");
        await expect(first.core.sync()).resolves.toBe(true);

        // The devices then take turns: each receives, edits the same documents, and sends.
        await expect(second.core.sync()).resolves.toBe(true);
        await editNote(second, "second", "note", 40);
        await second.db.bulkDocs([largeChunk("second-binary-1"), note("recording", ["second-binary-1"])]);
        await second.db.put(largeChunk("second-binary-2"));
        await edit(second, "recording", ["second-binary-2"]);
        await second.db.put(largeChunk("second-binary-3"));
        await edit(second, "attachment", ["first-binary-1", "second-binary-3"]);
        await expect(second.core.sync()).resolves.toBe(true);

        await expect(first.core.sync()).resolves.toBe(true);
        await editNote(first, "first", "note", 45);
        await first.db.put(largeChunk("first-binary-4"));
        await edit(first, "picture", ["first-binary-4"]);
        await expect(first.core.sync()).resolves.toBe(true);

        await expect(second.core.sync()).resolves.toBe(true);
        await editNote(second, "second", "note", 45);
        await second.db.put(largeChunk("second-binary-4"));
        await edit(second, "recording", ["second-binary-4"]);
        await expect(second.core.sync()).resolves.toBe(true);

        await expect(first.core.sync()).resolves.toBe(true);
        await editNote(first, "first", "note", 30);
        await first.db.put(largeChunk("first-binary-5"));
        await edit(first, "attachment", ["first-binary-5"]);
        await expect(first.core.sync()).resolves.toBe(true);
        await expect(second.core.sync()).resolves.toBe(true);

        // The note now has over a hundred revisions after the one which was uploaded with large chunks, so the
        // history of its latest revision no longer reaches back to that one.
        const latestGeneration = await generationOf(first, "note");
        expect(latestGeneration - generationWithLargeChunks).toBeGreaterThan(100);
        expect((await first.db.get("note", { revs: true }))._revisions!.ids).toHaveLength(100);
        const journals = remote.keys();
        expect(journals.filter(isBulkKey).length).toBeGreaterThanOrEqual(6);
        expect(journals.filter((key) => !isBulkKey(key)).length).toBeGreaterThanOrEqual(6);
        // Large chunks and documents lie interleaved in upload order, also within one group.
        expect(remote.groups().some((group) => group.some(({ key }) => isBulkKey(key)) && group.length > 1)).toBe(true);

        // The reference applies the identical objects strictly in upload order.
        const reference = createDevice("reference", inUploadOrderOnly(remote));
        await expect(reference.core.receiveRemoteJournal()).resolves.toBe(true);
        expect(reference.saved.filter((revision) => revision.startsWith("note@")).length).toBeGreaterThanOrEqual(5);

        // One receiver fetches everything in one receive, as a restore does.
        const restored = createDevice("restored", remote.storage);
        const downloadsBefore = remote.transfers.length;
        await expect(restored.core.receiveRemoteJournal()).resolves.toBe(true);
        const restoredDownloads = remote.transfers.slice(downloadsBefore).map((entry) => entry.split(" ")[1]);
        const firstBulkDownload = restoredDownloads.findIndex(isBulkKey);
        expect(restoredDownloads.slice(0, firstBulkDownload).some(isBulkKey)).toBe(false);
        expect(restoredDownloads.slice(firstBulkDownload).every(isBulkKey)).toBe(true);

        // Another synchronises in cycles which each fetch one journal of large chunks.
        const cycling = createDevice("cycling", remote.storage);
        cycling.core.bulkReceiveBudgetMs = 0;
        const uploadsBefore = remote.upload.mock.calls.length;
        let cycles = 0;
        do {
            await expect(cycling.core.sync()).resolves.toBe(true);
            cycles++;
        } while (cycling.core.hasDeferredRemoteContent() && cycles < 100);
        expect(cycles).toBeGreaterThanOrEqual(6);
        expect(cycling.core.hasDeferredRemoteContent()).toBe(false);
        // A device which only received has nothing of its own to send.
        expect(remote.upload.mock.calls.length).toBe(uploadsBefore);

        const referenceTrees = await revisionTrees(reference);
        const referenceChunks = await storedChunks(reference);
        expect(referenceTrees.map(({ id }) => id)).toEqual(["attachment", "note", "picture", "recording"]);
        for (const receiver of [restored, cycling]) {
            expect(receiver.saved).toEqual(reference.saved);
            expect(receiver.handedOn).toEqual(reference.handedOn);
            expect(await revisionTrees(receiver)).toEqual(referenceTrees);
            expect(await storedChunks(receiver)).toEqual(referenceChunks);
        }
        // No revision was left as a conflict, on the receivers or on the devices which wrote them.
        for (const device of [reference, restored, cycling, first, second]) {
            for (const tree of await revisionTrees(device)) {
                expect(tree.conflicts, tree.id).toEqual([]);
                expect(tree.rev, tree.id).toBe(referenceTrees.find(({ id }) => id === tree.id)!.rev);
            }
        }
    }, 60_000);
});
