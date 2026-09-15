import { describe, it, expect, beforeEach, afterEach, vi } from "vitest";
import PouchDB from "pouchdb-core";
import MemoryAdapter from "pouchdb-adapter-memory";
import { JournalSyncCore } from "./JournalSyncCore.ts";
import type { IJournalStorage } from "./objectstore/JournalStorageAdapter.ts";
import type { LiveSyncJournalReplicatorEnv } from "./LiveSyncJournalReplicatorEnv.ts";
import {
    DEFAULT_SETTINGS,
    type BucketSyncSetting,
    type EntryDoc,
    ProtocolVersions,
    DOCID_JOURNAL_SYNC_PARAMETERS,
    type DocumentID,
    type FilePathWithPrefix,
    type PlainEntry,
} from "@lib/common/types.ts";
import { type SimpleStore, pickBucketSyncSettings } from "@lib/common/utils.ts";
import { CheckPointInfoDefault, type CheckPointInfo } from "./JournalSyncTypes.ts";
import { wrappedDeflate, wrappedInflate } from "@lib/pouchdb/compress.ts";
import { REMOTE_CHUNK_FETCHED } from "@lib/pouchdb/LiveSyncLocalDB.ts";
import { createServiceContext } from "@lib/services/base/ServiceBase.ts";

PouchDB.plugin(MemoryAdapter);

describe("JournalSyncCore", () => {
    let dbCounter = 0;
    let localDB: PouchDB.Database<EntryDoc>;
    let env: LiveSyncJournalReplicatorEnv;
    let mockStorage: IJournalStorage;
    let core: JournalSyncCore;
    let virtualStorage: Map<string, Uint8Array>;
    let context: ReturnType<typeof createServiceContext>;
    let checkpointState: CheckPointInfo;

    beforeEach(async () => {
        dbCounter++;
        localDB = new PouchDB(`test_db_${dbCounter}`, { adapter: "memory" });
        virtualStorage = new Map();
        context = createServiceContext();

        mockStorage = {
            upload: vi.fn(async (file: string, buffer: Uint8Array) => {
                virtualStorage.set(file, buffer);
                return true;
            }),
            download: vi.fn(async (file: string) => {
                const data = virtualStorage.get(file);
                if (data === undefined) return false;
                return data;
            }),
            downloadWithResult: vi.fn(async (file: string) => {
                const data = virtualStorage.get(file);
                if (data === undefined) return { status: "not-found" as const };
                return { status: "available" as const, value: data };
            }),
            listFiles: vi.fn(async () => {
                return Array.from(virtualStorage.keys());
            }),
            deleteFile: vi.fn(async (file: string) => {
                virtualStorage.delete(file);
            }),
        } as unknown as IJournalStorage;

        env = {
            services: {
                context,
                database: {
                    localDatabase: {
                        localDatabase: localDB,
                    },
                },
                setting: {
                    currentSettings: () => ({ ...DEFAULT_SETTINGS }),
                    getDeviceAndVaultName: () => "synthetic-device-a",
                },
                replicator: {
                    replicationStatics: {
                        value: {
                            sent: 0,
                            arrived: 0,
                            maxPullSeq: 0,
                            maxPushSeq: 0,
                            lastSyncPullSeq: 0,
                            lastSyncPushSeq: 0,
                            syncStatus: "NOT_CONNECTED",
                        },
                    },
                },
            },
        } as unknown as LiveSyncJournalReplicatorEnv;

        checkpointState = structuredClone(CheckPointInfoDefault);
        const store = {
            get: vi.fn(async () => structuredClone(checkpointState)),
            set: vi.fn(async (_key: string, value: CheckPointInfo) => {
                checkpointState = structuredClone(value);
            }),
            keys: vi.fn(async () => []),
            delete: vi.fn(async () => {}),
        } as unknown as SimpleStore<CheckPointInfo>;

        const settings: BucketSyncSetting = pickBucketSyncSettings(DEFAULT_SETTINGS);
        core = new JournalSyncCore(settings, store, env, mockStorage);
    });

    afterEach(async () => {
        await localDB.destroy();
    });

    describe("getSyncParameters", () => {
        it("throws SyncParamsNotFoundError if sync parameters do not exist in storage", async () => {
            await expect(core.getSyncParameters()).rejects.toThrowError("Missing sync parameters");
        });

        it("returns downloaded sync parameters", async () => {
            const params = { ...DEFAULT_SETTINGS, protocolVersion: ProtocolVersions.ADVANCED_E2EE, pbkdf2salt: "salt" };
            virtualStorage.set(DOCID_JOURNAL_SYNC_PARAMETERS, new TextEncoder().encode(JSON.stringify(params)));

            const fetched = await core.getSyncParameters();
            expect(fetched.pbkdf2salt).toBe("salt");
        });
    });

    describe("downloadJsonWithResult", () => {
        it("preserves a missing-object result", async () => {
            await expect(core.downloadJsonWithResult("missing.json")).resolves.toEqual({ status: "not-found" });
        });

        it("parses available JSON", async () => {
            virtualStorage.set("available.json", new TextEncoder().encode('{"value":42}'));

            await expect(core.downloadJsonWithResult<{ value: number }>("available.json")).resolves.toEqual({
                status: "available",
                value: { value: 42 },
            });
        });

        it("reports invalid JSON as unavailable", async () => {
            virtualStorage.set("invalid.json", new TextEncoder().encode("not-json"));

            const result = await core.downloadJsonWithResult("invalid.json");

            expect(result.status).toBe("unavailable");
            if (result.status === "unavailable") expect(result.error).toBeInstanceOf(SyntaxError);
        });
    });

    describe("sendLocalJournal", () => {
        it("should upload chunk properly via streams", async () => {
            // Insert some documents into local DB
            await localDB.bulkDocs([
                {
                    _id: "doc1" as DocumentID,
                    type: "plain",
                    path: "doc1" as FilePathWithPrefix,
                    children: [],
                    ctime: Date.now(),
                    mtime: Date.now(),
                    size: 0,
                    eden: {},
                } as PlainEntry,
                {
                    _id: "doc2" as DocumentID,
                    type: "plain",
                    path: "doc2" as FilePathWithPrefix,
                    children: [],
                    ctime: Date.now(),
                    mtime: Date.now(),
                    size: 0,
                    eden: {},
                } as PlainEntry,
            ]);

            core.processReplication = async () => true;

            await core.sendLocalJournal(true);

            // Check that it uploaded a chunk
            const uploadedFiles = Array.from(virtualStorage.keys());
            const chunks = uploadedFiles.filter((f) => f.endsWith(".jsonl.gz"));
            expect(chunks.length).toBe(1); // Should have created at least 1 chunk

            const compressedData = virtualStorage.get(chunks[0])!;
            expect(compressedData).toBeInstanceOf(Uint8Array);

            // Decompress and verify
            const decompressed = await wrappedInflate(compressedData as Uint8Array<ArrayBuffer>, {});
            const text = new TextDecoder().decode(decompressed);

            expect(text).toContain("doc1");
            expect(text).toContain("doc2");
        });

        it("reuses the same opaque operation key after a crash before the local checkpoint commit", async () => {
            await localDB.put({
                _id: "doc1" as DocumentID,
                type: "plain",
                path: "doc1" as FilePathWithPrefix,
                children: [],
                ctime: 1,
                mtime: 1,
                size: 0,
                eden: {},
            } as PlainEntry);

            await expect(core.sendLocalJournal()).resolves.toBe(true);
            const firstKey = [...virtualStorage.keys()].find((key) => key.endsWith(".jsonl.gz"));
            expect(firstKey).toMatch(/^[a-f0-9]{64}-docs\.jsonl\.gz$/u);

            checkpointState = structuredClone(CheckPointInfoDefault);
            await expect(core.sendLocalJournal()).resolves.toBe(true);
            const keysAfterRetry = [...virtualStorage.keys()].filter((key) => key.endsWith(".jsonl.gz"));

            expect(keysAfterRetry).toEqual([firstKey]);
            expect(mockStorage.upload).toHaveBeenCalledTimes(2);
        });

        it("sends the rest of a pack after an upload that followed a batch ending inside it fails", async () => {
            // 300 documents form three packs of 100. The first batch closes after 251 documents, inside the third pack.
            await localDB.bulkDocs(
                Array.from(
                    { length: 300 },
                    (_, index) =>
                        ({
                            _id: `doc${String(index).padStart(3, "0")}` as DocumentID,
                            type: "plain",
                            path: `doc${String(index).padStart(3, "0")}` as FilePathWithPrefix,
                            children: [],
                            ctime: 1,
                            mtime: 1,
                            size: 0,
                            eden: {},
                        }) as PlainEntry
                )
            );
            const upload = vi.mocked(mockStorage.upload);
            const storeUpload = upload.getMockImplementation()!;
            upload.mockImplementationOnce(storeUpload).mockImplementationOnce(async () => false);

            await expect(core.sendLocalJournal()).resolves.toBe(false);
            await expect(core.sendLocalJournal()).resolves.toBe(true);

            const sentDocuments = new Set<string>();
            for (const [key, value] of virtualStorage) {
                if (!key.endsWith(".jsonl.gz")) continue;
                const text = new TextDecoder().decode(await wrappedInflate(value as Uint8Array<ArrayBuffer>, {}));
                for (const match of text.matchAll(/"_id":"(doc\d{3})"/gu)) sentDocuments.add(match[1]);
            }
            expect(sentDocuments.size).toBe(300);
        });

        it("never reuses a journal name for other documents when a pack split into several batches is retried", async () => {
            // Seven chunks of about 3.5 MB fit in one pack but close a batch after every third chunk.
            const payload = "x".repeat(3_500_000);
            await localDB.bulkDocs(
                Array.from({ length: 7 }, (_, index) => ({
                    _id: `h:synthetic-chunk-${index}` as DocumentID,
                    type: "leaf" as const,
                    data: `${index}${payload}`,
                })) as EntryDoc[]
            );
            const upload = vi.mocked(mockStorage.upload);
            const storeUpload = upload.getMockImplementation()!;
            const writtenContent = new Map<string, Set<string>>();
            const recordingUpload = async (file: string, buffer: Uint8Array) => {
                const content = writtenContent.get(file) ?? new Set<string>();
                content.add(Buffer.from(buffer).toString("base64"));
                writtenContent.set(file, content);
                return await storeUpload(file, buffer, "application/octet-stream");
            };
            upload.mockImplementation(recordingUpload);
            upload.mockImplementationOnce(recordingUpload).mockImplementationOnce(async () => false);

            await expect(core.sendLocalJournal()).resolves.toBe(false);
            await expect(core.sendLocalJournal()).resolves.toBe(true);

            for (const [file, contents] of writtenContent) expect(contents.size, file).toBe(1);
            const sentChunks = new Set<string>();
            for (const [key, value] of virtualStorage) {
                if (!key.endsWith(".jsonl.gz")) continue;
                const text = new TextDecoder().decode(await wrappedInflate(value as Uint8Array<ArrayBuffer>, {}));
                // Chunks are written as "~<id><unit separator><data>", not as JSON.
                for (const match of text.matchAll(/~(h:synthetic-chunk-\d)/gu)) sentChunks.add(match[1]);
            }
            expect(sentChunks.size).toBe(7);
        });

        it("advances the checkpoint past a pack whose last row closes a batch", async () => {
            // Three chunks of about 3.5 MB close a batch exactly on the last row of their only pack.
            const payload = "x".repeat(3_500_000);
            await localDB.bulkDocs(
                Array.from({ length: 3 }, (_, index) => ({
                    _id: `h:synthetic-boundary-chunk-${index}` as DocumentID,
                    type: "leaf" as const,
                    data: `${index}${payload}`,
                })) as EntryDoc[]
            );

            await expect(core.sendLocalJournal()).resolves.toBe(true);

            expect(mockStorage.upload).toHaveBeenCalledTimes(1);
            expect(checkpointState.lastLocalSeq).toBe((await localDB.info()).update_seq);
        });

        describe("on a device whose local changes are already known", () => {
            const plainEntry = (id: string) =>
                ({
                    _id: id as DocumentID,
                    type: "plain",
                    path: id as FilePathWithPrefix,
                    children: [],
                    ctime: 1,
                    mtime: 1,
                    size: 0,
                    eden: {},
                }) as PlainEntry;

            const markKnown = async (ids: string[]) => {
                const written = await localDB.bulkDocs(ids.map(plainEntry));
                for (const row of written) {
                    if ("rev" in row) checkpointState.knownIDs.add(`${row.id}-${row.rev}`);
                }
            };

            const uploadedJournalText = async () => {
                let text = "";
                for (const [key, value] of virtualStorage) {
                    if (!key.endsWith(".jsonl.gz")) continue;
                    text += new TextDecoder().decode(await wrappedInflate(value as Uint8Array<ArrayBuffer>, {}));
                }
                return text;
            };

            it("records the scanned sequence so the next send does not scan the known changes again", async () => {
                await markKnown(["known1", "known2"]);
                await localDB.put({ _id: "h:known-chunk" as DocumentID, type: "leaf", data: "chunk" } as EntryDoc);
                checkpointState.knownIDs.add("h:known-chunk");

                await expect(core.sendLocalJournal()).resolves.toBe(true);
                expect(mockStorage.upload).not.toHaveBeenCalled();
                const updateSeq = (await localDB.info()).update_seq;
                expect(checkpointState.lastLocalSeq).toBe(updateSeq);

                const changes = vi.spyOn(localDB, "changes");
                await expect(core.sendLocalJournal()).resolves.toBe(true);
                expect(changes).toHaveBeenCalledTimes(1);
                expect(changes.mock.calls[0][0]).toMatchObject({ since: updateSeq });
            });

            it("sends only the new revision and records the known changes scanned after it", async () => {
                await markKnown(Array.from({ length: 50 }, (_, index) => `known${index}`));
                await localDB.put(plainEntry("fresh"));
                await markKnown(Array.from({ length: 150 }, (_, index) => `later${index}`));

                await expect(core.sendLocalJournal()).resolves.toBe(true);

                const text = await uploadedJournalText();
                expect(text).toContain('"_id":"fresh"');
                expect(text).not.toContain('"_id":"known');
                expect(text).not.toContain('"_id":"later');
                expect(checkpointState.lastLocalSeq).toBe((await localDB.info()).update_seq);
            });

            it("keeps the checkpoint before an unsent revision whose upload fails", async () => {
                await markKnown(["known1", "known2"]);
                await localDB.put(plainEntry("fresh"));
                await markKnown(["known3"]);
                vi.mocked(mockStorage.upload).mockImplementationOnce(async () => false);

                await expect(core.sendLocalJournal()).resolves.toBe(false);
                expect(checkpointState.lastLocalSeq).toBe(0);

                await expect(core.sendLocalJournal()).resolves.toBe(true);
                expect(await uploadedJournalText()).toContain('"_id":"fresh"');
                expect(checkpointState.lastLocalSeq).toBe((await localDB.info()).update_seq);
            });

            it("sends an unsent leaf revision of a document whose other revision is known", async () => {
                await markKnown(["conflicted"]);
                const unsentRev = "1-ffffffffffffffffffffffffffffffff";
                await localDB.bulkDocs([{ ...plainEntry("conflicted"), _rev: unsentRev, mtime: 2 }], {
                    new_edits: false,
                });

                await expect(core.sendLocalJournal()).resolves.toBe(true);

                const text = await uploadedJournalText();
                expect(text).toContain(`"_rev":"${unsentRev}"`);
                expect(text.match(/"_id":"conflicted"/gu)).toHaveLength(1);
            });
        });
    });

    describe("receiveRemoteJournal", () => {
        it("should parse and apply incoming documents with new_edits: false", async () => {
            // Put a mock compressed chunk into virtual storage
            const mockDoc = {
                _id: "remote_doc",
                _rev: "1-abc",
                data: "remote data",
                _revisions: {
                    start: 1,
                    ids: ["abc"],
                },
            };
            const rawData = JSON.stringify(mockDoc) + "\n";
            const compressedData = await wrappedDeflate(new TextEncoder().encode(rawData), {});

            virtualStorage.set("test_hash-0000000000000-12345.md", compressedData);

            core.processReplication = async () => true;

            await core.receiveRemoteJournal(true);

            // Verify it was applied to the database
            const localDoc = await localDB.get("remote_doc");
            expect(localDoc).toBeDefined();
            expect(localDoc._rev).toBe("1-abc");
        });

        it("discovers an unseen journal even when its opaque key sorts before prior receipts", async () => {
            checkpointState.receivedFiles.add("f".repeat(64) + "-docs.jsonl.gz");
            const mockDoc = {
                _id: "earlier_key_doc",
                _rev: "1-abc",
                data: "remote data",
                _revisions: { start: 1, ids: ["abc"] },
            };
            const compressedData = await wrappedDeflate(new TextEncoder().encode(`${JSON.stringify(mockDoc)}\n`), {});
            const unseenKey = `${"0".repeat(64)}-docs.jsonl.gz`;
            virtualStorage.set(unseenKey, compressedData);
            core.processReplication = async () => true;

            await expect(core.receiveRemoteJournal()).resolves.toBe(true);

            await expect(localDB.get("earlier_key_doc")).resolves.toMatchObject({ _rev: "1-abc" });
            expect(checkpointState.receivedFiles.has(unseenKey)).toBe(true);
        });

        it("reports an aborted journal listing as a failed receive instead of throwing", async () => {
            vi.mocked(mockStorage.listFiles).mockRejectedValueOnce(
                Object.assign(new Error("The request was aborted"), { name: "AbortError" })
            );

            await expect(core.receiveRemoteJournal()).resolves.toBe(false);
            expect(env.services.replicator.replicationStatics.value.syncStatus).toBe("ERRORED");
        });

        it.each(["missing", "unreadable"] as const)(
            "reports a %s journal as a failed receive without recording it or later journals as received",
            async (fault) => {
                const faultyKey = `${"1".repeat(64)}-docs.jsonl.gz`;
                const laterKey = `${"2".repeat(64)}-docs.jsonl.gz`;
                const laterDoc = { _id: "later_doc", _rev: "1-abc", data: "d", _revisions: { start: 1, ids: ["abc"] } };
                virtualStorage.set(
                    laterKey,
                    await wrappedDeflate(new TextEncoder().encode(`${JSON.stringify(laterDoc)}\n`), {})
                );
                if (fault === "unreadable") {
                    virtualStorage.set(faultyKey, new TextEncoder().encode("not a compressed journal"));
                } else {
                    vi.mocked(mockStorage.listFiles).mockResolvedValueOnce([faultyKey, laterKey]);
                }
                core.processReplication = async () => true;

                await expect(core.receiveRemoteJournal()).resolves.toBe(false);

                expect(env.services.replicator.replicationStatics.value.syncStatus).toBe("ERRORED");
                expect(checkpointState.receivedFiles.has(faultyKey)).toBe(false);
                expect(checkpointState.receivedFiles.has(laterKey)).toBe(false);
                await expect(localDB.get("later_doc")).rejects.toMatchObject({ status: 404 });
            }
        );
    });

    describe("processDocuments", () => {
        it("announces fetched chunks through the owning service context", async () => {
            const listener = vi.fn();
            context.events.onEvent(REMOTE_CHUNK_FETCHED, listener);

            await core.processDocuments([
                {
                    _id: "h:chunk" as DocumentID,
                    _rev: "1-chunk",
                    type: "leaf",
                    data: "chunk-data",
                },
            ]);

            expect(listener).toHaveBeenCalledWith(expect.objectContaining({ _id: "h:chunk" }));
        });

        it("does not advance the received cursor when a chunk cannot be committed", async () => {
            const bulkDocs = vi.spyOn(localDB, "bulkDocs").mockRejectedValueOnce(new Error("synthetic write failure"));

            await expect(
                core.processDocuments([
                    {
                        _id: "h:chunk" as DocumentID,
                        _rev: "1-chunk",
                        type: "leaf",
                        data: "chunk-data",
                    },
                ])
            ).resolves.toBe(false);

            expect(bulkDocs).toHaveBeenCalledOnce();
            expect(checkpointState.knownIDs.size).toBe(0);
        });
    });

    describe("abortStaleRemoteRequests", () => {
        it("delegates to storage which can abort its requests", () => {
            const abortRequestsStartedBefore = vi.fn(() => 2);
            mockStorage.abortRequestsStartedBefore = abortRequestsStartedBefore;

            expect(core.abortStaleRemoteRequests(1_234)).toBe(2);
            expect(abortRequestsStartedBefore).toHaveBeenCalledWith(1_234);
        });

        it("reports nothing aborted when storage cannot abort requests", () => {
            expect(core.abortStaleRemoteRequests(1_234)).toBe(0);
        });
    });
});
