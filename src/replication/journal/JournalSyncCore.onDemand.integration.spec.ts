import { afterEach, describe, expect, it } from "vitest";
import { randomUUID } from "node:crypto";
import { reactiveSource } from "octagonal-wheels/dataobject/reactive";
import { MinioStorageAdapter } from "./objectstore/MinioStorageAdapter";
import { chunk, closeDevices, createDevice, note } from "./journalSpecHarness.spec";
import type { LiveSyncJournalReplicatorEnv } from "./LiveSyncJournalReplicatorEnv";
import {
    DOCID_JOURNAL_SYNC_PARAMETERS,
    E2EEAlgorithms,
    ProtocolVersions,
    type BucketSyncSetting,
    type FilePathWithPrefix,
    type MetaEntry,
} from "@lib/common/types";

afterEach(closeDevices);
describe("on-demand authenticated object-storage integration", () => {
    it("fetches exact encrypted binary content over S3 HTTP without a receiver content database", async () => {
        const settings = {
            endpoint: process.env.minioEndpoint ?? "http://127.0.0.1:9000",
            accessKey: process.env.accessKey ?? "minioadmin",
            secretKey: process.env.secretKey ?? "minioadmin",
            bucket: process.env.bucketName ?? "livesync-test-bucket",
            region: "us-east-1",
            bucketPrefix: `jit-integration-${randomUUID()}/`,
            forcePathStyle: true,
            useCustomRequestHandler: false,
            bucketCustomHeaders: "",
        } as BucketSyncSetting;
        const requests = reactiveSource(0),
            responses = reactiveSource(0);
        const env = {
            services: {
                API: { getCustomFetchHandler: () => undefined, requestCount: requests, responseCount: responses },
            },
        } as unknown as LiveSyncJournalReplicatorEnv;
        const storage = new MinioStorageAdapter(settings, env);
        try {
            expect(
                await storage.upload(
                    DOCID_JOURNAL_SYNC_PARAMETERS,
                    new TextEncoder().encode(
                        JSON.stringify({
                            protocolVersion: ProtocolVersions.ADVANCED_E2EE,
                            pbkdf2salt: btoa("synthetic-http-jit-salt"),
                        })
                    ),
                    "application/json"
                )
            ).toBe(true);
            const sender = createDevice("jit-http-sender", storage);
            Object.assign(sender.settings, settings, {
                encrypt: true,
                passphrase: "synthetic-http-jit-key",
                E2EEAlgorithm: E2EEAlgorithms.V2,
            });
            Object.assign(sender.host.settings, sender.settings);
            sender.core.applyNewConfig(sender.settings, sender.store, sender.env);
            const bytes = new Uint8Array([137, 80, 78, 71, 0, 255, 42]);
            const meta = {
                ...note("synthetic-binary", ["synthetic-binary-body"]),
                path: "Archive/example.png" as FilePathWithPrefix,
                type: "newnote",
                size: bytes.length,
            } as MetaEntry;
            await sender.db.bulkDocs([chunk("synthetic-binary-body", Buffer.from(bytes).toString("base64")), meta]);
            expect(await sender.core.sendLocalJournal()).toBe(true);
            const receiver = createDevice("jit-http-receiver", storage);
            Object.assign(receiver.settings, sender.settings);
            Object.assign(receiver.host.settings, sender.settings);
            receiver.core.applyNewConfig(receiver.settings, receiver.store, receiver.env);
            const checkpoint = structuredClone(receiver.checkpoint);
            const before = requests.value;
            expect(new Uint8Array(await receiver.core.fetchRemoteFile(meta, 1024))).toEqual(bytes);
            expect(requests.value).toBeGreaterThan(before);
            expect(responses.value).toBe(requests.value);
            expect((await receiver.db.info()).doc_count).toBe(0);
            expect(receiver.handedOn).toEqual([]);
            expect(receiver.checkpoint).toEqual(checkpoint);
        } finally {
            // This adapter is confined to this test's randomly allocated synthetic prefix.
            await storage.deleteFiles(await storage.listFiles(""));
        }
    });
});
