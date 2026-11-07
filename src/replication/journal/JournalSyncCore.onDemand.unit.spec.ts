import { afterEach, describe, expect, it, vi } from "vitest";
import {
    DOCID_JOURNAL_SYNC_PARAMETERS,
    E2EEAlgorithms,
    ProtocolVersions,
    type MetaEntry,
    type FilePathWithPrefix,
} from "@lib/common/types";
import { journalIndexKey } from "./JournalSyncTypes";
import { chunk, closeDevices, createDevice, createRemote, encoder, note } from "./journalSpecHarness.spec";

afterEach(closeDevices);

describe("authenticated on-demand journal reads", () => {
    it("cancels an in-flight file decode even when no network chunks are needed", async () => {
        const remote = createRemote();
        const receiver = createDevice("jit-empty-decode", remote.storage);
        const meta = { ...note("empty"), type: "plain", size: 0 } as MetaEntry;
        const reading = receiver.core.fetchRemoteFile(meta, 1024);
        receiver.core.requestStop();
        await expect(reading).rejects.toThrow("stopped");
    });
    it("allows a fresh authorised read after stopping while keeping an older read cancelled", async () => {
        const remote = createRemote();
        const sender = createDevice("jit-cancel-sender", remote.storage);
        await sender.db.bulkDocs([chunk("cancelled", "synthetic content")]);
        expect(await sender.core.sendLocalJournal()).toBe(true);
        const receiver = createDevice("jit-cancel-receiver", remote.storage);
        let enter!: () => void, release!: () => void;
        const entered = new Promise<void>((resolve) => {
            enter = resolve;
        });
        const blocked = new Promise<void>((resolve) => {
            release = resolve;
        });
        const original = remote.storage.downloadWithResult.getMockImplementation()!;
        let journals = 0;
        remote.storage.downloadWithResult.mockImplementation(async (key: string) => {
            const result = await original(key);
            if (!key.startsWith("_") && ++journals === 1) {
                enter();
                await blocked;
            }
            return result;
        });
        const oldRead = receiver.core.fetchRemoteChunks(["h:cancelled"]);
        void oldRead.catch(() => {});
        await entered;
        receiver.core.requestStop();
        const fresh = await receiver.core.fetchRemoteChunks(["h:cancelled"]);
        expect(fresh.map(({ data }) => data)).toEqual(["synthetic content"]);
        expect(receiver.core.requestedStop).toBe(true);
        expect((await receiver.core.prepareOnDemandChunkIndex()).journals).toBeGreaterThan(0);
        release();
        await expect(oldRead).rejects.toThrow("stopped");
        expect((await receiver.db.info()).doc_count).toBe(0);
    });

    it("reconstructs binary bytes from R2 while the receiver has no local chunks or content files", async () => {
        const remote = createRemote();
        const sender = createDevice("jit-binary-sender", remote.storage);
        const content = new Uint8Array([0, 255, 42, 13, 10, 128]);
        const encoded = Buffer.from(content).toString("base64");
        const meta = {
            ...note("binary", ["binary-data"]),
            path: "Archive/synthetic.png" as FilePathWithPrefix,
            type: "newnote",
            size: content.length,
        } as MetaEntry;
        await sender.db.bulkDocs([chunk("binary-data", encoded), meta]);
        expect(await sender.core.sendLocalJournal()).toBe(true);
        const receiver = createDevice("jit-binary-receiver", remote.storage);
        const received = await receiver.core.fetchRemoteFile(meta, 1024);
        expect(new Uint8Array(received)).toEqual(content);
        expect((await receiver.db.info()).doc_count).toBe(0);
        expect(receiver.handedOn).toEqual([]);
        await expect(receiver.core.fetchRemoteFile({ ...meta, size: 2 }, 1024)).rejects.toThrow("size");
        await expect(receiver.core.fetchRemoteFile(meta, 2)).rejects.toThrow("cannot");
    });

    it("fetches requested encrypted content into an empty receiver without replaying notes or advancing its checkpoint", async () => {
        const remote = createRemote();
        remote.put(
            DOCID_JOURNAL_SYNC_PARAMETERS,
            encoder.encode(
                JSON.stringify({
                    protocolVersion: ProtocolVersions.ADVANCED_E2EE,
                    pbkdf2salt: btoa("synthetic-jit-salt-for-a-fixture"),
                })
            )
        );
        const sender = createDevice("jit-sender", remote.storage);
        Object.assign(sender.settings, {
            encrypt: true,
            passphrase: "synthetic-jit-passphrase",
            E2EEAlgorithm: E2EEAlgorithms.V2,
        });
        sender.core.applyNewConfig(sender.settings, sender.store, sender.env);
        Object.assign(sender.host.settings, sender.settings);
        await sender.db.bulkDocs([
            chunk("wanted", "synthetic private content"),
            chunk("unrelated", "other content"),
            note("unrelated-note", ["unrelated"]),
        ]);
        expect(await sender.core.sendLocalJournal()).toBe(true);
        for (const key of remote.keys())
            expect(new TextDecoder().decode(remote.dataOf(key))).not.toContain("synthetic private content");

        // A legacy journal has no index. Preparation reads it through the same authenticated decryption path.
        await remote.storage.deleteFiles(remote.keys().map(journalIndexKey));
        const receiver = createDevice("jit-receiver", remote.storage);
        Object.assign(receiver.settings, sender.settings);
        receiver.core.applyNewConfig(receiver.settings, receiver.store, receiver.env);
        Object.assign(receiver.host.settings, receiver.settings);
        const prepared = await receiver.core.prepareOnDemandChunkIndex();
        expect(prepared.indexed).toBe(remote.keys().length);
        const before = structuredClone(receiver.checkpoint);
        remote.transfers.length = 0;
        const fetched = await receiver.core.fetchRemoteChunks(["h:wanted"]);
        expect(fetched.map(({ _id, data }) => [_id, data])).toEqual([["h:wanted", "synthetic private content"]]);
        expect((await receiver.db.info()).doc_count).toBe(0);
        expect(receiver.handedOn).toEqual([]);
        expect(receiver.checkpoint).toEqual(before);
        expect(remote.transfers.some((line) => line.startsWith("upload "))).toBe(false);
        expect(remote.transfers.filter((line) => line.startsWith("download ") && !line.includes("_"))).toHaveLength(1);
    });

    it("refuses encrypted content under the wrong key without producing chunks", async () => {
        const remote = createRemote();
        remote.put(
            DOCID_JOURNAL_SYNC_PARAMETERS,
            encoder.encode(
                JSON.stringify({
                    protocolVersion: ProtocolVersions.ADVANCED_E2EE,
                    pbkdf2salt: btoa("synthetic-jit-salt-for-a-fixture"),
                })
            )
        );
        const sender = createDevice("jit-protected-sender", remote.storage);
        Object.assign(sender.settings, {
            encrypt: true,
            passphrase: "synthetic-correct-passphrase",
            E2EEAlgorithm: E2EEAlgorithms.V2,
        });
        Object.assign(sender.host.settings, sender.settings);
        await sender.db.bulkDocs([chunk("protected", "synthetic secret")]);
        expect(await sender.core.sendLocalJournal()).toBe(true);
        const receiver = createDevice("jit-wrong-key", remote.storage);
        Object.assign(receiver.settings, sender.settings, { passphrase: "synthetic-wrong-passphrase" });
        Object.assign(receiver.host.settings, receiver.settings);
        await expect(receiver.core.fetchRemoteChunks(["h:protected"])).rejects.toThrow();
        expect((await receiver.db.info()).doc_count).toBe(0);
        expect(receiver.handedOn).toEqual([]);
    });
});
