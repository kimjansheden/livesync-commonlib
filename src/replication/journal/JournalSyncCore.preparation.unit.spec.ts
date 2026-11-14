import { afterEach, describe, expect, it, vi } from "vitest";
import { closeDevices, createDevice, createRemote, encoder, docsKey, journalOf } from "./journalSpecHarness.spec";
import type { IJournalStorage } from "./objectstore/JournalStorageAdapter";
import { REMOTE_CHANGE_MARKER_KEY } from "./JournalSyncTypes";
import { DEFAULT_SYNC_PARAMETERS, DOCID_JOURNAL_SYNC_PARAMETERS } from "@lib/common/types";
import { CHECKPOINT_WRITE_INTERVAL_MS } from "./JournalSyncCore";

afterEach(async () => {
    vi.useRealTimers();
    await closeDevices();
});
function fixture() {
    const remote = createRemote();
    remote.put(
        DOCID_JOURNAL_SYNC_PARAMETERS,
        encoder.encode(JSON.stringify({ ...DEFAULT_SYNC_PARAMETERS, pbkdf2salt: "ERITFBUWFxgZGhscHR4fIA==" }))
    );
    remote.put(REMOTE_CHANGE_MARKER_KEY, encoder.encode(JSON.stringify({ version: 1, token: "a".repeat(32) })));
    const device = createDevice("prepared-reader", ownedStorage(remote));
    device.core.now = () => 1_000;
    return { remote, device };
}
function ownedStorage(
    remote: ReturnType<typeof createRemote>,
    abort: (before: number) => number = () => 0
): IJournalStorage {
    return {
        ...remote.storage,
        createIndependentReader: () => ({ ...remote.storage, abortRequestsStartedBefore: abort }),
    };
}
describe("read-only startup listing preparation", () => {
    it.each(["known", "absent"])("invalidates an older epoch before preparing with an %s marker", async (marker) => {
        const { remote, device } = fixture();
        if (marker === "absent") remote.removeButKeepListed(REMOTE_CHANGE_MARKER_KEY);
        await device.core.prepareRemoteListing();
        remote.put(
            DOCID_JOURNAL_SYNC_PARAMETERS,
            encoder.encode(JSON.stringify({ ...DEFAULT_SYNC_PARAMETERS, pbkdf2salt: "AQIDBAUGBwgJCgsMDQ4PEA==" }))
        );
        const key = docsKey("a");
        remote.put(key, await journalOf());
        await device.core.prepareRemoteListing();
        expect(remote.storage.listObjectsInUploadOrder).toHaveBeenCalledTimes(2);
        await device.core.ensureCheckpointCachesAreFresh();
        await expect(device.core.receiveRemoteJournal()).resolves.toBe(true);
        expect(device.checkpoint.receivedFiles.has(key)).toBe(true);
    });
    it.each(["missing", "shared", "non-abortable", "throwing"])(
        "skips speculative work for a %s reader",
        async (kind) => {
            const remote = createRemote();
            const storage: IJournalStorage = { ...remote.storage };
            if (kind === "shared") storage.createIndependentReader = () => storage;
            if (kind === "non-abortable") storage.createIndependentReader = () => ({ ...remote.storage });
            if (kind === "throwing")
                storage.createIndependentReader = () => {
                    throw new Error("Synthetic unavailable reader");
                };
            const device = createDevice("unsupported-preparation", storage);
            vi.useFakeTimers();
            await device.core.prepareRemoteListing();
            await device.core.waitForTransfersToSettle();
            expect(remote.downloadWithResult).not.toHaveBeenCalled();
            expect(remote.storage.listObjectsInUploadOrder).not.toHaveBeenCalled();
            expect(vi.getTimerCount()).toBe(0);
        }
    );
    it("keeps the owned read through an explicitly retained startup transition", async () => {
        const { remote, device } = fixture();
        let finish!: () => void;
        remote.storage.listObjectsInUploadOrder.mockImplementationOnce(
            () =>
                new Promise((resolve) => {
                    finish = () => resolve(remote.groups());
                })
        );
        const preparing = device.core.prepareRemoteListing();
        await vi.waitFor(() => expect(remote.storage.listObjectsInUploadOrder).toHaveBeenCalledOnce());
        device.core.requestStop(true);
        finish();
        await preparing;
        await device.core.ensureCheckpointCachesAreFresh();
        await device.core.receiveRemoteJournal();
        expect(remote.storage.listObjectsInUploadOrder).toHaveBeenCalledOnce();
    });
    it.each(["unchanged", "marker", "epoch"])(
        "revalidates prepared metadata for %s before receiving",
        async (change) => {
            const { remote, device } = fixture();
            await device.core.prepareRemoteListing();
            if (change === "marker")
                remote.put(
                    REMOTE_CHANGE_MARKER_KEY,
                    encoder.encode(JSON.stringify({ version: 1, token: "b".repeat(32) }))
                );
            if (change === "epoch")
                remote.put(
                    DOCID_JOURNAL_SYNC_PARAMETERS,
                    encoder.encode(
                        JSON.stringify({ ...DEFAULT_SYNC_PARAMETERS, pbkdf2salt: "AQIDBAUGBwgJCgsMDQ4PEA==" })
                    )
                );
            await device.core.ensureCheckpointCachesAreFresh();
            await expect(device.core.receiveRemoteJournal()).resolves.toBe(true);
            expect(remote.storage.listObjectsInUploadOrder).toHaveBeenCalledTimes(change === "unchanged" ? 1 : 2);
        }
    );
    it("reads the listing and local checkpoint without changing progress or uploading", async () => {
        const { remote, device } = fixture();
        await device.core.prepareRemoteListing();
        expect(remote.storage.listObjectsInUploadOrder).toHaveBeenCalledOnce();
        await device.core.waitForTransfersToSettle();
        expect(device.store.get).toHaveBeenCalledExactlyOnceWith(`bucketsync-checkpoint-${device.core.hash}`);
        expect(device.store.set).not.toHaveBeenCalled();
        expect(remote.upload).not.toHaveBeenCalled();
        expect(remote.uploadBeside).not.toHaveBeenCalled();
        expect(device.saved).toEqual([]);
    });
    it("coalesces a pending preparation and drains it when requested", async () => {
        const { remote, device } = fixture();
        let finish!: () => void;
        remote.storage.listObjectsInUploadOrder.mockImplementationOnce(
            () =>
                new Promise((resolve) => {
                    finish = () => resolve(remote.groups());
                })
        );
        const first = device.core.prepareRemoteListing(),
            second = device.core.prepareRemoteListing();
        expect(first).toBe(second);
        await vi.waitFor(() => expect(remote.storage.listObjectsInUploadOrder).toHaveBeenCalledOnce());
        let settled = false;
        const waiting = device.core.waitForTransfersToSettle().then(() => {
            settled = true;
        });
        await Promise.resolve();
        expect(settled).toBe(false);
        finish();
        await Promise.all([first, waiting]);
        expect(settled).toBe(true);
    });
    it("aborts a pending listing on stop without treating it as completed progress", async () => {
        const { remote } = fixture();
        let fail!: (error: Error) => void;
        const abort = vi.fn(() => {
            fail(new Error("synthetic cancellation"));
            return 1;
        });
        const device = createDevice("cancelled-reader", ownedStorage(remote, abort));
        remote.storage.listObjectsInUploadOrder.mockImplementationOnce(
            () =>
                new Promise((_resolve, reject) => {
                    fail = reject;
                })
        );
        const preparing = device.core.prepareRemoteListing();
        const rejected = expect(preparing).rejects.toThrow("synthetic cancellation");
        await vi.waitFor(() => expect(remote.storage.listObjectsInUploadOrder).toHaveBeenCalledOnce());
        device.core.requestStop();
        await rejected;
        await device.core.waitForTransfersToSettle();
        expect(abort).toHaveBeenCalledOnce();
        expect(device.store.set).not.toHaveBeenCalled();
    });
    it("bounds an unfinished preparation and settles the started request", async () => {
        const { remote } = fixture();
        let fail!: (error: Error) => void;
        const abort = vi.fn(() => {
            fail(new Error("synthetic deadline"));
            return 1;
        });
        const device = createDevice("bounded-reader", ownedStorage(remote, abort));
        remote.storage.listObjectsInUploadOrder.mockImplementationOnce(
            () =>
                new Promise((_resolve, reject) => {
                    fail = reject;
                })
        );
        vi.useFakeTimers();
        const preparing = device.core.prepareRemoteListing();
        const rejected = expect(preparing).rejects.toThrow("synthetic deadline");
        await vi.advanceTimersByTimeAsync(0);
        expect(remote.storage.listObjectsInUploadOrder).toHaveBeenCalledOnce();
        await vi.advanceTimersByTimeAsync(9_999);
        expect(abort).not.toHaveBeenCalled();
        await vi.advanceTimersByTimeAsync(1);
        await rejected;
        expect(abort).toHaveBeenCalledOnce();
        expect(vi.getTimerCount()).toBe(0);
    });
    it("does not initialise an absent remote", async () => {
        const remote = createRemote(),
            device = createDevice("unconfigured-reader", ownedStorage(remote));
        await expect(device.core.prepareRemoteListing()).rejects.toThrow("Missing sync parameters");
        expect(remote.upload).not.toHaveBeenCalled();
        expect(remote.uploadBeside).not.toHaveBeenCalled();
        expect(remote.storage.listObjectsInUploadOrder).not.toHaveBeenCalled();
    });
});

describe("local checkpoint preparation during startup", () => {
    const key = (device: ReturnType<typeof createDevice>) => `bucketsync-checkpoint-${device.core.hash}`;
    async function prepared() {
        const f = fixture();
        await f.device.core.ensureCheckpointCachesAreFresh();
        await f.device.core.sync();
        vi.mocked(f.device.store.get).mockClear();
        vi.mocked(f.device.store.set).mockClear();
        await f.device.core.prepareRemoteListing();
        await f.device.core.waitForTransfersToSettle();
        return f;
    }
    const checkpointReads = (device: ReturnType<typeof createDevice>) =>
        vi.mocked(device.store.get).mock.calls.filter(([read]) => read === key(device)).length;

    it("starts the local read while the independent remote listing is pending", async () => {
        const { device, remote } = fixture();
        let finish!: () => void;
        remote.storage.listObjectsInUploadOrder.mockImplementationOnce(
            () =>
                new Promise((resolve) => {
                    finish = () => resolve(remote.groups());
                })
        );
        const preparing = device.core.prepareRemoteListing();
        await vi.waitFor(() => expect(remote.storage.listObjectsInUploadOrder).toHaveBeenCalledOnce());
        expect(checkpointReads(device)).toBe(1);
        expect(device.store.set).not.toHaveBeenCalled();
        finish();
        await preparing;
    });

    it("reuses the prepared checkpoint through the local check, epoch check, and transfer", async () => {
        const { device } = await prepared();
        await expect(device.core.hasUnsentLocalChanges()).resolves.toBe(false);
        await device.core.ensureCheckpointCachesAreFresh();
        await expect(device.core.sync()).resolves.toBe(true);
        expect(checkpointReads(device)).toBe(1);
        expect(device.store.set).not.toHaveBeenCalled();
        await device.core.hasUnsentLocalChanges();
        expect(checkpointReads(device)).toBe(2);
    });

    it("reads a reset by another client instead of the prepared history", async () => {
        const { device } = await prepared();
        await device.createPaneClient().resetCheckpointInfo();
        const before = checkpointReads(device);
        await device.core.hasUnsentLocalChanges();
        expect(checkpointReads(device)).toBe(before + 1);
        expect((await device.core.getCheckpointInfo()).resetGeneration).toBe(1);
    });

    it("discards an expired or backwards-clock preparation", async () => {
        for (const elapsed of [CHECKPOINT_WRITE_INTERVAL_MS, -1]) {
            const { device } = await prepared();
            device.core.now = () => 1_000 + elapsed;
            await device.core.hasUnsentLocalChanges();
            expect(checkpointReads(device)).toBe(2);
        }
    });

    it("detects replacement of the backing database behind the same store handle", async () => {
        const { device } = await prepared();
        Object.defineProperty(device.store, "db", { get: () => Promise.resolve({}) });
        device.entries.delete(key(device));
        await device.core.hasUnsentLocalChanges();
        expect(checkpointReads(device)).toBe(2);
        expect((await device.core.getCheckpointInfo()).knownIDs.size).toBe(0);
    });

    it.each(["stop", "cache reset", "changed configuration", "changed store"])(
        "discards preparation on %s",
        async (change) => {
            const { device } = await prepared();
            if (change === "stop") device.core.requestStop();
            if (change === "cache reset") device.core.resetAllCaches();
            if (change === "changed configuration")
                device.core.applyNewConfig(
                    { ...device.settings, bucket: "different-synthetic-bucket" },
                    device.store,
                    device.env
                );
            if (change === "changed store")
                device.core.applyNewConfig(device.settings, { ...device.store }, device.env);
            const before = checkpointReads(device);
            await device.core.hasUnsentLocalChanges();
            expect(checkpointReads(device)).toBe(before + 1);
        }
    );

    it("retains the checkpoint only through an explicitly retained startup transition", async () => {
        const { device } = await prepared();
        device.core.requestStop(true);
        await device.core.hasUnsentLocalChanges();
        expect(checkpointReads(device)).toBe(1);
    });

    it.each(["stop", "cache reset", "changed configuration"])(
        "prepares again after a completed preparation is cancelled by %s",
        async (change) => {
            const { device } = await prepared();
            if (change === "stop") device.core.requestStop();
            if (change === "cache reset") device.core.resetAllCaches();
            if (change === "changed configuration")
                device.core.applyNewConfig(
                    { ...device.settings, bucket: "another-synthetic-bucket" },
                    device.store,
                    device.env
                );
            const before = checkpointReads(device);
            await device.core.prepareRemoteListing();
            expect(checkpointReads(device)).toBe(before + 1);
            await device.core.hasUnsentLocalChanges();
            expect(checkpointReads(device)).toBe(before + 1);
        }
    );

    it("can prepare again after a speculative local read fails", async () => {
        const { device } = fixture();
        vi.mocked(device.store.get).mockRejectedValueOnce(new Error("Synthetic checkpoint read failure"));
        await device.core.prepareRemoteListing();
        await device.core.prepareRemoteListing();
        await device.core.hasUnsentLocalChanges();
        expect(checkpointReads(device)).toBe(2);
    });

    it("retries an unavailable speculative read in the ordinary cycle", async () => {
        const { device } = fixture();
        vi.mocked(device.store.get).mockRejectedValueOnce(new Error("Synthetic unavailable local checkpoint"));
        await device.core.prepareRemoteListing();
        await device.core.waitForTransfersToSettle();
        await expect(device.core.hasUnsentLocalChanges()).resolves.toBe(false);
        expect(checkpointReads(device)).toBe(2);
        expect(device.store.set).not.toHaveBeenCalled();
    });

    it("drains a cancelled local read and never reuses its late result", async () => {
        const { device } = fixture();
        let finish!: () => void;
        vi.mocked(device.store.get).mockImplementationOnce(
            () =>
                new Promise((resolve) => {
                    finish = () => resolve(undefined);
                })
        );
        let preparationSettled = false;
        const preparing = device.core.prepareRemoteListing().then(() => {
            preparationSettled = true;
        });
        await vi.waitFor(() => expect(checkpointReads(device)).toBe(1));
        expect(preparationSettled).toBe(false);
        device.core.requestStop();
        let settled = false;
        const waiting = device.core.waitForTransfersToSettle().then(() => {
            settled = true;
        });
        await Promise.resolve();
        expect(settled).toBe(false);
        finish();
        await Promise.all([preparing, waiting]);
        expect(preparationSettled).toBe(true);
        await device.core.hasUnsentLocalChanges();
        expect(checkpointReads(device)).toBe(2);
        expect(device.store.set).not.toHaveBeenCalled();
    });
});
