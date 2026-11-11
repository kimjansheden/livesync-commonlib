import { afterEach, describe, expect, it, vi } from "vitest";
import { closeDevices, createDevice, createRemote, encoder } from "./journalSpecHarness.spec";
import { REMOTE_CHANGE_MARKER_KEY } from "./JournalSyncTypes";
import { DEFAULT_SYNC_PARAMETERS, DOCID_JOURNAL_SYNC_PARAMETERS } from "@lib/common/types";

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
    const device = createDevice("prepared-reader", remote.storage);
    device.core.now = () => 1_000;
    return { remote, device };
}
describe("read-only startup listing preparation", () => {
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
    it("reads the complete ordered listing without touching local progress or uploading", async () => {
        const { remote, device } = fixture();
        await device.core.prepareRemoteListing();
        expect(remote.storage.listObjectsInUploadOrder).toHaveBeenCalledOnce();
        expect(device.store.get).not.toHaveBeenCalled();
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
        const device = createDevice("cancelled-reader", { ...remote.storage, abortRequestsStartedBefore: abort });
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
        const device = createDevice("bounded-reader", { ...remote.storage, abortRequestsStartedBefore: abort });
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
        await vi.advanceTimersByTimeAsync(5_000);
        await rejected;
        expect(abort).toHaveBeenCalledOnce();
        expect(vi.getTimerCount()).toBe(0);
    });
    it("does not initialise an absent remote", async () => {
        const remote = createRemote(),
            device = createDevice("unconfigured-reader", remote.storage);
        await expect(device.core.prepareRemoteListing()).rejects.toThrow("Missing sync parameters");
        expect(remote.upload).not.toHaveBeenCalled();
        expect(remote.uploadBeside).not.toHaveBeenCalled();
        expect(remote.storage.listObjectsInUploadOrder).not.toHaveBeenCalled();
    });
});
