import { describe, expect, it, vi } from "vitest";
import { DEFAULT_SETTINGS, type ENSURE_DB_RESULT, type MetaEntry } from "@lib/common/types";
import { LiveSyncJournalReplicator } from "./LiveSyncJournalReplicator";
import type { LiveSyncJournalReplicatorEnv } from "./LiveSyncJournalReplicatorEnv";

function fixture(verdict: ENSURE_DB_RESULT) {
    const lifecycle = { unloaded: false, suspended: false };
    const settings = { ...DEFAULT_SETTINGS, versionUpFlash: "" };
    const env = {
        services: {
            setting: { currentSettings: () => settings },
            appLifecycle: { hasUnloaded: () => lifecycle.unloaded, isSuspended: () => lifecycle.suspended },
            replicator: {
                runBoundedRemoteActivity: async <T>(work: () => Promise<T>) => await work(),
                replicationStatics: { value: {} },
            },
        },
    } as unknown as LiveSyncJournalReplicatorEnv;
    const replicator = new LiveSyncJournalReplicator(env);
    const client = {
        isAvailable: vi.fn(async () => true),
        ensureCheckpointCachesAreFresh: vi.fn(async () => {}),
        fetchRemoteChunks: vi.fn(async () => []),
        fetchRemoteFile: vi.fn(async () => new ArrayBuffer(1)),
        prepareOnDemandChunkIndex: vi.fn(async () => ({ journals: 1, indexed: 0, chunks: 1 })),
        requestStop: vi.fn(),
    };
    vi.spyOn(replicator, "setupJournalSyncClient").mockReturnValue(client as never);
    vi.spyOn(replicator, "updateInfo").mockImplementation(() => {});
    const milestone = vi.spyOn(replicator, "ensureBucketIsCompatible").mockResolvedValue(verdict);
    return { replicator, client, milestone, lifecycle, settings };
}

describe("on-demand remote acceptance", () => {
    it.each(["NODE_LOCKED", "NODE_CLEANED", "INCOMPATIBLE"] as const)(
        "refuses all content and index operations when milestone returns %s",
        async (verdict) => {
            const f = fixture(verdict);
            expect(await f.replicator.fetchRemoteChunks(["h:synthetic"], false)).toBe(false);
            expect(await f.replicator.fetchRemoteFile({} as MetaEntry, 1024)).toBe(false);
            expect(await f.replicator.prepareOnDemandChunkIndex()).toBe(false);
            expect(f.milestone).toHaveBeenCalledTimes(3);
            expect(f.client.fetchRemoteChunks).not.toHaveBeenCalled();
            expect(f.client.fetchRemoteFile).not.toHaveBeenCalled();
            expect(f.client.prepareOnDemandChunkIndex).not.toHaveBeenCalled();
            expect(f.client.ensureCheckpointCachesAreFresh).not.toHaveBeenCalled();
        }
    );
    it("checks an accepted milestone without changing transfer checkpoints", async () => {
        const f = fixture("OK");
        expect(await f.replicator.fetchRemoteChunks(["h:synthetic"], false)).toEqual([]);
        expect(await f.replicator.fetchRemoteFile({} as MetaEntry, 1024)).toEqual(new ArrayBuffer(1));
        expect(await f.replicator.prepareOnDemandChunkIndex()).toEqual({ journals: 1, indexed: 0, chunks: 1 });
        expect(f.client.ensureCheckpointCachesAreFresh).not.toHaveBeenCalled();
    });
    it("does not resurrect a request whose compatibility preflight crossed suspension", async () => {
        const f = fixture("OK");
        f.milestone.mockImplementation(async () => {
            f.replicator.closeReplication();
            return "OK";
        });
        expect(await f.replicator.fetchRemoteChunks(["h:synthetic"], false)).toBe(false);
        expect(f.client.fetchRemoteChunks).not.toHaveBeenCalled();
    });
    it("blocks a pending local compatibility review and unloading", async () => {
        const f = fixture("OK");
        f.settings.versionUpFlash = "Review required";
        expect(await f.replicator.prepareOnDemandChunkIndex()).toBe(false);
        f.settings.versionUpFlash = "";
        f.lifecycle.unloaded = true;
        expect(await f.replicator.prepareOnDemandChunkIndex()).toBe(false);
        expect(f.milestone).not.toHaveBeenCalled();
    });
});
