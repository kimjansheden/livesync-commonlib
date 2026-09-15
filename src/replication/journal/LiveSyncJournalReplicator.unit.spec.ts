import { describe, expect, it, vi } from "vitest";
import { DEFAULT_SETTINGS, type RemoteDBSettings } from "@lib/common/types.ts";
import { LiveSyncJournalReplicator } from "./LiveSyncJournalReplicator.ts";

describe("LiveSyncJournalReplicator initialisation", () => {
    it("does not access the local database while constructing a remote-only replicator", () => {
        const getLocalDatabase = vi.fn(() => {
            throw new Error("Local database is not ready yet.");
        });
        const env = {
            services: {
                database: {
                    get localDatabase() {
                        return getLocalDatabase();
                    },
                },
            },
        } as unknown as ConstructorParameters<typeof LiveSyncJournalReplicator>[0];

        expect(() => new LiveSyncJournalReplicator(env)).not.toThrow();
        expect(getLocalDatabase).not.toHaveBeenCalled();
    });
});

describe("LiveSyncJournalReplicator remote preferred tweak values", () => {
    function createReplicator(result: unknown) {
        const replicator = Object.create(LiveSyncJournalReplicator.prototype) as LiveSyncJournalReplicator;
        vi.spyOn(replicator, "setupJournalSyncClient").mockReturnValue({
            downloadJson: vi.fn().mockResolvedValue(false),
            downloadJsonWithResult: vi.fn().mockResolvedValue(result),
        } as never);
        return replicator;
    }

    it("distinguishes a missing milestone from an unavailable object store", async () => {
        const replicator = createReplicator({ status: "not-found" });

        await expect(replicator.getRemotePreferredTweakValues({} as RemoteDBSettings)).resolves.toEqual({
            status: "not-configured",
            reason: "milestone-missing",
        });
    });

    it("reports an object-store read failure as unavailable", async () => {
        const failure = new Error("network failed");
        const replicator = createReplicator({ status: "unavailable", error: failure });

        await expect(replicator.getRemotePreferredTweakValues({} as RemoteDBSettings)).resolves.toEqual({
            status: "unavailable",
            error: failure,
        });
    });

    it("distinguishes a milestone without preferred values", async () => {
        const replicator = createReplicator({ status: "available", value: { tweak_values: {} } });

        await expect(replicator.getRemotePreferredTweakValues({} as RemoteDBSettings)).resolves.toEqual({
            status: "not-configured",
            reason: "preferred-values-missing",
        });
    });

    it("returns available preferred values explicitly", async () => {
        const values = { encrypt: true };
        const replicator = createReplicator({
            status: "available",
            value: { tweak_values: { PREFERRED: values } },
        });

        await expect(replicator.getRemotePreferredTweakValues({} as RemoteDBSettings)).resolves.toEqual({
            status: "available",
            values,
        });
    });
});

describe("LiveSyncJournalReplicator milestone check", () => {
    function createReplicator(milestone: unknown) {
        const replicator = Object.create(LiveSyncJournalReplicator.prototype) as LiveSyncJournalReplicator;
        replicator.env = {
            services: {
                API: { getAppVersion: () => "1.0.0", getPluginVersion: () => "1.0.0" },
                vault: { vaultName: () => "synthetic-vault", getVaultName: () => "synthetic-vault" },
                setting: { currentSettings: () => ({ ...DEFAULT_SETTINGS }) },
            },
        } as unknown as ConstructorParameters<typeof LiveSyncJournalReplicator>[0];
        replicator.nodeid = "synthetic-node";
        const uploadJson = vi.fn().mockResolvedValue(true);
        vi.spyOn(replicator, "setupJournalSyncClient").mockReturnValue({
            isAvailable: vi.fn().mockResolvedValue(true),
            ensureCheckpointCachesAreFresh: vi.fn().mockResolvedValue(undefined),
            downloadJsonWithResult: vi.fn().mockResolvedValue(milestone),
            getCheckpointInfo: vi.fn().mockResolvedValue({ receivedFiles: new Set() }),
            uploadJson,
        } as never);
        return { replicator, uploadJson };
    }

    it("stops without writing when the remote milestone cannot be read", async () => {
        const { replicator, uploadJson } = createReplicator({ status: "unavailable", error: new Error("aborted") });

        await expect(replicator.checkReplicationConnectivity(false)).resolves.toBe(false);
        expect(uploadJson).not.toHaveBeenCalled();
    });

    it("neither locks nor resolves a remote whose milestone cannot be read", async () => {
        const { replicator, uploadJson } = createReplicator({ status: "unavailable", error: new Error("aborted") });

        await expect(replicator.markRemoteLocked({} as RemoteDBSettings, false, false)).rejects.toThrow(
            "could not be read"
        );
        await expect(replicator.markRemoteResolved({} as RemoteDBSettings)).rejects.toThrow("could not be read");
        expect(uploadJson).not.toHaveBeenCalled();
    });

    it("keeps other devices' entries when resolving this device", async () => {
        const { replicator, uploadJson } = createReplicator({
            status: "available",
            value: {
                _id: "_00000000-milestone.json",
                type: "milestoneinfo",
                created: 1,
                locked: true,
                accepted_nodes: ["synthetic-other-node"],
                node_chunk_info: { "synthetic-other-node": { min: 0, max: 2 } },
                node_info: {},
                tweak_values: { "synthetic-other-node": {} },
            },
        });

        await replicator.markRemoteResolved({} as RemoteDBSettings);

        expect(uploadJson).toHaveBeenCalledWith(
            "_00000000-milestone.json",
            expect.objectContaining({
                accepted_nodes: ["synthetic-other-node", "synthetic-node"],
                tweak_values: { "synthetic-other-node": {} },
            })
        );
    });

    it("creates the milestone when the remote has none", async () => {
        const { replicator, uploadJson } = createReplicator({ status: "not-found" });

        await expect(replicator.checkReplicationConnectivity(false)).resolves.toBe(true);
        expect(uploadJson).toHaveBeenCalledWith(
            "_00000000-milestone.json",
            expect.objectContaining({ accepted_nodes: ["synthetic-node"], locked: false })
        );
    });
});

describe("LiveSyncJournalReplicator replication result", () => {
    it.each([
        [true, true],
        [false, false],
    ])("reports a synchronisation result of %s as %s", async (synchronised, expected) => {
        const replicator = Object.create(LiveSyncJournalReplicator.prototype) as LiveSyncJournalReplicator;
        vi.spyOn(replicator, "checkReplicationConnectivity").mockResolvedValue(true);
        vi.spyOn(replicator, "setupJournalSyncClient").mockReturnValue({
            sync: vi.fn().mockResolvedValue(synchronised),
        } as never);

        await expect(replicator.openReplication({} as RemoteDBSettings, false, false)).resolves.toBe(expected);
    });
});

describe("LiveSyncJournalReplicator remote reset", () => {
    it("aborts and waits for running transfers before it clears the bucket", async () => {
        const replicator = Object.create(LiveSyncJournalReplicator.prototype) as LiveSyncJournalReplicator;
        const calls: string[] = [];
        let finishTransfer!: () => void;
        const client = {
            requestStop: vi.fn(() => calls.push("stop")),
            abortStaleRemoteRequests: vi.fn((startedBefore: number) => {
                calls.push(`abort:${startedBefore}`);
                return 1;
            }),
            waitForTransfersToSettle: vi.fn(async () => {
                calls.push("wait");
                await new Promise<void>((resolve) => (finishTransfer = resolve));
                calls.push("settled");
            }),
            resetBucket: vi.fn(async () => {
                calls.push("reset");
                return true;
            }),
        };
        vi.spyOn(replicator, "setupJournalSyncClient").mockReturnValue(client as never);
        replicator.updateInfo = vi.fn();
        vi.spyOn(replicator, "tryCreateRemoteDatabase").mockResolvedValue(undefined);

        const reset = replicator.tryResetRemoteDatabase({} as RemoteDBSettings);
        await vi.waitFor(() => expect(calls).toContain("wait"));
        expect(client.resetBucket).not.toHaveBeenCalled();
        finishTransfer();
        await reset;

        expect(calls).toEqual(["stop", `abort:${Number.POSITIVE_INFINITY}`, "wait", "settled", "reset"]);
    });
});

describe("LiveSyncJournalReplicator stale remote requests", () => {
    it("aborts stale requests through the existing journal client", () => {
        const replicator = Object.create(LiveSyncJournalReplicator.prototype) as LiveSyncJournalReplicator;
        const abortStaleRemoteRequests = vi.fn(() => 1);
        replicator._client = { abortStaleRemoteRequests } as never;

        expect(replicator.abortStaleRemoteRequests(5_000)).toBe(1);
        expect(abortStaleRemoteRequests).toHaveBeenCalledWith(5_000);
    });

    it("does not create a journal client only to abort requests", () => {
        const replicator = Object.create(LiveSyncJournalReplicator.prototype) as LiveSyncJournalReplicator;
        const setupJournalSyncClient = vi.spyOn(replicator, "setupJournalSyncClient");

        expect(replicator.abortStaleRemoteRequests(5_000)).toBe(0);
        expect(setupJournalSyncClient).not.toHaveBeenCalled();
    });
});
