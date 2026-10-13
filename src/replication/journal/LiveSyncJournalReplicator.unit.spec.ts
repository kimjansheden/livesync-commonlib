import { describe, expect, it, vi } from "vitest";
import {
    DEFAULT_SETTINGS,
    DEVICE_ID_PREFERRED,
    TweakValuesTemplate,
    type RemoteDBSettings,
} from "@lib/common/types.ts";
import { extractObject } from "@lib/common/utils.ts";
import type { RunningBuild } from "@lib/services/base/IService.ts";
import { LiveSyncAbstractReplicator } from "@lib/replication/LiveSyncAbstractReplicator.ts";
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
    function createReplicator(milestone: unknown, runningBuild?: RunningBuild) {
        const replicator = Object.create(LiveSyncJournalReplicator.prototype) as LiveSyncJournalReplicator;
        replicator.env = {
            services: {
                API: {
                    getAppVersion: () => "1.0.0",
                    getPluginVersion: () => "1.0.0",
                    ...(runningBuild ? { getRunningBuild: () => runningBuild } : {}),
                },
                vault: { vaultName: () => "synthetic-vault", getVaultName: () => "synthetic-vault" },
                setting: { currentSettings: () => ({ ...DEFAULT_SETTINGS }) },
            },
        } as unknown as ConstructorParameters<typeof LiveSyncJournalReplicator>[0];
        replicator.nodeid = "synthetic-node";
        const uploadJson = vi.fn().mockResolvedValue(true);
        const ensureCheckpointCachesAreFresh = vi.fn().mockResolvedValue(undefined);
        const sendLocalJournal = vi.fn().mockResolvedValue(true);
        const receiveRemoteJournal = vi.fn().mockResolvedValue(true);
        vi.spyOn(replicator, "setupJournalSyncClient").mockReturnValue({
            isAvailable: vi.fn().mockResolvedValue(true),
            ensureCheckpointCachesAreFresh,
            sendLocalJournal,
            receiveRemoteJournal,
            downloadJsonWithResult: vi.fn().mockResolvedValue(milestone),
            getCheckpointInfo: vi.fn().mockResolvedValue({ receivedFiles: new Set() }),
            uploadJson,
        } as never);
        return { replicator, uploadJson, ensureCheckpointCachesAreFresh, sendLocalJournal, receiveRemoteJournal };
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

    describe("of a milestone which holds this device's current entry", () => {
        const MINUTE = 60_000;
        const tweaks = extractObject(TweakValuesTemplate, DEFAULT_SETTINGS);
        /** The milestone as the object storage returns it, without a document revision. */
        const currentMilestone = (connectedAgo: number) => ({
            status: "available",
            value: {
                _id: "_00000000-milestone.json",
                type: "milestoneinfo",
                created: 1,
                locked: false,
                accepted_nodes: ["synthetic-node"],
                node_chunk_info: { "synthetic-node": { min: 0, max: 2, current: 2 } },
                node_info: {
                    "synthetic-node": {
                        app_version: "1.0.0",
                        plugin_version: "1.0.0",
                        vault_name: "synthetic-vault",
                        device_name: "synthetic-vault",
                        progress: "",
                        last_connected: Date.now() - connectedAgo,
                    },
                },
                tweak_values: { "synthetic-node": tweaks, [DEVICE_ID_PREFERRED]: tweaks },
            },
        });

        it("does not write it again in a cycle within ten minutes of the last write", async () => {
            const { replicator, uploadJson } = createReplicator(currentMilestone(9 * MINUTE));

            await expect(replicator.checkReplicationConnectivity(false)).resolves.toBe(true);

            expect(uploadJson).not.toHaveBeenCalled();
        });

        it("writes it at once when the host reports a running build which the entry does not hold", async () => {
            const build = { mainJsSha256: "c".repeat(64), releaseNumber: 12 };
            const { replicator, uploadJson } = createReplicator(currentMilestone(1 * MINUTE), build);

            await expect(replicator.checkReplicationConnectivity(false)).resolves.toBe(true);

            expect(uploadJson).toHaveBeenCalledWith(
                "_00000000-milestone.json",
                expect.objectContaining({
                    node_info: {
                        "synthetic-node": expect.objectContaining({
                            plugin_main_js_sha256: build.mainJsSha256,
                            plugin_release_number: build.releaseNumber,
                        }),
                    },
                })
            );
        });

        it("does not write it again for a running build which the entry already holds", async () => {
            const build = { mainJsSha256: "c".repeat(64), releaseNumber: 12 };
            const milestone = currentMilestone(1 * MINUTE);
            Object.assign(milestone.value.node_info["synthetic-node"], {
                plugin_main_js_sha256: build.mainJsSha256,
                plugin_release_number: build.releaseNumber,
            });
            const { replicator, uploadJson } = createReplicator(milestone, build);

            await expect(replicator.checkReplicationConnectivity(false)).resolves.toBe(true);

            expect(uploadJson).not.toHaveBeenCalled();
        });

        it("writes it again once the last write is older than ten minutes", async () => {
            const { replicator, uploadJson } = createReplicator(currentMilestone(11 * MINUTE));

            await expect(replicator.checkReplicationConnectivity(false)).resolves.toBe(true);

            expect(uploadJson).toHaveBeenCalledOnce();
        });

        it("checks fresh parameters on both direct send and direct receive routes", async () => {
            const { replicator, ensureCheckpointCachesAreFresh, sendLocalJournal, receiveRemoteJournal } =
                createReplicator(currentMilestone(1 * MINUTE));

            await expect(replicator.replicateAllToServer({} as RemoteDBSettings)).resolves.toBe(true);
            await expect(replicator.replicateAllFromServer({} as RemoteDBSettings)).resolves.toBe(true);

            expect(ensureCheckpointCachesAreFresh).toHaveBeenCalledTimes(2);
            expect(sendLocalJournal).toHaveBeenCalledOnce();
            expect(receiveRemoteJournal).toHaveBeenCalledOnce();
        });
    });
});

describe("LiveSyncJournalReplicator unsent local changes", () => {
    it.each([true, false])("reports %s as its journal client does", async (unsent) => {
        const replicator = Object.create(LiveSyncJournalReplicator.prototype) as LiveSyncJournalReplicator;
        vi.spyOn(replicator, "setupJournalSyncClient").mockReturnValue({
            hasUnsentLocalChanges: vi.fn().mockResolvedValue(unsent),
        } as never);

        await expect(replicator.hasUnsentLocalChanges()).resolves.toBe(unsent);
    });
});

describe("LiveSyncJournalReplicator content which is received later", () => {
    it.each([true, false])("reports %s as its journal client does, without creating a client", (answer) => {
        const replicator = Object.create(LiveSyncJournalReplicator.prototype) as LiveSyncJournalReplicator;
        const setupJournalSyncClient = vi.spyOn(replicator, "setupJournalSyncClient");
        const hasDeferredRemoteContent = vi.fn(() => answer);
        const takeDeferredReceiveContinuation = vi.fn(() => answer);
        replicator._client = { hasDeferredRemoteContent, takeDeferredReceiveContinuation } as never;

        expect(replicator.hasDeferredRemoteContent()).toBe(answer);
        expect(replicator.takeDeferredReceiveContinuation()).toBe(answer);

        expect(hasDeferredRemoteContent).toHaveBeenCalledOnce();
        expect(takeDeferredReceiveContinuation).toHaveBeenCalledOnce();
        expect(setupJournalSyncClient).not.toHaveBeenCalled();
    });

    it("cannot rule such content out before it has a journal client, and has no continuation", () => {
        const replicator = Object.create(LiveSyncJournalReplicator.prototype) as LiveSyncJournalReplicator;
        const setupJournalSyncClient = vi.spyOn(replicator, "setupJournalSyncClient");

        // Nothing has listed the remote yet, as with a client which has not synchronised.
        expect(replicator.hasDeferredRemoteContent()).toBe(true);
        expect(replicator.takeDeferredReceiveContinuation()).toBe(false);
        expect(setupJournalSyncClient).not.toHaveBeenCalled();
    });

    it("is none for a replicator which receives everything in one go", () => {
        const replicator = Object.create(LiveSyncAbstractReplicator.prototype) as LiveSyncAbstractReplicator;

        expect(replicator.hasDeferredRemoteContent()).toBe(false);
        expect(replicator.takeDeferredReceiveContinuation()).toBe(false);
    });

    it("lets its journal client ask the host whether such content may be received now", () => {
        const env = {
            services: {
                setting: { currentSettings: () => ({ ...DEFAULT_SETTINGS }) },
                keyValueDB: { simpleStore: {} },
            },
        } as unknown as ConstructorParameters<typeof LiveSyncJournalReplicator>[0];
        const replicator = new LiveSyncJournalReplicator(env);
        const client = { applyNewConfig: vi.fn(), mayReceiveBulkJournals: () => true };
        replicator._client = client as never;

        // Unless the host says otherwise, it may.
        expect(replicator.mayReceiveDeferredContent()).toBe(true);
        expect(replicator.client.mayReceiveBulkJournals()).toBe(true);

        // The host replaces its answer after the client was set up, and may change it at any time.
        let hidden = true;
        replicator.mayReceiveDeferredContent = () => !hidden;
        expect(client.mayReceiveBulkJournals()).toBe(false);
        hidden = false;
        expect(client.mayReceiveBulkJournals()).toBe(true);
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
