import { afterEach, describe, expect, it, vi } from "vitest";
import { ReplicationService, type ReplicationServiceDependencies } from "./ReplicationService.ts";
import { ServiceContext } from "./ServiceBase.ts";
import { REMOTE_COUCHDB, REMOTE_MINIO } from "@lib/common/types";

class TestReplicationService extends ReplicationService<ServiceContext> {}

function createReplicationQueueStore(initial?: unknown) {
    let value: unknown = initial;
    return {
        get: vi.fn(async () => structuredClone(value)),
        atomicUpdate: vi.fn(async (_key: string, change: (current: unknown) => { value: unknown; result: unknown }) => {
            const next = change(structuredClone(value));
            value = structuredClone(next.value);
            return next.result;
        }),
    };
}

describe("ReplicationService activity boundary", () => {
    const createDependencies = () => {
        const openReplication = vi.fn().mockResolvedValue(true);
        const runFiniteReplicationActivity = vi.fn(async (task: () => unknown) => await task());
        const onResumed = { addHandler: vi.fn() };
        const onLoaded = { addHandler: vi.fn() };
        const getUnresolvedMessages = Object.assign(vi.fn().mockResolvedValue([]), {
            addHandler: vi.fn(),
        });
        // As in the lifecycle service, an unloaded host is never ready.
        const lifecycle = { unloaded: false };
        const dependencies = {
            APIService: { isOnline: true, addLog: vi.fn(), isMobile: vi.fn(() => false) },
            appLifecycleService: {
                isReady: () => !lifecycle.unloaded,
                hasUnloaded: () => lifecycle.unloaded,
                getUnresolvedMessages,
                onLoaded,
                onResumed,
            },
            databaseService: {},
            fileProcessingService: {
                commitPendingFileEvents: vi.fn().mockResolvedValue(true),
            },
            replicationQueueStore: createReplicationQueueStore(),
            replicatorService: {
                getActiveReplicator: () => ({ openReplication }),
                runFiniteReplicationActivity,
            },
            settingService: {
                currentSettings: () => ({ versionUpFlash: "" }),
            },
        } as unknown as ReplicationServiceDependencies;

        return { dependencies, lifecycle, onLoaded, onResumed, openReplication, runFiniteReplicationActivity };
    };

    it("counts a cycle from its readiness checks on, before it counts as finite replication", async () => {
        const { dependencies, runFiniteReplicationActivity } = createDependencies();
        const service = new TestReplicationService(new ServiceContext(), dependencies);
        const counted: Record<string, number> = {};
        service.onCheckReplicationReady.addHandler(async () => {
            counted.policyCheck = service.replicationCycleCount.value;
            return true;
        });
        vi.mocked(dependencies.fileProcessingService.commitPendingFileEvents).mockImplementation(async () => {
            counted.commit = service.replicationCycleCount.value;
            return true;
        });
        service.onBeforeReplicate.addHandler(async () => {
            counted.preparation = service.replicationCycleCount.value;
            counted.finiteActivitiesSoFar = runFiniteReplicationActivity.mock.calls.length;
            return true;
        });
        expect(service.replicationCycleCount.value).toBe(0);

        await expect(service.replicate()).resolves.toBe(true);

        expect(counted.policyCheck).toBeGreaterThan(0);
        expect(counted.commit).toBeGreaterThan(0);
        expect(counted.preparation).toBeGreaterThan(0);
        expect(counted.finiteActivitiesSoFar).toBe(0);
        expect(service.replicationCycleCount.value).toBe(0);
    });

    it("counts a readiness check which a caller makes on its own, and stops counting when it fails", async () => {
        const { dependencies } = createDependencies();
        const service = new TestReplicationService(new ServiceContext(), dependencies);
        let counted = 0;
        service.onBeforeReplicate.addHandler(async () => {
            counted = service.replicationCycleCount.value;
            return false;
        });

        await expect(service.isReplicationReady()).resolves.toBe(false);

        expect(counted).toBe(1);
        expect(service.replicationCycleCount.value).toBe(0);
    });

    it("counts a cycle while it waits out the minimum interval between replications", async () => {
        const { dependencies } = createDependencies();
        Object.assign(dependencies.settingService, {
            currentSettings: () => ({ versionUpFlash: "", syncMinimumInterval: 300 }),
        });
        const service = new TestReplicationService(new ServiceContext(), dependencies);
        service.previousReplicated = Date.now();

        const waiting = service.replicateByEvent();
        await vi.waitFor(() => expect(service.replicationCycleCount.value).toBeGreaterThan(0));
        expect(dependencies.fileProcessingService.commitPendingFileEvents).not.toHaveBeenCalled();

        await expect(waiting).resolves.toBe(true);
        expect(service.replicationCycleCount.value).toBe(0);
    });

    it.each<[string, (service: TestReplicationService) => Promise<unknown>]>([
        ["an ordinary cycle", (service) => service.replicate()],
        ["an event-triggered cycle", (service) => service.replicateByEvent()],
    ])("reports a peak of exactly one for %s, whatever the nesting", async (_, run) => {
        const { dependencies } = createDependencies();
        const service = new TestReplicationService(new ServiceContext(), dependencies);
        let peak = 0;
        const sample = () => (peak = Math.max(peak, service.replicationCycleCount.value));
        // Sampled at the deepest points a cycle reaches: the readiness check (which re-enters the counter), the
        // commit, and the pre-flight handler.
        service.onCheckReplicationReady.addHandler(async () => (sample(), true));
        service.onBeforeReplicate.addHandler(async () => (sample(), true));
        vi.mocked(dependencies.fileProcessingService.commitPendingFileEvents).mockImplementation(
            async () => (sample(), true)
        );

        await run(service);

        expect(peak).toBe(1);
        expect(service.replicationCycleCount.value).toBe(0);
    });

    describe("after the host has unloaded", () => {
        it("neither records a request nor opens a replication", async () => {
            const { dependencies, lifecycle, openReplication } = createDependencies();
            const service = new TestReplicationService(new ServiceContext(), dependencies);
            lifecycle.unloaded = true;

            await expect(service.replicate(true)).resolves.toBe(false);
            await expect(service.replicateByEvent(true)).resolves.toBe(false);

            expect(dependencies.replicationQueueStore.atomicUpdate).not.toHaveBeenCalled();
            expect(openReplication).not.toHaveBeenCalled();
        });

        it("ends a cycle which was waiting for its turn when the host unloaded", async () => {
            const { dependencies, lifecycle, openReplication, runFiniteReplicationActivity } = createDependencies();
            const commitPendingFileEvents = dependencies.fileProcessingService.commitPendingFileEvents;
            Object.assign(dependencies.settingService, {
                currentSettings: () => ({ versionUpFlash: "", syncMinimumInterval: 50 }),
            });
            const service = new TestReplicationService(new ServiceContext(), dependencies);
            service.previousReplicated = Date.now();

            const waiting = service.replicateByEvent();
            lifecycle.unloaded = true;

            await expect(waiting).resolves.toBe(false);
            expect(commitPendingFileEvents).not.toHaveBeenCalled();
            expect(runFiniteReplicationActivity).not.toHaveBeenCalled();
            expect(openReplication).not.toHaveBeenCalled();
        });

        type Arrange = (
            service: TestReplicationService,
            unload: () => void,
            dependencies: ReplicationServiceDependencies
        ) => void;
        const unloadDuring: [string, Arrange][] = [
            [
                "a readiness policy check",
                (service, unload) =>
                    void service.onCheckReplicationReady.addHandler(async () => {
                        unload();
                        return true;
                    }),
            ],
            [
                "the commit of pending file events",
                (_service, unload, dependencies) =>
                    void vi
                        .mocked(dependencies.fileProcessingService.commitPendingFileEvents)
                        .mockImplementation(async () => {
                            unload();
                            return true;
                        }),
            ],
            [
                "a handler which prepares the replication",
                (service, unload) =>
                    void service.onBeforeReplicate.addHandler(async () => {
                        unload();
                        return true;
                    }),
            ],
        ];

        it.each(unloadDuring)("opens no replication when the host unloads during %s", async (_, arrange) => {
            const { dependencies, lifecycle, openReplication, runFiniteReplicationActivity } = createDependencies();
            const service = new TestReplicationService(new ServiceContext(), dependencies);
            const failed = vi.fn(async () => true);
            service.onReplicationFailed.addHandler(failed);
            arrange(service, () => (lifecycle.unloaded = true), dependencies);

            await expect(service.replicate(true)).resolves.toBe(false);

            expect(runFiniteReplicationActivity).not.toHaveBeenCalled();
            expect(openReplication).not.toHaveBeenCalled();
            expect(failed).not.toHaveBeenCalled();
        });

        it.each(unloadDuring)(
            "answers a caller of the readiness check that it is not ready when the host unloads during %s",
            async (_, arrange) => {
                const { dependencies, lifecycle } = createDependencies();
                const service = new TestReplicationService(new ServiceContext(), dependencies);
                // The lifecycle of this host stays ready, so only the check for the unload can answer.
                Object.assign(dependencies.appLifecycleService, { isReady: () => true });
                arrange(service, () => (lifecycle.unloaded = true), dependencies);

                await expect(service.isReplicationReady(true)).resolves.toBe(false);
            }
        );

        it("does not go on to the next check when the host unloads during one", async () => {
            const { dependencies, lifecycle } = createDependencies();
            const service = new TestReplicationService(new ServiceContext(), dependencies);
            const prepare = vi.fn(async () => true);
            service.onBeforeReplicate.addHandler(prepare);
            service.onCheckReplicationReady.addHandler(async () => {
                lifecycle.unloaded = true;
                return true;
            });

            await expect(service.replicate()).resolves.toBe(false);

            expect(dependencies.fileProcessingService.commitPendingFileEvents).not.toHaveBeenCalled();
            expect(prepare).not.toHaveBeenCalled();
        });

        it("does not report a replication which the unload ended as failed", async () => {
            const { dependencies, lifecycle, openReplication } = createDependencies();
            const service = new TestReplicationService(new ServiceContext(), dependencies);
            const failed = vi.fn(async () => true);
            service.onReplicationFailed.addHandler(failed);
            openReplication.mockImplementation(async () => {
                lifecycle.unloaded = true;
                return false;
            });

            await expect(service.replicate(true)).resolves.toBe(false);

            expect(openReplication).toHaveBeenCalledOnce();
            expect(failed).not.toHaveBeenCalled();
        });

        it("performs no replication request for a caller which did not check readiness", async () => {
            const { dependencies, lifecycle, openReplication } = createDependencies();
            const service = new TestReplicationService(new ServiceContext(), dependencies);
            const failed = vi.fn(async () => true);
            service.onReplicationFailed.addHandler(failed);
            lifecycle.unloaded = true;

            await expect(service.performReplication(true)).resolves.toBe(false);

            expect(openReplication).not.toHaveBeenCalled();
            expect(failed).not.toHaveBeenCalled();
        });

        it("does not resume pending replication", async () => {
            // A cycle which could not start leaves its request pending for the next load or resume.
            const interrupted = createDependencies();
            Object.assign(interrupted.dependencies.APIService, { isOnline: false });
            await new TestReplicationService(new ServiceContext(), interrupted.dependencies).replicate();
            const unloaded = createDependencies();
            unloaded.dependencies.replicationQueueStore = interrupted.dependencies.replicationQueueStore;
            new TestReplicationService(new ServiceContext(), unloaded.dependencies);
            const resumeHandler = unloaded.onResumed.addHandler.mock.calls[0][0] as () => Promise<boolean>;
            vi.mocked(unloaded.dependencies.replicationQueueStore.atomicUpdate).mockClear();
            unloaded.lifecycle.unloaded = true;

            await expect(resumeHandler()).resolves.toBe(true);
            await new Promise((resolve) => setTimeout(resolve, 20));

            expect(unloaded.dependencies.replicationQueueStore.atomicUpdate).not.toHaveBeenCalled();
            expect(unloaded.openReplication).not.toHaveBeenCalled();
        });
    });

    it("runs a ready one-shot replication through the bounded remote activity boundary", async () => {
        const { dependencies, openReplication, runFiniteReplicationActivity } = createDependencies();
        const service = new TestReplicationService(new ServiceContext(), dependencies);

        await expect(service.replicate(true)).resolves.toBe(true);

        expect(runFiniteReplicationActivity).toHaveBeenCalledOnce();
        expect(runFiniteReplicationActivity).toHaveBeenCalledWith(expect.any(Function), {
            label: "replication",
        });
        expect(openReplication).toHaveBeenCalledOnce();
    });

    it("does not start an activity while replication readiness checks fail", async () => {
        const { dependencies, openReplication, runFiniteReplicationActivity } = createDependencies();
        Object.assign(dependencies.APIService, { isOnline: false });
        const service = new TestReplicationService(new ServiceContext(), dependencies);

        await expect(service.replicate(true)).resolves.toBe(false);

        expect(runFiniteReplicationActivity).not.toHaveBeenCalled();
        expect(openReplication).not.toHaveBeenCalled();
    });

    it("honours reusable replication policy checks before starting standard replication", async () => {
        const { dependencies, openReplication, runFiniteReplicationActivity } = createDependencies();
        const service = new TestReplicationService(new ServiceContext(), dependencies);
        const policyCheck = vi.fn(async () => false);
        service.onCheckReplicationReady.addHandler(policyCheck);

        await expect(service.replicate(true)).resolves.toBe(false);

        expect(policyCheck).toHaveBeenCalledWith(true);
        expect(runFiniteReplicationActivity).not.toHaveBeenCalled();
        expect(openReplication).not.toHaveBeenCalled();
    });

    it("ends the bounded activity before handling a failed replication", async () => {
        const { dependencies, openReplication } = createDependencies();
        const calls: string[] = [];
        openReplication.mockResolvedValue(false);
        (dependencies.replicatorService as any).runFiniteReplicationActivity = vi.fn(async (task: () => unknown) => {
            calls.push("activity-started");
            try {
                return await task();
            } finally {
                calls.push("activity-ended");
            }
        });
        const service = new TestReplicationService(new ServiceContext(), dependencies);
        service.onReplicationFailed.addHandler(async () => {
            calls.push("failure-handled");
            return false;
        });

        await expect(service.replicate(true)).resolves.toBe(false);

        expect(calls).toEqual(["activity-started", "activity-ended", "failure-handled"]);
    });

    it("preserves failure handling for direct performReplication callers", async () => {
        const { dependencies, openReplication, runFiniteReplicationActivity } = createDependencies();
        openReplication.mockResolvedValue(false);
        const service = new TestReplicationService(new ServiceContext(), dependencies);
        const handleFailure = vi.fn(async () => false);
        service.onReplicationFailed.addHandler(handleFailure);

        await expect(service.performReplication(true)).resolves.toBe(false);

        expect(handleFailure).toHaveBeenCalledWith(true);
        expect(runFiniteReplicationActivity).not.toHaveBeenCalled();
    });

    it("runs a second finite cycle when an event arrives during active replication", async () => {
        const { dependencies, openReplication } = createDependencies();
        let releaseFirst!: () => void;
        const firstCycle = new Promise<void>((resolve) => {
            releaseFirst = resolve;
        });
        let calls = 0;
        openReplication.mockImplementation(async () => {
            calls++;
            if (calls === 1) await firstCycle;
            return true;
        });
        const service = new TestReplicationService(new ServiceContext(), dependencies);

        const first = service.replicateByEvent();
        await vi.waitFor(() => expect(openReplication).toHaveBeenCalledOnce());
        const second = service.replicateByEvent();
        releaseFirst();

        await expect(Promise.all([first, second])).resolves.toEqual([true, true]);
        expect(openReplication).toHaveBeenCalledTimes(2);
    });

    it("resumes a durable pending generation when the restarted host finishes loading", async () => {
        const replicationQueueStore = createReplicationQueueStore();
        const first = createDependencies();
        first.dependencies.replicationQueueStore = replicationQueueStore;
        Object.assign(first.dependencies.APIService, { isOnline: false });
        const interrupted = new TestReplicationService(new ServiceContext(), first.dependencies);
        await expect(interrupted.replicate()).resolves.toBe(false);

        const restarted = createDependencies();
        restarted.dependencies.replicationQueueStore = replicationQueueStore;
        new TestReplicationService(new ServiceContext(), restarted.dependencies);
        const resumeHandler = restarted.onLoaded.addHandler.mock.calls[0][0] as () => Promise<boolean>;

        await expect(resumeHandler()).resolves.toBe(true);
        await vi.waitFor(() => expect(restarted.openReplication).toHaveBeenCalledOnce());
    });

    it("lets later resume handlers run while a pending cycle is still unwinding", async () => {
        const { dependencies, onResumed, openReplication } = createDependencies();
        let releaseCycle!: () => void;
        openReplication.mockImplementation(
            () =>
                new Promise<boolean>((resolve) => {
                    releaseCycle = () => resolve(true);
                })
        );
        const service = new TestReplicationService(new ServiceContext(), dependencies);
        const running = service.replicate();
        await vi.waitFor(() => expect(openReplication).toHaveBeenCalledOnce());
        const resumeHandler = onResumed.addHandler.mock.calls[0][0] as () => Promise<boolean>;

        await expect(resumeHandler()).resolves.toBe(true);

        releaseCycle();
        await expect(running).resolves.toBe(true);
    });

    describe("a lease left by a process which was closed during a cycle", () => {
        const LEASE_TTL_MS = 45_000;
        /** The queue as the closed process left it: its generation pending and its lease still unexpired. */
        const leftQueue = () => ({
            schema: 1,
            requestedGeneration: 1,
            completedGeneration: 0,
            fencingToken: 3,
            lease: { holderId: "closed-process", fencingToken: 3, expiresAt: Date.now() + LEASE_TTL_MS - 1_000 },
        });

        function restart(isMobile: boolean, remoteType = REMOTE_MINIO) {
            const restarted = createDependencies();
            restarted.dependencies.replicationQueueStore = createReplicationQueueStore(leftQueue());
            Object.assign(restarted.dependencies.APIService, { isMobile: vi.fn(() => isMobile) });
            Object.assign(restarted.dependencies.settingService, {
                currentSettings: () => ({ remoteType, versionUpFlash: "" }),
            });
            new TestReplicationService(new ServiceContext(), restarted.dependencies);
            const resumeHandler = restarted.onLoaded.addHandler.mock.calls[0][0] as () => Promise<boolean>;
            return { ...restarted, resumeHandler };
        }

        afterEach(() => {
            vi.useRealTimers();
        });

        it("is taken over at once on the mobile app", async () => {
            vi.useFakeTimers();
            const { resumeHandler, openReplication } = restart(true);

            await expect(resumeHandler()).resolves.toBe(true);
            await vi.advanceTimersByTimeAsync(0);

            expect(openReplication).toHaveBeenCalledOnce();
        });

        it("is waited out on other hosts", async () => {
            vi.useFakeTimers();
            const { resumeHandler, openReplication } = restart(false);

            await expect(resumeHandler()).resolves.toBe(true);
            await vi.advanceTimersByTimeAsync(LEASE_TTL_MS - 2_000);
            expect(openReplication).not.toHaveBeenCalled();

            await vi.advanceTimersByTimeAsync(2_000);
            expect(openReplication).toHaveBeenCalledOnce();
        });

        it("keeps the existing wait on a mobile CouchDB host", async () => {
            vi.useFakeTimers();
            const { resumeHandler, openReplication } = restart(true, REMOTE_COUCHDB);

            await expect(resumeHandler()).resolves.toBe(true);
            await vi.advanceTimersByTimeAsync(LEASE_TTL_MS - 2_000);
            expect(openReplication).not.toHaveBeenCalled();

            await vi.advanceTimersByTimeAsync(2_000);
            expect(openReplication).toHaveBeenCalledOnce();
        });
    });

    it("does not cancel later resume handlers when the pending cycle fails", async () => {
        const replicationQueueStore = createReplicationQueueStore();
        const first = createDependencies();
        first.dependencies.replicationQueueStore = replicationQueueStore;
        Object.assign(first.dependencies.APIService, { isOnline: false });
        await expect(new TestReplicationService(new ServiceContext(), first.dependencies).replicate()).resolves.toBe(
            false
        );

        const resumed = createDependencies();
        resumed.dependencies.replicationQueueStore = replicationQueueStore;
        resumed.openReplication.mockRejectedValue(new Error("The request was aborted"));
        new TestReplicationService(new ServiceContext(), resumed.dependencies);
        const resumeHandler = resumed.onResumed.addHandler.mock.calls[0][0] as () => Promise<boolean>;

        await expect(resumeHandler()).resolves.toBe(true);
        await vi.waitFor(() => expect(resumed.openReplication).toHaveBeenCalled());
    });
});

describe("ReplicationService full upload", () => {
    it("uses standard replication without offering the obsolete bulk chunk pre-send", async () => {
        const askYesNoDialog = vi.fn().mockResolvedValue("yes");
        const sendChunks = vi.fn().mockResolvedValue(true);
        const replicateAllToServer = vi.fn().mockResolvedValue(true);
        const dependencies = {
            APIService: {
                addLog: vi.fn(),
                confirm: { askYesNoDialog },
                isMobile: vi.fn(() => false),
            },
            appLifecycleService: {
                isReady: () => true,
                onLoaded: { addHandler: vi.fn() },
                onResumed: { addHandler: vi.fn() },
                getUnresolvedMessages: Object.assign(vi.fn().mockResolvedValue([]), {
                    addHandler: vi.fn(),
                }),
            },
            databaseService: {},
            fileProcessingService: {},
            replicationQueueStore: createReplicationQueueStore(),
            replicatorService: {
                getActiveReplicator: () => ({
                    isChunkSendingSupported: true,
                    sendChunks,
                    replicateAllToServer,
                }),
            },
            settingService: {
                currentSettings: () => ({}),
            },
        } as unknown as ReplicationServiceDependencies;
        const service = new TestReplicationService(new ServiceContext(), dependencies);

        await expect(service.replicateAllToRemote(true)).resolves.toBe(true);

        expect(askYesNoDialog).not.toHaveBeenCalled();
        expect(sendChunks).not.toHaveBeenCalled();
        expect(replicateAllToServer).toHaveBeenCalledOnce();
    });
});

describe("ReplicationService rebuild maintenance", () => {
    function createMaintenanceService({ applicationReady = false, databaseReady = true } = {}) {
        const replicateAllToServer = vi.fn(async () => true);
        const replicateAllFromServer = vi.fn(async () => true);
        const dependencies = {
            APIService: { addLog: vi.fn(), isMobile: vi.fn(() => false) },
            appLifecycleService: {
                isReady: vi.fn(() => applicationReady),
                onLoaded: { addHandler: vi.fn() },
                onResumed: { addHandler: vi.fn() },
                getUnresolvedMessages: Object.assign(vi.fn().mockResolvedValue([]), {
                    addHandler: vi.fn(),
                }),
            },
            databaseService: {
                isDatabaseReady: vi.fn(() => databaseReady),
            },
            fileProcessingService: {},
            replicationQueueStore: createReplicationQueueStore(),
            replicatorService: {
                getActiveReplicator: vi.fn(() => ({ replicateAllToServer, replicateAllFromServer })),
            },
            settingService: {
                currentSettings: vi.fn(() => ({})),
            },
        } as unknown as ReplicationServiceDependencies;
        const service = new TestReplicationService(new ServiceContext(), dependencies);
        return { replicateAllFromServer, replicateAllToServer, service };
    }

    it("keeps ordinary full replication behind application readiness", async () => {
        const { replicateAllFromServer, replicateAllToServer, service } = createMaintenanceService();

        await expect(service.replicateAllFromRemote()).resolves.toBe(false);
        await expect(service.replicateAllToRemote()).resolves.toBe(false);

        expect(replicateAllFromServer).not.toHaveBeenCalled();
        expect(replicateAllToServer).not.toHaveBeenCalled();
    });

    it("allows explicit rebuild transfers when only the selected physical database is ready", async () => {
        const { replicateAllFromServer, replicateAllToServer, service } = createMaintenanceService();

        await expect(service.replicateAllFromRemoteForRebuild()).resolves.toBe(true);
        await expect(service.replicateAllToRemoteForRebuild()).resolves.toBe(true);

        expect(replicateAllFromServer).toHaveBeenCalledOnce();
        expect(replicateAllToServer).toHaveBeenCalledOnce();
    });

    it("rejects rebuild transfers when the selected physical database is not ready", async () => {
        const { replicateAllFromServer, replicateAllToServer, service } = createMaintenanceService({
            databaseReady: false,
        });

        await expect(service.replicateAllFromRemoteForRebuild()).resolves.toBe(false);
        await expect(service.replicateAllToRemoteForRebuild()).resolves.toBe(false);

        expect(replicateAllFromServer).not.toHaveBeenCalled();
        expect(replicateAllToServer).not.toHaveBeenCalled();
    });
});
