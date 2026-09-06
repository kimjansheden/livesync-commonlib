import { describe, expect, it, vi } from "vitest";
import { ReplicationService, type ReplicationServiceDependencies } from "./ReplicationService.ts";
import { ServiceContext } from "./ServiceBase.ts";

class TestReplicationService extends ReplicationService<ServiceContext> {}

function createReplicationQueueStore() {
    let value: unknown;
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
        const dependencies = {
            APIService: { isOnline: true, addLog: vi.fn() },
            appLifecycleService: {
                isReady: () => true,
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

        return { dependencies, onLoaded, onResumed, openReplication, runFiniteReplicationActivity };
    };

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
        expect(restarted.openReplication).toHaveBeenCalledOnce();
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
            APIService: { addLog: vi.fn() },
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
