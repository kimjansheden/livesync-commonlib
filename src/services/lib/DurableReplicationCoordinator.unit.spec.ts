import { describe, expect, it, vi } from "vitest";
import {
    DurableReplicationCoordinator,
    MAX_REPLICATION_LEASE_TTL_MS,
    REPLICATION_QUEUE_SCHEMA,
    type ReplicationQueueState,
} from "./DurableReplicationCoordinator.ts";

function deferred() {
    let resolve!: (value: boolean) => void;
    const promise = new Promise<boolean>((resolver) => {
        resolve = resolver;
    });
    return { promise, resolve };
}

function memoryStore(initial?: ReplicationQueueState) {
    let value = initial ? structuredClone(initial) : undefined;
    let transaction = Promise.resolve();
    return {
        get: vi.fn(async () => (value ? structuredClone(value) : undefined)),
        atomicUpdate: vi.fn(
            async <R>(
                _key: string,
                change: (current: ReplicationQueueState | undefined) => { value: ReplicationQueueState; result: R }
            ) => {
                let release!: () => void;
                const previous = transaction;
                transaction = new Promise<void>((resolve) => {
                    release = resolve;
                });
                await previous;
                try {
                    const next = change(value ? structuredClone(value) : undefined);
                    value = structuredClone(next.value);
                    return next.result;
                } finally {
                    release();
                }
            }
        ),
        read: () => (value ? structuredClone(value) : undefined),
    };
}

describe("DurableReplicationCoordinator", () => {
    it("runs one cycle at a time and drains an event queued during the active cycle", async () => {
        const store = memoryStore();
        const firstCycle = deferred();
        let active = 0;
        let maximumActive = 0;
        let cycles = 0;
        const task = vi.fn(async () => {
            cycles++;
            active++;
            maximumActive = Math.max(maximumActive, active);
            if (cycles === 1) await firstCycle.promise;
            active--;
            return true;
        });
        const coordinator = new DurableReplicationCoordinator(store, { holderId: "holder-a" });

        const first = coordinator.enqueue(task);
        await vi.waitFor(() => expect(task).toHaveBeenCalledTimes(1));
        const second = coordinator.enqueue(task);
        await vi.waitFor(() => expect(store.read()?.requestedGeneration).toBe(2));
        firstCycle.resolve(true);

        await expect(Promise.all([first, second])).resolves.toEqual([true, true]);
        expect(task).toHaveBeenCalledTimes(2);
        expect(maximumActive).toBe(1);
        expect(store.read()).toMatchObject({ requestedGeneration: 2, completedGeneration: 2 });
    });

    it("drains a request persisted while the previous drain is returning", async () => {
        let value: ReplicationQueueState | undefined;
        let reads = 0;
        const releaseTerminalRead = deferred();
        const terminalReadStarted = deferred();
        const store = {
            get: vi.fn(async () => {
                reads++;
                const snapshot = value ? structuredClone(value) : undefined;
                if (reads === 1) {
                    terminalReadStarted.resolve(true);
                    await releaseTerminalRead.promise;
                }
                return snapshot;
            }),
            atomicUpdate: vi.fn(
                async <R>(
                    _key: string,
                    change: (current: ReplicationQueueState | undefined) => { value: ReplicationQueueState; result: R }
                ) => {
                    const next = change(value ? structuredClone(value) : undefined);
                    value = structuredClone(next.value);
                    return next.result;
                }
            ),
        };
        const task = vi.fn(async () => true);
        const coordinator = new DurableReplicationCoordinator(store, { holderId: "holder-a" });

        const first = coordinator.enqueue(task);
        await terminalReadStarted.promise;
        const second = coordinator.enqueue(task);
        await vi.waitFor(() => expect(value?.requestedGeneration).toBe(2));
        releaseTerminalRead.resolve(true);

        await expect(Promise.all([first, second])).resolves.toEqual([true, true]);
        expect(task).toHaveBeenCalledTimes(2);
        expect(value).toMatchObject({ requestedGeneration: 2, completedGeneration: 2 });
    });

    it("serialises generation and lease updates shared by independent runtime coordinators", async () => {
        const now = 1_000;
        const store = memoryStore();
        const firstCycle = deferred();
        let active = 0;
        let maximumActive = 0;
        const task = vi.fn(async () => {
            active++;
            maximumActive = Math.max(maximumActive, active);
            if (task.mock.calls.length === 1) await firstCycle.promise;
            active--;
            return true;
        });
        const waitForFirst = vi.fn(async (_milliseconds: number) => {
            await firstCycle.promise;
        });
        const first = new DurableReplicationCoordinator(store, {
            holderId: "holder-a",
            now: () => now,
            wait: waitForFirst,
            scheduleRenewal: () => () => undefined,
        });
        const second = new DurableReplicationCoordinator(store, {
            holderId: "holder-b",
            now: () => now,
            wait: waitForFirst,
            scheduleRenewal: () => () => undefined,
        });

        const firstRun = first.enqueue(task);
        await vi.waitFor(() => expect(task).toHaveBeenCalledOnce());
        const secondRun = second.enqueue(task);
        await vi.waitFor(() => expect(waitForFirst).toHaveBeenCalledOnce());
        firstCycle.resolve(true);

        await expect(Promise.all([firstRun, secondRun])).resolves.toEqual([true, true]);
        expect(maximumActive).toBe(1);
        expect(store.read()).toMatchObject({ requestedGeneration: 2, completedGeneration: 2, fencingToken: 2 });
    });

    it("leaves a failed generation pending and resumes it after a new process starts", async () => {
        const store = memoryStore();
        const first = new DurableReplicationCoordinator(store, { holderId: "holder-a" });

        await expect(first.enqueue(async () => false)).resolves.toBe(false);
        expect(store.read()).toMatchObject({ requestedGeneration: 1, completedGeneration: 0 });

        const restarted = new DurableReplicationCoordinator(store, { holderId: "holder-b" });
        const task = vi.fn(async () => true);
        await expect(restarted.resumePending(task)).resolves.toBe(true);
        expect(task).toHaveBeenCalledOnce();
        expect(store.read()).toMatchObject({ requestedGeneration: 1, completedGeneration: 1 });
    });

    it("releases the lease and leaves the generation pending when a cycle throws", async () => {
        const store = memoryStore();
        const coordinator = new DurableReplicationCoordinator(store, { holderId: "holder-a" });

        await expect(
            coordinator.enqueue(async () => {
                throw new Error("The request was aborted");
            })
        ).rejects.toThrow("The request was aborted");

        expect(store.read()).toMatchObject({ requestedGeneration: 1, completedGeneration: 0 });
        expect(store.read()?.lease).toBeUndefined();
    });

    it("runs a caller's own attempt when the drain it joined throws", async () => {
        const store = memoryStore();
        const coordinator = new DurableReplicationCoordinator(store, { holderId: "holder-a" });
        const firstCycle = deferred();
        let cycles = 0;
        const task = vi.fn(async () => {
            cycles++;
            if (cycles === 1) {
                await firstCycle.promise;
                throw new Error("The request was aborted");
            }
            return true;
        });

        const first = coordinator.enqueue(task);
        await vi.waitFor(() => expect(task).toHaveBeenCalledOnce());
        const second = coordinator.enqueue(task);
        firstCycle.resolve(true);

        await expect(first).rejects.toThrow("The request was aborted");
        await expect(second).resolves.toBe(true);
        expect(task).toHaveBeenCalledTimes(2);
        expect(store.read()).toMatchObject({ requestedGeneration: 2, completedGeneration: 2 });
    });

    it("reports a failure after its own attempt fails too", async () => {
        const store = memoryStore();
        const coordinator = new DurableReplicationCoordinator(store, { holderId: "holder-a" });
        const firstCycle = deferred();
        let cycles = 0;
        const task = vi.fn(async () => {
            cycles++;
            if (cycles === 1) await firstCycle.promise;
            return false;
        });

        const first = coordinator.enqueue(task);
        await vi.waitFor(() => expect(task).toHaveBeenCalledOnce());
        const second = coordinator.enqueue(task);
        firstCycle.resolve(true);

        await expect(first).resolves.toBe(false);
        await expect(second).resolves.toBe(false);
        expect(task).toHaveBeenCalledTimes(2);
        expect(store.read()).toMatchObject({ requestedGeneration: 2, completedGeneration: 0 });
    });

    it("shares one retry between callers which joined the same failed drain", async () => {
        const store = memoryStore();
        const coordinator = new DurableReplicationCoordinator(store, { holderId: "holder-a" });
        const firstCycle = deferred();
        let cycles = 0;
        const task = vi.fn(async () => {
            cycles++;
            if (cycles === 1) {
                await firstCycle.promise;
                throw new Error("The request was aborted");
            }
            return true;
        });

        const first = coordinator.enqueue(task);
        await vi.waitFor(() => expect(task).toHaveBeenCalledOnce());
        const second = coordinator.enqueue(task);
        const third = coordinator.enqueue(task);
        await vi.waitFor(() => expect(store.read()?.requestedGeneration).toBe(3));
        firstCycle.resolve(true);

        await expect(first).rejects.toThrow("The request was aborted");
        await expect(Promise.all([second, third])).resolves.toEqual([true, true]);
        expect(task).toHaveBeenCalledTimes(2);
    });

    it("does not repeat a stopped cycle for a caller whose generation it already covered", async () => {
        const store = memoryStore();
        const coordinator = new DurableReplicationCoordinator(store, { holderId: "holder-a" });
        const task = vi.fn(async () => false);

        const first = coordinator.enqueue(task);
        const second = coordinator.enqueue(task);

        await expect(Promise.all([first, second])).resolves.toEqual([false, false]);
        expect(task).toHaveBeenCalledOnce();
        expect(store.read()).toMatchObject({ requestedGeneration: 2, completedGeneration: 0 });
    });

    it("waits for an abandoned lease to expire and acquires it with a higher fencing token", async () => {
        let now = 1_000;
        const store = memoryStore({
            schema: REPLICATION_QUEUE_SCHEMA,
            requestedGeneration: 1,
            completedGeneration: 0,
            fencingToken: 4,
            lease: { holderId: "crashed-holder", fencingToken: 4, expiresAt: 5_000 },
        });
        const wait = vi.fn(async (milliseconds: number) => {
            now += milliseconds;
        });
        const coordinator = new DurableReplicationCoordinator(store, {
            holderId: "holder-new",
            now: () => now,
            wait,
        });

        await expect(coordinator.resumePending(async () => true)).resolves.toBe(true);
        expect(wait).toHaveBeenCalledWith(4_000);
        expect(store.read()).toMatchObject({ fencingToken: 5, completedGeneration: 1 });
        expect(store.read()?.lease).toBeUndefined();
    });

    it("takes over at once a lease an earlier process left, where only one process runs at a time", async () => {
        const store = memoryStore({
            schema: REPLICATION_QUEUE_SCHEMA,
            requestedGeneration: 1,
            completedGeneration: 0,
            fencingToken: 4,
            lease: { holderId: "closed-process", fencingToken: 4, expiresAt: 45_000 },
        });
        const wait = vi.fn(async () => undefined);
        const task = vi.fn(async () => true);
        const coordinator = new DurableReplicationCoordinator(store, {
            holderId: "holder-new",
            now: () => 1_000,
            wait,
            takeOverLeaseOfEarlierProcess: true,
        });

        await expect(coordinator.resumePending(task)).resolves.toBe(true);

        expect(wait).not.toHaveBeenCalled();
        expect(task).toHaveBeenCalledOnce();
        // The higher fencing token keeps a cycle of the earlier process, were it still running, from completing.
        expect(store.read()).toMatchObject({ fencingToken: 5, completedGeneration: 1 });
    });

    it("still waits out a lease which another holder acquires after the first acquisition", async () => {
        const now = () => 1_000;
        const store = memoryStore();
        const otherCycle = deferred();
        // Waiting lasts until the other holder has finished and released its lease.
        const wait = vi.fn(async (_milliseconds: number) => {
            await otherCycle.promise;
        });
        const mobile = new DurableReplicationCoordinator(store, {
            holderId: "holder-mobile",
            now,
            wait,
            scheduleRenewal: () => () => undefined,
            takeOverLeaseOfEarlierProcess: true,
        });
        const other = new DurableReplicationCoordinator(store, {
            holderId: "holder-other",
            now,
            scheduleRenewal: () => () => undefined,
        });
        await expect(mobile.enqueue(async () => true)).resolves.toBe(true);

        const otherRun = other.enqueue(() => otherCycle.promise);
        await vi.waitFor(() => expect(store.read()?.lease?.holderId).toBe("holder-other"));
        const mobileTask = vi.fn(async () => true);
        const mobileRun = mobile.enqueue(mobileTask);

        await vi.waitFor(() => expect(wait).toHaveBeenCalledOnce());
        expect(mobileTask).not.toHaveBeenCalled();
        otherCycle.resolve(true);
        // The other holder completes its generations only because its lease was left to it, and it drains the one
        // requested meanwhile as well.
        await expect(Promise.all([otherRun, mobileRun])).resolves.toEqual([true, true]);
        expect(store.read()).toMatchObject({ requestedGeneration: 3, completedGeneration: 3 });
    });

    it("renews only its current unexpired lease while a cycle is active", async () => {
        let now = 1_000;
        let renewalCallback: (() => void) | undefined;
        const stopRenewal = vi.fn();
        const store = memoryStore();
        const cycle = deferred();
        const coordinator = new DurableReplicationCoordinator(store, {
            holderId: "holder-a",
            now: () => now,
            scheduleRenewal: (callback) => {
                renewalCallback = callback;
                return stopRenewal;
            },
        });

        const running = coordinator.enqueue(() => cycle.promise);
        await vi.waitFor(() => expect(renewalCallback).toBeTypeOf("function"));
        expect(store.read()?.lease?.expiresAt).toBe(46_000);

        now = 10_000;
        renewalCallback?.();
        await vi.waitFor(() => expect(store.read()?.lease?.expiresAt).toBe(55_000));
        cycle.resolve(true);

        await expect(running).resolves.toBe(true);
        expect(stopRenewal).toHaveBeenCalledOnce();
    });

    it("rejects an expired lease before recording a generation as complete", async () => {
        let now = 1_000;
        const store = memoryStore();
        const coordinator = new DurableReplicationCoordinator(store, {
            holderId: "holder-a",
            leaseTtlMs: 10,
            now: () => now,
            scheduleRenewal: () => () => undefined,
        });

        await expect(
            coordinator.enqueue(async () => {
                now = 1_010;
                return true;
            })
        ).resolves.toBe(false);
        expect(store.read()).toMatchObject({ requestedGeneration: 1, completedGeneration: 0 });
    });

    it("rejects excessive TTLs and corrupt persisted state", async () => {
        expect(
            () =>
                new DurableReplicationCoordinator(memoryStore(), {
                    holderId: "holder-a",
                    leaseTtlMs: MAX_REPLICATION_LEASE_TTL_MS + 1,
                })
        ).toThrow(/45000/u);

        const corrupt = memoryStore({
            schema: REPLICATION_QUEUE_SCHEMA,
            requestedGeneration: 0,
            completedGeneration: 1,
            fencingToken: 0,
        });
        const coordinator = new DurableReplicationCoordinator(corrupt, { holderId: "holder-a" });
        await expect(coordinator.resumePending(async () => true)).rejects.toThrow(/generation order/u);
    });
});
