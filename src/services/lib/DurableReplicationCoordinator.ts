import type { AtomicSimpleStore } from "@lib/interfaces/KeyValueDatabase";
import { secureRandomHex } from "@lib/common/securityHelpers";

export const MAX_REPLICATION_LEASE_TTL_MS = 45_000;
export const DEFAULT_REPLICATION_LEASE_TTL_MS = MAX_REPLICATION_LEASE_TTL_MS;
export const REPLICATION_QUEUE_SCHEMA = 1;

export type ReplicationQueueState = {
    schema: typeof REPLICATION_QUEUE_SCHEMA;
    requestedGeneration: number;
    completedGeneration: number;
    fencingToken: number;
    lease?: {
        holderId: string;
        fencingToken: number;
        expiresAt: number;
    };
};

type ReplicationTask = () => Promise<boolean | void>;
type LeaseAttempt =
    | { acquired: false; complete: true }
    | { acquired: false; waitMs: number }
    | { acquired: true; fencingToken: number; expiresAt: number; target: number };

type CoordinatorOptions = {
    holderId?: string;
    leaseTtlMs?: number;
    now?: () => number;
    wait?: (milliseconds: number) => Promise<void>;
    scheduleRenewal?: (callback: () => void, intervalMs: number) => () => void;
};

const STATE_KEY = "state";

function createInitialState(): ReplicationQueueState {
    return {
        schema: REPLICATION_QUEUE_SCHEMA,
        requestedGeneration: 0,
        completedGeneration: 0,
        fencingToken: 0,
    };
}

function assertInteger(value: unknown, label: string): asserts value is number {
    if (!Number.isSafeInteger(value) || (value as number) < 0) {
        throw new Error(`Invalid durable replication ${label}.`);
    }
}

function validateState(value: unknown): ReplicationQueueState {
    if (value === undefined || value === null) return createInitialState();
    if (!value || typeof value !== "object" || Array.isArray(value)) {
        throw new Error("Invalid durable replication queue state.");
    }
    const state = value as Partial<ReplicationQueueState>;
    const keys = Object.keys(state).sort();
    const expected = ["schema", "requestedGeneration", "completedGeneration", "fencingToken"];
    if (state.lease !== undefined) expected.push("lease");
    if (JSON.stringify(keys) !== JSON.stringify(expected.sort()) || state.schema !== REPLICATION_QUEUE_SCHEMA) {
        throw new Error("Invalid durable replication queue state schema.");
    }
    assertInteger(state.requestedGeneration, "requested generation");
    assertInteger(state.completedGeneration, "completed generation");
    assertInteger(state.fencingToken, "fencing token");
    if (state.completedGeneration > state.requestedGeneration) {
        throw new Error("Invalid durable replication generation order.");
    }
    if (state.lease !== undefined) {
        const leaseKeys = Object.keys(state.lease).sort();
        if (JSON.stringify(leaseKeys) !== JSON.stringify(["expiresAt", "fencingToken", "holderId"])) {
            throw new Error("Invalid durable replication lease schema.");
        }
        if (typeof state.lease.holderId !== "string" || state.lease.holderId.length < 8) {
            throw new Error("Invalid durable replication lease holder.");
        }
        assertInteger(state.lease.fencingToken, "lease fencing token");
        assertInteger(state.lease.expiresAt, "lease expiry");
        if (state.lease.fencingToken !== state.fencingToken) {
            throw new Error("Invalid durable replication lease fencing token.");
        }
    }
    return {
        ...(state as ReplicationQueueState),
        ...(state.lease ? { lease: { ...state.lease } } : {}),
    };
}

function defaultWait(milliseconds: number): Promise<void> {
    return new Promise((resolve) => globalThis.setTimeout(resolve, milliseconds));
}

function defaultScheduleRenewal(callback: () => void, intervalMs: number): () => void {
    const timer = globalThis.setInterval(callback, intervalMs);
    return () => globalThis.clearInterval(timer);
}

export class DurableReplicationCoordinator {
    private readonly holderId: string;
    private readonly leaseTtlMs: number;
    private readonly now: () => number;
    private readonly wait: (milliseconds: number) => Promise<void>;
    private readonly scheduleRenewal: (callback: () => void, intervalMs: number) => () => void;
    private running: Promise<boolean> | undefined;

    constructor(
        private readonly store: Pick<AtomicSimpleStore<ReplicationQueueState>, "get" | "atomicUpdate">,
        options: CoordinatorOptions = {}
    ) {
        this.leaseTtlMs = options.leaseTtlMs ?? DEFAULT_REPLICATION_LEASE_TTL_MS;
        if (
            !Number.isSafeInteger(this.leaseTtlMs) ||
            this.leaseTtlMs <= 0 ||
            this.leaseTtlMs > MAX_REPLICATION_LEASE_TTL_MS
        ) {
            throw new Error("The replication lease TTL must be between 1 and 45000 milliseconds.");
        }
        this.holderId = options.holderId ?? secureRandomHex(16);
        if (this.holderId.length < 8) throw new Error("The replication lease holder ID is invalid.");
        this.now = options.now ?? Date.now;
        this.wait = options.wait ?? defaultWait;
        this.scheduleRenewal = options.scheduleRenewal ?? defaultScheduleRenewal;
    }

    private async readState(): Promise<ReplicationQueueState> {
        return validateState(await this.store.get(STATE_KEY));
    }

    private async updateState<T>(
        change: (state: ReplicationQueueState) => { state: ReplicationQueueState; result: T }
    ): Promise<T> {
        return await this.store.atomicUpdate(STATE_KEY, (stored) => {
            const current = validateState(stored);
            const { state, result } = change(current);
            validateState(state);
            return { value: state, result };
        });
    }

    private async requestGeneration(): Promise<number> {
        return await this.updateState((state) => {
            state.requestedGeneration++;
            return { state, result: state.requestedGeneration };
        });
    }

    private async acquireLease(): Promise<{ fencingToken: number; expiresAt: number; target: number } | undefined> {
        while (true) {
            const now = this.now();
            const attempt = await this.updateState<LeaseAttempt>((state) => {
                if (state.completedGeneration >= state.requestedGeneration) {
                    return { state, result: { acquired: false as const, complete: true as const } };
                }
                if (state.lease && state.lease.holderId !== this.holderId && state.lease.expiresAt > now) {
                    return { state, result: { acquired: false as const, waitMs: state.lease.expiresAt - now } };
                }
                const fencingToken = state.fencingToken + 1;
                const expiresAt = now + this.leaseTtlMs;
                state.fencingToken = fencingToken;
                state.lease = { holderId: this.holderId, fencingToken, expiresAt };
                return {
                    state,
                    result: {
                        acquired: true as const,
                        fencingToken,
                        expiresAt,
                        target: state.requestedGeneration,
                    },
                };
            });
            if ("complete" in attempt) return undefined;
            if ("waitMs" in attempt) {
                await this.wait(Math.max(1, Math.min(attempt.waitMs, this.leaseTtlMs)));
                continue;
            }
            return attempt;
        }
    }

    private async renewLease(fencingToken: number): Promise<boolean> {
        const now = this.now();
        return await this.updateState((state) => {
            const valid =
                state.lease?.holderId === this.holderId &&
                state.lease.fencingToken === fencingToken &&
                state.lease.expiresAt > now;
            if (!valid) return { state, result: false };
            state.lease.expiresAt = now + this.leaseTtlMs;
            return { state, result: true };
        });
    }

    private async releaseLease(fencingToken: number): Promise<void> {
        await this.updateState((state) => {
            if (state.lease?.holderId === this.holderId && state.lease.fencingToken === fencingToken) {
                delete state.lease;
            }
            return { state, result: undefined };
        });
    }

    private async completeGeneration(target: number, fencingToken: number): Promise<boolean> {
        const now = this.now();
        return await this.updateState((state) => {
            const valid =
                state.lease?.holderId === this.holderId &&
                state.lease.fencingToken === fencingToken &&
                state.lease.expiresAt > now;
            if (!valid) return { state, result: false };
            state.completedGeneration = Math.max(state.completedGeneration, target);
            delete state.lease;
            return { state, result: true };
        });
    }

    private async runLeasedTask(task: ReplicationTask, fencingToken: number): Promise<boolean> {
        let leaseValid = true;
        let renewal = Promise.resolve();
        const stopRenewal = this.scheduleRenewal(
            () => {
                renewal = renewal
                    .then(async () => {
                        leaseValid &&= await this.renewLease(fencingToken);
                    })
                    .catch(() => {
                        leaseValid = false;
                    });
            },
            Math.max(1, Math.floor(this.leaseTtlMs / 3))
        );
        try {
            const result = await task();
            await renewal;
            return result !== false && leaseValid;
        } finally {
            stopRenewal();
        }
    }

    private async drain(task: ReplicationTask): Promise<boolean> {
        while (true) {
            const lease = await this.acquireLease();
            if (!lease) return true;
            let succeeded: boolean;
            try {
                succeeded = await this.runLeasedTask(task, lease.fencingToken);
            } catch (error) {
                this.lastFailedTarget = lease.target;
                // A thrown cycle must not keep other runtimes waiting for the lease to expire.
                try {
                    await this.releaseLease(lease.fencingToken);
                } catch {
                    // Keep the cycle's own error; the lease then expires on its own.
                }
                throw error;
            }
            if (!succeeded) {
                this.lastFailedTarget = lease.target;
                await this.releaseLease(lease.fencingToken);
                return false;
            }
            if (!(await this.completeGeneration(lease.target, lease.fencingToken))) return false;
        }
    }

    /** The generation a failed drain last attempted, so joiners can tell whether it covered their own request. */
    private lastFailedTarget = 0;

    private async run(task: ReplicationTask, ownGeneration?: number): Promise<boolean> {
        let joinedFailedDrain = false;
        while (true) {
            let startedHere = false;
            if (!this.running) {
                this.running = this.drain(task).finally(() => {
                    this.running = undefined;
                });
                startedHere = true;
            }
            let drained: boolean;
            try {
                drained = await this.running;
            } catch (error) {
                if (startedHere) throw error;
                drained = false;
            }
            if (!drained) {
                // A drain started by an earlier caller can fail before this caller's generation was attempted.
                // Try that work once with a drain of this caller's own rather than inheriting the failure, but do
                // not repeat a stopped or failed attempt which already covered this caller's generation.
                const attemptedBefore = ownGeneration === undefined || ownGeneration <= this.lastFailedTarget;
                if (startedHere || joinedFailedDrain || attemptedBefore) return false;
                joinedFailedDrain = true;
                continue;
            }
            const state = await this.readState();
            if (state.completedGeneration >= state.requestedGeneration) return true;
        }
    }

    async enqueue(task: ReplicationTask): Promise<boolean> {
        const requestedGeneration = await this.requestGeneration();
        const succeeded = await this.run(task, requestedGeneration);
        if (!succeeded) return false;
        return (await this.readState()).completedGeneration >= requestedGeneration;
    }

    async resumePending(task: ReplicationTask): Promise<boolean> {
        const state = await this.readState();
        if (state.completedGeneration >= state.requestedGeneration) return true;
        return await this.run(task);
    }
}
