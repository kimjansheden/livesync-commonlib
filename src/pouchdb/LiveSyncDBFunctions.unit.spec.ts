import { describe, expect, it, vi } from "vitest";
import {
    DEFAULT_SETTINGS,
    DEVICE_ID_PREFERRED,
    TweakValuesTemplate,
    type DeviceInfo,
    type EntryMilestoneInfo,
} from "@lib/common/types.ts";
import { extractObject } from "@lib/common/utils.ts";
import { ensureRemoteIsCompatible } from "./LiveSyncDBFunctions.ts";

const VERSION = { min: 0, max: 2, current: 2 } as const;
const DEVICE_INFO: DeviceInfo = {
    device_name: "current-device",
    vault_name: "synthetic-vault",
    app_version: "1.0.0",
    plugin_version: "1.0.0",
    progress: "checkpoint-current",
};

function lockedMilestone(cleaned = false): EntryMilestoneInfo {
    return {
        _id: "_local/obsidian_livesync_milestone",
        type: "milestoneinfo",
        created: 1,
        locked: true,
        cleaned,
        accepted_nodes: ["accepted-node"],
        node_chunk_info: {
            "accepted-node": VERSION,
            "rejected-node": VERSION,
        },
        node_info: {
            "accepted-node": { ...DEVICE_INFO, last_connected: 10 },
            "rejected-node": {
                ...DEVICE_INFO,
                device_name: "rejected-device",
                last_connected: 20,
            },
        },
        tweak_values: {},
    };
}

describe("ensureRemoteIsCompatible locked milestone", () => {
    it("rejects an unaccepted node without writing a heartbeat or mutating the milestone", async () => {
        const milestone = lockedMilestone();
        const before = structuredClone(milestone);
        const update = vi.fn();

        await expect(
            ensureRemoteIsCompatible(
                milestone,
                DEFAULT_SETTINGS,
                "rejected-node",
                VERSION,
                { ...DEVICE_INFO, progress: "checkpoint-new" },
                update
            )
        ).resolves.toBe("NODE_LOCKED");

        expect(update).not.toHaveBeenCalled();
        expect(milestone).toEqual(before);
    });

    it("reports a cleaned remote without mutating it", async () => {
        const milestone = lockedMilestone(true);
        const before = structuredClone(milestone);
        const update = vi.fn();

        await expect(
            ensureRemoteIsCompatible(milestone, DEFAULT_SETTINGS, "rejected-node", VERSION, DEVICE_INFO, update)
        ).resolves.toBe("NODE_CLEANED");

        expect(update).not.toHaveBeenCalled();
        expect(milestone).toEqual(before);
    });

    it("reports an accepted locked node without refreshing compatibility metadata", async () => {
        const milestone = lockedMilestone();
        const before = structuredClone(milestone);
        const update = vi.fn();

        await expect(
            ensureRemoteIsCompatible(
                milestone,
                DEFAULT_SETTINGS,
                "accepted-node",
                VERSION,
                { ...DEVICE_INFO, plugin_version: "2.0.0" },
                update
            )
        ).resolves.toBe("LOCKED");

        expect(update).not.toHaveBeenCalled();
        expect(milestone).toEqual(before);
    });

    it("rejects an incompatible accepted peer before returning the locked status", async () => {
        const milestone = lockedMilestone();
        milestone.accepted_nodes.push("peer-node");
        milestone.node_chunk_info["peer-node"] = { min: 3, max: 4, current: 4 };
        const before = structuredClone(milestone);
        const update = vi.fn();

        await expect(
            ensureRemoteIsCompatible(milestone, DEFAULT_SETTINGS, "accepted-node", VERSION, DEVICE_INFO, update)
        ).resolves.toBe("INCOMPATIBLE");

        expect(update).not.toHaveBeenCalled();
        expect(milestone).toEqual(before);
    });
});

describe("ensureRemoteIsCompatible milestone writes", () => {
    const NODE = "current-node";
    const MINUTE = 60_000;
    const tweaks = extractObject(TweakValuesTemplate, DEFAULT_SETTINGS);

    /** A milestone which already holds this device's current entry, last connected `connectedAgo` ago. */
    function currentMilestone(connectedAgo: number, revision?: string): EntryMilestoneInfo {
        return {
            _id: "_local/obsidian_livesync_milestone",
            ...(revision ? { _rev: revision } : {}),
            type: "milestoneinfo",
            created: 1,
            locked: false,
            accepted_nodes: [NODE],
            node_chunk_info: { [NODE]: VERSION },
            node_info: { [NODE]: { ...DEVICE_INFO, last_connected: Date.now() - connectedAgo } },
            tweak_values: { [NODE]: tweaks, [DEVICE_ID_PREFERRED]: tweaks },
        } as EntryMilestoneInfo;
    }

    const check = (milestone: EntryMilestoneInfo | false, deviceInfo: DeviceInfo, refreshMs?: number) => {
        const update = vi.fn(async () => undefined);
        const result = ensureRemoteIsCompatible(
            milestone,
            DEFAULT_SETTINGS,
            NODE,
            VERSION,
            deviceInfo,
            update,
            refreshMs
        );
        return { result, update };
    };

    it("does not write a current milestone again while its connection time is within the refresh interval", async () => {
        const { result, update } = check(currentMilestone(5 * MINUTE), DEVICE_INFO, 10 * MINUTE);

        await expect(result).resolves.toBe("OK");
        expect(update).not.toHaveBeenCalled();
    });

    it("writes the milestone once its connection time is older than the refresh interval", async () => {
        const { result, update } = check(currentMilestone(11 * MINUTE), DEVICE_INFO, 10 * MINUTE);

        await expect(result).resolves.toBe("OK");
        expect(update).toHaveBeenCalledOnce();
    });

    it("writes the milestone at once when this device's information has changed", async () => {
        const { result, update } = check(
            currentMilestone(MINUTE / 2),
            { ...DEVICE_INFO, progress: "checkpoint-newer" },
            10 * MINUTE
        );

        await expect(result).resolves.toBe("OK");
        expect(update).toHaveBeenCalledWith(
            expect.objectContaining({
                node_info: { [NODE]: expect.objectContaining({ progress: "checkpoint-newer" }) },
            })
        );
    });

    it("writes a milestone which the remote does not have yet", async () => {
        const { result, update } = check(false, DEVICE_INFO, 10 * MINUTE);

        await expect(result).resolves.toBe("OK");
        expect(update).toHaveBeenCalledOnce();
    });

    it("refreshes the connection time of a document store every minute by default", async () => {
        const recent = check(currentMilestone(MINUTE / 2, "3-remote"), DEVICE_INFO);
        await expect(recent.result).resolves.toBe("OK");
        expect(recent.update).not.toHaveBeenCalled();

        const older = check(currentMilestone(2 * MINUTE, "3-remote"), DEVICE_INFO);
        await expect(older.result).resolves.toBe("OK");
        expect(older.update).toHaveBeenCalledOnce();
    });
});
