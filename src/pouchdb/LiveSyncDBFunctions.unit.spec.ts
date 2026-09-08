import { describe, expect, it, vi } from "vitest";
import { DEFAULT_SETTINGS, type DeviceInfo, type EntryMilestoneInfo } from "@lib/common/types.ts";
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
