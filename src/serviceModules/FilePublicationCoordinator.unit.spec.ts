import { describe, expect, it, vi } from "vitest";
import { FilePublicationCoordinator, UnknownFileWriteStateError } from "./FilePublicationCoordinator";
import type { FilePathWithPrefix, UXStat } from "@lib/common/types";
import type { FileReflectionProvenanceRecord } from "@lib/interfaces/FileReflectionProvenance";

const path = "image.bin" as FilePathWithPrefix;
function fixture() {
    const records = new Map<string, FileReflectionProvenanceRecord>();
    const store = {
        get: vi.fn(async (p: string) => records.get(p)),
        set: vi.fn(async (p: string, r: FileReflectionProvenanceRecord) => {
            records.set(p, r);
        }),
        delete: vi.fn(async (p: string) => {
            records.delete(p);
        }),
        move: vi.fn(),
    };
    const dependencies = {
        store,
        normalise: (p: string) => p.toLowerCase(),
        stat: vi.fn(async (): Promise<UXStat | null> => null),
    };
    return { records, store, dependencies, owner: new FilePublicationCoordinator(dependencies) };
}

describe("FilePublicationCoordinator", () => {
    it("retains a publication gap across restart and clears only a successful publication token", async () => {
        const f = fixture();
        const token = await f.owner.begin(path, "2-remote");
        const restarted = new FilePublicationCoordinator(f.dependencies);
        expect(await restarted.missingPublication(path)).toBe("2-remote");
        await restarted.reflect(path, "2-remote", 20, true, token);
        expect(await restarted.missingPublication(path)).toBeUndefined();
        expect(f.records.get(path)).toEqual({
            revision: "2-remote",
            observedStorageMtime: 20,
            reflectedFromDatabase: true,
        });
    });
    it("never classifies an existing complete or locally replaced file as a publication gap", async () => {
        const f = fixture();
        await f.owner.begin(path, "2-remote");
        f.dependencies.stat.mockResolvedValue({ type: "file", size: 7, mtime: 99, ctime: 1 });
        expect(await f.owner.missingPublication(path)).toBeUndefined();
        expect(f.records.get(path)).toBeUndefined();
        f.dependencies.stat.mockResolvedValue(null);
        expect(await f.owner.missingPublication(path)).toBeUndefined();
    });
    it("fails closed on unreadable provenance even in a fresh process", async () => {
        const f = fixture();
        f.store.get.mockRejectedValue(new Error("unreadable"));
        await expect(f.owner.missingPublication(path)).rejects.toBeInstanceOf(UnknownFileWriteStateError);
        await expect(f.owner.begin(path, "2-remote")).rejects.toBeInstanceOf(UnknownFileWriteStateError);
        expect(f.store.set).not.toHaveBeenCalled();
    });
    it("does not remove a mark when the completion record fails", async () => {
        const f = fixture();
        const token = await f.owner.begin(path, "2-remote");
        f.store.set.mockRejectedValue(new Error("quota"));
        await expect(f.owner.reflect(path, "2-remote", 20, true, token)).rejects.toBeInstanceOf(
            UnknownFileWriteStateError
        );
        expect(await f.owner.missingPublication(path)).toBe("2-remote");
    });
    it("ordinary provenance and a different token cannot clear a publication in progress", async () => {
        const f = fixture();
        const token = await f.owner.begin(path, "2-remote");
        await f.owner.reflect(path, "1-local", 10);
        await f.owner.reflect(path, "2-remote", 20, true, "other");
        expect(f.records.get(path)?.pendingPublication?.token).toBe(token);
        await expect(f.owner.delete(path)).rejects.toBeInstanceOf(UnknownFileWriteStateError);
    });
    it("leaves an old unreleased in-place marker unknown without altering storage", async () => {
        const f = fixture();
        f.records.set(path, { revision: "2-remote", incompleteWriteRevision: "2-remote#old" });
        await expect(f.owner.missingPublication(path)).rejects.toBeInstanceOf(UnknownFileWriteStateError);
        expect(f.store.delete).not.toHaveBeenCalled();
        expect(f.store.set).not.toHaveBeenCalled();
    });
    it("protects renamed destination before failed source record removal", async () => {
        const f = fixture();
        await f.owner.begin(path, "2-remote");
        f.dependencies.stat.mockResolvedValue({ type: "file", size: 12, mtime: 20, ctime: 1 });
        f.store.delete.mockRejectedValue(new Error("unwritable"));
        await expect(f.owner.move(path, "new.bin" as FilePathWithPrefix)).rejects.toBeInstanceOf(
            UnknownFileWriteStateError
        );
        expect(f.records.get("new.bin")?.pendingPublication).toEqual(f.records.get(path)?.pendingPublication);
    });
    it("serialises two writers before staging, including equivalent normalised paths", async () => {
        const f = fixture();
        let resume!: () => void;
        let entered!: () => void;
        const pause = new Promise<void>((r) => {
            resume = r;
        });
        const started = new Promise<void>((r) => {
            entered = r;
        });
        const order: number[] = [];
        const first = f.owner.run(path, async () => {
            order.push(1);
            entered();
            await pause;
        });
        await started;
        const second = f.owner.run("IMAGE.BIN" as FilePathWithPrefix, async () => {
            order.push(2);
        });
        await Promise.resolve();
        expect(order).toEqual([1]);
        resume();
        await Promise.all([first, second]);
        expect(order).toEqual([1, 2]);
    });
});
