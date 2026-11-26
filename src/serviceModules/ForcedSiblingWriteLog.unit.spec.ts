import { describe, expect, it } from "vitest";
import type { FilePathWithPrefix } from "@lib/common/types";
import type { FileReflectionProvenance } from "@lib/interfaces/FileReflectionProvenance";
import {
    FORCED_SIBLING_WRITE_LIMIT,
    FORCED_SIBLING_WRITES_KEY,
    ForcedSiblingWriteLog,
    type ForcedSiblingWrite,
} from "./ForcedSiblingWriteLog";

function entry(index: number): ForcedSiblingWrite {
    return {
        path: "notes/packing.md" as FilePathWithPrefix,
        reason: "beside-unreflected-winner",
        baseRevision: `3-base${index}`,
        winnerRevision: `4-winner${index}`,
        storedRevision: `4-stored${index}`,
        at: 1_000 + index,
    };
}

function createProvenance() {
    const diagnostics = new Map<string, unknown>();
    const provenance = {
        readDiagnostic: async (name: string) => diagnostics.get(name),
        writeDiagnostic: async (name: string, value: unknown) => {
            diagnostics.set(name, structuredClone(value));
        },
    } as unknown as FileReflectionProvenance;
    return { provenance, diagnostics };
}

describe("ForcedSiblingWriteLog", () => {
    it("keeps the latest entries, oldest first, in the provenance store", async () => {
        const { provenance, diagnostics } = createProvenance();
        const log = new ForcedSiblingWriteLog(provenance);

        for (let i = 0; i < FORCED_SIBLING_WRITE_LIMIT + 3; i++) await log.append(entry(i));

        const kept = await log.read();
        expect(kept).toHaveLength(FORCED_SIBLING_WRITE_LIMIT);
        expect(kept[0]).toEqual(entry(3));
        expect(kept.at(-1)).toEqual(entry(FORCED_SIBLING_WRITE_LIMIT + 2));
        expect(diagnostics.get(FORCED_SIBLING_WRITES_KEY)).toEqual(kept);
    });

    it("reads what an earlier instance kept, as after a restart", async () => {
        const { provenance } = createProvenance();
        await new ForcedSiblingWriteLog(provenance).append(entry(1));

        await expect(new ForcedSiblingWriteLog(provenance).read()).resolves.toEqual([entry(1)]);
    });

    it("keeps concurrent appends", async () => {
        const { provenance } = createProvenance();
        const log = new ForcedSiblingWriteLog(provenance);

        await Promise.all([log.append(entry(1)), log.append(entry(2)), log.append(entry(3))]);

        await expect(log.read()).resolves.toHaveLength(3);
    });

    it("ignores a stored value which is not a list of entries", async () => {
        const { provenance, diagnostics } = createProvenance();
        diagnostics.set(FORCED_SIBLING_WRITES_KEY, [{ unexpected: true }, entry(1)]);

        await expect(new ForcedSiblingWriteLog(provenance).read()).resolves.toEqual([entry(1)]);
        diagnostics.set(FORCED_SIBLING_WRITES_KEY, "garbage");
        await expect(new ForcedSiblingWriteLog(provenance).read()).resolves.toEqual([]);
    });

    it("keeps entries in memory when the host keeps no diagnostics", async () => {
        const log = new ForcedSiblingWriteLog({} as FileReflectionProvenance);

        await log.append(entry(1));

        await expect(log.read()).resolves.toEqual([entry(1)]);
        await expect(new ForcedSiblingWriteLog(undefined).read()).resolves.toEqual([]);
    });
});
