import { afterEach, describe, expect, it, vi } from "vitest";
import { createTextBlob, isDocContentSame } from "./utils";

const SLICE = 8 * 1024 * 1024;

function patternBytes(length: number) {
    const bytes = new Uint8Array(length);
    for (let index = 0; index < length; index++) bytes[index] = (index * 31 + 7) & 0xff;
    return bytes;
}

describe("isDocContentSame", () => {
    afterEach(() => {
        vi.restoreAllMocks();
    });

    it("compares a large buffer with a blob without reading either in full", async () => {
        const bytes = patternBytes(SLICE * 2 + 123);
        const blob = new Blob([bytes]);
        const readSizes: number[] = [];
        const original = Blob.prototype.arrayBuffer;
        vi.spyOn(Blob.prototype, "arrayBuffer").mockImplementation(function (this: Blob) {
            readSizes.push(this.size);
            return original.call(this);
        });

        await expect(isDocContentSame(bytes.buffer, blob)).resolves.toBe(true);

        expect(readSizes.length).toBeGreaterThan(1);
        expect(Math.max(...readSizes)).toBeLessThanOrEqual(SLICE);
    });

    it("detects a difference in the last byte of large content", async () => {
        const bytes = patternBytes(SLICE + 10);
        const changed = bytes.slice();
        changed[changed.length - 1] ^= 0xff;

        await expect(isDocContentSame(bytes.buffer, changed.buffer)).resolves.toBe(false);
        await expect(isDocContentSame(new Blob([bytes]), new Blob([changed]))).resolves.toBe(false);
    });

    it("keeps comparing text with its encoded bytes and rejects different sizes", async () => {
        const encoded = new TextEncoder().encode("same text").buffer;

        await expect(isDocContentSame(["same ", "text"], encoded)).resolves.toBe(true);
        await expect(isDocContentSame(createTextBlob("same text"), "same text")).resolves.toBe(true);
        await expect(isDocContentSame("same text", "same text!")).resolves.toBe(false);
        await expect(isDocContentSame("", new ArrayBuffer(0))).resolves.toBe(true);
    });
});
