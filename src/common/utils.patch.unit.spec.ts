import { describe, expect, it } from "vitest";

import { applyPatch, generatePatchObj, mergeObject } from "./utils.patch.ts";

const maliciousRecord = (key: string): Record<string, unknown> =>
    JSON.parse(`{"${key}":{"polluted":true}}`) as Record<string, unknown>;

describe("object patch key safety", () => {
    it.each(["__proto__", "constructor", "prototype"])("rejects %s at the patch boundary", (key) => {
        expect(() => applyPatch({}, maliciousRecord(key))).toThrow(`Unsafe patch key: ${key}`);
        expect(({} as { polluted?: boolean }).polluted).toBeUndefined();
    });

    it("rejects a dangerous key recursively", () => {
        expect(() => applyPatch({ nested: {} }, { nested: maliciousRecord("__proto__") })).toThrow(
            "Unsafe patch key: __proto__"
        );
        expect(({} as { polluted?: boolean }).polluted).toBeUndefined();
    });

    it("rejects dangerous keys while generating or merging objects", () => {
        expect(() => generatePatchObj({}, maliciousRecord("constructor"))).toThrow("Unsafe patch key: constructor");
        expect(() => mergeObject({}, maliciousRecord("prototype"))).toThrow("Unsafe patch key: prototype");
    });

    it("still applies ordinary nested patches", () => {
        expect(applyPatch({ nested: { before: true } }, { nested: { after: true } })).toEqual({
            nested: { before: true, after: true },
        });
    });

    it("does not treat inherited properties as patch targets or operators", () => {
        expect(applyPatch({}, { toString: { nested: true } })).toEqual({ toString: { nested: true } });
        expect(applyPatch({}, { safe: Object.create({ "\u0001__SWAP": "inherited" }) })).toEqual({
            safe: {},
        });
    });
});
