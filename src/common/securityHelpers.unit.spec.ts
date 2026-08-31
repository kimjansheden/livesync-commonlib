import { describe, expect, it, vi } from "vitest";

import {
    assertSafePatchKey,
    isCloudantEndpointUri,
    isTrustedWorkerMessageOrigin,
    secureRandomHex,
    secureRandomInteger,
    stripTrailingSlashes,
} from "./securityHelpers.ts";

describe("security helpers", () => {
    it.each(["__proto__", "constructor", "prototype"])("rejects the unsafe patch key %s", (key) => {
        expect(() => assertSafePatchKey(key)).toThrow(`Unsafe patch key: ${key}`);
    });

    it("accepts ordinary own-property keys", () => {
        expect(() => assertSafePatchKey("safe-key")).not.toThrow();
    });

    it.each([
        "https://account.cloudant.com",
        "https://account.cloudantnosqldb.appdomain.cloud",
        "https://account-bluemix.private.cloudantnosqldb.appdomain.cloud",
        "https://ACCOUNT.CLOUDANT.COM./database",
    ])("recognises an exact IBM Cloudant hostname boundary", (uri) => {
        expect(isCloudantEndpointUri(uri)).toBe(true);
    });

    it.each([
        "https://cloudant.com",
        "https://cloudantnosqldb.appdomain.cloud",
        "https://account.cloudant.com.attacker.example",
        "https://attacker.example/path/.cloudant.com",
        "not a URL containing .cloudant.com",
    ])("rejects a non-Cloudant or malformed URI: %s", (uri) => {
        expect(isCloudantEndpointUri(uri)).toBe(false);
    });

    it("accepts only the dedicated-worker empty origin or the exact owner origin", () => {
        expect(isTrustedWorkerMessageOrigin("", "https://vault.example")).toBe(true);
        expect(isTrustedWorkerMessageOrigin("https://vault.example", "https://vault.example")).toBe(true);
        expect(isTrustedWorkerMessageOrigin("https://vault.example.attacker.test", "https://vault.example")).toBe(
            false
        );
    });

    it.each([
        ["https://example.test///", "https://example.test"],
        ["https://example.test/path", "https://example.test/path"],
        ["////", ""],
        ["", ""],
    ])("removes only trailing slashes from %s", (value, expected) => {
        expect(stripTrailingSlashes(value)).toBe(expected);
    });

    it("uses rejection sampling for a bounded cryptographic integer", () => {
        const values = [250, 7];
        const filler = vi.fn((target: Uint8Array) => {
            target[0] = values.shift() ?? 0;
            return target;
        });

        expect(secureRandomInteger(10, filler)).toBe(7);
        expect(filler).toHaveBeenCalledTimes(2);
    });

    it("accepts both inclusive random-integer bounds", () => {
        expect(secureRandomInteger(1, (target) => target)).toBe(0);
        expect(
            secureRandomInteger(256, (target) => {
                target[0] = 255;
                return target;
            })
        ).toBe(255);
    });

    it.each([0, -1, 257, 1.5])("rejects an invalid random-integer bound: %s", (value) => {
        expect(() => secureRandomInteger(value)).toThrow(RangeError);
    });

    it("encodes cryptographic random bytes as fixed-width hexadecimal", () => {
        const filler = vi.fn((target: Uint8Array) => {
            target.set([0, 15, 16, 255]);
            return target;
        });

        expect(secureRandomHex(4, filler)).toBe("000f10ff");
        expect(filler).toHaveBeenCalledTimes(1);
    });

    it("uses the platform cryptographic source by default for one byte", () => {
        const getRandomValues = vi.spyOn(globalThis.crypto, "getRandomValues").mockImplementation((target) => {
            const bytes = target as Uint8Array;
            bytes[0] = 171;
            return target;
        });

        expect(secureRandomHex(1)).toBe("ab");
        expect(getRandomValues).toHaveBeenCalledTimes(1);
        getRandomValues.mockRestore();
    });

    it("rejects invalid random-byte lengths and malformed random sources", () => {
        expect(() => secureRandomHex(0)).toThrow(RangeError);
        expect(() => secureRandomHex(1.5)).toThrow(RangeError);
        expect(() => secureRandomHex(2, () => new Uint8Array(1))).toThrow("unexpected byte count");
    });
});
