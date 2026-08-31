export type RandomByteFiller = (target: Uint8Array) => Uint8Array;

const fillWithCrypto = (target: Uint8Array): Uint8Array => {
    crypto.getRandomValues(target);
    return target;
};

const FORBIDDEN_PATCH_KEYS = new Set(["__proto__", "constructor", "prototype"]);

export function assertSafePatchKey(key: string): void {
    if (FORBIDDEN_PATCH_KEYS.has(key)) {
        throw new Error(`Unsafe patch key: ${key}`);
    }
}

export function isCloudantEndpointUri(uri: string): boolean {
    try {
        const hostname = new URL(uri).hostname.toLowerCase().replace(/\.$/, "");
        return hostname.endsWith(".cloudant.com") || hostname.endsWith(".cloudantnosqldb.appdomain.cloud");
    } catch {
        return false;
    }
}

export function isTrustedWorkerMessageOrigin(origin: string, expectedOrigin: string): boolean {
    // Dedicated-worker MessageEvents use an empty origin in browsers. Some
    // compatible runtimes provide the owning document's exact origin instead.
    return origin === "" || origin === expectedOrigin;
}

export function stripTrailingSlashes(value: string): string {
    let end = value.length;
    while (value.charCodeAt(end - 1) === 47) {
        end -= 1;
    }
    return value.slice(0, end);
}

export function secureRandomInteger(maxExclusive: number, fillRandomBytes: RandomByteFiller = fillWithCrypto): number {
    if (!Number.isInteger(maxExclusive) || maxExclusive < 1 || maxExclusive > 256) {
        throw new RangeError("maxExclusive must be an integer from 1 through 256.");
    }

    const acceptedRange = 256 - (256 % maxExclusive);
    const randomByte = new Uint8Array(1);
    do {
        fillRandomBytes(randomByte);
    } while (randomByte[0] >= acceptedRange);
    return randomByte[0] % maxExclusive;
}

export function secureRandomHex(byteLength: number, fillRandomBytes: RandomByteFiller = fillWithCrypto): string {
    if (!Number.isInteger(byteLength) || byteLength < 1) {
        throw new RangeError("byteLength must be a positive integer.");
    }

    const randomBytes = fillRandomBytes(new Uint8Array(byteLength));
    if (randomBytes.length !== byteLength) {
        throw new Error("The random source returned an unexpected byte count.");
    }
    return Array.from(randomBytes, (value) => value.toString(16).padStart(2, "0")).join("");
}
