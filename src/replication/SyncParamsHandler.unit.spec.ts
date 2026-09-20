import { beforeEach, describe, expect, it, vi } from "vitest";

import { DEFAULT_SYNC_PARAMETERS, ProtocolVersions, type SyncParameters } from "@lib/common/types.ts";
import { base64ToArrayBufferInternalBrowser } from "@lib/string_and_binary/convert.ts";
import {
    clearHandlers,
    createSyncParamsHanderForServer,
    SyncParamsFetchError,
    SyncParamsNotFoundError,
} from "./SyncParamsHandler.ts";

/**
 * The handler creates a new security seed (the PBKDF2 salt) whenever the remote reports that no sync parameters
 * exist. A read which merely failed must therefore never reach the handler as "not found": the new seed would
 * replace the one every journal already on the remote was encrypted with.
 */
describe("SyncParamsHandler", () => {
    const STORED_SALT = "c3ludGhldGljLXNhbHQ=";
    const STORED_SALT_BYTES = new Uint8Array(base64ToArrayBufferInternalBrowser(STORED_SALT));
    const initialParameters = (): SyncParameters => ({
        ...DEFAULT_SYNC_PARAMETERS,
        protocolVersion: ProtocolVersions.ADVANCED_E2EE,
        pbkdf2salt: "",
    });

    let remoteParameters: SyncParameters | undefined;
    let put: ReturnType<typeof vi.fn>;
    let get: ReturnType<typeof vi.fn>;
    let create: ReturnType<typeof vi.fn>;
    let serverCounter = 0;

    const handler = () => {
        serverCounter++;
        return createSyncParamsHanderForServer(`synthetic-server-${serverCounter}`, { put, get, create });
    };

    beforeEach(() => {
        clearHandlers();
        remoteParameters = undefined;
        put = vi.fn(async (params: SyncParameters) => {
            remoteParameters = structuredClone(params);
            return true;
        });
        get = vi.fn(async () => {
            if (!remoteParameters) throw new SyncParamsNotFoundError("Missing sync parameters");
            return structuredClone(remoteParameters);
        });
        create = vi.fn(async () => initialParameters());
    });

    it("creates and stores a security seed when the remote reports no parameters", async () => {
        const salt = await handler().getPBKDF2Salt();

        expect(salt).toBeInstanceOf(Uint8Array);
        expect(salt.byteLength).toBeGreaterThan(0);
        expect(create).toHaveBeenCalledOnce();
        expect(remoteParameters?.pbkdf2salt).toBeTruthy();
        // The seed handed out must be the one which reached the remote, or the next device derives another key.
        expect(salt).toEqual(new Uint8Array(base64ToArrayBufferInternalBrowser(remoteParameters!.pbkdf2salt)));
    });

    it("creates a security seed when the missing-parameters error arrives wrapped in a fetch error", async () => {
        get.mockImplementationOnce(async () => {
            throw SyncParamsFetchError.fromError(new SyncParamsNotFoundError("Missing sync parameters"));
        });

        await expect(handler().getPBKDF2Salt()).resolves.toBeInstanceOf(Uint8Array);
        expect(create).toHaveBeenCalledOnce();
    });

    it("keeps the stored security seed when the parameters can be read", async () => {
        remoteParameters = { ...initialParameters(), pbkdf2salt: STORED_SALT };

        // Asserting the bytes, not only the type: a fresh seed returned beside an untouched remote breaks
        // decryption exactly as badly as overwriting the stored one.
        await expect(handler().getPBKDF2Salt()).resolves.toEqual(STORED_SALT_BYTES);

        expect(put).not.toHaveBeenCalled();
        expect(create).not.toHaveBeenCalled();
        expect(remoteParameters.pbkdf2salt).toBe(STORED_SALT);
    });

    it("writes nothing when the parameters cannot be read", async () => {
        remoteParameters = { ...initialParameters(), pbkdf2salt: STORED_SALT };
        get.mockImplementation(async () => {
            throw new SyncParamsFetchError("Could not read remote sync parameters", {
                cause: new Error("synthetic object store failure"),
            });
        });

        await expect(handler().getPBKDF2Salt()).rejects.toBeInstanceOf(SyncParamsFetchError);

        expect(create).not.toHaveBeenCalled();
        expect(put).not.toHaveBeenCalled();
        expect(remoteParameters.pbkdf2salt).toBe(STORED_SALT);
    });

    it("reports a failed read as a failed fetch instead of an empty parameter set", async () => {
        get.mockImplementation(async () => {
            throw new SyncParamsFetchError("Could not read remote sync parameters");
        });

        await expect(handler().fetch()).resolves.toBe(false);
        expect(put).not.toHaveBeenCalled();
    });

    it("adds a security seed to parameters which were stored without one", async () => {
        remoteParameters = initialParameters();

        const salt = await handler().getPBKDF2Salt();

        expect(create).not.toHaveBeenCalled();
        expect(put).toHaveBeenCalledOnce();
        expect(remoteParameters.pbkdf2salt).toBeTruthy();
        expect(salt).toEqual(new Uint8Array(base64ToArrayBufferInternalBrowser(remoteParameters.pbkdf2salt)));
    });
});
