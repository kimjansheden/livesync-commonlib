import { DEFAULT_SETTINGS, type ObsidianLiveSyncSettings } from "@lib/common/types";
import { describe, expect, it } from "vitest";
import {
    clearConnectionSettings,
    copyConnectionSettings,
    hasConnectionSettings,
    restoreConnectionSettings,
    type PersistedConnectionSettings,
} from "./connectionSettingsPersistence";

function settings(patch: Partial<ObsidianLiveSyncSettings> = {}): ObsidianLiveSyncSettings {
    return { ...DEFAULT_SETTINGS, ...patch };
}

const completeConnection: PersistedConnectionSettings = {
    couchDB_DBNAME: "synthetic-db",
    couchDB_PASSWORD: "synthetic-password",
    couchDB_URI: "https://couch.example.invalid",
    couchDB_USER: "synthetic-user",
    accessKey: "SYNTHETICACCESSKEY",
    bucket: "synthetic-bucket",
    endpoint: "https://objects.example.invalid",
    region: "auto",
    secretKey: "synthetic-secret-key",
    useCustomRequestHandler: true,
    bucketCustomHeaders: "X-Synthetic: marker",
    couchDB_CustomHeaders: "Authorization: synthetic-token",
    useJWT: true,
    jwtKey: "synthetic-jwt-key",
    jwtAlgorithm: "HS512",
    jwtKid: "synthetic-kid",
    jwtExpDuration: 17,
    jwtSub: "synthetic-sub",
    useRequestAPI: true,
    bucketPrefix: "synthetic-prefix/",
    forcePathStyle: true,
};

describe("connection settings persistence", () => {
    it.each([
        "couchDB_URI",
        "couchDB_USER",
        "couchDB_PASSWORD",
        "couchDB_DBNAME",
        "accessKey",
        "secretKey",
        "bucket",
        "endpoint",
    ] as const)("treats a non-empty %s as a connection requiring encryption", (field) => {
        expect(hasConnectionSettings({ ...copyConnectionSettings(settings()), [field]: "synthetic-marker" })).toBe(
            true
        );
    });

    it("does not create an encrypted connection blob for empty compatibility fields", () => {
        expect(hasConnectionSettings(copyConnectionSettings(settings()))).toBe(false);
    });

    it("copies every connection field without sharing the source object", () => {
        const source = settings(completeConnection);
        const copied = copyConnectionSettings(source);

        expect(copied).toEqual(completeConnection);
        expect(copied).not.toBe(source);
    });

    it("clears every persisted compatibility field", () => {
        const target = settings(completeConnection);

        clearConnectionSettings(target);

        expect(copyConnectionSettings(target)).toEqual({
            couchDB_DBNAME: "",
            couchDB_PASSWORD: "",
            couchDB_URI: "",
            couchDB_USER: "",
            accessKey: "",
            bucket: "",
            endpoint: "",
            region: "",
            secretKey: "",
            useCustomRequestHandler: false,
            bucketCustomHeaders: "",
            couchDB_CustomHeaders: "",
            useJWT: false,
            jwtKey: "",
            jwtAlgorithm: "HS256",
            jwtKid: "",
            jwtExpDuration: 0,
            jwtSub: "",
            useRequestAPI: false,
            bucketPrefix: "",
            forcePathStyle: false,
        });
    });

    it("restores every recognised field and ignores unrelated decrypted properties", () => {
        const target = settings();

        restoreConnectionSettings(target, {
            ...completeConnection,
            unrelated: "must-not-be-applied",
        } as Partial<PersistedConnectionSettings>);

        expect(copyConnectionSettings(target)).toEqual(completeConnection);
        expect(target).not.toHaveProperty("unrelated");
    });
});
