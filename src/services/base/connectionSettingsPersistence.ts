import type { BucketSyncSetting, CouchDBConnection, ObsidianLiveSyncSettings } from "@lib/common/types";

export type PersistedConnectionSettings = CouchDBConnection & BucketSyncSetting;

const CONNECTION_MARKER_FIELDS = [
    "couchDB_URI",
    "couchDB_USER",
    "couchDB_PASSWORD",
    "couchDB_DBNAME",
    "accessKey",
    "secretKey",
    "bucket",
    "endpoint",
] as const satisfies readonly (keyof PersistedConnectionSettings)[];

/** Return whether the flat compatibility projection contains a remote connection. */
export function hasConnectionSettings(settings: PersistedConnectionSettings): boolean {
    return CONNECTION_MARKER_FIELDS.some((field) => settings[field] !== "");
}

/** Copy every flat connection field into the object encrypted at rest. */
export function copyConnectionSettings(settings: PersistedConnectionSettings): PersistedConnectionSettings {
    return {
        couchDB_DBNAME: settings.couchDB_DBNAME,
        couchDB_PASSWORD: settings.couchDB_PASSWORD,
        couchDB_URI: settings.couchDB_URI,
        couchDB_USER: settings.couchDB_USER,
        accessKey: settings.accessKey,
        bucket: settings.bucket,
        endpoint: settings.endpoint,
        region: settings.region,
        secretKey: settings.secretKey,
        useCustomRequestHandler: settings.useCustomRequestHandler,
        bucketCustomHeaders: settings.bucketCustomHeaders,
        couchDB_CustomHeaders: settings.couchDB_CustomHeaders,
        useJWT: settings.useJWT,
        jwtKey: settings.jwtKey,
        jwtAlgorithm: settings.jwtAlgorithm,
        jwtKid: settings.jwtKid,
        jwtExpDuration: settings.jwtExpDuration,
        jwtSub: settings.jwtSub,
        useRequestAPI: settings.useRequestAPI,
        bucketPrefix: settings.bucketPrefix,
        forcePathStyle: settings.forcePathStyle,
    };
}

/** Remove every flat connection field from the copy written to settings storage. */
export function clearConnectionSettings(settings: ObsidianLiveSyncSettings): void {
    Object.assign(settings, {
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
    } satisfies PersistedConnectionSettings);
}

/** Restore only the recognised flat connection fields from a decrypted object. */
export function restoreConnectionSettings(
    settings: ObsidianLiveSyncSettings,
    decrypted: Partial<PersistedConnectionSettings>
): void {
    const recognised = copyConnectionSettings({
        ...copyConnectionSettings(settings),
        ...decrypted,
    });
    Object.assign(settings, recognised);
}
