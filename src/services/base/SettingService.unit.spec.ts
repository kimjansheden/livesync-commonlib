import { describe, expect, it, vi } from "vitest";
import {
    CURRENT_SETTING_VERSION,
    DEFAULT_SETTINGS,
    LOG_LEVEL_INFO,
    LOG_LEVEL_NOTICE,
    REMOTE_COUCHDB,
    REMOTE_MINIO,
    SALT_OF_PASSPHRASE,
} from "@lib/common/types";
import { SettingService } from "./SettingService";
import { ServiceContext } from "./ServiceBase";
import type { ObsidianLiveSyncSettings } from "@lib/common/types";
import { ConnectionStringParser } from "@lib/common/ConnectionString";
import { encryptString } from "@lib/encryption/stringEncryption";

class TestSettingService extends SettingService<ServiceContext> {
    lastSavedSetting?: ObsidianLiveSyncSettings;
    readonly localItems = new Map<string, string>();
    protected setItem(key: string, value: string): void {
        this.localItems.set(key, value);
    }
    protected getItem(key: string): string {
        return this.localItems.get(key) ?? "";
    }
    protected deleteItem(key: string): void {
        this.localItems.delete(key);
    }
    protected saveData(setting: ObsidianLiveSyncSettings): Promise<void> {
        this.lastSavedSetting = JSON.parse(JSON.stringify(setting));
        return Promise.resolve();
    }
    protected loadData(): Promise<ObsidianLiveSyncSettings | undefined> {
        return Promise.resolve(undefined);
    }
}

function createService(onDisplayLanguageChanged?: (language: ObsidianLiveSyncSettings["displayLanguage"]) => void) {
    const service = new TestSettingService(new ServiceContext(), {
        APIService: {
            getSystemVaultName: vi.fn(() => "vault"),
            getAppID: vi.fn(() => "app"),
            confirm: {
                askString: vi.fn(() => Promise.resolve("")),
            },
            addLog: vi.fn(),
        } as any,
        onDisplayLanguageChanged,
    } as any);
    service.settings = {
        ...DEFAULT_SETTINGS,
        remoteConfigurations: {},
        activeConfigurationId: "",
    };
    return service;
}

describe("SettingService", () => {
    it("delegates the loaded display language to the host", async () => {
        const onDisplayLanguageChanged = vi.fn();
        const service = createService(onDisplayLanguageChanged);
        vi.spyOn(service as any, "loadData").mockResolvedValue({
            ...DEFAULT_SETTINGS,
            displayLanguage: "ja",
        });

        await service.loadSettings();

        expect(onDisplayLanguageChanged).toHaveBeenCalledOnce();
        expect(onDisplayLanguageChanged).toHaveBeenCalledWith("ja");
    });

    it("exposes exact device-local configuration without placing it in the settings document", () => {
        const service = createService();

        service.setDeviceLocalConfig("legacy-version-marker", "12");

        expect(service.getDeviceLocalConfig("legacy-version-marker")).toBe("12");
        expect(service.localItems.get("legacy-version-marker")).toBe("12");
        expect(service.currentSettings()).not.toHaveProperty("legacy-version-marker");

        service.deleteDeviceLocalConfig("legacy-version-marker");
        expect(service.getDeviceLocalConfig("legacy-version-marker")).toBe("");
    });

    it("adjustSettings should migrate legacy remote settings into remoteConfigurations", async () => {
        const service = createService();
        const settings = {
            ...DEFAULT_SETTINGS,
            remoteConfigurations: {},
            activeConfigurationId: "",
            remoteType: REMOTE_COUCHDB,
            couchDB_URI: "http://localhost:5984",
            couchDB_USER: "user",
            couchDB_PASSWORD: "password",
            couchDB_DBNAME: "vault",
        };

        const adjusted = await service.adjustSettings(settings);

        expect(adjusted.remoteConfigurations["legacy-couchdb"]?.uri).toContain(
            "sls+http://user:password@localhost:5984"
        );
        expect(adjusted.activeConfigurationId).toBe("legacy-couchdb");
    });

    it("applyExternalSettings should merge current settings and migrate imported legacy remote settings", async () => {
        const service = createService();
        const saveSpy = vi.spyOn(service, "saveSettingData").mockResolvedValue();

        await service.applyExternalSettings(
            {
                couchDB_URI: "http://localhost:5984",
                couchDB_USER: "user",
                couchDB_PASSWORD: "password",
                couchDB_DBNAME: "vault",
            },
            true
        );

        expect(service.currentSettings().remoteConfigurations["legacy-couchdb"]?.uri).toContain(
            "sls+http://user:password@localhost:5984"
        );
        expect(service.currentSettings().activeConfigurationId).toBe("legacy-couchdb");
        expect(saveSpy).toHaveBeenCalledTimes(1);
    });

    it("saveSettingData should encrypt remote configuration URIs before persisting", async () => {
        const service = createService();
        const plainURI = "sls+http://user:password@localhost:5984/?db=vault";
        service.settings = {
            ...service.settings,
            remoteConfigurations: {
                r1: {
                    id: "r1",
                    name: "Primary",
                    uri: plainURI,
                    isEncrypted: false,
                },
            },
            activeConfigurationId: "r1",
        };

        await service.saveSettingData();

        const persisted = service.lastSavedSetting;
        expect(persisted).toBeDefined();
        expect(persisted?.remoteConfigurations.r1.isEncrypted).toBe(true);
        expect(persisted?.remoteConfigurations.r1.uri).not.toBe(plainURI);
    });

    it("saveSettingData should not mutate in-memory remote configuration URIs", async () => {
        const service = createService();
        const plainURI = "sls+http://user:password@localhost:5984/?db=vault";
        service.settings = {
            ...service.settings,
            remoteConfigurations: {
                r1: {
                    id: "r1",
                    name: "Primary",
                    uri: plainURI,
                    isEncrypted: false,
                },
            },
            activeConfigurationId: "r1",
        };

        await service.saveSettingData();

        expect(service.currentSettings().remoteConfigurations.r1.uri).toBe(plainURI);
        expect(service.currentSettings().remoteConfigurations.r1.isEncrypted).toBe(false);
    });

    it.each(["accessKey", "secretKey", "bucket", "endpoint"] as const)(
        "saveSettingData should encrypt an Object Storage connection identified only by %s",
        async (field) => {
            const service = createService();
            service.settings = {
                ...service.settings,
                [field]: "synthetic-marker",
            };

            await service.saveSettingData();

            expect(service.lastSavedSetting?.encryptedCouchDBConnection).not.toBe("");
            expect(service.lastSavedSetting?.[field]).toBe("");
        }
    );

    it("saveSettingData should encrypt, scrub, and restore every Object Storage field", async () => {
        const service = createService();
        const expected = {
            accessKey: "SYNTHETICACCESSKEY",
            secretKey: "synthetic-secret-key",
            bucket: "synthetic-bucket",
            endpoint: "https://objects.example.invalid",
            region: "auto",
            useCustomRequestHandler: true,
            bucketCustomHeaders: "X-Synthetic: marker",
            bucketPrefix: "synthetic-prefix/",
            forcePathStyle: true,
        };
        service.settings = { ...service.settings, ...expected };

        await service.saveSettingData();

        const persisted = structuredClone(service.lastSavedSetting!);
        expect(persisted.encryptedCouchDBConnection).not.toBe("");
        for (const field of Object.keys(expected) as (keyof typeof expected)[]) {
            expect(persisted[field]).not.toBe(expected[field]);
        }

        const decrypted = await service.decryptSettings(persisted);
        expect(decrypted).toMatchObject(expected);
    });

    it("saveSettingData should encrypt, scrub, and restore auxiliary CouchDB credentials", async () => {
        const service = createService();
        const expected = {
            couchDB_URI: "https://couch.example.invalid",
            couchDB_USER: "synthetic-user",
            couchDB_PASSWORD: "synthetic-password",
            couchDB_DBNAME: "synthetic-db",
            couchDB_CustomHeaders: "Authorization: synthetic-token",
            useJWT: true,
            jwtKey: "synthetic-jwt-key",
            jwtAlgorithm: "HS512" as const,
            jwtKid: "synthetic-kid",
            jwtExpDuration: 17,
            jwtSub: "synthetic-sub",
            useRequestAPI: true,
        };
        service.settings = { ...service.settings, ...expected };

        await service.saveSettingData();

        const persisted = structuredClone(service.lastSavedSetting!);
        expect(persisted.encryptedCouchDBConnection).not.toBe("");
        expect(persisted.couchDB_CustomHeaders).toBe("");
        expect(persisted.jwtKey).toBe("");
        expect(persisted.jwtKid).toBe("");
        expect(persisted.jwtSub).toBe("");

        const decrypted = await service.decryptSettings(persisted);
        expect(decrypted).toMatchObject(expected);
    });

    it("decryptSettings should restore encrypted remote configuration URIs", async () => {
        const service = createService();
        const plainURI = "sls+s3://ak:sk@example.com/?endpoint=https%3A%2F%2Fexample.com&bucket=vault";
        service.settings = {
            ...service.settings,
            remoteConfigurations: {
                r1: {
                    id: "r1",
                    name: "Primary",
                    uri: plainURI,
                    isEncrypted: false,
                },
            },
            activeConfigurationId: "r1",
        };

        await service.saveSettingData();
        const encrypted = JSON.parse(JSON.stringify(service.lastSavedSetting!)) as ObsidianLiveSyncSettings;

        const decrypted = await service.decryptSettings(encrypted);

        expect(decrypted.remoteConfigurations.r1.isEncrypted).toBe(false);
        expect(decrypted.remoteConfigurations.r1.uri).toBe(plainURI);
    });

    it("decryptSettings should repair a plain-text remote URI that is incorrectly marked as encrypted", async () => {
        const service = createService();
        const plainURI = "sls+http://user:password@localhost:5984/?db=vault";

        const decrypted = await service.decryptSettings({
            ...DEFAULT_SETTINGS,
            remoteConfigurations: {
                r1: {
                    id: "r1",
                    name: "Primary",
                    uri: plainURI,
                    isEncrypted: true,
                },
            },
            activeConfigurationId: "r1",
        });

        expect(decrypted.remoteConfigurations.r1.uri).toBe(plainURI);
        expect(decrypted.remoteConfigurations.r1.isEncrypted).toBe(false);
    });

    it("loadSettings should apply P2P active remote fields without overwriting remoteType", async () => {
        const service = createService();
        const couchURI = ConnectionStringParser.serialize({
            type: "couchdb",
            settings: {
                ...DEFAULT_SETTINGS,
                couchDB_URI: "http://localhost:5984",
                couchDB_USER: "user",
                couchDB_PASSWORD: "password",
                couchDB_DBNAME: "vault",
            },
        });
        const p2pURI = ConnectionStringParser.serialize({
            type: "p2p",
            settings: {
                ...DEFAULT_SETTINGS,
                P2P_roomID: "123-456-789-abc",
                P2P_passphrase: "passphrase",
                P2P_relays: "wss://exp-relay.vrtmrz.net/",
            },
        });

        vi.spyOn(service as any, "loadData").mockResolvedValue({
            ...DEFAULT_SETTINGS,
            remoteType: REMOTE_COUCHDB,
            remoteConfigurations: {
                couch: {
                    id: "couch",
                    name: "CouchDB",
                    uri: couchURI,
                    isEncrypted: false,
                },
                p2p: {
                    id: "p2p",
                    name: "P2P",
                    uri: p2pURI,
                    isEncrypted: false,
                },
            },
            activeConfigurationId: "couch",
            P2P_ActiveRemoteConfigurationId: "p2p",
        } as ObsidianLiveSyncSettings);

        await service.loadSettings();

        expect(service.currentSettings().remoteType).toBe(REMOTE_COUCHDB);
        expect(service.currentSettings().P2P_roomID).toBe("123-456-789-abc");
        expect(service.currentSettings().P2P_ActiveRemoteConfigurationId).toBe("p2p");
    });

    it("loadSettings should persist the detected schema version without changing explicit sync choices", async () => {
        const service = createService();
        const storedSettings: Partial<ObsidianLiveSyncSettings> = {
            ...DEFAULT_SETTINGS,
            liveSync: true,
            syncOnSave: true,
            syncOnStart: true,
            remoteConfigurations: {
                couch: {
                    id: "couch",
                    name: "CouchDB",
                    uri: "sls+http://user:password@localhost:5984/?db=vault",
                    isEncrypted: false,
                },
            },
            activeConfigurationId: "couch",
        };
        delete storedSettings.settingVersion;
        vi.spyOn(service as any, "loadData").mockResolvedValue(storedSettings);

        await service.loadSettings();

        expect(service.lastSavedSetting).toMatchObject({
            settingVersion: CURRENT_SETTING_VERSION,
            liveSync: true,
            syncOnSave: true,
            syncOnStart: true,
        });
    });

    it("keeps a non-empty legacy default-equivalent store unconfigured when isConfigured is absent", async () => {
        const service = createService();
        vi.spyOn(service as any, "loadData").mockResolvedValue({
            liveSync: DEFAULT_SETTINGS.liveSync,
        });

        await service.loadSettings();

        expect(service.currentSettings().isConfigured).toBe(false);
    });

    describe("an active remote configuration encrypted under an earlier passphrase", () => {
        const EARLIER_PASSPHRASE = "synthetic-earlier-passphrase";
        const connection = {
            accessKey: "SYNTHETICACCESSKEY",
            secretKey: "synthetic-secret-key",
            bucket: "synthetic-bucket",
            endpoint: "https://objects.example.invalid",
            region: "auto",
        };

        const encryptedEarlier = async (uri: string) =>
            await encryptString(uri, EARLIER_PASSPHRASE + SALT_OF_PASSPHRASE);

        /** Settings as stored after the connection was encrypted again under this device's current passphrase. */
        async function storedSettings(overrides: Partial<ObsidianLiveSyncSettings> = {}) {
            const service = createService();
            service.settings = { ...service.settings, ...connection, remoteType: REMOTE_MINIO };
            await service.saveSettingData();
            const staleURI = ConnectionStringParser.serialize({
                type: "s3",
                settings: { ...DEFAULT_SETTINGS, ...connection, bucket: "synthetic-earlier-bucket" },
            });
            return {
                ...structuredClone(service.lastSavedSetting!),
                remoteType: REMOTE_MINIO,
                remoteConfigurations: {
                    "legacy-s3": {
                        id: "legacy-s3",
                        name: "S3 Remote",
                        uri: await encryptedEarlier(staleURI),
                        isEncrypted: true,
                    },
                },
                activeConfigurationId: "legacy-s3",
                ...overrides,
            } as ObsidianLiveSyncSettings;
        }

        const noticesOf = (service: TestSettingService) =>
            vi
                .mocked(service["APIService"].addLog)
                .mock.calls.filter(([, level]) => (level ?? LOG_LEVEL_INFO) >= LOG_LEVEL_NOTICE);

        it("is recreated from the decrypted connection without a notice", async () => {
            const service = createService();

            const decrypted = await service.decryptSettings(await storedSettings());

            const recreated = decrypted.remoteConfigurations["legacy-s3"];
            expect(recreated).toMatchObject({ id: "legacy-s3", name: "S3 Remote", isEncrypted: false });
            expect(ConnectionStringParser.parse(recreated.uri)).toMatchObject({ type: "s3", settings: connection });
            expect(noticesOf(service)).toEqual([]);
        });

        it("is activated at load without a notice, and encrypted under the current passphrase when saved", async () => {
            const service = createService();
            const stored = await storedSettings();
            vi.spyOn(service as any, "loadData").mockResolvedValue(stored);

            await service.loadSettings();
            expect(service.currentSettings()).toMatchObject({ remoteType: REMOTE_MINIO, ...connection });
            expect(noticesOf(service)).toEqual([]);

            await service.saveSettingData();
            const saved = structuredClone(service.lastSavedSetting!);
            expect(saved.remoteConfigurations["legacy-s3"].isEncrypted).toBe(true);
            const next = createService();
            const reloaded = await next.decryptSettings(saved);
            expect(ConnectionStringParser.parse(reloaded.remoteConfigurations["legacy-s3"].uri)).toMatchObject({
                settings: connection,
            });
            expect(noticesOf(next)).toEqual([]);
        });

        it("leaves an entry which is not the active one as it is", async () => {
            const service = createService();
            const stored = await storedSettings();
            const inactiveURI = await encryptedEarlier("sls+s3://synthetic-other-remote");
            stored.remoteConfigurations.other = { id: "other", name: "Other", uri: inactiveURI, isEncrypted: true };

            const decrypted = await service.decryptSettings(stored);

            expect(decrypted.remoteConfigurations.other).toEqual({
                id: "other",
                name: "Other",
                uri: inactiveURI,
                isEncrypted: true,
            });
            expect(decrypted.remoteConfigurations["legacy-s3"].isEncrypted).toBe(false);
        });

        it("is not recreated from a connection of another type", async () => {
            const service = createService();
            const stored = await storedSettings({ remoteType: REMOTE_COUCHDB });

            const decrypted = await service.decryptSettings(stored);

            expect(decrypted.remoteConfigurations["legacy-s3"]).toMatchObject({ isEncrypted: true });
            expect(noticesOf(service).length).toBeGreaterThan(0);
        });

        it("is only recreated from a connection which was decrypted", async () => {
            const service = createService();
            const stored = await storedSettings({ encryptedCouchDBConnection: "", ...connection });

            const decrypted = await service.decryptSettings(stored);

            expect(decrypted.remoteConfigurations["legacy-s3"]).toMatchObject({ isEncrypted: true });
        });

        it("is not recreated when the connection cannot be decrypted either", async () => {
            const service = createService();
            const stored = await storedSettings();
            stored.encryptedCouchDBConnection = await encryptedEarlier(JSON.stringify(connection));

            const decrypted = await service.decryptSettings(stored);

            expect(decrypted.remoteConfigurations["legacy-s3"]).toMatchObject({ isEncrypted: true });
            expect(noticesOf(service).length).toBeGreaterThan(0);
        });
    });

    it("saveSettingData should apply patches from onBeforeSaveSettingData handlers", async () => {
        const service = createService();

        (service.onBeforeSaveSettingData as any).addHandler(async () => ({ tweakModified: 100 }), 10);
        (service.onBeforeSaveSettingData as any).addHandler(async () => ({ tweakModified: 200 }), 20);

        await service.saveSettingData();

        expect(service.lastSavedSetting?.tweakModified).toBe(200);
    });
});
