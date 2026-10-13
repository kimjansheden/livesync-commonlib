import { describe, expect, it, vi } from "vitest";

import type { KeyValueDatabase } from "@lib/interfaces/KeyValueDatabase";
import { createServiceContext } from "./ServiceBase";
import { KeyValueDBService, type KeyValueDBDependencies } from "./KeyValueDBService";

vi.mock("octagonal-wheels/promises", async (importOriginal) => {
    const original = (await importOriginal()) as Record<string, unknown>;
    return {
        ...original,
        delay: vi.fn(async () => undefined),
    };
});

class TestKeyValueDBService extends KeyValueDBService {}

describe("KeyValueDBService factory boundary", () => {
    it("does not register application database lifecycle handlers for direct access", () => {
        const onSettingLoaded = vi.fn();
        const onResetDatabase = vi.fn();
        const onDatabaseInitialisation = vi.fn();
        const onUnloadDatabase = vi.fn();
        const onCloseDatabase = vi.fn();
        const dependencies = {
            openKeyValueDatabase: vi.fn(),
            vault: { getVaultName: () => "test-vault" },
            appLifecycle: { onSettingLoaded: { addHandler: onSettingLoaded } },
            databaseEvents: {
                onResetDatabase: { addHandler: onResetDatabase },
                onDatabaseInitialisation: { addHandler: onDatabaseInitialisation },
                onUnloadDatabase: { addHandler: onUnloadDatabase },
                onCloseDatabase: { addHandler: onCloseDatabase },
            },
            registerLifecycleHandlers: false,
        } as unknown as KeyValueDBDependencies;

        new TestKeyValueDBService(createServiceContext(), dependencies);

        expect(onSettingLoaded).not.toHaveBeenCalled();
        expect(onResetDatabase).not.toHaveBeenCalled();
        expect(onDatabaseInitialisation).not.toHaveBeenCalled();
        expect(onUnloadDatabase).not.toHaveBeenCalled();
        expect(onCloseDatabase).not.toHaveBeenCalled();
    });

    it("creates a namespaced store handle before the backing database is initialised", () => {
        const dependencies = {
            openKeyValueDatabase: vi.fn(),
            vault: { getVaultName: () => "test-vault" },
            appLifecycle: { onSettingLoaded: { addHandler: vi.fn() } },
            databaseEvents: {
                onResetDatabase: { addHandler: vi.fn() },
                onDatabaseInitialisation: { addHandler: vi.fn() },
                onUnloadDatabase: { addHandler: vi.fn() },
                onCloseDatabase: { addHandler: vi.fn() },
            },
        } as unknown as KeyValueDBDependencies;
        const service = new TestKeyValueDBService(createServiceContext(), dependencies);

        expect(() => service.openSimpleStore("early-composition")).not.toThrow();
    });

    it("fails store operations promptly instead of waiting for lifecycle initialisation", async () => {
        const dependencies = {
            openKeyValueDatabase: vi.fn(),
            vault: { getVaultName: () => "test-vault" },
            appLifecycle: { onSettingLoaded: { addHandler: vi.fn() } },
            databaseEvents: {
                onResetDatabase: { addHandler: vi.fn() },
                onDatabaseInitialisation: { addHandler: vi.fn() },
                onUnloadDatabase: { addHandler: vi.fn() },
                onCloseDatabase: { addHandler: vi.fn() },
            },
        } as unknown as KeyValueDBDependencies;
        const service = new TestKeyValueDBService(createServiceContext(), dependencies);
        const store = service.openSimpleStore("early-composition");

        await expect(store.get("key")).rejects.toThrow("KeyValueDB is not initialized yet");
    });

    it("opens the database through the factory supplied by its composition", async () => {
        let onSettingLoaded: (() => Promise<boolean>) | undefined;
        const database = {
            get: vi.fn(),
            set: vi.fn(),
            del: vi.fn(),
            clear: vi.fn(),
            keys: vi.fn(),
            close: vi.fn(),
            destroy: vi.fn(),
        } as unknown as KeyValueDatabase;
        const openKeyValueDatabase = vi.fn(async () => database);

        const dependencies = {
            openKeyValueDatabase,
            vault: { getVaultName: () => "test-vault" },
            appLifecycle: {
                onSettingLoaded: {
                    addHandler: (handler: () => Promise<boolean>) => {
                        onSettingLoaded = handler;
                    },
                },
            },
            databaseEvents: {
                onResetDatabase: { addHandler: vi.fn() },
                onDatabaseInitialisation: { addHandler: vi.fn() },
                onUnloadDatabase: { addHandler: vi.fn() },
                onCloseDatabase: { addHandler: vi.fn() },
            },
        } as unknown as KeyValueDBDependencies;
        const service = new TestKeyValueDBService(createServiceContext(), dependencies);
        const store = service.openSimpleStore<string>("composed-before-open");

        expect(onSettingLoaded).toBeDefined();
        await expect(onSettingLoaded!()).resolves.toBe(true);
        expect(openKeyValueDatabase).toHaveBeenCalledWith("test-vault-livesync-kv");
        await store.set("key", "value");
        expect(database.set).toHaveBeenCalledWith("composed-before-open-key", "value");
    });
});

describe("KeyValueDBService of a host which unloads", () => {
    async function createLoadedService() {
        const handlers: Record<string, (argument?: unknown) => Promise<boolean>> = {};
        const register = (name: string) => ({
            addHandler: (handler: (argument?: unknown) => Promise<boolean>) => void (handlers[name] = handler),
        });
        const database = {
            get: vi.fn(async () => "stored"),
            set: vi.fn(async () => "key"),
            del: vi.fn(),
            clear: vi.fn(),
            keys: vi.fn(),
            close: vi.fn(async () => undefined),
            destroy: vi.fn(),
        };
        const openKeyValueDatabase = vi.fn(async () => database as unknown as KeyValueDatabase);
        const context = createServiceContext();
        const service = new TestKeyValueDBService(context, {
            openKeyValueDatabase,
            vault: { getVaultName: () => "test-vault" },
            appLifecycle: { onSettingLoaded: register("onSettingLoaded") },
            databaseEvents: {
                onResetDatabase: register("onResetDatabase"),
                onDatabaseInitialisation: register("onDatabaseInitialisation"),
                onUnloadDatabase: register("onUnloadDatabase"),
                onCloseDatabase: register("onCloseDatabase"),
            },
        } as unknown as KeyValueDBDependencies);
        await handlers.onSettingLoaded();
        const store = service.openSimpleStore<unknown>("queue");
        return { service, store, database, openKeyValueDatabase, handlers, context };
    }

    it("closes the store with the local database and opens it again while the host is loaded", async () => {
        const { store, database, openKeyValueDatabase, handlers } = await createLoadedService();

        await handlers.onCloseDatabase();
        await store.set("snapshot", ["kept"]);
        await expect(handlers.onDatabaseInitialisation()).resolves.toBe(true);

        expect(database.close).toHaveBeenCalled();
        expect(database.set).toHaveBeenCalledWith("queue-snapshot", ["kept"]);
        expect(openKeyValueDatabase).toHaveBeenCalledTimes(2);
    });

    it("keeps the store usable while the host unloads, until its local database is closed", async () => {
        const { store, database, context } = await createLoadedService();
        context.events.emitEvent("plugin-unloaded");

        await store.set("snapshot", ["last before unload"]);

        expect(database.set).toHaveBeenCalledWith("queue-snapshot", ["last before unload"]);
    });

    it.each(["onUnloadDatabase", "onCloseDatabase"])(
        "lets go of the store when %s closes the local database of an unloading host, and never opens it again",
        async (closing) => {
            const { service, store, database, openKeyValueDatabase, handlers, context } = await createLoadedService();
            context.events.emitEvent("plugin-unloaded");

            await handlers[closing]();

            expect(database.close).toHaveBeenCalledOnce();
            await expect(store.set("snapshot", ["late"])).rejects.toThrow("KeyValueDB is not initialized yet");
            await expect(store.get("snapshot")).rejects.toThrow("KeyValueDB is not initialized yet");
            expect(() => service.kvDB).toThrow("KeyValueDB is not initialized yet");
            await expect(handlers.onDatabaseInitialisation()).resolves.toBe(false);
            await expect(handlers.onSettingLoaded()).resolves.toBe(false);
            expect(database.set).not.toHaveBeenCalled();
            expect(openKeyValueDatabase).toHaveBeenCalledOnce();
        }
    );

    it("lets go of the store when the unload completes without a local database having been closed", async () => {
        const { store, database, openKeyValueDatabase, handlers, context } = await createLoadedService();
        context.events.emitEvent("plugin-unloaded");

        context.events.emitEvent("platform-unloaded");
        await Promise.resolve();

        expect(database.close).toHaveBeenCalledOnce();
        await expect(store.set("snapshot", ["late"])).rejects.toThrow("KeyValueDB is not initialized yet");
        await expect(handlers.onDatabaseInitialisation()).resolves.toBe(false);
        expect(openKeyValueDatabase).toHaveBeenCalledOnce();
    });

    it("does not resurrect the store through a database reset which overlaps the unload", async () => {
        const { store, database, openKeyValueDatabase, handlers, context } = await createLoadedService();
        context.events.emitEvent("plugin-unloaded");
        await handlers.onCloseDatabase();
        expect(database.close).toHaveBeenCalledOnce();

        // A reset which arrives after the store was closed for good must not reopen it.
        await expect(handlers.onResetDatabase(undefined)).resolves.toBe(false);

        expect(database.destroy).not.toHaveBeenCalled();
        expect(openKeyValueDatabase).toHaveBeenCalledOnce();
        await expect(store.set("snapshot", ["late"])).rejects.toThrow("KeyValueDB is not initialized yet");
    });

    it("drops a store it had just reopened for a reset when the unload completes during that reopen", async () => {
        const { store, database, openKeyValueDatabase, handlers, context } = await createLoadedService();
        const reopened = { ...database, close: vi.fn(async () => undefined) };
        // The reset passes its entry check, then the unload completes while the store is being reopened.
        openKeyValueDatabase.mockImplementationOnce(async () => {
            context.events.emitEvent("plugin-unloaded");
            context.events.emitEvent("platform-unloaded");
            await Promise.resolve();
            return reopened as unknown as KeyValueDatabase;
        });

        await expect(handlers.onResetDatabase(undefined)).resolves.toBe(false);

        // The freshly opened store is let go of, not kept.
        expect(reopened.close).toHaveBeenCalledOnce();
        await expect(store.set("after-reset", ["late"])).rejects.toThrow("KeyValueDB is not initialized yet");
    });

    it("resets the store normally while the host is loaded", async () => {
        const { store, database, openKeyValueDatabase, handlers } = await createLoadedService();

        await expect(handlers.onResetDatabase(undefined)).resolves.toBe(true);

        expect(database.destroy).toHaveBeenCalledOnce();
        expect(openKeyValueDatabase).toHaveBeenCalledTimes(2);
        await expect(store.set("after-reset", ["ok"])).resolves.toBeUndefined();
    });
});
