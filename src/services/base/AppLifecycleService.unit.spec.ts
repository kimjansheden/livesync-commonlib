import { describe, expect, it } from "vitest";
import { AppLifecycleService, type AppLifecycleServiceDependencies } from "./AppLifecycleService.ts";
import { ServiceContext } from "./ServiceBase.ts";

class TestAppLifecycleService extends AppLifecycleService<ServiceContext> {
    performRestart(): void {}
    askRestart(): void {}
    scheduleRestart(): void {}
    isReloadingScheduled(): boolean {
        return false;
    }
}

function createLifecycle(isConfigured = true) {
    return new TestAppLifecycleService(new ServiceContext(), {
        settingService: { currentSettings: () => ({ isConfigured }) },
    } as unknown as AppLifecycleServiceDependencies);
}

describe("AppLifecycleService unloaded state", () => {
    it("is ready and not suspended while a configured host runs", () => {
        const lifecycle = createLifecycle();
        lifecycle.markIsReady();

        expect(lifecycle.hasUnloaded()).toBe(false);
        expect(lifecycle.isReady()).toBe(true);
        expect(lifecycle.isSuspended()).toBe(false);
    });

    it("is suspended and not ready once the host has unloaded", () => {
        const lifecycle = createLifecycle();
        lifecycle.markIsReady();

        lifecycle.markUnloaded();

        expect(lifecycle.hasUnloaded()).toBe(true);
        expect(lifecycle.isReady()).toBe(false);
        expect(lifecycle.isSuspended()).toBe(true);
    });

    it("stays suspended and not ready, whatever marks it ready or resumed afterwards", () => {
        const lifecycle = createLifecycle();
        lifecycle.markUnloaded();

        lifecycle.markIsReady();
        lifecycle.setSuspended(false);

        expect(lifecycle.isReady()).toBe(false);
        expect(lifecycle.isSuspended()).toBe(true);
    });
});
