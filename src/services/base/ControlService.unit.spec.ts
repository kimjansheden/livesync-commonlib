import { describe, expect, it, vi } from "vitest";
import { ControlService, type ControlServiceDependencies } from "./ControlService.ts";
import { ServiceContext } from "./ServiceBase.ts";

function createControlService(onRealiseSetting: () => Promise<boolean> = async () => true) {
    const observedWhileResumed: boolean[] = [];
    let control: ControlService | undefined;
    const dependencies = {
        appLifecycleService: {
            onLoaded: { addHandler: vi.fn() },
            onSuspending: vi.fn(async () => true),
            isSuspended: vi.fn(() => false),
            onResuming: vi.fn(async () => true),
            onResumed: vi.fn(async () => {
                observedWhileResumed.push(control!.isApplyingSettings());
                return true;
            }),
        },
        settingService: {
            onBeforeRealiseSetting: vi.fn(async () => true),
            onRealiseSetting: vi.fn(onRealiseSetting),
            onSettingRealised: vi.fn(async () => true),
        },
        databaseService: { localDatabase: { refreshSettings: vi.fn() } },
        fileProcessingService: { commitPendingFileEvents: vi.fn(async () => true) },
        replicatorService: {},
        APIService: { addLog: vi.fn() },
    } as unknown as ControlServiceDependencies;
    control = new ControlService(new ServiceContext(), dependencies);
    return { control, observedWhileResumed };
}

describe("ControlService unloading", () => {
    function createUnloadingControlService() {
        const order: string[] = [];
        let unloaded = false;
        const context = new ServiceContext();
        let control: ControlService | undefined;
        const dependencies = {
            appLifecycleService: {
                onLoaded: { addHandler: vi.fn() },
                hasUnloaded: () => unloaded,
                markUnloaded: () => {
                    unloaded = true;
                    order.push("marked");
                },
                onBeforeUnload: vi.fn(async () => {
                    order.push(`before-unload:${control!.hasUnloaded()}`);
                    return true;
                }),
                onAppUnload: vi.fn(async () => []),
                onUnload: vi.fn(async () => true),
                onLoad: vi.fn(async () => true),
                onReady: vi.fn(async () => true),
            },
            settingService: {},
            databaseService: { localDatabaseDirect: undefined },
            fileProcessingService: {},
            replicatorService: {},
            APIService: { addLog: vi.fn() },
        } as unknown as ControlServiceDependencies;
        control = new ControlService(context, dependencies);
        context.events.onceEvent("plugin-unloaded", () => order.push(`event:${control!.hasUnloaded()}`));
        return { control, order, lifecycle: dependencies.appLifecycleService };
    }

    it("loads and becomes ready while it has not been unloaded", async () => {
        const { control, lifecycle } = createUnloadingControlService();

        await expect(control.onLoad()).resolves.toBe(true);
        await expect(control.onReady()).resolves.toBe(true);

        expect(lifecycle.onLoad).toHaveBeenCalledOnce();
        expect(lifecycle.onReady).toHaveBeenCalledOnce();
    });

    it("neither loads nor becomes ready once it has been unloaded", async () => {
        const { control, lifecycle } = createUnloadingControlService();
        await control.onUnload();

        await expect(control.onLoad()).resolves.toBe(false);
        await expect(control.onReady()).resolves.toBe(false);

        expect(lifecycle.onLoad).not.toHaveBeenCalled();
        expect(lifecycle.onReady).not.toHaveBeenCalled();
    });

    it("is not unloaded before the unload begins", () => {
        expect(createUnloadingControlService().control.hasUnloaded()).toBe(false);
    });

    it("is unloaded as soon as the unload is called, before anything is awaited", async () => {
        const { control, order } = createUnloadingControlService();

        const unloading = control.onUnload();

        expect(control.hasUnloaded()).toBe(true);
        await unloading;
        expect(order).toEqual(["marked", "event:true", "before-unload:true"]);
    });
});

describe("ControlService.isApplyingSettings", () => {
    it("reports settings as being applied while applying them resumes the lifecycle, and not afterwards", async () => {
        const { control, observedWhileResumed } = createControlService();
        expect(control.isApplyingSettings()).toBe(false);

        await control.applySettings();

        expect(observedWhileResumed).toEqual([true]);
        expect(control.isApplyingSettings()).toBe(false);
    });

    it("stops reporting settings as being applied when applying them fails", async () => {
        const { control } = createControlService(async () => {
            throw new Error("synthetic failure");
        });

        await expect(control.applySettings()).rejects.toThrow("synthetic failure");

        expect(control.isApplyingSettings()).toBe(false);
    });
});
