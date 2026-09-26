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
