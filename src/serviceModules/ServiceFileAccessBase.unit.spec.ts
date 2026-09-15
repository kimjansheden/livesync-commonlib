import { describe, expect, it, vi } from "vitest";
import { ServiceFileAccessBase } from "./ServiceFileAccessBase";
import type { FilePath, FilePathWithPrefix } from "@lib/common/types";

type MockFolder = {
    kind: "folder";
    path: string;
    parent: MockFolder | null;
    children: MockNode[];
};

type MockFile = {
    kind: "file";
    path: string;
    parent: MockFolder;
};

type MockNode = MockFolder | MockFile;

describe("ServiceFileAccessBase deletion recursion", () => {
    it("does not delete parent folder when siblings still exist", async () => {
        const root: MockFolder = { kind: "folder", path: "/", parent: null, children: [] };
        const folder: MockFolder = { kind: "folder", path: "A.inbox", parent: root, children: [] };
        const target: MockFile = { kind: "file", path: "A.inbox/target.md", parent: folder };
        const sibling: MockFile = { kind: "file", path: "A.inbox/sibling.md", parent: folder };

        root.children = [folder];
        folder.children = [target, sibling];

        const nodes = new Map<string, MockNode>([
            [root.path, root],
            [folder.path, folder],
            [target.path, target],
            [sibling.path, sibling],
        ]);

        const removeNode = (node: MockNode) => {
            nodes.delete(node.path);
            if (node.parent) {
                node.parent.children = node.parent.children.filter((child) => child.path !== node.path);
            }
        };

        const trashedPaths: string[] = [];
        const vaultAccess = {
            getPath: (file: MockNode) => file.path,
            getAbstractFileByPath: (path: string) => nodes.get(path) ?? null,
            isFile: (item: unknown): item is MockFile => !!item && (item as MockNode).kind === "file",
            isFolder: (item: unknown): item is MockFolder => !!item && (item as MockNode).kind === "folder",
            trash: vi.fn(async (item: MockNode) => {
                trashedPaths.push(item.path);
                removeNode(item);
                return await Promise.resolve();
            }),
            delete: vi.fn(async (item: MockNode) => {
                removeNode(item);
                return await Promise.resolve();
            }),
        };

        const service = new ServiceFileAccessBase<any>({
            API: { addLog: vi.fn() } as any,
            appLifecycle: { onFirstInitialise: { addHandler: vi.fn() } } as any,
            fileProcessing: { commitPendingFileEvents: { addHandler: vi.fn() } } as any,
            vault: { isTargetFile: vi.fn().mockResolvedValue(true) } as any,
            setting: {
                currentSettings: vi.fn().mockReturnValue({
                    trashInsteadDelete: true,
                    doNotDeleteFolder: false,
                }),
            } as any,
            storageEventManager: {
                beginWatch: vi.fn(),
                appendQueue: vi.fn(),
                isWaiting: vi.fn(),
                waitForIdle: vi.fn(),
                restoreState: vi.fn(),
            } as any,
            storageAccessManager: {} as any,
            vaultAccess: vaultAccess as any,
        });

        await service.deleteVaultItem(target.path as FilePathWithPrefix);

        expect(trashedPaths).toContain(target.path);
        expect(trashedPaths).not.toContain(folder.path);
        expect(nodes.has(sibling.path)).toBe(true);
    });
});

describe("ServiceFileAccessBase readFileAuto", () => {
    function createServiceForRead(vaultAccessOverrides: Record<string, unknown>) {
        const vaultAccess = {
            getAbstractFileByPath: vi.fn(async () => await Promise.resolve({ kind: "file", path: "assets/image.png" })),
            isFile: vi.fn((item: unknown) => !!item && (item as { kind?: string }).kind === "file"),
            vaultReadAuto: vi.fn(async () => await Promise.resolve(new ArrayBuffer(0))),
            vaultRead: vi.fn(async () => await Promise.resolve("")),
            ...vaultAccessOverrides,
        };

        const service = new ServiceFileAccessBase<any>({
            API: { addLog: vi.fn() } as any,
            appLifecycle: { onFirstInitialise: { addHandler: vi.fn() } } as any,
            fileProcessing: { commitPendingFileEvents: { addHandler: vi.fn() } } as any,
            vault: { isTargetFile: vi.fn().mockResolvedValue(true) } as any,
            setting: {
                currentSettings: vi.fn().mockReturnValue({
                    trashInsteadDelete: true,
                    doNotDeleteFolder: false,
                }),
            } as any,
            storageEventManager: {
                beginWatch: vi.fn(),
                appendQueue: vi.fn(),
                isWaiting: vi.fn(),
                waitForIdle: vi.fn(),
                restoreState: vi.fn(),
            } as any,
            storageAccessManager: {} as any,
            vaultAccess: vaultAccess as any,
        });

        return { service, vaultAccess };
    }

    it("reads binary files through vaultReadAuto", async () => {
        const expected = Uint8Array.from([0xde, 0xad, 0xbe, 0xef]).buffer;
        const { service, vaultAccess } = createServiceForRead({
            getAbstractFileByPath: vi.fn(async () => await Promise.resolve({ kind: "file", path: "assets/image.png" })),
            vaultReadAuto: vi.fn(async () => await Promise.resolve(expected)),
            vaultRead: vi.fn(() => {
                throw new Error("vaultRead should not be used by readFileAuto");
            }),
        });

        const result = await service.readFileAuto("assets/image.png");

        expect(result).toBe(expected);
        expect(vaultAccess.vaultReadAuto).toHaveBeenCalledTimes(1);
        expect(vaultAccess.vaultRead).not.toHaveBeenCalled();
    });

    it("reads markdown files through vaultReadAuto", async () => {
        const expected = "# hello";
        const { service, vaultAccess } = createServiceForRead({
            getAbstractFileByPath: vi.fn(async () => await Promise.resolve({ kind: "file", path: "notes/test.md" })),
            vaultReadAuto: vi.fn(async () => await Promise.resolve(expected)),
            vaultRead: vi.fn(() => {
                throw new Error("vaultRead should not be used by readFileAuto");
            }),
        });

        const result = await service.readFileAuto("notes/test.md");

        expect(result).toBe(expected);
        expect(vaultAccess.vaultReadAuto).toHaveBeenCalledTimes(1);
        expect(vaultAccess.vaultRead).not.toHaveBeenCalled();
    });
});

describe("ServiceFileAccessBase renameFile", () => {
    it("renames through the vault adapter and marks the target as touched", async () => {
        const nativeFile = {
            kind: "file",
            path: "Calculus.md",
            stat: { ctime: 1, mtime: 2, size: 4, type: "file" },
        };
        const vaultAccess = {
            getAbstractFileByPath: vi.fn().mockResolvedValue(nativeFile),
            isFile: vi.fn((item: unknown) => !!item && (item as { kind?: string }).kind === "file"),
            vaultRename: vi.fn(async (file: typeof nativeFile, newPath: string) => {
                file.path = newPath;
                return file;
            }),
            nativeFileToUXFileInfoStub: vi.fn((file: typeof nativeFile) => ({
                name: file.path.split("/").pop() ?? file.path,
                path: file.path as FilePath,
                stat: file.stat,
            })),
            touch: vi.fn().mockResolvedValue(undefined),
        };
        const service = new ServiceFileAccessBase<any>({
            API: { addLog: vi.fn() } as any,
            appLifecycle: { onFirstInitialise: { addHandler: vi.fn() } } as any,
            fileProcessing: { commitPendingFileEvents: { addHandler: vi.fn() } } as any,
            vault: { isTargetFile: vi.fn().mockResolvedValue(true) } as any,
            setting: { currentSettings: vi.fn().mockReturnValue({}) } as any,
            storageEventManager: {
                beginWatch: vi.fn(),
                appendQueue: vi.fn(),
                isWaiting: vi.fn(),
                waitForIdle: vi.fn(),
                restoreState: vi.fn(),
            } as any,
            storageAccessManager: {} as any,
            vaultAccess: vaultAccess as any,
        });

        await expect(
            service.renameFile("Calculus.md" as FilePathWithPrefix, "calculus.md" as FilePathWithPrefix)
        ).resolves.toEqual(expect.objectContaining({ path: "calculus.md" }));

        expect(vaultAccess.vaultRename).toHaveBeenCalledWith(nativeFile, "calculus.md");
        expect(vaultAccess.touch).toHaveBeenCalledWith("calculus.md");
    });
});

describe("ServiceFileAccessBase hidden removal", () => {
    function createServiceForHiddenRemoval(vaultAccessOverrides: Record<string, unknown>) {
        const vaultAccess = {
            normalisePath: (path: string) => path,
            adapterRemove: vi.fn(async () => {}),
            ...vaultAccessOverrides,
        };
        const service = new ServiceFileAccessBase({
            API: { addLog: vi.fn() },
            appLifecycle: { onFirstInitialise: { addHandler: vi.fn() } },
            fileProcessing: { commitPendingFileEvents: { addHandler: vi.fn() } },
            vault: {},
            setting: { currentSettings: () => ({}) },
            storageEventManager: {},
            storageAccessManager: {},
            vaultAccess: vaultAccess as any,
        } as any);
        return { service, vaultAccess };
    }

    it("reports success when the adapter no longer finds the removed file", async () => {
        // The confirming read is asynchronous: comparing the unresolved promise instead of its value is never
        // null, so a removal which did happen would be reported as a failure.
        const { service, vaultAccess } = createServiceForHiddenRemoval({
            tryAdapterStat: vi.fn(async () => null),
        });

        await expect(service.removeHidden("image.bin")).resolves.toBe(true);

        expect(vaultAccess.adapterRemove).toHaveBeenCalledWith("image.bin");
    });

    it("reports failure when the file is still there after the removal", async () => {
        const { service } = createServiceForHiddenRemoval({
            tryAdapterStat: vi.fn(async () => ({ ctime: 1, mtime: 2, size: 3, type: "file" })),
        });

        await expect(service.removeHidden("image.bin")).resolves.toBe(false);
    });
});

describe("ServiceFileAccessBase appendStorageEvents", () => {
    it("queues the events through the storage event manager used by the watcher", async () => {
        const appendQueue = vi.fn().mockResolvedValue(undefined);
        const service = new ServiceFileAccessBase<any>({
            API: { addLog: vi.fn() } as any,
            appLifecycle: { onFirstInitialise: { addHandler: vi.fn() } } as any,
            fileProcessing: { commitPendingFileEvents: { addHandler: vi.fn() } } as any,
            vault: {} as any,
            setting: {} as any,
            storageEventManager: { appendQueue } as any,
            storageAccessManager: {} as any,
            vaultAccess: {} as any,
        });
        const events = [
            {
                type: "CREATE" as const,
                file: {
                    name: "note.md",
                    path: "note.md" as FilePath,
                    stat: { size: 1, mtime: 1, ctime: 1, type: "file" as const },
                },
                revalidate: true,
            },
        ];

        await service.appendStorageEvents(events);

        expect(appendQueue).toHaveBeenCalledWith(events);
    });
});
