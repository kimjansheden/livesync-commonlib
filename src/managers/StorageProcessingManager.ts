import type { FilePathWithPrefix } from "@lib/common/models/db.type";
import type { UXFileInfoStub } from "@lib/common/models/fileaccess.type";
import type { IStorageAccessManager } from "@lib/interfaces/StorageAccess";
import { serialized } from "octagonal-wheels/concurrency/lock";
import type { FileWithFileStat, FileWithStatAsProp } from "@lib/common/models/fileaccess.type";
const fileLockPrefix = "file-lock:";
export const STORAGE_INGESTION_BARRIER_MS = 500;

type TouchedFile = {
    key: string;
    touchedAt: number;
};

export class StorageAccessManager implements IStorageAccessManager {
    constructor(private readonly now: () => number = Date.now) {}

    processingFiles: Set<FilePathWithPrefix> = new Set();
    processWriteFile<T>(file: UXFileInfoStub | FilePathWithPrefix, proc: () => Promise<T>): Promise<T> {
        const path = typeof file === "string" ? file : file.path;
        return serialized(`${fileLockPrefix}${path}`, async () => {
            try {
                this.processingFiles.add(path);
                return await proc();
            } finally {
                this.processingFiles.delete(path);
            }
        });
    }
    processReadFile<T>(file: UXFileInfoStub | FilePathWithPrefix, proc: () => Promise<T>): Promise<T> {
        const path = typeof file === "string" ? file : file.path;
        return serialized(`${fileLockPrefix}${path}`, async () => {
            try {
                this.processingFiles.add(path);
                return await proc();
            } finally {
                this.processingFiles.delete(path);
            }
        });
    }
    isFileProcessing(file: UXFileInfoStub | FilePathWithPrefix): boolean {
        const path = typeof file === "string" ? file : file.path;
        return this.processingFiles.has(path);
    }
    private touchedFiles: TouchedFile[] = [];

    touch(file: FileWithFileStat | FileWithStatAsProp): void {
        const key =
            "stat" in file
                ? `${file.path}-${file.stat.mtime}-${file.stat.size}`
                : `${file.path}-${file.mtime}-${file.size}`;
        this.touchedFiles.unshift({ key, touchedAt: this.now() });
        this.touchedFiles = this.touchedFiles.slice(0, 100);
    }

    recentlyTouched(file: FileWithStatAsProp | FileWithFileStat) {
        const key =
            "stat" in file
                ? `${file.path}-${file.stat.mtime}-${file.stat.size}`
                : `${file.path}-${file.mtime}-${file.size}`;
        const barrierStart = this.now() - STORAGE_INGESTION_BARRIER_MS;
        this.touchedFiles = this.touchedFiles.filter((entry) => entry.touchedAt >= barrierStart);
        return this.touchedFiles.some((entry) => entry.key === key);
    }
    clearTouched() {
        this.touchedFiles = [];
    }
}
