import { FilePublicationCoordinator } from "./FilePublicationCoordinator";
import { LOG_LEVEL_INFO, LOG_LEVEL_NOTICE, LOG_LEVEL_VERBOSE } from "octagonal-wheels/common/logger";
import { serialized } from "octagonal-wheels/concurrency/lock";
import type {
    AnyEntry,
    FileEventItem,
    FilePath,
    FilePathWithPrefix,
    LoadedEntry,
    MetaEntry,
    UXFileInfo,
    UXFileInfoStub,
    UXFolderInfo,
    UXInternalFileInfoStub,
    UXStat,
} from "@lib/common/types";
import {
    compareMTime,
    createBlob,
    delay,
    fireAndForget,
    getDocDataAsArray,
    isDocContentSame,
    isTextBlob,
    isTextDocument,
    readAsBlob,
    readContent,
} from "@lib/common/utils";
import { EVENT_CONFLICT_CANCELLED } from "@lib/events/coreEvents";
import { shouldBeIgnored, stripAllPrefixes } from "@lib/string_and_binary/path";
import { Semaphore } from "octagonal-wheels/concurrency/semaphore";
import type { LiveSyncEventHub } from "@lib/hub/hub";
import type { IFileHandler } from "@lib/interfaces/FileHandler.ts";
import { ServiceModuleBase } from "@lib/serviceModules/ServiceModuleBase";
import type { APIService } from "@lib/services/base/APIService.ts";
import { type DatabaseFileAccess } from "@lib/interfaces/DatabaseFileAccess.ts";
import type { StorageAccess } from "@lib/interfaces/StorageAccess.ts";
import type { FileProcessingService } from "@lib/services/base/FileProcessingService.ts";
import type { ReplicationService } from "@lib/services/base/ReplicationService.ts";
import type { ConflictService } from "@lib/services/base/ConflictService.ts";
import type { PathService } from "@lib/services/base/PathService.ts";
import type { SettingService } from "@lib/services/base/SettingService.ts";
import type { VaultService } from "@lib/services/base/VaultService.ts";
import { getStoragePathFromUXFileInfo } from "@lib/common/typeUtils";
import { EVEN, TARGET_IS_NEW } from "@lib/common/models/shared.const.symbols";
import { tryGetFilePath } from "@lib/common/utils.doc";
import type {
    FileReflectionProvenance,
    FileReflectionProvenanceRecord,
} from "@lib/interfaces/FileReflectionProvenance.ts";

export interface ServiceFileHandlerDependencies {
    events: LiveSyncEventHub;
    API: APIService;
    databaseFileAccess: DatabaseFileAccess;
    storageAccess: StorageAccess;
    fileProcessing: FileProcessingService;
    replication: ReplicationService;
    conflict: ConflictService;
    path: PathService;
    setting: SettingService;
    vault: VaultService;
    /**
     * Device-local record of the exact database revision reflected in storage.
     *
     * This is optional for compatibility hosts. Maintained hosts should provide
     * it so edits made while a document is conflicted extend the displayed
     * branch instead of whichever branch PouchDB currently selects as winner.
     * The host must finish opening its backing store before it dispatches file
     * or replication events; provenance does not hide lifecycle violations by
     * waiting for readiness.
     */
    fileReflectionProvenance?: FileReflectionProvenance;
}

async function isIncomingTextClearExtension(
    incomingContent: string | string[] | Blob | ArrayBuffer,
    localContent: string | string[] | Blob | ArrayBuffer
): Promise<boolean> {
    const incomingBlob = createBlob(incomingContent);
    const localBlob = createBlob(localContent);
    if (!isTextBlob(incomingBlob) || !isTextBlob(localBlob)) {
        return false;
    }
    const incomingText = await incomingBlob.text();
    const localText = await localBlob.text();
    // Every text extends empty text, so an emptied local file would always look like a clean extension and be
    // overwritten without preserving the emptying.
    if (localText.length === 0) {
        return false;
    }
    return incomingText.startsWith(localText) || incomingText.endsWith(localText);
}

/** Delays before an empty stored file is checked again; the content usually lands within milliseconds. */
const EMPTY_STORE_RECHECK_DELAYS_MS = [3_000, 15_000] as const;

/** Binary files of at least this size are written to storage in parts instead of as one buffer. */
const LARGE_BINARY_STREAM_BYTES = 16 * 1024 * 1024;

/** Files of at least this size may be recognised as unchanged from the recorded reflection instead of by content. */
const RECOGNISE_REFLECTED_STORAGE_BYTES = 1024 * 1024;

/**
 * Entries with at least this many chunks are written in parts whatever their recorded size says.
 * A recorded size which is wrong is exactly the case this must survive.
 */
const LARGE_BINARY_STREAM_CHUNKS = 128;

async function serializedByKeys<T>(keys: readonly string[], callback: () => Promise<T>): Promise<T> {
    const [key, ...remainingKeys] = keys;
    if (key === undefined) return await callback();
    return await serialized(key, () => serializedByKeys(remainingKeys, callback));
}

function getParentPath(path: string): string {
    const lastSeparator = path.lastIndexOf("/");
    return lastSeparator < 0 ? "" : path.slice(0, lastSeparator);
}

function isFolderInfo(info: UXFileInfoStub | UXFolderInfo | null): info is UXFolderInfo {
    return info?.isFolder === true;
}

type RestoredFileEventAction =
    | { kind: "none" }
    | { kind: "store"; file: UXFileInfoStub }
    | { kind: "delete"; path: FilePath; baseRevision?: string }
    | { kind: "rename"; file: UXFileInfoStub; oldPath: FilePathWithPrefix };

export abstract class ServiceFileHandlerBase
    extends ServiceModuleBase<ServiceFileHandlerDependencies>
    implements IFileHandler
{
    private events: LiveSyncEventHub;
    private databaseFileAccess: DatabaseFileAccess;
    private storageAccess: StorageAccess;
    private conflict: ConflictService;
    private path: PathService;
    private setting: SettingService;
    private vault: VaultService;
    private fileReflectionProvenance?: FileReflectionProvenance;
    constructor(services: ServiceFileHandlerDependencies) {
        super(services);
        this.events = services.events;
        this.databaseFileAccess = services.databaseFileAccess;
        this.storageAccess = services.storageAccess;
        this.conflict = services.conflict;
        this.path = services.path;
        this.setting = services.setting;
        this.vault = services.vault;
        this.fileReflectionProvenance = services.fileReflectionProvenance;
        services.fileProcessing.processFileEvent.addHandler(this._anyHandlerProcessesFileEvent.bind(this), 100);
        services.replication.processSynchroniseResult.addHandler(this._anyProcessReplicatedDoc.bind(this), 100);
    }
    private coordinator?: FilePublicationCoordinator;
    private get writeCoordinator(): FilePublicationCoordinator {
        return (this.coordinator ??= new FilePublicationCoordinator({
            store: this.fileReflectionProvenance!,
            normalise: (path) => this.storage.normalisePath(path),
            stat: (path) => this.storage.statHidden(path),
        }));
    }
    get db() {
        return this.databaseFileAccess;
    }
    get storage() {
        return this.storageAccess;
    }
    private async getProvenance(path: FilePathWithPrefix): Promise<FileReflectionProvenanceRecord | undefined> {
        if (!this.fileReflectionProvenance) return undefined;
        const record = await this.writeCoordinator.get(path);
        if (!record || record.pendingPublication) return undefined;
        const entry = await this.db.fetchEntryMeta(path, record.revision, true);
        if (entry && !entry._deleted && !entry.deleted) return record;
        if (!record.pendingPublication) await this.writeCoordinator.delete(path);
        return undefined;
    }

    private async setProvenance(
        path: FilePathWithPrefix,
        revision: string | undefined,
        mtime?: number,
        reflected = false,
        token?: string
    ): Promise<void> {
        if (!this.fileReflectionProvenance || !revision) return;
        await this.writeCoordinator.reflect(path, revision, mtime, reflected, token);
    }

    private async deleteProvenance(path: FilePathWithPrefix): Promise<void> {
        if (this.fileReflectionProvenance) await this.writeCoordinator.delete(path);
    }

    /**
     * Whether storage still holds exactly the revision this device last reflected between storage and the database.
     *
     * The storage event caused by our own write can arrive after the touch barrier. Recognising it from the recorded
     * revision, modification time and size avoids reading the whole file again, which for a large attachment on a
     * mobile device costs as much memory as the transfer itself.
     */
    private async isStorageUnchangedSinceReflected(
        file: UXFileInfoStub | UXInternalFileInfoStub,
        expectedRevision?: string
    ): Promise<boolean> {
        if (!this.fileReflectionProvenance) return false;
        // Recognising our own write by revision, size and modification time saves reading a large file twice.
        // Smaller files keep the ordinary content comparison, which also survives a coarse filesystem clock.
        if (!file.stat || file.stat.size < RECOGNISE_REFLECTED_STORAGE_BYTES) return false;
        let record: FileReflectionProvenanceRecord | undefined;
        try {
            record = await this.writeCoordinator.get(file.path as FilePathWithPrefix);
        } catch {
            return false;
        }
        if (!record || record.pendingPublication || record.observedStorageMtime === undefined) return false;
        if (expectedRevision !== undefined && record.revision !== expectedRevision) return false;
        // An event can carry the stat from when a long write began, so the current stat decides.
        const stat = await this.storage.stat(file.path);
        if (!stat || stat.mtime !== record.observedStorageMtime) return false;
        const current = await this.db.fetchEntryMeta(file as UXFileInfoStub, undefined, true);
        if (!current || current._deleted || current.deleted) return false;
        if (current._rev !== record.revision || current.size !== stat.size) return false;
        return (await this.db.getConflictedRevs(file as UXFileInfoStub)).length === 0;
    }

    /**
     * Load a database entry and its content for comparison with storage.
     *
     * Binary content is assembled in batches into one buffer and its chunk text is not kept, so an unchanged
     * large attachment is compared byte for byte instead of as base64 text against raw bytes, which never
     * matches. Like `fetchEntry`, this returns false when the content cannot be loaded.
     */
    private async fetchEntryForComparison(
        file: UXFileInfoStub | UXInternalFileInfoStub,
        revision?: string
    ): Promise<{ entry: MetaEntry | LoadedEntry; content: string | string[] | ArrayBuffer } | false> {
        if (this.db.fetchBinaryContentFromMeta) {
            const meta = await this.db.fetchEntryMeta(file as UXFileInfoStub, revision, true);
            if (meta && !meta.deleted && !meta._deleted && !isTextDocument(meta)) {
                const binary = await this.db.fetchBinaryContentFromMeta(meta);
                if (binary === false) return false;
                if (binary.status === "ok") return { entry: meta, content: binary.data };
            }
        }
        const loaded = await this.db.fetchEntry(file as UXFileInfoStub, revision, true, true);
        return loaded === false ? false : { entry: loaded, content: getDocDataAsArray(loaded.data) };
    }

    private async findUniqueContentRevision(file: UXFileInfo): Promise<string | undefined> {
        try {
            const revisions = await this.db.findContentRevisions(file, file.body);
            return revisions.length === 1 ? revisions[0] : undefined;
        } catch (ex) {
            this._log(`Could not reconstruct file reflection provenance for ${file.path}`, LOG_LEVEL_VERBOSE);
            this._log(ex, LOG_LEVEL_VERBOSE);
            return undefined;
        }
    }

    private async getProvenBaseRevision(
        file: UXFileInfo,
        preferredPath?: FilePathWithPrefix
    ): Promise<string | undefined> {
        const path = (preferredPath ?? file.path) as FilePathWithPrefix;
        const recorded =
            (await this.getProvenance(path)) ??
            (preferredPath && preferredPath !== file.path ? await this.getProvenance(file.path) : undefined);
        // A stored record identifies the branch which produced the displayed
        // file. Its current content may legitimately have been edited to equal
        // another branch, so content matching must never override that identity.
        if (recorded) {
            return recorded.revision;
        }
        const matched = await this.findUniqueContentRevision(file);
        if (matched) {
            await this.setProvenance(path, matched, file.stat.mtime);
            return matched;
        }
        return undefined;
    }

    getPath(entry: AnyEntry): FilePathWithPrefix {
        return this.path.getPath(entry);
    }
    getPathWithoutPrefix(entry: AnyEntry): FilePathWithPrefix {
        return stripAllPrefixes(this.path.getPath(entry));
    }

    async readFileFromStub(file: UXFileInfoStub | UXFileInfo) {
        if ("body" in file && file.body) {
            return file;
        }
        const readFile = await this.storage.readStubContent(file);
        if (!readFile) {
            throw new Error(`File ${file.path} is not exist on the storage`);
        }
        return readFile;
    }
    private async infoToStub<T extends UXFileInfoStub | UXFileInfo | UXInternalFileInfoStub>(
        info: null | T | FilePathWithPrefix | FilePath
    ): Promise<T | UXFileInfoStub | null> {
        if (info == null) return null;
        const file = typeof info === "string" ? await this.storage.getFileStub(info) : info;
        return file;
    }

    async storeFileToDB(
        info: UXFileInfoStub | UXFileInfo | UXInternalFileInfoStub | FilePathWithPrefix,
        force: boolean = false,
        onlyChunks: boolean = false,
        /** Path this file was renamed from, so an unfinished copy written under it is still recognised. */
        preferredBasePath?: FilePathWithPrefix
    ): Promise<boolean> {
        return await this.storeFileToDBFromRevision(info, force, onlyChunks, preferredBasePath);
    }

    async storeFileToDBWithBaseRevision(
        info: UXFileInfoStub | UXFileInfo | FilePathWithPrefix,
        baseRevision: string,
        createIfDifferent: boolean = true
    ): Promise<boolean> {
        const file = await this.infoToStub(info);
        if (file == null) {
            this._log(`File ${tryGetFilePath(info)} is not exist on the storage`, LOG_LEVEL_VERBOSE);
            return false;
        }
        if (file.isInternal) {
            this._log(
                `Internal file ${file.path} is not allowed to be stored through the ordinary file handler`,
                LOG_LEVEL_VERBOSE
            );
            return false;
        }

        if (await this.getPendingPublicationRevision(file.path as FilePathWithPrefix)) {
            // Storage holds a partial copy this device is writing. Storing it on a base revision would publish
            // the partial content as the resolution of a conflict.
            this._log(
                `Storage holds an unfinished copy of ${file.path}; it is not stored on a base revision`,
                LOG_LEVEL_NOTICE
            );
            return false;
        }

        const [baseEntry, currentEntry, conflictedRevisions] = await Promise.all([
            this.db.fetchEntryMeta(file, baseRevision, true),
            this.db.fetchEntryMeta(file, undefined, true),
            this.db.getConflictedRevs(file),
        ]);
        const liveRevisions = new Set([
            ...(currentEntry && currentEntry._rev ? [currentEntry._rev] : []),
            ...conflictedRevisions,
        ]);
        if (!baseEntry || baseEntry._rev !== baseRevision || !liveRevisions.has(baseRevision)) {
            this._log(
                `Could not store ${file.path} on revision ${baseRevision}; the selected revision is no longer live`,
                LOG_LEVEL_NOTICE
            );
            return false;
        }

        const readFile = await this.readFileFromStub(file);
        if (!baseEntry.deleted && !baseEntry._deleted) {
            const loadedBase = await this.db.fetchEntry(file, baseRevision, true, true);
            if (loadedBase && (await isDocContentSame(getDocDataAsArray(loadedBase.data), readFile.body))) {
                await this.setProvenance(file.path, baseRevision, readFile.stat.mtime);
                await this.conflict.queueCheckFor(file.path);
                return true;
            }
        }
        if (!createIfDifferent) {
            this._log(
                `Could not mark ${file.path} as revision ${baseRevision}; the storage content differs`,
                LOG_LEVEL_NOTICE
            );
            return false;
        }

        const storedRevision = await this.db.storeWithBaseRevision(readFile, baseRevision, true);
        if (storedRevision === false) {
            return false;
        }
        await this.setProvenance(file.path, storedRevision, readFile.stat.mtime);
        await this.conflict.queueCheckFor(file.path);
        return true;
    }

    private async storeFileToDBFromRevision(
        info: UXFileInfoStub | UXFileInfo | UXInternalFileInfoStub | FilePathWithPrefix,
        force: boolean = false,
        onlyChunks: boolean = false,
        preferredBasePath?: FilePathWithPrefix
    ): Promise<boolean> {
        const file = await this.infoToStub(info);
        if (file == null) {
            this._log(`File ${tryGetFilePath(info)} is not exist on the storage`, LOG_LEVEL_VERBOSE);
            return false;
        }
        // const file = item.args.file;
        if (file.isInternal) {
            this._log(
                `Internal file ${file.path} is not allowed to be processed on processFileEvent`,
                LOG_LEVEL_VERBOSE
            );
            return false;
        }
        if (preferredBasePath && preferredBasePath !== file.path && this.fileReflectionProvenance) {
            await this.writeCoordinator.move(preferredBasePath, file.path as FilePathWithPrefix);
        }
        if (await this.getPendingPublicationRevision(file.path as FilePathWithPrefix)) {
            return this.recoverMissingPublication(file.path as FilePathWithPrefix);
        }
        // Chunk-only repair does not create a document revision and therefore
        // does not change which revision storage represents. It runs after the unfinished-copy check, so a
        // partial copy is not split into chunks.
        if (onlyChunks) {
            const readFile = await this.readFileFromStub(file);
            return await this.db.createChunks(readFile, force, true);
        }

        if (!force && (await this.isStorageUnchangedSinceReflected(file))) {
            this._log(`File ${file.path} is not changed since this device last reflected it`, LOG_LEVEL_VERBOSE);
            return true;
        }
        const readFile = await this.readFileFromStub(file);
        // First, check the file on the database
        let loadedEntry = await this.fetchEntryForComparison(file);
        const entry = loadedEntry === false ? false : loadedEntry.entry;
        const conflictedRevs = await this.db.getConflictedRevs(file);
        const isConflicted = conflictedRevs.length > 0;

        if (isConflicted) {
            const baseRevision = await this.getProvenBaseRevision(readFile, preferredBasePath);
            if (baseRevision) {
                const baseEntry = await this.db.fetchEntry(file, baseRevision, true, true);
                if (baseEntry && (await isDocContentSame(getDocDataAsArray(baseEntry.data), readFile.body)) && !force) {
                    await this.setProvenance(file.path, baseRevision, readFile.stat.mtime);
                    this._log(`File ${file.path} is not changed on its displayed conflict branch`, LOG_LEVEL_VERBOSE);
                    return true;
                }
                const storedRevision = await this.db.storeWithBaseRevision(readFile, baseRevision, true);
                if (storedRevision === false) return false;
                if (preferredBasePath && preferredBasePath !== file.path) {
                    await this.deleteProvenance(preferredBasePath);
                }
                await this.setProvenance(file.path, storedRevision, readFile.stat.mtime);
                await this.conflict.queueCheckFor(file.path);
                return true;
            }
            // Missing chunks can make the winning entry body unavailable while its metadata
            // and revision tree remain readable. Preserving the local bytes as a sibling only
            // requires the exact winning revision; it does not require trusting its content.
            const currentEntry = entry || (await this.db.fetchEntryMeta(file, undefined, true));
            const currentRevision = currentEntry && currentEntry._rev;
            if (!currentRevision) {
                this._log(
                    `Could not preserve the unknown conflict branch for ${file.path}; no current revision is available`,
                    LOG_LEVEL_NOTICE
                );
                await this.conflict.queueCheckFor(file.path);
                return false;
            }
            const storedRevision = await this.db.storeAsConflictedRevisionWithResult(readFile, currentRevision, true);
            if (storedRevision === false) {
                this._log(`Could not preserve the unknown conflict branch for ${file.path}`, LOG_LEVEL_NOTICE);
                await this.conflict.queueCheckFor(file.path);
                return false;
            }
            if (preferredBasePath && preferredBasePath !== file.path) {
                await this.deleteProvenance(preferredBasePath);
            }
            await this.setProvenance(file.path, storedRevision, readFile.stat.mtime);
            await this.conflict.queueCheckFor(file.path);
            return true;
        }

        if (!entry || entry.deleted || entry._deleted) {
            // If the file is not exist on the database, then it should be created.
            const storedRevision = await this.db.storeWithBaseRevision(readFile, entry && entry._rev, true);
            if (storedRevision === false) return false;
            if (preferredBasePath && preferredBasePath !== file.path) {
                await this.deleteProvenance(preferredBasePath);
            }
            await this.setProvenance(file.path, storedRevision, readFile.stat.mtime);
            this.recheckIfStoredEmpty(readFile);
            return true;
        }

        // entry is exist on the database, check the difference between the file and the entry.

        let shouldApplied = false;
        if (!force && !onlyChunks) {
            // 1. if the time stamp is far different, then it should be updated.
            // Note: This checks only the mtime with the resolution reduced to 2 seconds.
            //       2 seconds it for the ZIP file's mtime. If not, we cannot backup the vault as the ZIP file.
            //       This is hardcoded on `compareMtime` of `src/common/utils.ts`.
            if (this.path.compareFileFreshness(file, entry) !== EVEN) {
                shouldApplied = true;
            }
            // 2. if not, the content should be checked.
            if (!shouldApplied) {
                if (loadedEntry !== false && (await isDocContentSame(loadedEntry.content, readFile.body))) {
                    // Timestamp is different but the content is same. therefore, two timestamps should be handled as same.
                    // So, mark the changes are same.
                    this.path.markChangesAreSame(readFile, readFile.stat.mtime, entry.mtime);
                } else {
                    shouldApplied = true;
                }
            }

            if (!shouldApplied) {
                await this.setProvenance(file.path, entry._rev, readFile.stat.mtime);
                this._log(`File ${file.path} is not changed`, LOG_LEVEL_VERBOSE);
                return true;
            }
        }
        // The compared content is not needed while the new revision is split and stored.
        loadedEntry = false;
        const storedRevision = await this.db.storeWithBaseRevision(readFile, entry._rev, true);
        if (storedRevision === false) return false;
        if (preferredBasePath && preferredBasePath !== file.path) {
            await this.deleteProvenance(preferredBasePath);
        }
        await this.setProvenance(file.path, storedRevision, readFile.stat.mtime);
        this.recheckIfStoredEmpty(readFile);
        return true;
    }

    private readonly emptyStoreRechecks = new Set<string>();

    /**
     * Check a file again shortly after it was stored while empty.
     *
     * Obsidian on Android can report a file as empty while concurrent writes are in progress, and its
     * content may reach storage moments later under nearly the same modification time. No further
     * storage event is raised for that, and the scan compares modification times at a 2-second
     * resolution, so the local change would otherwise stay unsynchronised until the file is edited
     * again. Storing again goes through the ordinary content comparison.
     */
    private recheckIfStoredEmpty(readFile: UXFileInfo) {
        if (createBlob(readFile.body).size !== 0) return;
        const path = readFile.path;
        if (this.emptyStoreRechecks.has(path)) return;
        this.emptyStoreRechecks.add(path);
        fireAndForget(async () => {
            try {
                for (const wait of EMPTY_STORE_RECHECK_DELAYS_MS) {
                    await delay(wait);
                    const stat = await this.storage.stat(path);
                    if (!stat) return;
                    if (stat.size === 0) continue;
                    this._log(`${path} was stored while empty and has content now; storing it again`, LOG_LEVEL_INFO);
                    await this.storeFileToDB(path as FilePathWithPrefix);
                    return;
                }
            } catch (ex) {
                this._log(`Could not check ${path} again after storing it while empty`, LOG_LEVEL_NOTICE);
                this._log(ex, LOG_LEVEL_VERBOSE);
            } finally {
                this.emptyStoreRechecks.delete(path);
            }
        });
    }

    /** Store a deletion on the revision storage displayed, so every other live branch is kept as a conflict. */
    private async deleteOnRevision(path: FilePathWithPrefix, revision: string): Promise<boolean> {
        const storedRevision = await this.db.storeDeletionWithBaseRevision(path, revision);
        if (storedRevision === false) return false;
        await this.deleteProvenance(path);
        await this.conflict.queueCheckFor(path);
        return true;
    }

    async deleteFileFromDB(info: UXFileInfoStub | UXInternalFileInfoStub | FilePath): Promise<boolean> {
        const file = await this.infoToStub(info);
        const path = (typeof info === "string" ? info : tryGetFilePath(info)) as FilePathWithPrefix | undefined;
        if (path !== undefined && (await this.getPendingPublicationRevision(path))) {
            return this.recoverMissingPublication(path);
        }
        if (file == null) {
            // infoToStub -> getFileStub stats the storage, but in the offline-scanner
            // `delete-db` path the file is by definition already gone from storage, so the
            // stub is always null and the delete silently no-ops (returns false). No
            // tombstone ever reaches the database and the next scan resurrects the file.
            // Fall back to a path-based database delete, the same approach the CLI `rm`
            // command uses (databaseFileAccess.delete accepts a bare path).
            if (path === undefined) {
                this._log(`File ${tryGetFilePath(info)} is not exist on the storage`, LOG_LEVEL_VERBOSE);
                return false;
            }
            const entryByPath = await this.db.fetchEntry(path as FilePathWithPrefix, undefined, true, true);
            if (!entryByPath || entryByPath.deleted || entryByPath._deleted) {
                this._log(
                    `File ${path} is not exist on the storage nor the database (or already deleted)`,
                    LOG_LEVEL_VERBOSE
                );
                return false;
            }
            const conflictedRevs = await this.db.getConflictedRevs(path);
            if (conflictedRevs.length > 0) {
                const provenance = await this.getProvenance(path);
                if (!provenance) {
                    this._log(
                        `The deleted storage file ${path} has conflicts, but its displayed revision is unknown; preserving every database branch`,
                        LOG_LEVEL_NOTICE
                    );
                    await this.conflict.queueCheckFor(path);
                    return true;
                }
                return await this.deleteOnRevision(path, provenance.revision);
            }
            this._log(`File ${path} is missing on storage; deleting from the database by path`, LOG_LEVEL_INFO);
            const deleted = await this.db.delete(path);
            if (deleted) await this.deleteProvenance(path);
            return deleted;
        }
        // const file = item.args.file;
        if (file.isInternal) {
            this._log(
                `Internal file ${file.path} is not allowed to be processed on processFileEvent`,
                LOG_LEVEL_VERBOSE
            );
            return false;
        }
        // First, check the file on the database
        const entry = await this.db.fetchEntry(file, undefined, true, true);
        if (!entry || entry.deleted || entry._deleted) {
            this._log(`File ${file.path} is not exist or already deleted on the database`, LOG_LEVEL_VERBOSE);
            return false;
        }
        // Check the file is already conflicted. if so, only the conflicted one should be deleted.
        const conflictedRevs = await this.db.getConflictedRevs(file);
        if (conflictedRevs.length > 0) {
            let baseRevision = (await this.getProvenance(file.path))?.revision;
            if (!baseRevision) {
                try {
                    const readFile = await this.readFileFromStub(file);
                    baseRevision = await this.findUniqueContentRevision(readFile);
                } catch {
                    // A deletion event can arrive after storage has removed the
                    // file, so content reconstruction is only opportunistic.
                }
            }
            if (!baseRevision) {
                this._log(
                    `The deleted storage file ${file.path} has conflicts, but its displayed revision is unknown; preserving every database branch`,
                    LOG_LEVEL_NOTICE
                );
                await this.conflict.queueCheckFor(file.path);
                return true;
            }
            const storedRevision = await this.db.storeDeletionWithBaseRevision(file.path, baseRevision);
            if (storedRevision === false) return false;
            await this.deleteProvenance(file.path);
            await this.conflict.queueCheckFor(file.path);
            return true;
        }
        // Otherwise, the file should be deleted simply. This is the previous behaviour.
        const deleted = await this.db.delete(file);
        if (deleted) await this.deleteProvenance(file.path);
        return deleted;
    }

    async renameFileInDB(info: UXFileInfoStub | UXFileInfo, oldPath: FilePath | FilePathWithPrefix): Promise<boolean> {
        const newPath = getStoragePathFromUXFileInfo(info);
        const [oldDocumentId, newDocumentId] = await Promise.all([
            this.path.path2id(oldPath),
            this.path.path2id(newPath),
        ]);

        if (oldDocumentId === newDocumentId) {
            this._log(`Updating the stored path for case-only rename: ${oldPath} -> ${newPath}`, LOG_LEVEL_VERBOSE);
            return await this.storeFileToDBFromRevision(info, true, false, oldPath as FilePathWithPrefix);
        }

        const oldEntry = await this.db.fetchEntryMeta(oldPath, undefined, true);
        const newEntry = await this.db.fetchEntryMeta(newPath, undefined, true);
        if (newEntry && !newEntry.deleted && !newEntry._deleted) {
            this._log(
                `Refusing to overwrite the existing database entry while renaming ${oldPath} to ${newPath}`,
                LOG_LEVEL_NOTICE
            );
            return false;
        }
        // The source path is passed on, so an unfinished copy written under it is recognised and never
        // published under the new name.
        if (!(await this.storeFileToDB(info, true, false, oldPath as FilePathWithPrefix))) {
            this._log(`Failed to store rename target; preserving source in the database: ${oldPath}`, LOG_LEVEL_NOTICE);
            return false;
        }
        if (!oldEntry || oldEntry.deleted || oldEntry._deleted) {
            this._log(`Rename source is not present in the database: ${oldPath}`, LOG_LEVEL_VERBOSE);
            return true;
        }

        const oldConflicts = await this.db.getConflictedRevs(oldPath);
        if (oldConflicts.length > 0) {
            let baseRevision = (await this.getProvenance(oldPath as FilePathWithPrefix))?.revision;
            if (!baseRevision) {
                const readFile = await this.readFileFromStub(info);
                const revisions = await this.db.findContentRevisions(oldPath as FilePathWithPrefix, readFile.body);
                baseRevision = revisions.length === 1 ? revisions[0] : undefined;
            }
            if (!baseRevision) {
                this._log(
                    `Renamed ${oldPath} to ${newPath}, but preserved every conflicted source branch because the displayed source revision is unknown`,
                    LOG_LEVEL_NOTICE
                );
                await this.conflict.queueCheckFor(oldPath as FilePathWithPrefix);
                return true;
            }
            const storedRevision = await this.db.storeDeletionWithBaseRevision(
                oldPath as FilePathWithPrefix,
                baseRevision
            );
            if (storedRevision === false) return false;
            await this.deleteProvenance(oldPath as FilePathWithPrefix);
            await this.conflict.queueCheckFor(oldPath as FilePathWithPrefix);
            return true;
        }
        const deleted = await this.db.delete(oldPath as FilePathWithPrefix);
        if (deleted) await this.deleteProvenance(oldPath as FilePathWithPrefix);
        return deleted;
    }

    async deleteRevisionFromDB(
        info: UXFileInfoStub | FilePath | FilePathWithPrefix,
        rev: string
    ): Promise<boolean | undefined> {
        const path = getStoragePathFromUXFileInfo(info);
        const provenance = await this.getProvenance(path);
        const deleted = await this.db.delete(info, rev);
        if (deleted && provenance?.revision === rev) {
            await this.deleteProvenance(path);
        }
        return deleted;
    }

    async resolveConflictedByDeletingRevision(
        info: UXFileInfoStub | FilePath,
        rev: string
    ): Promise<boolean | undefined> {
        const path = getStoragePathFromUXFileInfo(info);
        const file = await this.infoToStub(info);
        const docEntry = await this.db.fetchEntryMeta(file ?? info, rev, true);
        if (!docEntry) {
            this._log(`Failed to read the conflicted revision ${rev} of ${path}`, LOG_LEVEL_VERBOSE);
            return false;
        }
        if (!(await this.deleteRevisionFromDB(info, rev))) {
            this._log(`Failed to delete the conflicted revision ${rev} of ${path}`, LOG_LEVEL_VERBOSE);
            return false;
        }
        // This legacy operation deliberately applies the branch which it has
        // just removed. The public exact-revision operation accepts only live
        // branches, so it cannot be used after the deletion. Preserve the old
        // ordering by applying the metadata captured before deletion, then
        // discard any provenance for the now non-live revision.
        if (!(await this.applyDatabaseEntryToStorage(docEntry, file, true))) {
            this._log(`Failed to apply the resolved revision ${rev} of ${path} to the storage`, LOG_LEVEL_VERBOSE);
            return false;
        }
        await this.deleteProvenance(path);
    }

    async dbToStorageWithSpecificRev(
        info: UXFileInfoStub | UXFileInfo | FilePath | FilePathWithPrefix | null,
        rev: string,
        force?: boolean
    ): Promise<boolean> {
        if (info == null) {
            this._log(`Cannot select database revision ${rev} without a file path`, LOG_LEVEL_VERBOSE);
            return false;
        }
        const file = await this.infoToStub(info);
        const databaseTarget = file ?? info;
        const [docEntry, currentEntry, conflictedRevisions] = await Promise.all([
            this.db.fetchEntryMeta(databaseTarget, rev, true),
            this.db.fetchEntryMeta(databaseTarget, undefined, true),
            this.db.getConflictedRevs(databaseTarget),
        ]);
        if (!docEntry) {
            this._log(`File ${tryGetFilePath(info)} is not exist on the database`, LOG_LEVEL_VERBOSE);
            return false;
        }
        const liveRevisions = new Set([
            ...(currentEntry && currentEntry._rev ? [currentEntry._rev] : []),
            ...conflictedRevisions,
        ]);
        if (!liveRevisions.has(rev)) {
            this._log(
                `Could not apply ${tryGetFilePath(info)} revision ${rev}; the selected revision is no longer live`,
                LOG_LEVEL_NOTICE
            );
            return false;
        }
        return await this.applyDatabaseEntryToStorage(docEntry, file, force, true);
    }

    async dbToStorage(
        entryInfo: MetaEntry | FilePathWithPrefix,
        info: UXFileInfoStub | UXFileInfo | FilePath | null,
        force?: boolean
    ): Promise<boolean> {
        const file = await this.infoToStub(info);
        const pathFromEntryInfo = typeof entryInfo === "string" ? entryInfo : this.getPath(entryInfo);
        const docEntry = await this.db.fetchEntryMeta(pathFromEntryInfo, undefined, true);
        if (!docEntry) {
            this._log(`File ${pathFromEntryInfo} is not exist on the database`, LOG_LEVEL_VERBOSE);
            return false;
        }
        return await this.applyDatabaseEntryToStorage(docEntry, file, force);
    }

    private async applyDatabaseEntryToStorage(
        docEntry: MetaEntry,
        file: UXFileInfoStub | UXFileInfo | null,
        force?: boolean,
        allowExistingConflicts: boolean = false
    ): Promise<boolean> {
        const mode = file == null ? "create" : "modify";
        const path = this.getPath(docEntry);
        const settings = this.setting.currentSettings();
        // 1. Check if it already conflicted.
        const revs = await this.db.getConflictedRevs(path);
        if (revs.length > 0 && !allowExistingConflicts) {
            // Some conflicts are exist.
            if (settings.writeDocumentsIfConflicted) {
                // If configured to write the document even if conflicted, then it should be written.
                // NO OP
            } else {
                // If not, then it should be checked. and will be processed later (i.e., after the conflict is resolved).
                await this.conflict.queueCheckForIfOpen(path);
                return true;
            }
        }

        // 2. Check if the file is already exist on the storage.
        let existDoc = await this.storage.getStub(path);
        if (isFolderInfo(existDoc)) {
            this._log(`Folder ${path} is already exist on the storage as a folder`, LOG_LEVEL_VERBOSE);
            // We can do nothing, and other modules should also nothing to do.
            return true;
        }

        const stagesBinary =
            !isTextDocument(docEntry) &&
            (docEntry.size >= LARGE_BINARY_STREAM_BYTES ||
                (docEntry.children?.length ?? 0) >= LARGE_BINARY_STREAM_CHUNKS) &&
            Boolean(
                this.fileReflectionProvenance &&
                this.db.iterateBinaryContentFromMeta &&
                this.storage.writeBinaryFileInParts &&
                this.storage.supportsBinaryPartWrites?.()
            );
        const approvedTarget = stagesBinary ? await this.storage.statHidden(path) : (existDoc?.stat ?? null);
        if (stagesBinary && (approvedTarget?.type === "folder" || (approvedTarget && !existDoc))) return false;
        if (
            stagesBinary &&
            approvedTarget &&
            existDoc &&
            (approvedTarget.size !== existDoc.stat.size || approvedTarget.mtime !== existDoc.stat.mtime)
        )
            return false;

        // Check existence of both file and docEntry.
        const existOnDB = !(docEntry._deleted || docEntry.deleted || false);
        if (!existOnDB)
            return this.applyDatabaseDeletion(docEntry, Boolean(force), settings.writeDocumentsIfConflicted);
        if (existDoc && existDoc.path !== path) {
            const [existingDocumentId, targetDocumentId] = await Promise.all([
                this.path.path2id(existDoc.path),
                this.path.path2id(path),
            ]);
            if (existingDocumentId !== targetDocumentId) {
                this._log(
                    `Refusing to overwrite ${existDoc.path} while applying the distinct path ${path}`,
                    LOG_LEVEL_NOTICE
                );
                return false;
            }
            if (getParentPath(existDoc.path) !== getParentPath(path)) {
                this._log(
                    `Refusing to apply a filename case change across differently cased parent directories: ${existDoc.path} -> ${path}`,
                    LOG_LEVEL_NOTICE
                );
                return false;
            }
            const renamedFile = await this.storage.renameFile(existDoc, path);
            if (!renamedFile) {
                this._log(`Could not apply the stored filename case: ${existDoc.path} -> ${path}`, LOG_LEVEL_NOTICE);
                return false;
            }
            // The file moved, so a mark describing it moves too. Dropping it would leave the partial content
            // at the new name unmarked, and the next event would preserve those bytes as a conflicted revision.
            if (this.fileReflectionProvenance)
                await this.writeCoordinator.move(existDoc.path as FilePathWithPrefix, path);
            existDoc = renamedFile;
        }
        // Okay, the file is exist on the database. Let's check the file is exist on the storage.
        if (existDoc && !force && (await this.isStorageUnchangedSinceReflected(existDoc, docEntry._rev))) {
            this._log(`File ${docEntry.path} is already reflected in storage`, LOG_LEVEL_VERBOSE);
            return true;
        }
        // The content is read only where it is needed. Sizes already decide whether content can be equal, and a
        // large attachment is written in parts, so it never has to exist as one buffer.
        let loadedContent: string | ArrayBuffer | undefined;
        const loadContent = async (): Promise<string | ArrayBuffer | false> => {
            if (loadedContent === undefined) {
                const read = await this.readEntryContentForStorage(docEntry, path, settings.processSizeMismatchedFiles);
                if (read === false) return false;
                loadedContent = read;
            }
            return loadedContent;
        };

        if (allowExistingConflicts && existDoc && !force && existDoc.stat.size === docEntry.size) {
            const docData = await loadContent();
            if (docData === false) return false;
            const readFile = await this.readFileFromStub(existDoc);
            if (await isDocContentSame(docData, readFile.body)) {
                await this.setProvenance(path, docEntry._rev, existDoc.stat.mtime, true);
                this.path.markChangesAreSame(docEntry, docEntry.mtime, existDoc.stat.mtime);
                return true;
            }
        }

        if (existDoc && !force) {
            // The file is exist on the storage. Let's check the difference between the file and the entry.
            // But, if force is true, then it should be updated.
            // Ok, we have to compare.
            let shouldApplied = false;
            // 1. if the time stamp is far different, then it should be updated.
            // Note: This checks only the mtime with the resolution reduced to 2 seconds.
            //       2 seconds it for the ZIP file's mtime. If not, we cannot backup the vault as the ZIP file.
            //       This is hardcoded on `compareMtime` of `src/common/utils.ts`.
            const freshness = this.path.compareFileFreshness(existDoc, docEntry);
            if (freshness !== EVEN) {
                shouldApplied = true;
            }
            // 2. if not, the content should be checked. Contents of different sizes cannot be equal, so that
            //    difference is decided without reading either side.
            if (!shouldApplied && existDoc.stat.size !== docEntry.size) {
                shouldApplied = true;
            }

            if (!shouldApplied) {
                const docData = await loadContent();
                if (docData === false) return false;
                const readFile = await this.readFileFromStub(existDoc);
                if (await isDocContentSame(docData, readFile.body)) {
                    // The content is same. So, we do not need to update the file.
                    shouldApplied = false;
                    // Timestamp is different but the content is same. therefore, two timestamps should be handled as same.
                    // So, mark the changes are same.
                    this.path.markChangesAreSame(docEntry, docEntry.mtime, existDoc.stat.mtime);
                } else {
                    shouldApplied = true;
                }
            }
            if (!shouldApplied) {
                await this.setProvenance(path, docEntry._rev, existDoc.stat.mtime, true);
                this._log(`File ${docEntry.path} is not changed`, LOG_LEVEL_VERBOSE);
                return true;
            }
            if (!force && !settings.writeDocumentsIfConflicted) {
                // Recognising an incoming version which merely extends the local text needs that text. Binary
                // content of a different size can never be such an extension, so it is not read for this.
                if (loadedContent === undefined && isTextDocument(docEntry)) {
                    const docData = await loadContent();
                    if (docData === false) return false;
                }
                if (await this.preserveUnsyncedStorageAsConflict(path, existDoc, docEntry, loadedContent)) {
                    return true;
                }
            }
            // Let's apply the changes.
        } else {
            this._log(
                `File ${docEntry.path} ${existDoc ? "(new) " : ""} ${force ? " (forced)" : ""}`,
                LOG_LEVEL_VERBOSE
            );
        }
        // Storage content which had to be preserved has been preserved by now, so a large attachment can be
        // written in parts. Holding the whole file as one buffer, as the general path does, exhausts memory on
        // a mobile device, and that also applies when an existing or partially written file is replaced.
        if (stagesBinary) {
            const streamed = await this.writeEntryToStorageInParts(docEntry, path, mode, approvedTarget);
            if (streamed !== "unsupported") {
                return streamed;
            }
        }
        const docData = await loadContent();
        if (docData === false) {
            return false;
        }
        await this.storage.ensureDir(path);
        const ret = await this.storage.writeFileAuto(path, docData, { ctime: docEntry.ctime, mtime: docEntry.mtime });
        await this.storage.touched(path);
        if (ret) {
            if (this.fileReflectionProvenance) {
                const storedStat = await this.storage.stat(path);
                await this.setProvenance(path, docEntry._rev, storedStat?.mtime, true);
            }
        }
        this.storage.triggerFileEvent(mode, path);
        return ret;
    }
    private async applyDatabaseDeletion(
        docEntry: MetaEntry,
        force: boolean,
        writeIfConflicted: boolean
    ): Promise<boolean> {
        const path = this.getPath(docEntry);
        const operation = async () => {
            const latest = await this.db.fetchEntryMeta(path, undefined, true);
            if (!latest || latest._rev !== docEntry._rev || (!latest.deleted && !latest._deleted)) return false;
            const current = await this.storage.getStub(path);
            if (isFolderInfo(current)) return false;
            if (current) {
                if (
                    !force &&
                    !writeIfConflicted &&
                    (await this.preserveUnsyncedStorageAsConflict(path, current, latest))
                )
                    return true;
                await this.storage.deleteVaultItem(path);
            }
            if (!(await this.confirmAbsent(path))) return false;
            if (this.fileReflectionProvenance) await this.writeCoordinator.discardForDatabaseDeletion(path);
            return true;
        };
        return this.fileReflectionProvenance ? this.writeCoordinator.run(path, operation) : operation();
    }

    private async writeEntryToStorageInParts(
        docEntry: MetaEntry,
        path: FilePathWithPrefix,
        mode: string,
        approvedTarget: UXStat | null
    ): Promise<boolean | "unsupported"> {
        const iterate = this.db.iterateBinaryContentFromMeta;
        const write = this.storage.writeBinaryFileInParts;
        if (!iterate || !write || !docEntry._rev) return "unsupported";
        return this.writeCoordinator.run(path, async () => {
            await this.writeCoordinator.get(path);
            await this.storage.ensureDir(path);
            let token: string | undefined;
            try {
                const written = await write.call(
                    this.storage,
                    path,
                    iterate.call(this.db, docEntry),
                    {
                        expectedTarget: approvedTarget,
                        size: docEntry.size,
                        beforePublish: async () => {
                            token = await this.writeCoordinator.begin(path, docEntry._rev!);
                        },
                        afterPublish: async (stat) => {
                            if (!token || stat.type !== "file" || stat.size !== docEntry.size)
                                throw new Error("Publication was not confirmed");
                            await this.writeCoordinator.reflect(path, docEntry._rev!, stat.mtime, true, token);
                        },
                    },
                    { ctime: docEntry.ctime, mtime: docEntry.mtime }
                );
                if (!written) return false;
                await this.storage.touched(path);
                this.storage.triggerFileEvent(mode, path);
                return true;
            } catch (error) {
                // The target is complete or missing behind a durable mark. Never fall back to a whole buffer.
                this._log(
                    `Staged publication of ${path} did not finish; the existing queue may retry it`,
                    LOG_LEVEL_NOTICE
                );
                this._log(error, LOG_LEVEL_VERBOSE);
                return false;
            }
        });
    }

    private async confirmAbsent(path: FilePathWithPrefix): Promise<boolean> {
        return !(await this.storage.isExistsIncludeHidden(path).catch((): boolean => true));
    }

    private async getPendingPublicationRevision(path: FilePathWithPrefix): Promise<string | undefined> {
        if (!this.fileReflectionProvenance) return undefined;
        return this.writeCoordinator.missingPublication(path);
    }

    private async recoverMissingPublication(path: FilePathWithPrefix): Promise<boolean> {
        const entry = await this.db.fetchEntryMeta(path, undefined, true);
        if (!entry) return false;
        // Conflicts and an existing replacement still use the ordinary preservation flow; no forced overwrite.
        return this.applyDatabaseEntryToStorage(entry, null);
    }

    /**
     * Read the database content which is about to be reflected into storage.
     *
     * Binary entries are assembled batch by batch into a single buffer so that a large attachment is held
     * once instead of as chunk text plus several decoded copies. Text entries, legacy encodings, accepted
     * size mismatches and hosts without batched reading keep the general path.
     */
    private async readEntryContentForStorage(
        docEntry: MetaEntry,
        path: FilePathWithPrefix,
        processSizeMismatchedFiles: boolean
    ): Promise<string | ArrayBuffer | false> {
        if (!isTextDocument(docEntry) && this.db.fetchBinaryContentFromMeta) {
            const binary = await this.db.fetchBinaryContentFromMeta(docEntry);
            if (binary === false) {
                this._log(`File ${path} is not exist on the database`, LOG_LEVEL_VERBOSE);
                return false;
            }
            if (binary.status === "ok") {
                return binary.data;
            }
            // (Zero is a special case, may be created by some APIs and it might be acceptable).
            if (binary.status === "size-mismatch" && !processSizeMismatchedFiles && docEntry.size != 0) {
                this._log(
                    `File ${path} seems to be corrupted! Writing prevented. (${docEntry.size} != ${binary.decodedSize})`,
                    LOG_LEVEL_NOTICE
                );
                return false;
            }
        }
        const docRead = await this.db.fetchEntryFromMeta(docEntry);
        if (!docRead) {
            this._log(`File ${path} is not exist on the database`, LOG_LEVEL_VERBOSE);
            return false;
        }
        // If we want to process size mismatched files -- in case of having files created by some integrations, enable the toggle.
        if (!processSizeMismatchedFiles) {
            // Check the file is not corrupted
            // (Zero is a special case, may be created by some APIs and it might be acceptable).
            if (docRead.size != 0 && docRead.size !== readAsBlob(docRead).size) {
                this._log(
                    `File ${path} seems to be corrupted! Writing prevented. (${docRead.size} != ${readAsBlob(docRead).size})`,
                    LOG_LEVEL_NOTICE
                );
                return false;
            }
        }
        return readContent(docRead);
    }

    /**
     * Whether an empty file in storage is the state this device itself wrote from the database.
     *
     * A file which was reflected empty is not local work, so newer content replaces it without a conflict.
     * A file the user emptied carries a modification time this device never recorded.
     */
    private async isReflectedEmptiness(
        path: FilePathWithPrefix,
        stat: { mtime: number } | null | undefined
    ): Promise<boolean> {
        if (!this.fileReflectionProvenance || !stat) return false;
        let record: FileReflectionProvenanceRecord | undefined;
        try {
            record = await this.writeCoordinator.get(path);
        } catch {
            return false;
        }
        if (!record || record.pendingPublication || record.observedStorageMtime === undefined) return false;
        // Only a record written by a reflection proves that the database produced what storage holds. A record
        // written while storing storage into the database describes a file the device merely read.
        if (record.pendingPublication || !record.reflectedFromDatabase) return false;
        return record.observedStorageMtime === stat.mtime;
    }

    private async preserveUnsyncedStorageAsConflict(
        path: FilePathWithPrefix,
        existDoc: UXFileInfoStub,
        incomingEntry: MetaEntry,
        incomingContent?: string | string[] | Blob | ArrayBuffer
    ): Promise<boolean> {
        let readFile = await this.readFileFromStub(existDoc);
        if (incomingContent && (await isDocContentSame(incomingContent, readFile.body))) {
            return false;
        }
        if (incomingContent && (await isIncomingTextClearExtension(incomingContent, readFile.body))) {
            return false;
        }
        if (!incomingEntry._rev) {
            return false;
        }
        // A file which is empty in storage while the incoming entry is not must be preserved, even though an
        // empty revision usually exists in the history of a file which was created empty. Sizes decide it, so
        // this also covers an attachment whose content was never loaded. A fresh stat confirms the emptiness,
        // because a file can be reported as empty for a moment while it is being written. Emptiness which this
        // device itself reflected is not local work and is replaced without a conflict.
        const localStat = createBlob(readFile.body).size === 0 ? await this.storage.stat(path) : undefined;
        if (localStat && localStat.size > 0) {
            // The body was read before the content landed. Preserving it would store a spurious empty
            // revision, so the file is read again.
            readFile = await this.readFileFromStub(existDoc);
        }
        const localIsEmpty = localStat?.size === 0;
        if (localIsEmpty && incomingEntry.size > 0 && !(await this.isReflectedEmptiness(path, localStat))) {
            const storedRevision = await this.db.storeAsConflictedRevisionWithResult(
                readFile,
                incomingEntry._rev,
                true
            );
            if (storedRevision === false) {
                this._log(`Prevented overwriting the emptied local file ${path}`, LOG_LEVEL_NOTICE);
                return true;
            }
            await this.setProvenance(path, storedRevision, readFile.stat.mtime);
            this._log(`Preserved the emptied local file ${path} as a conflict`, LOG_LEVEL_NOTICE);
            await this.conflict.queueCheckFor(path);
            return true;
        }
        if (await this.db.hasContentInRevisionHistory(path, readFile.body, incomingEntry._rev)) {
            return false;
        }
        const storedRevision = await this.db.storeAsConflictedRevisionWithResult(readFile, incomingEntry._rev, true);
        if (storedRevision === false) {
            this._log(`Prevented overwriting unsynchronised local changes for ${path}`, LOG_LEVEL_NOTICE);
            return true;
        }
        await this.setProvenance(path, storedRevision, readFile.stat.mtime);
        this._log(`Preserved unsynchronised local changes as a conflict for ${path}`, LOG_LEVEL_NOTICE);
        await this.conflict.queueCheckFor(path);
        return true;
    }

    private async _anyHandlerProcessesFileEvent(item: FileEventItem): Promise<boolean> {
        if (item.restoredFromPreviousRuntime) {
            return await this._processRestoredFileEvent(item);
        }
        const eventItem = item.args;
        const type = item.type;
        const path = eventItem.file.path;
        if (!(await this.isCurrentPathSelected(path))) {
            return false;
        }
        if (type === "RENAME" && !eventItem.oldPath) {
            this._log(`Rename event for ${path} has no source path`, LOG_LEVEL_VERBOSE);
            return false;
        }
        const eventPaths = type === "RENAME" ? [path, eventItem.oldPath as FilePathWithPrefix] : [path];
        return await this.serializedByFileEventPaths(eventPaths, async () => {
            switch (type) {
                case "CREATE":
                case "CHANGED":
                    return await this.storeFileToDB(item.args.file);
                case "DELETE":
                    return await this.deleteFileFromDB(item.args.file);
                case "RENAME":
                    return await this.renameFileInDB(
                        item.args.file as UXFileInfoStub,
                        item.args.oldPath as FilePathWithPrefix
                    );
                case "INTERNAL":
                    // this should be handled on the other module.
                    return false;
                default:
                    this._log(`Unsupported event type: ${type as string}`, LOG_LEVEL_VERBOSE);
                    return false;
            }
        });
    }

    private async serializedByFileEventPaths<T>(
        eventPaths: readonly FilePathWithPrefix[],
        callback: (isSameDocument: boolean) => Promise<T>
    ): Promise<T> {
        const documentIds = await Promise.all(eventPaths.map((eventPath) => this.path.path2id(eventPath)));
        const lockKeys = [...new Set(documentIds)].sort().map((documentId) => `processFileEvent-${documentId}`);
        return await serializedByKeys(lockKeys, () =>
            callback(documentIds.length === 2 && documentIds[0] === documentIds[1])
        );
    }

    /**
     * Revalidate a persisted storage operation against the current storage state.
     *
     * Snapshot entries preserve operation intent and ordering only. Their file
     * stub, timestamps, and existence assumptions can be stale after a restart.
     * Current inclusion is therefore read from storage, while destructive work
     * is allowed only after current absence has been observed.
     */
    private async _processRestoredFileEvent(item: FileEventItem): Promise<boolean> {
        const eventItem = item.args;
        const type = item.type;
        const path = eventItem.file.path;
        if (type === "RENAME" && !eventItem.oldPath) {
            this._log(`Rename event for ${path} has no source path`, LOG_LEVEL_VERBOSE);
            return true;
        }

        const eventPaths = type === "RENAME" ? [path, eventItem.oldPath as FilePathWithPrefix] : [path];
        return await this.serializedByFileEventPaths(eventPaths, async (isSameDocument) => {
            let action: RestoredFileEventAction;
            try {
                action = await this._planRestoredFileEvent(item, isSameDocument);
            } catch (ex) {
                this.logRestoredEventValidationFailure(path, ex);
                return true;
            }

            switch (action.kind) {
                case "none":
                    return true;
                case "store":
                    return await this.storeFileToDB(action.file);
                case "delete":
                    return action.baseRevision
                        ? await this.deleteOnRevision(action.path as FilePathWithPrefix, action.baseRevision)
                        : await this.deleteFileFromDB(action.path);
                case "rename":
                    return await this.renameFileInDB(action.file, action.oldPath);
            }
        });
    }

    private async _planRestoredFileEvent(
        item: FileEventItem,
        isSameDocument: boolean
    ): Promise<RestoredFileEventAction> {
        const path = item.args.file.path;
        switch (item.type) {
            case "CREATE":
            case "CHANGED": {
                const current = this.getExactCurrentFile(await this.storage.getStub(path), path);
                if (!current || !(await this.isCurrentFileSelected(current))) return { kind: "none" };
                return (await this.holdsRecordedRevision(current))
                    ? { kind: "none" }
                    : { kind: "store", file: current };
            }
            case "DELETE": {
                const current = await this.storage.getStub(path);
                if (current !== null || !(await this.canApplyRestoredDeletion(path))) return { kind: "none" };
                return await this._planRestoredDeletion(item.args.file);
            }
            case "RENAME":
                return await this._planRestoredRename(path, item.args.oldPath as FilePathWithPrefix, isSameDocument);
            case "INTERNAL":
                return { kind: "none" };
            default:
                this._log(`Unsupported event type: ${item.type as string}`, LOG_LEVEL_VERBOSE);
                return { kind: "none" };
        }
    }

    private async _planRestoredRename(
        newPath: FilePathWithPrefix,
        oldPath: FilePathWithPrefix,
        isSameDocument: boolean
    ): Promise<RestoredFileEventAction> {
        if (isSameDocument) {
            const currentTarget = this.getExactCurrentFile(await this.storage.getStub(newPath), newPath);
            if (!currentTarget || !(await this.isCurrentFileSelected(currentTarget))) {
                return { kind: "none" };
            }
            return { kind: "rename", file: currentTarget, oldPath };
        }

        const currentTargetItem = await this.storage.getStub(newPath);
        let currentSourceItem: UXFileInfoStub | UXFolderInfo | null = null;
        let sourceWasInspected = true;
        try {
            currentSourceItem = await this.storage.getStub(oldPath);
        } catch (ex) {
            sourceWasInspected = false;
            this.logRestoredEventValidationFailure(oldPath, ex);
        }
        const currentTarget = this.getExactCurrentFile(currentTargetItem, newPath);
        if (currentTargetItem !== null) {
            if (!currentTarget || !(await this.isCurrentFileSelected(currentTarget))) {
                return { kind: "none" };
            }
            if (!sourceWasInspected || currentSourceItem !== null || !(await this.canApplyRestoredDeletion(oldPath))) {
                return { kind: "store", file: currentTarget };
            }
            return { kind: "rename", file: currentTarget, oldPath };
        }

        if (!sourceWasInspected || currentSourceItem !== null || !(await this.canApplyRestoredDeletion(oldPath))) {
            return { kind: "none" };
        }
        return { kind: "delete", path: oldPath as FilePath };
    }

    /**
     * Whether storage holds the revision this device last recorded for the file.
     *
     * A revalidated event says only that storage may have changed while no watcher reported it. Storage which
     * still holds exactly what this device reflected or stored carries no local work. Storing it again would
     * publish unchanged content as a new revision, or replace a newer revision which has reached the database
     * but not yet storage. A file restored after its deletion is local work and is stored. A large file is
     * recognised, as for the storage event of our own write, by the recorded modification time and size only,
     * so its content is not read here. In every other case, including when the record cannot be checked, the
     * file is stored as before.
     */
    private async holdsRecordedRevision(file: UXFileInfoStub): Promise<boolean> {
        try {
            const record = await this.getProvenance(file.path as FilePathWithPrefix);
            if (!record) return false;
            const [stat, recordedMeta, winner] = await Promise.all([
                this.storage.stat(file.path),
                this.db.fetchEntryMeta(file, record.revision, true),
                this.db.fetchEntryMeta(file, undefined, true),
            ]);
            if (!stat || !recordedMeta || recordedMeta.size !== stat.size) return false;
            if (!winner || winner._deleted || winner.deleted) return false;
            if (stat.size >= RECOGNISE_REFLECTED_STORAGE_BYTES) return record.observedStorageMtime === stat.mtime;
            const recorded = await this.fetchEntryForComparison(file, record.revision);
            if (recorded === false) return false;
            const readFile = await this.readFileFromStub(file);
            return await isDocContentSame(recorded.content, readFile.body);
        } catch (ex) {
            this._log(ex, LOG_LEVEL_VERBOSE);
            return false;
        }
    }

    /**
     * Plan a revalidated deletion, which may predate a newer revision reaching the database.
     *
     * When storage last displayed an older revision than the current one, the deletion is stored on that
     * revision, so the newer one becomes a conflict instead of being removed silently. Without a record, for
     * example after this device reflected a remote deletion, a rule like that of the Offline Scanner decides:
     * the deletion applies only when the event's time, the file as last seen or the moment of deletion, is not
     * older than the current revision. Otherwise the next scan, which follows a restored snapshot directly,
     * reconciles the path. When the database cannot be checked, the path is deleted as before.
     */
    private async _planRestoredDeletion(file: UXFileInfoStub): Promise<RestoredFileEventAction> {
        const path = file.path as FilePath;
        try {
            const winner = await this.db.fetchEntryMeta(file.path, undefined, true);
            if (!winner || winner._deleted || winner.deleted) return { kind: "delete", path };
            const record = await this.getProvenance(file.path);
            if (record) {
                return record.revision === winner._rev
                    ? { kind: "delete", path }
                    : { kind: "delete", path, baseRevision: record.revision };
            }
            if (file.stat?.mtime !== undefined && compareMTime(file.stat.mtime, winner.mtime) === TARGET_IS_NEW) {
                this._log(
                    `Deletion of ${path} predates its current revision; the next scan reconciles it`,
                    LOG_LEVEL_INFO
                );
                return { kind: "none" };
            }
            return { kind: "delete", path };
        } catch (ex) {
            this._log(ex, LOG_LEVEL_VERBOSE);
            return { kind: "delete", path };
        }
    }

    private getExactCurrentFile(
        current: UXFileInfoStub | UXFolderInfo | null,
        expectedPath: FilePathWithPrefix
    ): UXFileInfoStub | null {
        if (current === null || isFolderInfo(current)) {
            return null;
        }
        return this.storage.normalisePath(current.path) === this.storage.normalisePath(expectedPath) ? current : null;
    }

    private async isCurrentFileSelected(file: UXFileInfoStub): Promise<boolean> {
        if (!(await this.isCurrentPathSelected(file.path))) {
            return false;
        }
        if (this.vault.isFileSizeTooLarge(file.stat.size)) {
            this._log(`File ${file.path} exceeds the current maximum size`, LOG_LEVEL_VERBOSE);
            return false;
        }
        return true;
    }

    private async isCurrentPathSelected(path: FilePathWithPrefix): Promise<boolean> {
        if (!(await this.vault.isTargetFile(path))) {
            this._log(`File ${path} is not the target file`, LOG_LEVEL_VERBOSE);
            return false;
        }
        if (shouldBeIgnored(path)) {
            this._log(`File ${path} should be ignored`, LOG_LEVEL_VERBOSE);
            return false;
        }
        return true;
    }

    private async canApplyRestoredDeletion(path: FilePathWithPrefix): Promise<boolean> {
        try {
            return await this.isCurrentPathSelected(path);
        } catch (ex) {
            this.logRestoredEventValidationFailure(path, ex);
            return false;
        }
    }

    private logRestoredEventValidationFailure(path: FilePathWithPrefix, ex: unknown): void {
        this._log(
            `Could not validate the queued storage operation for ${path} against current storage; a later Offline Scanner run will reconcile the current state`,
            LOG_LEVEL_NOTICE
        );
        this._log(ex, LOG_LEVEL_VERBOSE);
    }

    async _anyProcessReplicatedDoc(entry: MetaEntry): Promise<boolean> {
        return await serialized(`processReplicatedDoc-${entry._id}`, async () => {
            if (!(await this.vault.isTargetFile(entry.path))) {
                this._log(`File ${entry.path} is not the target file`, LOG_LEVEL_VERBOSE);
                return false;
            }
            if (this.vault.isFileSizeTooLarge(entry.size)) {
                this._log(`File ${entry.path} is too large (on database) to be processed`, LOG_LEVEL_VERBOSE);
                return false;
            }
            if (shouldBeIgnored(entry.path)) {
                this._log(`File ${entry.path} should be ignored`, LOG_LEVEL_VERBOSE);
                return false;
            }
            const path = this.getPath(entry);

            const targetFile = await this.storage.getStub(this.getPathWithoutPrefix(entry));
            if (isFolderInfo(targetFile)) {
                this._log(`${path} is already exist as the folder`);
                // Nothing to do and other modules should also nothing to do.
                return true;
            } else {
                if (targetFile && this.vault.isFileSizeTooLarge(targetFile.stat.size)) {
                    this._log(`File ${targetFile.path} is too large (on storage) to be processed`, LOG_LEVEL_VERBOSE);
                    return false;
                }
                this._log(
                    `Processing ${path} (${entry._id.substring(0, 8)} :${entry._rev?.substring(0, 5)}) : Started...`,
                    LOG_LEVEL_VERBOSE
                );
                // Before writing (or skipped ), merging dialogue should be cancelled.
                this.events.emitEvent(EVENT_CONFLICT_CANCELLED, path);
                const ret = await this.dbToStorage(entry, targetFile);
                this._log(`Processing ${path} (${entry._id.substring(0, 8)} :${entry._rev?.substring(0, 5)}) : Done`);
                return ret;
            }
        });
    }

    async createAllChunks(showingNotice?: boolean): Promise<void> {
        this._log("Collecting local files on the storage", LOG_LEVEL_VERBOSE);
        const semaphore = Semaphore(10);

        let processed = 0;
        const filesStorageSrc = await this.storage.getFiles();
        const incProcessed = () => {
            processed++;
            if (processed % 25 == 0)
                this._log(
                    `Creating missing chunks: ${processed} of ${total} files`,
                    showingNotice ? LOG_LEVEL_NOTICE : LOG_LEVEL_INFO,
                    "chunkCreation"
                );
        };
        const total = filesStorageSrc.length;
        const procAllChunks = filesStorageSrc.map(async (file) => {
            if (!(await this.vault.isTargetFile(file))) {
                incProcessed();
                return true;
            }
            if (this.vault.isFileSizeTooLarge(file.stat.size)) {
                incProcessed();
                return true;
            }
            if (shouldBeIgnored(file.path)) {
                incProcessed();
                return true;
            }
            const release = await semaphore.acquire();
            incProcessed();
            try {
                await this.storeFileToDB(file, false, true);
            } catch (ex) {
                this._log(ex, LOG_LEVEL_VERBOSE);
            } finally {
                release();
            }
        });
        await Promise.all(procAllChunks);
        this._log(
            `Creating chunks Done: ${processed} of ${total} files`,
            showingNotice ? LOG_LEVEL_NOTICE : LOG_LEVEL_INFO,
            "chunkCreation"
        );
    }
}
