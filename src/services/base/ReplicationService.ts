import {
    LOG_LEVEL_INFO,
    LOG_LEVEL_NOTICE,
    LOG_LEVEL_VERBOSE,
    type LOG_LEVEL,
    type ObsidianLiveSyncSettings,
} from "@lib/common/types";
import { handlers } from "@lib/services/lib/HandlerUtils";
import type {
    IAPIService,
    IDatabaseService,
    IFileProcessingService,
    IReplicationService,
    IReplicatorService,
    ISettingService,
} from "./IService";
import type { AtomicSimpleStore } from "@lib/interfaces/KeyValueDatabase";
import { ServiceBase, type ServiceContext } from "./ServiceBase";
import { reactiveSource } from "octagonal-wheels/dataobject/reactive";
import { createInstanceLogFunction, MARK_LOG_NETWORK_ERROR, type LogFunction } from "@lib/services/lib/logUtils";
import type { LiveSyncAbstractReplicator } from "@lib/replication/LiveSyncAbstractReplicator";
import { UnresolvedErrorManager } from "./UnresolvedErrorManager";
import type { AppLifecycleService } from "./AppLifecycleService";
import { isLockAcquired } from "octagonal-wheels/concurrency/lock";
import {
    DurableReplicationCoordinator,
    type ReplicationQueueState,
} from "@lib/services/lib/DurableReplicationCoordinator";
import { delay, fireAndForget } from "octagonal-wheels/promises";

export interface ReplicationServiceDependencies {
    APIService: IAPIService;
    settingService: ISettingService;
    appLifecycleService: AppLifecycleService;
    databaseService: IDatabaseService;
    replicatorService: IReplicatorService;
    fileProcessingService: IFileProcessingService;
    replicationQueueStore: Pick<AtomicSimpleStore<ReplicationQueueState>, "get" | "atomicUpdate">;
}
/**
 * The ReplicationService provides methods for managing replication processes.
 */
export abstract class ReplicationService<T extends ServiceContext = ServiceContext>
    extends ServiceBase<T>
    implements IReplicationService
{
    private _unresolvedErrorManager: UnresolvedErrorManager;

    showError(msg: string, max_log_level: LOG_LEVEL = LOG_LEVEL_NOTICE) {
        this._unresolvedErrorManager.showError(msg, max_log_level);
    }
    clearErrors() {
        this._unresolvedErrorManager.clearErrors();
    }

    _log: LogFunction;
    settingService: ISettingService;
    appLifecycleService: AppLifecycleService;
    replicatorService: IReplicatorService;
    APIService: IAPIService;
    fileProcessing: IFileProcessingService;
    databaseService: IDatabaseService;
    private readonly replicationCoordinator: DurableReplicationCoordinator;
    constructor(context: T, dependencies: ReplicationServiceDependencies) {
        super(context);
        this.appLifecycleService = dependencies.appLifecycleService;
        this.settingService = dependencies.settingService;
        this.replicatorService = dependencies.replicatorService;
        this.APIService = dependencies.APIService;
        this.fileProcessing = dependencies.fileProcessingService;
        this.databaseService = dependencies.databaseService;
        this.replicationCoordinator = new DurableReplicationCoordinator(dependencies.replicationQueueStore);
        // Load and resume handlers run in order and stop at the first failure. Resuming pending replication must
        // neither hold them up while a cycle is still unwinding nor cancel later handlers such as the periodic
        // timer when it fails; a failed attempt stays pending for the next trigger.
        const resumePendingReplication = () => {
            fireAndForget(() => this.replicationCoordinator.resumePending(() => this.runReplicationCycle(false)));
            return Promise.resolve(true);
        };
        this.appLifecycleService.onLoaded.addHandler(resumePendingReplication);
        this.appLifecycleService.onResumed.addHandler(resumePendingReplication);
        this._log = createInstanceLogFunction("ReplicationService", dependencies.APIService);
        this._unresolvedErrorManager = new UnresolvedErrorManager(
            dependencies.appLifecycleService,
            this.context.events
        );
    }
    /**
     * Process a synchronisation result document.
     */
    readonly processSynchroniseResult = handlers<IReplicationService>().anySuccess("processSynchroniseResult");

    /**
     * Process a synchronisation result document for optional entries i.e., hidden files.
     */
    readonly processOptionalSynchroniseResult = handlers<IReplicationService>().anySuccess(
        "processOptionalSynchroniseResult"
    );
    /**
     * Process an array of synchronisation result documents.
     * @param docs An array of documents to parse and handle.
     */
    readonly parseSynchroniseResult = handlers<IReplicationService>().all("parseSynchroniseResult");
    /**
     * Process a virtual document (e.g., for customisation sync).
     */
    readonly processVirtualDocument = handlers<IReplicationService>().anySuccess("processVirtualDocument");

    /**
     * An event triggered before starting replication.
     */
    readonly onBeforeReplicate = handlers<IReplicationService>().bailFirstFailure("onBeforeReplicate");

    /**
     * Lightweight, repeatable policy checks shared by every replication entry point.
     * Handlers must remain idempotent because a high-level replication may cross
     * more than one entry point before work begins.
     */
    readonly onCheckReplicationReady = handlers<IReplicationService>().bailFirstFailure("onCheckReplicationReady");

    /**
     *  Check if the replication is ready to start.
     * @param showMessage Whether to show messages to the user.
     */
    async isReplicationReady(showMessage: boolean = false): Promise<boolean> {
        if (!this.appLifecycleService.isReady()) {
            this._log(`Not ready`);
            return false;
        }
        if (!(await this.onCheckReplicationReady(showMessage))) {
            return false;
        }
        const currentSettings = this.settingService.currentSettings();

        if (isLockAcquired("cleanup")) {
            this._log(this.context.translate("Replicator.Message.Cleaned"), LOG_LEVEL_NOTICE);
            return false;
        }

        if (currentSettings.versionUpFlash != "") {
            this._log(this.context.translate("Replicator.Message.VersionUpFlash"), LOG_LEVEL_NOTICE);
            return false;
        }

        if (!(await this.fileProcessing.commitPendingFileEvents())) {
            this.showError(this.context.translate("Replicator.Message.Pending"), LOG_LEVEL_NOTICE);
            return false;
        }

        if (!this.APIService.isOnline) {
            this.showError("Network is offline", showMessage ? LOG_LEVEL_NOTICE : LOG_LEVEL_INFO);
            return false;
        }
        if (!(await this.onBeforeReplicate(showMessage))) {
            // check for tagged network errors for filtering by NetworkWarningStyles
            const hasNetworkError = (await this.appLifecycleService.getUnresolvedMessages())
                .flat()
                .some((e) => typeof e == "string" && e.indexOf(MARK_LOG_NETWORK_ERROR) !== -1);
            if (!hasNetworkError) {
                this.showError(this.context.translate("Replicator.Message.SomeModuleFailed"), LOG_LEVEL_NOTICE);
            } else {
                this._log(this.context.translate("Replicator.Message.SomeModuleFailed"), LOG_LEVEL_INFO);
            }
            return false;
        }
        this.clearErrors();
        return true;
    }

    onReplicationFailed = handlers<IReplicationService>().bailFirstFailure("onReplicationFailed");

    private async performReplicationRequest(showMessage?: boolean): Promise<boolean | void> {
        const activeReplicator = this.replicatorService.getActiveReplicator();
        if (!activeReplicator) {
            this._log(`No active replicator found`, showMessage ? LOG_LEVEL_NOTICE : LOG_LEVEL_INFO);
            return false;
        }
        const settings = this.settingService.currentSettings();
        return await activeReplicator.openReplication(settings, false, !!showMessage, false);
    }

    /**
     * Perform replication and handle a failed result.
     * @param showMessage Whether to show replication progress messages.
     */
    async performReplication(showMessage?: boolean): Promise<boolean | void> {
        const result = await this.performReplicationRequest(showMessage);
        if (!result) return await this.onReplicationFailed(showMessage);
        return result;
    }

    /**
     * Start the replication process.
     * @param showMessage Whether to show messages to the user.
     */
    private async runReplicationCycle(showMessage?: boolean): Promise<boolean | void> {
        try {
            const checkBeforeReplicate = await this.isReplicationReady(showMessage);
            if (!checkBeforeReplicate) return false;
            const result = await this.replicatorService.runFiniteReplicationActivity(
                () => this.performReplicationRequest(showMessage),
                { label: "replication" }
            );
            if (!result) return await this.onReplicationFailed(showMessage);
            return result;
        } finally {
            this.previousReplicated = Date.now();
        }
    }

    async replicate(showMessage?: boolean): Promise<boolean | void> {
        return await this.replicationCoordinator.enqueue(() => this.runReplicationCycle(showMessage));
    }

    private async runEventReplicationCycle(showMessage?: boolean): Promise<boolean | void> {
        const least = this.settingService.currentSettings().syncMinimumInterval;
        if (least > 0) {
            const elapsed = Date.now() - this.previousReplicated;
            const waitMs = Math.max(0, least - elapsed);
            if (waitMs > 0) {
                this._log(
                    `Replication triggered by event is queued for ${waitMs}ms to honour the minimum interval.`,
                    LOG_LEVEL_VERBOSE
                );
                await delay(waitMs);
            }
        }
        return await this.runReplicationCycle(showMessage);
    }

    previousReplicated: number = 0;
    /**
     * Start the replication process triggered by an event (e.g., file change).
     * @param showMessage Whether to show messages to the user.
     */
    replicateByEvent(showMessage?: boolean): Promise<boolean | void> {
        return this.replicationCoordinator.enqueue(() => this.runEventReplicationCycle(showMessage));
    }

    /**
     * Check if there is a connection failure with the remote database.
     */
    readonly checkConnectionFailure = handlers<IReplicationService>().firstResult("checkConnectionFailure");
    databaseQueueCount = reactiveSource(0);
    storageApplyingCount = reactiveSource(0);
    replicationResultCount = reactiveSource(0);

    getActiveReplicatorFor(usage: string) {
        const activeReplicator = this.replicatorService.getActiveReplicator();
        if (!activeReplicator) {
            this._log(`Active replicator not found during ${usage}`, LOG_LEVEL_NOTICE);
            return false;
        }
        return activeReplicator;
    }

    private async performReplicateAllToRemote(showingNotice: boolean): Promise<boolean> {
        if (!(await this.onBeforeReplicate(showingNotice))) {
            this._log(this.context.translate("Replicator.Message.SomeModuleFailed"), LOG_LEVEL_NOTICE);
            return false;
        }
        const currentSettings = this.settingService.currentSettings();
        const activeReplicator = this.getActiveReplicatorFor("sending data to remote");
        if (!activeReplicator) {
            return false;
        }
        const ret = await activeReplicator.replicateAllToServer(currentSettings, showingNotice);
        if (ret) return true;
        const checkResult = await this.checkConnectionFailure();
        if (checkResult == "CHECKAGAIN")
            return await activeReplicator.replicateAllToServer(currentSettings, showingNotice);
        return !checkResult;
    }

    private async performReplicateAllFromRemote(showingNotice: boolean): Promise<boolean> {
        const activeReplicator = this.getActiveReplicatorFor("fetching data from remote");
        if (!activeReplicator) {
            return false;
        }
        const currentSettings = this.settingService.currentSettings();
        const ret = await activeReplicator.replicateAllFromServer(currentSettings, showingNotice);
        if (ret) return true;
        const checkResult = await this.checkConnectionFailure();
        if (checkResult == "CHECKAGAIN")
            return await activeReplicator.replicateAllFromServer(currentSettings, showingNotice);
        return !checkResult;
    }

    async replicateAllToRemote(showingNotice: boolean = false): Promise<boolean> {
        if (!this.appLifecycleService.isReady()) return false;
        return await this.performReplicateAllToRemote(showingNotice);
    }

    async replicateAllFromRemote(showingNotice: boolean = false): Promise<boolean> {
        if (!this.appLifecycleService.isReady()) return false;
        return await this.performReplicateAllFromRemote(showingNotice);
    }

    /**
     * Perform a full upload owned by an active rebuild while the physical database is ready.
     *
     * This concrete maintenance entry point is intentionally absent from `IReplicationService`;
     * it is not a general application-readiness bypass.
     */
    async replicateAllToRemoteForRebuild(showingNotice: boolean = false): Promise<boolean> {
        if (!this.databaseService.isDatabaseReady()) {
            this._log("The selected local database is not ready for the rebuild upload.", LOG_LEVEL_NOTICE);
            return false;
        }
        return await this.performReplicateAllToRemote(showingNotice);
    }

    /**
     * Perform a full download owned by an active rebuild while the physical database is ready.
     *
     * This concrete maintenance entry point is intentionally absent from `IReplicationService`;
     * it is not a general application-readiness bypass.
     */
    async replicateAllFromRemoteForRebuild(showingNotice: boolean = false): Promise<boolean> {
        if (!this.databaseService.isDatabaseReady()) {
            this._log("The selected local database is not ready for the rebuild download.", LOG_LEVEL_NOTICE);
            return false;
        }
        return await this.performReplicateAllFromRemote(showingNotice);
    }

    private _getReplicatorAndPerform(
        action: string,
        perform: (setting: ObsidianLiveSyncSettings, replicator: LiveSyncAbstractReplicator) => Promise<void>
    ) {
        const activeReplicator = this.getActiveReplicatorFor(action);
        if (!activeReplicator) {
            return Promise.resolve();
        }
        const currentSettings = this.settingService.currentSettings();
        return perform(currentSettings, activeReplicator);
    }

    async markLocked(lockByClean: boolean = false): Promise<void> {
        return await this._getReplicatorAndPerform(
            "marking remote locked",
            async (currentSettings, activeReplicator) => {
                return await activeReplicator.markRemoteLocked(currentSettings, true, lockByClean);
            }
        );
    }

    async markUnlocked(): Promise<void> {
        return await this._getReplicatorAndPerform(
            "marking remote unlocked",
            async (currentSettings, activeReplicator) => {
                return await activeReplicator.markRemoteLocked(currentSettings, false, false);
            }
        );
    }

    async markResolved(): Promise<void> {
        return await this._getReplicatorAndPerform(
            "marking remote resolved",
            async (currentSettings, activeReplicator) => {
                return await activeReplicator.markRemoteResolved(currentSettings);
            }
        );
    }
}
