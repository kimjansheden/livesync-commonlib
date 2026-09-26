import { LOG_LEVEL_INFO, LOG_LEVEL_NOTICE } from "octagonal-wheels/common/logger";
import type { NecessaryServices } from "@lib/interfaces/ServiceModule";
import type { DatabasePreparationOptions, VaultScanOutcome } from "@lib/services/base/IService";
import { clearVaultScanOutcome } from "@lib/serviceFeatures/offlineScanner";
import { UnresolvedErrorManager } from "@lib/services/base/UnresolvedErrorManager";
import { createInstanceLogFunction, type LogFunction } from "@lib/services/lib/logUtils";

/**
 * Initialise the database and trigger a full vault scan.
 *
 * A scan which fails stops the preparation, unless the host asks for `completeAfterFailedPairs` and the scan returned
 * its aggregate result with only individual pairs failed. The remaining phases then run as after a successful scan,
 * and the scan's outcome reaches the host through `scanOutcome`, which is cleared before the scan.
 * @param host Services container
 * @param log Logging function
 * @param errorManager Error manager
 * @param showingNotice Whether to show notices during initialisation
 * @param reopenDatabase Whether to reopen the database connection
 * @param ignoreSuspending Whether to ignore suspension settings
 * @param options How the host wants the database prepared
 * @returns True if initialisation succeeded
 */

export async function prepareDatabaseForUse(
    host: NecessaryServices<
        "appLifecycle" | "setting" | "vault" | "path" | "database" | "databaseEvents" | "fileProcessing" | "replicator",
        never
    >,
    log: LogFunction,
    errorManager: UnresolvedErrorManager,
    showingNotice: boolean = false,
    reopenDatabase: boolean = true,
    ignoreSuspending: boolean = false,
    options: DatabasePreparationOptions = {}
): Promise<boolean> {
    const appLifecycle = host.services.appLifecycle;
    appLifecycle.resetIsReady();

    if (
        reopenDatabase &&
        !(await host.services.database.openDatabase({
            databaseEvents: host.services.databaseEvents,
            replicator: host.services.replicator,
        }))
    ) {
        return false;
    }
    if (!host.services.database.isDatabaseReady()) {
        return false;
    }
    const scanOutcome: VaultScanOutcome = options.scanOutcome ?? {};
    // Only what this preparation's scan reports may complete it, even when no scan runs at all.
    clearVaultScanOutcome(scanOutcome);
    if (!(await host.services.vault.scanVault(showingNotice, ignoreSuspending, scanOutcome))) {
        const failedPairs = scanOutcome.failedPairs ?? 0;
        if (!options.completeAfterFailedPairs || failedPairs === 0) {
            return false;
        }
        log(
            `The scan could not process ${failedPairs} file(s); preparation continues as the host asked`,
            LOG_LEVEL_INFO
        );
    }
    const ERR_INITIALISATION_FAILED = `Initializing database has been failed on some module!`;
    if (!(await host.services.databaseEvents.onDatabaseInitialised(showingNotice))) {
        errorManager.showError(ERR_INITIALISATION_FAILED, LOG_LEVEL_NOTICE);
        return false;
    }
    errorManager.clearError(ERR_INITIALISATION_FAILED);
    // Run queued event once.
    if (!(await host.services.fileProcessing.commitPendingFileEvents())) {
        return false;
    }
    appLifecycle.markIsReady();
    return true;
}

/**
 * Associate the initialiser file feature with the app lifecycle events.
 * This function binds initialization handlers to the appropriate lifecycle events.
 * @param host Services container with required dependencies
 */
export function usePrepareDatabaseForUse(
    host: NecessaryServices<
        | "API"
        | "appLifecycle"
        | "setting"
        | "vault"
        | "path"
        | "database"
        | "databaseEvents"
        | "fileProcessing"
        | "replicator",
        never
    >
) {
    const log = createInstanceLogFunction("SF:prepareDatabaseForUse", host.services.API);
    const errorManager = new UnresolvedErrorManager(host.services.appLifecycle, host.services.context.events);

    // Handler for database initialisation
    const initialiseDatabaseHandler = async (
        showingNotice: boolean = false,
        reopenDatabase: boolean = true,
        ignoreSuspending: boolean = false,
        options: DatabasePreparationOptions = {}
    ): Promise<boolean> => {
        return await prepareDatabaseForUse(
            host,
            log,
            errorManager,
            showingNotice,
            reopenDatabase,
            ignoreSuspending,
            options
        );
    };

    // Bind handlers to lifecycle events
    host.services.databaseEvents.initialiseDatabase.addHandler(initialiseDatabaseHandler);
}
