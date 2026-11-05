import { LOG_LEVEL_INFO, Logger } from "@lib/common/logger";
import {
    LOG_LEVEL_VERBOSE,
    SYNCINFO_ID,
    VER,
    VERSIONING_DOCID,
    type EntryVersionInfo,
    type SyncInfo,
} from "@lib/common/types";
import { resolveWithIgnoreKnownError } from "@lib/common/utils";
import { isErrorOfMissingDoc } from "./utils_couchdb";

export const checkRemoteVersion = async (
    db: PouchDB.Database,
    migrate: (from: number, to: number) => Promise<boolean>,
    barrier: number = VER
): Promise<boolean> => {
    try {
        const versionInfo = (await db.get(VERSIONING_DOCID)) as EntryVersionInfo;
        if (versionInfo.type != "versioninfo") {
            return false;
        }
        // const salt = versionInfo?.pbkdf2salt;
        const version = versionInfo.version;
        if (version < barrier) {
            const versionUpResult = await migrate(version, barrier);
            if (versionUpResult) {
                await bumpRemoteVersion(db);
                return true;
            }
        }
        // setPBKDF2Salt(salt);
        if (version == barrier) return true;
        return false;
    } catch (ex) {
        if (isErrorOfMissingDoc(ex)) {
            if (await bumpRemoteVersion(db)) {
                return true;
            }
            return false;
        }
        throw ex;
    }
};
export const bumpRemoteVersion = async (db: PouchDB.Database, barrier: number = VER): Promise<boolean> => {
    const vi: EntryVersionInfo = {
        _id: VERSIONING_DOCID,
        version: barrier,
        type: "versioninfo",
        // pbkdf2salt: "", // this will be set later.
    };
    const versionInfo = await resolveWithIgnoreKnownError<EntryVersionInfo>(db.get(VERSIONING_DOCID), vi);
    if (versionInfo.type != "versioninfo") {
        return false;
    }
    vi._rev = versionInfo._rev;

    await db.put(vi);
    return true;
};

export const checkSyncInfo = async (db: PouchDB.Database): Promise<boolean> => {
    try {
        const syncinfo = (await db.get(SYNCINFO_ID)) as SyncInfo;
        console.log(syncinfo);
        // if we could decrypt the doc, it must be ok.
        return true;
    } catch (ex) {
        if (isErrorOfMissingDoc(ex)) {
            const randomStrSrc = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
            const temp = [...Array(30)]
                .map((_e) => Math.floor(Math.random() * randomStrSrc.length))
                .map((e) => randomStrSrc[e])
                .join("");
            const newSyncInfo: SyncInfo = {
                _id: SYNCINFO_ID,
                type: "syncinfo",
                data: temp,
            };
            if (await db.put(newSyncInfo)) {
                return true;
            }
            return false;
        } else {
            console.dir(ex);
            return false;
        }
    }
};

// Selectors to already transferred compromised chunks (before h:, i.e., "0123456")
const SELECTOR_COMPROMISED_CHUNK_1 = {
    selector: {
        _id: {
            $lt: "h:",
        },
        type: "leaf",
    },
} as const;

// Selectors to already transferred compromised chunks (after h:, i.e., "ijklmnop")
const SELECTOR_COMPROMISED_CHUNK_2 = {
    selector: {
        _id: {
            $gt: "h;",
        },
        type: "leaf",
    },
} as const;

/**
 * Counts the number of remote (potentially) compromised chunks in the database.
 * @param db The PouchDB database instance.
 * @returns The number of compromised chunks or false if an error occurs.
 */
export async function countCompromisedChunks(db: PouchDB.Database): Promise<number | false> {
    try {
        Logger(`Counting compromised chunks...`, LOG_LEVEL_VERBOSE);
        const task1 = db.find(SELECTOR_COMPROMISED_CHUNK_1);
        const task2 = db.find(SELECTOR_COMPROMISED_CHUNK_2);
        const [result1, result2] = await Promise.all([task1, task2]);
        return result1.docs.length + result2.docs.length;
    } catch (ex) {
        Logger(`Error counting compromised chunks!`, LOG_LEVEL_INFO);
        Logger(ex, LOG_LEVEL_VERBOSE);
        return false;
    }
}

export const COMPROMISED_CHUNK_SCAN_RECEIPT_KEY = "compromised-chunk-clean-scan-v1";
const CHUNK_NAMESPACE_POLICY = "leaf-h-namespace-v1";
const COMPROMISED_CHUNK_CHANGE_BATCH = 256;
type CleanChunkReceipt = {
    version: 1;
    policy: typeof CHUNK_NAMESPACE_POLICY;
    databaseId: string;
    verifiedSequence: number;
};
type CleanChunkReceiptStore = {
    get(key: string): Promise<unknown>;
    set(key: string, value: unknown): Promise<unknown>;
};
type LocalChunkDatabase = PouchDB.Database & { id?: () => Promise<string> };
function comparableLocalSequence(sequence: unknown): sequence is number {
    return typeof sequence === "number" && Number.isSafeInteger(sequence) && sequence >= 0;
}
function cleanChunkReceipt(value: unknown, databaseId: string, sequence: number): value is CleanChunkReceipt {
    if (!value || typeof value !== "object") return false;
    const receipt = value as Partial<CleanChunkReceipt>;
    return (
        receipt.version === 1 &&
        receipt.policy === CHUNK_NAMESPACE_POLICY &&
        receipt.databaseId === databaseId &&
        comparableLocalSequence(receipt.verifiedSequence) &&
        receipt.verifiedSequence <= sequence
    );
}
class IncomparableChunkSequence extends Error {}

/** Object Storage's local safety gate. Opaque remote sequences retain the original full check. */
export async function countCompromisedChunksSinceReceipt(
    db: LocalChunkDatabase,
    store: CleanChunkReceiptStore
): Promise<number | false> {
    try {
        const fullLocalCheck = async (): Promise<number | false> => {
            const count = await countCompromisedChunks(db);
            if (count !== 0) return count;
            // The historical remote selector uses > h; and omits that exact boundary ID.
            const boundary = await db.allDocs<{ type?: string }>({ keys: ["h;"], include_docs: true });
            return boundary.rows.some((row) => "doc" in row && row.doc?.type === "leaf") ? 1 : 0;
        };
        const identity = db.id?.bind(db);
        if (!identity) return await fullLocalCheck();
        const databaseId = await identity();
        if (typeof databaseId !== "string" || databaseId.length === 0) return await fullLocalCheck();
        const initial = await db.info();
        const initialSequence = initial.update_seq;
        if (!comparableLocalSequence(initialSequence)) return await fullLocalCheck();
        const saved = await store.get(COMPROMISED_CHUNK_SCAN_RECEIPT_KEY);
        const valid = cleanChunkReceipt(saved, databaseId, initialSequence);
        if (valid && saved.verifiedSequence === initialSequence) return 0;

        let since = valid ? saved.verifiedSequence : initialSequence;
        if (!valid) {
            // Doctor/settings flags do not prove a clean scan: the historical dialogue could be dismissed.
            const full = await fullLocalCheck();
            if (full !== 0) return full;
        }
        const snapshot = await db.info();
        const target = snapshot.update_seq;
        if (!comparableLocalSequence(target) || target < since) return await fullLocalCheck();
        try {
            while (since < target) {
                // Do not materialise the bodies of newly stored modern chunks.
                const page = await db.changes({
                    since,
                    limit: COMPROMISED_CHUNK_CHANGE_BATCH,
                    include_docs: false,
                    live: false,
                });
                if (!comparableLocalSequence(page.last_seq) || page.last_seq <= since)
                    throw new IncomparableChunkSequence();
                const candidates = [
                    ...new Set(
                        page.results
                            .filter((change) => !change.deleted && !change.id.startsWith("h:"))
                            .map((change) => change.id)
                    ),
                ];
                if (candidates.length > 0) {
                    const rows = await db.allDocs<{ type?: string }>({ keys: candidates, include_docs: true });
                    const compromised = rows.rows.filter((row) => "doc" in row && row.doc?.type === "leaf").length;
                    if (compromised > 0) return compromised;
                }
                since = page.last_seq;
                if (page.results.length === 0 && since < target) throw new IncomparableChunkSequence();
            }
        } catch (error) {
            if (!(error instanceof IncomparableChunkSequence)) throw error;
            return await fullLocalCheck();
        }
        const final = await db.info();
        if (
            !comparableLocalSequence(final.update_seq) ||
            final.update_seq < target ||
            final.update_seq > since ||
            (await identity()) !== databaseId
        )
            return false;
        await store.set(COMPROMISED_CHUNK_SCAN_RECEIPT_KEY, {
            version: 1,
            policy: CHUNK_NAMESPACE_POLICY,
            databaseId,
            verifiedSequence: target,
        } satisfies CleanChunkReceipt);
        return 0;
    } catch (error) {
        Logger("Den lokala kontrollen av komprometterade chunkar kunde inte verifieras", LOG_LEVEL_INFO);
        Logger(error, LOG_LEVEL_VERBOSE);
        return false;
    }
}
