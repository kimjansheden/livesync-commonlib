export type CheckPointInfo = {
    lastLocalSeq: number | string;
    journalEpoch: string;
    knownIDs: Set<string>;
    sentIDs: Set<string>;
    receivedFiles: Set<string>;
    sentFiles: Set<string>;
    /**
     * Incremented whenever an update removes exchange history, such as a reset of the sent history or a
     * confirmed remote wipe. A send only records its progress while this value is the one it started with.
     */
    resetGeneration: number;
};
/**
 * Creates an empty checkpoint. Updates add to its sets in place, so each checkpoint needs its own sets
 * instead of sharing those of {@link CheckPointInfoDefault}.
 */
export function createCheckPointInfoDefault(): CheckPointInfo {
    return {
        lastLocalSeq: 0,
        journalEpoch: "",
        knownIDs: new Set<string>(),
        sentIDs: new Set<string>(),
        receivedFiles: new Set<string>(),
        sentFiles: new Set<string>(),
        resetGeneration: 0,
    };
}
export const CheckPointInfoDefault: CheckPointInfo = createCheckPointInfoDefault();
