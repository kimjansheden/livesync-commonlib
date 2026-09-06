import type { SimpleStore } from "octagonal-wheels/databases/SimpleStoreBase";

export interface KeyValueDatabase {
    get<T>(key: IDBValidKey): Promise<T>;
    set<T>(key: IDBValidKey, value: T): Promise<IDBValidKey>;
    atomicUpdate<T, R>(key: IDBValidKey, change: (current: T | undefined) => { value: T; result: R }): Promise<R>;
    del(key: IDBValidKey): Promise<void>;
    clear(): Promise<void>;
    keys(query?: IDBValidKey | IDBKeyRange, count?: number): Promise<IDBValidKey[]>;
    close(): Promise<void>;
    destroy(): Promise<void>;
}

export type AtomicSimpleStore<T> = SimpleStore<T> & {
    atomicUpdate<R>(key: string, change: (current: T | undefined) => { value: T; result: R }): Promise<R>;
};

/** Opens the key-value database owned by one service composition. */
export type KeyValueDatabaseFactory = (databaseKey: string) => Promise<KeyValueDatabase>;
