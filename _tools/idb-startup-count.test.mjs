import "fake-indexeddb/auto";
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { randomUUID } from "node:crypto";
import { test } from "node:test";
import { transformSync } from "esbuild";
import PouchDB from "pouchdb-core";
import { patchIdbStartupCount, readPatchedIdbAdapter } from "./idb-startup-count.mjs";

const require = createRequire(import.meta.url);
const compiled = transformSync(await readPatchedIdbAdapter(), { format: "cjs", loader: "js" }).code;
const module = { exports: {} };
new Function("require", "module", "exports", compiled)(require, module, module.exports);
PouchDB.plugin(module.exports.default);

test("the reviewed IDB patch rejects unreviewed dependency bytes", () => {
    assert.throws(() => patchIdbStartupCount("changed dependency"), /source changed/);
});

test("IDB reopens reuse atomic counts, and missing or invalid counts recover once", async () => {
    const name = `startup-count-${randomUUID()}`;
    let recounts = 0;
    const count = IDBIndex.prototype.count;
    IDBIndex.prototype.count = function (...args) {
        if (this.name === "deletedOrLocal") recounts++;
        return count.apply(this, args);
    };
    let db;
    const open = () => new PouchDB(name, { adapter: "idb" });
    const modifyCounter = async (value) => {
        const databases = await indexedDB.databases();
        const physical = databases.find((row) => row.name.endsWith(name));
        assert.ok(physical);
        const connection = await new Promise((resolve, reject) => {
            const request = indexedDB.open(physical.name);
            request.onsuccess = () => resolve(request.result);
            request.onerror = () => reject(request.error);
        });
        try {
            await new Promise((resolve, reject) => {
                const tx = connection.transaction("meta-store", "readwrite"),
                    store = tx.objectStore("meta-store");
                store.get("meta-store").onsuccess = (event) => {
                    const row = event.target.result;
                    if (value === undefined) delete row.docCount;
                    else row.docCount = value;
                    store.put(row);
                };
                tx.oncomplete = resolve;
                tx.onabort = () => reject(tx.error);
            });
        } finally {
            connection.close();
        }
    };
    try {
        db = open();
        assert.equal((await db.info()).doc_count, 0);
        assert.equal(recounts, 1);
        await db.bulkDocs([
            { _id: "a", value: 1 },
            { _id: "b", value: 2 },
            { _id: "_local/receipt", value: 3 },
        ]);
        await db.close();
        db = open();
        assert.equal((await db.info()).doc_count, 2);
        assert.equal(recounts, 1);
        await db.remove(await db.get("a"));
        const b = await db.get("b");
        await db.put({ ...b, value: 4 });
        await assert.rejects(db.put({ ...b, value: 5 }), (error) => error.status === 409);
        if (db) {
            await db.close();
            db = undefined;
        }
        db = open();
        assert.equal((await db.info()).doc_count, 1);
        assert.equal(recounts, 1);
        await db.close();
        db = undefined;
        for (const value of [undefined, -1, 1.5, "invalid"]) {
            await modifyCounter(value);
            const before = recounts;
            db = open();
            assert.equal((await db.info()).doc_count, 1);
            assert.equal(recounts, before + 1);
            await db.close();
            db = undefined;
            db = open();
            assert.equal((await db.info()).doc_count, 1);
            assert.equal(recounts, before + 1);
            await db.close();
            db = undefined;
        }
        db = open();
        await db.remove(await db.get("b"));
        await db.close();
        const beforeEmptyReopen = recounts;
        db = open();
        assert.equal((await db.info()).doc_count, 0);
        assert.equal(recounts, beforeEmptyReopen);
        await db.destroy();
        db = undefined;
    } finally {
        IDBIndex.prototype.count = count;
        if (db) await db.destroy();
    }
});
