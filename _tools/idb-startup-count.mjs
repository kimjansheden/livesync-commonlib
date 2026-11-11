import { createHash } from "node:crypto";
import { copyFile, readFile, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname, resolve } from "node:path";

const require = createRequire(import.meta.url);
const reviewedSourceSha256 = "474d791202bcc6c51a15eb37bbc85e581671131ed38bf09875d935ee8c6cf1ab";

/** Reuse the count maintained in the same transaction as each IDB document write. */
export function patchIdbStartupCount(source) {
    if (createHash("sha256").update(source).digest("hex") !== reviewedSourceSha256) {
        throw new Error("The IDB adapter source changed; review its metadata invariants before rebuilding.");
    }
    const before = `      metaDoc = e.target.result || { id: META_STORE };
      storeMetaDocIfReady();`;
    const after = `      metaDoc = e.target.result || { id: META_STORE };
      // bulkDocs updates docCount atomically with the document mutations.
      // Older or invalid metadata is counted once before it can be reused.
      if (Number.isSafeInteger(metaDoc.docCount) && metaDoc.docCount >= 0) {
        docCount = metaDoc.docCount;
        storeMetaDocIfReady();
      } else {
        countDocs(txn, function (count) {
          docCount = count;
          storeMetaDocIfReady();
        });
      }`;
    const recount = `    countDocs(txn, function (count) {
      docCount = count;
      storeMetaDocIfReady();
    });`;
    if (!source.includes(before) || !source.includes(recount))
        throw new Error("Expected IDB startup operations missing.");
    return source
        .replace(before, after)
        .replace(recount, "    // The metadata read above selects the cached count or its recovery recount.");
}

export async function readPatchedIdbAdapter() {
    const entry = require.resolve("pouchdb-adapter-idb");
    const path = resolve(dirname(entry), "index.es.js");
    return patchIdbStartupCount(await readFile(path, "utf8"));
}

export async function writePatchedIdbAdapter(outputDirectory) {
    // Keep the upstream module imports external so package dependency and licence tooling still sees them.
    await writeFile(resolve(outputDirectory, "pouchdb/idb-adapter.js"), await readPatchedIdbAdapter());
    await copyFile(
        resolve(dirname(require.resolve("pouchdb-adapter-idb")), "../LICENSE"),
        resolve(outputDirectory, "pouchdb/idb-adapter.LICENSE")
    );
}
