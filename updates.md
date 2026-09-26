# Updates

## Unreleased

### Fixed

- On Android, a file no longer reaches other devices as empty because shared storage reported it as empty for a while. That storage can keep a freshly written file at size zero for minutes, occasionally longer, while its content is already on disk, and every reader then sees nothing. The empty read was stored as a revision and replaced the real version everywhere, both for a file created on the device and for one this device had just written while receiving it.
- After writing a received file with content on Android, the file handler reads it back, and while it reads as empty writes it again, at most three times. Writing again corrects the stale size at once. Only the file just written is rewritten, and only a read of zero bytes counts; a received empty file is expected to read as empty. Large binary files which are written in parts (from 16 MiB, `LARGE_BINARY_STREAM_BYTES`) are not read back and rewritten this way; an empty read of such a file is still never stored over its content in the database.
- On Android, a read of zero bytes of a file whose database entry has content is never stored, neither as a revision nor as a conflicted revision. Content which appears is stored as an ordinary change. Deliberately emptying such a file on the device therefore no longer reaches other devices; deleting it still does.
- On Android, a new file, or one which is empty in the database, is stored empty only after it has stayed empty through checks after 3 seconds, 15 seconds, 1 minute, 5 minutes and 15 minutes. Content which appears meanwhile is stored at once. A file which really is empty is therefore synchronised about 21 minutes late, and only if the application runs that long; otherwise the next scan starts the checks again. An empty file which this device itself wrote while receiving it is not held back.
- One confirmation runs per path, and every operation which arrives meanwhile waits in it with its original arguments: stores, stores on a selected revision, and incoming revisions or deletions. Incoming ones run first, once content appears or the checks have passed, and are then applied without preserving the empty read as a conflict. A storage event which reads the file with content runs what waited before it is stored itself, and then reads the file again. Waiting operations run under the same per-path lock as storage events. A check which fails does not settle the confirmation; the next check decides.
- A rename does not delete its source from the database until its target is stored, and a target which stays empty is never stored over a source with content. The source then stays in the database, so the scan of the next start may show the old file again next to the new one: a duplicate, not a loss.
- The confirmations stop when the host unloads. After that nothing waits: an empty read is not stored, a store or rename reports failure, and an incoming change over an empty read is not applied. The scan of the next start reconciles those files.
- Files read with content are handled as before. On every other platform the observable behaviour is unchanged. Internals shared with them were restructured: the preservation of unsynchronised storage content, the application of a database deletion and the rename, which now deletes its source in a separate step. Tests lock their behaviour on other platforms.
- A received deletion or revision of a file of at least 50 MiB (`LARGE_FILE_BYTES`) no longer reads the whole local file to look for unsynchronised changes when this device's record shows that the file still holds a revision which the incoming one was made on: the record was written when this device reflected that revision from the database, the revision is in the incoming revision's history, and the file system itself still reports the modification time and size recorded with it. Reading such a file whole for a deletion could exhaust the memory of a mobile device. Smaller files, files without such a record, and files changed since are compared by content as before, and a local change is still preserved as a conflict. `DatabaseFileAccess.isRevisionInHistory()` decides the history from the revision tree without loading content; hosts without it keep the content comparison.
- The full scan processes pairs where either side is at least 50 MiB one at a time, beside the other pairs, which keep a concurrency of ten.
- The storage-event queue handles files of at least 50 MiB one at a time, as their events report their size, while smaller files keep their concurrency. Storing several such files at once, for example three files of 700 MB which appear in the Vault together, could exhaust the memory of the host. The limit also applies to events restored at start-up and to those the full scan queues again. Events of one path keep their order, and batching is unchanged.
- The full scan hands the pairs which failed over for another attempt. Entries which could not be written to storage, by result or by error, go to `ReplicationService.parseSynchroniseResult()`; storage files and offline deletions which could not be stored into the database are queued again as revalidated storage events through `StorageAccess.appendStorageEvents()`. Anything else, and anything the host cannot take or fails to take, is tried again by the next full scan. The scan still returns `false` when a pair fails.
- The full scan reports its outcome to its own caller: `FullScanOptions.outcome`, and a new third argument of `scanVault()`, receive how many pairs failed and how many of them each queue took, once the whole scan has returned. The scan clears the object first, so one reused from an earlier scan never reports that scan. A scan which could not run, or an error at any point, leaves the outcome empty.
- `prepareDatabaseForUse()` completes the preparation after a scan which returned with only failed pairs when the host asks for it with the new `completeAfterFailedPairs` option of `initialiseDatabase()`. It then dispatches `onDatabaseInitialised()` with the usual error handling, releases the current batch waits and sets readiness, and hands the scan's outcome to the host. It clears that outcome before the scan, so an outcome left by an earlier scan cannot complete a preparation whose scan did not run. Without the option, a failed scan stops the preparation as before.
- A binary file deleted in storage no longer has its content loaded from the database to confirm that the winning revision is here before the deletion is stored: its chunks are checked in small batches without holding them, through the new optional `DatabaseFileAccess.inspectBinaryContentFromMeta()`. Loading a large file whole could exhaust the memory of a mobile device. Text files, and hosts without that method, load the entry as before.
- `LiveSyncLocalDB.inspectDBEntryBinaryContent()` tells whether a binary entry can be written in parts, has a chunk missing, or needs the general loading path, without holding or decoding its content. `getDBEntryFromMeta()` takes the level at which a failed load is logged, so a caller which reports the failure itself can log it quietly.

## 0.1.19-security.10

### Fixed

- A fetch performed in remediation mode can now complete. The mode reflects only the documents modified before the configured moment, and it prevents reconciliation scanning between the storage and the local database. The fetch requested that prevented scan twice and treated each refusal as a failure: once when storing the current files of the Vault beforehand, and once when finalising. A fetch started to recover an earlier state therefore ended in an error, after the local database had been reset and with reflection left suspended. Both scans are now skipped in this mode. The files in storage are also no longer stored in the database first, which would have published the state being replaced.
- The host remains restricted after such a fetch, as it is during an ordinary start in this mode, rather than reporting readiness which its prevented scan cannot support. Received documents are still reflected within the configured limit, and storage events stay unqueued, so local changes are not sent.
- Rebuilding is now refused while remediation mode is active, before the local database is reset. Rebuilding publishes the current storage as the remote, which is the opposite of restoring an earlier state; it previously failed on the same prevented scan once the local database had already been reset.

## 0.1.19-security.9

### Fixed

- A journal history reset no longer gets overwritten by a transfer which is still running. The maintenance actions which reset the sent or received history, or delete the remote, use their own journal client on the same store. A send running at the same time recorded its progress afterwards, so the reset was undone and the changes before that point were never sent again, for example to the cleared remote. A receive re-recorded changes from the old remote as known, so the next send skipped them. Each update which removes history now increments a reset generation in the checkpoint, and a transfer records progress only while the generation is the one it started with. Otherwise the transfer stops as failed, and the next one starts from the reset checkpoint. Checkpoint updates are serialised, so concurrent updates from several clients no longer discard each other.
- Clearing the bucket always resets the checkpoint afterwards. Previously it did so only when journals remained after the deletion, which is normally not the case, so a transfer which started while the bucket was being cleared could record journals of the cleared remote as received or known.
- Resetting the remote from a journal replicator now aborts the running requests of its client and waits for its transfers to settle before the bucket is cleared, so a transfer which started before the reset no longer uploads to the cleared bucket. A cycle which starts later can still run while the bucket is cleared; the reset generation makes it fail instead of recording progress, and a send which joins it reports that failure.
- After another device wiped the remote, this device now scans its local database from the start again. The wipe was detected and the dedupe caches were cleared, but the sent sequence was kept, so changes this device had sent only to the old remote were never sent to the new one. A sync receives the new remote first and skips every revision it already holds. Revisions and chunks which the new remote lacks are uploaded, such as unused chunks or files which the wiping device did not have; a device which should not restore them must fetch the rebuilt remote instead of resuming. A send without a preceding receive, such as sending everything to the remote, uploads the whole local database.
- A reset checkpoint no longer shares its sets with the default checkpoint. An update which added to a set of a checkpoint read from an empty store changed the default, so a later reset kept those entries.

## 0.1.19-security.8

### Fixed

- A storage change made while a host was starting could stay local until the next start. The start-up scan lists storage and marks the application ready before the Vault watcher is registered, so a file created, changed, deleted or renamed after that listing was neither scanned nor watched. Once the watcher has begun, the Offline Scanner now lists storage again and queues the differences through the watcher path as intent to revalidate. A file is not queued when its size is as listed and its modification time is the one the scan recorded, and a path the scan removed itself is not queued. A file the scan wrote may still be queued, for example on a host which does not keep the modification time of a written file; the file handler then recognises it as described below. The listing is kept only until this start-up reconciliation, which leaves the changes to the next scan if the filename case setting changed in the meantime.
- A revalidated storage operation, from this reconciliation or restored from the previous run, no longer stores a file which still holds the revision this device last reflected or stored for it. Storing it published unchanged content as a new revision, or replaced a newer revision which had reached the database but not yet storage. Small files are compared by content; a large file is recognised by its recorded modification time and size, so its content is not loaded. A file without a usable record and every other file are stored as before. A file present while the current revision is a deletion is stored too, so a deletion which reaches the database during start-up may be undone rather than local work lost.
- A revalidated deletion no longer removes a newer revision silently. When storage last displayed an older revision, the deletion is stored on that revision and the newer one becomes a conflict for the usual conflict resolution. Without a record, the deletion applies only when the time of the event, the file as last seen or the moment of deletion, is not older than the current revision, similar to the rule of the Offline Scanner, and the next scan reconciles the path otherwise. As with that rule, clocks which disagree between devices can still decide the wrong way.

## 0.1.19-security.7

### Fixed

- A completed journal send records the last scanned local sequence even when every scanned entry was already received or sent. Previously the position advanced only when something was uploaded, so a device which had only received changes, such as a client restored by Standard Fetch, read its whole local database again on every cycle before its next receive could start. The position is recorded only after every read pack has been uploaded, so a failed upload still leaves its unsent entries to the next attempt.

The first send after a full restore still scans the received entries once; its duration grows with the local database because the IndexedDB change feed reads each stored document.

## 0.1.19-security.6

### Fixed

- Large binary entries can be read in bounded batches and passed to a host as successive parts instead of retaining every encoded chunk and several decoded copies. Binary comparisons use slices without copying the whole file.
- The optional staged-binary host capability must keep every part outside synchronised paths until the entire file is complete. Publication checks the target state approved by the existing conflict flow and uses callbacks to persist a recoverable publication marker before replacement and complete provenance inside the host queue. Hosts without this capability retain their existing storage path.
- A missing target after interrupted publication is recovered through the current database revision and conflict rules, never published as a local deletion. An existing target is always a complete version; an interrupted completion record is retired before a later genuine local deletion. Unreadable provenance fails closed.
- Source, staging, and publication failures never hand a partially processed large file to a whole-buffer fallback. The unreleased in-place retry maps, partial-file classifiers, and forced handovers have been removed.
- The offline scan compares sizes as well as rounded timestamps, and recently stored empty files are checked again for late content. Intentional remote emptying still follows the normal newer-version rules.
- Reflection provenance avoids rereading an unchanged large attachment. Only a completed database reflection establishes that provenance; ordinary storage observations do not certify a remote write.
- Hidden-file removal now awaits its confirming metadata read.

The staged publication contract is exercised by the maintained Obsidian Windows/Android adapter. It permits a recoverable missing-file window on Android and does not claim atomic replacement. Native device and 1 GB transfer evidence belongs to the downstream pilot report, not to the package unit-test result.

## 0.1.19-security.5

### Fixed

- Object Storage journal requests can now be aborted when a host knows they were suspended. `abortStaleRemoteRequests(startedBefore)` on a replicator aborts only journal requests which started before that time and are still in flight, so a request frozen while a mobile app was in the background no longer holds the durable replication lease until the network stack gives up. The interrupted operation fails normally and its work remains pending; replicators without abortable remote requests report that nothing was aborted.
- An Object Storage milestone which cannot be read now stops the connection check without writing. Previously an interrupted or failed read was treated like a missing milestone, and a fresh default could overwrite the remote lock state and other devices' entries.
- A journal upload batch which ends inside a pack now advances the local checkpoint only past the packs it contains completely. If the next upload failed, the rest of that pack was previously skipped for good.
- Journal file names now also depend on the documents in the batch. A retry of the same batch still reuses its object, but a retry with other documents can no longer overwrite a journal file which other devices may already have received.
- Locking, unlocking or resolving an Object Storage remote now stops without writing when the milestone cannot be read, instead of replacing it with a fresh default.
- A failed or aborted journal listing now fails the receive like a failed download, instead of throwing out of the replication cycle.
- A caller which joins a replication drain that then fails now runs one attempt of its own when the failed drain never attempted its generation, so a request made while an interrupted cycle unwinds is not lost. A stopped or failed attempt which already covered the caller's request is not repeated, and callers of one failed drain share a single retry.
- An earlier CouchDB synchronisation which ends after a newer one has started no longer aborts the newer synchronisation's controller.
- Resuming pending replication on load and resume now runs in the background. It no longer holds up the remaining lifecycle handlers while a cycle is still unwinding, and a failed attempt no longer stops them, which previously left the periodic replication timer disabled after resume.
- Journal replication now reports an interrupted or failed cycle as unsuccessful, so its durable generation stays pending instead of being recorded as complete.
- A replication cycle which throws now releases its lease at once, so another runtime does not wait for the lease to expire.

## 0.1.19-security.3

### Fixed

- Finite replication triggers now use a persistent single-flight generation queue with a renewable, fenced lease capped at 45 seconds. Triggers arriving during an active cycle force a subsequent cycle, interrupted work remains pending across restart, and stale leases can be reclaimed without parallel local writers.
- Object Storage journal uploads now use an opaque, deterministic operation identity and advance receive checkpoints only after successful local processing. Journal discovery follows every paginated S3 listing and cannot miss an unseen object merely because its obfuscated key sorts before an earlier checkpoint.
- Storage writes received from a remote peer now retain their in-flight ingestion guard for 500 milliseconds, preventing the receiver's own disk reflection from being republished as a user edit.
- The security test toolchain now pins the patched `qs` 6.16.0 transitively, keeping the release audit free of the superseded 6.15.3 advisories.

## 0.1.19-security.2

### Fixed

- Attested security releases now bind source-receipt lockfile and package hashes to the exact jq argument names, with a regression contract that rejects undeclared receipt variables before publication.

## 0.1.19-security.1

### Fixed

- Pure Object Storage configurations now activate encrypted settings persistence even when no CouchDB field is populated. The persisted settings copy also clears and restores every encrypted CouchDB and Object Storage connection field, including custom headers, JWT material, bucket prefixes, and transport options.
- Package-boundary tooling, packed-package checks, the headless-service bundle contract, and downstream-import inventory now run correctly on Windows as well as POSIX systems. Generated package cleanup also retries transient file locks from synchronised working directories.

## 0.1.19

### Fixed

- Reset and rebuild workflows now reset the local database selected by the resulting settings, rather than whichever database wrapper was already active. During a suffix transition, this prevents a database which was not reset from reopening with stale content. Failures stop before rebuilt events, remote resets, or uploads (Self-hosted LiveSync issue #1126).
- Rejected or incomplete database initialisation no longer leaves either the physical database or the application marked as ready. Vault scanning and completion hooks now form explicit readiness boundaries, and direct file manipulation rejects an unaccepted database initialisation.

### Improved

- Restored storage events are now revalidated against current exact storage paths before replay. Current file contents replace saved observations, stale deletions are suppressed, and rename halves are admitted only when their current path state supports them.

## 0.1.18

### Added

- P2P profiles can now select an outgoing RPC message bound through `P2P_maxWirePayloadBytes`. `P2PMessageSizePresets` provides Standard (15,360 bytes), Reduced (2,048 bytes), Conservative (1,024 bytes), and Maximum compatibility (800 bytes), while omitted or invalid values retain the established Standard behaviour. This gives constrained WebRTC paths a supported alternative to modifying installed package code. Thank you to @andrewschreiber for the detailed diagnosis and reproducible workaround in [issue #97](https://github.com/vrtmrz/livesync-commonlib/issues/97).
- P2P profiles can select automatic ICE routing or require a configured TURN relay through `P2P_connectionPath`. Relay-only routing is effective only when at least one syntactically valid `turn:` or `turns:` URL is present. P2P connection strings and QR-encoded settings retain both compatibility choices.

### Improved

- A serving P2P transport is now recreated when its effective message bound or connection path changes. Repeated opens with unchanged compatibility settings remain idempotent.

## 0.1.17

### Added

- `storeWithLiveBaseRevision()` conditionally stores content below an exact current revision-tree leaf using PouchDB's ordinary revision check. Maintained hosts can therefore create a successor without force-writing below a stale base, while the existing `storeWithBaseRevision()` operation retains its deliberate force-write behaviour.

## 0.1.16

### Improved

- CouchDB connection handling is now more robust through the flat `OwnedCouchDBConnection` contract. Its idempotent `close()` cancels abort-capable requests before closing PouchDB. If the complete one-shot preflight remains unsettled for 60 seconds on the web-compatible fetch path, an internal safety fuse ends the attempt, releases its shared operation, and closes the temporary connection before a later trigger can try again. This is not a limit on replication duration. The explicitly selected native Request API retains its existing behaviour because that adapter cannot currently honour transport cancellation.

## 0.1.15

### Fixed

- Start-up offline scans are now faster when Path Obfuscation is enabled.

## 0.1.14

### Fixed

- Commonlib now closes the temporary CouchDB connections it creates for finite remote operations, including one-shot replication, Security Seed refreshes, chunk transfer, maintenance operations, and status queries. Continuous replication closes its previous connection before a retry or restart, while caller-provided connections remain under caller ownership. Connection set-up failures also close the partially initialised handle without masking the original connection error (PR #112). Thank you to @apple-ouyang for the contribution!

## 0.1.13

### Fixed

- Generated packages now declare root and subpath TypeScript mappings derived from the same public export inventory. TypeScript's `Node10` module resolution therefore finds the intended declarations instead of treating valid Commonlib imports as unresolved `error` types in downstream tooling.
- `octagonal-wheels` 0.1.53 is now the minimum dependency, bringing equivalent declaration mappings to its public entries.

## 0.1.12

### Fixed

- Fast Fetch now writes deletion tombstones to the local database without attempting to decrypt them. A tombstone has no encrypted payload, and decryption previously aborted the whole fetch at the first deleted document. New devices could not complete their initial sync on vaults that contain old deletions ([Self-hosted LiveSync issue #1099](https://github.com/vrtmrz/obsidian-livesync/issues/1099); PR #108). Thank you to @KennethLloyd for the contribution!
- Offline scans now validate each Metadata document against its actual database ID before pairing it with storage. An inconsistent entry is left unchanged and cannot trigger reflection, deletion, or last-seen updates; a separate, consistent entry for the same logical path continues normally. Maintained hosts can inspect the mismatch by actual ID and explicitly repair one unambiguous entry at a time.

## 0.1.11

### Fixed

- Full offline scans now distinguish completed, deliberately skipped, and failed storage/database pairs. Failed database-to-storage reflections no longer record the database mtime as local last-seen evidence, preventing a later `NEWER_WINS` scan from misclassifying a still-missing file as an offline deletion. Actual failures propagate to maintained hosts, while conflict and size-policy skips remain non-fatal ([Self-hosted LiveSync issue #1065](https://github.com/vrtmrz/obsidian-livesync/issues/1065)).

## 0.1.10

### Fixed

- Fast Fetch now falls back to Standard Fetch when the internal Request API is enabled, avoiding a buffered transport which cannot provide the progressive response reading or request cancellation Fast Fetch requires. Standard Fetch also discards obsolete Fast Fetch checkpoints after resetting the local database ([Self-hosted LiveSync issue #1020](https://github.com/vrtmrz/obsidian-livesync/issues/1020)).

## 0.1.9

### Fixed

- Fast Fetch now forwards configured CouchDB custom headers to every changes-feed request, allowing reverse proxies such as Cloudflare Access to authenticate initial setup consistently with ordinary replication (PR #82). Thank you to @nimula for the contribution!

## 0.1.8

### Fixed

- Fast Fetch now uses a one-second idle timeout for each finite CouchDB changes page instead of a heartbeat, allowing CouchDB 3.2 to return its terminator after the currently available rows have been persisted.

## 0.1.7

### Fixed

- Fast Fetch now sizes each finite CouchDB changes page from a one-row status probe, counts the returned result together with `pending`, and resumes from the page's opaque `last_seq` without comparing token representations. Heartbeat-enabled feeds no longer wait for future writes after the currently available rows have been persisted.

## 0.1.6

### Fixed

- Fast Fetch now completes only after the captured CouchDB changes target has been persisted, and resumes transient interruptions from the last durable checkpoint. Decryption, protocol, and local write failures stop without finalising an incomplete local database ([Self-hosted LiveSync issue #1065](https://github.com/vrtmrz/obsidian-livesync/issues/1065)).

## 0.1.5

### Changed

- Remote-preferred synchronisation setting reads now report explicit available, not-configured, unavailable, or unsupported outcomes, so clients can distinguish a remote without saved synchronisation settings from one whose settings could not be read.

## 0.1.4

### Added

- The settings schema now includes controls for allowing operating-system sleep during finite synchronisation operations on every platform or on desktop only. Setup URIs preserve both preferences.

## 0.1.3

### Fixed

- Remote-only connection and configuration checks no longer access the local database while constructing a replicator, preventing start-up failures before local database initialisation ([Self-hosted LiveSync issue #1064](https://github.com/vrtmrz/obsidian-livesync/issues/1064)).

## 0.1.2

### Fixed

- Unnecessary missing-content warnings are now suppressed when a local file already matches known synchronised history; the existence check stops at the first exact content match instead of reading older revisions which cannot change its result.
- Remote chunk fetching now keeps successfully returned chunks when another requested chunk is unavailable, preventing the latter from making the whole request appear to have failed ([Self-hosted LiveSync issue #771](https://github.com/vrtmrz/obsidian-livesync/issues/771)).

## 0.1.1

### Improved

- `DirectFileManipulator` can receive a host fetch implementation for direct CouchDB access in runtimes such as Deno.
- Direct file manipulation now avoids application-owned replication and key-value database lifecycle work.

### Fixed

- `DirectFileManipulator` now yields metadata-only enumeration results, restores its dedicated path-obfuscation passphrase, and contains document loading failures observed while watching changes (PR #22). Thank you to @es617 for the fixes!
- `DirectFileManipulator` now reports initialisation failures through its readiness promise, and headless logging and manager construction use the capabilities supplied by their composition. This also addresses the start-up failures independently identified in PR #50. Thank you to @adriy-be for the diagnosis and proposed fixes!

### Deprecated

- `SvelteDialogMixIn` remains available for compatibility, but maintained hosts should compose their dialogue lifecycle explicitly.
