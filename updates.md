# Updates

## Unreleased

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
