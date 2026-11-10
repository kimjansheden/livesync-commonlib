---
date: 2026-11-10
commonlib_version: '0.1.19-security.26'
livesync_version: '1.0.21'
status: unreleased-lookup-candidate
---

# On-demand Object Storage content

`LiveSyncJournalReplicator` exposes bounded content reads to the existing Self-hosted LiveSync host through its explicit compatibility export. The host owns attachment selection, durable file recovery records, storage operations, and presentation. These methods do not enable CouchDB on-demand chunking or change ordinary journal reception.

`prepareOnDemandChunkIndex()` explicitly prepares encrypted chunk indexes for legacy journals and device-local chunk locators. New uploads publish the same chunk-only index format. Preparation can download legacy journals and publish missing indexes; run it before moving local content out of a Vault.

`fetchRemoteChunks(ids)` reads requested immutable chunks from authenticated journals. It rejects metadata identifiers and oversized requests, checks the current remote listing and removal records, and never advances the receiver checkpoint or applies unrelated document revisions. Opening content does not publish missing indexes. An unreadable or malformed index fails rather than being treated as an absent legacy index.

The lookup candidate searches compact indexes in batches of at most 32 before downloading unindexed legacy journals. Legacy fallback runs only for content still missing after that search, with at most four journal downloads per reader. A lookup persists locators only for requested chunks; explicit preparation still indexes the complete catalogue. Every started batch settles before failure or cancellation returns. These limits apply to each reader, so the host must also bound simultaneous content requests. Focused tests verify catalogue-sized lookup work, persistence counts, legacy fallback and cancellation; they do not establish Android startup or rendering latency.

`fetchRemoteFile(meta, maxBytes)` reconstructs one file from its exact metadata and immutable chunk identifiers, including legacy Eden content. It reads no local chunk cache, enforces a decoded-size limit, and returns an `ArrayBuffer`. The host must retain the metadata descriptor independently of local revision compaction, verify the result against its saved content hash, and preserve a recoverable original before changing physical storage. A false replicator result or a rejected read leaves that original under host ownership.

The replicator uses the normal remote milestone and bounded-activity guards. Rejected, cleaned, and incompatible devices cannot read content or prepare indexes. A resumed host can authorise a fresh content operation; a cancelled older operation stays fenced even when the new one succeeds. Content preflight does not reset transfer checkpoints. Core tests cover encrypted reads, wrong keys, stale locators, interrupted ownership, exact binary reconstruction, and unchanged checkpoints. The S3-compatible HTTP integration additionally fetches encrypted content into a receiver with an empty content database. Android rendering, file movement, and cold-start latency require downstream runtime verification.
