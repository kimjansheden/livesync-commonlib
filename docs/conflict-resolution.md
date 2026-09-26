# Conflict resolution and file provenance

This document defines the Commonlib-owned revision-tree rules used when LiveSync detects, merges, resolves, and reflects file conflicts. A host still owns its user interface, binary-file policy, persistence, and the decision to offer an explicit newer-file mode.

## Revision-tree model

PouchDB stores every file metadata document as a revision tree. One deterministic leaf is returned as the current winner, while the other live leaves appear in `_conflicts`. The winner is a database choice, not evidence that its content is newer, safer, or the version displayed by a host.

For example:

```text
A1
├── B1 ── C1 ── D1
└── B2 ── C2
```

`D1` and `C2` are the leaves to compare. Their nearest common ancestor is `A1`, not `B1` or `B2`. Automatic three-way merge may proceed only when the same `available` revision is present in both leaf histories. A matching generation number alone does not establish ancestry.

Resolving a conflict writes the selected or merged result on one branch and deletes every losing live leaf which the resolver has observed. Deleted leaves remain part of the tree until compaction removes their bodies. A stale client can therefore receive the resolved branch and the tombstone for a branch whose old content is still present in its storage.

## Chunk reachability during conflicts

A host which collects unreferenced chunks must include the current winner, every other live conflict leaf, their available divergent revisions, and their nearest available shared ancestor in its reachability scan. This preserves the content and merge base required to review an unresolved conflict. Chunk identifiers are shared across documents, so one reachability set must cover the whole database.

After the conflict is resolved, the deleted losing branch and no-longer-needed merge ancestry stop protecting their unique chunks. An ordinary superseded linear revision also does not protect its former chunks. A host must keep garbage collection separate from repair because collection cannot reconstruct content which is already unavailable.

## Safety invariants

Commonlib follows these rules:

- compare file content byte-for-byte; a path, size, modification time, or revision generation is not proof of identity;
- use the nearest `available` revision shared by both leaves as the base for three-way merge;
- retain a manual conflict when the common revision or a required body is missing or compacted;
- treat content found in any available branch of the same tree, including an ancestor of a deleted leaf, as content which has already been synchronised;
- preserve storage content as a conflict when it is absent from all available branches; and
- never select the newest modification time as a package-level default.

The history check gathers available revisions from every live branch, then compares revision bodies until it finds an exact byte match. Its boolean existence form stops at that first match because older bodies cannot change the answer. This avoids reading obsolete historical bodies whose unique chunks were collected after they stopped being reachable.

This check matters after a resolution has propagated. If a receiving device still displays the deleted losing revision, that exact content is known synchronised content. The resolved winner may replace it without recreating the conflict. If the device has edited that content again, the bytes no longer match the old revision, so the storage-protection guard preserves the new edit.

## Resolution classes

| State                                                                           | Safe automatic action                                                                                  |
| ------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| Both leaves contain identical bytes                                             | Collapse the duplicate leaf without creating merged content.                                           |
| Text or structured data has a shared available base and non-overlapping changes | Perform a conservative three-way merge.                                                                |
| One side deletes content which the other leaves unchanged                       | Preserve the deletion.                                                                                 |
| One side deletes content which the other modifies                               | Retain a manual conflict.                                                                              |
| Both sides insert different content at the same place                           | Object Storage retains both branches for manual resolution; CouchDB keeps its existing additive merge. |
| A receiving file matches any available revision in the tree                     | Apply the propagated database result.                                                                  |
| A receiving file matches no available revision                                  | Preserve it as an unsynchronised conflict.                                                             |
| A body or common ancestor is missing or compacted                               | Retain a manual conflict.                                                                              |
| Content is binary or otherwise not semantically mergeable                       | Leave the selection policy to the host; Commonlib cannot infer user intent.                            |

## Stale and concurrent resolutions

A client can resolve only the leaves which it has observed. If it resolves an older pair while another client has already extended one branch, replication can reveal another live leaf and the document remains conflicted. Two clients can also resolve different leaves concurrently. Neither result is authoritative merely because it has a higher generation or modification time; after the trees meet, every remaining live leaf must be considered again.

This is expected conflict behaviour, not a replication reset. A resolver should process the current leaves repeatedly until one live result remains or user action is required.

A conservative Object Storage merge produces the same result wherever it runs. Its outcome names the revision it was merged on and the latest creation and modification times of the two leaves. The Object Storage host passes that target to `storeContent()`, which stores the merged content as a child of the exact revision through an ordinary, unforced write. PouchDB derives such a revision from its content, so two devices which merge the same leaves create the same revision instead of two merges which conflict with each other. The write is refused when that revision is no longer a live leaf, so a merge never replaces a revision which arrived after it was computed. CouchDB retains its earlier merge write and timestamp behaviour.

Object Storage does not automatically merge two different insertions at the same position. They can be successive states of one edit, such as `I` and `I morning`, which an additive merge would stack as extra lines. CouchDB retains its earlier additive merge.

When two Object Storage devices create the same path independently and the conditional write finds that a remote version already exists, the local creation is stored as a separate root branch. Both versions remain readable conflict revisions; the local file is not made a child of the remote version. CouchDB retains its earlier latest-revision lookup for a creation with no known base.

## More than two live versions

When a document has three or more live leaves, Commonlib compares the current PouchDB winner with one remaining leaf at a time. The remaining candidates are ordered by revision generation ascending, original leaf modification time ascending, then the complete revision ID in code-unit lexical order. A missing or non-finite modification time sorts before a finite time. This order makes the next pair reproducible; it does not make an earlier modification time authoritative.

Each duplicate collapse, conservative merge, or host-directed manual choice is committed to the ordinary revision tree before the next pair is considered. The resolver then reads the current live leaves again instead of retaining a separate accumulator. Completed stages therefore survive a process restart, while a new or externally resolved leaf is considered from the tree which actually exists at the next check.

A host must not apply a dialogue result after either compared revision has ceased to be the current pair. It should discard that stale result, refresh its warning or dialogue state, and queue the path again when conflicts remain.

## File-reflection provenance

The compatibility implementation accepts an injected, device-local `FileReflectionProvenance` capability owned by the database-to-storage composition rather than by a filesystem adapter. Maintained hosts persist:

```text
path -> { revision, observedStorageMtime? }
```

`revision` is the exact database revision which most recently produced the file displayed on that device. `observedStorageMtime` is the raw modification time observed after reflection. It is neither rounded nor compared across devices, and revision identity never comes from a timestamp, path, size, or content hash. Together with the recorded revision and its size, an unchanged modification time proves what storage holds only where reading the file costs too much: for the storage event of this device's own write from one mebibyte, and for an incoming deletion or revision made on the recorded revision from 50 MiB, as described below. Otherwise it is a change hint only.

The record is updated only after a successful database-to-storage reflection or storage-to-database write. A plain read does not change provenance. A record remains authoritative when a user edits the displayed file to bytes which happen to equal another branch; content equality must not silently change the branch being extended.

User-directed reconciliation may select any exact current live revision, including a conflict leaf rather than the deterministic winner. `dbToStorageWithSpecificRev()` rechecks that the selected revision is still live before reflecting its content. `storeFileToDBWithBaseRevision()` performs the same live check before storing current storage content as a child of that revision. If the content already matches the selected non-deleted revision, it records that revision directly without creating a child. A host can set `createIfDifferent` to `false` when it intends only to record an exact content match; differing content then fails instead of changing the revision tree. A host may also create a logical deletion on an explicitly selected branch with `deleteRevisionFromDB()` after performing its own current-live-revision check. This leaves storage unchanged and clears device-local provenance only when that record names the deleted revision.

`storeWithBaseRevision()` is the force-write primitive for deliberately preserving a branch below an exact revision, including a revision which is no longer a leaf. It must not be used when an operation means 'advance this revision only if it is still current'. For that compare-and-store boundary, `storeWithLiveBaseRevision()` uses PouchDB's ordinary revision check and returns `false` if the supplied base has already been advanced. Any current revision-tree leaf is eligible, including a non-winning conflict leaf or a logical-deletion leaf. Without a base revision it only creates the document, and is refused while a live document exists. The caller must refresh its state after refusal rather than silently falling back to the deterministic winner.

With Object Storage, the file handler stores an ordinary storage change this way, on the revision it compared the file with. When another writer advanced that revision meanwhile, it reads the file and the database again and stores once more, so successive stores of one device form a single chain instead of sibling branches. On that second attempt the storage content extends the new winner only when device-local provenance records the winner as the revision storage was stored from or reflected into. Otherwise the winner came from elsewhere, for example from another device whose revision has not been reflected yet, and the storage content is preserved as a conflict on the revision it was made on. A second refusal leaves the file to its next storage event or scan. CouchDB retains its earlier forced-write policy for ordinary storage changes.

The same Object Storage rule applies when such a revision arrived before the store began: a received revision is written into the database before it is reflected into storage, and a storage change can be stored in between. When device-local provenance records a revision which the winner descends from, storage still shows that revision, so the change was made without the winner's change. A change is then preserved as a conflict on the recorded revision instead of being stored over the winner, as the reflection of the winner preserves it when the reflection comes first. For a small file, the handler also checks its bytes before deciding that storage is unchanged despite an equal size and modification time. Storage which still holds the recorded content is not stored; the reflection of the winner replaces it. A forced store, a file without a record, and a winner which was not made on the recorded revision are stored on the winner as before. If provenance cannot be read, the store is deferred rather than extending an unseen winner.

A successful non-deleted reconciliation records the exact selected or newly created revision as provenance. Applying a logical deletion removes the storage item and its provenance record. Deletion provenance is not retained indefinitely: an absent storage item and a current logical-deletion winner already agree and need no further reconciliation.

Hosts may construct the namespaced store handle during service composition, before its backing database is open. Store operations begin only after the host's storage lifecycle is ready. They fail rather than wait when that lifecycle contract is violated, because an implicit readiness wait could hang after failed initialisation or wait on its own initialisation handler. Database reset is a transient unavailable boundary; the host must avoid file processing during it and reconstruct derived provenance after reopening.

When a record is absent, the implementation reconstructs it only if the current storage bytes match exactly one available revision body. No match, or more than one match, leaves the branch identity unknown.

## Operations while a conflict exists

With a proven displayed revision:

- an edit writes a child of that exact revision;
- a deletion writes a logical-deletion child of that exact revision, using the document's `deleted` marker rather than a PouchDB `_deleted` tombstone, so the operation remains a visible live branch until the conflict is resolved;
- a case-only rename writes the new path as a child of the displayed revision in the same document tree; and
- a cross-path rename stores the target first, then writes a logical-deletion child on the displayed source branch.

When edit provenance cannot be reconstructed, the new bytes are preserved as another manual-resolution branch instead of being attached silently to the deterministic winner. A deletion has no remaining file body from which to reconstruct provenance, so an unproven deletion preserves every live branch and requests conflict review. A cross-path rename with an unproven source keeps the newly stored target and preserves every source branch for review.

If chunks needed to reconstruct the winning body are missing, the document metadata and revision tree may still identify the exact winning revision and its parent. In that case, an edited storage file is preserved as a sibling branch from the shared parent without treating the unavailable body as trusted content. A generation-one revision has no parent from which a sibling can be created. If the parent or revision metadata is unavailable, Commonlib refuses the write and requests conflict review rather than inventing a base revision. A host may offer a separately confirmed exact-revision discard: logical deletion needs the revision metadata, not its chunks, and a confirmed replacement can then be stored. This is a recovery decision and must not be performed automatically.

These fallbacks favour recoverability. They may temporarily leave a duplicate target or a still-visible source conflict, but they do not discard a branch whose relationship to the user's operation cannot be proved.

## Worked revision-tree scenarios

### Editing the branch displayed by a host

Assume that the database has this conflict:

```text
A1
├── B1 ── C1     database winner
└── B2 ── C2     displayed in storage
```

The host records `C2` when it reflects that branch into storage. If the user edits the displayed file, Commonlib writes the new revision as a child of `C2`:

```text
A1
├── B1 ── C1
└── B2 ── C2 ── D2     edited content
```

It does not attach the edit to `C1`, even if PouchDB still returns `C1` as its deterministic winner. Replication carries both live leaves, `C1` and `D2`, so a resolver can compare the branches which actually produced the conflict.

### Deleting the displayed branch

With the same starting tree and recorded `C2` provenance, deleting the storage file writes a logical-deletion child of `C2`:

```text
A1
├── B1 ── C1
└── B2 ── C2 ── D2 (deleted: true)
```

`D2` remains a live metadata branch. The host can therefore ask whether to retain `C1` or the deletion. A PouchDB `_deleted` tombstone would remove that decision from the live conflict and is not used for this operation.

### Renaming while a conflict is visible

For a case-only rename, such as `Note.md` to `note.md`, the renamed entry is written as a child of the displayed revision in the same document tree. The other live branch remains available for resolution.

For a cross-path rename, such as `draft.md` to `published.md`, Commonlib stores `published.md` first. It then writes a logical-deletion child of the recorded branch in the conflicted `draft.md` tree. Storing the target first favours recoverability: an interruption can leave a duplicate for review, but it cannot remove the only copy before the target exists.

### Reconstructing provenance after local state is unavailable

Suppose a local-database reset removed the device-local record, but the storage file still has exactly the bytes held by `C2`. If `C2` is the only available revision with those bytes, Commonlib reconstructs `C2` as the displayed base and an edit extends it normally.

If both `C1` and `C2` contain the same bytes, or neither available body matches, content cannot identify the displayed branch. An edit is preserved as another manual-resolution branch. A deletion preserves both existing branches, and a cross-path rename preserves the source branches after storing the target. These outcomes may require user review, but they do not guess that the deterministic winner was displayed.

### Receiving a resolution while showing the losing branch

Device A may resolve the conflict while Device B still displays the bytes from `C2`. When the resolved tree reaches Device B, the all-branch history check recognises those bytes below the deleted losing leaf. Device B can apply the resolution without recreating the conflict. If Device B edited the file after displaying `C2`, its bytes no longer match the historical revision and the overwrite guard preserves that new local edit instead.

### Receiving a deletion or revision of a large file this device recorded

Reading a large file whole to look for unsynchronised changes costs a mobile device as much memory as the file. For a file of at least `LARGE_FILE_BYTES` (50 MiB), the overwrite guard therefore accepts the recorded revision, together with the observed modification time and size, as proof of what storage holds, and does not read the file: when the record was written by reflecting that revision from the database, the revision is in the history of the incoming deletion or revision, and a stat of the file system itself, rather than the stat the host keeps for the file, still shows the recorded modification time and the size of that revision, storage holds nothing which the incoming branch has not seen, and the file is deleted or replaced. The revision identifier decides the relationship; the modification time and size confirm that storage has not changed since the record was made, as they do when a device recognises its own write. A record written while storing storage into the database does not count, because it describes a file the device merely read. Below 50 MiB, and for a file without a usable record, a record of an unfinished publication, of a deletion or of such a store, or another modification time or size, the content comparison runs as before, so a local change is still preserved as a conflict.

### Starting and resetting the provenance store

A host may create the namespaced provenance handle while composing services. It then opens the backing key-value database in its sequential settings lifecycle before enabling scans, storage watchers, or replication. If opening fails, start-up stops; a provenance operation is not held waiting for a readiness state which may never arrive.

During a local-database reset, the store is temporarily unavailable. A racing lookup fails promptly and is treated as unknown provenance, selecting the conservative behaviours above. Once the database has reopened, a later scan can reconstruct uniquely identifiable records from exact revision bodies.

## Unsafe shortcuts

Do not:

- use the first revision whose generation is lower than the other leaf as a supposed common ancestor;
- select a winner from modification time unless the host exposes and the user selects that destructive policy;
- assume the PouchDB winner is the content currently displayed in storage;
- replace a recorded displayed revision merely because current bytes match another branch;
- recreate a conflict from a byte-identical deleted losing revision after a remote resolution;
- discard storage content because history lookup failed;
- infer revision identity from path, size, modification time, or hash without the revision identifier; or
- automatically merge overlapping text changes or unrelated binary contents.

## Verification ownership

Commonlib unit tests build real in-memory PouchDB trees and inject provenance fakes at the file-handler boundary. They cover unequal branch lengths, nearest shared ancestry, deterministic selection from multiple live leaves, reconstruction of a later manual pair after an earlier sensible merge, content retained below a deleted losing leaf, recorded and reconstructed branch identity, ambiguous content, conflict-time editing, missing-body preservation from available parent metadata, refusal to invent a parent for a generation-one revision, exact logical deletion and replacement of an unreadable generation-one live leaf, case-only rename, cross-path rename, a recorded large file replaced or deleted without being read, and safe unproven fallbacks. A maintained host should additionally verify its composition: persistent device-local provenance, real file events, replication of the resulting revision trees, and dialogue policy.
