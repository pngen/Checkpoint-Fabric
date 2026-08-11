# Architecture

## 1. Positioning and boundary

Checkpoint Fabric answers one systems question:

> **What execution state must survive?**

Its authority begins when a workload declares checkpointable state through providers
and ends when a coherent checkpoint is durably committed, verified, restored, migrated,
forked, rolled back, or retired. The fabric owns workload/checkpoint identity,
generations, execution frontiers, consistency and resumability claims, compatibility,
manifest integrity, durable locations, lifecycle, lineage, reservations, fencing,
recovery, and audit.

The fabric does not discover arbitrary process or accelerator state, schedule compute,
provide distributed object storage, decide economic retention, or secure an untrusted
network. Those responsibilities remain with providers, orchestrators, storage systems,
Reclaim Fabric, and deployment infrastructure respectively.

Interfaces in `integrations.rs` expose pure descriptors for FlashTier, Context Fabric,
Compute Fabric, and Reclaim Fabric. The core has no build or runtime dependency on any
of them.

## 2. Design principles

1. **State is explicit.** A component exists only when a provider describes and
   captures it.
2. **Claims follow evidence.** Missing quiescence, frontier, replay, compatibility, or
   integrity evidence lowers the result or rejects the operation.
3. **Authority is singular.** One coordinator data directory has one process owner and
   one monotonically advancing coordinator epoch.
4. **Physical truth precedes metadata success.** A checkpoint becomes `AVAILABLE` only
   after atomic local promotion and a complete recoverable metadata image.
5. **Commit sets are atomic.** Related lifecycle, generation, authority, lineage, and
   journal changes become visible in one SQLite transaction.
6. **Ambiguity is not success.** Recovery completes only states whose durable evidence
   is sufficient; incomplete states fail and are cleaned.
7. **Integrity is anchored.** A node cannot redefine the expected manifest during
   verify, restore, or replication.
8. **Cleanup is contained.** Attempts own staging leases and cleanup targets are
   structural, not arbitrary paths.
9. **Unsupported means unsupported.** A type or extension interface does not imply a
   shipped implementation.

## 3. Process architecture

```text
                       durable metadata authority
                   +-------------------------------+
client / CLI ------> Coordinator                    |
  framed TCP        | epoch, policy, reservations  |
                    | SQLite WAL, journal, audit   |
                    +---------------+---------------+
                                    |
                         epoch-checked node RPC
                    +---------------+---------------+
                    |                               |
             +------v------+                 +------v------+
             | Node A      |                 | Node B      |
             | providers   |<--- fetch ----->| providers   |
             | local store |    1 MiB chunks | local store |
             +-------------+                 +-------------+
```

The CLI is only an RPC client. Nodes host provider instances and physical checkpoint
directories. The coordinator owns authoritative metadata and calls nodes for physical
work. Cross-process communication uses the same framed TCP implementation in examples,
tests, and the standalone binary.

## 4. First-class model

### 4.1 Workload

A workload is stable logical execution, not a PID. Its durable record includes:

| Field | Meaning |
|---|---|
| `workload_id` | stable random or caller-supplied 128-bit identity |
| `workload_generation` | continuation generation; restore/rollback advances it |
| `checkpoint_generation` | latest committed checkpoint generation |
| `execution_epoch` | monotonic fencing generation |
| `active_node` | node currently holding execution authority |
| `fence_token`, `fence_epoch` | continuation claim bound to the current epoch |
| `parent_workload`, `fork_generation` | fork ancestry |
| runtime/schema/backend descriptors | compatibility input |
| `single_active` | whether concurrent active continuations are forbidden |

### 4.2 Checkpoint

`CheckpointObject` is the authoritative metadata object. It records identity and both
generations; capture attempt and coordinator epoch; source; execution frontier;
checkpoint, consistency, and resumability classes; typed components and sizes; durable
locations and replica count; parent, supersession, and lineage; runtime/hardware
requirements and external dependencies; policy version; integrity and lifecycle;
protection/retirement state; restore history; metadata; and the canonical manifest JSON
and digest.

`IntegrityState` is `Pending`, `Valid`, `Corrupt`, or `Unverifiable`. The model's
restorable classification requires lifecycle `AVAILABLE` or `RESTORED` and integrity
`Valid`. Every concrete restore additionally revalidates the coordinator-held anchor and
the physical replica before applying state.

### 4.3 Components

Each component has a stable ID, typed class, generation, required flag, logical size,
codec/storage representation, stored and decoded hashes, schema version, restore
handler, compatibility data, dependencies, and capture/restore status.

The type vocabulary includes application, filesystem, process metadata, registered
memory, accelerator, runtime, queue, scheduler, RNG, resources, tools, model, KV, and
custom state. This is a description vocabulary. Only the providers listed in section 8
are shipped.

### 4.4 Execution frontier and side effects

`ExecutionFrontier` identifies the logical step, workload generation, execution epoch,
application sequence, durable side-effect boundary, state-domain generations, tool and
external commit frontiers, last completed operation, in-flight operations, and replay
flags. `SideEffectManifest` classifies external effects as committed, replayable,
non-replayable, unknown, or uncommitted.

An unresolved unknown or non-replayable effect prevents an `EXACT` claim. Checkpoint
Fabric records and reasons about this boundary; it does not transact with the external
system.

## 5. Consistency and resumability

Consistency is evidence-based:

| Class | Required evidence |
|---|---|
| `CRASH_CONSISTENT` | state is no stronger than interruption recovery |
| `APPLICATION_CONSISTENT` | all selected providers cooperatively acknowledge quiescence |
| `EXECUTION_CONSISTENT` | cooperative acknowledgement plus an explicit frontier |

Forced or absent quiescence cannot substantiate application consistency. A requested
stronger class is downgraded when evidence is absent; policy may reject the downgraded
result.

Resumability is ordered from `NON_RESUMABLE` through `RESTART_FROM_CHECKPOINT`,
`DEGRADED`, `EQUIVALENT`, and `EXACT`. Exact requires execution consistency, an exact
frontier, verified deterministic replay, and no unresolved side-effect boundary.
Compatibility translation, backend changes, missing optional handlers, and a
restore-without-resume can lower the achieved class.

## 6. Lifecycle

The capture happy path is:

```text
CREATED -> CAPTURING -> CAPTURED -> VALIDATING
        -> SEALED -> PERSISTING -> AVAILABLE
```

The restore path is:

```text
AVAILABLE/RESTORED -> RESTORE_PENDING -> RESTORING -> RESTORED
RESTORING -- failed before commit --> AVAILABLE
```

`RETIRED` and `FAILED` are terminal for ordinary operation. Archive restore is represented
in policy, but the ordinary coordinator validation refuses retired checkpoints in the
current flow. Illegal transitions return typed errors. Persistence helpers that finalize
capture and restore validate the expected source lifecycle inside their transaction.

## 7. Capture protocol and atomic commit

1. Authorize `capture`; load the workload; validate interval, frontier generation,
   component IDs, side effects, and active authority.
2. Reserve the workload, create an attempt, and journal `reserved`.
3. Insert the checkpoint scaffold and enter `CAPTURING`.
4. The node prepares and quiesces selected providers, captures each payload, writes
   compressed stored bytes plus integrity sidecars, and verifies provider output.
5. The coordinator checks required-component completeness, derives achieved consistency
   and resumability, and records sizes.
6. Enter `CAPTURED` then `VALIDATING`; create the canonical manifest, integrity root, and
   SHA-256 digest; persist the coordinator's manifest anchor; enter `SEALED`.
7. Enter `PERSISTING`; the node writes manifest sidecars and atomically renames the
   staging directory to `checkpoints/<checkpoint-id>`.
8. Persist the complete checkpoint image: components, exact location, sizes, manifest
   JSON/digest, policy, and derived classifications. Only then journal `persisted`.
9. One SQLite transaction validates that image and commits `AVAILABLE`/`Valid`,
   reciprocal supersession, lineage, workload checkpoint counter and resumability, and
   journal `db_committed`.
10. Resume the source, journal `resume_done`, attempt policy replication, release the
    reservation, and finish the attempt.

A source-resume failure after step 9 does not erase a committed checkpoint. It is
reported as `RESUME_FAILED`. Failures before the metadata commit resume/abort providers,
remove staging and any uncommitted promoted object, mark the attempt failed, and release
the reservation.

## 8. Provider boundary

`CaptureProvider` owns the actual state and implements prepare, quiesce, capture,
capture verification, source resume/abort, restore, restore verification, cleanup, and
restore commit.

Shipped providers:

- `ApplicationStateProvider`: caller callbacks with optional quiesce/resume/cleanup.
- `FilesystemProvider`: file/directory payloads and confined, journaled restore.
- `ProcessMetadataProvider`: redacted diagnostics; restore is intentionally a no-op.
- `MemoryRegionProvider`: caller-registered named byte regions.
- `CustomProvider`: caller-owned callbacks behind the same contract.

There is no arbitrary process-image, open-kernel-resource, or accelerator-state provider.
A standalone node therefore captures only components attached by an embedding
application; an empty requested set is not a native process checkpoint.

### Filesystem restore rollback

Before mutation, the filesystem provider writes a per-attempt durable journal below the
configured restore root. It records previous bytes for overwritten files and which files
and directories are newly created. It rejects absolute/traversal paths, duplicate
destinations, the reserved journal directory, symlink roots, symlink ancestors, and
symlink destinations.

If provider application or verification fails, cleanup runs in reverse provider order.
If the process restarts before generation commit, the durable journal lets a new provider
instance restore touched files and remove only paths it created; unrelated files remain.
The journal is discarded only after the coordinator's restore commit and authority grant.

## 9. Restore, rollback, migration, and fork

### 9.1 Restore

1. Authorize restore and any rollback/migration modifier separately; the two modifiers
   are mutually exclusive.
2. Require an active target, sufficient replicas, valid lifecycle, resolved required
   context references, a legal single-active claim, and a non-incompatible target.
3. Reserve the checkpoint and enter `RESTORE_PENDING`.
4. Replicate to the target if it lacks a local copy, then enter `RESTORING`.
5. Require the coordinator manifest digest. The node validates the local manifest,
   digest sidecar, manifest identities, integrity root, component paths, sizes, CRC-32C,
   stored SHA-256, decoded size, and decoded SHA-256 before applying providers in order.
6. Journal `components_restored`.
7. One transaction advances workload generation and execution epoch, installs the target
   node/fence token, updates checkpoint lifecycle/count/result, appends rollback or
   migration lineage, and journals `generation_committed`.
8. `NodeResume` installs the authority grant even when the caller requested
   restore-and-hold. Provider commit hooks discard rollback journals; optional execution
   resume follows.

A pre-commit failure rolls provider state back and returns the checkpoint to `AVAILABLE`.
A failure after step 7 is `COMMITTED_NO_RESUME`; committed generation and authority are
not falsely rolled back.

### 9.2 Rollback and migration

Rollback uses restore but always creates a new workload generation and a `ROLLBACK_OF`
lineage record. It does not rewrite history.

Migration is checkpoint-based. A missing target replica is fetched and verified, restore
commits target authority with a new token/epoch, and `MIGRATED_FROM` is recorded. The old
node's heartbeat no longer matches and its local attachment is revoked. The target must
already be registered and expose compatible providers; the fabric does not provision it.

### 9.3 Fork

Fork creates an independent workload whose parent is the checkpoint's workload and whose
fork generation advances. Child insertion and `FORKED_FROM` lineage occur in one
transaction. The parent checkpoint is not mutated, and a conflict cannot leave a hidden
ordinary workload behind.

## 10. Compatibility

Compatibility is a pure deterministic evaluation over operating system, architecture,
backend, state schema, workload class, required accelerator capabilities, format version,
restore-handler versions, and policy. It returns `COMPATIBLE`,
`COMPATIBLE_WITH_TRANSLATION`, `COMPATIBLE_DEGRADED`, or `INCOMPATIBLE` with reasons.

A translation verdict classifies what would be required; the repository does not ship a
general translation engine. A required provider missing on the target is incompatible.
Cross-OS/cross-architecture and schema behavior follow explicit policy, while a backend
change is degraded.

## 11. Integrity and persistent layout

The local node layout is:

```text
<node-data>/
  staging/<attempt-id>/
  checkpoints/<checkpoint-id>/
    manifest
    manifest.digest
    components/<component-id>
    components/<component-id>.sha256
    components/<component-id>.crc32c
    integrity/root
    integrity/<component-id>.sha256
```

The format-v1 manifest is fixed-field-order, compact JSON. Its integrity root is:

```text
SHA-256(component content hashes || canonical manifest with empty root)
```

The final manifest digest is SHA-256 over canonical bytes including that root. The
coordinator retains both canonical JSON and digest. Restore, verification, and
replication require physical manifest bytes and `manifest.digest` to equal this anchor.
CRC-32C detects common stored-byte corruption quickly; SHA-256 and exact byte counts
provide full unkeyed integrity checks. These are not signatures or malicious-writer
authentication.

Local promotion is an atomic directory rename after write-handle flushes. Unix also
recursively syncs files and syncs the parent directory. Those read/directory-handle sync
steps are no-ops on Windows; this is a documented platform durability limit.

## 12. Metadata persistence and coordinator authority

The schema-v1 SQLite store contains policies, workloads, checkpoints, components,
lineage, attempts, nodes, reservations, audit, and recovery journal tables. It uses WAL
and `synchronous=FULL`; a newer schema version is refused.

Opening the store obtains an exclusive `coordinator.lock`. A second process cannot use
the same data directory. The coordinator epoch is claimed transactionally: an automatic
start uses stored epoch + 1, and an explicit epoch must strictly exceed the stored value.
This is single-writer failover fencing, not consensus or distributed leader election.

Capture and restore reservations are uniquely scoped and expire. Checkpoint identity and
`(workload_id, checkpoint_generation)` are unique; conflicting inserts fail instead of
silently appending components to another object.

## 13. Crash recovery

Recovery reconciles active attempts from durable journal state:

| Durable evidence | Recovery decision |
|---|---|
| capture before `persisted` | fail attempt, release reservation, resume/clean source |
| capture `persisted`, no `db_committed`, complete anchored metadata | rerun atomic capture commit idempotently |
| capture `persisted`, incomplete location/anchor/metadata | mark failed/corrupt and delete incomplete physical object |
| restore before component application | fail attempt and clean staging |
| restore `components_restored`, no `generation_committed` | provider rollback, return checkpoint to `AVAILABLE`, fail attempt |
| restore `generation_committed`, no `resumed` | preserve commit, install/finalize target authority, report resume unverified |
| committed attempt | confirm committed and release reservation |

Recovery also expires reservations and marks stale nodes. It never reconstructs missing
components, byte counts, locations, or lineage from an empty scaffold.

## 14. Nodes, restart, and fencing

A node identity is `name@pid@boot-id`, with a fresh random boot ID per start. Registration
and every heartbeat carry the boot ID and coordinator epoch. On startup the node removes
abandoned staging and enumerates structurally committed checkpoint directories. The
coordinator rebinds matching durable locations from the old transient node identity to
the new one.

Heartbeats report attached workloads with execution epoch/fence token and provider
versions. A mismatch causes coordinator-side release and node-side detachment. A stale
node causes the workload execution epoch to advance and authority to clear. Ten
consecutive heartbeat failures stop the node.

Epochs, boot IDs, fence tokens, and roles protect runtime ordering inside the trusted
deployment. Because TCP peers are not authenticated, none is a network credential.

## 15. Transport and at-most-once behavior

The 24-byte `CFAB` header contains protocol version, typed operation, flags, request ID,
payload length, and CRC-32C. Payloads are JSON and capped at 16 MiB. Receive buffers are
bounded; malformed, truncated, oversized, unknown-operation, version, and checksum
failures close the connection. Read/write timeout is 30 seconds. The coordinator accepts
64 concurrent connections by default; a node accepts 16.

Once a request frame has been written, losing the response is ambiguous. `RpcClient`
returns the original error and reconnects only for the caller's next operation. It never
automatically replays the failed call. This is at-most-once client behavior, not global
server-side request deduplication.

## 16. Replication bounds

The coordinator grants the source a checkpoint-scoped fetch token with a five-minute
TTL. The target fetches the manifest and then each component in 1 MiB chunks. It requires
the coordinator anchor, identical manifest/component lists, safe exact paths, expected
offsets, nonzero progress until completion, no overshoot beyond declared size, exact
final size, and final SHA-256 before promotion.

Capture replication is best effort toward `min_valid_replicas`; restore later enforces
the policy minimum. Replication is bounded per chunk and by manifest-declared component
size, but there is no tenant-wide storage or request quota.

## 17. Cleanup, retirement, and shutdown

Active capture and replication staging paths hold in-memory leases. Every ten heartbeats,
the node removes only unleased staging directories older than the configured TTL (one
hour by default). Structured attempt IDs derive a direct staging child. Legacy explicit
paths must be one direct child lexically and, when present, canonically.

Retirement refuses protected checkpoints and active restore reservations. It asks every
recorded node to delete the checkpoint and treats both RPC failure and structured node
error as failure. Only after all deletions succeed does SQLite mark `RETIRED`, clear
locations, and zero the replica count. A partial failure retains metadata for retry.

Coordinator and node shutdown stop accept/heartbeat loops and join workers within
bounded transport timeouts. The multiprocess example owns children with `Drop` guards
that kill and reap on panic as well as on normal cleanup.

## 18. Source map

| Module | Responsibility |
|---|---|
| `checkpoint`, `workload`, `frontier`, `sideeffect` | durable object and continuation model |
| `capture`, `restore`, `migration`, `lineage`, `lifecycle` | pure operation semantics |
| `manifest`, `integrity`, `compression`, `compatibility` | format, verification, codec, target checks |
| `providers`, `storage`, `node` | physical state, local durability, node execution |
| `coordinator`, `persistence`, `recovery`, `policy`, `audit` | authority, transactions, reconciliation, governance |
| `protocol`, `transport` | bounded multiprocess wire behavior |
| `cli`, `cli_impl`, `main` | thin command-line client and process entry points |
| `integrations` | dependency-free sibling descriptors |

## 19. Current limits

- Single coordinator authority; no replicated consensus or automatic HA.
- Plaintext, unauthenticated TCP and caller-asserted roles; trusted network only.
- No at-rest encryption or integrated secret manager.
- Local filesystem is the only shipped durable storage backend.
- No native arbitrary-process, open-resource, or accelerator snapshot provider.
- Target provisioning/scheduling and general compatibility translation are external.
- Incremental/delta types and ancestry validation exist, but built-in capture emits only
  `FULL`, `APPLICATION`, or `PORTABLE` payloads.
- Multiprocess closure validation used one physical machine; no multi-host claim.
- No tenant quotas or published performance claims.
