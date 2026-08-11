# Checkpoint Fabric

**Checkpoint Fabric is a vendor-neutral execution-survival runtime for AI
infrastructure.**

It answers the question:

> **What execution state must survive?**

Checkpoint Fabric turns application- and runtime-supplied state into coherent,
sealed checkpoint objects with explicit consistency, resumability, compatibility,
integrity, lineage, authority, and lifecycle semantics. It coordinates capture,
durable persistence, verification, restore, migration, rollback, fork, replication,
retirement, and crash recovery without binding the core to a model framework,
accelerator vendor, orchestrator, or external storage product.

The runtime is a distinct authority domain with its own coordinator, node processes,
provider interface, SQLite metadata store, local durable backend, framed TCP protocol,
CLI, examples, tests, and recovery journal. The core runs independently of the four
surrounding Summon runtimes.

## The stack

| Runtime | Question it answers | Responsibility |
|---|---|---|
| **FlashTier** | Where do the bytes live? | physical byte residency |
| **Context Fabric** | Where does accumulated reusable computation live? | reusable computational-state residency |
| **Compute Fabric** | Where should the next computation run? | execution placement |
| **Reclaim Fabric** | What state is still worth keeping? | state lifecycle and reclamation |
| **Checkpoint Fabric** | **What execution state must survive?** | **coherent execution capture and continuation** |

Checkpoint Fabric exposes narrow descriptors for the sibling boundaries. It does not
contain their source code and does not require them to build or run.

## What Checkpoint Fabric is not

- **Not a transparent process freezer or VM snapshotter.** State enters through an
  explicit provider. The shipped process provider records redacted metadata; it does
  not capture an arbitrary native process image.
- **Not an accelerator checkpoint implementation.** Accelerator and runtime component
  classes can be described, but 1.0.0 does not pretend to capture unsupported device,
  driver, queue, or kernel state.
- **Not a compute scheduler.** The caller selects a restore or migration target;
  Checkpoint Fabric validates that target and transfers execution authority.
- **Not a distributed object store.** The shipped durable backend is a local filesystem
  with atomic promotion. `StorageBackend` is an extension boundary, not evidence that
  object-store or distributed-filesystem backends are implemented.
- **Not an exactly-once side-effect service.** Execution frontiers and side-effect
  manifests make replay risk explicit. External systems still own their transaction
  and idempotency semantics.
- **Not a consensus system or network security layer.** The coordinator is a single
  durable authority, and the v1 TCP control plane is plaintext and unauthenticated.

## Core capabilities

1. Model a stable workload across process, node, backend, and execution-generation
   changes; process IDs are never workload identity.
2. Capture typed state components through `CaptureProvider`, with required-component
   completeness, cooperative quiescence, explicit execution frontiers, and honest
   `CRASH_CONSISTENT`, `APPLICATION_CONSISTENT`, or `EXECUTION_CONSISTENT` results.
3. Derive resumability without overclaiming: `NON_RESUMABLE`,
   `RESTART_FROM_CHECKPOINT`, `DEGRADED`, `EQUIVALENT`, or `EXACT`.
4. Seal a canonical format-v1 manifest whose SHA-256 digest is retained as the
   coordinator's durable anchor. Component stored bytes carry SHA-256 and CRC-32C
   sidecars; decoded content and the checkpoint integrity root are verified.
5. Persist checkpoint metadata, components, locations, generations, lineage,
   supersession, reservations, attempts, policy versions, audit records, and recovery
   journal entries in SQLite WAL mode with `synchronous=FULL`.
6. Commit capture metadata atomically after physical promotion. Recovery can complete a
   fully described persisted capture idempotently, but rejects incomplete metadata
   instead of fabricating an available checkpoint.
7. Restore only through an anchored manifest and verified component set. Provider
   changes remain rollback-capable until checkpoint lifecycle, workload generation,
   execution epoch, authority claim, lineage, and journal state commit atomically.
8. Perform checkpoint-based migration, explicit rollback to a new execution generation,
   and atomic fork creation with durable lineage and no intermediate orphan workload.
9. Fence single-active continuations with coordinator epochs, workload execution epochs,
   random fence tokens, node boot identities, heartbeat validation, and stale-node
   revocation. These are authority controls, not authentication credentials.
10. Rebind committed local replicas after a node restart by reconciling the node's
    advertised on-disk inventory with durable checkpoint locations.
11. Replicate in 1 MiB chunks under a time-limited fetch token. The receiver checks the
    coordinator manifest anchor, component identity and path, declared byte count,
    forward progress, and final hashes before atomic promotion.
12. Bound framed messages to 16 MiB, coordinator connections to 64 by default, node
    connections to 16 by default, transport I/O with timeouts, decompression with an
    output cap, and replication by manifest-declared sizes. Ambiguous failed RPCs are
    returned to the caller and are never replayed automatically.
13. Contain cleanup to structured attempt/checkpoint identifiers or a canonical direct
    child of the staging root. Active staging leases cannot be removed by the periodic
    TTL sweep.
14. Retire safely: physical replica deletion must succeed before metadata locations are
    cleared, so interrupted retirement remains discoverable and retryable.

## Architecture at a glance

```text
 CLI / library client
        |
        | CFAB framed TCP (request id, length, CRC-32C)
        v
+----------------------------+
| Coordinator                |
| epoch + policy authority   |
| SQLite + recovery journal  |
+-------------+--------------+
              | epoch-checked node RPC
       +------+--------------------+
       |                           |
+------v-------+             +-----v--------+
| Node A       | 1 MiB fetch | Node B       |
| providers    +------------>+ providers    |
| local store  |             | local store  |
+--------------+             +--------------+
```

Capture follows:

```text
CREATED -> CAPTURING -> CAPTURED -> VALIDATING -> SEALED
        -> PERSISTING -> AVAILABLE
```

Restore follows:

```text
AVAILABLE -> RESTORE_PENDING -> RESTORING -> RESTORED
RESTORING -- pre-commit failure + provider rollback --> AVAILABLE
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for transaction boundaries, recovery
decisions, persistent layout, authority rules, and current limits.

## Quick start

Requirements: Rust 1.81 or newer and a C toolchain for bundled SQLite.

```text
# Build the library and checkpointfabric binary
cargo build --release

# Terminal 1: start the single durable coordinator
cargo run --release -- coordinator start \
  --data-dir ./data/coordinator --listen 127.0.0.1:7901

# Terminal 2: start a node
cargo run --release -- node start --name node-a \
  --data-dir ./data/node-a --coordinator 127.0.0.1:7901

# Terminal 3: discover the concrete name@pid@boot node identity
cargo run --release -- --coordinator 127.0.0.1:7901 nodes

# Create and checkpoint a workload on that node
cargo run --release -- --coordinator 127.0.0.1:7901 \
  workload create --owner demo --node <NODE_ID>
cargo run --release -- --coordinator 127.0.0.1:7901 capture <WORKLOAD_ID>
cargo run --release -- --coordinator 127.0.0.1:7901 \
  checkpoint verify <CHECKPOINT_ID>
cargo run --release -- --coordinator 127.0.0.1:7901 \
  restore <CHECKPOINT_ID> <NODE_ID>
```

The standalone node process has no application-specific providers by default. Use the
Rust provider API to attach real application, filesystem, process-metadata, registered
memory-region, or custom state. A capture with no requested components is valid but does
not imply native process-state capture. Add global `--json` for structured CLI output.

## Shipped providers

| Provider | Current behavior |
|---|---|
| `ApplicationStateProvider` | caller-supplied snapshot/apply callbacks with optional quiesce, resume, and cleanup hooks |
| `FilesystemProvider` | files/directories serialized as an application payload; confined restore with a durable rollback journal |
| `ProcessMetadataProvider` | identity and diagnostics with environment-value redaction/allowlisting; no native process image |
| `MemoryRegionProvider` | caller-registered named host byte regions; no arbitrary address-space capture |
| `CustomProvider` | caller-owned capture, restore, verification, and cleanup callbacks behind the same contract |

Unknown providers, codecs, required restore handlers, incompatible schemas, unsafe
component paths, and unsupported capture semantics are rejected rather than substituted.

## Examples

Twelve runnable examples in `examples/` exercise the implementation end to end:

1. `01_basic` — register, capture, seal, and inspect one application component.
2. `02_application_consistent` — cooperative quiescence and an explicit frontier.
3. `03_verification` — verify a pristine checkpoint and reject tampering.
4. `04_restore_after_loss` — restore application state after simulated process loss.
5. `05_lineage` — consecutive generations and reciprocal supersession links.
6. `06_fork` — atomically create an independent descendant workload.
7. `07_rollback` — restore an older checkpoint into a new execution generation.
8. `08_migration` — stream a replica, restore a target, and transfer authority.
9. `09_compatibility` — structured incompatibility rejection and compatible restore.
10. `10_corruption` — fail closed on corrupted component bytes.
11. `11_recovery` — reconcile persisted and pre-durable crash points.
12. `12_multiprocess` — real coordinator/node child processes driven by the CLI.

```text
cargo run --example 01_basic
cargo run --example 12_multiprocess
```

Example 12 owns its children through panic-safe guards so an assertion failure cannot
silently leave the coordinator or node running.

## CLI surface

```text
checkpointfabric coordinator start|stop
checkpointfabric node start
checkpointfabric workload create|inspect|list|fence|lineage
checkpointfabric capture|capture-status
checkpointfabric checkpoint inspect|list|verify|protect|pin|unprotect|retire|lineage
checkpointfabric restore|rollback|fork|migrate|compatibility
checkpointfabric audit|recovery|stats|nodes
```

`src/lib.rs` exposes the same coordinator, object model, policy, provider, storage,
compatibility, manifest, lifecycle, and protocol modules for embedding in Rust code.

## Validation evidence

The final closure state for the current implementation is:

| Validation surface | Result |
|---|---:|
| Full test suite | **148 passed** |
| Runnable examples | **12/12 passed** |
| Property and security tests | **15 passed** |
| Failure-injection tests | **5 passed** |
| Multiprocess validation | **exactly 3 runs, 24/24 passed** |
| Compiler/linter warnings | **0** |
| Failures | **0** |

The category rows describe the same closure campaign and are not intended to be summed.
Multiprocess validation used real OS processes and framed TCP on one physical machine.

## Development

```text
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
cargo test --test property --test security
cargo test --test failure_injection
cargo test --test multiprocess   # run exactly three times for the closure gate
cargo build --release --all-features
```

Run all examples explicitly when changing a public operation or lifecycle path. The
current `fabric` benchmark target is only a compileable placeholder and contains no
registered performance cases; no throughput or latency claim is made here.

## Documentation

- [ARCHITECTURE.md](ARCHITECTURE.md) — system model, transactions, recovery, and limits.
- [SECURITY.md](SECURITY.md) — trust boundaries, fail-closed behavior, and reporting.
- [CONTRIBUTING.md](CONTRIBUTING.md) — validation and hardening expectations.
- [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) — community participation standards.
- [CHANGELOG.md](CHANGELOG.md) — implemented surface and final hardening history.

## Current limitations

- The coordinator is a single durable authority protected by an exclusive local
  data-directory lease; it is not a replicated consensus service.
- Network transport has integrity framing and authority epochs but no peer
  authentication, authorization identity proof, or encryption. Use loopback or a
  separately protected network.
- The only shipped durable backend is local filesystem storage. Multi-host validation,
  object storage, distributed filesystems, RDMA, and accelerator-native capture are not
  present in the closed implementation.
- The object model represents incremental and delta checkpoints and validates their
  ancestry, but the built-in capture flow does not produce incremental/delta payloads.
- On Windows, child-file and parent-directory `fsync` steps around promotion are no-ops;
  writes themselves are flushed through write handles before rename.
- Integrity hashes are unkeyed corruption checks, not signatures or protection against a
  malicious writer with access to checkpoint files or the SQLite store.

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
