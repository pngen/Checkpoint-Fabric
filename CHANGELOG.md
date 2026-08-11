# Changelog

All notable changes to Checkpoint Fabric are documented here.
Format: [Keep a Changelog](https://keepachangelog.com/). Versioning: SemVer.

## [Unreleased]

Initial implementation of a vendor-neutral execution-survival runtime for AI
infrastructure.

### Added

- **Workload model:** stable 128-bit identity, workload/checkpoint generations,
  execution epochs, active-node claims, fence tokens, parent/fork generation,
  compatibility descriptors, protection, and single-active policy.
- **Checkpoint model:** typed components, execution frontier, consistency,
  resumability, dependencies, compatibility, locations, lineage, supersession,
  manifest anchors, lifecycle, integrity, restore history, and retirement state.
- **Lifecycle:** strict capture and restore transition graphs with durable, auditable
  state and typed rejection of illegal transitions.
- **Providers:** callback-based application state, filesystem/blob, diagnostic process
  metadata, registered host-memory regions, and opaque custom adapters.
- **Capture and restore:** provider quiescence/capture/verify/resume/cleanup, honest
  consistency/resumability derivation, required-component validation, checkpoint-based
  migration, rollback, fork, and restore-and-hold.
- **Integrity:** canonical format-v1 JSON manifests, coordinator-held SHA-256 anchors,
  checkpoint integrity roots, exact sizes, stored SHA-256 and CRC-32C sidecars, decoded
  content verification, and fail-closed restore/replication/verification.
- **Persistence:** SQLite schema v1 in WAL/`synchronous=FULL` mode for workloads,
  checkpoints, components, policies, nodes, reservations, attempts, lineage, audit, and
  recovery journal; exclusive coordinator data-directory lease and monotonic epochs.
- **Storage:** local filesystem staging and atomic directory promotion, committed
  enumeration, safe relative paths, stale-staging recovery, and retry-safe deletion.
- **Recovery:** deterministic reconciliation of capture/restore journal boundaries,
  stale nodes, expired reservations, source resume, provider rollback, and committed
  restore finalization.
- **Transport:** bounded `CFAB` framed TCP with typed operations, request IDs, CRC-32C,
  timeouts, malformed-frame rejection, bounded connections, and graceful shutdown.
- **Policy and compatibility:** versioned role policy, consistency/replica/retention/
  compression limits, deterministic target verdicts, and sibling-runtime descriptors.
- **CLI and examples:** coordinator/node processes; workload/checkpoint management;
  capture, restore, rollback, fork, migration, compatibility, audit, recovery, stats;
  and twelve runnable examples.
- **Validation:** unit/integration closure of 148 passed tests, 12/12 examples, 15
  property/security tests, 5 failure-injection tests, and exactly three multiprocess
  runs totaling 24/24 passed, with zero warnings and zero failures.

### Fixed during hardening

- Capture now persists the complete component/location/size/manifest image before the
  `persisted` journal boundary and atomically commits availability, reciprocal
  supersession, lineage, workload counters, and recovery marker.
- Recovery refuses incomplete persisted scaffolds instead of manufacturing an unusable
  available checkpoint.
- Canonical manifest JSON and digest remain anchored in coordinator metadata; restore,
  verify, and replication require that anchor and fail closed on mismatch.
- Restore now enters `RESTORING`, separately enforces rollback/migration authorization,
  rejects contradictory options, and atomically commits generation, execution authority,
  checkpoint state, lineage, and recovery marker.
- Filesystem restore gained a durable provider-restart-safe rollback journal that
  restores overwritten data, removes only attempt-created paths, and preserves unrelated
  files.
- Node boot identities are validated, coordinator epochs are exclusively and
  monotonically owned, fence claims require exact epochs, and committed replicas rebind
  after node restart.
- Component, restore, and cleanup paths reject traversal/symlink escapes. Live staging
  leases and TTL aging eliminate the heartbeat cleanup race.
- Fork creation and lineage are one transaction, eliminating duplicate/orphan workload
  side effects.
- Retirement checks structured node errors and completes physical deletion before
  clearing durable locations, leaving failures retryable.
- RPC clients no longer replay ambiguous failed mutations; reconnection applies only to
  the next caller-issued operation.
- Replication requires safe paths, anchored manifests, exact offsets and sizes, positive
  progress, 1 MiB chunks, and final hashes before promotion.
- Multiprocess example children are owned by panic-safe cleanup guards and are killed and
  reaped during unwinding.

### Security

- Frames, IDs, codecs, component names, manifests, byte counts, compatibility inputs,
  restore paths, and cleanup targets are validated.
- The current TCP protocol remains plaintext and unauthenticated; roles are
  caller-asserted. Deploy only on loopback or a protected network.
- Integrity hashes are unkeyed corruption checks and storage is not encrypted.

