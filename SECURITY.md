# Security

## Trust boundaries

```text
client / CLI  -- plaintext framed TCP -->  coordinator  -- SQLite + lock
                                               |
                                      plaintext framed TCP
                                               v
                                             node  -- providers + local files
```

| Boundary | Current trust model |
|---|---|
| Client → coordinator | Untrusted framing/input, but unauthenticated peer. Roles and actor names are caller-asserted. |
| Coordinator → node | Epoch-checked input. Coordinator epochs are ordering authority, not credentials. |
| Node → coordinator | Boot ID, node ID, heartbeat token, and epoch are validated but do not cryptographically authenticate the process. |
| Node → node replication | Expiring checkpoint-scoped fetch token over plaintext TCP; token is a capability, not encrypted identity. |
| SQLite and checkpoint files | Trusted local storage. Integrity detects accidental/tampered bytes unless an attacker can rewrite both authority metadata and files consistently. |

The v1 network protocol has no TLS or peer authentication. Run it only on loopback or a
separately authenticated, protected network. Any reachable client can assert an actor and
roles; policy authorization is meaningful only after deployment infrastructure controls
network access. Fence tokens, boot IDs, epochs, and fetch tokens are visible on the wire.

## Authority and fencing

Default role policy permits owners to capture and fork; owners or operators to restore;
operators to retire, migrate, fence, and roll back; and operators or auditors to verify.
Operator/root roles override operation lists. Raw restore requests cannot turn on rollback
or migration without the corresponding additional authorization, and rollback plus
migration is rejected.

One process holds an exclusive lock on the coordinator data directory. Coordinator epochs
advance transactionally and cannot be reused or regressed. Workload claims require the
exact execution epoch, current active node, current fence token, and registered node boot
identity. Stale-node detection advances the workload epoch and revokes authority.

These mechanisms prevent stale actors inside the trusted runtime from committing. They do
not provide cryptographic identity against a hostile network peer.

## Input validation

- Frames require `CFAB` magic, protocol version 1, a known typed operation, a matching
  request ID, a payload no larger than 16 MiB, and CRC-32C. Invalid frames close the
  connection.
- IDs and generations are parsed strictly. Component IDs are 1–256 characters, unique,
  and must already equal their safe filesystem representation.
- Unknown compression codecs are rejected. Decompression is capped (1 GiB by default
  when no smaller policy limit is configured).
- Restore compatibility checks OS, architecture, backend, schema, workload class,
  accelerator requirements, format, and provider handlers.
- `safe_join` rejects absolute, rooted, and parent-traversal paths. Filesystem restore
  also rejects duplicate destinations, reserved journal paths, and symlink roots,
  ancestors, and destinations.
- Cleanup normally uses attempt/checkpoint identifiers. A legacy explicit staging path
  must be exactly one direct child of the staging root and must remain there after
  canonicalization.

## Integrity and fail-closed behavior

- The coordinator stores the canonical manifest and its SHA-256 digest as the authority
  anchor.
- The manifest integrity root covers component content hashes and canonical metadata.
- Stored components have exact sizes, SHA-256 hashes, and CRC-32C sidecars; decoded
  content is checked again with SHA-256.
- Verify, restore, and replication require both local manifest sidecar agreement and a
  match with the coordinator anchor.
- A missing anchor, malformed manifest, unsafe path, missing required handler, byte-count
  mismatch, corruption, incompatible target, or insufficient replica count rejects the
  operation.
- Recovery re-commits a persisted capture only when its complete anchored metadata image
  exists. It marks incomplete state failed instead of constructing success.
- Restore provider mutations remain rollback-capable until the generation/authority
  transaction commits. Filesystem rollback journals survive provider restart and restore
  only paths touched by that attempt.
- Retirement keeps locations discoverable until every physical delete succeeds.

SHA-256 and CRC-32C are unkeyed integrity checks, not signatures. There is no encryption
at rest. A malicious local writer able to change SQLite and checkpoint files together is
outside the current integrity guarantee.

## Resource and denial-of-service bounds

- 16 MiB maximum frame payload and bounded receive buffer.
- 30-second transport read/write timeouts.
- 64 coordinator and 16 node concurrent connections by default.
- 64 KiB socket reads and 1 MiB replication chunks.
- Replication rejects zero progress, offset mismatch, declared-size overshoot, incorrect
  final size, and final hash mismatch.
- Zstandard decompression stops at the configured component limit (1 GiB fallback).
- Staging sweeps retain active leases and age unleased entries by a one-hour default TTL.
- Ambiguous failed RPC calls are not automatically replayed.
- Server shutdown, worker ownership, and multiprocess example cleanup are bounded and
  panic-safe.

There are no tenant-wide request, metadata-growth, storage, or bandwidth quotas. A slow
peer can consume a connection until an I/O timeout, and valid dataset-sized operations
still scale with persisted state.

## Local filesystem assumptions

The node data directory and provider restore roots must be writable only by trusted local
principals. Path and symlink checks defend against untrusted serialized paths, but no
claim is made against a privileged same-host attacker racing filesystem changes. On
Windows, recursive read-handle and parent-directory `fsync` steps are unavailable; data
writes are still flushed through write handles before atomic rename.

## Process metadata

`ProcessMetadataProvider` is diagnostic. Environment values are emitted only for an
explicit allowlist; otherwise matching names are recorded without their values. Review
all custom provider payloads and application metadata before treating a checkpoint as
free of secrets.

## Unsafe code

The current `src/`, `tests/`, `examples/`, and `benches/` trees contain no Rust `unsafe`
blocks. Provider callbacks can still perform arbitrary application-defined operations.

## Vulnerability reporting

Report security issues through GitHub's private vulnerability-reporting channel for this
repository rather than a public issue. Include the affected version, trust boundary,
minimal reproducer, impact, and suggested mitigation if known. The repository does not
publish a response-time SLA or supported-branch policy.

