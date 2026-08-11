# Contributing

Thanks for contributing to Checkpoint Fabric.

## Ground rules

- Treat lifecycle, generations, authority, physical ownership, and recovery evidence as
  correctness boundaries.
- Keep the core vendor-, framework-, workload-, orchestrator-, and storage-product
  neutral. Put state-specific behavior behind provider or storage interfaces.
- Do not claim support from an enum, descriptor, or trait alone. Unsupported behavior
  must remain explicit and fail closed.
- Do not weaken assertions, skip crash points, or lengthen timeouts to hide a defect.
- Keep all targets warning-clean under warnings-as-errors.
- Preserve typed errors; normal peer, provider, storage, and compatibility failures must
  not panic.

## Prerequisites

- Rust 1.81 or newer and Cargo.
- A C toolchain for bundled SQLite (`rusqlite` with the `bundled` feature).

## Development loop

```text
cargo build
cargo test --all-targets
cargo run --example 01_basic
cargo run --example 12_multiprocess
cargo bench --no-run
```

The Criterion target currently contains no registered measurements. Compiling it is a
build check, not performance evidence.

## Validation before submitting

```text
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets
cargo test --test property --test security
cargo test --test failure_injection
cargo test --test multiprocess
cargo build --release
```

Run all twelve examples for changes to public operations, providers, transport,
lifecycle, recovery, or CLI behavior. Changes to multiprocess ownership must repeat the
multiprocess suite and verify that every coordinator/node child is reaped on success,
error, and panic.

## Hardening invariants

Changes must preserve:

1. Required state components are never silently omitted from a committed checkpoint.
2. Consistency and resumability never exceed the evidence actually captured.
3. A committed checkpoint has a durable location, complete component metadata, and a
   coordinator-held manifest anchor.
4. Capture availability, supersession, lineage, workload counters, and commit journal
   marker change atomically.
5. Restore generation, execution epoch, target claim, lifecycle/counts, lineage, and
   commit journal marker change atomically.
6. Pre-commit restore failure reverses provider mutations; post-commit failure never
   pretends the generation commit was rolled back.
7. Stale coordinator epochs, node boot identities, workload epochs, nodes, and fence
   tokens cannot retain authority.
8. Restore, verify, and replication compare physical state to the coordinator manifest
   anchor and fail closed.
9. Ambiguous failed RPCs are never replayed automatically.
10. Replication makes bounded positive progress and cannot exceed manifest-declared size.
11. Cleanup cannot escape the staging root or remove a live leased attempt.
12. Node restart rebinds only structurally committed checkpoints advertised from that
    node's storage.
13. Fork cannot create an unlinked extra workload.
14. Retirement cannot erase the metadata needed to retry failed physical deletion.
15. Recovery cannot fabricate missing components, locations, counters, lineage, or
    authority state.
16. Owned multiprocess children are killed and reaped during unwinding.

Any change that crosses physical and metadata state needs regression tests at every
meaningful crash boundary. Authority/path changes need property or security tests.
Coordinator/node behavior needs a real-process test in addition to in-process coverage.

## Code style

- Follow the responsibility-based module layout.
- Prefer deterministic data structures and stable ordering where output is persisted or
  compared.
- Keep commit boundaries visible and short; do not split one invariant across unrelated
  transactions.
- Avoid hidden fallback. Return `UnsupportedBackend`, compatibility, policy, integrity,
  or provider errors as appropriate.
- Use `cargo fmt`; document public types and non-obvious durability assumptions.
- No `unsafe` without a narrowly documented invariant and dedicated tests.

## Documentation and evidence

Update README/architecture/security/changelog text when public behavior, trust
boundaries, commands, or validation evidence changes. Do not publish machine-specific
performance numbers without a real registered benchmark, methodology, build profile,
hardware, dataset, and repeated results.

## License

By contributing, you agree that your contributions are licensed under the Apache License
2.0 (see [LICENSE](LICENSE)). No contributor license agreement is required.
Participation is also subject to [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

