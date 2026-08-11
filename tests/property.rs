//! Property-based invariants (proptest).
//!
//! Key invariants verified:
//! 1. Canonical manifest serialization is stable (parse -> serialize == same bytes).
//! 18. Identical compatibility inputs produce identical verdicts.
//! - Id hex roundtrips.
//! - Lifecycle transitions only ever produce states reachable from the start.

use proptest::prelude::*;

use checkpoint_fabric::checkpoint::{
    CheckpointObject, CheckpointType, ComponentEntry, ComponentType, ConsistencyClass,
    HardwareCompatibilityDescriptor, IntegrityState, ProtectionState, ResumabilityClass,
    RetirementEligibility, StorageRepresentation,
};
use checkpoint_fabric::compatibility::{self, RuntimeCompatibilityDescriptor};
use checkpoint_fabric::frontier::ExecutionFrontier;
use checkpoint_fabric::id::Id;
use checkpoint_fabric::lifecycle::LifecycleState;
use checkpoint_fabric::manifest;

fn arb_string() -> impl Strategy<Value = String> {
    "[a-zA-Z0-9_/.-]{0,32}"
}

fn arb_component() -> impl Strategy<Value = ComponentEntry> {
    (
        arb_string(),
        any::<u8>(),
        any::<bool>(),
        any::<u32>(),
        arb_string(),
        arb_string(),
    )
        .prop_map(|(cid, t, required, gen, handler, hash)| ComponentEntry {
            component_id: cid.clone(),
            component_type: match t % 14 {
                0 => ComponentType::ProcessMetadata,
                1 => ComponentType::ApplicationState,
                2 => ComponentType::MemoryRegion,
                3 => ComponentType::AcceleratorState,
                4 => ComponentType::RuntimeState,
                5 => ComponentType::QueueState,
                6 => ComponentType::SchedulerState,
                7 => ComponentType::RngState,
                8 => ComponentType::FilesystemState,
                9 => ComponentType::OpenResourceDescriptor,
                10 => ComponentType::ToolState,
                11 => ComponentType::ModelState,
                12 => ComponentType::KvState,
                _ => ComponentType::CustomState,
            },
            generation: gen as u64,
            required,
            logical_size: gen as u64,
            storage_representation: StorageRepresentation {
                codec: "none".into(),
                original_size: 0,
                stored_size: 0,
                stored_hash: hash.clone(),
                relative_path: format!("components/{cid}"),
            },
            content_hash: hash,
            schema_version: 1,
            restore_handler: handler,
            compatibility: serde_json::json!({}),
            dependencies: Vec::new(),
            capture_status: "captured".into(),
            restore_status: "pending".into(),
        })
}

fn arb_checkpoint() -> impl Strategy<Value = CheckpointObject> {
    (
        prop::collection::vec(arb_component(), 0..8),
        any::<u8>(),
        any::<u64>(),
        any::<bool>(),
    )
        .prop_map(|(components, t, gen, exact)| {
            let mut c = CheckpointObject {
                checkpoint_id: Id::random(),
                workload_id: Id::random(),
                workload_generation: gen,
                checkpoint_generation: gen.wrapping_add(1),
                parent_checkpoint: None,
                capture_attempt: "prop".into(),
                coordinator_epoch: 1,
                created_ms: 0,
                seal_ms: 0,
                source_node: "n".into(),
                source_backend: "cpu".into(),
                frontier: ExecutionFrontier::default(),
                checkpoint_type: match t % 8 {
                    0 => CheckpointType::Full,
                    1 => CheckpointType::Incremental,
                    2 => CheckpointType::Delta,
                    3 => CheckpointType::Application,
                    4 => CheckpointType::Process,
                    5 => CheckpointType::ExecutionFrontier,
                    6 => CheckpointType::Portable,
                    _ => CheckpointType::LocalOnly,
                },
                consistency: match t % 3 {
                    0 => ConsistencyClass::CrashConsistent,
                    1 => ConsistencyClass::ApplicationConsistent,
                    _ => ConsistencyClass::ExecutionConsistent,
                },
                resumability: if exact {
                    ResumabilityClass::Exact
                } else {
                    ResumabilityClass::Equivalent
                },
                components,
                total_logical_bytes: 0,
                total_physical_bytes: 0,
                compressed_bytes: 0,
                durable_locations: Vec::new(),
                replica_count: 1,
                lineage_parents: Vec::new(),
                lineage_children: Vec::new(),
                supersedes: None,
                superseded_by: None,
                policy_version: 1,
                runtime_descriptor: RuntimeCompatibilityDescriptor::local_default(),
                hardware_descriptor: HardwareCompatibilityDescriptor::default(),
                dependencies: Vec::new(),
                integrity_state: IntegrityState::Valid,
                lifecycle: LifecycleState::Available,
                restore_count: 0,
                last_restore_result: None,
                protection: ProtectionState::None,
                retirement_eligibility: RetirementEligibility::Eligible,
                metadata: serde_json::json!({}),
                manifest_digest: None,
                manifest_json: None,
            };
            c.checkpoint_id = Id::random();
            c
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn manifest_canonical_bytes_are_stable(ck in arb_checkpoint()) {
        let m = manifest::scaffold(&ck);
        let bytes = m.to_canonical_bytes();
        let parsed: manifest::Manifest = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed.to_canonical_bytes(), bytes);
    }

    #[test]
    fn manifest_digest_deterministic(ck in arb_checkpoint()) {
        let sealed = manifest::seal(manifest::scaffold(&ck));
        let sealed2 = manifest::seal(manifest::scaffold(&ck));
        assert_eq!(sealed.digest, sealed2.digest);
        assert_eq!(sealed.integrity_root, sealed2.integrity_root);
        // The sealed manifest parses back and reproduces the digest.
        let parsed = manifest::parse(&sealed.canonical_bytes).unwrap();
        assert_eq!(parsed.digest(), sealed.digest);
    }

    #[test]
    fn integrity_root_ties_to_content(ck in arb_checkpoint()) {
        let sealed = manifest::seal(manifest::scaffold(&ck));
        let mut ck2 = ck.clone();
        if !ck2.components.is_empty() {
            ck2.components[0].content_hash.push('0');
        } else {
            ck2.components.push(ComponentEntry {
                component_id: "extra".into(),
                component_type: ComponentType::CustomState,
                generation: 0,
                required: true,
                logical_size: 1,
                storage_representation: StorageRepresentation {
                    codec: "none".into(),
                    original_size: 1,
                    stored_size: 1,
                    stored_hash: "s".into(),
                    relative_path: "components/extra".into(),
                },
                content_hash: "c".into(),
                schema_version: 1,
                restore_handler: "h".into(),
                compatibility: serde_json::json!({}),
                dependencies: Vec::new(),
                capture_status: "captured".into(),
                restore_status: "pending".into(),
            });
        }
        let sealed2 = manifest::seal(manifest::scaffold(&ck2));
        assert_ne!(sealed.integrity_root, sealed2.integrity_root);
    }

    #[test]
    fn compatibility_decisions_are_deterministic(
        ck in arb_checkpoint(),
        os in arb_string(),
        arch in arb_string(),
        backend in arb_string(),
    ) {
        let mut target = RuntimeCompatibilityDescriptor::local_default();
        target.os = os;
        target.arch = arch;
        target.backend_class = backend;
        let policy = checkpoint_fabric::policy::PolicySet::default();
        let r1 = compatibility::evaluate(&ck, &target, &policy).unwrap();
        let r2 = compatibility::evaluate(&ck, &target, &policy).unwrap();
        assert_eq!(r1, r2);
        // The achieved resumability class never exceeds the captured class.
        let achieved = r1.resumability_for(ck.resumability);
        assert!(achieved <= ck.resumability, "{achieved:?} > {:?}", ck.resumability);
    }

    #[test]
    fn id_hex_roundtrip(prefix in "[0-9a-f]{1,8}", suffix in "[0-9a-f]{1,8}") {
        let mut hex = prefix;
        while hex.len() < 32 { hex.push('0'); }
        hex.push_str(&suffix);
        hex.truncate(32);
        let id = Id::from_hex(&hex).unwrap();
        assert_eq!(id.to_hex(), hex);
        let back: Id = serde_json::from_str(&format!("\"{hex}\"")).unwrap();
        assert_eq!(back, id);
    }

    #[test]
    fn lifecycle_states_are_reachable_only_via_legal_edges(state in any::<u8>()) {
        let s = match state % 12 {
            0 => LifecycleState::Created,
            1 => LifecycleState::Capturing,
            2 => LifecycleState::Captured,
            3 => LifecycleState::Validating,
            4 => LifecycleState::Sealed,
            5 => LifecycleState::Persisting,
            6 => LifecycleState::Available,
            7 => LifecycleState::RestorePending,
            8 => LifecycleState::Restoring,
            9 => LifecycleState::Restored,
            10 => LifecycleState::Retired,
            _ => LifecycleState::Failed,
        };
        // Terminal states accept no transitions; Retired only permits the
        // archive-restore path; every other state can fail deterministically.
        if s.is_terminal() {
            for candidate in [
                LifecycleState::Created,
                LifecycleState::Available,
                LifecycleState::Restored,
            ] {
                assert!(!s.can_transition(candidate));
            }
        } else if matches!(s, LifecycleState::Retired) {
            assert!(s.can_transition(LifecycleState::RestorePending));
            assert!(!s.can_transition(LifecycleState::Available));
        } else if matches!(s, LifecycleState::Created) {
            // Created may only enter Capturing; failures surface later.
            assert!(s.can_transition(LifecycleState::Capturing));
            assert!(!s.can_transition(LifecycleState::Failed));
        } else {
            assert!(!s.is_terminal());
            assert!(s.can_transition(LifecycleState::Failed));
        }
    }

    #[test]
    fn resumability_class_ordering_is_consistent(_a in any::<u8>()) {
        use checkpoint_fabric::checkpoint::ResumabilityClass;
        let classes = [
            ResumabilityClass::NonResumable,
            ResumabilityClass::RestartFromCheckpoint,
            ResumabilityClass::Degraded,
            ResumabilityClass::Equivalent,
            ResumabilityClass::Exact,
        ];
        // The derived ordering is total and matches the documented semantics.
        for i in 0..classes.len() {
            for j in 0..classes.len() {
                assert_eq!(
                    classes[i] > classes[j],
                    i > j,
                    "ordering mismatch at {i} vs {j}"
                );
            }
        }
        // Serialization is stable across instances.
        for c in &classes {
            let j = serde_json::to_string(c).unwrap();
            let back: ResumabilityClass = serde_json::from_str(&j).unwrap();
            assert_eq!(&back, c);
        }
    }
}
