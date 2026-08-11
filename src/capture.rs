//! Capture orchestration: planning, validation, sealing, and commit.
//!
//! The coordinator drives the capture protocol; this module holds the pure
//! decision logic (planning, frontier validation, resumability derivation,
//! journal states) so it is unit-testable without processes.

use serde::{Deserialize, Serialize};

use crate::checkpoint::{CheckpointObject, CheckpointType, ConsistencyClass, ResumabilityClass};
use crate::errors::{FabricError, FabricResult};
use crate::frontier::ExecutionFrontier;
use crate::policy::PolicySet;
use crate::sideeffect::SideEffectManifest;
use crate::workload::Workload;

/// Quiescence modes for capture.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum QuiescenceMode {
    Cooperative,
    Forced,
    None,
}

impl QuiescenceMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Cooperative => "COOPERATIVE",
            Self::Forced => "FORCED",
            Self::None => "NONE",
        }
    }
}

/// User options for a capture request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CaptureOptions {
    /// Consistency class to claim. When unset, the coordinator derives the most
    /// honest class from the actual capture behavior.
    pub consistency: Option<ConsistencyClass>,
    pub quiescence: QuiescenceMode,
    /// The execution frontier captured by the application.
    pub frontier: Option<ExecutionFrontier>,
    /// Application-side side-effect manifest.
    pub side_effects: Option<SideEffectManifest>,
    /// Component capture requests (typed descriptors and any constraints).
    pub components: Vec<CaptureComponentRequest>,
    /// Arbitrary metadata attached to the checkpoint.
    pub metadata: serde_json::Value,
}

impl Default for CaptureOptions {
    fn default() -> Self {
        Self {
            consistency: None,
            quiescence: QuiescenceMode::None,
            frontier: None,
            side_effects: None,
            components: Vec::new(),
            metadata: serde_json::json!({}),
        }
    }
}

/// Request to capture one typed component.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CaptureComponentRequest {
    pub component_id: String,
    pub component_type: crate::checkpoint::ComponentType,
    pub required: bool,
    pub schema_version: u32,
    pub restore_handler: String,
}

/// The durable record of a capture or restore attempt.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptRecord {
    pub id: String,
    pub kind: String,
    pub checkpoint_id: Option<crate::id::Id>,
    pub workload_id: Option<crate::id::Id>,
    pub node: String,
    pub state: String,
    pub started_ms: u64,
    pub finished_ms: Option<u64>,
    pub result: Option<String>,
    pub error: Option<String>,
}

/// Derive the checkpoint type from capture options and policy.
pub fn derive_checkpoint_type(options: &CaptureOptions, policy: &PolicySet) -> CheckpointType {
    if options.components.iter().all(|c| c.required) && policy.require_portable {
        CheckpointType::Portable
    } else if options.components.iter().any(|c| c.required) {
        CheckpointType::Application
    } else {
        CheckpointType::Full
    }
}

/// Derive the honest consistency class from requested class, quiescence mode,
/// and whether a frontier was provided. Never overstates the achieved class.
pub fn derive_consistency(
    requested: Option<ConsistencyClass>,
    quiescence: QuiescenceMode,
    frontier: Option<&ExecutionFrontier>,
    cooperative_ack: bool,
) -> ConsistencyClass {
    match requested {
        Some(ConsistencyClass::ExecutionConsistent) => {
            if quiescence == QuiescenceMode::Cooperative && cooperative_ack && frontier.is_some() {
                ConsistencyClass::ExecutionConsistent
            } else {
                ConsistencyClass::CrashConsistent
            }
        }
        Some(ConsistencyClass::ApplicationConsistent) => {
            if quiescence == QuiescenceMode::Cooperative && cooperative_ack {
                ConsistencyClass::ApplicationConsistent
            } else {
                // Neither FORCED nor NONE quiescence can substantiate an
                // application-consistent claim.
                ConsistencyClass::CrashConsistent
            }
        }
        Some(ConsistencyClass::CrashConsistent) | None => ConsistencyClass::CrashConsistent,
    }
}

/// Derive the honest resumability class from consistency, frontier flags, and
/// side-effect safety. Never overstates the achieved class.
pub fn derive_resumability(
    consistency: ConsistencyClass,
    frontier: Option<&ExecutionFrontier>,
    side_effects: Option<&SideEffectManifest>,
) -> ResumabilityClass {
    if consistency != ConsistencyClass::ExecutionConsistent {
        // Without an execution-consistent frontier we can only promise
        // restart-from-checkpoint semantics.
        return ResumabilityClass::RestartFromCheckpoint;
    }
    let f = match frontier {
        Some(f) => f,
        None => return ResumabilityClass::RestartFromCheckpoint,
    };
    if let Some(se) = side_effects {
        if se.validate_for_exact().is_err() {
            return ResumabilityClass::Degraded;
        }
    }
    if f.flags.pending_non_replayable || f.flags.unknown_side_effects {
        return ResumabilityClass::Degraded;
    }
    if f.flags.deterministic_replay_verified && f.flags.exact {
        ResumabilityClass::Exact
    } else if f.flags.equivalent {
        ResumabilityClass::Equivalent
    } else {
        ResumabilityClass::Degraded
    }
}

/// Validate a capture request against the workload and policy.
pub fn validate_capture_request(
    workload: &Workload,
    options: &CaptureOptions,
    policy: &PolicySet,
    last_capture_ms: Option<u64>,
    now: u64,
) -> FabricResult<()> {
    if policy.min_capture_interval_ms > 0 {
        if let Some(last) = last_capture_ms {
            if now.saturating_sub(last) < policy.min_capture_interval_ms {
                return Err(FabricError::PolicyViolation(format!(
                    "capture interval {}ms not elapsed since last capture",
                    policy.min_capture_interval_ms
                )));
            }
        }
    }
    for c in &options.components {
        if c.component_id.is_empty() || c.component_id.len() > 256 {
            return Err(FabricError::InvalidArgument(
                "component id must be 1..=256 chars".into(),
            ));
        }
        if crate::storage::sanitize_segment(&c.component_id) != c.component_id {
            return Err(FabricError::InvalidArgument(format!(
                "component id '{}' contains unsafe path characters",
                c.component_id
            )));
        }
    }
    let ids: Vec<&String> = options.components.iter().map(|c| &c.component_id).collect();
    let unique: std::collections::HashSet<&String> = ids.iter().copied().collect();
    if unique.len() != ids.len() {
        return Err(FabricError::InvalidArgument(
            "duplicate component ids in capture request".into(),
        ));
    }
    if let Some(f) = &options.frontier {
        if f.workload_generation != workload.workload_generation {
            return Err(FabricError::FrontierInconsistency(format!(
                "frontier workload generation {} does not match workload generation {}",
                f.workload_generation, workload.workload_generation
            )));
        }
    }
    if let Some(se) = &options.side_effects {
        for e in &se.entries {
            if e.side_effect_id.is_empty() {
                return Err(FabricError::InvalidArgument(
                    "side effect id must not be empty".into(),
                ));
            }
        }
        if (se.frontier_boundary as usize) > se.entries.len() {
            return Err(FabricError::FrontierInconsistency(
                "side-effect boundary exceeds entry count".into(),
            ));
        }
    }
    Ok(())
}

/// Determine whether a workload is capture-eligible given its fence state.
pub fn workload_captureable(workload: &Workload, actor_node: Option<&str>) -> FabricResult<()> {
    if workload.is_fenced() {
        return Err(FabricError::FencingFailure(format!(
            "workload {} is fenced; capture requires an active continuation",
            workload.workload_id
        )));
    }
    if workload.single_active {
        if let Some(node) = actor_node {
            if let Some(active) = &workload.active_node {
                if active != node {
                    return Err(FabricError::FencingFailure(format!(
                        "capture requested from node {node} but active node is {active}"
                    )));
                }
            }
        }
    }
    Ok(())
}

/// Build the checkpoint object scaffold for a capture attempt.
#[allow(clippy::too_many_arguments)]
pub fn checkpoint_scaffold(
    workload: &Workload,
    checkpoint_id: crate::id::Id,
    checkpoint_generation: u64,
    attempt_id: &str,
    coordinator_epoch: u64,
    source_node: &str,
    options: &CaptureOptions,
    policy: &PolicySet,
) -> CheckpointObject {
    CheckpointObject {
        checkpoint_id,
        workload_id: workload.workload_id,
        workload_generation: workload.workload_generation,
        checkpoint_generation,
        parent_checkpoint: None,
        capture_attempt: attempt_id.to_string(),
        coordinator_epoch,
        created_ms: crate::time::now_ms(),
        seal_ms: 0,
        source_node: source_node.to_string(),
        source_backend: workload.backend_class.clone(),
        frontier: options.frontier.clone().unwrap_or_default(),
        checkpoint_type: derive_checkpoint_type(options, policy),
        consistency: options
            .consistency
            .unwrap_or(ConsistencyClass::CrashConsistent),
        resumability: ResumabilityClass::RestartFromCheckpoint,
        components: Vec::new(),
        total_logical_bytes: 0,
        total_physical_bytes: 0,
        compressed_bytes: 0,
        durable_locations: Vec::new(),
        replica_count: 0,
        lineage_parents: Vec::new(),
        lineage_children: Vec::new(),
        supersedes: None,
        superseded_by: None,
        policy_version: policy.version,
        runtime_descriptor: workload.runtime.clone(),
        hardware_descriptor: crate::checkpoint::HardwareCompatibilityDescriptor::default(),
        dependencies: Vec::new(),
        integrity_state: crate::checkpoint::IntegrityState::Pending,
        lifecycle: crate::lifecycle::LifecycleState::Created,
        restore_count: 0,
        last_restore_result: None,
        protection: crate::checkpoint::ProtectionState::None,
        retirement_eligibility: crate::checkpoint::RetirementEligibility::NotEligible,
        metadata: options.metadata.clone(),
        manifest_digest: None,
        manifest_json: None,
    }
}

/// Validate a captured component set for completeness (invariant #1).
pub fn validate_component_completeness(
    requested: &[CaptureComponentRequest],
    captured: &[crate::checkpoint::ComponentEntry],
) -> FabricResult<()> {
    let captured_ids: std::collections::HashSet<&str> =
        captured.iter().map(|c| c.component_id.as_str()).collect();
    for r in requested {
        if r.required && !captured_ids.contains(r.component_id.as_str()) {
            return Err(FabricError::IncompleteCheckpoint(format!(
                "required component '{}' was not captured",
                r.component_id
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::ComponentType;
    use crate::frontier::{InFlightOperation, ResumabilityFlags};
    use crate::sideeffect::{SideEffectEntry, SideEffectStatus};

    fn workload() -> Workload {
        Workload {
            workload_id: crate::id::Id::random(),
            workload_generation: 3,
            owner: "t".into(),
            class: "test".into(),
            created_ms: 0,
            execution_epoch: 2,
            active_node: Some("n1".into()),
            backend_class: "cpu".into(),
            checkpoint_generation: 1,
            parent_workload: None,
            fork_generation: 0,
            policy_version: 1,
            metadata: serde_json::json!({}),
            state_schema_version: 1,
            runtime: crate::compatibility::RuntimeCompatibilityDescriptor::local_default(),
            resumability_class: ResumabilityClass::Equivalent,
            protection: crate::workload::ProtectionSpec::default(),
            single_active: true,
            fence_token: Some("t".into()),
            fence_epoch: 2,
        }
    }

    fn frontier(flags: ResumabilityFlags) -> ExecutionFrontier {
        ExecutionFrontier {
            workload_generation: 3,
            execution_epoch: 2,
            logical_step: 10,
            flags,
            ..ExecutionFrontier::default()
        }
    }

    #[test]
    fn consistency_never_overstated() {
        let f = frontier(ResumabilityFlags::default());
        assert_eq!(
            derive_consistency(
                Some(ConsistencyClass::ExecutionConsistent),
                QuiescenceMode::None,
                Some(&f),
                false
            ),
            ConsistencyClass::CrashConsistent
        );
        assert_eq!(
            derive_consistency(
                Some(ConsistencyClass::ExecutionConsistent),
                QuiescenceMode::Cooperative,
                Some(&f),
                true
            ),
            ConsistencyClass::ExecutionConsistent
        );
        assert_eq!(
            derive_consistency(
                Some(ConsistencyClass::ExecutionConsistent),
                QuiescenceMode::Cooperative,
                None,
                true
            ),
            ConsistencyClass::CrashConsistent
        );
        assert_eq!(
            derive_consistency(
                Some(ConsistencyClass::ApplicationConsistent),
                QuiescenceMode::Forced,
                Some(&f),
                false
            ),
            ConsistencyClass::CrashConsistent
        );
        assert_eq!(
            derive_consistency(
                Some(ConsistencyClass::ApplicationConsistent),
                QuiescenceMode::Cooperative,
                Some(&f),
                true
            ),
            ConsistencyClass::ApplicationConsistent
        );
    }

    #[test]
    fn resumability_derivation() {
        // Execution-consistent with verified replay -> EXACT
        let f = frontier(ResumabilityFlags {
            deterministic_replay_verified: true,
            exact: true,
            ..ResumabilityFlags::default()
        });
        assert_eq!(
            derive_resumability(ConsistencyClass::ExecutionConsistent, Some(&f), None),
            ResumabilityClass::Exact
        );
        // Unknown side effects -> DEGRADED
        let se = SideEffectManifest {
            entries: vec![SideEffectEntry {
                side_effect_id: "s1".into(),
                kind: "db".into(),
                status: SideEffectStatus::Unknown,
                idempotency_key: None,
                committed_ms: None,
            }],
            frontier_boundary: 0,
        };
        assert_eq!(
            derive_resumability(ConsistencyClass::ExecutionConsistent, Some(&f), Some(&se)),
            ResumabilityClass::Degraded
        );
        // No frontier -> RESTART_FROM_CHECKPOINT
        assert_eq!(
            derive_resumability(ConsistencyClass::CrashConsistent, Some(&f), None),
            ResumabilityClass::RestartFromCheckpoint
        );
    }

    #[test]
    fn capture_request_validation() {
        let w = workload();
        let p = PolicySet::default();
        let opts = CaptureOptions::default();
        assert!(validate_capture_request(&w, &opts, &p, None, 0).is_ok());
        let mut dup = CaptureOptions {
            components: vec![
                CaptureComponentRequest {
                    component_id: "a".into(),
                    component_type: ComponentType::ApplicationState,
                    required: true,
                    schema_version: 1,
                    restore_handler: "application".into(),
                },
                CaptureComponentRequest {
                    component_id: "a".into(),
                    component_type: ComponentType::ApplicationState,
                    required: true,
                    schema_version: 1,
                    restore_handler: "application".into(),
                },
            ],
            ..Default::default()
        };
        assert!(validate_capture_request(&w, &dup, &p, None, 0).is_err());
        dup.components.pop();
        let mut f = frontier(ResumabilityFlags::default());
        f.workload_generation = 99;
        let opts2 = CaptureOptions {
            frontier: Some(f),
            ..Default::default()
        };
        assert!(validate_capture_request(&w, &opts2, &p, None, 0).is_err());
    }

    #[test]
    fn completeness_enforced() {
        let req = vec![CaptureComponentRequest {
            component_id: "must".into(),
            component_type: ComponentType::ApplicationState,
            required: true,
            schema_version: 1,
            restore_handler: "application".into(),
        }];
        assert!(validate_component_completeness(&req, &[]).is_err());
    }

    #[test]
    fn fenced_workload_not_captureable() {
        let mut w = workload();
        assert!(workload_captureable(&w, Some("n1")).is_ok());
        assert!(workload_captureable(&w, Some("n2")).is_err());
        w.fence_token = None;
        assert!(workload_captureable(&w, Some("n1")).is_err());
    }

    #[test]
    fn in_flight_op_roundtrip() {
        let op = InFlightOperation {
            operation_id: "op-1".into(),
            kind: "batch".into(),
            started_ms: 1,
            idempotent: false,
            externally_committed: false,
        };
        let j = serde_json::to_string(&op).unwrap();
        let back: InFlightOperation = serde_json::from_str(&j).unwrap();
        assert_eq!(op, back);
    }
}
