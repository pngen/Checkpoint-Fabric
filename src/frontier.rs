//! The execution frontier: the logical point to which a workload can safely resume.
//!
//! The runtime distinguishes completed work, durably committed work, in-flight work,
//! replayable work, and non-replayable side effects. Capture must not silently cross
//! an unresolved non-idempotent side-effect boundary.

use serde::{Deserialize, Serialize};

/// An operation that is in flight at capture time.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InFlightOperation {
    pub operation_id: String,
    pub kind: String,
    pub started_ms: u64,
    /// Whether re-executing this operation is safe after a restore.
    pub idempotent: bool,
    /// Whether this operation had already committed externally at capture time.
    pub externally_committed: bool,
}

/// Flags describing what the frontier supports.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResumabilityFlags {
    pub exact: bool,
    pub equivalent: bool,
    pub degraded: bool,
    /// Deterministic replay was verified for captured nondeterministic state.
    pub deterministic_replay_verified: bool,
    pub pending_non_replayable: bool,
    pub unknown_side_effects: bool,
}

impl Default for ResumabilityFlags {
    fn default() -> Self {
        Self {
            exact: true,
            equivalent: true,
            degraded: true,
            deterministic_replay_verified: false,
            pending_non_replayable: false,
            unknown_side_effects: false,
        }
    }
}

/// The logical execution frontier of a workload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ExecutionFrontier {
    pub workload_generation: u64,
    pub execution_epoch: u64,
    /// Monotonic logical step / sequence number.
    pub logical_step: u64,
    pub application_sequence: Option<String>,
    /// Number of durably committed side effects at capture time.
    pub durable_side_effect_boundary: u64,
    /// Map of state domain -> generation.
    pub state_generation_map: std::collections::BTreeMap<String, u64>,
    pub tool_item_frontier: Option<String>,
    pub external_commit_frontier: Option<String>,
    pub last_completed_operation_id: Option<String>,
    pub in_flight_operations: Vec<InFlightOperation>,
    pub flags: ResumabilityFlags,
}

impl ExecutionFrontier {
    /// Rebase the frontier onto a new workload generation / execution epoch.
    /// Used when a restored checkpoint becomes the new active execution.
    pub fn rebase(&self, workload_generation: u64, execution_epoch: u64) -> Self {
        let mut f = self.clone();
        f.workload_generation = workload_generation;
        f.execution_epoch = execution_epoch;
        f
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_frontier_is_safe() {
        let f = ExecutionFrontier::default();
        assert!(!f.flags.pending_non_replayable);
        assert!(!f.flags.unknown_side_effects);
        assert!(f.flags.exact);
    }

    #[test]
    fn rebase_preserves_frontier() {
        let f = ExecutionFrontier {
            logical_step: 42,
            ..ExecutionFrontier::default()
        };
        let r = f.rebase(7, 3);
        assert_eq!(r.logical_step, 42);
        assert_eq!(r.workload_generation, 7);
        assert_eq!(r.execution_epoch, 3);
    }
}
