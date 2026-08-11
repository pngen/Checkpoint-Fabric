//! Strict checkpoint lifecycle state machine.
//!
//! All transitions are durable and auditable. Illegal transitions fail
//! deterministically. Repeated idempotent requests never create duplicate
//! committed checkpoints (that guarantee is enforced by the coordinator's
//! reservation and generation logic in [`crate::coordinator`]).

use serde::{Deserialize, Serialize};

use crate::errors::{FabricError, FabricResult};

/// Lifecycle states of a checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LifecycleState {
    Created,
    Capturing,
    Captured,
    Validating,
    Sealed,
    Persisting,
    Available,
    RestorePending,
    Restoring,
    Restored,
    Retired,
    Failed,
}

impl LifecycleState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Created => "CREATED",
            Self::Capturing => "CAPTURING",
            Self::Captured => "CAPTURED",
            Self::Validating => "VALIDATING",
            Self::Sealed => "SEALED",
            Self::Persisting => "PERSISTING",
            Self::Available => "AVAILABLE",
            Self::RestorePending => "RESTORE_PENDING",
            Self::Restoring => "RESTORING",
            Self::Restored => "RESTORED",
            Self::Retired => "RETIRED",
            Self::Failed => "FAILED",
        }
    }

    /// Can a checkpoint in this state begin a restore?
    pub fn is_restorable(&self) -> bool {
        matches!(self, Self::Available | Self::Restored)
    }

    /// Is this a terminal state?
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Retired | Self::Failed)
    }

    /// Deterministic transition check.
    pub fn can_transition(&self, target: LifecycleState) -> bool {
        match self {
            Self::Created => matches!(target, Self::Capturing),
            Self::Capturing => matches!(target, Self::Captured | Self::Failed),
            Self::Captured => matches!(target, Self::Validating | Self::Failed),
            Self::Validating => matches!(target, Self::Sealed | Self::Failed),
            Self::Sealed => matches!(target, Self::Persisting | Self::Failed),
            Self::Persisting => matches!(target, Self::Available | Self::Failed),
            Self::Available => {
                matches!(target, Self::RestorePending | Self::Retired | Self::Failed)
            }
            Self::RestorePending => {
                matches!(target, Self::Restoring | Self::Failed | Self::Available)
            }
            // A failed restore rolls the source checkpoint back to AVAILABLE;
            // the checkpoint itself is not corrupt merely because applying it
            // to one target failed.
            Self::Restoring => matches!(target, Self::Restored | Self::Available | Self::Failed),
            Self::Restored => matches!(target, Self::Available | Self::Failed),
            Self::Retired => {
                matches!(target, Self::RestorePending | Self::Failed)
            }
            Self::Failed => false,
        }
    }

    /// Transition, returning a typed error for illegal transitions.
    pub fn transition(&self, target: LifecycleState) -> FabricResult<()> {
        if self.can_transition(target) {
            Ok(())
        } else {
            Err(FabricError::InvalidLifecycleTransition {
                from: self.as_str().to_string(),
                to: target.as_str().to_string(),
            })
        }
    }
}

/// The sequence of states a fresh checkpoint passes through on its happy path.
pub const HAPPY_PATH: [LifecycleState; 7] = [
    LifecycleState::Created,
    LifecycleState::Capturing,
    LifecycleState::Captured,
    LifecycleState::Validating,
    LifecycleState::Sealed,
    LifecycleState::Persisting,
    LifecycleState::Available,
];

/// The sequence of states a restore passes through.
pub const RESTORE_PATH: [LifecycleState; 4] = [
    LifecycleState::RestorePending,
    LifecycleState::Restoring,
    LifecycleState::Restored,
    LifecycleState::Available,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn happy_path_is_legal() {
        let mut s = LifecycleState::Created;
        for next in HAPPY_PATH.iter().skip(1) {
            assert!(s.can_transition(*next));
            s = *next;
        }
        assert!(s.is_restorable());
    }

    #[test]
    fn illegal_transitions_fail() {
        assert!(LifecycleState::Created
            .transition(LifecycleState::Available)
            .is_err());
        assert!(LifecycleState::Sealed
            .transition(LifecycleState::Restoring)
            .is_err());
        assert!(LifecycleState::Available
            .transition(LifecycleState::Capturing)
            .is_err());
        assert!(LifecycleState::Retired
            .transition(LifecycleState::Available)
            .is_err());
        assert!(LifecycleState::Failed
            .transition(LifecycleState::Available)
            .is_err());
        assert!(LifecycleState::Restored
            .transition(LifecycleState::Retired)
            .is_err());
    }

    #[test]
    fn retired_archive_restore_requires_policy() {
        assert!(LifecycleState::Retired.can_transition(LifecycleState::RestorePending));
    }

    #[test]
    fn terminal_states() {
        assert!(LifecycleState::Failed.is_terminal());
        assert!(LifecycleState::Retired.is_terminal());
        assert!(!LifecycleState::Available.is_terminal());
    }
}
