//! Replayable audit records.
//!
//! Every authority-bearing operation writes an audit record with stable
//! operation names, identities, and results, so audit trails can be replayed
//! deterministically.

use serde::{Deserialize, Serialize};

use crate::id::Id;

/// Outcome of an audited operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditResult {
    Ok,
    Failed,
    Recovered,
    Rejected,
}

impl AuditResult {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Failed => "failed",
            Self::Recovered => "recovered",
            Self::Rejected => "rejected",
        }
    }

    pub fn parse_str(s: &str) -> Self {
        match s {
            "ok" => Self::Ok,
            "failed" => Self::Failed,
            "recovered" => Self::Recovered,
            _ => Self::Rejected,
        }
    }
}

/// One audit record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuditRecord {
    pub seq: i64,
    pub ts_ms: u64,
    pub actor: String,
    pub op: String,
    pub workload_id: Option<Id>,
    pub checkpoint_id: Option<Id>,
    pub result: AuditResult,
    pub detail: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn result_roundtrip() {
        for r in [
            AuditResult::Ok,
            AuditResult::Failed,
            AuditResult::Recovered,
            AuditResult::Rejected,
        ] {
            assert_eq!(AuditResult::parse_str(r.as_str()), r);
        }
    }
}
