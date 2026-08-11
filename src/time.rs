//! Wall-clock helpers. All Checkpoint Fabric timestamps are milliseconds since the
//! Unix epoch, stored as `u64`.

use std::time::{SystemTime, UNIX_EPOCH};

/// Milliseconds since the Unix epoch.
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monotonicish() {
        let a = now_ms();
        let b = now_ms();
        assert!(b >= a);
    }
}
