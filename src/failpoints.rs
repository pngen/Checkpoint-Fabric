//! Deterministic failure injection.
//!
//! Failpoints are named, one-shot, and only ever armed explicitly (in tests or via
//! the `CF_FAILPOINT` environment variable read at process start). When armed, the
//! named checkpoint returns `FabricError::FailPoint`, which aborts the operation
//! exactly as a real fault at that point would.
//!
//! Failpoints are never armed by production code paths.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

use crate::errors::{FabricError, FabricResult};

static ARMED: LazyLock<Mutex<HashMap<String, u32>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

/// Arm a failpoint with `count` firings before it disarms itself.
pub fn arm(name: &str, count: u32) {
    let mut m = ARMED.lock().unwrap();
    m.insert(name.to_string(), count);
}

/// Disarm a failpoint.
pub fn disarm(name: &str) {
    let mut m = ARMED.lock().unwrap();
    m.remove(name);
}

/// Clear all failpoints.
pub fn clear() {
    let mut m = ARMED.lock().unwrap();
    m.clear();
}

/// Query whether a failpoint is currently armed.
pub fn is_armed(name: &str) -> bool {
    ARMED.lock().unwrap().contains_key(name)
}

/// Fire the named failpoint if armed. Returns an error when triggered.
pub fn fire(name: &str) -> FabricResult<()> {
    let mut m = ARMED.lock().unwrap();
    if let Some(count) = m.get_mut(name) {
        if *count > 0 {
            *count -= 1;
            if *count == 0 {
                m.remove(name);
            }
            return Err(FabricError::FailPoint(name.to_string()));
        }
    }
    Ok(())
}

/// Parse a `CF_FAILPOINT`-style list ("name1,name2:3") into arms.
pub fn parse_env(spec: &str) -> Vec<(String, u32)> {
    spec.split(',')
        .filter(|s| !s.is_empty())
        .map(|part| {
            if let Some((name, count)) = part.split_once(':') {
                let n = count.parse::<u32>().unwrap_or(1);
                (name.trim().to_string(), n.max(1))
            } else {
                (part.trim().to_string(), 1)
            }
        })
        .collect()
}

/// Arm failpoints from an environment-style spec string.
pub fn arm_from_spec(spec: &str) {
    for (name, count) in parse_env(spec) {
        arm(&name, count);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fires_once() {
        clear();
        arm("test.fp", 1);
        assert!(is_armed("test.fp"));
        assert!(fire("test.fp").is_err());
        assert!(!is_armed("test.fp"));
        assert!(fire("test.fp").is_ok());
    }

    #[test]
    fn spec_parsing() {
        assert_eq!(parse_env("a,b:3"), vec![("a".into(), 1), ("b".into(), 3)]);
        assert_eq!(parse_env(""), vec![]);
    }
}
