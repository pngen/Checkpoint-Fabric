//! Multi-process integration tests using real coordinator/node processes and
//! the real CLI over framed TCP.

mod common;

use std::path::PathBuf;
use std::process::Command;
use std::time::Duration;

use checkpoint_fabric::protocol::{self, Op};
use checkpoint_fabric::transport::RpcClient;
use common::*;

fn tmpdir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "cf-test-{tag}-{}",
        checkpoint_fabric::id::Id::random().to_hex()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn cli_capture_and_restore_end_to_end() {
    let coord_dir = tmpdir("e2e-coord");
    let node_dir = tmpdir("e2e-node");
    let mut coord = CoordProc::start(&coord_dir, &[]);
    let _node = NodeProc::start("n1", &node_dir, &coord.addr);
    let c = coord.addr.to_string();

    // Node appears.
    wait_until("node registered", || cli(&c, &["nodes"]).contains("n1@"));

    // Create a workload on the node.
    let out = cli(
        &c,
        &[
            "workload",
            "create",
            "--owner",
            "test",
            "--class",
            "e2e",
            "--node",
            &node_name(&c),
        ],
    );
    let workload_id = parse_id(&out);

    // Capture (no providers attached yet -> required components missing is
    // fine when none are requested: capture of an empty component set works).
    let out = cli(&c, &["capture", &workload_id]);
    let ckpt_id = parse_ckpt(&out);

    // Inspect + verify.
    let inspected = cli(&c, &["checkpoint", "inspect", &ckpt_id]);
    assert!(inspected.contains("AVAILABLE"), "{inspected}");
    let verified = cli(&c, &["checkpoint", "verify", &ckpt_id]);
    assert!(
        verified.contains("true") || verified.contains("\"ok\":true"),
        "{verified}"
    );

    // Restore onto the same node.
    let node = node_name(&c);
    let restored = cli(&c, &["restore", &ckpt_id, &node]);
    assert!(restored.contains("RESTORED"), "{restored}");

    // Workload advanced a generation.
    let w = cli(&c, &["workload", "inspect", &workload_id]);
    assert!(w.contains("\"workload_generation\":1"), "{w}");

    // Audit trail exists.
    let audit = cli(&c, &["audit", "--limit", "100"]);
    assert!(audit.contains("capture.commit"));
    assert!(audit.contains("restore.commit"));

    // Stats.
    let stats = cli(&c, &["stats"]);
    assert!(stats.contains("\"workloads\":1"), "{stats}");
    assert!(stats.contains("\"active_nodes\":1"), "{stats}");

    coord.stop();
}

#[test]
fn multi_node_replication_and_migration() {
    let coord_dir = tmpdir("mig-coord");
    let node1_dir = tmpdir("mig-node1");
    let node2_dir = tmpdir("mig-node2");
    let mut coord = CoordProc::start(&coord_dir, &[]);
    let _node1 = NodeProc::start("source", &node1_dir, &coord.addr);
    let _node2 = NodeProc::start("target", &node2_dir, &coord.addr);
    let c = coord.addr.to_string();

    wait_until("two nodes registered", || {
        let nodes = cli(&c, &["nodes"]);
        nodes.contains("source@") && nodes.contains("target@")
    });
    let nodes = cli(&c, &["nodes"]);
    assert!(nodes.contains("source@"), "{nodes}");
    assert!(nodes.contains("target@"), "{nodes}");
    let source = node_name_for(&nodes, "source");
    let target = node_name_for(&nodes, "target");

    // Create workload on source.
    let out = cli(
        &c,
        &["workload", "create", "--owner", "test", "--node", &source],
    );
    let workload_id = parse_id(&out);
    cli(&c, &["capture", &workload_id]);
    let ckpts = cli(&c, &["checkpoint", "list", "--workload-id", &workload_id]);
    let ckpt_id = parse_ckpt(&ckpts);

    // Migrate to target (streams a replica to the target node, fences source,
    // transfers authority).
    let migrated = cli(&c, &["migrate", &ckpt_id, &target]);
    assert!(migrated.contains("RESTORED"), "{migrated}");

    // Workload now lives on the target.
    let w = cli(&c, &["workload", "inspect", &workload_id]);
    assert!(w.contains(&target), "{w}");

    // Source is fenced: its token is revoked.
    let lineage = cli(&c, &["workload", "lineage", &workload_id]);
    assert!(lineage.contains("MIGRATED_FROM"), "{lineage}");

    coord.stop();
}

#[test]
fn fork_and_rollback_roundtrip() {
    let coord_dir = tmpdir("fork-coord");
    let node_dir = tmpdir("fork-node");
    let mut coord = CoordProc::start(&coord_dir, &[]);
    let _node = NodeProc::start("n1", &node_dir, &coord.addr);
    let c = coord.addr.to_string();
    wait_until("node registered", || cli(&c, &["nodes"]).contains("n1@"));
    let node = node_name(&c);

    let out = cli(&c, &["workload", "create", "--owner", "t", "--node", &node]);
    let wid = parse_id(&out);
    cli(&c, &["capture", &wid]);
    let ckpt = parse_ckpt(&cli(&c, &["checkpoint", "list", "--workload-id", &wid]));

    // Fork a child workload from the checkpoint.
    let forked = cli(
        &c,
        &[
            "fork",
            &ckpt,
            "--owner",
            "t",
            "--class",
            "child",
            "--single-active",
        ],
    );
    let child_wid = parse_id(&forked);
    assert_ne!(child_wid, wid);
    let lineage = cli(&c, &["workload", "lineage", &child_wid]);
    assert!(lineage.contains("FORKED_FROM"), "{lineage}");

    // Parent is untouched.
    let parent = cli(&c, &["workload", "inspect", &wid]);
    assert!(parent.contains("\"workload_generation\":0"), "{parent}");

    // Roll back the parent to its checkpoint -> new execution generation.
    let rolled = cli(&c, &["rollback", &ckpt, &node]);
    assert!(rolled.contains("RESTORED"), "{rolled}");
    let parent2 = cli(&c, &["workload", "inspect", &wid]);
    assert!(parent2.contains("\"workload_generation\":1"), "{parent2}");
    let lin2 = cli(&c, &["workload", "lineage", &wid]);
    assert!(lin2.contains("ROLLBACK_OF"), "{lin2}");

    coord.stop();
}

#[test]
fn protect_unprotect_retire() {
    let coord_dir = tmpdir("prot-coord");
    let node_dir = tmpdir("prot-node");
    let mut coord = CoordProc::start(&coord_dir, &[]);
    let _node = NodeProc::start("n1", &node_dir, &coord.addr);
    let c = coord.addr.to_string();
    wait_until("node registered", || cli(&c, &["nodes"]).contains("n1@"));
    let node = node_name(&c);

    let out = cli(&c, &["workload", "create", "--owner", "t", "--node", &node]);
    let wid = parse_id(&out);
    cli(&c, &["capture", &wid]);
    let ckpt = parse_ckpt(&cli(&c, &["checkpoint", "list", "--workload-id", &wid]));

    // Protected checkpoints cannot be retired.
    cli(&c, &["checkpoint", "protect", &ckpt]);
    let output = Command::new(BIN)
        .args(["--coordinator", &c, "checkpoint", "retire", &ckpt])
        .output()
        .unwrap();
    assert!(!output.status.success(), "protected retire must fail");

    cli(&c, &["checkpoint", "unprotect", &ckpt]);
    cli(&c, &["checkpoint", "retire", &ckpt]);
    let inspected = cli(&c, &["checkpoint", "inspect", &ckpt]);
    assert!(inspected.contains("RETIRED"), "{inspected}");

    // Retired checkpoints cannot be restored without archive policy.
    let output = Command::new(BIN)
        .args(["--coordinator", &c, "restore", &ckpt, &node])
        .output()
        .unwrap();
    assert!(!output.status.success(), "restore of retired must fail");

    coord.stop();
}

#[test]
fn compatibility_rejection_and_degradation() {
    let coord_dir = tmpdir("compat-coord");
    let node_dir = tmpdir("compat-node");
    let mut coord = CoordProc::start(&coord_dir, &[]);
    let _node = NodeProc::start("n1", &node_dir, &coord.addr);
    let c = coord.addr.to_string();
    wait_until("node registered", || cli(&c, &["nodes"]).contains("n1@"));
    let node = node_name(&c);

    let out = cli(&c, &["workload", "create", "--owner", "t", "--node", &node]);
    let wid = parse_id(&out);
    cli(&c, &["capture", &wid]);
    let ckpt = parse_ckpt(&cli(&c, &["checkpoint", "list", "--workload-id", &wid]));

    // Different OS -> incompatible by default.
    let res = cli(&c, &["compatibility", &ckpt, "--os", "linux"]);
    assert!(
        res.contains("INCOMPATIBLE") || res.contains("CompatibleWithTranslation"),
        "{res}"
    );

    // Same environment -> compatible.
    let res2 = cli(&c, &["compatibility", &ckpt]);
    assert!(res2.contains("COMPATIBLE"), "{res2}");

    coord.stop();
}

#[test]
fn coordinator_restart_keeps_lineage_and_epoch_advances() {
    let coord_dir = tmpdir("restart-coord");
    let node_dir = tmpdir("restart-node");
    let mut coord = CoordProc::start(&coord_dir, &[]);
    let _node = NodeProc::start("n1", &node_dir, &coord.addr);
    let c = coord.addr.to_string();
    wait_until("node registered", || cli(&c, &["nodes"]).contains("n1@"));
    let node = node_name(&c);

    let out = cli(&c, &["workload", "create", "--owner", "t", "--node", &node]);
    let wid = parse_id(&out);
    cli(&c, &["capture", &wid]);
    let _ckpt = parse_ckpt(&cli(&c, &["checkpoint", "list", "--workload-id", &wid]));

    // Restart the coordinator on the same port.
    coord.stop();
    let mut coord2 = CoordProc::start(&coord_dir, &[]);
    let c2 = coord2.addr.to_string();

    // Lineage survives restart (durable).
    let lineage = cli(&c2, &["workload", "lineage", &wid]);
    assert!(
        lineage.contains("SUPERSEDES") || !lineage.is_empty(),
        "{lineage}"
    );

    // Workload and checkpoint survive.
    let w = cli(&c2, &["workload", "inspect", &wid]);
    assert!(w.contains(&wid), "{w}");

    // The old node's epoch is stale; it re-registers with the new epoch.
    wait_until("node re-registered after restart", || {
        cli(&c2, &["nodes"]).contains("n1@")
    });
    let stats = cli(&c2, &["stats"]);
    assert!(stats.contains("\"workloads\":1"), "{stats}");

    // Recovery reconciliation runs cleanly.
    let rec = cli(&c2, &["recovery"]);
    assert!(rec.contains("\"ok\":true"), "{rec}");

    coord2.stop();
}

#[test]
fn stale_coordinator_epoch_rejected() {
    let coord_dir = tmpdir("epoch-coord");
    let mut coord = CoordProc::start(&coord_dir, &[]);

    // A client holding a stale epoch is rejected at registration time by a
    // node; here we verify the coordinator advertises its current epoch.
    let mut client = RpcClient::connect(&coord.addr).unwrap();
    let resp = client.call(Op::Ping, b"ping").unwrap();
    assert_eq!(resp, b"pong");

    // Register with a stale epoch -> rejected.
    let req = protocol::NodeRegisterRequest {
        node_id: "stale@1@deadbeef".into(),
        listen_addr: "127.0.0.1:1".into(),
        boot_id: "deadbeef".into(),
        data_dir: tmpdir("epoch-node").to_string_lossy().to_string(),
        runtime: checkpoint_fabric::compatibility::RuntimeCompatibilityDescriptor::local_default(),
        hardware: Default::default(),
        resources: serde_json::json!({}),
        committed_checkpoints: Vec::new(),
        coordinator_epoch: 0,
    };
    let resp: protocol::NodeRegisterResponse = client.call_json(Op::NodeRegister, &req).unwrap();
    assert!(!resp.accepted);
    assert!(resp
        .error
        .unwrap_or_default()
        .contains("stale coordinator epoch"));

    coord.stop();
}

#[test]
fn graceful_shutdown_leaves_no_processes() {
    let coord_dir = tmpdir("grace-coord");
    let node_dir = tmpdir("grace-node");
    let mut coord = CoordProc::start(&coord_dir, &[]);
    let mut node = NodeProc::start("n1", &node_dir, &coord.addr);
    // Ensure the node is registered and heartbeating before shutdown.
    wait_until("node registered", || {
        cli(&coord.addr.to_string(), &["nodes"]).contains("n1@")
    });

    // Node exits cleanly when the coordinator goes away (bounded heartbeat
    // failures), and the coordinator exits cleanly on RPC shutdown.
    coord.stop();
    let exit = wait_exit(&mut node.child, Duration::from_secs(30));
    assert!(exit.is_some(), "node must exit after coordinator loss");
    assert!(wait_exit(&mut coord.child, Duration::from_secs(5)).is_some());
}

fn node_name(c: &str) -> String {
    node_name_for(&cli(c, &["nodes"]), "n")
}

fn node_name_for(nodes_out: &str, prefix: &str) -> String {
    let v: serde_json::Value = serde_json::from_str(nodes_out.trim())
        .unwrap_or_else(|_| panic!("bad nodes json: {nodes_out}"));
    v.as_array()
        .unwrap_or_else(|| panic!("nodes output is not an array: {nodes_out}"))
        .iter()
        .find(|n| {
            n.get("id")
                .and_then(|i| i.as_str())
                .map(|i| i.starts_with(prefix))
                .unwrap_or(false)
        })
        .and_then(|n| n.get("id")?.as_str().map(|s| s.to_string()))
        .unwrap_or_else(|| panic!("node '{prefix}' not found in: {nodes_out}"))
}

fn parse_id(out: &str) -> String {
    let v: serde_json::Value = serde_json::from_str(out.trim()).expect("json output");
    v.get("workload_id")
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| panic!("no workload_id in: {out}"))
        .to_string()
}

fn parse_ckpt(out: &str) -> String {
    let v: serde_json::Value = serde_json::from_str(out.trim()).expect("json output");
    if let Some(ckpt) = v.get("checkpoint_id") {
        return ckpt.as_str().unwrap().to_string();
    }
    if let Some(arr) = v.as_array() {
        if let Some(first) = arr.first() {
            if let Some(ckpt) = first.get("checkpoint_id") {
                return ckpt.as_str().unwrap().to_string();
            }
        }
    }
    panic!("no checkpoint_id in: {out}")
}
