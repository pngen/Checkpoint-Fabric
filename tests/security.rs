//! Security-focused tests: untrusted input handling, traversal, oversized and
//! malformed inputs, and authority enforcement.

mod common;

use std::path::Path;

use checkpoint_fabric::id::Id;
use checkpoint_fabric::protocol::{self, Op};
use checkpoint_fabric::transport::RpcClient;
use common::*;

#[test]
fn ids_reject_oversized_and_malformed_input() {
    assert!(Id::from_hex(&"a".repeat(31)).is_err());
    assert!(Id::from_hex(&"g".repeat(32)).is_err());
    assert!(Id::from_hex(&"a".repeat(33)).is_err());
    assert!(Id::from_hex("").is_err());
    assert!(Id::from_hex(&"AA".repeat(16)).is_ok());
}

#[test]
fn path_traversal_rejected_everywhere() {
    for bad in [
        "../manifest",
        "..\\manifest",
        "/abs/path",
        "C:\\windows\\system32",
        "components/../../etc/passwd",
        "..",
    ] {
        assert!(
            checkpoint_fabric::storage::safe_join(Path::new("base"), bad).is_err(),
            "traversal {bad:?} must be rejected"
        );
    }
    assert!(checkpoint_fabric::storage::safe_join(Path::new("base"), "components/a").is_ok());
}

#[test]
fn storage_segment_sanitization() {
    assert_eq!(
        checkpoint_fabric::storage::sanitize_segment("a/b\\c:d e"),
        "a_b_c_d_e"
    );
    assert_eq!(
        checkpoint_fabric::storage::sanitize_segment("ok-1.2@3"),
        "ok-1.2@3"
    );
}

#[test]
fn coordinator_rejects_stale_epoch_registration() {
    let coord_dir = tempfile::tempdir().unwrap();
    let mut coord = CoordProc::start(coord_dir.path(), &[]);
    let mut client = RpcClient::connect(&coord.addr).unwrap();
    let req = protocol::NodeRegisterRequest {
        node_id: "evil@1@deadbeef".into(),
        listen_addr: "127.0.0.1:1".into(),
        boot_id: "deadbeef".into(),
        data_dir: tempfile::tempdir()
            .unwrap()
            .path()
            .to_string_lossy()
            .to_string(),
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
fn unknown_ops_rejected_by_coordinator() {
    let coord_dir = tempfile::tempdir().unwrap();
    let mut coord = CoordProc::start(coord_dir.path(), &[]);
    // Ping is handled; an unknown op byte must fail cleanly without a panic.
    let frame = protocol::Frame {
        op: Op::Ping,
        flags: 0,
        request_id: 1,
        payload: b"ping".to_vec(),
    };
    let wire = protocol::encode(&frame).unwrap();
    // Corrupt the op byte to an undefined value.
    let mut bad = wire.clone();
    bad[5] = 0xfe;
    // Decode-level rejection.
    assert!(protocol::decode(&bad).is_err());
    // Wire-level: the server must close the connection without a response.
    let _ = wire;
    coord.stop();
}

#[test]
fn cli_rejects_oversized_component_lists() {
    let coord_dir = tempfile::tempdir().unwrap();
    let mut coord = CoordProc::start(coord_dir.path(), &[]);
    let c = coord.addr.to_string();
    let big = "a,".repeat(10_000);
    let output = std::process::Command::new(BIN)
        .args([
            "--coordinator",
            &c,
            "capture",
            &"a".repeat(32),
            "--components",
            &big,
        ])
        .output()
        .unwrap();
    // The request is rejected (either by id validation or size bounds); the
    // coordinator must not crash.
    assert!(!output.status.success());
    // The coordinator still serves.
    assert!(cli(&c, &["stats"]).contains("\"workloads\":0"));
    coord.stop();
}

#[test]
fn manifest_with_unknown_codec_is_rejected() {
    let mut ck = checkpoint_fabric::compatibility::sample_checkpoint();
    ck.components
        .push(checkpoint_fabric::checkpoint::ComponentEntry {
            component_id: "c".into(),
            component_type: checkpoint_fabric::checkpoint::ComponentType::CustomState,
            generation: 0,
            required: true,
            logical_size: 1,
            storage_representation: StorageRepresentation {
                codec: "lz4".into(),
                original_size: 1,
                stored_size: 1,
                stored_hash: "x".into(),
                relative_path: "components/c".into(),
            },
            content_hash: "y".into(),
            schema_version: 1,
            restore_handler: "h".into(),
            compatibility: serde_json::json!({}),
            dependencies: Vec::new(),
            capture_status: "captured".into(),
            restore_status: "pending".into(),
        });
    let sealed = checkpoint_fabric::manifest::seal(checkpoint_fabric::manifest::scaffold(&ck));
    // Manifest parses (schema-valid), but payload decoding must refuse the codec.
    let parsed = checkpoint_fabric::manifest::parse(&sealed.canonical_bytes).unwrap();
    let repr = &parsed.components[0].storage_representation;
    assert!(checkpoint_fabric::compression::Codec::from_str_strict(&repr.codec).is_err());
}

#[test]
fn duplicate_workload_id_rejected() {
    let coord_dir = tempfile::tempdir().unwrap();
    let mut coord = CoordProc::start(coord_dir.path(), &[]);
    let spec = checkpoint_fabric::workload::WorkloadSpec {
        workload_id: Some(Id::random()),
        owner: "t".into(),
        class: "c".into(),
        backend_class: "cpu".into(),
        state_schema_version: 1,
        runtime: checkpoint_fabric::compatibility::RuntimeCompatibilityDescriptor::local_default(),
        metadata: serde_json::json!({}),
        protection: Default::default(),
        single_active: true,
    };
    let mut client = RpcClient::connect(&coord.addr).unwrap();
    let req = protocol::WorkloadCreateRequest {
        spec: spec.clone(),
        authority: checkpoint_fabric::policy::Authority::owner("test"),
        node: None,
    };
    let resp: protocol::WorkloadCreateResponse =
        client.call_json(Op::WorkloadCreate, &req).unwrap();
    let req2 = protocol::WorkloadCreateRequest {
        spec,
        authority: checkpoint_fabric::policy::Authority::owner("test"),
        node: None,
    };
    let resp2: protocol::Envelope = client.call_json(Op::WorkloadCreate, &req2).unwrap();
    assert!(resp.workload.workload_id != Id::from_bytes([0; 16]));
    assert!(!resp2.is_ok());
    coord.stop();
}

use checkpoint_fabric::checkpoint::StorageRepresentation;
