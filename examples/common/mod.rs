//! Shared helpers for the runnable examples.
//!
//! Each example is a small program that exercises the Checkpoint Fabric library
//! API. Most examples run a coordinator + node in-process (real framed TCP over
//! loopback sockets); example 12 additionally demonstrates real processes.

#![allow(dead_code)]

use std::sync::{Arc, OnceLock};

use checkpoint_fabric::capture::CaptureOptions;
use checkpoint_fabric::checkpoint::ComponentType;
use checkpoint_fabric::coordinator::{Coordinator, CoordinatorConfig, OperationContext};
use checkpoint_fabric::node::{NodeConfig, NodeRuntime};
use checkpoint_fabric::providers::{ApplicationStateProvider, ProviderSpec};
use checkpoint_fabric::workload::{ProtectionSpec, Workload, WorkloadSpec};

/// A running coordinator + node with a captured workload state cell.
pub struct Harness {
    pub coord: Arc<Coordinator>,
    pub node: Arc<NodeRuntime>,
    pub workload: Workload,
    pub ctx: OperationContext,
    /// The application state cell captured and restored by the workload.
    pub cell: Arc<std::sync::Mutex<Vec<u8>>>,
    pub temp: tempfile::TempDir,
}

pub struct HarnessOptions {
    pub single_active: bool,
    pub state: Vec<u8>,
}

impl Default for HarnessOptions {
    fn default() -> Self {
        Self {
            single_active: true,
            state: b"hello-checkpoint-fabric".to_vec(),
        }
    }
}

pub fn temp_dir(tag: &str) -> tempfile::TempDir {
    tempfile::Builder::new()
        .prefix(&format!("checkpoint-fabric-example-{tag}-"))
        .tempdir()
        .expect("temp dir")
}

/// Start an in-process coordinator + node and register a workload with one
/// application-state component. Real sockets are used throughout.
pub fn harness(tag: &str, options: HarnessOptions) -> Harness {
    let tmp = temp_dir(tag);
    let coord_dir = tmp.path().join("coord");
    let node_dir = tmp.path().join("node");
    std::fs::create_dir_all(&coord_dir).unwrap();
    std::fs::create_dir_all(&node_dir).unwrap();

    let cfg = CoordinatorConfig {
        data_dir: coord_dir,
        listen: Some("127.0.0.1:0".parse().unwrap()),
        epoch: Some(1),
        stale_ms: 60_000,
        policy: None,
        failpoints: None,
    };
    let coord = Coordinator::open(cfg).unwrap().start_server().unwrap();

    let node_cfg = NodeConfig::default_in("example", coord.listen_addr().unwrap(), node_dir);
    let node = NodeRuntime::start(node_cfg).unwrap();

    let ctx = OperationContext {
        actor: "example".into(),
        roles: vec!["owner".into(), "operator".into()],
        stale_ms: 60_000,
    };

    let spec = WorkloadSpec {
        workload_id: None,
        owner: "example".into(),
        class: "demo".into(),
        backend_class: "cpu".into(),
        state_schema_version: 1,
        runtime: checkpoint_fabric::compatibility::RuntimeCompatibilityDescriptor::local_default(),
        metadata: serde_json::json!({ "example": tag }),
        protection: ProtectionSpec::default(),
        single_active: options.single_active,
    };
    let workload = coord
        .create_workload(&spec, Some(&node.node_id), &ctx)
        .unwrap();

    let cell = Arc::new(std::sync::Mutex::new(options.state));
    let provider = ApplicationStateProvider::new(
        ProviderSpec {
            component_id: "app".into(),
            component_type: ComponentType::ApplicationState,
            required: true,
            schema_version: 1,
            restore_handler: "example/app".into(),
            compatibility: serde_json::json!({}),
            dependencies: Vec::new(),
        },
        {
            let cell = cell.clone();
            move || Ok(cell.lock().unwrap().clone())
        },
        {
            let cell = cell.clone();
            move |bytes| {
                *cell.lock().unwrap() = bytes.to_vec();
                Ok(())
            }
        },
    );
    // Re-claim the workload with the provider attached.
    coord.fence_workload(&workload.workload_id, &ctx).unwrap();
    let w = coord
        .inspect_workload(&workload.workload_id)
        .unwrap()
        .unwrap();
    node.attach_workload(&w.workload_id, w.execution_epoch, vec![Arc::new(provider)])
        .unwrap();

    Harness {
        coord,
        node,
        workload: w,
        ctx,
        cell,
        temp: tmp,
    }
}

/// Capture options for the single "app" component.
pub fn capture_options() -> CaptureOptions {
    CaptureOptions {
        components: vec![checkpoint_fabric::capture::CaptureComponentRequest {
            component_id: "app".into(),
            component_type: ComponentType::ApplicationState,
            required: true,
            schema_version: 1,
            restore_handler: "example/app".into(),
        }],
        ..CaptureOptions::default()
    }
}

/// Print a section banner.
pub fn banner(title: &str) {
    println!("\n=== {title} ===");
}

/// Run an example function with a banner and a status line.
pub fn run(name: &str, f: impl FnOnce()) {
    println!("checkpoint-fabric example: {name}");
    f();
    println!("checkpoint-fabric example: {name} OK");
}

/// Path to the `checkpointfabric` binary for process-based examples.
///
/// `CARGO_BIN_EXE_checkpointfabric` is set by Cargo in the runtime environment
/// when running examples, not as a compile-time env var, so resolve it lazily.
pub fn bin() -> &'static str {
    static BIN: OnceLock<String> = OnceLock::new();
    BIN.get_or_init(|| {
        std::env::var("CARGO_BIN_EXE_checkpointfabric")
            .unwrap_or_else(|_| "checkpointfabric".to_string())
    })
}
