//! Implementation of CLI commands. Kept separate so `cli.rs` stays declarative.
use std::net::SocketAddr;

use clap::Parser;

use crate::audit::AuditResult;
use crate::capture::{CaptureComponentRequest, CaptureOptions, QuiescenceMode};
use crate::checkpoint::{ComponentType, ConsistencyClass};
use crate::cli::{CheckpointCmd, Cli, Command, CoordinatorCmd, NodeCmd, WorkloadCmd};
use crate::compatibility::RuntimeCompatibilityDescriptor;
use crate::errors::{FabricError, FabricResult};
use crate::policy::{Authority, PolicySet};
use crate::protocol::{self, Op};
use crate::transport::RpcClient;
use crate::workload::{ProtectionSpec, WorkloadSpec};

pub fn run_impl(cli: &Cli) -> anyhow::Result<()> {
    let coord_addr: SocketAddr = cli
        .coordinator
        .parse()
        .map_err(|e| anyhow::anyhow!("bad coordinator address '{}': {e}", cli.coordinator))?;
    let json = cli.json;
    match &cli.command {
        Command::Coordinator { cmd } => match cmd {
            CoordinatorCmd::Start {
                data_dir,
                listen,
                epoch,
                policy_file,
                stale_ms,
            } => run_coordinator_start(data_dir, listen, *epoch, policy_file.as_deref(), *stale_ms),
            CoordinatorCmd::Stop => {
                let mut c = RpcClient::connect(&coord_addr)?;
                c.call(
                    Op::CoordinatorShutdown,
                    &serde_json::to_vec(&protocol::Envelope::ok())?,
                )?;
                println!("shutdown requested");
                Ok(())
            }
        },
        Command::Node { cmd } => match cmd {
            NodeCmd::Start {
                name,
                data_dir,
                coordinator,
                listen,
                heartbeat_ms,
            } => run_node_start(name, data_dir, coordinator, listen, *heartbeat_ms),
        },
        Command::Workload { cmd } => {
            let mut c = RpcClient::connect(&coord_addr)?;
            match cmd {
                WorkloadCmd::Create {
                    owner,
                    class,
                    backend,
                    schema,
                    single_active,
                    node,
                } => {
                    let spec = WorkloadSpec {
                        workload_id: None,
                        owner: owner.clone(),
                        class: class.clone(),
                        backend_class: backend.clone(),
                        state_schema_version: *schema,
                        runtime: RuntimeCompatibilityDescriptor::local_default(),
                        metadata: serde_json::json!({}),
                        protection: ProtectionSpec::default(),
                        single_active: *single_active,
                    };
                    let req = protocol::WorkloadCreateRequest {
                        spec,
                        authority: Authority::owner("cli"),
                        node: node.clone(),
                    };
                    let resp: protocol::WorkloadCreateResponse =
                        c.call_json(Op::WorkloadCreate, &req)?;
                    print_value(json, &resp.workload);
                }
                WorkloadCmd::Inspect { workload_id } => {
                    let req = protocol::WorkloadIdRequest {
                        workload_id: crate::id::Id::from_hex(workload_id)?,
                        authority: Authority::operator("cli"),
                    };
                    let payload = c.call(Op::WorkloadInspect, &serde_json::to_vec(&req)?)?;
                    emit(&payload, json)?;
                }
                WorkloadCmd::List => {
                    let payload = c.call(Op::WorkloadList, b"{}")?;
                    emit(&payload, json)?;
                }
                WorkloadCmd::Fence { workload_id } => {
                    let req = protocol::WorkloadIdRequest {
                        workload_id: crate::id::Id::from_hex(workload_id)?,
                        authority: Authority::operator("cli"),
                    };
                    let payload = c.call(Op::WorkloadFence, &serde_json::to_vec(&req)?)?;
                    emit(&payload, json)?;
                }
                WorkloadCmd::Lineage { workload_id } => {
                    let req = protocol::WorkloadIdRequest {
                        workload_id: crate::id::Id::from_hex(workload_id)?,
                        authority: Authority::operator("cli"),
                    };
                    let payload = c.call(Op::WorkloadLineage, &serde_json::to_vec(&req)?)?;
                    emit(&payload, json)?;
                }
            }
            Ok(())
        }
        Command::Capture {
            workload_id,
            consistency,
            quiescence,
            components,
            metadata,
        } => {
            let mut c = RpcClient::connect(&coord_addr)?;
            let consistency = match consistency.as_deref() {
                None => None,
                Some("crash") | Some("CRASH_CONSISTENT") => Some(ConsistencyClass::CrashConsistent),
                Some("application") | Some("APPLICATION_CONSISTENT") => {
                    Some(ConsistencyClass::ApplicationConsistent)
                }
                Some("execution") | Some("EXECUTION_CONSISTENT") => {
                    Some(ConsistencyClass::ExecutionConsistent)
                }
                Some(other) => return Err(anyhow::anyhow!("unknown consistency class '{other}'")),
            };
            let quiescence = match quiescence.to_lowercase().as_str() {
                "cooperative" => QuiescenceMode::Cooperative,
                "forced" => QuiescenceMode::Forced,
                "none" => QuiescenceMode::None,
                other => return Err(anyhow::anyhow!("unknown quiescence mode '{other}'")),
            };
            let component_ids: Vec<String> = components
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            let comp_reqs: Vec<CaptureComponentRequest> = component_ids
                .into_iter()
                .map(|cid| CaptureComponentRequest {
                    component_id: cid,
                    component_type: ComponentType::CustomState,
                    required: true,
                    schema_version: 1,
                    restore_handler: "cli".into(),
                })
                .collect();
            let metadata_value = match metadata {
                Some(m) => serde_json::from_str(m)?,
                None => serde_json::json!({}),
            };
            let options = CaptureOptions {
                consistency,
                quiescence,
                components: comp_reqs,
                metadata: metadata_value,
                ..Default::default()
            };
            let req = protocol::CaptureRequest {
                workload_id: crate::id::Id::from_hex(workload_id)?,
                options,
                authority: Authority::owner("cli"),
            };
            let payload = c.call(Op::Capture, &serde_json::to_vec(&req)?)?;
            emit(&payload, json)?;
            Ok(())
        }
        Command::CaptureStatus { attempt_id } => {
            let mut c = RpcClient::connect(&coord_addr)?;
            let req = protocol::AttemptStatusRequest {
                attempt_id: attempt_id.clone(),
            };
            let payload = c.call(Op::CaptureStatus, &serde_json::to_vec(&req)?)?;
            emit(&payload, json)?;
            Ok(())
        }
        Command::Checkpoint { cmd } => {
            let mut c = RpcClient::connect(&coord_addr)?;
            match cmd {
                CheckpointCmd::Inspect { checkpoint_id } => {
                    let req = protocol::CheckpointIdRequest {
                        checkpoint_id: crate::id::Id::from_hex(checkpoint_id)?,
                        authority: Authority::operator("cli"),
                    };
                    let payload = c.call(Op::CheckpointInspect, &serde_json::to_vec(&req)?)?;
                    emit(&payload, json)?;
                }
                CheckpointCmd::List { workload_id } => {
                    let req = protocol::CheckpointListRequest {
                        workload_id: workload_id
                            .as_deref()
                            .map(crate::id::Id::from_hex)
                            .transpose()?,
                    };
                    let payload = c.call(Op::CheckpointList, &serde_json::to_vec(&req)?)?;
                    emit(&payload, json)?;
                }
                CheckpointCmd::Verify { checkpoint_id } => {
                    let req = protocol::CheckpointIdRequest {
                        checkpoint_id: crate::id::Id::from_hex(checkpoint_id)?,
                        authority: Authority::operator("cli"),
                    };
                    let payload = c.call(Op::CheckpointVerify, &serde_json::to_vec(&req)?)?;
                    emit(&payload, json)?;
                }
                CheckpointCmd::Protect { checkpoint_id } => {
                    let req = protocol::CheckpointIdRequest {
                        checkpoint_id: crate::id::Id::from_hex(checkpoint_id)?,
                        authority: Authority::operator("cli"),
                    };
                    expect_ok(&mut c, Op::CheckpointProtect, &req)?;
                    println!("protected");
                }
                CheckpointCmd::Pin { checkpoint_id } => {
                    let req = protocol::CheckpointIdRequest {
                        checkpoint_id: crate::id::Id::from_hex(checkpoint_id)?,
                        authority: Authority::operator("cli"),
                    };
                    expect_ok(&mut c, Op::CheckpointProtect, &req)?;
                    println!("pinned");
                }
                CheckpointCmd::Unprotect { checkpoint_id } => {
                    let req = protocol::CheckpointIdRequest {
                        checkpoint_id: crate::id::Id::from_hex(checkpoint_id)?,
                        authority: Authority::operator("cli"),
                    };
                    expect_ok(&mut c, Op::CheckpointUnprotect, &req)?;
                    println!("unprotected");
                }
                CheckpointCmd::Retire { checkpoint_id } => {
                    let req = protocol::CheckpointIdRequest {
                        checkpoint_id: crate::id::Id::from_hex(checkpoint_id)?,
                        authority: Authority::operator("cli"),
                    };
                    expect_ok(&mut c, Op::CheckpointRetire, &req)?;
                    println!("retired");
                }
                CheckpointCmd::Lineage { checkpoint_id } => {
                    let req = protocol::CheckpointIdRequest {
                        checkpoint_id: crate::id::Id::from_hex(checkpoint_id)?,
                        authority: Authority::operator("cli"),
                    };
                    let payload = c.call(Op::CheckpointLineage, &serde_json::to_vec(&req)?)?;
                    emit(&payload, json)?;
                }
            }
            Ok(())
        }
        Command::Restore {
            checkpoint_id,
            node,
            no_resume,
            force,
        } => {
            let mut c = RpcClient::connect(&coord_addr)?;
            let options = crate::restore::RestoreOptions {
                resume: !no_resume,
                migration: *force,
                ..Default::default()
            };
            let req = protocol::RestoreRequest {
                checkpoint_id: crate::id::Id::from_hex(checkpoint_id)?,
                node: node.clone(),
                options,
                authority: Authority::operator("cli"),
            };
            let payload = c.call(Op::Restore, &serde_json::to_vec(&req)?)?;
            emit(&payload, json)?;
            Ok(())
        }
        Command::Rollback {
            checkpoint_id,
            node,
        } => {
            let mut c = RpcClient::connect(&coord_addr)?;
            let options = crate::restore::RestoreOptions {
                rollback: true,
                resume: true,
                ..Default::default()
            };
            let req = protocol::RestoreRequest {
                checkpoint_id: crate::id::Id::from_hex(checkpoint_id)?,
                node: node.clone(),
                options,
                authority: Authority::operator("cli"),
            };
            let payload = c.call(Op::Rollback, &serde_json::to_vec(&req)?)?;
            emit(&payload, json)?;
            Ok(())
        }
        Command::Fork {
            checkpoint_id,
            owner,
            class,
            backend,
            single_active,
        } => {
            let mut c = RpcClient::connect(&coord_addr)?;
            let spec = WorkloadSpec {
                workload_id: None,
                owner: owner.clone(),
                class: class.clone(),
                backend_class: backend.clone(),
                state_schema_version: 1,
                runtime: RuntimeCompatibilityDescriptor::local_default(),
                metadata: serde_json::json!({}),
                protection: ProtectionSpec::default(),
                single_active: *single_active,
            };
            let req = protocol::ForkRequest {
                checkpoint_id: crate::id::Id::from_hex(checkpoint_id)?,
                spec,
                authority: Authority::owner("cli"),
            };
            let resp: protocol::ForkResponse = c.call_json(Op::Fork, &req)?;
            print_value(json, &resp.workload);
            Ok(())
        }
        Command::Migrate {
            checkpoint_id,
            node,
        } => {
            let mut c = RpcClient::connect(&coord_addr)?;
            let req = protocol::MigrateRequest {
                checkpoint_id: crate::id::Id::from_hex(checkpoint_id)?,
                target_node: node.clone(),
                authority: Authority::operator("cli"),
            };
            let payload = c.call(Op::Migrate, &serde_json::to_vec(&req)?)?;
            emit(&payload, json)?;
            Ok(())
        }
        Command::Compatibility {
            checkpoint_id,
            os,
            arch,
            backend,
        } => {
            let mut c = RpcClient::connect(&coord_addr)?;
            let mut target = RuntimeCompatibilityDescriptor::local_default();
            if let Some(os) = os {
                target.os = os.clone();
            }
            if let Some(arch) = arch {
                target.arch = arch.clone();
            }
            if let Some(backend) = backend {
                target.backend_class = backend.clone();
            }
            let req = protocol::CompatibilityRequest {
                checkpoint_id: crate::id::Id::from_hex(checkpoint_id)?,
                target,
                authority: Authority::operator("cli"),
            };
            let payload = c.call(Op::Compatibility, &serde_json::to_vec(&req)?)?;
            emit(&payload, json)?;
            Ok(())
        }
        Command::Audit { since_ms, limit } => {
            let mut c = RpcClient::connect(&coord_addr)?;
            let req = protocol::AuditRequest {
                since_ms: *since_ms,
                limit: *limit,
                authority: Authority::operator("cli"),
            };
            let payload = c.call(Op::Audit, &serde_json::to_vec(&req)?)?;
            emit(&payload, json)?;
            Ok(())
        }
        Command::Recovery { dry_run } => {
            let mut c = RpcClient::connect(&coord_addr)?;
            let req = protocol::RecoveryRequest {
                dry_run: *dry_run,
                authority: Authority::operator("cli"),
            };
            let payload = c.call(Op::Recovery, &serde_json::to_vec(&req)?)?;
            emit(&payload, json)?;
            Ok(())
        }
        Command::Stats => {
            let mut c = RpcClient::connect(&coord_addr)?;
            let payload = c.call(Op::Stats, b"{}")?;
            emit(&payload, json)?;
            Ok(())
        }
        Command::Nodes => {
            let mut c = RpcClient::connect(&coord_addr)?;
            let payload = c.call(Op::NodeList, b"{}")?;
            emit(&payload, json)?;
            Ok(())
        }
    }
}

fn run_coordinator_start(
    data_dir: &str,
    listen: &str,
    epoch: Option<u64>,
    policy_file: Option<&str>,
    stale_ms: Option<u64>,
) -> anyhow::Result<()> {
    let policy = match policy_file {
        Some(path) => {
            let text = std::fs::read_to_string(path)?;
            Some(PolicySet::from_json(&text)?)
        }
        None => None,
    };
    let cfg = crate::coordinator::CoordinatorConfig {
        data_dir: std::path::PathBuf::from(data_dir),
        listen: Some(listen.parse()?),
        epoch,
        stale_ms: stale_ms.unwrap_or(crate::coordinator::DEFAULT_STALE_MS),
        policy,
        failpoints: std::env::var("CF_FAILPOINT").ok(),
    };
    let coordinator = crate::coordinator::Coordinator::open(cfg)?.start_server()?;
    let addr = coordinator.listen_addr().unwrap();
    eprintln!(
        "coordinator listening on {addr} (epoch {})",
        coordinator.epoch
    );
    let stop = coordinator.stop_flag();
    let handler_stop = stop.clone();
    ctrlc::set_handler(move || {
        handler_stop.store(true, std::sync::atomic::Ordering::SeqCst);
    })
    .map_err(|e| anyhow::anyhow!("ctrlc: {e}"))?;
    // Wait until shutdown is requested, checking periodically.
    while !stop.load(std::sync::atomic::Ordering::SeqCst) {
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
    coordinator.shutdown();
    eprintln!("coordinator stopped");
    Ok(())
}

fn run_node_start(
    name: &str,
    data_dir: &str,
    coordinator: &str,
    listen: &str,
    heartbeat_ms: Option<u64>,
) -> anyhow::Result<()> {
    let cfg = crate::node::NodeConfig {
        name: name.to_string(),
        coordinator_addr: coordinator.parse()?,
        listen_addr: listen.parse()?,
        data_dir: std::path::PathBuf::from(data_dir),
        heartbeat_ms: heartbeat_ms.unwrap_or(1_000),
        max_connections: 16,
        staging_ttl_ms: 3_600_000,
        runtime: RuntimeCompatibilityDescriptor::local_default(),
        hardware: crate::checkpoint::HardwareCompatibilityDescriptor::default(),
        resources: serde_json::json!({}),
        failpoints: std::env::var("CF_FAILPOINT").ok(),
    };
    let node = crate::node::NodeRuntime::start(cfg)?;
    eprintln!(
        "node {} listening on {:?}",
        node.node_id,
        node.listen_addr().unwrap()
    );

    let stop = node.stop_flag();
    let handler_stop = stop.clone();
    ctrlc::set_handler(move || {
        handler_stop.store(true, std::sync::atomic::Ordering::SeqCst);
    })
    .map_err(|e| anyhow::anyhow!("ctrlc: {e}"))?;
    while !stop.load(std::sync::atomic::Ordering::SeqCst) {
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
    node.shutdown();
    eprintln!("node stopped");
    Ok(())
}

fn emit(payload: &[u8], json: bool) -> anyhow::Result<()> {
    if let Ok(envelope) = serde_json::from_slice::<protocol::Envelope>(payload) {
        if !envelope.is_ok() {
            let msg = envelope
                .error
                .clone()
                .unwrap_or_else(|| "unknown error".into());
            if json {
                println!("{}", serde_json::to_string_pretty(&envelope)?);
            } else {
                eprintln!("error: {msg}");
            }
            std::process::exit(1);
        }
    }
    let value: serde_json::Value = serde_json::from_slice(payload)?;
    print_value(json, &value);
    Ok(())
}

/// Call an op and fail with the coordinator's error if the envelope says so.
fn expect_ok<T: serde::Serialize>(
    client: &mut RpcClient,
    op: Op,
    payload: &T,
) -> anyhow::Result<()> {
    let response = client.call(op, &serde_json::to_vec(payload)?)?;
    if let Ok(envelope) = serde_json::from_slice::<protocol::Envelope>(&response) {
        if !envelope.is_ok() {
            anyhow::bail!(
                "{}",
                envelope.error.unwrap_or_else(|| "unknown error".into())
            );
        }
    }
    Ok(())
}

fn print_value(json: bool, value: &impl serde::Serialize) {
    if json {
        if let Ok(s) = serde_json::to_string_pretty(value) {
            println!("{s}");
        }
    } else if let Ok(s) = serde_json::to_string(value) {
        println!("{s}");
    }
}

#[allow(dead_code)]
fn _audit_result_used() {
    let _ = AuditResult::Ok;
}

#[allow(dead_code)]
fn _fabric_error_marker() -> FabricResult<()> {
    Err(FabricError::Internal("unused".into()))
}

/// Parse policy file helper for tests.
pub fn _parse_cli() -> Cli {
    let args: Vec<String> = vec!["checkpointfabric".into(), "stats".into()];
    Cli::parse_from(args)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_parses() {
        let cli: Cli = Cli::parse_from(["checkpointfabric", "--json", "stats"]);
        assert!(cli.json);
        assert!(matches!(cli.command, Command::Stats));
    }

    #[test]
    fn cli_parses_capture() {
        let cli: Cli = Cli::parse_from([
            "checkpointfabric",
            "capture",
            "abc",
            "--consistency",
            "execution",
            "--quiescence",
            "cooperative",
            "--components",
            "a,b",
        ]);
        match cli.command {
            Command::Capture {
                workload_id,
                consistency,
                quiescence,
                components,
                ..
            } => {
                assert_eq!(workload_id, "abc");
                assert_eq!(consistency.as_deref(), Some("execution"));
                assert_eq!(quiescence, "cooperative");
                assert_eq!(components, "a,b");
            }
            _ => panic!("expected capture"),
        }
    }
}
