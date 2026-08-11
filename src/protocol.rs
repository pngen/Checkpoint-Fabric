//! Wire protocol definitions.
//!
//! Framing: magic + version + op + flags + request id + payload length + CRC-32C
//! over header and payload, then the payload. All lengths are bounded; malformed,
//! truncated, and oversized frames are rejected and close the connection.

use serde::{Deserialize, Serialize};

use crate::errors::{FabricError, FabricResult};
use crate::integrity;

pub const MAGIC: &[u8; 4] = b"CFAB";
pub const PROTOCOL_VERSION: u8 = 1;
pub const MAX_FRAME_PAYLOAD: usize = 16 * 1024 * 1024;
pub const HEADER_LEN: usize = 24;
pub const FLAG_RESPONSE: u8 = 0x01;
pub const FLAG_CLOSE: u8 = 0x02;

/// Operation codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum Op {
    Ping = 1,
    Pong = 2,
    Hello = 3,
    HelloAck = 4,
    WorkloadCreate = 10,
    WorkloadInspect = 11,
    WorkloadList = 12,
    WorkloadFence = 13,
    WorkloadLineage = 14,
    WorkloadAttach = 15,
    WorkloadDetach = 16,
    Capture = 20,
    CaptureStatus = 21,
    CheckpointInspect = 22,
    CheckpointList = 23,
    CheckpointVerify = 24,
    CheckpointProtect = 25,
    CheckpointUnprotect = 26,
    CheckpointRetire = 27,
    CheckpointLineage = 28,
    Restore = 30,
    Rollback = 31,
    Fork = 32,
    Migrate = 33,
    Compatibility = 40,
    Audit = 41,
    Recovery = 42,
    Stats = 43,
    NodeList = 44,
    NodeInfo = 45,
    CoordinatorShutdown = 46,
    NodeRegister = 50,
    NodeRegisterAck = 51,
    NodeHeartbeat = 52,
    NodeHeartbeatAck = 53,
    NodeCaptureResult = 54,
    NodeRestoreResult = 55,
    NodeVerifyResult = 56,
    NodeCleanupResult = 57,
    NodeReplicateResult = 58,
    NodeCaptureRequest = 70,
    NodeRestoreRequest = 71,
    NodeReplicateRequest = 72,
    NodeCleanupRequest = 73,
    NodeVerifyRequest = 74,
    NodeDetachRequest = 75,
    NodeAllowFetch = 76,
    NodePromoteRequest = 77,
    NodePromoteResult = 78,
    NodeResumeRequest = 79,
    NodeResumeResult = 80,
    FetchManifest = 90,
    FetchComponent = 91,
    FetchChunkResponse = 92,
    NodeProbeRequest = 93,
    NodeProbeResult = 94,
}

impl Op {
    pub fn from_u8(v: u8) -> Option<Self> {
        use Op::*;
        Some(match v {
            1 => Ping,
            2 => Pong,
            3 => Hello,
            4 => HelloAck,
            10 => WorkloadCreate,
            11 => WorkloadInspect,
            12 => WorkloadList,
            13 => WorkloadFence,
            14 => WorkloadLineage,
            15 => WorkloadAttach,
            16 => WorkloadDetach,
            20 => Capture,
            21 => CaptureStatus,
            22 => CheckpointInspect,
            23 => CheckpointList,
            24 => CheckpointVerify,
            25 => CheckpointProtect,
            26 => CheckpointUnprotect,
            27 => CheckpointRetire,
            28 => CheckpointLineage,
            30 => Restore,
            31 => Rollback,
            32 => Fork,
            33 => Migrate,
            40 => Compatibility,
            41 => Audit,
            42 => Recovery,
            43 => Stats,
            44 => NodeList,
            45 => NodeInfo,
            46 => CoordinatorShutdown,
            50 => NodeRegister,
            51 => NodeRegisterAck,
            52 => NodeHeartbeat,
            53 => NodeHeartbeatAck,
            54 => NodeCaptureResult,
            55 => NodeRestoreResult,
            56 => NodeVerifyResult,
            57 => NodeCleanupResult,
            58 => NodeReplicateResult,
            70 => NodeCaptureRequest,
            71 => NodeRestoreRequest,
            72 => NodeReplicateRequest,
            73 => NodeCleanupRequest,
            74 => NodeVerifyRequest,
            75 => NodeDetachRequest,
            76 => NodeAllowFetch,
            77 => NodePromoteRequest,
            78 => NodePromoteResult,
            79 => NodeResumeRequest,
            80 => NodeResumeResult,
            90 => FetchManifest,
            91 => FetchComponent,
            92 => FetchChunkResponse,
            93 => NodeProbeRequest,
            94 => NodeProbeResult,
            _ => return None,
        })
    }
}

/// A decoded frame.
#[derive(Debug, Clone)]
pub struct Frame {
    pub op: Op,
    pub flags: u8,
    pub request_id: u64,
    pub payload: Vec<u8>,
}

impl Frame {
    pub fn is_response(&self) -> bool {
        self.flags & FLAG_RESPONSE != 0
    }

    pub fn is_close(&self) -> bool {
        self.flags & FLAG_CLOSE != 0
    }

    pub fn response(op: Op, request_id: u64, payload: Vec<u8>) -> Self {
        Self {
            op,
            flags: FLAG_RESPONSE,
            request_id,
            payload,
        }
    }
}

/// Encode a frame into wire bytes.
pub fn encode(frame: &Frame) -> FabricResult<Vec<u8>> {
    if frame.payload.len() > MAX_FRAME_PAYLOAD {
        return Err(FabricError::ProtocolError(format!(
            "frame payload {} exceeds maximum {}",
            frame.payload.len(),
            MAX_FRAME_PAYLOAD
        )));
    }
    let mut out = Vec::with_capacity(HEADER_LEN + frame.payload.len());
    out.extend_from_slice(MAGIC);
    out.push(PROTOCOL_VERSION);
    out.push(frame.op as u8);
    out.push(frame.flags);
    out.push(0);
    out.extend_from_slice(&frame.request_id.to_be_bytes());
    out.extend_from_slice(&(frame.payload.len() as u32).to_be_bytes());
    // CRC-32C over header (minus the crc field itself) and payload.
    let mut digest = integrity::crc_digest();
    digest.update(&out);
    digest.update(&frame.payload);
    out.extend_from_slice(&digest.finalize().to_be_bytes());
    out.extend_from_slice(&frame.payload);
    Ok(out)
}

/// Decode a frame from a full buffer of at least `HEADER_LEN` bytes.
/// Returns the frame and the number of bytes consumed (may be less than buffer
/// length if the buffer contains multiple frames or a partial tail).
pub fn decode(buf: &[u8]) -> FabricResult<Option<(Frame, usize)>> {
    if buf.len() < HEADER_LEN {
        return Ok(None);
    }
    if &buf[0..4] != MAGIC {
        return Err(FabricError::ProtocolError("bad frame magic".into()));
    }
    if buf[4] != PROTOCOL_VERSION {
        return Err(FabricError::ProtocolError(format!(
            "unsupported protocol version {}",
            buf[4]
        )));
    }
    let op = Op::from_u8(buf[5])
        .ok_or_else(|| FabricError::ProtocolError(format!("unknown op {}", buf[5])))?;
    let flags = buf[6];
    let request_id = u64::from_be_bytes(buf[8..16].try_into().unwrap());
    let payload_len = u32::from_be_bytes(buf[16..20].try_into().unwrap()) as usize;
    if payload_len > MAX_FRAME_PAYLOAD {
        return Err(FabricError::ProtocolError(format!(
            "frame payload length {payload_len} exceeds maximum {MAX_FRAME_PAYLOAD}"
        )));
    }
    let total = HEADER_LEN + payload_len;
    if buf.len() < total {
        return Ok(None);
    }
    let expected_crc = u32::from_be_bytes(buf[20..24].try_into().unwrap());
    let mut digest = integrity::crc_digest();
    digest.update(&buf[..20]);
    digest.update(&buf[24..total]);
    if digest.finalize() != expected_crc {
        return Err(FabricError::ProtocolError("frame checksum mismatch".into()));
    }
    Ok(Some((
        Frame {
            op,
            flags,
            request_id,
            payload: buf[24..total].to_vec(),
        },
        total,
    )))
}

/// Response envelope: status code and optional error.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub status: u16,
    pub error: Option<String>,
}

impl Envelope {
    pub fn ok() -> Self {
        Self {
            status: 0,
            error: None,
        }
    }

    pub fn err(msg: &str) -> Self {
        Self {
            status: 1,
            error: Some(msg.to_string()),
        }
    }

    pub fn is_ok(&self) -> bool {
        self.status == 0
    }
}

/// Client-originated request payloads.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HelloRequest {
    pub name: String,
    pub protocol_version: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadCreateRequest {
    pub spec: crate::workload::WorkloadSpec,
    pub authority: crate::policy::Authority,
    pub node: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadCreateResponse {
    pub workload: crate::workload::Workload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadIdRequest {
    pub workload_id: crate::id::Id,
    pub authority: crate::policy::Authority,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaptureRequest {
    pub workload_id: crate::id::Id,
    pub options: crate::capture::CaptureOptions,
    pub authority: crate::policy::Authority,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaptureResponse {
    pub attempt_id: String,
    pub checkpoint_id: crate::id::Id,
    pub checkpoint_generation: u64,
    pub node: String,
    pub consistency: crate::checkpoint::ConsistencyClass,
    pub resumability: crate::checkpoint::ResumabilityClass,
    pub state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttemptStatusRequest {
    pub attempt_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttemptStatusResponse {
    pub attempt: crate::capture::AttemptRecord,
    pub checkpoint_id: Option<crate::id::Id>,
    pub state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointIdRequest {
    pub checkpoint_id: crate::id::Id,
    pub authority: crate::policy::Authority,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointListRequest {
    pub workload_id: Option<crate::id::Id>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreRequest {
    pub checkpoint_id: crate::id::Id,
    pub node: String,
    pub options: crate::restore::RestoreOptions,
    pub authority: crate::policy::Authority,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreResponse {
    pub attempt_id: String,
    pub checkpoint_id: crate::id::Id,
    pub workload_generation: u64,
    pub execution_epoch: u64,
    pub resumability: crate::checkpoint::ResumabilityClass,
    pub state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForkRequest {
    pub checkpoint_id: crate::id::Id,
    pub spec: crate::workload::WorkloadSpec,
    pub authority: crate::policy::Authority,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForkResponse {
    pub workload: crate::workload::Workload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrateRequest {
    pub checkpoint_id: crate::id::Id,
    pub target_node: String,
    pub authority: crate::policy::Authority,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompatibilityRequest {
    pub checkpoint_id: crate::id::Id,
    pub target: crate::compatibility::RuntimeCompatibilityDescriptor,
    pub authority: crate::policy::Authority,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditRequest {
    pub since_ms: Option<u64>,
    pub limit: usize,
    pub authority: crate::policy::Authority,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryRequest {
    pub dry_run: bool,
    pub authority: crate::policy::Authority,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRegisterRequest {
    pub node_id: String,
    pub listen_addr: String,
    pub boot_id: String,
    pub data_dir: String,
    pub runtime: crate::compatibility::RuntimeCompatibilityDescriptor,
    pub hardware: crate::checkpoint::HardwareCompatibilityDescriptor,
    pub resources: serde_json::Value,
    /// Checkpoints discovered in this node's durable storage on boot. The
    /// coordinator uses these ids to rebind locations after a node restart.
    pub committed_checkpoints: Vec<crate::id::Id>,
    pub coordinator_epoch: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRegisterResponse {
    pub accepted: bool,
    pub coordinator_epoch: u64,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeHeartbeatRequest {
    pub node_id: String,
    pub boot_id: String,
    pub coordinator_epoch: u64,
    pub workloads: Vec<HeartbeatWorkload>,
    pub resources: serde_json::Value,
    pub committed_checkpoints: Vec<crate::id::Id>,
    /// Restore-handler -> schema version map of providers hosted on this node.
    pub provider_versions: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatWorkload {
    pub workload_id: crate::id::Id,
    pub fence_token: String,
    pub execution_epoch: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeHeartbeatResponse {
    pub ok: bool,
    pub stale_workloads: Vec<crate::id::Id>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeCaptureRequest {
    pub attempt_id: String,
    pub checkpoint_id: crate::id::Id,
    pub workload_id: crate::id::Id,
    pub checkpoint_generation: u64,
    pub consistency: crate::checkpoint::ConsistencyClass,
    pub quiescence: crate::capture::QuiescenceMode,
    pub components: Vec<crate::capture::CaptureComponentRequest>,
    pub compression: crate::compression::CompressionSpec,
    pub coordinator_epoch: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeCaptureResult {
    pub attempt_id: String,
    pub ok: bool,
    pub error: Option<String>,
    pub components: Vec<crate::checkpoint::ComponentEntry>,
    pub total_logical_bytes: u64,
    pub total_physical_bytes: u64,
    pub compressed_bytes: u64,
    /// Whether all providers acknowledged quiescence cooperatively.
    pub cooperative_ack: bool,
    /// Node-side path of the staged checkpoint directory.
    pub staging_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRestoreRequest {
    pub attempt_id: String,
    pub checkpoint_id: crate::id::Id,
    pub workload_id: crate::id::Id,
    /// Coordinator-held integrity anchor. A locally self-consistent replacement
    /// manifest must not be accepted unless it matches this digest.
    pub expected_manifest_digest: String,
    pub max_component_bytes: Option<u64>,
    pub coordinator_epoch: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeRestoreResult {
    pub attempt_id: String,
    pub ok: bool,
    pub error: Option<String>,
    pub restored_components: Vec<String>,
}

/// Asks a node whether it holds a local replica of a checkpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeProbeRequest {
    pub checkpoint_id: crate::id::Id,
    pub coordinator_epoch: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeProbeResult {
    pub checkpoint_id: crate::id::Id,
    pub has_replica: bool,
}

/// Coordinator asks the node to write the sealed manifest and promote the
/// staged checkpoint into its committed location.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodePromoteRequest {
    pub attempt_id: String,
    pub checkpoint_id: crate::id::Id,
    pub manifest_bytes: Vec<u8>,
    pub digest: String,
    pub integrity_root: String,
    pub coordinator_epoch: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodePromoteResult {
    pub attempt_id: String,
    pub ok: bool,
    pub error: Option<String>,
    pub commit_path: Option<String>,
}

/// Coordinator asks the node to resume the source (capture) or target (restore).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeResumeRequest {
    pub attempt_id: String,
    pub checkpoint_id: crate::id::Id,
    pub workload_id: crate::id::Id,
    /// Present for restore/migration authority transfer; absent when merely
    /// resuming a quiesced capture source.
    pub fence_token: Option<String>,
    pub execution_epoch: Option<u64>,
    /// Whether provider resume hooks should run. Authority is installed even
    /// when this is false.
    pub resume: bool,
    pub coordinator_epoch: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeResumeResult {
    pub attempt_id: String,
    pub ok: bool,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeVerifyRequest {
    pub checkpoint_id: crate::id::Id,
    pub expected_manifest_digest: String,
    pub coordinator_epoch: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeVerifyResult {
    pub checkpoint_id: crate::id::Id,
    pub ok: bool,
    pub error: Option<String>,
    pub manifest_digest: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeCleanupRequest {
    /// Preferred safe form: attempt ids are resolved beneath the node's own
    /// staging root.
    pub staging_attempts: Vec<String>,
    /// Legacy explicit paths remain supported but are canonicalized and must be
    /// direct children of the staging root.
    pub staging_paths: Vec<String>,
    /// Provider state to roll back after a restore abort.
    pub restore_attempts: Vec<RestoreCleanupTarget>,
    pub checkpoint_ids: Vec<crate::id::Id>,
    pub coordinator_epoch: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreCleanupTarget {
    pub attempt_id: String,
    pub checkpoint_id: crate::id::Id,
    pub workload_id: crate::id::Id,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeCleanupResult {
    pub removed_paths: Vec<String>,
    pub removed_checkpoints: Vec<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeReplicateRequest {
    pub attempt_id: String,
    pub checkpoint_id: crate::id::Id,
    pub expected_manifest_digest: String,
    pub from_addr: String,
    pub fetch_token: String,
    pub coordinator_epoch: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeReplicateResult {
    pub attempt_id: String,
    pub ok: bool,
    pub error: Option<String>,
    pub path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeAllowFetchRequest {
    pub checkpoint_id: crate::id::Id,
    pub fetch_token: String,
    pub ttl_ms: u64,
    pub coordinator_epoch: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchManifestRequest {
    pub checkpoint_id: crate::id::Id,
    pub fetch_token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchManifestResponse {
    pub ok: bool,
    pub error: Option<String>,
    pub manifest_bytes: Option<Vec<u8>>,
    pub components: Vec<crate::checkpoint::ComponentEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchComponentRequest {
    pub checkpoint_id: crate::id::Id,
    pub component_id: String,
    pub offset: u64,
    pub fetch_token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchChunkResponse {
    pub ok: bool,
    pub error: Option<String>,
    pub offset: u64,
    pub bytes: Vec<u8>,
    pub last: bool,
}

/// Node attachment request: a node claims a workload with provider specs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadAttachRequest {
    pub workload_id: crate::id::Id,
    pub node: String,
    pub node_boot_id: String,
    pub providers: Vec<crate::providers::ProviderSpec>,
    pub fence_epoch: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadAttachResponse {
    pub accepted: bool,
    pub fence_token: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadDetachRequest {
    pub workload_id: crate::id::Id,
    pub node: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_roundtrip() {
        let f = Frame {
            op: Op::Ping,
            flags: 0,
            request_id: 42,
            payload: b"hello".to_vec(),
        };
        let wire = encode(&f).unwrap();
        let (decoded, consumed) = decode(&wire).unwrap().unwrap();
        assert_eq!(consumed, wire.len());
        assert_eq!(decoded.op, Op::Ping);
        assert_eq!(decoded.request_id, 42);
        assert_eq!(decoded.payload, b"hello");
    }

    #[test]
    fn multi_frame_buffer() {
        let f1 = Frame {
            op: Op::Ping,
            flags: 0,
            request_id: 1,
            payload: b"a".to_vec(),
        };
        let f2 = Frame {
            op: Op::Pong,
            flags: 0,
            request_id: 2,
            payload: b"bb".to_vec(),
        };
        let mut buf = encode(&f1).unwrap();
        buf.extend_from_slice(&encode(&f2).unwrap());
        let (d1, c1) = decode(&buf).unwrap().unwrap();
        assert_eq!(d1.request_id, 1);
        let (d2, c2) = decode(&buf[c1..]).unwrap().unwrap();
        assert_eq!(d2.request_id, 2);
        assert_eq!(c2, buf.len() - c1);
    }

    #[test]
    fn bad_magic_rejected() {
        let f = Frame {
            op: Op::Ping,
            flags: 0,
            request_id: 1,
            payload: vec![],
        };
        let mut wire = encode(&f).unwrap();
        wire[0] = b'X';
        assert!(decode(&wire).is_err());
    }

    #[test]
    fn corrupted_payload_rejected() {
        let f = Frame {
            op: Op::Ping,
            flags: 0,
            request_id: 1,
            payload: b"payload".to_vec(),
        };
        let mut wire = encode(&f).unwrap();
        let len = wire.len();
        wire[len - 1] ^= 0xff;
        assert!(decode(&wire).is_err());
    }

    #[test]
    fn oversized_payload_rejected() {
        let mut buf = MAGIC.to_vec();
        buf.push(PROTOCOL_VERSION);
        buf.push(Op::Ping as u8);
        buf.push(0);
        buf.push(0);
        buf.extend_from_slice(&0u64.to_be_bytes());
        buf.extend_from_slice(&((MAX_FRAME_PAYLOAD as u32) + 1).to_be_bytes());
        buf.extend_from_slice(&0u32.to_be_bytes());
        assert!(decode(&buf).is_err());
    }

    #[test]
    fn truncated_frame_returns_none() {
        let f = Frame {
            op: Op::Ping,
            flags: 0,
            request_id: 1,
            payload: b"1234567890".to_vec(),
        };
        let wire = encode(&f).unwrap();
        assert!(decode(&wire[..wire.len() - 3]).unwrap().is_none());
    }

    #[test]
    fn unknown_op_rejected() {
        let mut wire = MAGIC.to_vec();
        wire.push(PROTOCOL_VERSION);
        wire.push(0xff);
        wire.push(0);
        wire.push(0);
        wire.extend_from_slice(&0u64.to_be_bytes());
        wire.extend_from_slice(&0u32.to_be_bytes());
        let crc = integrity::crc32c(&wire);
        wire.extend_from_slice(&crc.to_be_bytes());
        assert!(decode(&wire).is_err());
    }

    #[test]
    fn envelope_serde() {
        let e = Envelope::err("boom");
        let j = serde_json::to_vec(&e).unwrap();
        let back: Envelope = serde_json::from_slice(&j).unwrap();
        assert!(!back.is_ok());
        assert_eq!(back.error.as_deref(), Some("boom"));
    }
}
