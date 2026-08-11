//! Pluggable compression.
//!
//! The architecture is not bound to one codec. 1.0.0 ships `none` and `zstd`.
//! Codec, codec version, original size, stored size, and hashes are all recorded
//! so decompression can be verified.

use std::io::{BufReader, BufWriter, Read, Write};

use serde::{Deserialize, Serialize};

use crate::errors::{FabricError, FabricResult};

/// Codec identifiers. Unknown identifiers are rejected, never silently mapped.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Codec {
    None,
    Zstd,
}

impl Codec {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Zstd => "zstd",
        }
    }

    pub fn from_str_strict(s: &str) -> FabricResult<Self> {
        match s {
            "none" => Ok(Self::None),
            "zstd" => Ok(Self::Zstd),
            other => Err(FabricError::UnsupportedBackend(format!(
                "unknown compression codec '{other}'"
            ))),
        }
    }
}

/// Compression settings recorded on a checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompressionSpec {
    pub codec: Codec,
    pub level: i32,
    pub format_version: String,
}

impl CompressionSpec {
    pub fn none() -> Self {
        Self {
            codec: Codec::None,
            level: 0,
            format_version: "1".into(),
        }
    }

    pub fn zstd(level: i32) -> Self {
        Self {
            codec: Codec::Zstd,
            level: level.clamp(1, 22),
            format_version: "1".into(),
        }
    }

    pub fn is_compressed(&self) -> bool {
        self.codec != Codec::None
    }
}

/// Statistics from a compression pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompressionStats {
    pub original_size: u64,
    pub stored_size: u64,
}

/// Compress a byte slice with the codec.
pub fn compress_bytes(spec: &CompressionSpec, payload: &[u8]) -> FabricResult<Vec<u8>> {
    match spec.codec {
        Codec::None => Ok(payload.to_vec()),
        Codec::Zstd => zstd::stream::encode_all(payload, spec.level)
            .map_err(|e| FabricError::Internal(format!("zstd encode: {e}"))),
    }
}

/// Decompress a byte slice with the codec, bounding output size.
pub fn decompress_bytes(
    spec: &CompressionSpec,
    stored: &[u8],
    max_output: u64,
) -> FabricResult<Vec<u8>> {
    match spec.codec {
        Codec::None => {
            if stored.len() as u64 > max_output {
                return Err(FabricError::IntegrityFailure(
                    "stored payload exceeds bounded output size".into(),
                ));
            }
            Ok(stored.to_vec())
        }
        Codec::Zstd => {
            let mut decoder = zstd::stream::read::Decoder::new(stored)
                .map_err(|e| FabricError::Internal(format!("zstd decode: {e}")))?;
            let mut out = Vec::new();
            decoder
                .by_ref()
                .take(max_output.saturating_add(1))
                .read_to_end(&mut out)
                .map_err(|e| FabricError::Internal(format!("zstd decode: {e}")))?;
            if out.len() as u64 > max_output {
                return Err(FabricError::IntegrityFailure(
                    "decompressed payload exceeds bounded output size".into(),
                ));
            }
            Ok(out)
        }
    }
}

/// Stream a reader into a writer applying the codec; returns original and stored sizes.
pub fn compress_stream<R: Read, W: Write>(
    spec: &CompressionSpec,
    reader: R,
    writer: W,
) -> FabricResult<CompressionStats> {
    let mut reader = BufReader::with_capacity(1 << 20, reader);
    let mut counting = CountingWriter {
        inner: BufWriter::with_capacity(1 << 20, writer),
        bytes: 0,
    };
    let mut original = 0u64;
    match spec.codec {
        Codec::None => {
            let mut buf = vec![0u8; 1 << 20];
            loop {
                let n = reader.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                original += n as u64;
                counting.write_all(&buf[..n])?;
            }
        }
        Codec::Zstd => {
            let mut enc = zstd::stream::write::Encoder::new(&mut counting, spec.level)
                .map_err(|e| FabricError::Internal(format!("zstd encoder: {e}")))?;
            let mut buf = vec![0u8; 1 << 20];
            loop {
                let n = reader.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                original += n as u64;
                enc.write_all(&buf[..n])?;
            }
            enc.finish()
                .map_err(|e| FabricError::Internal(format!("zstd finish: {e}")))?;
        }
    }
    counting.flush()?;
    let stored = counting.bytes;
    let _ = counting
        .into_inner()
        .map_err(|e| FabricError::Internal(format!("flush: {e}")))?;
    Ok(CompressionStats {
        original_size: original,
        stored_size: stored,
    })
}

struct CountingWriter<W: Write> {
    inner: BufWriter<W>,
    bytes: u64,
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.bytes = self.bytes.saturating_add(n as u64);
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

impl<W: Write> CountingWriter<W> {
    fn into_inner(self) -> std::io::Result<W> {
        self.inner.into_inner().map_err(|e| e.into_error())
    }
}

/// Stream a stored reader into a writer decoding the codec; returns decoded size.
pub fn decompress_stream<R: Read, W: Write>(
    spec: &CompressionSpec,
    reader: R,
    writer: W,
    max_output: u64,
) -> FabricResult<u64> {
    let mut reader = BufReader::with_capacity(1 << 20, reader);
    let mut writer = BufWriter::with_capacity(1 << 20, writer);
    let mut out = 0u64;
    match spec.codec {
        Codec::None => {
            let mut buf = vec![0u8; 1 << 20];
            loop {
                let n = reader.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                out += n as u64;
                if out > max_output {
                    return Err(FabricError::IntegrityFailure(
                        "decompressed payload exceeds bounded output size".into(),
                    ));
                }
                writer.write_all(&buf[..n])?;
            }
        }
        Codec::Zstd => {
            let mut decoder = zstd::stream::read::Decoder::new(reader)
                .map_err(|e| FabricError::Internal(format!("zstd decoder: {e}")))?;
            let mut buf = vec![0u8; 1 << 20];
            loop {
                let n = decoder.read(&mut buf)?;
                if n == 0 {
                    break;
                }
                out += n as u64;
                if out > max_output {
                    return Err(FabricError::IntegrityFailure(
                        "decompressed payload exceeds bounded output size".into(),
                    ));
                }
                writer.write_all(&buf[..n])?;
            }
        }
    }
    writer.flush()?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn none_roundtrip() {
        let spec = CompressionSpec::none();
        let data = b"payload".repeat(1000);
        let stored = compress_bytes(&spec, &data).unwrap();
        assert_eq!(stored, data);
        let back = decompress_bytes(&spec, &stored, 1 << 20).unwrap();
        assert_eq!(back, data);
    }

    #[test]
    fn zstd_roundtrip() {
        let spec = CompressionSpec::zstd(3);
        let data = b"compressible-payload-".repeat(10_000);
        let stored = compress_bytes(&spec, &data).unwrap();
        assert!(stored.len() < data.len());
        let back = decompress_bytes(&spec, &stored, 1 << 20).unwrap();
        assert_eq!(back, data);
    }

    #[test]
    fn zstd_bounded_output() {
        let spec = CompressionSpec::zstd(3);
        let data = b"x".repeat(100_000);
        let stored = compress_bytes(&spec, &data).unwrap();
        assert!(decompress_bytes(&spec, &stored, 10_000).is_err());
    }

    #[test]
    fn stream_roundtrip() {
        let spec = CompressionSpec::zstd(3);
        let data = b"streamy-".repeat(50_000);
        let mut out = Vec::new();
        let stats = compress_stream(&spec, &data[..], &mut out).unwrap();
        assert_eq!(stats.original_size, data.len() as u64);
        assert_eq!(stats.stored_size, out.len() as u64);
        let mut back = Vec::new();
        let n = decompress_stream(&spec, &out[..], &mut back, 1 << 20).unwrap();
        assert_eq!(n, data.len() as u64);
        assert_eq!(back, data);
    }

    #[test]
    fn unknown_codec_rejected() {
        assert!(Codec::from_str_strict("lz4").is_err());
    }
}
