//! Valkey compressed-stream envelopes and bounded LZ4 frame decoding.

use lz4_flex::frame::{BlockMode, BlockSize, FrameEncoder, FrameInfo};
use std::io::Write;
use xxhash_rust::xxh32::xxh32;

pub const VCS_RDB_HEADER: &[u8; 7] = b"VCS\x01\x01\x00\x01";
const LZ4_FRAME_MAGIC: &[u8; 4] = b"\x04\x22\x4d\x18";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RdbCompression {
    No,
    #[default]
    Yes,
    Lzf,
    Lz4,
}

#[derive(Clone, Copy, Debug)]
pub struct RdbSnapshotOptions {
    pub compression: RdbCompression,
    pub checksum: bool,
}

impl RdbCompression {
    pub fn parse(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "no" => Some(Self::No),
            "yes" => Some(Self::Yes),
            "lzf" => Some(Self::Lzf),
            "lz4" => Some(Self::Lz4),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::No => "no",
            Self::Yes => "yes",
            Self::Lzf => "lzf",
            Self::Lz4 => "lz4",
        }
    }

    pub fn uses_lzf(self) -> bool {
        matches!(self, Self::Yes | Self::Lzf)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompressionErrorKind {
    Incompatible,
    Corrupt,
    Truncated,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompressionError {
    pub kind: CompressionErrorKind,
    pub physical_offset: usize,
}

impl CompressionError {
    pub fn message(self) -> &'static str {
        match self.kind {
            CompressionErrorKind::Incompatible => "Invalid or unsupported RDB stream envelope",
            CompressionErrorKind::Corrupt => "Corrupt compressed RDB stream",
            CompressionErrorKind::Truncated => "Compressed RDB stream did not end cleanly",
        }
    }
}

pub struct DecodedRdbStream {
    pub bytes: Vec<u8>,
    pub physical_bytes: usize,
}

pub fn encode_rdb_stream(bytes: &[u8], checksum: bool) -> std::io::Result<Vec<u8>> {
    let info = FrameInfo::new()
        .block_size(BlockSize::Max64KB)
        .block_mode(BlockMode::Linked)
        .block_checksums(checksum)
        .content_checksum(checksum);
    let mut encoder = FrameEncoder::with_frame_info(info, VCS_RDB_HEADER.to_vec());
    encoder.write_all(bytes)?;
    encoder.finish().map_err(std::io::Error::other)
}

/// Decode one frame and ignore bytes beyond its boundary, like the plain RDB
/// loader. Codec checksum skipping never skips structural validation.
pub fn decode_rdb_stream(
    bytes: &[u8],
    skip_checksums: bool,
) -> Result<Option<DecodedRdbStream>, CompressionError> {
    if !bytes.starts_with(b"VCS") {
        return Ok(None);
    }
    let mut reader = FrameReader { bytes, offset: 0 };
    if reader.take(7)? != VCS_RDB_HEADER {
        return Err(reader.error(CompressionErrorKind::Incompatible));
    }
    if reader.take(4)? != LZ4_FRAME_MAGIC {
        return Err(reader.error(CompressionErrorKind::Corrupt));
    }
    let descriptor_start = reader.offset;
    let flags = reader.take(1)?[0];
    let block_descriptor = reader.take(1)?[0];
    let block_size = match (block_descriptor >> 4) & 7 {
        4 => 64 * 1024,
        5 => 256 * 1024,
        6 => 1024 * 1024,
        7 => 4 * 1024 * 1024,
        _ => return Err(reader.error(CompressionErrorKind::Corrupt)),
    };
    if flags & 0xc2 != 0x40 || block_descriptor & 0x8f != 0 {
        return Err(reader.error(CompressionErrorKind::Corrupt));
    }
    let content_size = if flags & 8 != 0 {
        Some(u64::from_le_bytes(
            reader
                .take(8)?
                .try_into()
                .map_err(|_| reader.error(CompressionErrorKind::Corrupt))?,
        ))
    } else {
        None
    };
    if flags & 1 != 0 {
        // A dictionary ID requires a dictionary negotiated outside the RDB
        // envelope; Valkey's RDB writer never emits such frames.
        return Err(reader.error(CompressionErrorKind::Incompatible));
    }
    let descriptor_end = reader.offset;
    let header_checksum = reader.take(1)?[0];
    if header_checksum != (xxh32(&bytes[descriptor_start..descriptor_end], 0) >> 8) as u8 {
        return Err(reader.error(CompressionErrorKind::Corrupt));
    }
    let mut output = Vec::new();
    let mut block = vec![0u8; block_size];
    loop {
        let block_info = reader.read_u32()?;
        if block_info == 0 {
            break;
        }
        let input_size = (block_info & 0x7fff_ffff) as usize;
        if input_size == 0 || input_size > block_size {
            return Err(reader.error(CompressionErrorKind::Corrupt));
        }
        let input = reader.take(input_size)?;
        if flags & 0x10 != 0 {
            let expected = reader.read_u32()?;
            if !skip_checksums && xxh32(input, 0) != expected {
                return Err(reader.error(CompressionErrorKind::Corrupt));
            }
        }
        let decoded = if block_info & 0x8000_0000 != 0 {
            input
        } else {
            let dictionary = if flags & 0x20 == 0 {
                &output[output.len().saturating_sub(64 * 1024)..]
            } else {
                &[]
            };
            let count = lz4_flex::block::decompress_into_with_dict(input, &mut block, dictionary)
                .map_err(|_| reader.error(CompressionErrorKind::Corrupt))?;
            &block[..count]
        };
        output
            .try_reserve(decoded.len())
            .map_err(|_| reader.error(CompressionErrorKind::Corrupt))?;
        output.extend_from_slice(decoded);
    }
    if flags & 4 != 0 {
        let expected = reader.read_u32()?;
        if !skip_checksums && xxh32(&output, 0) != expected {
            return Err(reader.error(CompressionErrorKind::Corrupt));
        }
    }
    if content_size.is_some_and(|expected| expected != output.len() as u64) {
        return Err(reader.error(CompressionErrorKind::Corrupt));
    }
    Ok(Some(DecodedRdbStream {
        bytes: output,
        physical_bytes: reader.offset,
    }))
}

struct FrameReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> FrameReader<'a> {
    fn error(&self, kind: CompressionErrorKind) -> CompressionError {
        CompressionError {
            kind,
            physical_offset: self.offset,
        }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], CompressionError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or_else(|| self.error(CompressionErrorKind::Corrupt))?;
        let result = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| self.error(CompressionErrorKind::Truncated))?;
        self.offset = end;
        Ok(result)
    }

    fn read_u32(&mut self) -> Result<u32, CompressionError> {
        Ok(u32::from_le_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| self.error(CompressionErrorKind::Corrupt))?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn linked_blocks_round_trip_with_and_without_checksums() {
        let payload: Vec<u8> = (0..220_000).map(|index| (index % 251) as u8).collect();
        for checksum in [false, true] {
            let encoded = encode_rdb_stream(&payload, checksum).expect("encode LZ4 frame");
            let decoded = decode_rdb_stream(&encoded, false)
                .expect("decode LZ4 frame")
                .expect("compressed envelope");
            assert_eq!(decoded.bytes, payload);
            assert_eq!(decoded.physical_bytes, encoded.len());
            let mut standard_decoder = lz4_flex::frame::FrameDecoder::new(&encoded[7..]);
            let mut standard_output = Vec::new();
            standard_decoder
                .read_to_end(&mut standard_output)
                .expect("standard LZ4 decoder");
            assert_eq!(standard_output, payload);
        }
    }

    #[test]
    fn checksum_skip_preserves_framing_and_envelope_validation() {
        let mut encoded = encode_rdb_stream(b"snapshot payload", true).expect("encode frame");
        let last = encoded.len() - 1;
        encoded[last] ^= 1;
        assert!(matches!(
            decode_rdb_stream(&encoded, false),
            Err(CompressionError {
                kind: CompressionErrorKind::Corrupt,
                ..
            })
        ));
        assert_eq!(
            decode_rdb_stream(&encoded, true)
                .expect("skip checksum")
                .expect("envelope")
                .bytes,
            b"snapshot payload",
        );
        assert!(matches!(
            decode_rdb_stream(&encoded[..last], true),
            Err(CompressionError {
                kind: CompressionErrorKind::Truncated,
                ..
            })
        ));
        for offset in 3..7 {
            let mut incompatible = encoded.clone();
            incompatible[offset] ^= 1;
            assert!(matches!(
                decode_rdb_stream(&incompatible, true),
                Err(CompressionError {
                    kind: CompressionErrorKind::Incompatible,
                    ..
                })
            ));
        }
    }

    #[test]
    fn decoder_consumes_one_frame_and_leaves_trailing_bytes() {
        let mut encoded = encode_rdb_stream(b"snapshot payload", true).expect("encode frame");
        let frame_length = encoded.len();
        encoded.extend_from_slice(b"trailing bytes");
        let decoded = decode_rdb_stream(&encoded, false)
            .expect("decode frame")
            .expect("envelope");
        assert_eq!(decoded.physical_bytes, frame_length);
        assert_eq!(decoded.bytes, b"snapshot payload");
        assert!(decode_rdb_stream(b"VALKEY080", false)
            .expect("plain RDB")
            .is_none());
    }
}
