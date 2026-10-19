//! Host-neutral AOF manifest envelope codec and dual-slot selection.
//!
//! The browser OPFS persistence path keeps two manifest slots so a generation
//! switch is atomic: the writer always updates the slot that does *not* hold the
//! currently active manifest, then both slots are compared on startup and the
//! higher-generation slot wins. To avoid reimplementing CRC64 in JavaScript, the
//! JS worker only reads and writes the raw slot bytes and checks whether the
//! referenced files exist; all framing, checksumming and payload parsing live
//! here and compile on both the native and `wasm` targets.
//!
//! Envelope layout (design §9, big-endian integers):
//!
//! ```text
//! magic           8 bytes  = b"VKAOFMAN"
//! format_version  u16
//! flags           u16
//! generation      u64
//! payload_length  u32
//! payload_crc64   u64      = crc64(0, payload)
//! header_crc64    u64      = crc64(0, bytes from magic through payload_crc64)
//! payload         payload_length bytes
//! ```
//!
//! The payload is Valkey-style manifest text (`file <token> seq <n> type <k>`),
//! so [`parse_manifest_bytes`](crate::foundation::io::persistence::parse_manifest_bytes)
//! can decode it unchanged.

use super::codec::types::crc64;
use super::manifest_parse::{format_manifest_token, AofManifestEntry};

/// Envelope magic marker identifying a VersalKV AOF manifest slot.
pub const MANIFEST_ENVELOPE_MAGIC: &[u8; 8] = b"VKAOFMAN";

/// Current envelope format version.
pub const MANIFEST_ENVELOPE_VERSION: u16 = 1;

/// Byte length of the fixed envelope header preceding the payload.
/// magic(8) + version(2) + flags(2) + generation(8) + payload_length(4)
/// + payload_crc64(8) + header_crc64(8).
pub const MANIFEST_ENVELOPE_HEADER_LEN: usize = 8 + 2 + 2 + 8 + 4 + 8 + 8;

/// Offset of the trailing header CRC within the header; everything before it is
/// covered by `header_crc64`.
const HEADER_CRC_OFFSET: usize = MANIFEST_ENVELOPE_HEADER_LEN - 8;

/// A decoded manifest envelope: its generation, flags and raw payload bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestEnvelope {
    pub generation: u64,
    pub flags: u16,
    pub payload: Vec<u8>,
}

/// A failure encountered while decoding a manifest envelope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ManifestEnvelopeError {
    /// The buffer is shorter than the fixed header.
    Truncated,
    /// The magic marker does not match.
    BadMagic,
    /// The format version is not understood.
    UnsupportedVersion(u16),
    /// `payload_length` exceeds the available bytes.
    PayloadLengthMismatch,
    /// The header CRC does not match the header bytes.
    HeaderCrcMismatch,
    /// The payload CRC does not match the payload bytes.
    PayloadCrcMismatch,
}

/// Which of the two manifest slots a value refers to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ManifestSlot {
    Zero,
    One,
}

impl ManifestSlot {
    /// The other slot in the pair.
    pub fn other(self) -> Self {
        match self {
            ManifestSlot::Zero => ManifestSlot::One,
            ManifestSlot::One => ManifestSlot::Zero,
        }
    }

    /// The zero-based index of this slot (0 or 1).
    pub fn index(self) -> usize {
        match self {
            ManifestSlot::Zero => 0,
            ManifestSlot::One => 1,
        }
    }
}

/// Encode a manifest envelope around `payload` with the given generation/flags.
pub fn encode_manifest_envelope(generation: u64, flags: u16, payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(MANIFEST_ENVELOPE_HEADER_LEN + payload.len());
    buf.extend_from_slice(MANIFEST_ENVELOPE_MAGIC);
    buf.extend_from_slice(&MANIFEST_ENVELOPE_VERSION.to_be_bytes());
    buf.extend_from_slice(&flags.to_be_bytes());
    buf.extend_from_slice(&generation.to_be_bytes());
    buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    let payload_crc = crc64(0, payload);
    buf.extend_from_slice(&payload_crc.to_be_bytes());
    // `header_crc64` covers every header byte written so far (magic through
    // payload_crc64), then is appended as the final header field.
    let header_crc = crc64(0, &buf);
    buf.extend_from_slice(&header_crc.to_be_bytes());
    buf.extend_from_slice(payload);
    buf
}

/// Decode and validate a manifest envelope from `bytes`.
pub fn decode_manifest_envelope(bytes: &[u8]) -> Result<ManifestEnvelope, ManifestEnvelopeError> {
    if bytes.len() < MANIFEST_ENVELOPE_HEADER_LEN {
        return Err(ManifestEnvelopeError::Truncated);
    }
    if &bytes[0..8] != MANIFEST_ENVELOPE_MAGIC {
        return Err(ManifestEnvelopeError::BadMagic);
    }
    let version = u16::from_be_bytes([bytes[8], bytes[9]]);
    if version != MANIFEST_ENVELOPE_VERSION {
        return Err(ManifestEnvelopeError::UnsupportedVersion(version));
    }
    let flags = u16::from_be_bytes([bytes[10], bytes[11]]);
    let generation = u64::from_be_bytes(bytes[12..20].try_into().expect("8-byte generation"));
    let payload_length =
        u32::from_be_bytes(bytes[20..24].try_into().expect("4-byte length")) as usize;
    let payload_crc = u64::from_be_bytes(bytes[24..32].try_into().expect("8-byte payload crc"));
    let header_crc = u64::from_be_bytes(
        bytes[HEADER_CRC_OFFSET..MANIFEST_ENVELOPE_HEADER_LEN]
            .try_into()
            .expect("8-byte header crc"),
    );
    if crc64(0, &bytes[0..HEADER_CRC_OFFSET]) != header_crc {
        return Err(ManifestEnvelopeError::HeaderCrcMismatch);
    }
    let payload_end = MANIFEST_ENVELOPE_HEADER_LEN
        .checked_add(payload_length)
        .ok_or(ManifestEnvelopeError::PayloadLengthMismatch)?;
    if bytes.len() < payload_end {
        return Err(ManifestEnvelopeError::PayloadLengthMismatch);
    }
    let payload = bytes[MANIFEST_ENVELOPE_HEADER_LEN..payload_end].to_vec();
    if crc64(0, &payload) != payload_crc {
        return Err(ManifestEnvelopeError::PayloadCrcMismatch);
    }
    Ok(ManifestEnvelope {
        generation,
        flags,
        payload,
    })
}

/// Build the Valkey-style manifest text payload from the given entries.
pub fn build_manifest_payload(entries: &[AofManifestEntry]) -> Vec<u8> {
    use std::fmt::Write as _;
    let mut text = String::new();
    for entry in entries {
        let _ = writeln!(
            text,
            "file {} seq {} type {}",
            format_manifest_token(&entry.filename),
            entry.sequence,
            entry.kind
        );
    }
    text.into_bytes()
}

/// Compare two optional raw slot buffers and return the valid one with the
/// higher generation, if any. Invalid slots are ignored. When both slots decode
/// and tie on generation (which should not occur in practice) slot zero wins.
pub fn select_higher_generation(
    slot0: Option<&[u8]>,
    slot1: Option<&[u8]>,
) -> Option<(ManifestSlot, ManifestEnvelope)> {
    let decoded0 = slot0.and_then(|bytes| decode_manifest_envelope(bytes).ok());
    let decoded1 = slot1.and_then(|bytes| decode_manifest_envelope(bytes).ok());
    match (decoded0, decoded1) {
        (Some(env0), Some(env1)) => {
            if env1.generation > env0.generation {
                Some((ManifestSlot::One, env1))
            } else {
                Some((ManifestSlot::Zero, env0))
            }
        }
        (Some(env0), None) => Some((ManifestSlot::Zero, env0)),
        (None, Some(env1)) => Some((ManifestSlot::One, env1)),
        (None, None) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::super::manifest_parse::parse_manifest_bytes;
    use super::*;

    fn sample_entries() -> Vec<AofManifestEntry> {
        vec![
            AofManifestEntry {
                filename: "appendonly.aof.0000000001.base.rdb".to_owned(),
                sequence: 1,
                kind: 'b',
            },
            AofManifestEntry {
                filename: "appendonly.aof.0000000001.incr.aof".to_owned(),
                sequence: 1,
                kind: 'i',
            },
        ]
    }

    #[test]
    fn round_trips_an_envelope() {
        let payload = build_manifest_payload(&sample_entries());
        let encoded = encode_manifest_envelope(7, 0, &payload);
        let decoded = decode_manifest_envelope(&encoded).unwrap();
        assert_eq!(decoded.generation, 7);
        assert_eq!(decoded.flags, 0);
        assert_eq!(decoded.payload, payload);

        // The payload is plain manifest text and parses back to the entries.
        let entries = parse_manifest_bytes(&decoded.payload).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].kind, 'b');
        assert_eq!(entries[1].kind, 'i');
        assert_eq!(entries[1].filename, "appendonly.aof.0000000001.incr.aof");
    }

    #[test]
    fn rejects_a_truncated_header() {
        let encoded = encode_manifest_envelope(1, 0, b"payload");
        let truncated = &encoded[..MANIFEST_ENVELOPE_HEADER_LEN - 1];
        assert_eq!(
            decode_manifest_envelope(truncated),
            Err(ManifestEnvelopeError::Truncated)
        );
    }

    #[test]
    fn rejects_bad_magic_and_version() {
        let mut encoded = encode_manifest_envelope(1, 0, b"payload");
        encoded[0] = b'X';
        assert_eq!(
            decode_manifest_envelope(&encoded),
            Err(ManifestEnvelopeError::BadMagic)
        );

        let mut encoded = encode_manifest_envelope(1, 0, b"payload");
        encoded[8] = 0;
        encoded[9] = 2;
        // Mutating the version invalidates the header CRC first, which is
        // checked before the version only if ordering allowed; the version
        // check runs before the CRC, so an unsupported version is reported.
        assert_eq!(
            decode_manifest_envelope(&encoded),
            Err(ManifestEnvelopeError::UnsupportedVersion(2))
        );
    }

    #[test]
    fn detects_header_and_payload_crc_corruption() {
        // Corrupt a header byte (the flags field): header CRC must fail.
        let mut encoded = encode_manifest_envelope(1, 0, b"payload");
        encoded[11] ^= 0xff;
        assert_eq!(
            decode_manifest_envelope(&encoded),
            Err(ManifestEnvelopeError::HeaderCrcMismatch)
        );

        // Corrupt a payload byte: header CRC passes but payload CRC fails.
        let mut encoded = encode_manifest_envelope(1, 0, b"payload");
        let last = encoded.len() - 1;
        encoded[last] ^= 0xff;
        assert_eq!(
            decode_manifest_envelope(&encoded),
            Err(ManifestEnvelopeError::PayloadCrcMismatch)
        );
    }

    #[test]
    fn rejects_a_short_payload() {
        let mut encoded = encode_manifest_envelope(1, 0, b"payload");
        // Drop the final payload byte so the declared length overruns the buffer.
        encoded.pop();
        assert_eq!(
            decode_manifest_envelope(&encoded),
            Err(ManifestEnvelopeError::PayloadLengthMismatch)
        );
    }

    #[test]
    fn selects_the_higher_generation_slot() {
        let payload = build_manifest_payload(&sample_entries());
        let low = encode_manifest_envelope(3, 0, &payload);
        let high = encode_manifest_envelope(9, 0, &payload);

        let (slot, env) = select_higher_generation(Some(&low), Some(&high)).unwrap();
        assert_eq!(slot, ManifestSlot::One);
        assert_eq!(env.generation, 9);

        let (slot, env) = select_higher_generation(Some(&high), Some(&low)).unwrap();
        assert_eq!(slot, ManifestSlot::Zero);
        assert_eq!(env.generation, 9);
    }

    #[test]
    fn ignores_invalid_slots_and_handles_empty() {
        let payload = build_manifest_payload(&sample_entries());
        let valid = encode_manifest_envelope(4, 0, &payload);
        let garbage = vec![0u8; 4];

        let (slot, env) = select_higher_generation(Some(&garbage), Some(&valid)).unwrap();
        assert_eq!(slot, ManifestSlot::One);
        assert_eq!(env.generation, 4);

        let (slot, _) = select_higher_generation(Some(&valid), None).unwrap();
        assert_eq!(slot, ManifestSlot::Zero);

        assert!(select_higher_generation(None, None).is_none());
        assert!(select_higher_generation(Some(&garbage), None).is_none());
    }

    #[test]
    fn slot_helpers() {
        assert_eq!(ManifestSlot::Zero.other(), ManifestSlot::One);
        assert_eq!(ManifestSlot::One.other(), ManifestSlot::Zero);
        assert_eq!(ManifestSlot::Zero.index(), 0);
        assert_eq!(ManifestSlot::One.index(), 1);
    }
}
