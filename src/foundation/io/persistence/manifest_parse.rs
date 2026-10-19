//! Host-neutral AOF manifest parsing and token formatting.
//!
//! These routines parse the Valkey-style AOF manifest text and format manifest
//! tokens. They operate purely on in-memory bytes/strings (no file system), so
//! they compile on both native and `wasm`; the browser startup-recovery path
//! parses a manifest read from OPFS through [`parse_manifest_bytes`].
//!
//! The native manifest module (`persistence::aof::manifest`) re-exports these
//! and reuses `parse_entry`/`tokenize` for its file-backed helpers.

use std::io;
use std::path::Path;

pub const MAX_MANIFEST_LINE_LEN: usize = 1024;

#[derive(Clone, Debug)]
pub struct AofManifestEntry {
    pub filename: String,
    pub sequence: u64,
    pub kind: char,
}

/// Parse an AOF manifest from in-memory bytes.
/// Mirrors `aofLoadManifestFromFile` in `thirdparty/valkey/src/aof.c`. Line
/// length, duplicate base files, monotonic increment sequences and unknown
/// file types are enforced with the exact error messages the C version
/// emits so that `valkey-check-aof` can forward them to Tcl assertions.
pub fn parse_manifest_bytes(bytes: &[u8]) -> io::Result<Vec<AofManifestEntry>> {
    let content =
        std::str::from_utf8(bytes).map_err(|_| invalid_data("Invalid AOF manifest file format"))?;
    if content.is_empty() {
        return Err(invalid_data("Found an empty AOF manifest"));
    }
    let mut entries = Vec::new();
    let mut base_seen = false;
    let mut last_base_seq: Option<u64> = None;
    let mut last_incr_seq: Option<u64> = None;
    let mut had_any_line = false;
    for raw_line in content.split_terminator('\n') {
        let line_with_cr = raw_line;
        let line = line_with_cr.strip_suffix('\r').unwrap_or(line_with_cr);
        if line_with_cr.len() + 1 > MAX_MANIFEST_LINE_LEN + 1 {
            return Err(invalid_data("The AOF manifest file contains too long line"));
        }
        if line.starts_with('#') {
            had_any_line = true;
            continue;
        }
        let trimmed = line.trim_matches(|c: char| c == ' ' || c == '\t' || c == '\r');
        if trimmed.is_empty() {
            return Err(invalid_data("Invalid AOF manifest file format"));
        }
        had_any_line = true;
        let entry = parse_entry(trimmed)?;
        let last_sequence = match entry.kind {
            'b' => {
                if base_seen {
                    return Err(invalid_data("Found duplicate base file information"));
                }
                base_seen = true;
                Some(&mut last_base_seq)
            }
            'i' => Some(&mut last_incr_seq),
            'h' => None,
            _ => return Err(invalid_data("Unknown AOF file type")),
        };
        if let Some(last_sequence) = last_sequence {
            if last_sequence.is_some_and(|previous| entry.sequence <= previous) {
                return Err(invalid_data("Found a non-monotonic sequence number"));
            }
            *last_sequence = Some(entry.sequence);
        }
        entries.push(entry);
    }
    if !had_any_line {
        return Err(invalid_data("Found an empty AOF manifest"));
    }
    Ok(entries)
}

pub fn format_manifest_token(value: &str) -> String {
    if !value
        .bytes()
        .any(|byte| byte.is_ascii_whitespace() || matches!(byte, b'"' | b'\\'))
    {
        return value.to_owned();
    }
    let mut encoded = String::with_capacity(value.len() + 2);
    encoded.push('"');
    for byte in value.bytes() {
        match byte {
            b'\n' => encoded.push_str("\\n"),
            b'\r' => encoded.push_str("\\r"),
            b'\t' => encoded.push_str("\\t"),
            b'"' => encoded.push_str("\\\""),
            b'\\' => encoded.push_str("\\\\"),
            0x20..=0x7e => encoded.push(byte as char),
            _ => {
                use std::fmt::Write as _;
                let _ = write!(encoded, "\\x{byte:02x}");
            }
        }
    }
    encoded.push('"');
    encoded
}

pub(super) fn tokenize(line: &str) -> io::Result<Vec<String>> {
    let bytes = line.as_bytes();
    let mut tokens = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        while index < bytes.len() && bytes[index].is_ascii_whitespace() {
            index += 1;
        }
        if index == bytes.len() {
            break;
        }
        if bytes[index] != b'"' {
            let start = index;
            while index < bytes.len() && !bytes[index].is_ascii_whitespace() {
                index += 1;
            }
            tokens.push(line[start..index].to_owned());
            continue;
        }
        index += 1;
        let mut token = Vec::new();
        let mut closed = false;
        while index < bytes.len() {
            match bytes[index] {
                b'"' => {
                    index += 1;
                    closed = true;
                    break;
                }
                b'\\' => {
                    index += 1;
                    decode_escape(bytes, &mut index, &mut token)?;
                }
                byte => {
                    token.push(byte);
                    index += 1;
                }
            }
        }
        if !closed || (index < bytes.len() && !bytes[index].is_ascii_whitespace()) {
            return Err(invalid_data("Invalid AOF manifest file format"));
        }
        tokens.push(
            String::from_utf8(token)
                .map_err(|_| invalid_data("Invalid AOF manifest file format"))?,
        );
    }
    Ok(tokens)
}

fn decode_escape(bytes: &[u8], index: &mut usize, output: &mut Vec<u8>) -> io::Result<()> {
    let Some(&escaped) = bytes.get(*index) else {
        return Err(invalid_data("Invalid AOF manifest file format"));
    };
    *index += 1;
    match escaped {
        b'n' => output.push(b'\n'),
        b'r' => output.push(b'\r'),
        b't' => output.push(b'\t'),
        b'\\' | b'"' => output.push(escaped),
        b'x' if *index + 1 < bytes.len() => {
            let digits = std::str::from_utf8(&bytes[*index..*index + 2])
                .map_err(|_| invalid_data("Invalid AOF manifest file format"))?;
            output.push(
                u8::from_str_radix(digits, 16)
                    .map_err(|_| invalid_data("Invalid AOF manifest file format"))?,
            );
            *index += 2;
        }
        _ => return Err(invalid_data("Invalid AOF manifest file format")),
    }
    Ok(())
}

pub(super) fn parse_entry(line: &str) -> io::Result<AofManifestEntry> {
    let tokens = tokenize(line)?;
    if tokens.len() < 6 || tokens.len() % 2 != 0 {
        return Err(invalid_data("Invalid AOF manifest file format"));
    }
    let mut filename = None;
    let mut sequence = None;
    let mut kind = None;
    for pair in tokens.as_chunks::<2>().0 {
        match pair[0].as_str() {
            "file" if filename.is_none() => filename = Some(pair[1].clone()),
            "seq" if sequence.is_none() => sequence = pair[1].parse::<u64>().ok(),
            "type" if kind.is_none() => kind = pair[1].chars().next(),
            "file" | "seq" | "type" => {
                return Err(invalid_data("Invalid AOF manifest file format"));
            }
            _ => {}
        }
    }
    match (filename, sequence, kind) {
        (Some(filename), Some(_), Some(_)) if !is_safe_manifest_filename(&filename) => {
            Err(invalid_data("Invalid AOF manifest file format"))
        }
        (Some(filename), Some(sequence), Some(kind)) if matches!(kind, 'b' | 'i' | 'h') => {
            Ok(AofManifestEntry {
                filename,
                sequence,
                kind,
            })
        }
        (Some(_), Some(_), Some(_)) => Err(invalid_data("Unknown AOF file type")),
        _ => Err(invalid_data("Invalid AOF manifest file format")),
    }
}

fn is_safe_manifest_filename(filename: &str) -> bool {
    let mut components = Path::new(filename).components();
    matches!(components.next(), Some(std::path::Component::Normal(_)))
        && components.next().is_none()
}

pub(super) fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
