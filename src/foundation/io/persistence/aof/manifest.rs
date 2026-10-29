use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

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

pub fn resolve_aof_read_paths_checked(
    dir: &str,
    filename: &str,
    appenddirname: &str,
) -> io::Result<Vec<PathBuf>> {
    if appenddirname.is_empty() {
        return Ok(vec![Path::new(dir).join(filename)]);
    }
    let app_dir = Path::new(dir).join(appenddirname);
    let manifest_path = app_dir.join(format!("{}.manifest", filename));
    let content = read_or_upgrade_legacy(&manifest_path, &app_dir, dir, filename)?;
    let Some(content) = content else {
        return Ok(vec![app_dir.join(format!("{}.1.incr.aof", filename))]);
    };
    let parsed_entries = parse_manifest_bytes(content.as_bytes())?;

    let mut entries = Vec::new();
    for entry in parsed_entries {
        let path = app_dir.join(&entry.filename);
        migrate_interrupted_legacy(dir, filename, &entry.filename, &path)?;
        std::fs::metadata(&path).map_err(|error| {
            io::Error::new(error.kind(), format!("{} {}", entry.filename, error))
        })?;
        if entry.kind != 'h' {
            entries.push(path);
        }
    }
    if entries.is_empty() {
        return Err(invalid_data("Found an empty AOF manifest"));
    }
    Ok(entries)
}

pub fn ensure_aof_increment(dir: &str, filename: &str, appenddirname: &str) -> io::Result<()> {
    let app_dir = Path::new(dir).join(appenddirname);
    let manifest_path = app_dir.join(format!("{}.manifest", filename));
    let content = std::fs::read_to_string(&manifest_path)?;
    let entries = read_manifest_entries(&manifest_path)?;
    if entries.iter().any(|entry| entry.kind == 'i') {
        return Ok(());
    }
    let incr_name = format!("{}.1.incr.aof", filename);
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(app_dir.join(&incr_name))?;
    append_manifest_entry(&manifest_path, &content, &incr_name, 1)
}

pub fn next_rewrite_sequences(
    manifest_path: &Path,
    increment_rotated: bool,
) -> io::Result<(u64, u64)> {
    let content = match std::fs::read_to_string(manifest_path) {
        Ok(content) => content,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok((1, 1)),
        Err(error) => return Err(error),
    };
    let (max_base, max_incr) = max_sequences(&content)?;
    let next_increment = if increment_rotated {
        max_incr.max(1)
    } else {
        max_incr.saturating_add(1).max(1)
    };
    Ok((max_base.saturating_add(1).max(1), next_increment))
}

pub fn rotate_aof_increment(dir: &str, filename: &str, appenddirname: &str) -> io::Result<PathBuf> {
    let app_dir = Path::new(dir).join(appenddirname);
    let manifest_path = app_dir.join(format!("{}.manifest", filename));
    let content = std::fs::read_to_string(&manifest_path)?;
    let (_, max_incr) = max_sequences(&content)?;
    let sequence = max_incr.saturating_add(1).max(1);
    let incr_name = format!("{}.{}.incr.aof", filename, sequence);
    let incr_path = app_dir.join(&incr_name);
    OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&incr_path)?;
    append_manifest_entry(&manifest_path, &content, &incr_name, sequence)?;
    Ok(incr_path)
}

pub fn read_manifest_entries(manifest_path: &Path) -> io::Result<Vec<AofManifestEntry>> {
    let content = std::fs::read_to_string(manifest_path)?;
    content
        .split_terminator('\n')
        .map(|line| parse_entry(line.strip_suffix('\r').unwrap_or(line)))
        .collect()
}

pub fn gc_aof_history(dir: &str, filename: &str, appenddirname: &str) -> io::Result<()> {
    let app_dir = Path::new(dir).join(appenddirname);
    let manifest_path = app_dir.join(format!("{}.manifest", filename));
    let entries = read_manifest_entries(&manifest_path)?;
    let mut active_manifest = String::new();
    for entry in entries {
        if entry.kind == 'h' {
            let _ = std::fs::remove_file(app_dir.join(entry.filename));
            continue;
        }
        use std::fmt::Write as _;
        writeln!(
            active_manifest,
            "file {} seq {} type {}",
            format_manifest_token(&entry.filename),
            entry.sequence,
            entry.kind
        )
        .map_err(io::Error::other)?;
    }
    std::fs::write(manifest_path, active_manifest)
}

fn max_sequences(content: &str) -> io::Result<(u64, u64)> {
    let mut max_base = 0u64;
    let mut max_incr = 0u64;
    for line in content.split_terminator('\n') {
        let tokens = tokenize(line.strip_suffix('\r').unwrap_or(line))?;
        let mut sequence = None;
        let mut entry_type = None;
        for pair in tokens.as_chunks::<2>().0 {
            match pair[0].as_str() {
                "seq" => sequence = pair[1].parse::<u64>().ok(),
                "type" => entry_type = Some(pair[1].as_str()),
                _ => {}
            }
        }
        match (sequence, entry_type) {
            (Some(sequence), Some("b")) => max_base = max_base.max(sequence),
            (Some(sequence), Some("i")) => max_incr = max_incr.max(sequence),
            _ => {}
        }
    }
    Ok((max_base, max_incr))
}

fn read_or_upgrade_legacy(
    manifest_path: &Path,
    app_dir: &Path,
    dir: &str,
    filename: &str,
) -> io::Result<Option<String>> {
    match std::fs::read_to_string(manifest_path) {
        Ok(content) => Ok(Some(content)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let old_path = Path::new(dir).join(filename);
            if !old_path.exists() {
                return Ok(None);
            }
            std::fs::create_dir_all(app_dir)?;
            // The legacy file must remain the final replay input until load
            // succeeds. Startup creates the new increment after recovery.
            let content = format!("file {} seq 1 type b\n", format_manifest_token(filename));
            std::fs::write(manifest_path, &content)?;
            // Persist first so an interrupted move can be resumed from the
            // base-only manifest by migrate_interrupted_legacy.
            std::fs::rename(&old_path, app_dir.join(filename))?;
            Ok(Some(content))
        }
        Err(error) => Err(error),
    }
}

fn migrate_interrupted_legacy(
    dir: &str,
    configured_filename: &str,
    entry_filename: &str,
    destination: &Path,
) -> io::Result<()> {
    if destination.exists() || entry_filename != configured_filename {
        return Ok(());
    }
    let old_path = Path::new(dir).join(configured_filename);
    if old_path.exists() {
        std::fs::rename(old_path, destination)?;
    }
    Ok(())
}

fn append_manifest_entry(
    manifest_path: &Path,
    old_content: &str,
    incr_name: &str,
    sequence: u64,
) -> io::Result<()> {
    let mut manifest = OpenOptions::new().append(true).open(manifest_path)?;
    if !old_content.ends_with('\n') {
        manifest.write_all(b"\n")?;
    }
    writeln!(
        manifest,
        "file {} seq {} type i",
        format_manifest_token(incr_name),
        sequence
    )
}

fn tokenize(line: &str) -> io::Result<Vec<String>> {
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

fn parse_entry(line: &str) -> io::Result<AofManifestEntry> {
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

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
