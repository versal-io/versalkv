use std::fs::OpenOptions;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

// The host-neutral manifest text parsing/formatting lives in
// `persistence::manifest_parse` so it compiles on `wasm`. Re-export the public
// items and reuse the private helpers for the file-backed functions below.
pub use crate::foundation::io::persistence::manifest_parse::{
    format_manifest_token, parse_manifest_bytes, AofManifestEntry,
};
use crate::foundation::io::persistence::manifest_parse::{invalid_data, parse_entry, tokenize};

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
            std::fs::rename(&old_path, app_dir.join(filename))?;
            let incr_name = format!("{}.1.incr.aof", filename);
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(app_dir.join(&incr_name))?;
            let content = format!(
                "file {} seq 1 type b\nfile {} seq 1 type i\n",
                format_manifest_token(filename),
                format_manifest_token(&incr_name)
            );
            std::fs::write(manifest_path, &content)?;
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
