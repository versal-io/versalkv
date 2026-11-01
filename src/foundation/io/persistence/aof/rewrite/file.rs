use super::encode::{write_command_snapshot, write_function_snapshot};
use super::AofRewriteFileOptions;
use crate::foundation::io::store::kv_engine::DbSet;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::Path;

pub(super) const AOF_REWRITE_AUTOSYNC_BYTES: usize = 32 * 1024 * 1024;

struct IncrementalSyncWriter<W, F>
where
    W: Write,
    F: FnMut(&mut W) -> std::io::Result<()>,
{
    inner: W,
    incremental_sync: bool,
    bytes_since_sync: usize,
    sync: F,
}

impl<W, F> IncrementalSyncWriter<W, F>
where
    W: Write,
    F: FnMut(&mut W) -> std::io::Result<()>,
{
    fn new(inner: W, incremental_sync: bool, sync: F) -> Self {
        Self {
            inner,
            incremental_sync,
            bytes_since_sync: 0,
            sync,
        }
    }

    fn finish(mut self) -> std::io::Result<W> {
        self.flush()?;
        Ok(self.inner)
    }

    fn sync_completed_intervals(&mut self) -> std::io::Result<()> {
        while self.incremental_sync && self.bytes_since_sync >= AOF_REWRITE_AUTOSYNC_BYTES {
            (self.sync)(&mut self.inner)?;
            self.bytes_since_sync -= AOF_REWRITE_AUTOSYNC_BYTES;
        }
        Ok(())
    }
}

impl<W, F> Write for IncrementalSyncWriter<W, F>
where
    W: Write,
    F: FnMut(&mut W) -> std::io::Result<()>,
{
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let written = self.inner.write(buf)?;
        self.bytes_since_sync = self.bytes_since_sync.saturating_add(written);
        self.sync_completed_intervals()?;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

pub fn write_rewrite_temp(
    dbs: &DbSet,
    function_codes: &[Vec<u8>],
    migration_snapshot: &[u8],
    options: &AofRewriteFileOptions,
    pid: i32,
) -> std::io::Result<()> {
    let aof_dir = options.aof_dir();
    std::fs::create_dir_all(&aof_dir)?;
    let tmp_path = options.temp_path(pid);
    let result = write_temp_contents(dbs, function_codes, migration_snapshot, options, &tmp_path);
    if result.is_err() {
        let _ = std::fs::remove_file(tmp_path);
    }
    result
}

fn write_temp_contents(
    dbs: &DbSet,
    function_codes: &[Vec<u8>],
    migration_snapshot: &[u8],
    options: &AofRewriteFileOptions,
    tmp_path: &Path,
) -> std::io::Result<()> {
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(tmp_path)?;
    let mut file =
        IncrementalSyncWriter::new(file, options.incremental_fsync, |file| file.sync_data());
    if options.use_rdb_preamble {
        file.write_all(
            &crate::foundation::io::persistence::rdb::serialize::serialize_replication_snapshot(
                dbs,
                options.key_delay_us,
                &[],
                true,
                None,
                &[],
                crate::foundation::io::persistence::rdb::compression::RdbSnapshotOptions {
                    compression: options.rdb_compression,
                    checksum: options.rdb_checksum,
                },
            ),
        )?;
    } else {
        if options.timestamp_enabled {
            write_timestamp_annotation(&mut file)?;
        }
        write_command_snapshot(dbs, &mut file, options.key_delay_us)?;
    }
    file.write_all(migration_snapshot)?;
    write_function_snapshot(function_codes, &mut file)?;
    let file = file.finish()?;
    file.sync_all()
}

fn write_timestamp_annotation(writer: &mut impl Write) -> std::io::Result<()> {
    write!(
        writer,
        "#TS:{}\r\n",
        crate::foundation::io::store::kv_engine::now_ms() / 1_000
    )
}

pub fn finalize_rewrite(
    options: &AofRewriteFileOptions,
    pid: i32,
    rewrite_buffer: &[u8],
    append_enabled: bool,
    disable_auto_gc: bool,
    increment_rotated: bool,
) -> std::io::Result<()> {
    let tmp_path = options.temp_path(pid);
    let result = if options.appenddirname.is_empty() {
        finalize_single_file(options, &tmp_path, rewrite_buffer)
    } else {
        finalize_multi_part(
            options,
            &tmp_path,
            rewrite_buffer,
            append_enabled,
            disable_auto_gc,
            increment_rotated,
        )
    };
    if result.is_err() {
        let _ = std::fs::remove_file(tmp_path);
    }
    result
}

fn finalize_single_file(
    options: &AofRewriteFileOptions,
    tmp_path: &Path,
    rewrite_buffer: &[u8],
) -> std::io::Result<()> {
    let mut file = OpenOptions::new().append(true).open(tmp_path)?;
    file.write_all(rewrite_buffer)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(tmp_path, options.dir.join(&options.filename))
}

fn finalize_multi_part(
    options: &AofRewriteFileOptions,
    tmp_path: &Path,
    rewrite_buffer: &[u8],
    append_enabled: bool,
    disable_auto_gc: bool,
    increment_rotated: bool,
) -> std::io::Result<()> {
    let app_dir = options.aof_dir();
    std::fs::create_dir_all(&app_dir)?;
    let base_ext = if temp_uses_rdb_preamble(tmp_path)? {
        "rdb"
    } else {
        "aof"
    };
    let manifest_path = app_dir.join(format!("{}.manifest", options.filename));
    let old_entries = crate::foundation::io::persistence::read_manifest_entries(&manifest_path)
        .unwrap_or_default();
    let (base_sequence, incr_sequence) =
        crate::foundation::io::persistence::next_rewrite_sequences(
            &manifest_path,
            increment_rotated,
        )?;
    let base_name = format!("{}.{}.base.{}", options.filename, base_sequence, base_ext);
    let incr_name = format!("{}.{}.incr.aof", options.filename, incr_sequence);

    std::fs::rename(tmp_path, app_dir.join(&base_name))?;
    let mut manifest = if append_enabled {
        if !increment_rotated {
            write_increment(&app_dir.join(&incr_name), rewrite_buffer)?;
        }
        format!(
            "file {} seq {} type b\nfile {} seq {} type i\n",
            crate::foundation::io::persistence::format_manifest_token(&base_name),
            base_sequence,
            crate::foundation::io::persistence::format_manifest_token(&incr_name),
            incr_sequence
        )
    } else {
        format!(
            "file {} seq {} type b\n",
            crate::foundation::io::persistence::format_manifest_token(&base_name),
            base_sequence
        )
    };
    if disable_auto_gc {
        manifest = format!(
            "file {} seq {} type b\n",
            crate::foundation::io::persistence::format_manifest_token(&base_name),
            base_sequence
        );
        for kind in ['i', 'b', 'h'] {
            for entry in old_entries
                .iter()
                .filter(|entry| entry.kind == kind && entry.filename != incr_name)
            {
                manifest.push_str(&format!(
                    "file {} seq {} type h\n",
                    crate::foundation::io::persistence::format_manifest_token(&entry.filename),
                    entry.sequence
                ));
            }
        }
        if append_enabled {
            manifest.push_str(&format!(
                "file {} seq {} type i\n",
                crate::foundation::io::persistence::format_manifest_token(&incr_name),
                incr_sequence
            ));
        }
    }
    write_manifest_atomically(&manifest_path, manifest.as_bytes())?;
    if !disable_auto_gc {
        for entry in old_entries {
            let old_path = app_dir.join(entry.filename);
            let keep = old_path == app_dir.join(&base_name)
                || (append_enabled && old_path == app_dir.join(&incr_name));
            if !keep {
                let _ = std::fs::remove_file(old_path);
            }
        }
    }
    Ok(())
}

fn temp_uses_rdb_preamble(path: &Path) -> std::io::Result<bool> {
    let mut file = std::fs::File::open(path)?;
    let mut prefix = [0u8; 128];
    let length = file.read(&mut prefix)?;
    let mut bytes = &prefix[..length];
    if bytes.starts_with(b"#TS:") {
        if let Some(newline) = bytes.iter().position(|byte| *byte == b'\n') {
            bytes = &bytes[newline + 1..];
        }
    }
    Ok(crate::foundation::io::persistence::is_rdb_magic(bytes))
}

fn write_increment(path: &Path, rewrite_buffer: &[u8]) -> std::io::Result<()> {
    let temp_path = path.with_file_name(format!(
        ".{}.tmp",
        path.file_name().unwrap_or_default().to_string_lossy()
    ));
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temp_path)?;
    file.write_all(rewrite_buffer)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(temp_path, path)
}

fn write_manifest_atomically(path: &Path, manifest: &[u8]) -> std::io::Result<()> {
    let temp_path = path.with_extension("manifest.tmp");
    let mut file = std::fs::File::create(&temp_path)?;
    file.write_all(manifest)?;
    file.sync_all()?;
    drop(file);
    std::fs::rename(temp_path, path)?;
    std::fs::File::open(path.parent().unwrap_or(Path::new(".")))?.sync_all()
}

#[cfg(test)]
mod tests {
    use super::{finalize_rewrite, write_rewrite_temp, IncrementalSyncWriter};
    use crate::foundation::io::persistence::aof_rewrite::AofRewriteFileOptions;
    use crate::foundation::io::store::kv_engine::DbSet;
    use crate::foundation::io::store::Robj;
    use std::io::Write;
    use std::path::PathBuf;

    fn temp_dir(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "valkey-aof-rewrite-{}-{}-{}",
            name,
            std::process::id(),
            crate::foundation::io::sys::time::mstime()
        ));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    fn options(dir: PathBuf, multipart: bool) -> AofRewriteFileOptions {
        AofRewriteFileOptions {
            dir,
            filename: "appendonly.aof".to_string(),
            appenddirname: if multipart {
                "appendonlydir".to_string()
            } else {
                String::new()
            },
            use_rdb_preamble: false,
            timestamp_enabled: false,
            key_delay_us: 0,
            rdb_compression:
                crate::foundation::io::persistence::rdb::compression::RdbCompression::Yes,
            rdb_checksum: true,
            incremental_fsync: true,
        }
    }

    #[test]
    fn incremental_rewrite_writer_syncs_each_completed_interval() {
        let mut syncs = 0usize;
        let payload = vec![0u8; super::AOF_REWRITE_AUTOSYNC_BYTES * 2 + 1];
        let mut writer = IncrementalSyncWriter::new(Vec::new(), true, |_| {
            syncs += 1;
            Ok(())
        });

        writer.write_all(&payload).expect("write rewrite payload");
        let output = writer.finish().expect("finish rewrite writer");

        assert_eq!(output, payload);
        assert_eq!(syncs, 2);
    }

    #[test]
    fn command_snapshot_and_single_file_finalize_include_incremental_tail() {
        let dir = temp_dir("single");
        let options = options(dir.clone(), false);
        let mut dbs = DbSet::new(1);
        dbs.get_mut(0)
            .unwrap()
            .set(b"key".to_vec(), Robj::string("value"));

        write_rewrite_temp(&dbs, &[], &[], &options, 41).unwrap();
        finalize_rewrite(&options, 41, b"*1\r\n$4\r\nPING\r\n", true, false, false).unwrap();

        let bytes = std::fs::read(dir.join("appendonly.aof")).unwrap();
        assert!(bytes.windows(3).any(|window| window == b"SET"));
        assert!(bytes.ends_with(b"*1\r\n$4\r\nPING\r\n"));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn rewrite_timestamp_annotation_uses_crlf() {
        let dir = temp_dir("timestamp");
        let mut options = options(dir.clone(), false);
        options.timestamp_enabled = true;
        let mut dbs = DbSet::new(1);
        dbs.get_mut(0)
            .unwrap()
            .set(b"key".to_vec(), Robj::string("value"));

        write_rewrite_temp(&dbs, &[], &[], &options, 43).unwrap();
        let bytes = std::fs::read(options.temp_path(43)).unwrap();
        let newline = bytes
            .windows(2)
            .position(|window| window == b"\r\n")
            .expect("timestamp annotation must end with CRLF");
        assert!(bytes[..newline].starts_with(b"#TS:"));
        assert_eq!(bytes.get(newline + 2), Some(&b'*'));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn codec_fix_timestamp_rdb_preamble_starts_at_byte_zero() {
        use crate::foundation::io::persistence::rdb::compression::{
            decode_rdb_stream, RdbCompression,
        };

        for compression in [RdbCompression::No, RdbCompression::Yes, RdbCompression::Lz4] {
            let dir = temp_dir(&format!("timestamp-preamble-{compression:?}"));
            let mut options = options(dir.clone(), true);
            options.use_rdb_preamble = true;
            options.timestamp_enabled = true;
            options.rdb_compression = compression;
            let mut dbs = DbSet::new(1);
            dbs.get_mut(0)
                .unwrap()
                .set(b"key".to_vec(), Robj::string("value"));
            write_rewrite_temp(&dbs, &[], &[], &options, 45).unwrap();
            let bytes = std::fs::read(options.temp_path(45)).unwrap();
            assert!(
                bytes.starts_with(if compression == RdbCompression::Lz4 {
                    b"VCS"
                } else {
                    b"VALKEY"
                }),
                "preamble signature at byte zero for {compression:?}"
            );
            if compression == RdbCompression::Lz4 {
                let decoded = decode_rdb_stream(&bytes, false).unwrap().unwrap();
                assert_eq!(decoded.physical_bytes, bytes.len());
                assert!(decoded.bytes.starts_with(b"VALKEY"));
            }
            let tail = b"#TS:100\r\n*1\r\n$4\r\nPING\r\n";
            finalize_rewrite(&options, 45, tail, true, false, false).unwrap();
            assert_eq!(
                std::fs::read(options.aof_dir().join("appendonly.aof.1.base.rdb")).unwrap(),
                bytes
            );
            assert_eq!(
                std::fs::read(options.aof_dir().join("appendonly.aof.1.incr.aof")).unwrap(),
                tail
            );
            std::fs::remove_dir_all(dir).unwrap();
        }
    }

    #[test]
    fn rdb_preamble_honors_rdb_compression_option() {
        let dir = temp_dir("rdb-compression");
        let mut options = options(dir.clone(), false);
        options.use_rdb_preamble = true;
        let value = "compressible-value-".repeat(64);
        let mut dbs = DbSet::new(1);
        dbs.get_mut(0)
            .unwrap()
            .set(b"key".to_vec(), Robj::string(value.clone()));

        options.rdb_compression =
            crate::foundation::io::persistence::rdb::compression::RdbCompression::No;
        write_rewrite_temp(&dbs, &[], &[], &options, 51).unwrap();
        let uncompressed = std::fs::read(options.temp_path(51)).unwrap();
        options.rdb_compression =
            crate::foundation::io::persistence::rdb::compression::RdbCompression::Yes;
        write_rewrite_temp(&dbs, &[], &[], &options, 52).unwrap();
        let compressed = std::fs::read(options.temp_path(52)).unwrap();

        assert!(compressed.len() < uncompressed.len());
        assert!(uncompressed
            .windows(value.len())
            .any(|window| window == value.as_bytes()));
        assert!(!compressed
            .windows(value.len())
            .any(|window| window == value.as_bytes()));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn multipart_finalize_writes_base_increment_and_manifest() {
        let dir = temp_dir("multipart");
        let options = options(dir.clone(), true);
        let dbs = DbSet::new(1);

        write_rewrite_temp(&dbs, &[], &[], &options, 42).unwrap();
        finalize_rewrite(&options, 42, b"tail", true, false, false).unwrap();

        let app_dir = dir.join("appendonlydir");
        assert!(app_dir.join("appendonly.aof.1.base.aof").exists());
        assert_eq!(
            std::fs::read(app_dir.join("appendonly.aof.1.incr.aof")).unwrap(),
            b"tail"
        );
        assert_eq!(
            std::fs::read_to_string(app_dir.join("appendonly.aof.manifest")).unwrap(),
            "file appendonly.aof.1.base.aof seq 1 type b\nfile appendonly.aof.1.incr.aof seq 1 type i\n"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn rotated_increment_is_not_truncated_during_finalize() {
        let dir = temp_dir("rotated-increment");
        let options = options(dir.clone(), true);
        let app_dir = dir.join("appendonlydir");
        std::fs::create_dir_all(&app_dir).unwrap();
        std::fs::write(app_dir.join("appendonly.aof.1.base.aof"), b"old").unwrap();
        std::fs::write(app_dir.join("appendonly.aof.1.incr.aof"), b"committed").unwrap();
        std::fs::write(
            app_dir.join("appendonly.aof.manifest"),
            "file appendonly.aof.1.base.aof seq 1 type b\n\
             file appendonly.aof.1.incr.aof seq 1 type i\n",
        )
        .unwrap();
        let dbs = DbSet::new(1);
        write_rewrite_temp(&dbs, &[], &[], &options, 44).unwrap();

        finalize_rewrite(&options, 44, b"replacement", true, false, true).unwrap();

        assert_eq!(
            std::fs::read(app_dir.join("appendonly.aof.1.incr.aof")).unwrap(),
            b"committed"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn rewrite_temp_includes_function_load_frames() {
        let dir = temp_dir("functions");
        let options = options(dir.clone(), false);
        let dbs = DbSet::new(1);
        let code = b"#!lua name=test\nserver.register_function('test', function() return 1 end)";

        write_rewrite_temp(&dbs, &[code.to_vec()], &[], &options, 43).unwrap();

        let bytes = std::fs::read(options.temp_path(43)).unwrap();
        let mut expected =
            format!("*3\r\n$8\r\nFUNCTION\r\n$4\r\nLOAD\r\n${}\r\n", code.len()).into_bytes();
        expected.extend_from_slice(code);
        expected.extend_from_slice(b"\r\n");
        assert!(bytes.ends_with(&expected));
        let _ = std::fs::remove_dir_all(dir);
    }
}
