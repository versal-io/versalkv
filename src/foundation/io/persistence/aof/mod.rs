mod manifest;
mod manifest_install;
#[cfg(test)]
mod manifest_tests;
pub mod rewrite;

pub use manifest::{
    ensure_aof_increment, format_manifest_token, gc_aof_history, next_rewrite_sequences,
    parse_manifest_bytes, read_manifest_entries, resolve_aof_read_paths_checked,
    rotate_aof_increment, AofManifestEntry,
};
pub use manifest_install::{
    install_synced_rdb_as_aof_base, prepare_synced_rdb_as_aof_base, PreparedSyncedRdbAofBase,
};

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AofFsync {
    No,
    EverySec,
    Always,
}

#[allow(clippy::should_implement_trait)]
impl AofFsync {
    pub fn from_str(s: &str) -> Self {
        match s.to_ascii_lowercase().as_str() {
            "no" => AofFsync::No,
            "always" => AofFsync::Always,
            _ => AofFsync::EverySec,
        }
    }
    pub fn as_str(&self) -> &'static str {
        match self {
            AofFsync::No => "no",
            AofFsync::EverySec => "everysec",
            AofFsync::Always => "always",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AofOpenMode {
    CreateNew,
    AppendExisting,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AofLoadStats {
    pub cmd_count: u64,
    pub valid_up_to: usize,
}

#[derive(Debug)]
pub enum AofLoadError {
    Corrupt,
    ExecError(String),
    Io(io::Error),
}

pub trait AofCommandPlayer {
    fn select_db(&mut self, dbid: u32);
    fn execute(&mut self, args: &[&[u8]]) -> Result<(), String>;
}

struct FsyncThread {
    handle: Option<JoinHandle<()>>,
    shutdown: Arc<AtomicBool>,
    final_sync: Arc<AtomicBool>,
    done: Arc<(Mutex<bool>, Condvar)>,
}

impl FsyncThread {
    fn spawn(fd: File) -> Self {
        let shutdown = Arc::new(AtomicBool::new(false));
        let final_sync = Arc::new(AtomicBool::new(true));
        let done = Arc::new((Mutex::new(false), Condvar::new()));

        let th_shutdown = Arc::clone(&shutdown);
        let th_final_sync = Arc::clone(&final_sync);
        let th_done = Arc::clone(&done);

        let handle = thread::Builder::new()
            .name("aof-fsync".to_string())
            .spawn(move || {
                let mut last_fsync = crate::foundation::io::store::kv_engine::now_ms();
                loop {
                    if th_shutdown.load(Ordering::Acquire) {
                        break;
                    }
                    let now = crate::foundation::io::store::kv_engine::now_ms();
                    let elapsed = now.saturating_sub(last_fsync);
                    if elapsed >= 1000 {
                        let _ = fd.sync_data();
                        last_fsync = crate::foundation::io::store::kv_engine::now_ms();
                    }
                    let sleep_ms = if elapsed >= 1000 { 1 } else { 1000 - elapsed };
                    let wait_ms = sleep_ms.min(100);
                    thread::sleep(std::time::Duration::from_millis(wait_ms));
                }
                if th_final_sync.load(Ordering::Acquire) {
                    let _ = fd.sync_data();
                }
                let (lock, cvar) = &*th_done;
                if let Ok(mut done_flag) = lock.lock() {
                    *done_flag = true;
                    cvar.notify_one();
                }
            })
            .expect("failed to spawn aof-fsync thread");

        FsyncThread {
            handle: Some(handle),
            shutdown,
            final_sync,
            done,
        }
    }

    fn stop(&mut self, final_sync: bool) {
        self.final_sync.store(final_sync, Ordering::Release);
        self.shutdown.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            let (lock, cvar) = &*self.done;
            if let Ok(done_flag) = lock.lock() {
                if !*done_flag {
                    let _ = cvar
                        .wait_timeout_while(done_flag, std::time::Duration::from_secs(2), |done| {
                            !*done
                        })
                        .unwrap();
                }
            }
            let _ = handle.join();
        }
    }
}

impl Drop for FsyncThread {
    fn drop(&mut self) {
        self.stop(true);
    }
}

// ======================== AofAppender ========================

pub struct AofAppender {
    fd: File,
    fsync_mode: AofFsync,

    buf: Vec<u8>,

    current_size: u64,

    last_write_ok: bool,

    fsync_thread: Option<FsyncThread>,

    path: String,
}

impl AofAppender {
    fn from_open_file(fd: File, path: &Path, fsync_mode: AofFsync) -> io::Result<Self> {
        let current_size = fd.metadata()?.len();
        let fsync_thread = match fsync_mode {
            AofFsync::EverySec => Some(FsyncThread::spawn(fd.try_clone()?)),
            _ => None,
        };
        Ok(Self {
            fd,
            fsync_mode,
            buf: Vec::with_capacity(8 * 1024),
            current_size,
            last_write_ok: true,
            fsync_thread,
            path: path.to_string_lossy().into_owned(),
        })
    }

    pub fn open_append_path(path: &Path, fsync: AofFsync) -> io::Result<Self> {
        let fd = OpenOptions::new().create(true).append(true).open(path)?;
        Self::from_open_file(fd, path, fsync)
    }

    fn build_multi_file_path(
        dir: &str,
        filename: &str,
        appenddirname: &str,
        mode: AofOpenMode,
    ) -> (std::path::PathBuf, bool) {
        if appenddirname.is_empty() {
            (Path::new(dir).join(filename), false)
        } else if mode == AofOpenMode::AppendExisting {
            (resolve_aof_read_path(dir, filename, appenddirname), true)
        } else {
            let app_dir = Path::new(dir).join(appenddirname);
            let incr_name = format!("{}.1.incr.aof", filename);
            (app_dir.join(&incr_name), true)
        }
    }

    fn ensure_manifest(dir: &str, filename: &str, appenddirname: &str) -> io::Result<()> {
        if appenddirname.is_empty() {
            return Ok(());
        }
        let app_dir = Path::new(dir).join(appenddirname);
        let manifest_path = app_dir.join(format!("{}.manifest", filename));
        if !manifest_path.exists() {
            let incr_name = format!("{}.1.incr.aof", filename);
            let manifest_content =
                format!("file {} seq 1 type i\n", format_manifest_token(&incr_name));
            std::fs::write(&manifest_path, manifest_content)?;
        }
        Ok(())
    }

    pub fn open(
        dir: &str,
        filename: &str,
        appenddirname: &str,
        mode: AofOpenMode,
        fsync: AofFsync,
    ) -> io::Result<Self> {
        if !appenddirname.is_empty() {
            let app_dir = Path::new(dir).join(appenddirname);
            std::fs::create_dir_all(&app_dir)?;
            if mode == AofOpenMode::CreateNew {
                Self::ensure_manifest(dir, filename, appenddirname)?;
            }
        }
        let (path, _is_multi) = Self::build_multi_file_path(dir, filename, appenddirname, mode);
        let mut opts = OpenOptions::new();
        opts.write(true).create(true);
        match mode {
            AofOpenMode::CreateNew => {
                opts.truncate(true);
            }
            AofOpenMode::AppendExisting => {
                opts.append(true);
            }
        }
        let fd = opts.open(&path)?;
        Self::from_open_file(fd, &path, fsync)
    }

    pub fn feed(&mut self, frame: &[u8]) {
        self.buf.extend_from_slice(frame);
    }

    pub fn flush(&mut self, force: bool) -> io::Result<()> {
        if self.buf.is_empty() && !force {
            return Ok(());
        }
        if !self.buf.is_empty() {
            match self.fd.write_all(&self.buf) {
                Ok(()) => {
                    self.current_size += self.buf.len() as u64;
                    self.buf.clear();
                    self.last_write_ok = true;
                }
                Err(e) => {
                    self.last_write_ok = false;
                    return Err(e);
                }
            }
        }

        let need_fsync_now = force
            || match self.fsync_mode {
                AofFsync::Always => true,
                AofFsync::EverySec => false,
                AofFsync::No => false,
            };

        if need_fsync_now {
            match self.fd.sync_data() {
                Ok(()) => {
                    self.last_write_ok = true;
                }
                Err(e) => {
                    self.last_write_ok = false;
                    return Err(e);
                }
            }
        }
        Ok(())
    }

    pub fn force_fsync(&mut self) -> io::Result<()> {
        self.fd.sync_data()
    }

    pub fn set_fsync_mode(&mut self, mode: AofFsync) -> io::Result<()> {
        if mode == self.fsync_mode {
            return Ok(());
        }
        let old_was_bg = self.fsync_mode == AofFsync::EverySec;
        let new_is_bg = mode == AofFsync::EverySec;
        self.fsync_mode = mode;

        if old_was_bg && !new_is_bg {
            if let Some(mut th) = self.fsync_thread.take() {
                th.stop(false);
            }
        } else if new_is_bg && !old_was_bg {
            let th_fd = self.fd.try_clone()?;
            self.fsync_thread = Some(FsyncThread::spawn(th_fd));
        }
        Ok(())
    }

    pub fn fsync_mode(&self) -> AofFsync {
        self.fsync_mode
    }
    pub fn current_size(&self) -> u64 {
        self.current_size
    }
    pub fn buffer_allocation_bytes(&self) -> usize {
        self.buf.capacity()
    }
    pub fn last_write_ok(&self) -> bool {
        self.last_write_ok
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn shutdown(&mut self) -> io::Result<()> {
        if let Some(mut th) = self.fsync_thread.take() {
            th.stop(true);
        }
        self.flush(true)?;
        self.fd.sync_all()?;
        Ok(())
    }

    pub fn truncate(&mut self, size: u64) -> io::Result<()> {
        self.fd.set_len(size)?;
        self.current_size = size;
        Ok(())
    }
}

pub fn resolve_aof_read_path(dir: &str, filename: &str, appenddirname: &str) -> std::path::PathBuf {
    if appenddirname.is_empty() {
        return Path::new(dir).join(filename);
    }
    if let Ok(paths) = resolve_aof_read_paths_checked(dir, filename, appenddirname) {
        if let Some(path) = paths.last() {
            return path.clone();
        }
    }
    let incr_name = format!("{}.1.incr.aof", filename);
    Path::new(dir).join(appenddirname).join(&incr_name)
}

pub fn resolve_aof_read_paths(
    dir: &str,
    filename: &str,
    appenddirname: &str,
) -> Vec<std::path::PathBuf> {
    if appenddirname.is_empty() {
        return vec![Path::new(dir).join(filename)];
    }
    let app_dir = Path::new(dir).join(appenddirname);
    let manifest_path = app_dir.join(format!("{}.manifest", filename));
    if let Ok(content) = std::fs::read_to_string(&manifest_path) {
        let mut entries = Vec::new();
        for line in content.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 6
                && parts[0] == "file"
                && parts[2] == "seq"
                && parts[4] == "type"
                && matches!(parts[5], "b" | "i")
            {
                entries.push(app_dir.join(parts[1]));
            }
        }
        if !entries.is_empty() {
            return entries;
        }
    }
    vec![resolve_aof_read_path(dir, filename, appenddirname)]
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AofStep {
    Command(Vec<Vec<u8>>),
    Finished(AofLoadStats),
    Truncated(AofLoadStats),
}

#[derive(Default)]
pub struct AofDecoder {
    pos: usize,
    stats: AofLoadStats,
    finished: bool,
}

impl AofDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn position(&self) -> usize {
        self.pos
    }

    pub fn step(&mut self, bytes: &[u8]) -> Result<AofStep, AofLoadError> {
        if self.finished {
            return Ok(AofStep::Finished(self.stats));
        }
        while self.pos < bytes.len() {
            while self.pos < bytes.len() && (bytes[self.pos] == b'\r' || bytes[self.pos] == b'\n') {
                self.pos += 1;
            }
            if self.pos >= bytes.len() {
                break;
            }
            if bytes[self.pos] == b'#' {
                while self.pos < bytes.len() && bytes[self.pos] != b'\n' {
                    self.pos += 1;
                }
                continue;
            }
            self.stats.valid_up_to = self.pos;
            if bytes[self.pos] != b'*' {
                return Err(AofLoadError::Corrupt);
            }
            let Some((array_len, mut cursor)) = parse_int_line(bytes, self.pos + 1) else {
                self.finished = true;
                return Ok(AofStep::Truncated(self.stats));
            };
            if array_len <= 0 {
                return Err(AofLoadError::Corrupt);
            }
            let mut parts = Vec::with_capacity(array_len as usize);
            for _ in 0..array_len {
                if cursor >= bytes.len() || bytes[cursor] != b'$' {
                    self.finished = true;
                    return Ok(AofStep::Truncated(self.stats));
                }
                let Some((bulk_len, next)) = parse_int_line(bytes, cursor + 1) else {
                    self.finished = true;
                    return Ok(AofStep::Truncated(self.stats));
                };
                if bulk_len < 0 {
                    self.finished = true;
                    return Ok(AofStep::Truncated(self.stats));
                }
                cursor = next;
                let end = cursor
                    .checked_add(bulk_len as usize)
                    .ok_or(AofLoadError::Corrupt)?;
                if end + 2 > bytes.len() {
                    self.finished = true;
                    return Ok(AofStep::Truncated(self.stats));
                }
                if bytes[end] != b'\r' || bytes[end + 1] != b'\n' {
                    return Err(AofLoadError::Corrupt);
                }
                parts.push(bytes[cursor..end].to_vec());
                cursor = end + 2;
            }
            self.pos = cursor;
            self.stats.cmd_count += 1;
            self.stats.valid_up_to = self.pos;
            return Ok(AofStep::Command(parts));
        }
        self.stats.valid_up_to = bytes.len();
        self.finished = true;
        Ok(AofStep::Finished(self.stats))
    }
}

pub fn load_aof_bytes<P: AofCommandPlayer + ?Sized>(
    bytes: &[u8],
    player: &mut P,
) -> Result<AofLoadStats, AofLoadError> {
    let mut decoder = AofDecoder::new();
    loop {
        match decoder.step(bytes)? {
            AofStep::Command(parts) => {
                let args: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
                player.execute(&args).map_err(AofLoadError::ExecError)?;
            }
            AofStep::Finished(stats) | AofStep::Truncated(stats) => return Ok(stats),
        }
    }
}

fn parse_int_line(bytes: &[u8], start: usize) -> Option<(i64, usize)> {
    let mut pos = start;
    let mut sign: i64 = 1;
    let mut val: i64 = 0;
    if pos < bytes.len() && bytes[pos] == b'-' {
        sign = -1;
        pos += 1;
    }
    let mut any = false;
    while pos < bytes.len() && bytes[pos] != b'\r' {
        let c = bytes[pos];
        if !c.is_ascii_digit() {
            return None;
        }
        val = val.checked_mul(10)?.checked_add((c - b'0') as i64)?;
        any = true;
        pos += 1;
    }
    if !any {
        return None;
    }
    if pos + 2 > bytes.len() || bytes[pos] != b'\r' || bytes[pos + 1] != b'\n' {
        return None;
    }
    Some((val * sign, pos + 2))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockPlayer {
        db: u32,
        cmds: Vec<(u32, Vec<Vec<u8>>)>,
    }
    impl AofCommandPlayer for MockPlayer {
        fn select_db(&mut self, dbid: u32) {
            self.db = dbid;
        }
        fn execute(&mut self, args: &[&[u8]]) -> Result<(), String> {
            if !args.is_empty() && args[0].eq_ignore_ascii_case(b"select") {
                if args.len() >= 2 {
                    if let Ok(d) = std::str::from_utf8(args[1]).unwrap_or("").parse::<u32>() {
                        self.db = d;
                    }
                }
                return Ok(());
            }
            self.cmds
                .push((self.db, args.iter().map(|a| a.to_vec()).collect()));
            Ok(())
        }
    }

    struct ErrorPlayer;
    impl AofCommandPlayer for ErrorPlayer {
        fn select_db(&mut self, _dbid: u32) {}
        fn execute(&mut self, _args: &[&[u8]]) -> Result<(), String> {
            Err("boom".to_string())
        }
    }

    fn temp_test_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("valkey-aof-{}-{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn parse_select_and_set() {
        // SELECT 0 + SET foo bar
        let buf = b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let mut p = MockPlayer {
            db: 0,
            cmds: vec![],
        };
        let stats = load_aof_bytes(buf, &mut p).unwrap();
        assert_eq!(stats.cmd_count, 2);
        assert_eq!(p.cmds.len(), 1);
        assert_eq!(p.cmds[0].0, 0);
        assert_eq!(p.cmds[0].1[0], b"SET");
        assert_eq!(p.cmds[0].1[1], b"foo");
        assert_eq!(p.cmds[0].1[2], b"bar");
    }

    #[test]
    fn truncated_last_command_is_tolerated() {
        let buf = b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nba";
        let mut p = MockPlayer {
            db: 0,
            cmds: vec![],
        };
        let stats = load_aof_bytes(buf, &mut p).unwrap();
        assert_eq!(stats.cmd_count, 0);
    }

    #[test]
    fn multi_db_select() {
        let buf = b"*2\r\n$6\r\nSELECT\r\n$1\r\n2\r\n*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n\
                    *2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n";
        let mut p = MockPlayer {
            db: 0,
            cmds: vec![],
        };
        let stats = load_aof_bytes(buf, &mut p).unwrap();
        assert_eq!(stats.cmd_count, 4);
        assert_eq!(p.cmds[0].0, 2);
        assert_eq!(p.cmds[0].1[1], b"a");
        assert_eq!(p.cmds[1].0, 0);
        assert_eq!(p.cmds[1].1[1], b"b");
    }

    #[test]
    fn parse_multi_exec_frames() {
        // MULTI + SELECT 0 + SET a 1 + SET b 2 + EXEC
        let buf = b"*1\r\n$5\r\nMULTI\r\n\
                    *2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n\
                    *3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n\
                    *3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n\
                    *1\r\n$4\r\nEXEC\r\n";
        let mut p = MockPlayer {
            db: 0,
            cmds: vec![],
        };
        let stats = load_aof_bytes(buf, &mut p).unwrap();
        assert_eq!(stats.cmd_count, 5);
        assert_eq!(p.cmds.len(), 4);
        assert_eq!(p.cmds[0].1[0], b"MULTI");
        assert_eq!(p.cmds[1].0, 0);
        assert_eq!(p.cmds[1].1[0], b"SET");
        assert_eq!(p.cmds[1].1[1], b"a");
        assert_eq!(p.cmds[2].1[0], b"SET");
        assert_eq!(p.cmds[2].1[1], b"b");
        assert_eq!(p.cmds[3].1[0], b"EXEC");
        assert_eq!(p.db, 0);
    }

    #[test]
    fn fsync_mode_from_str() {
        assert_eq!(AofFsync::from_str("always"), AofFsync::Always);
        assert_eq!(AofFsync::from_str("no"), AofFsync::No);
        assert_eq!(AofFsync::from_str("EVERYSEC"), AofFsync::EverySec);
        assert_eq!(AofFsync::from_str("unknown"), AofFsync::EverySec);
        assert_eq!(AofFsync::No.as_str(), "no");
        assert_eq!(AofFsync::EverySec.as_str(), "everysec");
        assert_eq!(AofFsync::Always.as_str(), "always");
    }

    #[test]
    fn resolve_aof_read_path_uses_single_file_manifest_and_fallback_paths() {
        let dir = temp_test_dir("resolve");
        assert_eq!(
            resolve_aof_read_path(dir.to_str().unwrap(), "appendonly.aof", ""),
            dir.join("appendonly.aof")
        );

        assert_eq!(
            resolve_aof_read_path(dir.to_str().unwrap(), "appendonly.aof", "appendonlydir"),
            dir.join("appendonlydir").join("appendonly.aof.1.incr.aof")
        );

        let app_dir = dir.join("appendonlydir");
        std::fs::create_dir_all(&app_dir).unwrap();
        std::fs::write(
            app_dir.join("appendonly.aof.manifest"),
            "file appendonly.aof.1.base.rdb seq 1 type b\n\
             file appendonly.aof.2.incr.aof seq 2 type i\n\
             file appendonly.aof.3.incr.aof seq 3 type i\n",
        )
        .unwrap();
        for filename in [
            "appendonly.aof.1.base.rdb",
            "appendonly.aof.2.incr.aof",
            "appendonly.aof.3.incr.aof",
        ] {
            std::fs::write(app_dir.join(filename), b"").unwrap();
        }

        assert_eq!(
            resolve_aof_read_path(dir.to_str().unwrap(), "appendonly.aof", "appendonlydir"),
            app_dir.join("appendonly.aof.3.incr.aof")
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn resolve_aof_read_path_does_not_trust_invalid_manifest_filename() {
        let dir = temp_test_dir("resolve-invalid");
        let app_dir = dir.join("appendonlydir");
        std::fs::create_dir_all(&app_dir).unwrap();
        std::fs::write(
            app_dir.join("appendonly.aof.manifest"),
            "file ../../outside.aof seq 1 type i\n",
        )
        .unwrap();

        assert_eq!(
            resolve_aof_read_path(dir.to_str().unwrap(), "appendonly.aof", "appendonlydir"),
            app_dir.join("appendonly.aof.1.incr.aof")
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn resolve_aof_read_paths_preserves_manifest_base_then_incr_order() {
        let dir = temp_test_dir("resolve-sequence");
        let app_dir = dir.join("appendonlydir");
        std::fs::create_dir_all(&app_dir).unwrap();
        std::fs::write(
            app_dir.join("appendonly.aof.manifest"),
            "file appendonly.aof.1.base.rdb seq 1 type b\n\
             file appendonly.aof.2.incr.aof seq 2 type i\n",
        )
        .unwrap();

        let paths =
            resolve_aof_read_paths(dir.to_str().unwrap(), "appendonly.aof", "appendonlydir");
        assert_eq!(
            paths,
            vec![
                app_dir.join("appendonly.aof.1.base.rdb"),
                app_dir.join("appendonly.aof.2.incr.aof"),
            ]
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn appender_append_existing_uses_latest_manifest_incr_file() {
        let dir = temp_test_dir("append-existing-manifest");
        let app_dir = dir.join("appendonlydir");
        std::fs::create_dir_all(&app_dir).unwrap();
        std::fs::write(
            app_dir.join("appendonly.aof.manifest"),
            "file appendonly.aof.1.base.rdb seq 1 type b\n\
             file appendonly.aof.2.incr.aof seq 2 type i\n",
        )
        .unwrap();
        std::fs::write(app_dir.join("appendonly.aof.1.base.rdb"), b"").unwrap();
        std::fs::write(app_dir.join("appendonly.aof.2.incr.aof"), b"").unwrap();

        let appender = AofAppender::open(
            dir.to_str().unwrap(),
            "appendonly.aof",
            "appendonlydir",
            AofOpenMode::AppendExisting,
            AofFsync::No,
        )
        .unwrap();
        assert!(appender.path().ends_with("appendonly.aof.2.incr.aof"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn appender_reports_owned_buffer_allocation() {
        let dir = temp_test_dir("buffer-allocation");
        let mut appender = AofAppender::open(
            dir.to_str().unwrap(),
            "appendonly.aof",
            "",
            AofOpenMode::CreateNew,
            AofFsync::No,
        )
        .unwrap();

        let initial = appender.buffer_allocation_bytes();
        assert!(initial >= 8 * 1024);
        appender.feed(&vec![b'x'; 32 * 1024]);
        assert!(appender.buffer_allocation_bytes() >= 32 * 1024);

        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn corrupt_aof_frames_and_player_errors_are_reported() {
        let corrupt_inputs: [&[u8]; 4] =
            [b"not-array\r\n", b"*0\r\n", b"*-1\r\n", b"*1\r\n$1\r\naXX"];

        for input in corrupt_inputs {
            let mut p = MockPlayer {
                db: 0,
                cmds: vec![],
            };
            assert!(
                matches!(load_aof_bytes(input, &mut p), Err(AofLoadError::Corrupt)),
                "input {:?} should be corrupt",
                String::from_utf8_lossy(input)
            );
        }

        for input in [b"*1\r\n$-1\r\n".as_slice(), b"*1\r\n$abc\r\n".as_slice()] {
            let mut p = MockPlayer {
                db: 0,
                cmds: vec![],
            };
            assert_eq!(load_aof_bytes(input, &mut p).unwrap().cmd_count, 0);
        }

        let mut player = ErrorPlayer;
        let err = load_aof_bytes(b"*1\r\n$3\r\nSET\r\n", &mut player).unwrap_err();
        assert!(matches!(err, AofLoadError::ExecError(msg) if msg == "boom"));
    }

    #[test]
    fn parse_int_line_rejects_missing_digits_crlf_and_overflow() {
        assert_eq!(parse_int_line(b"123\r\n", 0), Some((123, 5)));
        assert_eq!(parse_int_line(b"-5\r\n", 0), Some((-5, 4)));
        assert_eq!(parse_int_line(b"\r\n", 0), None);
        assert_eq!(parse_int_line(b"12x\r\n", 0), None);
        assert_eq!(parse_int_line(b"12\n", 0), None);
        assert_eq!(parse_int_line(b"9223372036854775808\r\n", 0), None);
    }
}
