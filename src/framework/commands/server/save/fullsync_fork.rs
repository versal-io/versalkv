
use super::{child_after_fork, prepare_bgsave_cpu_affinity, ServerContext};
use crate::foundation::io::sys::fd::{pipe, PipeFlags};
use crate::foundation::io::sys::process::{
    exit_immediately, send_signal, wait_blocking, wait_nonblocking, ProcessSignal, WaitOutcome,
};
use crate::framework::capabilities::process::{fork_process, ForkBranch};
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::os::unix::io::{AsRawFd, RawFd};
use std::path::Path;

const DISK_RDB_FILE_BUFFER_BYTES: usize = 128 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskSyncChildExit {
    Running,
    Succeeded,
    Failed,
    NotRunning,
}

pub struct DisklessPipeChild {
    pub pid: i32,
    pub data_read_end: File,
    pub child_release_write_end: File,
}

pub(super) struct ReplicationRdbSource<'a> {
    dbs: &'a crate::foundation::io::store::kv_engine::DbSet,
    key_delay_us: i64,
    serialization: crate::foundation::io::persistence::RdbSerializeOptions,
    function_codes: &'a [Vec<u8>],
    replication: &'a crate::foundation::io::persistence::RdbReplicationInfo,
    additional_datasets: &'a [&'a crate::foundation::io::store::kv_engine::DbSet],
}

impl<'a> ReplicationRdbSource<'a> {
    pub(super) fn new(
        dbs: &'a crate::foundation::io::store::kv_engine::DbSet,
        key_delay_us: i64,
        serialization: crate::foundation::io::persistence::RdbSerializeOptions,
        function_codes: &'a [Vec<u8>],
        replication: &'a crate::foundation::io::persistence::RdbReplicationInfo,
        additional_datasets: &'a [&'a crate::foundation::io::store::kv_engine::DbSet],
    ) -> Self {
        Self {
            dbs,
            key_delay_us,
            serialization,
            function_codes,
            replication,
            additional_datasets,
        }
    }
}

pub(super) fn write_replication_rdb_streaming(
    source: ReplicationRdbSource<'_>,
    writer: &mut impl Write,
) -> std::io::Result<u64> {
    let mut bytes_written = 0u64;
    let mut write_error = None;
    let completed =
        crate::foundation::io::persistence::serialize_all_with_functions_and_replication_streaming_with_options(
            source.dbs,
            source.key_delay_us,
            source.function_codes,
            true,
            Some(source.replication),
            source.additional_datasets,
            source.serialization,
            |chunk| match writer.write_all(chunk) {
                Ok(()) => {
                    bytes_written = bytes_written.saturating_add(chunk.len() as u64);
                    true
                }
                Err(error) => {
                    write_error = Some(error);
                    false
                }
            },
        );
    if completed {
        Ok(bytes_written)
    } else {
        Err(write_error.unwrap_or_else(|| std::io::Error::other("RDB streaming write failed")))
    }
}

#[cfg(target_os = "linux")]
fn arm_parent_death_signal(expected_parent: libc::pid_t) -> bool {
    crate::framework::capabilities::process::arm_parent_death_signal(expected_parent)
}

#[cfg(not(target_os = "linux"))]
fn arm_parent_death_signal(expected_parent: libc::pid_t) -> bool {
    crate::foundation::io::sys::process::parent_process_id() == expected_parent
}

fn exit_child_if_parent_is_gone(expected_parent: libc::pid_t) {
    if arm_parent_death_signal(expected_parent) {
        return;
    }
    crate::foundation::io::sys::process::exit_immediately(1)
}

/// Mirror C's `startBgsaveForReplication`, which calls `debugPauseProcess()` in
/// the parent immediately after forking the replication RDB child when
/// `debug pause-after-fork` is set (replication.c). This SIGSTOPs the parent so
/// tests can observe the primary paused (`T` state) right after fork, then
/// resume it. Only the replication full-sync fork paths (disk / diskless /
/// direct-dual) call this — never plain `BGSAVE` — matching C, where the pause
/// lives in `startBgsaveForReplication` rather than the generic BGSAVE path.
/// Returns `true` when the parent was paused (and has since been continued), so
/// callers can log the crossing without duplicating the flag check.
pub fn pause_parent_after_replication_fork(ctx: &ServerContext) -> bool {
    if !ctx.config().server.debug_pause_after_fork {
        return false;
    }
    let _ = crate::framework::capabilities::process::stop_current_process();
    true
}

/// Progress markers emitted by a direct dual-channel RDB child. They cross the
/// fork boundary through a non-blocking pipe so the parent can distinguish a
/// stalled socket write from stalled serialization without child-side logging.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DirectDualRdbChildPhase {
    Forked,
    Started,
    HeaderSent,
    SerializeDone,
    MarkerSent,
}

impl DirectDualRdbChildPhase {
    pub(crate) const fn as_byte(self) -> u8 {
        match self {
            Self::Forked => b'F',
            Self::Started => b'B',
            Self::HeaderSent => b'H',
            Self::SerializeDone => b'S',
            Self::MarkerSent => b'M',
        }
    }

    pub(crate) const fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            b'F' => Some(Self::Forked),
            b'B' => Some(Self::Started),
            b'H' => Some(Self::HeaderSent),
            b'S' => Some(Self::SerializeDone),
            b'M' => Some(Self::MarkerSent),
            _ => None,
        }
    }
}

/// Best-effort, non-blocking collection of child progress. The direct child
/// writes at most three bytes, so draining to `WouldBlock` cannot monopolize
/// the primary event loop.
pub(crate) fn drain_direct_dual_rdb_phase_pipe(
    read_end: &mut File,
) -> Vec<DirectDualRdbChildPhase> {
    let mut phases = Vec::new();
    let mut buf = [0u8; 16];
    loop {
        match read_end.read(&mut buf) {
            Ok(0) => return phases,
            Ok(n) => phases.extend(
                buf[..n]
                    .iter()
                    .filter_map(|&byte| DirectDualRdbChildPhase::from_byte(byte)),
            ),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => return phases,
            Err(_) => return phases,
        }
    }
}

/// Fork-safe, best-effort phase write. The pipe is non-blocking and carries at
/// most three bytes, so an `EAGAIN` can only lose diagnostics, never progress.
fn child_write_direct_dual_rdb_phase(fd: RawFd, phase: DirectDualRdbChildPhase) {
    let stderr_line: &[u8] = match phase {
        DirectDualRdbChildPhase::Forked => b"direct dual RDB child phase=forked\n",
        DirectDualRdbChildPhase::Started => b"direct dual RDB child phase=started\n",
        DirectDualRdbChildPhase::HeaderSent => b"direct dual RDB child phase=header-sent\n",
        DirectDualRdbChildPhase::SerializeDone => b"direct dual RDB child phase=serialize-done\n",
        DirectDualRdbChildPhase::MarkerSent => b"direct dual RDB child phase=marker-sent\n",
    };
    crate::framework::capabilities::process::write_all_fd(fd, &[phase.as_byte()]);
    // The primary is deliberately SIGSTOPed by the Tcl fixture immediately
    // after fork, so it cannot drain the phase pipe until after the test's
    // `Done loading RDB` checkpoint. This direct stderr write makes the same
    // phase observable while the parent remains stopped.
    // SAFETY: STDERR_FILENO is inherited across fork and this uses write(2),
    // which is async-signal-safe with a valid static byte slice.
    crate::framework::capabilities::process::write_all_fd(libc::STDERR_FILENO, stderr_line);
}

/// Write `buf` in full to a pipe `fd` from within a post-fork child, retrying on
/// `EINTR`. Returns `false` if the write hit a real error (EPIPE / reader gone),
/// so the caller can `_exit(1)` and let the parent detect the short stream.
/// The child installs its signal handlers without `SA_RESTART` (see
/// `install_child_signal_handlers`), and `CONFIG SET rdb-key-save-delay` signals
/// RDB-shaped children, so an interrupted write is expected: retry on `EINTR`
/// rather than truncating the replica's full-sync stream.
/// fork-safety: only calls `write(2)`, which is async-signal-safe.
fn child_write_all(fd: i32, buf: &[u8]) -> bool {
    crate::framework::capabilities::process::write_all_fd(fd, buf)
}

fn child_wait_for_parent_release(read_end: File) {
    crate::framework::capabilities::process::wait_for_pipe_eof(read_end);
}

/// Write a direct RDB socket without changing the shared `O_NONBLOCK` flag.
/// The parent keeps this file description registered with the polling backend, so changing it
/// in the child can make a later parent-side read block.
pub(crate) fn child_write_socket_all(fd: RawFd, buf: &[u8], timeout_ms: u64) -> bool {
    crate::framework::capabilities::process::send_all(
        fd,
        buf,
        std::time::Duration::from_millis(timeout_ms),
    )
}

/// Broadcast a direct diskless RDB chunk to all surviving sockets. A failed
/// target is retired in-place so one disconnected replica does not abort the
/// child transfer for the remaining cohort.
/// Fork-safety: `fds` is allocated before `fork()`, and this function only
/// mutates that inherited allocation while calling the async-signal-safe
/// socket write/poll primitives used by `child_write_socket_all`.
pub(crate) fn child_write_active_sockets(fds: &mut [RawFd], buf: &[u8], timeout_ms: u64) -> bool {
    let mut still_active = false;
    for fd in fds {
        if *fd < 0 {
            continue;
        }
        if child_write_socket_all(*fd, buf, timeout_ms) {
            still_active = true;
        } else {
            *fd = -1;
        }
    }
    still_active
}

/// Match C `connSendTimeout()` for direct diskless RDB sockets. The timeout is
/// installed before fork so the child inherits it when it switches the sockets
/// to blocking mode for the RDB write.
pub(crate) fn set_direct_diskless_send_timeout(fd: RawFd, timeout_ms: u64) -> std::io::Result<()> {
    crate::framework::capabilities::process::set_send_timeout(
        fd,
        std::time::Duration::from_millis(timeout_ms),
    )
}

/// Shared fork primitive for full-sync pipe children. Forks a child that
/// serializes the RDB (with the given function library codes) to a pipe and
/// `_exit`s, returning `(child_pid, pipe_read_end)`. On `pipe2()`/`fork()`
/// failure returns `None`; the caller must fall back to in-process serialization
/// (mirrors `start_bgsave`'s synchronous fallback). fork-safety: the child only
/// runs `child_after_fork` + serialization + `write`/`_exit`, exactly the
/// accepted BGSAVE child window (ADR §2.2, §2.3).
/// When `frame_as_bulk` is true the child prefixes the stream with its own
/// `$<len>\r\n` bulk header, where `<len>` is the byte count of the child's own
/// serialization — the disk-based full-sync path, whose replica consumes a
/// length-delimited bulk. When false the stream is the raw RDB, EOF-framed by
/// the pipe close — the diskless full-sync path.
/// This is a pure fork primitive: it does NOT register the pid in the
/// persistence state machine. The caller owns the returned `File` and must call
/// the matching `begin_*_child(pid)`, otherwise the matching reaper has no pid.
fn fork_rdb_pipe_child(
    ctx: &mut ServerContext,
    function_codes: &[Vec<u8>],
    frame_as_bulk: bool,
) -> Option<(i32, File, Option<File>)> {
    let data_pipe = match pipe(PipeFlags::CLOEXEC) {
        Ok(pipe) => pipe,
        Err(_) => {
            crate::server_log!(
                warning,
                "full-sync pipe2() failed, falling back to in-process RDB"
            );
            return None;
        }
    };
    let release_pipe = if frame_as_bulk {
        None
    } else {
        match pipe(PipeFlags::CLOEXEC) {
            Ok(pipe) => Some(pipe),
            Err(_) => {
                crate::server_log!(
                    warning,
                    "diskless child release pipe2() failed, falling back to in-process RDB"
                );
                return None;
            }
        }
    };

    let listener_fds = ctx.listener_fds().to_vec();
    let replication = ctx.rdb_full_sync_replication_info();
    let slot_imports = ctx.slot_import_dbs();
    // Snapshot the debug key-save delay before forking so the child can throttle
    // its RDB stream (default 0 = no delay). Mirrors the in-process full-resync
    // path (fire_deferred_full_resyncs), BGSAVE, and AOF rewrite, and aligns with
    // C's diskless save calling debugDelay(server.rdb_key_save_delay) per key
    // (rdb.c). Reading it in the parent avoids touching shared config post-fork.
    let key_delay_us = ctx.config().persistence.rdb_key_save_delay;
    let serialization = crate::foundation::io::persistence::RdbSerializeOptions {
        compression: ctx.config().persistence.rdbcompression,
        checksum: true,
    };
    let bgsave_affinity = match prepare_bgsave_cpu_affinity(&ctx.config().server.bgsave_cpulist) {
        Ok(affinity) => affinity,
        Err(error) => {
            crate::server_log!(warning, "invalid bgsave-cpulist: {}", error);
            return None;
        }
    };
    // Capture before fork so the child can close the parent-death race after
    // installing Linux PDEATHSIG.
    let parent_pid = std::process::id() as libc::pid_t;
    let fork_started = std::time::Instant::now();
    let child = match fork_process() {
        Ok(ForkBranch::Parent(child)) => child,
        Ok(ForkBranch::Child) => {
            let (data_read_end, data_write_end) = data_pipe.into_parts();
            drop(data_read_end);
            let release_read_end = release_pipe.map(|pipe| {
                let (read_end, write_end) = pipe.into_parts();
                drop(write_end);
                read_end
            });
            let write_fd = data_write_end.as_raw_fd();

            child_after_fork(ctx.config(), &listener_fds);
            if let Some(affinity) = bgsave_affinity.as_ref() {
                let _ = crate::foundation::io::sys::process::apply_prepared_cpu_affinity(affinity);
            }
            exit_child_if_parent_is_gone(parent_pid);
            if !frame_as_bulk {
                let ok = crate::foundation::io::persistence::serialize_all_with_functions_and_replication_streaming_with_options(
                    ctx.dbs(), key_delay_us, function_codes, true, Some(&replication), &slot_imports,
                    serialization,
                    |chunk| child_write_all(write_fd, chunk),
                );
                drop(data_write_end);
                child_wait_for_parent_release(
                    release_read_end.expect("diskless child must own release read end"),
                );
                exit_immediately(if ok { 0 } else { 1 });
            }
            let bytes = crate::foundation::io::persistence::serialize_all_with_functions_and_replication_with_options(
                ctx.dbs(),
                key_delay_us,
                function_codes,
                true,
                Some(&replication),
                &slot_imports,
                serialization,
            );
            let header = format!("${}\r\n", bytes.len());
            let ok = child_write_all(write_fd, header.as_bytes())
                && child_write_all(write_fd, &bytes);
            drop(data_write_end);
            exit_immediately(if ok { 0 } else { 1 });
        }
        Err(_) => {
            crate::server_log!(
                warning,
                "full-sync fork() failed, falling back to in-process RDB"
            );
            return None;
        }
    };
    let pid = child.into_pid();
    let (read_end, write_end) = data_pipe.into_parts();
    drop(write_end);
    let child_release_write_end = release_pipe.map(|pipe| {
        let (read_end, write_end) = pipe.into_parts();
        drop(read_end);
        write_end
    });
    ctx.stats_mut()
        .record_fork(fork_started.elapsed().as_micros() as u64);
    crate::server_log!(notice, "Background RDB transfer started by pid {}", pid);
    Some((pid, read_end, child_release_write_end))
}

/// Fork a child that serializes the raw RDB (diskless full-sync) to a pipe and
/// `_exit`s. Returns `(child_pid, pipe_read_end)`; `None` on fork failure, so the
/// caller falls back to in-process serialization.
/// Pure fork primitive: does NOT register the pid. The caller owns the returned
/// `File` and must call `begin_diskless_child(pid)`, otherwise
/// `reap_diskless_child_if_any` has no pid to reap.
pub fn fork_diskless_rdb_child(
    ctx: &mut ServerContext,
    function_codes: &[Vec<u8>],
) -> Option<DisklessPipeChild> {
    let (pid, data_read_end, child_release_write_end) =
        fork_rdb_pipe_child(ctx, function_codes, false)?;
    Some(DisklessPipeChild {
        pid,
        data_read_end,
        child_release_write_end: child_release_write_end
            .expect("diskless fork must create a child release pipe"),
    })
}

pub fn fork_direct_diskless_rdb_child(
    ctx: &mut ServerContext,
    function_codes: &[Vec<u8>],
    fds: &[RawFd],
    marker: &[u8],
) -> Option<(i32, File, File)> {
    if fds.is_empty() {
        return None;
    }
    let metric_pipe = pipe(PipeFlags::CLOEXEC).ok()?;
    let phase_pipe = pipe(PipeFlags::CLOEXEC_NONBLOCK).ok()?;
    let listener_fds = ctx.listener_fds().to_vec();
    let replication = ctx.rdb_full_sync_replication_info();
    let slot_imports = ctx.slot_import_dbs();
    // Allocate before fork. The child mutates this inherited buffer to retire
    // disconnected socket targets without invoking the allocator post-fork.
    let mut child_fds = fds.to_vec();
    let key_delay_us = ctx.config().persistence.rdb_key_save_delay;
    let serialization = crate::foundation::io::persistence::RdbSerializeOptions {
        compression: ctx.config().persistence.rdbcompression,
        checksum: true,
    };
    let bgsave_affinity = match prepare_bgsave_cpu_affinity(&ctx.config().server.bgsave_cpulist) {
        Ok(affinity) => affinity,
        Err(error) => {
            crate::server_log!(warning, "invalid bgsave-cpulist: {}", error);
            return None;
        }
    };
    let parent_pid = std::process::id() as libc::pid_t;
    let send_timeout_ms = ctx.config().replication.repl_timeout.saturating_mul(1_000);
    for &fd in fds {
        if let Err(error) = set_direct_diskless_send_timeout(fd, send_timeout_ms) {
            crate::server_log!(
                warning,
                "direct diskless RDB send timeout setup failed for fd {}: {}",
                fd,
                error
            );
            return None;
        }
    }
    let started = std::time::Instant::now();
    let child = match fork_process().ok()? {
        ForkBranch::Parent(child) => child,
        ForkBranch::Child => {
            let (metric_read_end, metric_write_end) = metric_pipe.into_parts();
            let (phase_read_end, phase_write_end) = phase_pipe.into_parts();
            drop(metric_read_end);
            drop(phase_read_end);
            let metric_write_fd = metric_write_end.as_raw_fd();
            let phase_write_fd = phase_write_end.as_raw_fd();

        child_write_direct_dual_rdb_phase(phase_write_fd, DirectDualRdbChildPhase::Forked);
        child_after_fork(ctx.config(), &listener_fds);
        if let Some(affinity) = bgsave_affinity.as_ref() {
            let _ = crate::foundation::io::sys::process::apply_prepared_cpu_affinity(affinity);
        }
        exit_child_if_parent_is_gone(parent_pid);
        child_write_direct_dual_rdb_phase(phase_write_fd, DirectDualRdbChildPhase::Started);
        let mut bytes = (b"$EOF:".len() + marker.len() + b"\r\n".len()) as u64;
        let mut ok = child_write_active_sockets(&mut child_fds, b"$EOF:", send_timeout_ms)
            && child_write_active_sockets(&mut child_fds, marker, send_timeout_ms)
            && child_write_active_sockets(&mut child_fds, b"\r\n", send_timeout_ms);
        if ok {
            child_write_direct_dual_rdb_phase(phase_write_fd, DirectDualRdbChildPhase::HeaderSent);
        }
        if ok {
            ok = crate::foundation::io::persistence::serialize_all_with_functions_and_replication_streaming_with_options(ctx.dbs(), key_delay_us, function_codes, true, Some(&replication), &slot_imports, serialization, |chunk| {
                bytes = bytes.saturating_add(chunk.len() as u64);
                child_write_active_sockets(&mut child_fds, chunk, send_timeout_ms)
            });
        }
        if ok {
            child_write_direct_dual_rdb_phase(phase_write_fd, DirectDualRdbChildPhase::SerializeDone);
        }
        if ok {
            bytes = bytes.saturating_add(marker.len() as u64);
            ok = child_write_active_sockets(&mut child_fds, marker, send_timeout_ms);
        }
        if ok {
            child_write_direct_dual_rdb_phase(phase_write_fd, DirectDualRdbChildPhase::MarkerSent);
        }
        if ok {
            let _ = child_write_all(metric_write_fd, &bytes.to_ne_bytes());
        }
            drop(metric_write_end);
            drop(phase_write_end);
            exit_immediately(if ok { 0 } else { 1 });
        }
    };
    let pid = child.into_pid();
    let (metric_read_end, metric_write_end) = metric_pipe.into_parts();
    let (phase_read_end, phase_write_end) = phase_pipe.into_parts();
    drop(metric_write_end);
    drop(phase_write_end);
    ctx.stats_mut()
        .record_fork(started.elapsed().as_micros() as u64);
    Some((pid, metric_read_end, phase_read_end))
}

/// Fork a disk-based full-sync child that writes one unframed RDB snapshot to a
/// file path. The parent owns bulk framing and independent per-replica cursors.
/// The child registers in the DiskSync persistence slot, so only one RDB-shaped
/// child can exist at a time.
pub fn fork_disk_rdb_file_child(
    ctx: &mut ServerContext,
    function_codes: &[Vec<u8>],
    path: &Path,
) -> Option<i32> {
    let listener_fds = ctx.listener_fds().to_vec();
    let replication = ctx.rdb_full_sync_replication_info();
    let slot_imports = ctx.slot_import_dbs();
    let key_delay_us = ctx.config().persistence.rdb_key_save_delay;
    let serialization = crate::foundation::io::persistence::RdbSerializeOptions {
        compression: ctx.config().persistence.rdbcompression,
        checksum: true,
    };
    let bgsave_affinity = match prepare_bgsave_cpu_affinity(&ctx.config().server.bgsave_cpulist) {
        Ok(affinity) => affinity,
        Err(error) => {
            crate::server_log!(warning, "invalid bgsave-cpulist: {}", error);
            return None;
        }
    };
    let parent_pid = std::process::id() as libc::pid_t;
    let path = path.to_path_buf();
    let fork_started = std::time::Instant::now();
    let child = match fork_process() {
        Ok(ForkBranch::Parent(child)) => child,
        Ok(ForkBranch::Child) => {
            child_after_fork(ctx.config(), &listener_fds);
            if let Some(affinity) = bgsave_affinity.as_ref() {
                let _ = crate::foundation::io::sys::process::apply_prepared_cpu_affinity(affinity);
            }
            exit_child_if_parent_is_gone(parent_pid);
            let result = (|| -> std::io::Result<()> {
                let file = File::create(&path)?;
                let mut writer = BufWriter::with_capacity(DISK_RDB_FILE_BUFFER_BYTES, file);
                write_replication_rdb_streaming(
                    ReplicationRdbSource::new(
                        ctx.dbs(),
                        key_delay_us,
                        serialization,
                        function_codes,
                        &replication,
                        &slot_imports,
                    ),
                    &mut writer,
                )?;
                writer.flush()?;
                writer.get_ref().sync_all()
            })();
            exit_immediately(if result.is_ok() { 0 } else { 1 });
        }
        Err(_) => {
            crate::server_log!(
                warning,
                "disk-sync file fork() failed, falling back to in-process RDB"
            );
            return None;
        }
    };
    let pid = child.into_pid();

    ctx.stats_mut()
        .record_fork(fork_started.elapsed().as_micros() as u64);
    if ctx.persistence_mut().begin_disk_sync_child(pid).is_err() {
        terminate_and_reap_child(pid);
        let _ = std::fs::remove_file(path);
        return None;
    }
    Some(pid)
}

/// Non-blocking reap of a completed diskless full-sync child. Cron-polled; the
/// diskless relay also calls it on pipe EOF. Performs no dirty accounting.
/// This only releases the child slot; it does NOT validate the transfer. A child
/// that `_exit(1)`d on a truncated write is reaped exactly like a success — the
/// relay (Task 6) detects a short/failed stream via the pipe, not the exit code.
// TODO(dead_code): retained for legacy framework-level callers until Tasks 9-12
// finish migrating replication child owners to runtime ChildCompletion events.
#[allow(dead_code)]
pub fn reap_diskless_child_if_any(ctx: &mut ServerContext) {
    let Some(pid) = ctx.persistence().diskless_child_pid() else {
        return;
    };
    let outcome = match wait_nonblocking(pid) {
        Ok(WaitOutcome::Running) => return,
        Ok(outcome) => Some(outcome),
        Err(_) => None,
    };
    if !matches!(outcome, Some(WaitOutcome::Exited(0))) {
        crate::server_log!(
            warning,
            "Diskless RDB child {} exited abnormally (status={:?})",
            pid,
            outcome
        );
    }
    ctx.persistence_mut().complete_diskless_child();
}

/// Reap a disk-file full-sync child and preserve its terminal status for the
/// parent batch state machine.
#[cfg(test)]
pub fn reap_disk_sync_file_child(ctx: &mut ServerContext) -> DiskSyncChildExit {
    let Some(pid) = ctx.persistence().disk_sync_child_pid() else {
        return DiskSyncChildExit::NotRunning;
    };
    let outcome = match wait_nonblocking(pid) {
        Ok(WaitOutcome::Running) => return DiskSyncChildExit::Running,
        Ok(outcome) => Some(outcome),
        Err(_) => None,
    };
    ctx.persistence_mut().complete_disk_sync_child();
    if matches!(outcome, Some(WaitOutcome::Exited(0))) {
        DiskSyncChildExit::Succeeded
    } else {
        DiskSyncChildExit::Failed
    }
}

fn terminate_and_reap_child(pid: i32) {
    let _ = send_signal(pid, ProcessSignal::Kill);
    let _ = wait_blocking(pid);
}
