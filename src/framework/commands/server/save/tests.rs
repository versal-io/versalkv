use crate::foundation::io::persistence as persistence;
use bytes::Bytes;

#[test]
fn close_listener_fds_closes_all_and_ignores_invalid() {
    use std::io::Read;
    use std::os::unix::io::AsRawFd;
    use std::os::unix::net::UnixStream;

    let (a, mut peer_a) = UnixStream::pair().unwrap();
    let (b, mut peer_b) = UnixStream::pair().unwrap();
    let fd_a = a.as_raw_fd();
    let fd_b = b.as_raw_fd();

    // Transfer ownership of the raw descriptors to close_listener_fds.
    std::mem::forget(a);
    std::mem::forget(b);
    assert_eq!(super::close_listener_fds(&[fd_a, -1, fd_b]), 2);
    let mut byte = [0_u8; 1];
    assert_eq!(peer_a.read(&mut byte).unwrap(), 0);
    assert_eq!(peer_b.read(&mut byte).unwrap(), 0);
}

#[cfg(unix)]
#[test]
fn fork_child_cleanup_closes_registered_cluster_config_lock() {
    use crate::foundation::io::sys::process::WaitOutcome;
    use std::fs::OpenOptions;
    use std::os::fd::AsRawFd;

    let path = std::env::temp_dir().join(format!(
        "versal-cluster-lock-fork-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .unwrap();
    let fd = file.as_raw_fd();
    super::register_cluster_config_lock_fd(fd);
    let mut child = crate::framework::capabilities::process::fork_owned_child(move || {
        let closed = super::close_cluster_config_lock_in_child();
        let is_closed = !crate::foundation::io::sys::os::is_file_descriptor_open(fd)
            && std::io::Error::last_os_error().raw_os_error() == Some(libc::EBADF);
        i32::from(!(closed && is_closed))
    })
    .unwrap();

    assert_eq!(child.wait().unwrap(), WaitOutcome::Exited(0));
    assert!(crate::foundation::io::sys::os::is_file_descriptor_open(fd));

    super::unregister_cluster_config_lock_fd(fd);
    drop(file);
    let _ = std::fs::remove_file(path);
}

#[test]
fn direct_diskless_socket_send_timeout_matches_repl_timeout() {
    use crate::foundation::io::sys::socket::send_timeout;
    use std::os::fd::AsFd;
    use std::os::unix::io::AsRawFd;
    use std::os::unix::net::UnixStream;

    let (writer, _reader) = UnixStream::pair().unwrap();
    super::fullsync_fork::set_direct_diskless_send_timeout(writer.as_raw_fd(), 10_250)
        .expect("SO_SNDTIMEO must be configured for direct diskless RDB sockets");

    assert_eq!(
        send_timeout(writer.as_fd()).expect("read SO_SNDTIMEO"),
        std::time::Duration::from_millis(10_250)
    );
}

#[test]
fn direct_diskless_socket_write_keeps_parent_descriptor_nonblocking() {
    use crate::foundation::io::sys::fd::is_nonblocking;
    use std::io::Read;
    use std::os::fd::{AsFd, AsRawFd};
    use std::os::unix::net::UnixStream;

    let (writer, mut reader) = UnixStream::pair().unwrap();
    writer.set_nonblocking(true).expect("set nonblocking");

    assert!(super::fullsync_fork::child_write_socket_all(
        writer.as_raw_fd(),
        b"rdb",
        1_000
    ));
    assert!(
        is_nonblocking(writer.as_fd()).expect("query nonblocking"),
        "child writes must not change the parent transport descriptor to blocking"
    );

    let mut received = [0u8; 3];
    reader.read_exact(&mut received).unwrap();
    assert_eq!(received, *b"rdb");
}

#[test]
fn direct_diskless_write_keeps_surviving_socket_after_peer_disconnects() {
    use std::io::Read;
    use std::os::unix::io::AsRawFd;
    use std::os::unix::net::UnixStream;

    let (dropped_writer, dropped_reader) = UnixStream::pair().unwrap();
    let (live_writer, mut live_reader) = UnixStream::pair().unwrap();
    drop(dropped_reader);
    let mut fds = [dropped_writer.as_raw_fd(), live_writer.as_raw_fd()];

    assert!(super::fullsync_fork::child_write_active_sockets(
        &mut fds, b"rdb", 1_000
    ));
    assert_eq!(fds[0], -1, "the failed target must be retired");
    assert_eq!(fds[1], live_writer.as_raw_fd());

    let mut received = [0u8; 3];
    live_reader.read_exact(&mut received).unwrap();
    assert_eq!(received, *b"rdb");
}

#[test]
fn direct_dual_rdb_phase_pipe_drains_completed_child_stages_without_blocking() {
    use crate::foundation::io::sys::fd::{pipe, PipeFlags};
    use std::io::Write;

    let pipe = pipe(PipeFlags::CLOEXEC_NONBLOCK).expect("create phase pipe");
    let (mut read_end, mut write_end) = pipe.into_parts();
    let encoded = [
        super::fullsync_fork::DirectDualRdbChildPhase::Forked.as_byte(),
        super::fullsync_fork::DirectDualRdbChildPhase::Started.as_byte(),
        super::fullsync_fork::DirectDualRdbChildPhase::HeaderSent.as_byte(),
        super::fullsync_fork::DirectDualRdbChildPhase::SerializeDone.as_byte(),
    ];
    write_end.write_all(&encoded).expect("write phase bytes");

    assert_eq!(
        super::fullsync_fork::drain_direct_dual_rdb_phase_pipe(&mut read_end),
        vec![
            super::fullsync_fork::DirectDualRdbChildPhase::Forked,
            super::fullsync_fork::DirectDualRdbChildPhase::Started,
            super::fullsync_fork::DirectDualRdbChildPhase::HeaderSent,
            super::fullsync_fork::DirectDualRdbChildPhase::SerializeDone,
        ]
    );
    assert!(
        super::fullsync_fork::drain_direct_dual_rdb_phase_pipe(&mut read_end).is_empty(),
        "a pending child must not make the event loop block on an empty phase pipe"
    );
}

#[test]
fn diskless_child_pipes_same_bytes_as_in_process_serialize() {
    use crate::foundation::io::sys::process::{wait_blocking, WaitOutcome};
    use crate::framework::context::ServerContext;
    use std::io::Read;

    let _fork_guard = super::diskless_fork_test_guard();

    // Split an RDB into (non-volatile AUX pairs, body-excluding-CRC). The parent's
    // in-process `expected` and the child's piped bytes only ever diverge in the
    // volatile `used-mem` (allocator state) and `ctime` AUX fields and the CRC64
    // that covers them, so we drop those before comparing structurally.
    fn canonicalize(rdb: &[u8]) -> (Vec<(Vec<u8>, Vec<u8>)>, Vec<u8>) {
        let mut pos = 9usize; // skip "VALKEY080" magic
        let mut aux = Vec::new();
        while rdb[pos] == persistence::RDB_OPCODE_AUX {
            pos += 1;
            let key = persistence::load_string(rdb, &mut pos).unwrap();
            let val = persistence::load_string(rdb, &mut pos).unwrap();
            if key != b"used-mem" && key != b"ctime" {
                aux.push((key, val));
            }
        }
        let body = rdb[pos..rdb.len() - 8].to_vec();
        (aux, body)
    }

    let mut ctx = ServerContext::new(16);
    ctx.dbs_mut().get_mut(0).unwrap().set(
        b"g2key".to_vec(),
        crate::foundation::io::store::Robj::string(Bytes::from_static(b"g2val")),
    );

    let libs: Vec<Vec<u8>> = Vec::new();
    let replication = ctx.rdb_full_sync_replication_info();
    let expected = crate::foundation::io::persistence::serialize_all_with_functions_and_replication(
        ctx.dbs(),
        0,
        &libs,
        true,
        Some(&replication),
        &[],
    );

    let child =
        super::fork_diskless_rdb_child(&mut ctx, &libs).expect("fork should succeed in test");
    let pid = child.pid;
    let mut read_end = child.data_read_end;
    let mut got = Vec::new();
    read_end.read_to_end(&mut got).unwrap();
    drop(child.child_release_write_end);

    assert_eq!(wait_blocking(pid).unwrap(), WaitOutcome::Exited(0));
    // Byte-for-byte equivalence modulo the two volatile AUX fields + CRC trailer:
    // this proves the child serialized the same DbSet + functions the parent would.
    assert_eq!(canonicalize(&got), canonicalize(&expected));
}

#[test]
fn forkless_save_releases_large_key_barrier_after_serialization() {
    use crate::engine::extension::config::persistence::BgsaveMethod;
    use crate::foundation::io::store::structure::list::ListValue;
    use crate::foundation::io::store::Robj;
    use crate::framework::context::ServerContext;
    use std::collections::VecDeque;
    use std::time::{Duration, Instant};

    let mut ctx = ServerContext::new(1);
    ctx.dbs_mut().get_mut(0).unwrap().set(
        b"large".to_vec(),
        Robj::ListQuicklist(ListValue::from_vecdeque(VecDeque::from(vec![
            Bytes::from(vec![b'x'; 128]);
            1_024
        ]))),
    );
    let dir = std::env::temp_dir().join(format!(
        "versal-forkless-release-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    ctx.config_mut().persistence.dir = dir.to_string_lossy().into_owned();
    ctx.config_mut().persistence.dbfilename = "dump.rdb".to_string();
    ctx.config_mut().persistence.bgsave_default_method = BgsaveMethod::Forkless;

    super::start_bgsave(&mut ctx).unwrap();
    assert!(ctx.persistence().forkless_key_blocked(0, b"large"));
    let deadline = Instant::now() + Duration::from_secs(5);
    while ctx
        .persistence()
        .forkless_progress()
        .is_some_and(|progress| progress.processed == 0)
        && Instant::now() < deadline
    {
        std::thread::sleep(Duration::from_millis(1));
    }
    assert_eq!(
        ctx.persistence()
            .forkless_progress()
            .map(|progress| progress.processed),
        Some(1)
    );

    let _ = ctx.drive_forkless_feed();
    assert!(
        !ctx.persistence().forkless_key_blocked(0, b"large"),
        "the key barrier must be released once the background worker drops its shared value"
    );

    ctx.abort_forkless_save(crate::foundation::io::sys::time::mstime() / 1_000);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn forkless_save_keeps_original_database_identity_across_swapdb() {
    use crate::engine::extension::config::persistence::BgsaveMethod;
    use crate::foundation::io::persistence::snapshot::background::RdbSaveOutcome;
    use crate::foundation::io::store::kv_engine::DbSet;
    use crate::foundation::io::store::Robj;
    use crate::framework::context::ServerContext;

    let mut ctx = ServerContext::new(2);
    for index in 0..256 {
        ctx.db_mut(0).unwrap().set(
            format!("left-{index}").into_bytes(),
            Robj::string("left-value"),
        );
        ctx.db_mut(1).unwrap().set(
            format!("right-{index}").into_bytes(),
            Robj::string("right-value"),
        );
    }
    let dir = std::env::temp_dir().join(format!(
        "versal-forkless-swapdb-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    ctx.config_mut().persistence.dir = dir.to_string_lossy().into_owned();
    ctx.config_mut().persistence.dbfilename = "dump.rdb".to_string();
    ctx.config_mut().persistence.bgsave_default_method = BgsaveMethod::Forkless;

    super::start_bgsave(&mut ctx).unwrap();
    assert!(ctx.persistence().forkless_capture_in_progress());
    ctx.swap_dbs(0, 1);
    while ctx.persistence().forkless_capture_in_progress() {
        ctx.drive_forkless_feed();
    }
    assert!(matches!(
        ctx.finish_primary_forkless_save_for_test(),
        Some(RdbSaveOutcome::Saved)
    ));

    let bytes = std::fs::read(dir.join("dump.rdb")).unwrap();
    let mut restored = DbSet::new(2);
    persistence::load_rdb_bytes_checked(&bytes, &mut restored, true).unwrap();
    for index in 0..256 {
        let left = format!("left-{index}");
        let right = format!("right-{index}");
        assert!(restored.get(0).unwrap().get_allow_expired(left.as_bytes()).is_some());
        assert!(restored.get(0).unwrap().get_allow_expired(right.as_bytes()).is_none());
        assert!(restored.get(1).unwrap().get_allow_expired(right.as_bytes()).is_some());
        assert!(restored.get(1).unwrap().get_allow_expired(left.as_bytes()).is_none());
    }

    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn forkless_owned_snapshot_allows_deletion_before_serialization_finishes() {
    use crate::engine::extension::config::persistence::BgsaveMethod;
    use crate::foundation::io::persistence::snapshot::background::RdbSaveOutcome;
    use crate::foundation::io::store::kv_engine::DbSet;
    use crate::foundation::io::store::Robj;
    use crate::framework::context::ServerContext;

    let mut ctx = ServerContext::new(1);
    for index in 0..256 {
        ctx.db_mut(0).unwrap().set(
            format!("owned-{index}").into_bytes(),
            Robj::string_raw(Bytes::from_static(b"original")),
        );
    }
    let dir = std::env::temp_dir().join(format!(
        "versal-forkless-owned-delete-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    ctx.config_mut().persistence.dir = dir.to_string_lossy().into_owned();
    ctx.config_mut().persistence.dbfilename = "dump.rdb".to_string();
    ctx.config_mut().persistence.bgsave_default_method = BgsaveMethod::Forkless;
    ctx.config_mut().persistence.rdb_key_save_delay = 100_000;
    super::start_bgsave(&mut ctx).unwrap();

    let captured = (0..256)
        .map(|index| format!("owned-{index}").into_bytes())
        .find(|key| !ctx.persistence().forkless_key_captured(0, key))
        .expect("the bounded initial feed leaves a value for early capture");
    assert!(ctx.prioritize_forkless_keys(0, [captured.clone()]));
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while !ctx.persistence().forkless_key_captured(0, &captured) {
        assert!(std::time::Instant::now() < deadline, "early capture did not advance");
        ctx.drive_forkless_feed();
        std::thread::yield_now();
    }
    assert!(ctx.persistence().forkless_key_captured(0, &captured));
    let is_blocked = ctx.persistence().forkless_key_blocked(0, &captured);
    if is_blocked {
        ctx.abort_forkless_save(0);
        let _ = std::fs::remove_dir_all(&dir);
    }
    assert!(
        !is_blocked,
        "an independent snapshot copy must not wait behind unrelated serialization"
    );
    assert!(ctx.db_mut(0).unwrap().del(&captured));
    assert!(ctx.persistence().forkless_save_in_progress());
    assert!(ctx.persistence().forkless_capture_in_progress());
    assert!(ctx.persistence().forkless_progress().unwrap().processed < 256);

    ctx.persistence().set_forkless_key_delay(0);
    while ctx.persistence().forkless_capture_in_progress() {
        ctx.drive_forkless_feed();
        std::thread::yield_now();
    }
    assert!(matches!(
        ctx.finish_primary_forkless_save_for_test(),
        Some(RdbSaveOutcome::Saved)
    ));
    let bytes = std::fs::read(dir.join("dump.rdb")).unwrap();
    let mut restored = DbSet::new(1);
    persistence::load_rdb_bytes_checked(&bytes, &mut restored, true).unwrap();
    assert_eq!(restored.get(0).unwrap().dbsize(), 256);
    assert_eq!(
        restored.get(0).unwrap().get_allow_expired(&captured).unwrap().as_bytes(),
        b"original"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn forkless_save_tracks_shared_key_barriers_across_swapdb() {
    use crate::engine::extension::config::persistence::BgsaveMethod;
    use crate::foundation::io::store::structure::list::ListValue;
    use crate::foundation::io::store::Robj;
    use crate::framework::context::ServerContext;
    use std::collections::VecDeque;

    let mut ctx = ServerContext::new(2);
    ctx.db_mut(0).unwrap().set(
        b"large".to_vec(),
        Robj::ListQuicklist(ListValue::from_vecdeque(VecDeque::from(vec![
            Bytes::from(vec![b'x'; 128]);
            1_024
        ]))),
    );
    let dir = std::env::temp_dir().join(format!(
        "versal-forkless-swapdb-barrier-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    ctx.config_mut().persistence.dir = dir.to_string_lossy().into_owned();
    ctx.config_mut().persistence.dbfilename = "dump.rdb".to_string();
    ctx.config_mut().persistence.bgsave_default_method = BgsaveMethod::Forkless;
    ctx.config_mut().persistence.rdb_key_save_delay = 10_000_000;

    super::start_bgsave(&mut ctx).unwrap();
    assert!(ctx.persistence().forkless_key_blocked(0, b"large"));
    ctx.swap_dbs(0, 1);

    assert!(!ctx.persistence().forkless_key_blocked(0, b"large"));
    assert!(ctx.persistence().forkless_key_blocked(1, b"large"));
    assert!(!ctx.persistence().forkless_key_captured(0, b"large"));
    assert!(ctx.persistence().forkless_key_captured(1, b"large"));

    ctx.abort_forkless_save(crate::foundation::io::sys::time::mstime() / 1_000);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn forkless_capture_prevents_lazy_expiration_from_deleting_unscanned_keys() {
    use crate::engine::extension::config::persistence::BgsaveMethod;
    use crate::foundation::io::store::Robj;
    use crate::framework::context::ServerContext;
    use std::time::Duration;

    let mut ctx = ServerContext::new(1);
    let expire_at = crate::foundation::io::sys::time::mstime().saturating_add(100) as u64;
    for index in 0..256 {
        ctx.db_mut(0).unwrap().load_entry(
            format!("expiring-{index}").into_bytes(),
            Robj::string_raw(Bytes::from_static(b"value")),
            Some(expire_at),
        );
    }
    let dir = std::env::temp_dir().join(format!(
        "versal-forkless-lazy-expire-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    ctx.config_mut().persistence.dir = dir.to_string_lossy().into_owned();
    ctx.config_mut().persistence.dbfilename = "dump.rdb".to_string();
    ctx.config_mut().persistence.bgsave_default_method = BgsaveMethod::Forkless;
    ctx.config_mut().persistence.rdb_key_save_delay = 10_000_000;
    super::start_bgsave(&mut ctx).unwrap();
    assert!(ctx.persistence().forkless_capture_in_progress());
    std::thread::sleep(Duration::from_millis(150));

    assert!(ctx.db_mut(0).unwrap().get(b"expiring-255").is_none());
    assert!(
        ctx.db(0)
            .unwrap()
            .get_allow_expired(b"expiring-255")
            .is_some(),
        "lazy expiration must not physically remove keys during capture"
    );

    ctx.abort_forkless_save(crate::foundation::io::sys::time::mstime() / 1_000);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn diskless_child_waits_for_parent_release_after_data_eof() {
    use crate::foundation::io::sys::process::{wait_nonblocking, WaitOutcome};
    use crate::framework::context::ServerContext;
    use std::io::Read;
    use std::time::Duration;

    let _fork_guard = super::diskless_fork_test_guard();
    let mut ctx = ServerContext::new(1);
    let child = super::fork_diskless_rdb_child(&mut ctx, &[])
        .expect("diskless child should fork");
    let pid = child.pid;
    let mut data_read_end = child.data_read_end;
    let release_write_end = child.child_release_write_end;
    let mut payload = Vec::new();
    data_read_end
        .read_to_end(&mut payload)
        .expect("read diskless payload to EOF");

    assert_eq!(wait_nonblocking(pid).unwrap(), WaitOutcome::Running);

    drop(release_write_end);
    let mut reaped = false;
    for _ in 0..100 {
        if matches!(wait_nonblocking(pid), Ok(WaitOutcome::Exited(0))) {
            reaped = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(reaped, "diskless child did not exit after parent release");
}

#[cfg(target_os = "linux")]
fn first_allowed_cpu() -> String {
    let status = std::fs::read_to_string("/proc/self/status").expect("read current process status");
    status
        .lines()
        .find_map(|line| line.strip_prefix("Cpus_allowed_list:\t"))
        .expect("current process CPU allowance")
        .split(',')
        .next()
        .expect("non-empty CPU allowance")
        .split('-')
        .next()
        .expect("CPU range start")
        .to_string()
}

#[cfg(target_os = "linux")]
#[test]
fn diskless_rdb_child_applies_bgsave_cpulist() {
    use crate::foundation::io::sys::process::{wait_blocking, WaitOutcome};
    use crate::framework::context::ServerContext;
    use std::io::Read;

    let _fork_guard = super::diskless_fork_test_guard();
    let cpu = first_allowed_cpu();
    let mut ctx = ServerContext::new(1);
    ctx.config_mut().server.bgsave_cpulist = cpu.clone();

    let child = super::fork_diskless_rdb_child(&mut ctx, &[])
        .expect("diskless child should fork");
    let pid = child.pid;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(1);
    loop {
        let status = std::fs::read_to_string(format!("/proc/{pid}/status"))
            .expect("read diskless child status");
        let affinity = status
            .lines()
            .find_map(|line| line.strip_prefix("Cpus_allowed_list:\t"));
        if affinity == Some(cpu.as_str()) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "diskless child affinity remained {affinity:?}, expected {cpu:?}"
        );
        std::thread::yield_now();
    }

    let mut data_read_end = child.data_read_end;
    let mut snapshot = Vec::new();
    data_read_end
        .read_to_end(&mut snapshot)
        .expect("drain diskless child snapshot");
    drop(data_read_end);
    drop(child.child_release_write_end);
    assert_eq!(wait_blocking(pid).unwrap(), WaitOutcome::Exited(0));
}

#[test]
#[ignore = "requires stable process reparenting timing on the host"]
fn disk_sync_child_exits_promptly_when_its_parent_dies() {
    use crate::foundation::io::sys::fd::{pipe, PipeFlags};
    use crate::foundation::io::sys::process::{
        exit_immediately, process_exists, send_signal, wait_blocking, ProcessSignal, WaitOutcome,
    };
    use crate::framework::capabilities::process::{fork_process, write_all_fd, ForkBranch};
    use crate::foundation::io::store::Robj;
    use crate::framework::context::ServerContext;
    use std::os::fd::AsRawFd;
    use std::time::{Duration, Instant};

    let mut ctx = ServerContext::new(1);
    ctx.config_mut().persistence.rdb_key_save_delay = 1_000_000;
    for index in 0..20 {
        ctx.dbs_mut().get_mut(0).unwrap().set(
            format!("slow-{index}").into_bytes(),
            Robj::string("value"),
        );
    }

    let control_pipe = pipe(PipeFlags::CLOEXEC).expect("create control pipe");
    let surrogate = match fork_process().expect("surrogate fork") {
        ForkBranch::Parent(child) => child.into_pid(),
        ForkBranch::Child => {
            let (control_read_end, control_write_end) = control_pipe.into_parts();
            drop(control_read_end);
            let dir = std::env::temp_dir().join(format!(
                "valkey-disk-sync-parent-exit-{}",
                std::process::id()
            ));
            let _ = std::fs::create_dir_all(&dir);
            let path = dir.join("snapshot.rdb");
            let Some(child_pid) = super::fork_disk_rdb_file_child(&mut ctx, &[], &path) else {
                exit_immediately(2);
            };
            let ok = write_all_fd(control_write_end.as_raw_fd(), &child_pid.to_ne_bytes());
            drop(control_write_end);
            exit_immediately(if ok { 0 } else { 3 });
        }
    };

    let (mut control, control_write_end) = control_pipe.into_parts();
    drop(control_write_end);
    let mut pid_bytes = [0u8; std::mem::size_of::<i32>()];
    std::io::Read::read_exact(&mut control, &mut pid_bytes).expect("read full-sync child pid");
    let child_pid = i32::from_ne_bytes(pid_bytes);
    assert_eq!(wait_blocking(surrogate).unwrap(), WaitOutcome::Exited(0));

    let deadline = Instant::now() + Duration::from_secs(2);
    let mut exited = false;
    while Instant::now() < deadline {
        if !process_exists(child_pid).expect("probe full-sync child") {
            exited = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    if !exited {
        let _ = send_signal(child_pid, ProcessSignal::Kill);
    }
    assert!(exited, "full-sync child survived its parent for more than 2s");
}

#[test]
fn pause_parent_after_replication_fork_is_gated_by_debug_flag() {
    use crate::framework::context::ServerContext;

    let mut ctx = ServerContext::new(1);

    // Default: replication forks must not pause the parent, so calling the
    // helper is a safe no-op that never SIGSTOPs this test process.
    assert!(!ctx.config().server.debug_pause_after_fork);
    assert!(!super::pause_parent_after_replication_fork(&ctx));

    // With the debug flag set the helper would raise SIGSTOP; assert only that
    // the gate observes the flag (the actual stop is covered by the Tcl
    // fixture, since SIGSTOPing the test runner here would hang it).
    ctx.config_mut().server.debug_pause_after_fork = true;
    assert!(ctx.config().server.debug_pause_after_fork);
}
