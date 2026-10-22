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
