use super::fd::{
    descriptor_identity, descriptor_is_open, is_nonblocking, pipe, set_nonblocking, PipeFlags,
};
use super::os::{is_tty, poll_descriptors, PollDescriptor};
use super::process::{
    process_exists, send_signal, wait_blocking, wait_nonblocking, ProcessSignal, WaitOutcome,
};
use super::socket::{send_timeout, socket_port};
use super::time::sleep_interruptible;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::fd::{AsFd, AsRawFd, RawFd};
use std::time::{Duration, Instant};

fn assert_descriptor_released(fd: RawFd, original: (libc::dev_t, libc::ino_t)) {
    assert_ne!(
        descriptor_identity(fd),
        Some(original),
        "descriptor number still refers to the dropped pipe endpoint"
    );
}

#[test]
fn owned_pipe_round_trips_and_closes_write_end_on_drop() {
    let mut pipe = pipe(PipeFlags::CLOEXEC).expect("create pipe");
    let write_fd = pipe.write.as_raw_fd();
    let write_identity = descriptor_identity(write_fd).expect("identify pipe write end");
    pipe.write.write_all(b"ok").expect("write pipe");
    let mut bytes = [0_u8; 2];
    pipe.read.read_exact(&mut bytes).expect("read pipe");
    assert_eq!(&bytes, b"ok");
    drop(pipe);
    assert_descriptor_released(write_fd, write_identity);
}

#[test]
fn owned_pipe_closes_read_end_on_drop() {
    let pipe = pipe(PipeFlags::CLOEXEC).expect("create pipe");
    let read_fd = pipe.read.as_raw_fd();
    let read_identity = descriptor_identity(read_fd).expect("identify pipe read end");
    drop(pipe);
    assert_descriptor_released(read_fd, read_identity);
}

#[test]
fn descriptor_and_socket_options_are_exposed_without_raw_syscalls() {
    use std::os::unix::net::UnixStream;

    let (stream, _peer) = UnixStream::pair().expect("create socket pair");
    assert!(descriptor_is_open(stream.as_raw_fd()));
    set_nonblocking(stream.as_fd(), true).expect("set nonblocking");
    assert!(is_nonblocking(stream.as_fd()).expect("query nonblocking"));
    stream
        .set_write_timeout(Some(Duration::from_millis(250)))
        .expect("set write timeout");
    assert!(
        send_timeout(stream.as_fd()).expect("query send timeout") >= Duration::from_millis(250)
    );
}

#[test]
fn wait_adapters_distinguish_running_and_exited_child() {
    let mut child = std::process::Command::new("sh")
        .args(["-c", "sleep 0.1; exit 7"])
        .spawn()
        .expect("spawn child");
    let pid = child.id() as i32;
    assert_eq!(wait_nonblocking(pid).unwrap(), WaitOutcome::Running);
    std::thread::sleep(Duration::from_millis(150));
    assert_eq!(wait_blocking(pid).unwrap(), WaitOutcome::Exited(7));
    let _ = child.try_wait();
}

#[test]
fn signal_adapter_stops_continues_and_kills_owned_child() {
    let mut child = std::process::Command::new("sh")
        .args(["-c", "while :; do sleep 1; done"])
        .spawn()
        .expect("spawn child");
    let pid = child.id() as i32;
    assert!(process_exists(pid).expect("probe child"));
    send_signal(pid, ProcessSignal::Stop).expect("stop child");
    send_signal(pid, ProcessSignal::Continue).expect("continue child");
    send_signal(pid, ProcessSignal::Kill).expect("kill child");
    assert!(matches!(
        wait_blocking(pid).unwrap(),
        WaitOutcome::Signaled(libc::SIGKILL)
    ));
    let _ = child.wait();
    assert!(!process_exists(pid).expect("probe reaped child"));
}

#[test]
fn socket_port_reports_local_and_peer_ports() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind listener");
    let listener_addr = listener.local_addr().expect("listener address");
    let client = TcpStream::connect(listener_addr).expect("connect client");
    let client_addr = client.local_addr().expect("client address");
    let (server, _) = listener.accept().expect("accept client");
    assert_eq!(
        socket_port(server.as_fd(), false).unwrap(),
        listener_addr.port()
    );
    assert_eq!(
        socket_port(server.as_fd(), true).unwrap(),
        client_addr.port()
    );
    assert_eq!(
        socket_port(client.as_fd(), true).unwrap(),
        listener_addr.port()
    );
}

#[cfg(not(target_os = "linux"))]
#[test]
fn pidfd_reports_unsupported_without_a_platform_dependent_return_type() {
    let error = super::process::open_pidfd(std::process::id() as i32)
        .expect_err("non-Linux platforms do not support pidfd");
    assert_eq!(error.kind(), std::io::ErrorKind::Unsupported);
}

#[test]
fn socket_half_close_is_visible_before_buffered_bytes_are_consumed() {
    use std::net::Shutdown;
    use std::os::unix::net::UnixStream;

    let (mut stream, mut peer) = UnixStream::pair().expect("create socket pair");
    stream.set_nonblocking(true).expect("set nonblocking");
    peer.write_all(b"buffered").expect("write buffered bytes");
    peer.shutdown(Shutdown::Write).expect("half-close peer");
    assert!(super::socket::socket_read_closed_raw(stream.as_raw_fd()).expect("query half-close"));
    let mut bytes = [0; 8];
    stream.read_exact(&mut bytes).expect("read preserved bytes");
    assert_eq!(&bytes, b"buffered");
}

#[test]
fn poll_adapter_reports_readable_pipe() {
    let mut pipe = pipe(PipeFlags::CLOEXEC).expect("create pipe");
    pipe.write.write_all(b"x").expect("write pipe");
    let mut descriptors = [
        PollDescriptor::new(pipe.read.as_raw_fd(), libc::POLLIN),
        PollDescriptor::new(libc::STDIN_FILENO, libc::POLLIN),
    ];
    let ready = poll_descriptors(&mut descriptors, Duration::ZERO).expect("poll descriptors");
    assert!(ready > 0);
    assert_ne!(descriptors[0].revents() & libc::POLLIN, 0);
}

#[test]
fn pipe_is_not_a_terminal_and_cancelled_sleep_returns_immediately() {
    let pipe = pipe(PipeFlags::CLOEXEC).expect("create pipe");
    assert!(!is_tty(pipe.read.as_fd()));
    let start = Instant::now();
    sleep_interruptible(1_000_000, || true);
    assert!(start.elapsed() < Duration::from_millis(50));
}
