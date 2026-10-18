use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command};
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

use crate::bridge::{event_to_resp, load_test_sequences, TestEvent, TestSequence};

static SERVER_START_LOCK: Mutex<()> = Mutex::new(());

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind :0 failed");
    listener.local_addr().unwrap().port()
}

fn start_replica(port: u16) -> Child {
    Command::new(env!("CARGO_BIN_EXE_versal-server"))
        .arg("--port")
        .arg(port.to_string())
        .arg("--loglevel")
        .arg("warning")
        .spawn()
        .expect("failed to start versal-server")
}

fn wait_until_ready(port: u16) {
    let start = std::time::Instant::now();
    loop {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }
        if start.elapsed().as_secs() > 10 {
            panic!("replica did not become ready on port {}", port);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn send_replicaof(replica_port: u16, master_port: u16) -> TcpStream {
    let mut stream =
        TcpStream::connect(("127.0.0.1", replica_port)).expect("failed to connect to replica");
    let cmd = format!(
        "*3\r\n$9\r\nREPLICAOF\r\n$9\r\n127.0.0.1\r\n${}\r\n{}\r\n",
        master_port.to_string().len(),
        master_port
    );
    stream
        .write_all(cmd.as_bytes())
        .expect("failed to send REPLICAOF");
    stream.set_read_timeout(Some(Duration::from_secs(5))).ok();
    let mut resp = vec![0u8; 1024];
    let n = stream
        .read(&mut resp)
        .expect("failed to read REPLICAOF response");
    resp.truncate(n);
    assert_eq!(
        &resp,
        b"+OK\r\n",
        "REPLICAOF should return OK, got: {:?}",
        String::from_utf8_lossy(&resp)
    );
    stream
}

fn accept_master_connection(listener: &TcpListener, timeout_secs: u64) -> TcpStream {
    listener.set_nonblocking(true).ok();
    let start = std::time::Instant::now();
    loop {
        if let Ok((stream, _addr)) = listener.accept() {
            stream.set_nonblocking(false).ok();
            return stream;
        }
        if start.elapsed().as_secs() > timeout_secs {
            panic!("timed out waiting for replica to connect to master");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn resp_command_len(input: &[u8]) -> Option<usize> {
    if input.first() != Some(&b'*') {
        return None;
    }
    let count_end = input.windows(2).position(|window| window == b"\r\n")?;
    let count = std::str::from_utf8(&input[1..count_end])
        .ok()?
        .parse::<usize>()
        .ok()?;
    let mut cursor = count_end + 2;
    for _ in 0..count {
        if input.get(cursor) != Some(&b'$') {
            return None;
        }
        let length_start = cursor + 1;
        let length_end = input[length_start..]
            .windows(2)
            .position(|window| window == b"\r\n")?
            + length_start;
        let length = std::str::from_utf8(&input[length_start..length_end])
            .ok()?
            .parse::<usize>()
            .ok()?;
        cursor = length_end + 2;
        let frame_end = cursor.checked_add(length + 2)?;
        if input.get(cursor..frame_end)?.get(length..) != Some(b"\r\n") {
            return None;
        }
        cursor = frame_end;
    }
    Some(cursor)
}

struct MasterCommandReader {
    pending: Vec<u8>,
}

impl MasterCommandReader {
    fn read_command(&mut self, stream: &mut TcpStream, timeout_secs: u64) -> Vec<u8> {
        let mut buf = vec![0u8; 4096];
        stream.set_nonblocking(true).ok();
        let start = std::time::Instant::now();
        loop {
            if let Some(command_len) = resp_command_len(&self.pending) {
                let command = self.pending[..command_len].to_vec();
                self.pending.drain(..command_len);
                stream.set_nonblocking(false).ok();
                return command;
            }
            match stream.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => self.pending.extend_from_slice(&buf[..n]),
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    if start.elapsed().as_secs() > timeout_secs {
                        break;
                    }
                    thread::sleep(Duration::from_millis(50));
                }
                Err(_) => break,
            }
        }
        stream.set_nonblocking(false).ok();
        Vec::new()
    }
}

fn poll_read(
    reader: &mut MasterCommandReader,
    stream: &mut TcpStream,
    timeout_secs: u64,
) -> Vec<u8> {
    reader.read_command(stream, timeout_secs)
}

#[test]
fn resp_command_len_stops_at_the_first_pipelined_array() {
    let first = b"*2\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n";
    let second = b"*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$3\r\neof\r\n";
    let mut pipelined = first.to_vec();
    pipelined.extend_from_slice(second);

    assert_eq!(resp_command_len(&pipelined), Some(first.len()));
}

fn run_test_sequence(seq: &TestSequence, master_port: u16, replica_port: u16) {
    let _lock = SERVER_START_LOCK.lock().unwrap_or_else(|e| e.into_inner());

    let master_listener =
        TcpListener::bind(("127.0.0.1", master_port)).expect("failed to bind master port");

    let mut replica = start_replica(replica_port);
    wait_until_ready(replica_port);

    let mut replica_ctl = send_replicaof(replica_port, master_port);

    let mut master_stream = accept_master_connection(&master_listener, 10);
    let mut command_reader = MasterCommandReader {
        pending: Vec::new(),
    };

    for event in &seq.events {
        match event {
            TestEvent::ReplyPong => {
                let data = poll_read(&mut command_reader, &mut master_stream, 8);
                let cmd_str = String::from_utf8_lossy(&data);
                assert!(
                    cmd_str.contains("PING"),
                    "expected PING, got: {:?}",
                    cmd_str
                );
                let resp = event_to_resp(event);
                master_stream
                    .write_all(&resp)
                    .expect("failed to write PONG");
            }
            TestEvent::ReplyOk => {
                let data = poll_read(&mut command_reader, &mut master_stream, 5);
                let cmd_str = String::from_utf8_lossy(&data);
                assert!(
                    cmd_str.contains("REPLCONF"),
                    "expected REPLCONF, got: {:?}",
                    cmd_str
                );
                let resp = event_to_resp(event);
                master_stream.write_all(&resp).expect("failed to write OK");
            }
            TestEvent::ReplyFullResync => {
                let data = poll_read(&mut command_reader, &mut master_stream, 5);
                let cmd_str = String::from_utf8_lossy(&data);
                assert!(
                    cmd_str.contains("PSYNC"),
                    "expected PSYNC, got: {:?}",
                    cmd_str
                );
                let resp = event_to_resp(event);
                master_stream
                    .write_all(&resp)
                    .expect("failed to write FULLRESYNC");
            }
            TestEvent::SendRdbBulk(_) | TestEvent::SendRdbComplete => {
                let resp = event_to_resp(event);
                if !resp.is_empty() {
                    master_stream
                        .write_all(&resp)
                        .expect("failed to write RDB data");
                }
            }
            TestEvent::SendError(_) => {
                let resp = event_to_resp(event);
                master_stream
                    .write_all(&resp)
                    .expect("failed to write error");
            }
            TestEvent::WaitTimeout(ms) => {
                thread::sleep(Duration::from_millis(*ms));
            }
            TestEvent::AssertState(_) => {}
        }
    }

    let cleanup = b"*3\r\n$9\r\nREPLICAOF\r\n$2\r\nNO\r\n$3\r\nONE\r\n";
    replica_ctl.set_nonblocking(false).ok();
    let _ = replica_ctl.write_all(cleanup);
    thread::sleep(Duration::from_millis(200));
    let _ = replica.kill();
    let _ = replica.wait();
}

#[test]
fn test_repl_handshake_normal_path() {
    let sequences = load_test_sequences("tests/formal/fixtures/normal_path.xml");
    let seq = sequences
        .into_iter()
        .find(|s| s.name == "normal_path")
        .expect("normal_path sequence not found");
    let master_port = free_port();
    let replica_port = free_port();
    run_test_sequence(&seq, master_port, replica_port);
}

#[test]
fn test_repl_handshake_timeout() {
    let sequences = load_test_sequences("tests/formal/fixtures/timeout_path.xml");
    let seq = sequences
        .into_iter()
        .find(|s| s.name == "timeout_in_wait_pong")
        .expect("timeout_in_wait_pong sequence not found");
    let master_port = free_port();
    let replica_port = free_port();
    run_test_sequence(&seq, master_port, replica_port);
}

#[test]
fn test_repl_handshake_error() {
    let sequences = load_test_sequences("tests/formal/fixtures/rdb_error_path.xml");
    let seq = sequences
        .into_iter()
        .find(|s| s.name == "error_in_replconf_port")
        .expect("error_in_replconf_port sequence not found");
    let master_port = free_port();
    let replica_port = free_port();
    run_test_sequence(&seq, master_port, replica_port);
}
