//! Black-box coverage for the cluster priority contract introduced in Valkey 5813754ff.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

struct Server {
    child: Child,
    dir: PathBuf,
    port: u16,
}

impl Server {
    fn start(extra: &[&str]) -> Self {
        Self::spawn(None, extra).wait_ready()
    }

    fn start_from_config(config: &str, extra: &[&str]) -> Self {
        Self::spawn(Some(config), extra).wait_ready()
    }

    fn spawn(config: Option<&str>, extra: &[&str]) -> Self {
        let (listener, bus_listener, port) = loop {
            let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
            let port = listener.local_addr().unwrap().port();
            if let Some(bus_port) = port.checked_add(10_000) {
                if let Ok(bus_listener) = TcpListener::bind(("127.0.0.1", bus_port)) {
                    break (listener, bus_listener, port);
                }
            }
        };
        let dir = std::env::temp_dir().join(format!(
            "versal-cluster-priority-581-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&dir).unwrap();
        let output = std::fs::File::create(dir.join("server.log")).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_versal-server"));
        if let Some(config) = config {
            let path = dir.join("versal.conf");
            std::fs::write(&path, config).unwrap();
            command.arg(path);
        }
        drop((listener, bus_listener));
        let child = command
            .args([
                "--bind",
                "127.0.0.1",
                "--port",
                &port.to_string(),
                "--save",
                "",
                "--enable-debug-command",
                "yes",
            ])
            .args(extra)
            .current_dir(&dir)
            .stdout(Stdio::from(output.try_clone().unwrap()))
            .stderr(Stdio::from(output))
            .spawn()
            .unwrap();
        Self { child, dir, port }
    }

    fn wait_ready(mut self) -> Self {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if TcpStream::connect(("127.0.0.1", self.port)).is_ok() {
                break;
            }
            assert!(
                self.child.try_wait().unwrap().is_none() && Instant::now() < deadline,
                "server did not start: {}",
                std::fs::read_to_string(self.dir.join("server.log")).unwrap()
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        self
    }

    fn connect(&self) -> Client {
        Client::connect(self.port)
    }

    fn assert_startup_rejected(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(5);
        let status = loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                break status;
            }
            assert!(
                Instant::now() < deadline,
                "invalid configuration did not exit"
            );
            std::thread::sleep(Duration::from_millis(10));
        };
        assert!(
            !status.success(),
            "invalid configuration exited successfully"
        );
        let log = std::fs::read_to_string(self.dir.join("server.log")).unwrap();
        assert!(log.contains("cluster-replica-priority"), "{log}");
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

struct Client(BufReader<TcpStream>);

impl Client {
    fn connect(port: u16) -> Self {
        let stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        Self(BufReader::new(stream))
    }

    fn command(&mut self, args: &[&str]) -> String {
        let mut request = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            request.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            request.extend_from_slice(arg.as_bytes());
            request.extend_from_slice(b"\r\n");
        }
        self.0.get_mut().write_all(&request).unwrap();
        self.response()
    }

    fn response(&mut self) -> String {
        let mut line = String::new();
        self.0.read_line(&mut line).unwrap();
        assert!(!line.is_empty(), "server closed the connection");
        match line.as_bytes()[0] {
            b'$' => {
                let len: i64 = line[1..].trim().parse().unwrap();
                if len >= 0 {
                    let mut body = vec![0; len as usize + 2];
                    self.0.read_exact(&mut body).unwrap();
                    line.push_str(std::str::from_utf8(&body).unwrap());
                }
            }
            b'*' => {
                let count: i64 = line[1..].trim().parse().unwrap();
                for _ in 0..count.max(0) {
                    line.push_str(&self.response());
                }
            }
            _ => {}
        }
        line
    }
}

fn node_id(client: &mut Client) -> String {
    client
        .command(&["CLUSTER", "MYID"])
        .lines()
        .find(|line| line.len() == 40)
        .expect("CLUSTER MYID must return a node id")
        .to_string()
}

fn wait_priority(client: &mut Client, node_id: &str, value: u32) {
    let expected = format!(":{value}\r\n");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let reply = client.command(&["DEBUG", "CLUSTER-REPLICA-PRIORITY", node_id]);
        if reply == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "priority did not converge: {reply:?}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn heartbeat(sender: &str, port: u16, bus_port: u16, kind: u16, priorities: &[u32]) -> Vec<u8> {
    let mut packet = vec![0; 2256];
    packet[..4].copy_from_slice(b"RCmb");
    packet[8..10].copy_from_slice(&1u16.to_be_bytes());
    packet[10..12].copy_from_slice(&port.to_be_bytes());
    packet[12..14].copy_from_slice(&kind.to_be_bytes());
    packet[40..80].copy_from_slice(sender.as_bytes());
    packet[2168..2177].copy_from_slice(b"127.0.0.1");
    packet[2214..2216].copy_from_slice(&(priorities.len() as u16).to_be_bytes());
    packet[2248..2250].copy_from_slice(&bus_port.to_be_bytes());
    packet[2250..2252].copy_from_slice(&1u16.to_be_bytes());
    if !priorities.is_empty() {
        packet[2253] = 1 << 2;
    }
    for priority in priorities {
        packet.extend_from_slice(&16u32.to_be_bytes());
        packet.extend_from_slice(&9u16.to_be_bytes());
        packet.extend_from_slice(&[0; 2]);
        packet.extend_from_slice(&priority.to_be_bytes());
        packet.extend_from_slice(&[0xee; 4]);
    }
    let length = packet.len() as u32;
    packet[4..8].copy_from_slice(&length.to_be_bytes());
    packet
}

fn read_bus_packet(stream: &mut TcpStream) -> Vec<u8> {
    let mut prefix = [0; 8];
    stream.read_exact(&mut prefix).unwrap();
    assert_eq!(&prefix[..4], b"RCmb");
    let length = u32::from_be_bytes(prefix[4..8].try_into().unwrap()) as usize;
    assert!(
        (2256..=1_000_000).contains(&length),
        "invalid bus frame length"
    );
    let mut packet = vec![0; length];
    packet[..8].copy_from_slice(&prefix);
    stream.read_exact(&mut packet[8..]).unwrap();
    packet
}

fn read_priority_extension(stream: &mut TcpStream) -> Option<u32> {
    let packet = read_bus_packet(stream);
    let gossip_count = u16::from_be_bytes(packet[14..16].try_into().unwrap()) as usize;
    let extension_count = u16::from_be_bytes(packet[2214..2216].try_into().unwrap());
    let mut offset = 2256 + 104 * gossip_count;
    let mut priority = None;
    for _ in 0..extension_count {
        let extension_length =
            u32::from_be_bytes(packet[offset..offset + 4].try_into().unwrap()) as usize;
        let kind = u16::from_be_bytes(packet[offset + 4..offset + 6].try_into().unwrap());
        if kind == 9 {
            assert_eq!(extension_length, 16);
            assert_eq!(&packet[offset + 6..offset + 8], &[0; 2]);
            assert_eq!(&packet[offset + 12..offset + 16], &[0; 4]);
            priority = Some(u32::from_be_bytes(
                packet[offset + 8..offset + 12].try_into().unwrap(),
            ));
        }
        offset += extension_length;
    }
    assert_eq!(offset, packet.len());
    priority
}

fn complete_reverse_handshake(
    fake_bus: &TcpListener,
    peer_id: &str,
    peer_port: u16,
    priority: u32,
) -> TcpStream {
    fake_bus.set_nonblocking(true).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut reverse = loop {
        match fake_bus.accept() {
            Ok((stream, _)) => break stream,
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                assert!(
                    Instant::now() < deadline,
                    "reverse bus connection did not arrive"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => panic!("reverse bus accept failed: {error}"),
        }
    };
    reverse
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let ping = read_bus_packet(&mut reverse);
    assert_eq!(u16::from_be_bytes(ping[12..14].try_into().unwrap()), 0);
    reverse
        .write_all(&heartbeat(
            peer_id,
            peer_port,
            fake_bus.local_addr().unwrap().port(),
            1,
            &[priority],
        ))
        .unwrap();
    reverse
}

fn assert_local_priority(client: &mut Client, id: &str, priority: u32) {
    let value = priority.to_string();
    assert_eq!(
        client.command(&["CONFIG", "GET", "cluster-replica-priority"]),
        format!(
            "*2\r\n$24\r\ncluster-replica-priority\r\n${}\r\n{value}\r\n",
            value.len()
        )
    );
    assert_eq!(
        client.command(&["DEBUG", "CLUSTER-REPLICA-PRIORITY", id]),
        format!(":{priority}\r\n")
    );
}

#[test]
fn cluster_priority_survives_soft_and_hard_reset_and_rejoin() {
    let server = Server::start(&[
        "--cluster-enabled",
        "yes",
        "--cluster-replica-priority",
        "10",
        "--cluster-ping-interval",
        "100",
    ]);
    let mut client = server.connect();
    let mut id = node_id(&mut client);
    for mode in ["SOFT", "HARD"] {
        assert_eq!(client.command(&["CLUSTER", "RESET", mode]), "+OK\r\n");
        let next_id = node_id(&mut client);
        if mode == "HARD" {
            assert_ne!(next_id, id);
        } else {
            assert_eq!(next_id, id);
        }
        id = next_id;
        assert_local_priority(&mut client, &id, 10);
        let observer =
            Server::start(&["--cluster-enabled", "yes", "--cluster-ping-interval", "100"]);
        let mut observer = observer.connect();
        assert_eq!(
            observer.command(&["CLUSTER", "MEET", "127.0.0.1", &server.port.to_string()]),
            "+OK\r\n"
        );
        wait_priority(&mut observer, &id, 10);
    }
}

#[test]
fn cluster_priority_tcp_rebind_failure_is_atomic() {
    let server = Server::start(&[
        "--cluster-enabled",
        "yes",
        "--cluster-replica-priority",
        "10",
    ]);
    let mut client = server.connect();
    let id = node_id(&mut client);
    let occupied = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = occupied.local_addr().unwrap().port().to_string();
    let reply = client.command(&[
        "CONFIG",
        "SET",
        "cluster-replica-priority",
        "5",
        "port",
        &port,
    ]);
    assert!(reply.contains("Unable to listen on this port"), "{reply}");
    assert_local_priority(&mut client, &id, 10);
    assert_eq!(server.connect().command(&["PING"]), "+PONG\r\n");
}

#[cfg(feature = "tls")]
#[test]
fn cluster_priority_tls_reconfigure_failure_is_atomic() {
    let server = Server::start(&[
        "--cluster-enabled",
        "yes",
        "--cluster-replica-priority",
        "10",
        "--cluster-ping-interval",
        "100",
    ]);
    let observer = Server::start(&["--cluster-enabled", "yes", "--cluster-ping-interval", "100"]);
    let mut client = server.connect();
    let mut observer = observer.connect();
    let id = node_id(&mut client);
    assert_eq!(
        observer.command(&["CLUSTER", "MEET", "127.0.0.1", &server.port.to_string()]),
        "+OK\r\n"
    );
    wait_priority(&mut observer, &id, 10);
    let reserved_tls = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let tls_port = reserved_tls.local_addr().unwrap().port().to_string();
    let missing_cert = server.dir.join("missing.crt");
    let reply = client.command(&[
        "CONFIG",
        "SET",
        "cluster-replica-priority",
        "5",
        "tls-port",
        &tls_port,
        "tls-cert-file",
        missing_cert.to_str().unwrap(),
    ]);
    assert!(
        reply.contains("Unable to update TLS configuration"),
        "{reply}"
    );
    assert_local_priority(&mut client, &id, 10);
    assert_eq!(
        client.command(&["CONFIG", "GET", "tls-port"]),
        "*2\r\n$8\r\ntls-port\r\n$1\r\n0\r\n"
    );
    assert_eq!(
        client.command(&["CONFIG", "GET", "tls-cert-file"]),
        "*2\r\n$13\r\ntls-cert-file\r\n$0\r\n\r\n"
    );
    std::thread::sleep(Duration::from_millis(250));
    assert_eq!(
        observer.command(&["DEBUG", "CLUSTER-REPLICA-PRIORITY", &id]),
        ":10\r\n"
    );
    assert_eq!(server.connect().command(&["PING"]), "+PONG\r\n");
}

#[test]
fn cluster_priority_config_is_bounded_and_independent() {
    let server = Server::start(&["--cluster-enabled", "yes"]);
    let mut client = server.connect();
    let id = node_id(&mut client);
    assert_eq!(
        client.command(&["CONFIG", "GET", "cluster-replica-priority"]),
        "*2\r\n$24\r\ncluster-replica-priority\r\n$1\r\n0\r\n"
    );
    assert_eq!(
        client.command(&["DEBUG", "CLUSTER-REPLICA-PRIORITY", &id]),
        ":0\r\n"
    );
    for value in ["0", "1", "2147483647"] {
        assert_eq!(
            client.command(&["CONFIG", "SET", "cluster-replica-priority", value]),
            "+OK\r\n"
        );
        assert_eq!(
            client.command(&["DEBUG", "CLUSTER-REPLICA-PRIORITY", &id]),
            format!(":{value}\r\n")
        );
    }
    for value in ["-1", "2147483648", "4294967295", "abc", "1mb", ""] {
        assert!(
            client
                .command(&["CONFIG", "SET", "cluster-replica-priority", value])
                .starts_with('-'),
            "invalid cluster priority was accepted: {value:?}"
        );
        assert_eq!(
            client.command(&["DEBUG", "CLUSTER-REPLICA-PRIORITY", &id]),
            ":2147483647\r\n"
        );
    }
    assert!(client
        .command(&[
            "CONFIG",
            "SET",
            "cluster-replica-priority",
            "5",
            "cluster-node-timeout",
            "-1"
        ])
        .starts_with('-'));
    assert_eq!(
        client.command(&["DEBUG", "CLUSTER-REPLICA-PRIORITY", &id]),
        ":2147483647\r\n"
    );
    assert_eq!(
        client.command(&["CONFIG", "SET", "replica-priority", "0"]),
        "+OK\r\n"
    );
    std::thread::sleep(Duration::from_millis(150));
    assert_eq!(
        client.command(&["DEBUG", "CLUSTER-REPLICA-PRIORITY", &id]),
        ":2147483647\r\n"
    );
    assert!(client
        .command(&[
            "DEBUG",
            "CLUSTER-REPLICA-PRIORITY",
            "0000000000000000000000000000000000000000"
        ])
        .contains("Unknown node"));
}

#[test]
fn cluster_priority_cli_initializes_self_and_noncluster_debug_is_rejected() {
    let server = Server::start(&[
        "--cluster-enabled",
        "yes",
        "--cluster-replica-priority",
        "10",
    ]);
    let mut client = server.connect();
    let id = node_id(&mut client);
    assert_eq!(
        client.command(&["DEBUG", "CLUSTER-REPLICA-PRIORITY", &id]),
        ":10\r\n"
    );
    let standalone = Server::start(&[]);
    assert!(standalone
        .connect()
        .command(&["DEBUG", "CLUSTER-REPLICA-PRIORITY", &id])
        .contains("cluster mode"));
}

#[test]
fn cluster_priority_changes_propagate_and_zero_clears_remote_state() {
    let first = Server::start(&["--cluster-enabled", "yes", "--cluster-ping-interval", "100"]);
    let second = Server::start(&["--cluster-enabled", "yes", "--cluster-ping-interval", "100"]);
    let mut sender = first.connect();
    let mut observer = second.connect();
    let id = node_id(&mut sender);
    assert_eq!(
        observer.command(&["CLUSTER", "MEET", "127.0.0.1", &first.port.to_string()]),
        "+OK\r\n"
    );
    wait_priority(&mut observer, &id, 0);
    for value in [10, 5, 0] {
        assert_eq!(
            sender.command(&[
                "CONFIG",
                "SET",
                "cluster-replica-priority",
                &value.to_string()
            ]),
            "+OK\r\n"
        );
        wait_priority(&mut observer, &id, value);
    }
}

#[test]
fn cluster_priority_file_cli_and_rewrite_use_the_registry() {
    let config = "cluster-enabled yes\ncluster-replica-priority 7\n";
    let server = Server::start_from_config(config, &[]);
    let mut client = server.connect();
    let id = node_id(&mut client);
    assert_eq!(
        client.command(&["DEBUG", "CLUSTER-REPLICA-PRIORITY", &id]),
        ":7\r\n"
    );
    assert_eq!(
        client.command(&["CONFIG", "SET", "cluster-replica-priority", "5"]),
        "+OK\r\n"
    );
    assert_eq!(client.command(&["CONFIG", "REWRITE"]), "+OK\r\n");
    let rewritten = std::fs::read_to_string(server.dir.join("versal.conf")).unwrap();
    assert!(rewritten
        .lines()
        .any(|line| line == "cluster-replica-priority 5"));

    let overridden = Server::start_from_config(config, &["--cluster-replica-priority", "10"]);
    let mut client = overridden.connect();
    let id = node_id(&mut client);
    assert_eq!(
        client.command(&["DEBUG", "CLUSTER-REPLICA-PRIORITY", &id]),
        ":10\r\n"
    );
}

#[test]
fn cluster_priority_invalid_file_and_cli_values_abort_startup() {
    for directive in [
        "cluster-replica-priority",
        "cluster-replica-priority \"\"",
        "cluster-replica-priority -1",
        "cluster-replica-priority 2147483648",
        "cluster-replica-priority abc",
        "cluster-replica-priority 1mb",
        "cluster-replica-priority 1 2",
    ] {
        Server::spawn(Some(directive), &[]).assert_startup_rejected();
    }
    for value in ["-1", "2147483648", "abc", "1mb", ""] {
        Server::spawn(None, &["--cluster-replica-priority", value]).assert_startup_rejected();
    }
    Server::spawn(None, &["--cluster-replica-priority"]).assert_startup_rejected();
}

#[test]
fn cluster_priority_wire_layout_full_u32_duplicates_and_legacy_heartbeat() {
    let server = Server::start(&["--cluster-enabled", "yes", "--loglevel", "debug"]);
    let mut client = server.connect();
    assert_eq!(
        client.command(&["CONFIG", "SET", "cluster-replica-priority", "10"]),
        "+OK\r\n"
    );
    let fake_client = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let fake_bus = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let peer_port = fake_client.local_addr().unwrap().port();
    let peer_bus_port = fake_bus.local_addr().unwrap().port();
    let peer_id = "ffffffffffffffffffffffffffffffffffffffff";
    let mut stream = TcpStream::connect(("127.0.0.1", server.port + 10_000)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .write_all(&heartbeat(
            peer_id,
            peer_port,
            peer_bus_port,
            2,
            &[u32::MAX],
        ))
        .unwrap();
    assert_eq!(read_priority_extension(&mut stream), Some(10));
    let _reverse = complete_reverse_handshake(&fake_bus, peer_id, peer_port, u32::MAX);
    wait_priority(&mut client, peer_id, u32::MAX);

    // The anonymous MEET link may be retired with its handshake placeholder.
    // Continue on a fresh inbound link after the peer identity is confirmed.
    let mut stream = TcpStream::connect(("127.0.0.1", server.port + 10_000)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .write_all(&heartbeat(peer_id, peer_port, peer_bus_port, 0, &[10, 5]))
        .unwrap();
    wait_priority(&mut client, peer_id, 5);
    assert_eq!(read_priority_extension(&mut stream), Some(10));

    assert_eq!(
        client.command(&["CONFIG", "SET", "cluster-replica-priority", "0"]),
        "+OK\r\n"
    );
    stream
        .write_all(&heartbeat(peer_id, peer_port, peer_bus_port, 0, &[]))
        .unwrap();
    wait_priority(&mut client, peer_id, 0);
    let deadline = Instant::now() + Duration::from_secs(5);
    while read_priority_extension(&mut stream).is_some() {
        assert!(
            Instant::now() < deadline,
            "zero priority was still advertised"
        );
    }

    stream
        .write_all(&heartbeat(peer_id, peer_port, peer_bus_port, 0, &[5]))
        .unwrap();
    wait_priority(&mut client, peer_id, 5);
    let mut malformed = heartbeat(peer_id, peer_port, peer_bus_port, 0, &[10]);
    malformed[2256..2260].copy_from_slice(&8u32.to_be_bytes());
    stream.write_all(&malformed).unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let log = std::fs::read_to_string(server.dir.join("server.log")).unwrap();
        if log.contains("replica-priority extension payload must be 8 bytes") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "malformed extension was not rejected: {log}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        client.command(&["DEBUG", "CLUSTER-REPLICA-PRIORITY", peer_id]),
        ":5\r\n"
    );
}
