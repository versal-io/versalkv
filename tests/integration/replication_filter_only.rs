use crate::common::TestServer;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};
use versal::foundation::io::persistence::load_rdb_bytes_checked_with_functions;
use versal::foundation::io::store::kv_engine::DbSet;

const TIMEOUT: Duration = Duration::from_secs(15);
const MAX_RDB_BYTES: usize = 16 * 1024 * 1024;
const FUNCTION_CODE: &[u8] = b"#!lua name=filterlib\nredis.register_function('filter_echo', function(keys, args) return args[1] end)";
const KEY_VALUE: &[u8] = b"value\0with\r\nbinary";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Filter {
    All,
    Functions,
    Empty,
}

struct RawClient {
    stream: TcpStream,
}

impl RawClient {
    fn connect(server: &TestServer) -> Self {
        let stream = TcpStream::connect(("127.0.0.1", server.port)).unwrap();
        stream.set_write_timeout(Some(TIMEOUT)).unwrap();
        Self { stream }
    }

    fn send(&mut self, args: &[&[u8]]) {
        let mut request = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            request.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            request.extend_from_slice(arg);
            request.extend_from_slice(b"\r\n");
        }
        self.stream.write_all(&request).unwrap();
    }

    fn command(&mut self, args: &[&[u8]]) -> Vec<u8> {
        self.send(args);
        self.read_line(Instant::now() + TIMEOUT)
    }

    fn set_read_deadline(&self, deadline: Instant) {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .expect("protocol phase exceeded its absolute deadline");
        assert!(!remaining.is_zero());
        self.stream.set_read_timeout(Some(remaining)).unwrap();
    }

    fn read_exact(&mut self, bytes: &mut [u8], deadline: Instant) {
        let mut consumed = 0;
        while consumed < bytes.len() {
            self.set_read_deadline(deadline);
            let count = self.stream.read(&mut bytes[consumed..]).unwrap();
            assert_ne!(count, 0, "unexpected protocol EOF");
            consumed += count;
        }
    }

    fn read_line(&mut self, deadline: Instant) -> Vec<u8> {
        let mut line = Vec::new();
        loop {
            let mut byte = [0u8; 1];
            self.read_exact(&mut byte, deadline);
            if byte[0] == b'\r' {
                self.read_exact(&mut byte, deadline);
                assert_eq!(byte[0], b'\n', "invalid CRLF");
                return line;
            }
            assert_ne!(byte[0], b'\n', "bare LF in protocol line");
            line.push(byte[0]);
            assert!(line.len() <= 8192, "protocol line is too long");
        }
    }

    fn nonempty_line(&mut self, deadline: Instant) -> Vec<u8> {
        for _ in 0..=128 {
            let line = self.read_line(deadline);
            if !line.is_empty() {
                return line;
            }
        }
        panic!("too many CRLF keepalives");
    }

    fn read_rdb(&mut self, expects_eof: bool) -> Vec<u8> {
        let deadline = Instant::now() + TIMEOUT;
        let header = self.nonempty_line(deadline);
        if expects_eof {
            let marker = header.strip_prefix(b"$EOF:").expect("expected EOF framing");
            assert_eq!(marker.len(), 40);
            assert!(marker.iter().all(u8::is_ascii_hexdigit));
            let mut body = Vec::new();
            let mut searched = 0;
            loop {
                if let Some(position) = body[searched..]
                    .windows(marker.len())
                    .position(|window| window == marker)
                {
                    body.truncate(searched + position);
                    return body;
                }
                searched = body.len().saturating_sub(marker.len() - 1);
                // A read is shorter than the marker, so every EOF marker must
                // cross receive fragments regardless of TCP packet boundaries.
                let mut fragment = [0u8; 17];
                self.set_read_deadline(deadline);
                let count = self.stream.read(&mut fragment).unwrap();
                assert_ne!(count, 0, "RDB disconnected before its EOF marker");
                body.extend_from_slice(&fragment[..count]);
                assert!(body.len() <= MAX_RDB_BYTES + marker.len());
            }
        } else {
            let length =
                std::str::from_utf8(header.strip_prefix(b"$").expect("expected length framing"))
                    .unwrap()
                    .parse::<usize>()
                    .expect("All with diskless disabled must use a numeric length");
            assert!(length <= MAX_RDB_BYTES);
            let mut body = vec![0u8; length];
            self.read_exact(&mut body, deadline);
            body
        }
    }

    fn read_full_resync(&mut self, expects_eof: bool) -> Vec<u8> {
        let first = self.nonempty_line(Instant::now() + TIMEOUT);
        let words: Vec<_> = first.split(|byte| *byte == b' ').collect();
        assert_eq!(words.len(), 3);
        assert_eq!(words[0], b"+FULLRESYNC");
        assert_eq!(words[1].len(), 40);
        assert!(words[1].iter().all(u8::is_ascii_hexdigit));
        std::str::from_utf8(words[2])
            .unwrap()
            .parse::<i64>()
            .unwrap();
        self.read_rdb(expects_eof)
    }

    fn read_repl_command(&mut self, deadline: Instant) -> Vec<Vec<u8>> {
        let header = self.nonempty_line(deadline);
        let count = std::str::from_utf8(header.strip_prefix(b"*").unwrap())
            .unwrap()
            .parse::<usize>()
            .unwrap();
        assert!((1..=16).contains(&count));
        (0..count)
            .map(|_| {
                let header = self.read_line(deadline);
                let length = std::str::from_utf8(header.strip_prefix(b"$").unwrap())
                    .unwrap()
                    .parse::<usize>()
                    .unwrap();
                assert!(length <= 8192);
                let mut value = vec![0; length];
                self.read_exact(&mut value, deadline);
                let mut crlf = [0; 2];
                self.read_exact(&mut crlf, deadline);
                assert_eq!(&crlf, b"\r\n");
                value
            })
            .collect()
    }

    fn set_filter(&mut self, filter: Filter) {
        match filter {
            Filter::All => {}
            Filter::Functions => assert_eq!(
                self.command(&[b"REPLCONF", b"rdb-filter-only", b"functions"]),
                b"+OK"
            ),
            Filter::Empty => assert_eq!(
                self.command(&[b"REPLCONF", b"rdb-filter-only", b""]),
                b"+OK"
            ),
        }
    }

    fn client_id(&mut self) -> u64 {
        let response = self.command(&[b"CLIENT", b"ID"]);
        std::str::from_utf8(response.strip_prefix(b":").unwrap())
            .unwrap()
            .parse()
            .unwrap()
    }
}

fn start_server(diskless: &str, delay: &str) -> TestServer {
    TestServer::start(&[
        "--repl-diskless-sync",
        diskless,
        "--repl-diskless-sync-delay",
        delay,
        "--repl-diskless-sync-max-replicas",
        "0",
        "--dual-channel-replication-enabled",
        "yes",
        "--rdbcompression",
        "no",
    ])
}

fn info_field(server: &TestServer, section: &[u8], field: &str) -> String {
    let reply = server.connect().command(&[b"INFO", section]);
    String::from_utf8(reply)
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix(&format!("{field}:")).map(str::to_string))
        .unwrap_or_else(|| panic!("missing INFO field {field}"))
}

fn seed(server: &TestServer) -> String {
    let mut admin = server.connect();
    assert_eq!(
        admin.command(&[b"FUNCTION", b"LOAD", FUNCTION_CODE]),
        b"$9\r\nfilterlib\r\n"
    );
    assert_eq!(
        admin.command(&[b"SET", b"filter:key:0", KEY_VALUE]),
        b"+OK\r\n"
    );
    assert_eq!(admin.command(&[b"SELECT", b"1"]), b"+OK\r\n");
    assert_eq!(
        admin.command(&[b"SET", b"filter:key:1", b"database-one"]),
        b"+OK\r\n"
    );
    info_field(server, b"server", "valkey_version")
}

fn negotiate_single(client: &mut RawClient, version: &str) {
    assert_eq!(
        client.command(&[
            b"REPLCONF",
            b"version",
            version.as_bytes(),
            b"capa",
            b"eof",
            b"rdb-only",
            b"1",
        ]),
        b"+OK"
    );
}

fn assert_snapshot(bytes: &[u8], filter: Filter) {
    let mut dbs = DbSet::new(16);
    let (loaded, consumed, expired, functions) =
        load_rdb_bytes_checked_with_functions(bytes, &mut dbs, true)
            .expect("snapshot must pass the owned RDB decoder and checksum");
    assert_eq!(consumed, bytes.len());
    assert_eq!(expired, 0);
    assert_eq!(
        functions,
        if filter == Filter::Empty {
            Vec::new()
        } else {
            vec![FUNCTION_CODE.to_vec()]
        }
    );
    assert_eq!(loaded, if filter == Filter::All { 2 } else { 0 });
    for (db, key, value) in [
        (0, b"filter:key:0".as_slice(), KEY_VALUE),
        (1, b"filter:key:1".as_slice(), b"database-one".as_slice()),
    ] {
        assert_eq!(dbs.db_size(db), usize::from(filter == Filter::All));
        assert_eq!(
            dbs.get(db)
                .unwrap()
                .get_no_expire(key)
                .map(|object| object.as_bytes()),
            (filter == Filter::All).then_some(value)
        );
    }
    assert!(dbs.get(0).unwrap().get_no_expire(b"filter:after").is_none());
}

fn export_single(server: &TestServer, client: &mut RawClient, filter: Filter, diskless: &str) {
    client.send(&[b"PSYNC", b"?", b"-1"]);
    let snapshot = client.read_full_resync(diskless == "yes" || filter != Filter::All);
    assert_snapshot(&snapshot, filter);
    assert_eq!(
        info_field(server, b"replication", "replicas_waiting_psync"),
        "0"
    );
}

#[test]
fn all_rdb_filter_control_preserves_length_framing_without_diskless() {
    for diskless in ["yes", "no"] {
        let server = start_server(diskless, "0");
        let version = seed(&server);
        let mut client = RawClient::connect(&server);
        negotiate_single(&mut client, &version);
        export_single(&server, &mut client, Filter::All, diskless);
    }
}

#[test]
fn empty_rdb_filter_only_requests_an_empty_rdb() {
    for diskless in ["yes", "no"] {
        let server = start_server(diskless, "0");
        let version = seed(&server);
        let mut client = RawClient::connect(&server);
        negotiate_single(&mut client, &version);
        client.set_filter(Filter::Empty);
        export_single(&server, &mut client, Filter::Empty, diskless);
    }
}

#[test]
fn rdb_filter_only_accepts_quoted_functions() {
    for diskless in ["yes", "no"] {
        let server = start_server(diskless, "0");
        let version = seed(&server);
        let mut client = RawClient::connect(&server);
        negotiate_single(&mut client, &version);
        assert_eq!(
            client.command(&[b"REPLCONF", b"rdb-filter-only", b"\"functions\""]),
            b"+OK"
        );
        export_single(&server, &mut client, Filter::Functions, diskless);
    }
}

#[test]
fn rdb_filter_only_rejects_unterminated_quote_without_mutating_state() {
    for diskless in ["yes", "no"] {
        let server = start_server(diskless, "0");
        let version = seed(&server);
        let mut client = RawClient::connect(&server);
        negotiate_single(&mut client, &version);
        client.set_filter(Filter::Functions);
        assert_eq!(
            client.command(&[b"REPLCONF", b"rdb-filter-only", b"\"functions"]),
            b"-ERR Missing rdb-filter-only values"
        );
        export_single(&server, &mut client, Filter::Functions, diskless);
    }
}

#[test]
fn lexical_filter_failure_preserves_default_all_mode() {
    let server = start_server("no", "0");
    let version = seed(&server);
    let mut client = RawClient::connect(&server);
    negotiate_single(&mut client, &version);
    assert_eq!(
        client.command(&[b"REPLCONF", b"rdb-filter-only", b"\"functions"]),
        b"-ERR Missing rdb-filter-only values"
    );
    export_single(&server, &mut client, Filter::All, "no");
}

#[test]
fn rdb_filter_only_rejects_unsupported_values_after_initializing_empty() {
    for diskless in ["yes", "no"] {
        for (initial, token, expected_filter, error_word) in [
            (Filter::All, b"keys".as_slice(), Filter::Empty, "keys"),
            (
                Filter::Functions,
                b"keys functions".as_slice(),
                Filter::Empty,
                "keys",
            ),
            (
                Filter::All,
                b"functions keys".as_slice(),
                Filter::Functions,
                "keys",
            ),
            (Filter::Functions, b"\"\"".as_slice(), Filter::Empty, ""),
            (Filter::All, b"\"\"".as_slice(), Filter::Empty, ""),
        ] {
            let server = start_server(diskless, "0");
            let version = seed(&server);
            let mut client = RawClient::connect(&server);
            negotiate_single(&mut client, &version);
            client.set_filter(initial);
            assert_eq!(
                client.command(&[b"REPLCONF", b"rdb-filter-only", token]),
                format!("-ERR Unsupported rdb-filter-only option: {error_word}").as_bytes()
            );
            export_single(&server, &mut client, expected_filter, diskless);
        }
    }
}

#[test]
fn filtered_sync_without_eof_is_rejected_and_connection_remains_ordinary() {
    for diskless in ["yes", "no"] {
        for (filter, lexical_failure) in [
            (Filter::Functions, false),
            (Filter::Empty, false),
            (Filter::Functions, true),
        ] {
            let server = start_server(diskless, "0");
            let mut client = RawClient::connect(&server);
            client.set_filter(filter);
            if lexical_failure {
                assert_eq!(
                    client.command(&[b"REPLCONF", b"rdb-filter-only", b"\"functions"]),
                    b"-ERR Missing rdb-filter-only values"
                );
            }
            assert_eq!(
                client.command(&[b"PSYNC", b"?", b"-1"]),
                b"-ERR Filtered replica requires EOF capability"
            );
            assert_eq!(client.command(&[b"PING"]), b"+PONG");
        }
    }
}

#[test]
fn candidate_only_rdb_client_id_is_rejected_without_implicit_endoff() {
    for diskless in ["yes", "no"] {
        let server = start_server(diskless, "0");
        let mut main = RawClient::connect(&server);
        assert_eq!(
            main.command(&[b"REPLCONF", b"listening-port", b"6380"]),
            b"+OK"
        );
        let mut rdb = RawClient::connect(&server);
        rdb.set_filter(Filter::Functions);
        let rdb_id = rdb.client_id();
        assert_eq!(rdb.command(&[b"REPLCONF", b"rdb-channel", b"1"]), b"+OK");
        assert_eq!(
            info_field(&server, b"replication", "replicas_waiting_psync"),
            "0"
        );
        assert_eq!(
            main.command(&[
                b"REPLCONF",
                b"set-rdb-client-id",
                rdb_id.to_string().as_bytes()
            ]),
            format!("-ERR Unrecognized RDB client id {rdb_id}").as_bytes()
        );
        assert_eq!(main.command(&[b"PING"]), b"+PONG");
        rdb.stream
            .set_read_timeout(Some(Duration::from_millis(100)))
            .unwrap();
        let error = rdb.stream.peek(&mut [0u8; 1]).unwrap_err();
        assert!(matches!(
            error.kind(),
            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
        ));
    }
}

fn negotiate_dual(
    server: &TestServer,
    filter: Filter,
    version: &str,
) -> (RawClient, RawClient, u64) {
    let mut main = RawClient::connect(server);
    assert_eq!(
        main.command(&[
            b"REPLCONF",
            b"listening-port",
            b"6380",
            b"capa",
            b"eof",
            b"capa",
            b"psync2",
            b"capa",
            b"dual-channel",
            b"version",
            version.as_bytes(),
        ]),
        b"+OK"
    );
    assert_eq!(main.command(&[b"PSYNC", b"?", b"-1"]), b"+DUALCHANNELSYNC");
    let mut rdb = RawClient::connect(server);
    let public_id = rdb.client_id();
    rdb.set_filter(filter);
    assert_eq!(
        rdb.command(&[
            b"REPLCONF",
            b"capa",
            b"eof",
            b"rdb-only",
            b"1",
            b"rdb-channel",
            b"1",
            b"listening-port",
            b"6380",
            b"version",
            version.as_bytes(),
        ]),
        b"+OK"
    );
    (main, rdb, public_id)
}

fn finish_dual(main: &mut RawClient, rdb: &mut RawClient, public_id: u64, filter: Filter) -> i64 {
    let frame = rdb.nonempty_line(Instant::now() + TIMEOUT);
    let fields: Vec<_> = frame
        .strip_prefix(b"$ENDOFF:")
        .unwrap()
        .split(|b| *b == b' ')
        .collect();
    assert_eq!(fields.len(), 4);
    let offset = std::str::from_utf8(fields[0])
        .unwrap()
        .parse::<i64>()
        .unwrap();
    assert_eq!(fields[1].len(), 40);
    assert!(fields[1].iter().all(u8::is_ascii_hexdigit));
    let db = std::str::from_utf8(fields[2])
        .unwrap()
        .parse::<i32>()
        .unwrap();
    assert!((-1..=1).contains(&db));
    assert_eq!(fields[3], public_id.to_string().as_bytes());
    assert_eq!(
        main.command(&[
            b"REPLCONF",
            b"set-rdb-client-id",
            public_id.to_string().as_bytes()
        ]),
        b"+OK"
    );
    main.send(&[b"PSYNC", fields[1], (offset + 1).to_string().as_bytes()]);
    let continued = main.nonempty_line(Instant::now() + TIMEOUT);
    assert!(
        continued == b"+CONTINUE" || continued == [b"+CONTINUE ".as_slice(), fields[1]].concat()
    );
    assert_snapshot(&rdb.read_rdb(true), filter);
    main.send(&[b"REPLCONF", b"ACK", offset.to_string().as_bytes()]);
    offset
}

fn assert_main_set(server: &TestServer, main: &mut RawClient, offset: i64) {
    assert_eq!(
        server
            .connect()
            .command(&[b"SET", b"filter:after", b"incremental"]),
        b"+OK\r\n"
    );
    let deadline = Instant::now() + TIMEOUT;
    for _ in 0..64 {
        let command = main.read_repl_command(deadline);
        match command[0].as_slice() {
            b"SET" => {
                assert_eq!(
                    command,
                    vec![
                        b"SET".to_vec(),
                        b"filter:after".to_vec(),
                        b"incremental".to_vec()
                    ]
                );
                return;
            }
            b"SELECT" => assert_eq!(command, vec![b"SELECT".to_vec(), b"0".to_vec()]),
            b"PING" => assert_eq!(command.len(), 1),
            b"REPLCONF" => {
                assert_eq!(
                    command,
                    vec![b"REPLCONF".to_vec(), b"GETACK".to_vec(), b"*".to_vec()]
                );
                main.send(&[b"REPLCONF", b"ACK", offset.to_string().as_bytes()]);
            }
            _ => panic!("unexpected main stream command {command:?}"),
        }
    }
    panic!("main stream did not receive SET within its frame bound");
}

#[test]
fn real_dual_channel_sync_filters_rdb_and_preserves_main_command_stream() {
    for filter in [Filter::All, Filter::Functions, Filter::Empty] {
        let server = start_server("yes", "0");
        assert_eq!(
            server
                .connect()
                .command(&[b"DEBUG", b"delay-rdb-client-free-seconds", b"60"]),
            b"+OK\r\n"
        );
        let version = seed(&server);
        let (mut main, mut rdb, public_id) = negotiate_dual(&server, filter, &version);
        assert_eq!(
            info_field(&server, b"replication", "replicas_waiting_psync"),
            "0"
        );
        rdb.send(&[b"SYNC"]);
        // ENDOFF is the only event that publishes a real PSYNC wait.
        let deadline = Instant::now() + TIMEOUT;
        while info_field(&server, b"replication", "replicas_waiting_psync") != "1" {
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(5));
        }
        let offset = finish_dual(&mut main, &mut rdb, public_id, filter);
        assert_eq!(
            info_field(&server, b"replication", "replicas_waiting_psync"),
            "0"
        );
        assert_main_set(&server, &mut main, offset);
    }
}

#[test]
fn rdb_candidate_sync_requires_eof_without_starting_a_wait() {
    let server = start_server("yes", "0");
    let mut rdb = RawClient::connect(&server);
    assert_eq!(rdb.command(&[b"REPLCONF", b"rdb-channel", b"1"]), b"+OK");
    assert_eq!(
        rdb.command(&[b"SYNC"]),
        b"-ERR Filtered replica requires EOF capability"
    );
    assert_eq!(
        info_field(&server, b"replication", "replicas_waiting_psync"),
        "0"
    );
}

#[test]
fn delayed_filtered_fullsync_preserves_each_connections_mode() {
    for diskless in ["yes", "no"] {
        let server = start_server(diskless, "1");
        let version = seed(&server);
        let mut clients = Vec::new();
        for filter in [Filter::Functions, Filter::Empty, Filter::All] {
            let mut client = RawClient::connect(&server);
            negotiate_single(&mut client, &version);
            client.set_filter(filter);
            client.send(&[b"PSYNC", b"?", b"-1"]);
            clients.push((client, filter));
        }
        for (mut client, filter) in clients {
            assert_snapshot(
                &client.read_full_resync(diskless == "yes" || filter != Filter::All),
                filter,
            );
        }
    }
}

#[test]
fn pending_dual_cohorts_do_not_mix_filter_modes() {
    let server = start_server("yes", "1");
    assert_eq!(
        server
            .connect()
            .command(&[b"DEBUG", b"delay-rdb-client-free-seconds", b"60"]),
        b"+OK\r\n"
    );
    let version = seed(&server);
    let mut clients = Vec::new();
    for filter in [Filter::All, Filter::Functions, Filter::Empty] {
        let (main, mut rdb, public_id) = negotiate_dual(&server, filter, &version);
        rdb.send(&[b"SYNC"]);
        clients.push((main, rdb, public_id, filter));
    }
    for (mut main, mut rdb, public_id, filter) in clients {
        finish_dual(&mut main, &mut rdb, public_id, filter);
    }
    assert_eq!(
        info_field(&server, b"replication", "replicas_waiting_psync"),
        "0"
    );
}
