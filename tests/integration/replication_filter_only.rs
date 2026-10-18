use crate::common::TestServer;
use std::io::{Read, Write};
use std::net::TcpStream;

struct RawClient {
    stream: TcpStream,
}

impl RawClient {
    fn connect(server: &TestServer) -> Self {
        let stream = TcpStream::connect(("127.0.0.1", server.port)).unwrap();
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .unwrap();
        Self { stream }
    }

    fn command(&mut self, args: &[&[u8]]) -> Vec<u8> {
        let mut request = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            request.extend_from_slice(format!("${}\r\n", arg.len()).as_bytes());
            request.extend_from_slice(arg);
            request.extend_from_slice(b"\r\n");
        }
        self.stream.write_all(&request).unwrap();
        self.read_line()
    }

    fn read_line(&mut self) -> Vec<u8> {
        let mut line = Vec::new();
        loop {
            let mut byte = [0u8; 1];
            self.stream.read_exact(&mut byte).unwrap();
            if byte[0] == b'\r' {
                let mut lf = [0u8; 1];
                self.stream.read_exact(&mut lf).unwrap();
                assert_eq!(lf[0], b'\n');
                return line;
            }
            line.push(byte[0]);
        }
    }

    fn read_full_resync_rdb(&mut self) -> Vec<u8> {
        assert!(self.read_line().starts_with(b"+FULLRESYNC "));
        let bulk_header = self.read_line();
        let length = std::str::from_utf8(
            bulk_header
                .strip_prefix(b"$")
                .expect("PSYNC must return an RDB bulk header"),
        )
        .unwrap()
        .parse::<usize>()
        .unwrap();
        let mut body = vec![0u8; length];
        self.stream.read_exact(&mut body).unwrap();
        body
    }
}

#[test]
fn post_association_rdb_filter_updates_psync_request() {
    let server = TestServer::start_default();
    let mut main = RawClient::connect(&server);
    assert_eq!(
        main.command(&[b"REPLCONF", b"listening-port", b"6380"]),
        b"+OK"
    );

    let mut rdb = RawClient::connect(&server);
    assert_eq!(
        rdb.command(&[b"REPLCONF", b"rdb-filter-only", b"functions"]),
        b"+OK"
    );
    let rdb_id = std::str::from_utf8(rdb.command(&[b"CLIENT", b"ID"]).strip_prefix(b":").unwrap())
        .unwrap()
        .parse::<u64>()
        .unwrap();
    assert_eq!(rdb.command(&[b"REPLCONF", b"rdb-channel", b"1"]), b"+OK");
    assert_eq!(
        main.command(&[
            b"REPLCONF",
            b"set-rdb-client-id",
            rdb_id.to_string().as_bytes(),
        ]),
        b"+OK"
    );

    assert_eq!(
        server
            .connect()
            .command(&[b"SET", b"post-association-key", b"value"]),
        b"+OK\r\n"
    );
    main.stream
        .write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
        .unwrap();
    let rdb_bytes = main.read_full_resync_rdb();

    assert!(
        !rdb_bytes
            .windows(b"post-association-key".len())
            .any(|window| window == b"post-association-key"),
        "the associated RDB filter must be visible to PSYNC"
    );
}

#[test]
fn rdb_filter_only_rejects_unsupported_values() {
    let server = TestServer::start_default();
    let mut client = RawClient::connect(&server);

    assert_eq!(
        client.command(&[b"REPLCONF", b"rdb-filter-only", b"keys"]),
        b"-ERR Unsupported rdb-filter-only option: keys"
    );
}

#[test]
fn rdb_filter_only_accepts_quoted_functions() {
    let server = TestServer::start_default();
    let mut client = RawClient::connect(&server);

    assert_eq!(
        client.command(&[b"REPLCONF", b"rdb-filter-only", b"\"functions\""]),
        b"+OK"
    );
}

#[test]
fn rdb_filter_only_rejects_unterminated_quote_without_mutating_state() {
    let server = TestServer::start_default();
    let mut client = RawClient::connect(&server);

    assert_eq!(
        client.command(&[b"REPLCONF", b"rdb-filter-only", b"functions"]),
        b"+OK"
    );
    assert_eq!(
        client.command(&[b"REPLCONF", b"rdb-filter-only", b"\"functions"]),
        b"-ERR Missing rdb-filter-only values"
    );
    assert_eq!(
        client.command(&[b"SET", b"unterminated-filter-key", b"value"]),
        b"+OK"
    );

    client
        .stream
        .write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
        .unwrap();
    let rdb_bytes = client.read_full_resync_rdb();

    assert!(
        !rdb_bytes
            .windows(b"unterminated-filter-key".len())
            .any(|window| window == b"unterminated-filter-key"),
        "an invalid filter must not replace the previously selected filter"
    );
}

#[test]
fn empty_rdb_filter_only_requests_an_empty_rdb() {
    let server = TestServer::start_default();
    let mut client = RawClient::connect(&server);
    assert_eq!(
        client.command(&[b"SET", b"empty-filter-key", b"value"]),
        b"+OK"
    );
    assert_eq!(
        client.command(&[b"REPLCONF", b"rdb-filter-only", b""]),
        b"+OK"
    );

    client
        .stream
        .write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
        .unwrap();
    let rdb_bytes = client.read_full_resync_rdb();

    assert!(
        !rdb_bytes
            .windows(b"empty-filter-key".len())
            .any(|window| window == b"empty-filter-key"),
        "an empty filter must not fall back to an include-all RDB"
    );
}

#[test]
fn rdb_filter_only_rejects_explicit_empty_token_without_mutating_state() {
    let server = TestServer::start_default();
    let mut client = RawClient::connect(&server);

    assert_eq!(
        client.command(&[b"REPLCONF", b"rdb-filter-only", b"\"\""]),
        b"-ERR Unsupported rdb-filter-only option: "
    );
    assert_eq!(
        client.command(&[b"SET", b"explicit-empty-filter-key", b"value"]),
        b"+OK"
    );

    client
        .stream
        .write_all(b"*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n")
        .unwrap();
    let rdb_bytes = client.read_full_resync_rdb();

    assert!(
        rdb_bytes
            .windows(b"explicit-empty-filter-key".len())
            .any(|window| window == b"explicit-empty-filter-key"),
        "an invalid explicit empty filter must not replace the default all-data filter"
    );
}
