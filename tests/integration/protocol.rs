use crate::common::TestServer;

#[test]
fn inline_ping_returns_pong() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.send_raw(b"PING\r\n");
    assert_eq!(c.read_reply(), b"+PONG\r\n");
}

#[test]
fn multibulk_ping_returns_pong() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    assert_eq!(c.command(&[b"PING"]), b"+PONG\r\n");
}

#[test]
fn ping_with_argument_echoes_as_bulk() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    assert_eq!(c.command(&[b"PING", b"hello"]), b"$5\r\nhello\r\n");
}

#[test]
fn echo_returns_bulk() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    assert_eq!(c.command(&[b"ECHO", b"world"]), b"$5\r\nworld\r\n");
}

#[test]
fn split_packets_are_reassembled() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.send_raw(b"*2\r\n$4\r\nEC");
    std::thread::sleep(std::time::Duration::from_millis(30));
    c.send_raw(b"HO\r\n$5\r\nhe");
    std::thread::sleep(std::time::Duration::from_millis(30));
    c.send_raw(b"llo\r\n");
    assert_eq!(c.read_reply(), b"$5\r\nhello\r\n");
}

#[test]
fn unknown_command_returns_error() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    let reply = c.command_str(&[b"NOSUCHCMD"]);
    assert!(reply.starts_with("-ERR unknown command"), "got: {}", reply);
}

#[test]
fn wrong_arity_returns_error() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    let reply = c.command_str(&[b"GET"]);
    assert!(
        reply.starts_with("-ERR wrong number of arguments"),
        "got: {}",
        reply
    );
}
