use crate::common::TestServer;

fn integer_reply(reply: &[u8]) -> u64 {
    std::str::from_utf8(reply.strip_prefix(b":").expect("integer reply"))
        .expect("UTF-8 integer")
        .trim()
        .parse()
        .expect("numeric integer reply")
}

#[test]
fn client_id_is_stable_and_unique_per_connection() {
    let srv = TestServer::start_default();
    let mut first = srv.connect();
    let mut second = srv.connect();
    let first_id = integer_reply(&first.command(&[b"CLIENT", b"ID"]));
    let second_id = integer_reply(&second.command(&[b"CLIENT", b"ID"]));
    assert_ne!(first_id, second_id);
    assert_eq!(integer_reply(&first.command(&[b"CLIENT", b"ID"])), first_id);
    let info = first.command_str(&[b"CLIENT", b"INFO"]);
    assert!(info.contains(&format!("id={first_id} ")), "{info}");
}

#[test]
fn select_valid_db_ok() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    assert_eq!(c.command(&[b"SELECT", b"1"]), b"+OK\r\n");
}

#[test]
fn select_out_of_range_errors() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    let reply = c.command_str(&[b"SELECT", b"9999"]);
    assert!(
        reply.starts_with("-ERR DB index is out of range"),
        "got: {}",
        reply
    );
}

#[test]
fn hello_returns_map_with_server_valkey() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    let reply = c.command_str(&[b"HELLO"]);
    assert!(reply.contains("valkey"), "got: {}", reply);
    assert!(reply.contains("standalone"), "got: {}", reply);
}

#[test]
fn hello_three_returns_a_resp3_map() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    let reply = c.command(&[b"HELLO", b"3"]);
    assert!(
        reply.starts_with(b"%"),
        "expected RESP3 map, got: {:?}",
        reply
    );
    assert!(reply
        .windows(b"proto".len())
        .any(|window| window == b"proto"));
}

#[test]
fn reset_returns_reset() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    assert_eq!(c.command(&[b"RESET"]), b"+RESET\r\n");
}

#[test]
fn quit_closes_connection() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    assert_eq!(c.command(&[b"QUIT"]), b"+OK\r\n");
    assert!(c.is_closed(), "connection should be closed after QUIT");
}

#[test]
fn auth_without_password_configured_errors() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    let reply = c.command_str(&[b"AUTH", b"whatever"]);
    assert!(
        reply.contains("without any password configured"),
        "got: {}",
        reply
    );
}

#[test]
fn requirepass_flow_noauth_then_auth() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    assert_eq!(
        c.command(&[b"CONFIG", b"SET", b"requirepass", b"secret"]),
        b"+OK\r\n"
    );

    let mut c2 = srv.connect();
    let reply = c2.command_str(&[b"PING"]);
    assert!(reply.starts_with("-NOAUTH"), "got: {}", reply);
    let reply = c2.command_str(&[b"GET", b"k"]);
    assert!(reply.starts_with("-NOAUTH"), "got: {}", reply);

    let reply = c2.command_str(&[b"AUTH", b"wrong"]);
    assert!(reply.starts_with("-WRONGPASS"), "got: {}", reply);

    assert_eq!(c2.command(&[b"AUTH", b"secret"]), b"+OK\r\n");
    assert_eq!(c2.command(&[b"GET", b"nope"]), b"$-1\r\n");
}
