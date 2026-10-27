use crate::common::TestServer;

fn integer_reply(reply: &[u8]) -> u64 {
    std::str::from_utf8(reply.strip_prefix(b":").expect("integer reply"))
        .expect("UTF-8 integer")
        .trim()
        .parse()
        .expect("numeric integer reply")
}

#[test]
fn valkey_update_deferred_eviction_is_counted_once_and_preserves_small_clients() {
    use std::time::{Duration, Instant};

    let server = TestServer::start_default();
    let mut admin = server.connect();
    assert_eq!(
        admin.command(&[b"CLIENT", b"SETNAME", b"control"]),
        b"+OK\r\n"
    );
    assert_eq!(admin.command(&[b"CLIENT", b"NO-EVICT", b"on"]), b"+OK\r\n");
    let mut clients = Vec::new();
    for (name, size) in [
        ("big", 2 * 1024 * 1024),
        ("small0", 100 * 1024),
        ("small1", 100 * 1024),
    ] {
        let mut client = server.connect();
        assert_eq!(
            client.command(&[b"CLIENT", b"SETNAME", name.as_bytes()]),
            b"+OK\r\n"
        );
        let mut partial = format!("*2\r\n${size}\r\n").into_bytes();
        partial.resize(partial.len() + size, b'v');
        client.send_raw(&partial);
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let list = admin.command_str(&[b"CLIENT", b"LIST"]);
            let memory = client_field(&list, name, "tot-mem")
                .parse::<usize>()
                .expect("client memory");
            if memory >= size {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "query buffer did not fill: {list}"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        clients.push(client);
    }
    let list = admin.command_str(&[b"CLIENT", b"LIST"]);
    let big_id = client_field(&list, "big", "id").as_bytes();
    assert_eq!(
        admin.command(&[b"DEBUG", b"PROTECT-CLIENT", big_id]),
        b"+OK\r\n"
    );
    let limit = ["control", "small0", "small1"]
        .iter()
        .map(|name| {
            client_field(&list, name, "tot-mem")
                .parse::<usize>()
                .expect("client memory")
        })
        .sum::<usize>()
        + 1024;
    assert_eq!(
        admin.command(&[
            b"CONFIG",
            b"SET",
            b"maxmemory-clients",
            limit.to_string().as_bytes()
        ]),
        b"+OK\r\n"
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let stats = admin.command_str(&[b"INFO", b"stats"]);
        if stats.contains("evicted_clients:1\r\n") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "client was not evicted once: {stats}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let deadline = Instant::now() + Duration::from_millis(300);
    while Instant::now() < deadline {
        let stats = admin.command_str(&[b"INFO", b"stats"]);
        assert!(
            stats.contains("evicted_clients:1\r\n"),
            "deferred eviction was counted again: {stats}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let list = admin.command_str(&[b"CLIENT", b"LIST"]);
    assert!(client_field(&list, "big", "flags").contains('A'), "{list}");
    for name in ["small0", "small1"] {
        assert!(!client_field(&list, name, "flags").contains('A'), "{list}");
    }
}

fn client_field<'a>(list: &'a str, name: &str, field: &str) -> &'a str {
    let line = list
        .lines()
        .find(|line| {
            line.split_whitespace()
                .any(|part| part == format!("name={name}"))
        })
        .unwrap_or_else(|| panic!("client {name} missing: {list}"));
    line.split_whitespace()
        .find_map(|part| part.strip_prefix(&format!("{field}=")))
        .unwrap_or_else(|| panic!("field {field} missing: {line}"))
}

#[test]
fn valkey_update_quit_with_pending_output_remains_eligible_for_eviction() {
    use std::time::{Duration, Instant};

    let server = TestServer::start_with_env(
        &[],
        &[
            ("VALKEY_TEST_TCP_SNDBUF_BYTES", "65536"),
            ("VALKEY_TEST_TCP_RCVBUF_BYTES", "87380"),
        ],
    );
    let mut admin = server.connect();
    assert_eq!(admin.command(&[b"CLIENT", b"NO-EVICT", b"on"]), b"+OK\r\n");
    let value = vec![b'x'; 2 * 1024 * 1024];
    assert_eq!(admin.command(&[b"SET", b"large", &value]), b"+OK\r\n");
    let mut stalled = server.connect();
    assert_eq!(
        stalled.command(&[b"CLIENT", b"SETNAME", b"closing"]),
        b"+OK\r\n"
    );
    for _ in 0..8 {
        stalled.send(&[b"GET", b"large"]);
    }
    stalled.send(&[b"QUIT"]);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let list = admin.command_str(&[b"CLIENT", b"LIST"]);
        if client_field(&list, "closing", "cmd") == "quit"
            && client_field(&list, "closing", "tot-mem")
                .parse::<usize>()
                .expect("client memory")
                > 128 * 1024
        {
            assert!(
                client_field(&list, "closing", "flags").contains('c'),
                "{list}"
            );
            assert!(
                !client_field(&list, "closing", "flags").contains('A'),
                "{list}"
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "QUIT did not queue behind stalled output: {list}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(
        admin.command(&[b"CONFIG", b"SET", b"maxmemory-clients", b"128kb"]),
        b"+OK\r\n"
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let stats = admin.command_str(&[b"INFO", b"stats"]);
        if stats.contains("evicted_clients:1\r\n") {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "stalled QUIT client escaped eviction: {stats}"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let list = admin.command_str(&[b"CLIENT", b"LIST"]);
    assert!(!list.contains("name=closing "), "{list}");
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
