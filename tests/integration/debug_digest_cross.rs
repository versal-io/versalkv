use crate::common::{Client, TestServer};
use std::collections::HashMap;
use std::io::Write;
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

macro_rules! cmd {
    [$($part:expr),* $(,)?] => {{
        vec![$(::std::convert::AsRef::<[u8]>::as_ref($part).to_vec()),*]
    }};
}

struct CServer {
    child: Child,
    port: u16,
    dir: PathBuf,
}

impl CServer {
    fn connect(&self) -> Client {
        Client::connect_to(self.port)
    }
}

impl Drop for CServer {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn spawn_c_server(bin: &str) -> CServer {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind :0");
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let id = DIR_SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("valkey-c-digest-{}-{}", std::process::id(), id));
    std::fs::create_dir(&dir).expect("create c server temp dir");
    let port_str = port.to_string();
    let child = Command::new(bin)
        .args([
            "--port",
            &port_str,
            "--bind",
            "127.0.0.1",
            "--save",
            "",
            "--appendonly",
            "no",
            "--enable-debug-command",
            "yes",
            "--loglevel",
            "warning",
            "--unixsocket",
            "",
        ])
        .current_dir(&dir)
        .stdout(Stdio::null())
        .stderr(Stdio::from(
            std::fs::File::create(dir.join("stderr.log")).expect("create stderr file"),
        ))
        .spawn()
        .expect("spawn c valkey-server");

    let mut srv = CServer { child, port, dir };
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = srv.child.try_wait().expect("c server wait") {
            panic!("c server exited early: {status}");
        }
        if let Ok(mut stream) = std::net::TcpStream::connect(("127.0.0.1", port)) {
            let _ = stream.set_read_timeout(Some(Duration::from_millis(500)));
            let _ = stream.write_all(b"*1\r\n$4\r\nPING\r\n");
            let mut buf = [0u8; 16];
            if let Ok(n) = std::io::Read::read(&mut stream, &mut buf) {
                if n >= 5 && buf.starts_with(b"+PONG") {
                    break;
                }
            }
        }
        if std::time::Instant::now() > deadline {
            panic!("c server did not become ready");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    srv
}

static DIR_SEQ: AtomicU64 = AtomicU64::new(0);

fn load_dataset(c: &mut Client) {
    fn run(c: &mut Client, cmd: Vec<Vec<u8>>) {
        let refs: Vec<&[u8]> = cmd.iter().map(|x| x.as_slice()).collect();
        let reply = c.command(&refs);
        assert!(
            !reply.starts_with(b"-"),
            "setup command failed: {}",
            String::from_utf8_lossy(&reply)
        );
    }

    run(c, cmd!["SET", "str:plain", "hello"]);
    run(c, cmd!["SET", "str:big", &"x".repeat(60)]);
    run(c, cmd!["SET", "str:int", "42"]);
    run(c, cmd!["SET", "str:neg", "-7"]);
    run(c, cmd!["SET", "str:decs", "3.14"]);

    run(
        c,
        cmd!["RPUSH", "lst:lp", "a", "b", "c", "10", "01", "hello"],
    );
    let mut ql = vec![b"RPUSH".to_vec(), b"lst:ql".to_vec()];
    for i in 0..200 {
        ql.push(format!("item{i}").into_bytes());
    }
    run(c, ql);

    run(c, cmd!["SADD", "set:int", "1", "2", "3", "100", "-5"]);
    run(c, cmd!["SADD", "set:lp", "foo", "bar", "baz"]);
    run(c, cmd!["SADD", "set:mixed", "1", "2", "a", "b"]);
    let mut ht = vec![b"SADD".to_vec(), b"set:ht".to_vec()];
    for i in 0..200 {
        ht.push(format!("member{i}").into_bytes());
    }
    run(c, ht);

    // Hash listpack and hashtable encodings.
    run(c, cmd!["HSET", "h:lp", "f1", "v1", "f2", "42", "f3", "-3"]);
    let mut hht = vec![b"HSET".to_vec(), b"h:ht".to_vec()];
    for i in 0..200 {
        hht.push(format!("field{i}").into_bytes());
        hht.push(format!("value{i}").into_bytes());
    }
    run(c, hht);

    run(
        c,
        cmd![
            "ZADD",
            "z:lp",
            "1",
            "a",
            "1.5",
            "b",
            "0.1",
            "c",
            "0.0000001",
            "d",
            "100",
            "e"
        ],
    );
    let mut zs = vec![b"ZADD".to_vec(), b"z:skiplist".to_vec()];
    for i in 0..200 {
        zs.push(format!("{i}.5").into_bytes());
        zs.push(format!("z{i}").into_bytes());
    }
    run(c, zs);

    run(c, cmd!["XADD", "st", "1-0", "f", "v"]);
    run(c, cmd!["XADD", "st", "1-1", "num", "42", "name", "hello"]);

    run(c, cmd!["SET", "ttl:1", "x"]);
    run(c, cmd!["EXPIRE", "ttl:1", "10000"]);
    run(c, cmd!["SET", "ttl:2", "y"]);
    run(c, cmd!["PEXPIRE", "ttl:2", "500000000"]);

    let mut sa = vec![b"SADD".to_vec(), b"ord:set:a".to_vec()];
    let mut sb = vec![b"SADD".to_vec(), b"ord:set:b".to_vec()];
    for i in 0..25 {
        sa.push(format!("{i}").into_bytes());
        sb.push(format!("{}", 24 - i).into_bytes());
    }
    run(c, sa);
    run(c, sb);

    let mut ha = vec![b"HSET".to_vec(), b"ord:h:a".to_vec()];
    let mut hb = vec![b"HSET".to_vec(), b"ord:h:b".to_vec()];
    for i in 0..25 {
        ha.push(format!("f{i}").into_bytes());
        ha.push(format!("v{i}").into_bytes());
        hb.push(format!("f{}", 24 - i).into_bytes());
        hb.push(format!("v{}", 24 - i).into_bytes());
    }
    run(c, ha);
    run(c, hb);

    let mut za = vec![b"ZADD".to_vec(), b"ord:z:a".to_vec()];
    let mut zb = vec![b"ZADD".to_vec(), b"ord:z:b".to_vec()];
    for i in 0..25 {
        za.push(format!("{i}.25").into_bytes());
        za.push(format!("m{i}").into_bytes());
        zb.push(format!("{}.25", 24 - i).into_bytes());
        zb.push(format!("m{}", 24 - i).into_bytes());
    }
    run(c, za);
    run(c, zb);
}

fn all_keys() -> Vec<&'static str> {
    vec![
        "str:plain",
        "str:big",
        "str:int",
        "str:neg",
        "str:decs",
        "lst:lp",
        "lst:ql",
        "set:int",
        "set:lp",
        "set:mixed",
        "set:ht",
        "h:lp",
        "h:ht",
        "z:lp",
        "z:skiplist",
        "st",
        "ttl:1",
        "ttl:2",
        "ord:set:a",
        "ord:set:b",
        "ord:h:a",
        "ord:h:b",
        "ord:z:a",
        "ord:z:b",
    ]
}

#[test]
fn debug_digest_matches_c_server_byte_for_byte() {
    let c_bin = std::env::var("CROSS_DIGEST_C_SERVER").unwrap_or_else(|_| {
        format!(
            "{}/thirdparty/valkey/src/valkey-server",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if !std::path::Path::new(&c_bin).exists() {
        eprintln!("skip: C valkey-server not found at {c_bin} (build submodule or set CROSS_DIGEST_C_SERVER)");
        return;
    }

    let c_srv = spawn_c_server(&c_bin);
    let rust_srv = TestServer::start(&["--appendonly", "no"]);

    let mut cc = c_srv.connect();
    let mut rc = rust_srv.connect();
    load_dataset(&mut cc);
    load_dataset(&mut rc);

    let c_digest = cc.command_str(&[b"DEBUG", b"DIGEST"]);
    let rust_digest = rc.command_str(&[b"DEBUG", b"DIGEST"]);
    assert_eq!(c_digest, rust_digest, "whole-dataset DIGEST differs");

    let mut dv = vec![b"DEBUG".as_slice(), b"DIGEST-VALUE".as_slice()];
    let owned_keys: Vec<Vec<u8>> = all_keys()
        .into_iter()
        .map(|k| k.as_bytes().to_vec())
        .collect();
    dv.extend(owned_keys.iter().map(|k| k.as_slice()));
    let c_values = cc.command(&dv);
    let rust_values = rc.command(&dv);
    assert_eq!(c_values, rust_values, "DIGEST-VALUE differs");

    for (a, b) in [
        ("ord:set:a", "ord:set:b"),
        ("ord:h:a", "ord:h:b"),
        ("ord:z:a", "ord:z:b"),
    ] {
        let da = cc.command_str(&[b"DEBUG", b"DIGEST-VALUE", a.as_bytes()]);
        let db = cc.command_str(&[b"DEBUG", b"DIGEST-VALUE", b.as_bytes()]);
        assert_eq!(da, db, "C: {a} vs {b} order sensitivity");
        let da = rc.command_str(&[b"DEBUG", b"DIGEST-VALUE", a.as_bytes()]);
        let db = rc.command_str(&[b"DEBUG", b"DIGEST-VALUE", b.as_bytes()]);
        assert_eq!(da, db, "rust: {a} vs {b} order sensitivity");
    }
}

#[test]
fn debug_digest_includes_logically_expired_keys_like_c() {
    let c_bin = std::env::var("CROSS_DIGEST_C_SERVER").unwrap_or_else(|_| {
        format!(
            "{}/thirdparty/valkey/src/valkey-server",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if !std::path::Path::new(&c_bin).exists() {
        eprintln!("skip: C valkey-server not found at {c_bin}");
        return;
    }

    let c_srv = spawn_c_server(&c_bin);
    let rust_srv = TestServer::start(&["--appendonly", "no"]);
    let mut cc = c_srv.connect();
    let mut rc = rust_srv.connect();

    for c in [&mut cc, &mut rc] {
        assert_eq!(
            c.command_str(&[b"DEBUG", b"SET-ACTIVE-EXPIRE", b"0"]),
            "+OK\r\n"
        );
        assert_eq!(
            c.command_str(&[b"SET", b"exp:k", b"expired-val"]),
            "+OK\r\n"
        );
        assert_eq!(c.command_str(&[b"PEXPIRE", b"exp:k", b"100"]), ":1\r\n");
    }
    std::thread::sleep(Duration::from_millis(300));

    let c_dv = cc.command_str(&[b"DEBUG", b"DIGEST-VALUE", b"exp:k"]);
    let rust_dv = rc.command_str(&[b"DEBUG", b"DIGEST-VALUE", b"exp:k"]);
    assert_eq!(c_dv, rust_dv, "logically expired DIGEST-VALUE differs");
    assert_ne!(c_dv, "+0000000000000000000000000000000000000000\r\n");

    let c_d = cc.command_str(&[b"DEBUG", b"DIGEST"]);
    let rust_d = rc.command_str(&[b"DEBUG", b"DIGEST"]);
    assert_eq!(c_d, rust_d, "logically expired whole DIGEST differs");
}

fn parse_object_fields(line: &str) -> HashMap<String, String> {
    let line = line.strip_prefix('+').unwrap_or(line);
    let line = line.strip_suffix("\r\n").unwrap_or(line);
    line.split(' ')
        .filter_map(|tok| {
            tok.split_once(':')
                .map(|(k, v)| (k.to_string(), v.to_string()))
        })
        .collect()
}

fn assert_debug_object_fields_match(
    c_fields: &mut HashMap<String, String>,
    r_fields: &mut HashMap<String, String>,
    key: &str,
) {
    let c_idle = c_fields.remove("lru_seconds_idle");
    let r_idle = r_fields.remove("lru_seconds_idle");
    assert_eq!(c_fields, r_fields, "DEBUG OBJECT {key} fields differ");
    let (c_idle, r_idle) = match (c_idle, r_idle) {
        (Some(c_idle), Some(r_idle)) => (c_idle, r_idle),
        (c_idle, r_idle) => {
            assert_eq!(
                c_idle, r_idle,
                "DEBUG OBJECT {key} lru_seconds_idle field differs"
            );
            return;
        }
    };
    let c_idle = c_idle
        .parse::<u64>()
        .unwrap_or_else(|error| panic!("invalid C lru_seconds_idle for {key}: {error}"));
    let r_idle = r_idle
        .parse::<u64>()
        .unwrap_or_else(|error| panic!("invalid Rust lru_seconds_idle for {key}: {error}"));
    assert!(
        c_idle.abs_diff(r_idle) <= 1,
        "DEBUG OBJECT {key} lru_seconds_idle differs by more than one second: C={c_idle}, Rust={r_idle}"
    );
}

#[test]
fn debug_object_fields_match_c_server() {
    let c_bin = std::env::var("CROSS_DIGEST_C_SERVER").unwrap_or_else(|_| {
        format!(
            "{}/thirdparty/valkey/src/valkey-server",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if !std::path::Path::new(&c_bin).exists() {
        eprintln!("skip: C valkey-server not found at {c_bin}");
        return;
    }

    let c_srv = spawn_c_server(&c_bin);
    let rust_srv = TestServer::start(&["--appendonly", "no"]);
    let mut cc = c_srv.connect();
    let mut rc = rust_srv.connect();
    load_dataset(&mut cc);
    load_dataset(&mut rc);

    for key in all_keys() {
        let c_line = cc.command_str(&[b"DEBUG", b"OBJECT", key.as_bytes()]);
        let r_line = rc.command_str(&[b"DEBUG", b"OBJECT", key.as_bytes()]);
        let mut c_fields = parse_object_fields(&c_line);
        let mut r_fields = parse_object_fields(&r_line);
        c_fields.remove("at");
        r_fields.remove("at");
        c_fields.remove("lru");
        r_fields.remove("lru");
        assert_debug_object_fields_match(&mut c_fields, &mut r_fields, key);
    }

    let c_fast = parse_object_fields(&cc.command_str(&[b"DEBUG", b"OBJECT", b"lst:lp", b"fast"]));
    let r_fast = parse_object_fields(&rc.command_str(&[b"DEBUG", b"OBJECT", b"lst:lp", b"fast"]));
    assert!(!c_fast.contains_key("serializedlength"));
    assert!(!r_fast.contains_key("serializedlength"));
}

fn bulk_text(reply: &[u8]) -> &str {
    let s = std::str::from_utf8(reply).expect("utf8 reply");
    let s = s
        .strip_prefix('$')
        .map_or(s, |rest| match rest.split_once("\r\n") {
            Some((_, body)) => body.strip_suffix("\r\n").unwrap_or(body),
            None => rest,
        });
    s
}

fn entry_counts(text: &str) -> Vec<usize> {
    text.lines()
        .filter_map(|line| line.trim().strip_prefix("number of entries: "))
        .filter_map(|v| v.parse().ok())
        .collect()
}

#[test]
fn debug_htstats_matches_c_server_structure_and_edge_cases() {
    let c_bin = std::env::var("CROSS_DIGEST_C_SERVER").unwrap_or_else(|_| {
        format!(
            "{}/thirdparty/valkey/src/valkey-server",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if !std::path::Path::new(&c_bin).exists() {
        eprintln!("skip: C valkey-server not found at {c_bin}");
        return;
    }

    let c_srv = spawn_c_server(&c_bin);
    let rust_srv = TestServer::start(&["--appendonly", "no"]);
    let mut cc = c_srv.connect();
    let mut rc = rust_srv.connect();

    let empty = [b"DEBUG".as_slice(), b"HTSTATS".as_slice(), b"5".as_slice()];
    assert_eq!(
        cc.command(&empty),
        rc.command(&empty),
        "empty HTSTATS differs"
    );

    for dbid in [b"-1".as_slice(), b"16".as_slice()] {
        let q = [b"DEBUG".as_slice(), b"HTSTATS".as_slice(), dbid];
        assert_eq!(
            cc.command(&q),
            b"-ERR Out of range database\r\n",
            "C range error {dbid:?}",
        );
        assert_eq!(
            rc.command(&q),
            b"-ERR Out of range database\r\n",
            "rust range error {dbid:?}",
        );
    }
    let bad = [
        b"DEBUG".as_slice(),
        b"HTSTATS".as_slice(),
        b"abc".as_slice(),
    ];
    let expect_int = b"-ERR value is not an integer or out of range\r\n";
    assert_eq!(cc.command(&bad), expect_int);
    assert_eq!(rc.command(&bad), expect_int);

    load_dataset(&mut cc);
    load_dataset(&mut rc);

    let q = [b"DEBUG".as_slice(), b"HTSTATS".as_slice(), b"0".as_slice()];
    let c_text = bulk_text(&cc.command(&q)).to_string();
    let r_text = bulk_text(&rc.command(&q)).to_string();
    for text in [&c_text, &r_text] {
        let dict_at = text.find("[Dictionary HT]").expect("dictionary header");
        let exp_at = text.find("[Expires HT]").expect("expires header");
        assert!(dict_at < exp_at, "section order");
        assert!(text.contains("Hash table 0 stats (main hash table)"));
        assert!(
            !text.contains("Hash table 1 stats"),
            "unexpected rehash target"
        );
    }
    assert_eq!(entry_counts(&c_text), entry_counts(&r_text), "entry counts");
}

#[test]
fn debug_segfault_terminates_process_with_sigsegv_like_c() {
    use std::os::unix::process::ExitStatusExt;

    // Upstream debug.c's read-only mmap write terminates with SIGBUS on
    // Darwin, just like the Rust fault; Linux reports SIGSEGV instead.
    let expected_signal = if cfg!(target_os = "macos") {
        libc::SIGBUS
    } else {
        libc::SIGSEGV
    };

    let mut rust_srv = TestServer::start(&["--appendonly", "no"]);
    let mut rc = rust_srv.connect();
    rc.send_raw(b"*2\r\n$5\r\nDEBUG\r\n$8\r\nSEGFAULT\r\n");
    let status = rust_srv
        .wait_status(Duration::from_secs(5))
        .expect("rust server must exit after DEBUG SEGFAULT");
    assert_eq!(
        status.signal(),
        Some(expected_signal),
        "rust status: {status:?}"
    );
    let log = rust_srv.server_log();
    assert!(
        log.contains("crashed by signal"),
        "rust log missing crash marker:\n{log}"
    );

    let c_bin = std::env::var("CROSS_DIGEST_C_SERVER").unwrap_or_else(|_| {
        format!(
            "{}/thirdparty/valkey/src/valkey-server",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if !std::path::Path::new(&c_bin).exists() {
        eprintln!("skip C cross-check: valkey-server not found at {c_bin}");
        return;
    }
    let mut c_srv = spawn_c_server(&c_bin);
    let mut stream = std::net::TcpStream::connect(("127.0.0.1", c_srv.port)).expect("connect c");
    stream
        .write_all(b"*2\r\n$5\r\nDEBUG\r\n$8\r\nSEGFAULT\r\n")
        .expect("write c segfault");
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let c_status = loop {
        if let Some(status) = c_srv.child.try_wait().expect("c server wait") {
            break status;
        }
        assert!(std::time::Instant::now() < deadline, "c server did not die");
        std::thread::sleep(Duration::from_millis(20));
    };
    assert_eq!(
        c_status.signal(),
        Some(expected_signal),
        "c status: {c_status:?}"
    );
    assert_eq!(status.signal(), c_status.signal(), "crash signals differ");
}

#[test]
fn stored_string_embstr_boundary_matches_c_for_every_key_length() {
    let c_bin = std::env::var("CROSS_DIGEST_C_SERVER").unwrap_or_else(|_| {
        format!(
            "{}/thirdparty/valkey/src/valkey-server",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if !std::path::Path::new(&c_bin).exists() {
        eprintln!("skip: C valkey-server not found at {c_bin}");
        return;
    }

    let c_srv = spawn_c_server(&c_bin);
    let rust_srv = TestServer::start(&["--appendonly", "no"]);
    let mut cc = c_srv.connect();
    let mut rc = rust_srv.connect();

    let cases: [(usize, usize, usize); 4] =
        [(1, 112, 113), (4, 109, 110), (10, 103, 104), (30, 83, 84)];
    for (idx, (key_len, emb, raw)) in cases.into_iter().enumerate() {
        let key = format!("{idx}{}", "k".repeat(key_len - 1));
        for (n, want) in [(emb, "embstr"), (raw, "raw")] {
            let value = "x".repeat(n);
            for (c, tag) in [(&mut cc, "C"), (&mut rc, "rust")] {
                let reply = c.command_str(&[b"SET", key.as_bytes(), value.as_bytes()]);
                assert_eq!(reply, "+OK\r\n", "{tag} SET failed: {reply}");
                let enc = c.command_str(&[b"OBJECT", b"ENCODING", key.as_bytes()]);
                assert_eq!(
                    enc,
                    format!("${}\r\n{want}\r\n", want.len()),
                    "{tag}: key_len {key_len}, val {n} expected {want}"
                );
            }
        }
    }
}
