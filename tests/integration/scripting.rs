//! - FUNCTION LOAD / DELETE / LIST / FLUSH

use crate::common::TestServer;
use std::time::{Duration, Instant};

fn wait_until<F: FnMut() -> bool>(mut f: F, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if f() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

fn master_repl_offset(info: &str) -> i64 {
    info.lines()
        .find_map(|line| line.strip_prefix("master_repl_offset:"))
        .and_then(|v| v.parse::<i64>().ok())
        .expect("INFO replication missing master_repl_offset")
}

fn wait_replica_up(replica: &TestServer) {
    assert!(
        wait_until(
            || replica
                .connect()
                .command_str(&[b"INFO", b"replication"])
                .contains("master_link_status:up"),
            Duration::from_secs(10),
        ),
        "replica did not reach master_link_status:up"
    );
}

fn attach_replica(primary: &TestServer, replica: &TestServer) {
    let mut replica_client = replica.connect();
    let port = primary.port.to_string();
    assert_eq!(
        replica_client.command(&[b"REPLICAOF", b"127.0.0.1", port.as_bytes()]),
        b"+OK\r\n"
    );
    wait_replica_up(replica);
}

fn key_in_db(server: &TestServer, db: &[u8], key: &[u8]) -> Vec<u8> {
    let mut client = server.connect();
    assert_eq!(client.command(&[b"SELECT", db]), b"+OK\r\n");
    client.command(&[b"GET", key])
}

fn wait_for_aof_rewrite(client: &mut crate::common::Client) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let info = client.command_str(&[b"INFO", b"persistence"]);
        if info.contains("aof_rewrite_in_progress:0") && info.contains("aof_rewrite_scheduled:0") {
            return;
        }
        assert!(Instant::now() < deadline, "AOF rewrite did not finish");
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn active_incremental_aof(dir: &std::path::Path) -> Vec<u8> {
    let aof_dir = dir.join("appendonlydir");
    let manifest = std::fs::read_to_string(aof_dir.join("appendonly.aof.manifest"))
        .expect("read AOF manifest");
    let filename = manifest
        .lines()
        .find(|line| line.ends_with(" type i"))
        .and_then(|line| line.split_whitespace().nth(1))
        .expect("manifest incremental AOF entry");
    std::fs::read(aof_dir.join(filename)).expect("read incremental AOF")
}

#[test]
fn eval_basic() {
    let server = TestServer::start_default();
    let mut c = server.connect();
    let resp = c.command_str(&[b"EVAL", b"return 42", b"0"]);
    assert_eq!(resp, ":42\r\n");
}

#[test]
fn lua_insecure_api_config_toggles_getfenv() {
    let server = TestServer::start_default();
    let mut client = server.connect();

    let disabled = client.command_str(&[b"EVAL", b"return getfenv()", b"0"]);
    assert!(
        disabled.contains("nonexistent global variable 'getfenv'"),
        "unexpected disabled response: {disabled:?}"
    );

    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"lua-enable-insecure-api", b"yes"]),
        b"+OK\r\n"
    );
    let enabled = client.command_str(&[b"EVAL", b"return getfenv()", b"0"]);
    assert!(
        !enabled.starts_with("-"),
        "insecure API remained disabled: {enabled:?}"
    );

    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"lua-enable-insecure-api", b"no"]),
        b"+OK\r\n"
    );
    let disabled_again = client.command_str(&[b"EVAL", b"return getfenv()", b"0"]);
    assert!(
        disabled_again.contains("nonexistent global variable 'getfenv'"),
        "unexpected re-disabled response: {disabled_again:?}"
    );

    let startup = TestServer::start(&["--lua-enable-insecure-api", "yes"]);
    let mut startup_client = startup.connect();
    let startup_enabled = startup_client.command_str(&[b"EVAL", b"return getfenv()", b"0"]);
    assert!(
        !startup_enabled.starts_with("-"),
        "startup config did not enable insecure API: {startup_enabled:?}"
    );
}

#[test]
fn server_api_script_does_not_remove_following_expired_debug_object() {
    let server = TestServer::start_default();
    let mut client = server.connect();

    assert_eq!(
        client.command(&[
            b"EVAL",
            b"server.call('SET','key','value','PX','1'); server.call('DEBUG','SLEEP','0.01'); return server.call('EXISTS','key')",
            b"0",
        ]),
        b":1\r\n"
    );
    assert_eq!(
        client.command(&[b"DEBUG", b"SET-ACTIVE-EXPIRE", b"0"]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"SET", b"key", b"value", b"PX", b"1"]),
        b"+OK\r\n"
    );
    let debug_object = client.command_str(&[b"DEBUG", b"OBJECT", b"key"]);
    assert!(
        !debug_object.starts_with("-ERR no such key"),
        "DEBUG OBJECT unexpectedly lost key: {debug_object:?}"
    );
}

#[test]
fn queued_writable_eval_aborts_when_server_becomes_readonly_replica() {
    let primary = TestServer::start_default();
    let replica = TestServer::start_default();
    attach_replica(&primary, &replica);

    let mut admin = replica.connect();
    assert_eq!(admin.command(&[b"REPLICAOF", b"NO", b"ONE"]), b"+OK\r\n");

    let mut queued = replica.connect();
    assert_eq!(queued.command(&[b"MULTI"]), b"+OK\r\n");
    assert_eq!(
        queued.command(&[b"EVAL", b"#!lua\nreturn 1", b"0"]),
        b"+QUEUED\r\n"
    );

    attach_replica(&primary, &replica);
    let exec = queued.command_str(&[b"EXEC"]);
    assert!(
        exec.starts_with("-EXECABORT Transaction discarded because of: READONLY "),
        "unexpected EXEC reply: {exec:?}"
    );
}

#[test]
fn eval_string() {
    let server = TestServer::start_default();
    let mut c = server.connect();
    let resp = c.command_str(&[b"EVAL", b"return 'hello'", b"0"]);
    assert_eq!(resp, "$5\r\nhello\r\n");
}

#[test]
fn eval_syntax_error() {
    let server = TestServer::start_default();
    let mut c = server.connect();
    let resp = c.command_str(&[b"EVAL", b"syntax error!", b"0"]);
    assert!(
        resp.starts_with("-ERR Error compiling script"),
        "expected compile error, got: {:?}",
        resp
    );
}

#[test]
fn function_load_and_fcall() {
    let server = TestServer::start_default();
    let mut c = server.connect();

    let code = b"#!lua name=mylib flags=no-writes\nserver.register_function('myfunc', function(keys, args) return #keys + #args end)";
    let resp = c.command(&[b"FUNCTION", b"LOAD", code]);
    assert!(
        resp.starts_with(b"$"),
        "expected bulk string, got: {:?}",
        String::from_utf8_lossy(&resp)
    );

    let resp = c.command_str(&[b"FCALL", b"myfunc", b"2", b"k1", b"k2", b"a1", b"a2"]);
    assert_eq!(
        resp, ":4\r\n",
        "expected 2 keys + 2 args = 4, got: {:?}",
        resp
    );
}

#[test]
fn fcall_ro_rejects_writes() {
    let server = TestServer::start_default();
    let mut c = server.connect();

    let code = b"#!lua name=mylib\nserver.register_function('myfunc', function(keys, args) return redis.call('PING') end)";
    let resp = c.command_str(&[b"FUNCTION", b"LOAD", code]);
    assert!(
        resp.starts_with("$"),
        "expected bulk string, got: {:?}",
        resp
    );

    let resp = c.command_str(&[b"FCALL_RO", b"myfunc", b"0"]);
    assert!(
        resp.starts_with("-ERR Can not execute a script with write flag using *_ro command"),
        "expected error, got: {:?}",
        resp
    );
}

#[test]
fn function_delete_library() {
    let server = TestServer::start_default();
    let mut c = server.connect();

    let code = b"#!lua name=mylib flags=no-writes\nserver.register_function('myfunc', function(keys, args) return 42 end)";
    c.command(&[b"FUNCTION", b"LOAD", code]);

    let resp = c.command_str(&[b"FUNCTION", b"DELETE", b"mylib"]);
    assert_eq!(resp, "+OK\r\n");

    let resp = c.command_str(&[b"FCALL", b"myfunc", b"0"]);
    assert!(
        resp.starts_with("-ERR Function"),
        "expected error, got: {:?}",
        resp
    );
}

#[test]
fn function_list_libraries() {
    let server = TestServer::start_default();
    let mut c = server.connect();

    let code = b"#!lua name=mylib flags=no-writes\nserver.register_function('myfunc', function(keys, args) return 42 end)";
    c.command(&[b"FUNCTION", b"LOAD", code]);

    let resp = c.command(&[b"FUNCTION", b"LIST"]);
    assert!(resp.starts_with(b"*"), "expected array, got: {:?}", resp);
    assert!(resp.len() > 3, "expected non-empty list");
}

#[test]
fn function_flush_all() {
    let server = TestServer::start_default();
    let mut c = server.connect();

    let code = b"#!lua name=mylib flags=no-writes\nserver.register_function('myfunc', function(keys, args) return 42 end)";
    c.command(&[b"FUNCTION", b"LOAD", code]);

    let resp = c.command_str(&[b"FUNCTION", b"FLUSH"]);
    assert_eq!(resp, "+OK\r\n");

    let resp = c.command_str(&[b"FCALL", b"myfunc", b"0"]);
    assert!(
        resp.starts_with("-ERR Function"),
        "expected error, got: {:?}",
        resp
    );
}

#[test]
fn function_stats() {
    let server = TestServer::start_default();
    let mut c = server.connect();

    let resp = c.command(&[b"FUNCTION", b"STATS"]);
    assert!(resp.starts_with(b"*"), "expected array, got: {:?}", resp);
    let text = String::from_utf8_lossy(&resp);
    assert!(
        text.contains("running_script"),
        "expected running_script field"
    );
}

#[test]
fn function_help() {
    let server = TestServer::start_default();
    let mut c = server.connect();

    let resp = c.command(&[b"FUNCTION", b"HELP"]);
    assert!(resp.starts_with(b"*"), "expected array, got: {:?}", resp);
    let text = String::from_utf8_lossy(&resp);
    assert!(text.contains("FUNCTION <subcommand>"), "expected help text");
}

#[test]
fn function_dump_restore() {
    let server = TestServer::start_default();
    let mut c = server.connect();

    let code = b"#!lua name=mylib flags=no-writes\nserver.register_function('myfunc', function(keys, args) return 42 end)";
    c.command(&[b"FUNCTION", b"LOAD", code]);

    // DUMP
    let resp = c.command(&[b"FUNCTION", b"DUMP"]);
    assert!(
        resp.starts_with(b"$"),
        "expected bulk string, got: {:?}",
        resp
    );

    // RESP bulk string: $<length>\r\n<data>\r\n
    let crlf = resp.iter().position(|&b| b == b'\r').unwrap();
    let dump_data = &resp[crlf + 2..resp.len() - 2];

    // FLUSH
    c.command(&[b"FUNCTION", b"FLUSH"]);

    // RESTORE
    let resp = c.command_str(&[b"FUNCTION", b"RESTORE", dump_data]);
    assert_eq!(resp, "+OK\r\n");

    let resp = c.command_str(&[b"FCALL", b"myfunc", b"0"]);
    assert_eq!(resp, ":42\r\n");
}

#[test]
fn eval_write_propagation() {
    let server = TestServer::start_default();
    let mut c = server.connect();

    let resp = c.command_str(&[
        b"EVAL",
        b"redis.call('SET', 'testkey', 'testval') return redis.call('GET', 'testkey')",
        b"0",
    ]);
    assert_eq!(resp, "$7\r\ntestval\r\n");

    let resp = c.command_str(&[b"GET", b"testkey"]);
    assert_eq!(resp, "$7\r\ntestval\r\n");
}

#[test]
fn eval_readonly_does_not_advance_replication_offset() {
    let master = TestServer::start_default();
    let replica = TestServer::start_default();

    let mut replica_client = replica.connect();
    let port = master.port.to_string();
    assert_eq!(
        replica_client.command(&[b"REPLICAOF", b"127.0.0.1", port.as_bytes()]),
        b"+OK\r\n"
    );
    assert!(
        wait_until(
            || {
                replica
                    .connect()
                    .command_str(&[b"INFO", b"replication"])
                    .contains("master_link_status:up")
            },
            Duration::from_secs(10),
        ),
        "replica did not reach master_link_status:up"
    );

    let mut master_client = master.connect();
    assert_eq!(
        master_client.command(&[b"MSET", b"a", b"1", b"b", b"2"]),
        b"+OK\r\n"
    );
    assert!(
        wait_until(
            || {
                let mut c = replica.connect();
                c.command(&[b"MGET", b"a", b"b"]) == b"*2\r\n$1\r\n1\r\n$1\r\n2\r\n"
            },
            Duration::from_secs(10),
        ),
        "replica did not receive baseline dataset"
    );

    let before = master_repl_offset(&master_client.command_str(&[b"INFO", b"replication"]));
    assert_eq!(
        master_client.command_str(&[b"EVAL", b"return redis.call('mget', 'a', 'b')", b"0",]),
        "*2\r\n$1\r\n1\r\n$1\r\n2\r\n"
    );
    let after = master_repl_offset(&master_client.command_str(&[b"INFO", b"replication"]));

    assert_eq!(
        before, after,
        "read-only script must not advance replication offset"
    );
}

#[test]
fn eval_select_effects_replicate_to_each_database() {
    let primary = TestServer::start_default();
    let replica = TestServer::start_default();
    attach_replica(&primary, &replica);

    let script = b"redis.call('set','foo1','bar1'); \
        redis.call('select','10'); redis.call('incr','x'); \
        redis.call('select','11'); redis.call('incr','z')";
    let mut primary_client = primary.connect();
    assert_eq!(primary_client.command(&[b"EVAL", script, b"0"]), b"$-1\r\n");
    assert_eq!(primary_client.command(&[b"EVAL", script, b"0"]), b"$-1\r\n");

    assert!(
        wait_until(
            || key_in_db(&replica, b"0", b"foo1") == b"$4\r\nbar1\r\n"
                && key_in_db(&replica, b"10", b"x") == b"$1\r\n2\r\n"
                && key_in_db(&replica, b"11", b"z") == b"$1\r\n2\r\n",
            Duration::from_secs(10),
        ),
        "replica did not receive all cross-DB EVAL effects"
    );
}

#[test]
fn pipelined_direct_write_and_exec_keep_sink_order() {
    let mut primary = TestServer::start(&["--appendonly"]);
    let replica = TestServer::start_default();
    attach_replica(&primary, &replica);
    let mut client = primary.connect();

    client.send_raw(
        b"*3\r\n$3\r\nSET\r\n$5\r\norder\r\n$6\r\nbefore\r\n\
          *1\r\n$5\r\nMULTI\r\n\
          *3\r\n$3\r\nSET\r\n$5\r\norder\r\n$5\r\nafter\r\n\
          *1\r\n$4\r\nEXEC\r\n",
    );
    assert_eq!(client.read_reply(), b"+OK\r\n");
    assert_eq!(client.read_reply(), b"+OK\r\n");
    assert_eq!(client.read_reply(), b"+QUEUED\r\n");
    assert_eq!(client.read_reply(), b"*1\r\n+OK\r\n");

    assert!(
        wait_until(
            || key_in_db(&replica, b"0", b"order") == b"$5\r\nafter\r\n",
            Duration::from_secs(10),
        ),
        "replica observed EXEC before the direct SET"
    );

    let mut shutdown = primary.connect();
    shutdown.send(&[b"SHUTDOWN", b"NOSAVE"]);
    assert_eq!(primary.wait_exit(Duration::from_secs(5)), Some(0));
    let dir = primary.take_data_dir();
    let restarted = TestServer::start_in_dir(dir, &["--appendonly"]);
    assert_eq!(
        key_in_db(&restarted, b"0", b"order"),
        b"$5\r\nafter\r\n",
        "AOF replay observed EXEC before the direct SET"
    );
}

#[test]
fn transaction_eval_cross_db_effects_replicate_without_nested_exec() {
    let primary = TestServer::start_default();
    let replica = TestServer::start_default();
    attach_replica(&primary, &replica);
    let mut client = primary.connect();
    let script = b"redis.call('set','tx-db0','zero'); \
        redis.call('select','10'); redis.call('set','tx-db10','ten')";

    assert_eq!(client.command(&[b"MULTI"]), b"+OK\r\n");
    assert_eq!(client.command(&[b"EVAL", script, b"0"]), b"+QUEUED\r\n");
    assert_eq!(client.command(&[b"EXEC"]), b"*1\r\n$-1\r\n");

    assert!(
        wait_until(
            || key_in_db(&replica, b"0", b"tx-db0") == b"$4\r\nzero\r\n"
                && key_in_db(&replica, b"10", b"tx-db10") == b"$3\r\nten\r\n",
            Duration::from_secs(10),
        ),
        "replica did not receive transaction script effects in their databases"
    );
}

#[test]
fn eval_select_effects_append_exact_replication_frames() {
    let primary = TestServer::start_default();
    let replica = TestServer::start_default();
    attach_replica(&primary, &replica);
    let mut client = primary.connect();
    assert_eq!(client.command(&[b"SET", b"cursor-prime", b"1"]), b"+OK\r\n");

    let before = master_repl_offset(&client.command_str(&[b"INFO", b"replication"]));
    let script = b"redis.call('set','foo1','bar1'); \
        redis.call('select','10'); redis.call('incr','x'); \
        redis.call('select','11'); redis.call('incr','z')";
    assert_eq!(client.command(&[b"EVAL", script, b"0"]), b"$-1\r\n");
    let after = master_repl_offset(&client.command_str(&[b"INFO", b"replication"]));

    assert_eq!(after - before, 152);
}

#[test]
fn eval_error_keeps_completed_cross_db_effects() {
    let primary = TestServer::start_default();
    let replica = TestServer::start_default();
    attach_replica(&primary, &replica);
    let mut client = primary.connect();

    let reply = client.command(&[
        b"EVAL",
        b"redis.call('set','a','1'); redis.call('select','10'); redis.call('set','b','2'); redis.call('lpush','b','x')",
        b"0",
    ]);

    assert!(reply.starts_with(b"-WRONGTYPE"));
    assert!(
        wait_until(
            || key_in_db(&replica, b"0", b"a") == b"$1\r\n1\r\n"
                && key_in_db(&replica, b"10", b"b") == b"$1\r\n2\r\n",
            Duration::from_secs(10),
        ),
        "replica did not retain completed effects before the script error"
    );
}

#[test]
fn evalsha_select_effects_retain_database() {
    let primary = TestServer::start_default();
    let replica = TestServer::start_default();
    attach_replica(&primary, &replica);
    let mut client = primary.connect();
    let script = b"redis.call('set', 'sha-a', '1'); redis.call('select', '10'); redis.call('set', 'sha-b', '2')";
    let loaded = client.command(&[b"SCRIPT", b"LOAD", script]);
    assert!(loaded.starts_with(b"$40\r\n"));
    let sha = &loaded[5..45];

    assert_eq!(client.command(&[b"EVALSHA", sha, b"0"]), b"$-1\r\n");
    assert!(
        wait_until(
            || key_in_db(&replica, b"0", b"sha-a") == b"$1\r\n1\r\n"
                && key_in_db(&replica, b"10", b"sha-b") == b"$1\r\n2\r\n",
            Duration::from_secs(10),
        ),
        "replica did not receive EVALSHA effects in their original DBs"
    );
}

#[test]
fn fcall_select_effects_retain_database() {
    let primary = TestServer::start_default();
    let replica = TestServer::start_default();
    attach_replica(&primary, &replica);
    let mut client = primary.connect();
    let library = b"#!lua name=g5\nserver.register_function('crossdb', function(keys, args) redis.call('set', 'fn-a', '1'); redis.call('select', '10'); redis.call('set', 'fn-b', '2') end)";
    assert!(client
        .command(&[b"FUNCTION", b"LOAD", library])
        .starts_with(b"$"));

    assert_eq!(client.command(&[b"FCALL", b"crossdb", b"0"]), b"$-1\r\n");
    assert!(
        wait_until(
            || key_in_db(&replica, b"0", b"fn-a") == b"$1\r\n1\r\n"
                && key_in_db(&replica, b"10", b"fn-b") == b"$1\r\n2\r\n",
            Duration::from_secs(10),
        ),
        "replica did not receive FCALL effects in their original DBs"
    );
}

#[test]
fn eval_select_effects_survive_aof_replay() {
    let mut server = TestServer::start(&["--appendonly"]);
    let mut client = server.connect();
    let script = b"redis.call('set','aof-db0','1'); \
        redis.call('select','10'); redis.call('set','aof-db10','2'); \
        redis.call('select','11'); redis.call('set','aof-db11','3')";
    assert_eq!(client.command(&[b"EVAL", script, b"0"]), b"$-1\r\n");

    let mut shutdown = server.connect();
    shutdown.send(&[b"SHUTDOWN", b"NOSAVE"]);
    assert_eq!(server.wait_exit(Duration::from_secs(5)), Some(0));
    let dir = server.take_data_dir();

    let restarted = TestServer::start_in_dir(dir, &["--appendonly"]);
    assert_eq!(key_in_db(&restarted, b"0", b"aof-db0"), b"$1\r\n1\r\n");
    assert_eq!(key_in_db(&restarted, b"10", b"aof-db10"), b"$1\r\n2\r\n");
    assert_eq!(key_in_db(&restarted, b"11", b"aof-db11"), b"$1\r\n3\r\n");
}

fn exec_watched_get(client: &mut crate::common::Client, key: &[u8]) -> Vec<u8> {
    assert_eq!(client.command(&[b"MULTI"]), b"+OK\r\n");
    assert_eq!(client.command(&[b"GET", key]), b"+QUEUED\r\n");
    client.command(&[b"EXEC"])
}

#[test]
fn eval_noop_writes_do_not_invalidate_watch() {
    let server = TestServer::start_default();
    let mut watcher = server.connect();
    let mut script = server.connect();

    assert_eq!(watcher.command(&[b"SET", b"set-key", b"old"]), b"+OK\r\n");
    assert_eq!(watcher.command(&[b"WATCH", b"set-key"]), b"+OK\r\n");
    assert_eq!(
        script.command(&[
            b"EVAL",
            b"return redis.call('SET', KEYS[1], 'new', 'NX')",
            b"1",
            b"set-key",
        ]),
        b"$-1\r\n"
    );
    assert_eq!(
        exec_watched_get(&mut watcher, b"set-key"),
        b"*1\r\n$3\r\nold\r\n"
    );

    assert_eq!(watcher.command(&[b"WATCH", b"missing-list"]), b"+OK\r\n");
    assert_eq!(
        script.command(&[
            b"EVAL",
            b"return redis.call('LPUSHX', KEYS[1], 'v')",
            b"1",
            b"missing-list",
        ]),
        b":0\r\n"
    );
    assert_eq!(
        exec_watched_get(&mut watcher, b"missing-list"),
        b"*1\r\n$-1\r\n"
    );

    assert_eq!(watcher.command(&[b"WATCH", b"missing-rlist"]), b"+OK\r\n");
    assert_eq!(
        script.command(&[
            b"EVAL",
            b"return redis.call('RPUSHX', KEYS[1], 'v')",
            b"1",
            b"missing-rlist",
        ]),
        b":0\r\n"
    );
    assert_eq!(
        exec_watched_get(&mut watcher, b"missing-rlist"),
        b"*1\r\n$-1\r\n"
    );
}

#[test]
fn eval_noop_write_marks_script_dirty_without_invalidating_watch() {
    let server = TestServer::start_default();
    let mut watcher = server.connect();
    let mut script = server.connect();

    assert_eq!(watcher.command(&[b"WATCH", b"missing-key"]), b"+OK\r\n");
    assert_eq!(
        script.command(&[b"CONFIG", b"SET", b"maxmemory", b"1"]),
        b"+OK\r\n"
    );
    assert_eq!(
        script.command(&[
            b"EVAL",
            b"redis.call('DEL', KEYS[1]); redis.call('SET', KEYS[2], ARGV[1])",
            b"2",
            b"missing-key",
            b"written-key",
            b"value",
        ]),
        b"$-1\r\n"
    );
    assert_eq!(
        script.command(&[b"CONFIG", b"SET", b"maxmemory", b"0"]),
        b"+OK\r\n"
    );
    assert_eq!(
        script.command(&[b"GET", b"written-key"]),
        b"$5\r\nvalue\r\n"
    );
    assert_eq!(
        exec_watched_get(&mut watcher, b"missing-key"),
        b"*1\r\n$-1\r\n"
    );
}

#[test]
fn eval_may_replicate_error_does_not_mark_script_dirty() {
    let server = TestServer::start_default();
    let mut client = server.connect();

    assert_eq!(
        client.command(&[b"SET", b"wrong-type", b"value"]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"maxmemory", b"1"]),
        b"+OK\r\n"
    );
    let response = client.command(&[
        b"EVAL",
        b"redis.pcall('PFCOUNT', KEYS[1]); return redis.call('SET', KEYS[2], ARGV[1])",
        b"2",
        b"wrong-type",
        b"written-key",
        b"value",
    ]);
    assert!(
        response.starts_with(b"-OOM command not allowed"),
        "unexpected response: {}",
        String::from_utf8_lossy(&response)
    );
    assert_eq!(
        client.command(&[b"CONFIG", b"SET", b"maxmemory", b"0"]),
        b"+OK\r\n"
    );
    assert_eq!(client.command(&[b"GET", b"written-key"]), b"$-1\r\n");
}

#[test]
fn eval_repl_none_write_invalidates_watch_without_propagation() {
    let server = TestServer::start_default();
    let mut watcher = server.connect();
    let mut script = server.connect();

    assert_eq!(watcher.command(&[b"WATCH", b"local-key"]), b"+OK\r\n");
    assert_eq!(
        script.command(&[
            b"EVAL",
            b"redis.set_repl(redis.REPL_NONE); return redis.call('SET', KEYS[1], 'v')",
            b"1",
            b"local-key",
        ]),
        b"+OK\r\n"
    );
    assert_eq!(exec_watched_get(&mut watcher, b"local-key"), b"*-1\r\n");
}

#[test]
fn transaction_eval_repl_none_write_invalidates_other_watchers() {
    let server = TestServer::start_default();
    let mut watcher = server.connect();
    let mut transaction = server.connect();

    assert_eq!(watcher.command(&[b"WATCH", b"tx-local-key"]), b"+OK\r\n");
    assert_eq!(transaction.command(&[b"MULTI"]), b"+OK\r\n");
    assert_eq!(
        transaction.command(&[
            b"EVAL",
            b"redis.set_repl(redis.REPL_NONE); return redis.call('SET', KEYS[1], 'v')",
            b"1",
            b"tx-local-key",
        ]),
        b"+QUEUED\r\n"
    );
    assert_eq!(transaction.command(&[b"EXEC"]), b"*1\r\n+OK\r\n");
    assert_eq!(exec_watched_get(&mut watcher, b"tx-local-key"), b"*-1\r\n");
}

#[test]
fn transaction_checks_expired_watches_before_running_script_commands() {
    let server = TestServer::start_default();
    let mut client = server.connect();

    assert_eq!(
        client.command(&[b"DEBUG", b"SET-ACTIVE-EXPIRE", b"0"]),
        b"+OK\r\n"
    );
    assert_eq!(
        client.command(&[b"SET", b"expiring-watch", b"v", b"PX", b"100"]),
        b"+OK\r\n"
    );
    assert_eq!(client.command(&[b"WATCH", b"expiring-watch"]), b"+OK\r\n");
    assert_eq!(client.command(&[b"MULTI"]), b"+OK\r\n");
    assert_eq!(
        client.command(&[b"SET", b"must-not-commit", b"1"]),
        b"+QUEUED\r\n"
    );
    assert_eq!(
        client.command(&[
            b"EVAL",
            b"return redis.call('GET', KEYS[1])",
            b"1",
            b"expiring-watch",
        ]),
        b"+QUEUED\r\n"
    );
    std::thread::sleep(Duration::from_millis(200));

    assert_eq!(client.command(&[b"EXEC"]), b"*-1\r\n");
    assert_eq!(client.command(&[b"GET", b"must-not-commit"]), b"$-1\r\n");
}

#[test]
fn paused_transaction_exec_is_written_to_aof() {
    let mut server = TestServer::start(&["--appendonly"]);
    let mut transaction = server.connect();
    let mut admin = server.connect();
    let mut watcher = server.connect();

    assert_eq!(watcher.command(&[b"WATCH", b"paused-exec-key"]), b"+OK\r\n");

    assert_eq!(transaction.command(&[b"MULTI"]), b"+OK\r\n");
    assert_eq!(
        transaction.command(&[b"SET", b"paused-exec-key", b"value"]),
        b"+QUEUED\r\n"
    );
    assert_eq!(
        transaction.command(&[
            b"EVAL",
            b"redis.call('set','paused-db0','zero'); redis.call('select','10'); redis.call('set','paused-db10','ten')",
            b"0",
        ]),
        b"+QUEUED\r\n"
    );
    assert_eq!(
        admin.command(&[b"CLIENT", b"PAUSE", b"200", b"WRITE"]),
        b"+OK\r\n"
    );
    assert_eq!(transaction.command(&[b"EXEC"]), b"*2\r\n+OK\r\n$-1\r\n");
    assert_eq!(
        admin.command(&[b"GET", b"paused-exec-key"]),
        b"$5\r\nvalue\r\n"
    );
    assert_eq!(
        exec_watched_get(&mut watcher, b"paused-exec-key"),
        b"*-1\r\n"
    );

    let mut shutdown = server.connect();
    shutdown.send(&[b"SHUTDOWN", b"NOSAVE"]);
    assert_eq!(server.wait_exit(Duration::from_secs(5)), Some(0));
    let dir = server.take_data_dir();
    let aof = std::fs::read(dir.join("appendonlydir").join("appendonly.aof.1.incr.aof"))
        .expect("read incremental AOF");
    assert!(
        aof.windows(b"paused-exec-key".len())
            .any(|window| window == b"paused-exec-key"),
        "paused EXEC was not propagated to AOF: {}",
        String::from_utf8_lossy(&aof)
    );
    let db0 = aof
        .windows(b"paused-db0".len())
        .position(|window| window == b"paused-db0")
        .expect("DB0 script effect in incremental AOF");
    let select10 = aof
        .windows(b"SELECT\r\n$2\r\n10".len())
        .position(|window| window == b"SELECT\r\n$2\r\n10")
        .expect("SELECT 10 in incremental AOF");
    let db10 = aof
        .windows(b"paused-db10".len())
        .position(|window| window == b"paused-db10")
        .expect("DB10 script effect in incremental AOF");
    assert!(db0 < select10 && select10 < db10);

    let restarted = TestServer::start_in_dir(dir, &["--appendonly"]);
    assert_eq!(
        key_in_db(&restarted, b"0", b"paused-db0"),
        b"$4\r\nzero\r\n"
    );
    assert_eq!(
        key_in_db(&restarted, b"10", b"paused-db10"),
        b"$3\r\nten\r\n"
    );
}

#[test]
fn paused_eval_routes_typed_effects_to_replica_and_aof() {
    let mut primary = TestServer::start(&["--appendonly"]);
    let replica = TestServer::start_default();
    attach_replica(&primary, &replica);
    let mut admin = primary.connect();
    let mut script = primary.connect();

    assert_eq!(
        admin.command(&[b"CLIENT", b"PAUSE", b"100", b"WRITE"]),
        b"+OK\r\n"
    );
    assert_eq!(
        script.command(&[
            b"EVAL",
            b"redis.call('set','paused-eval-db0','zero'); redis.call('select','10'); redis.call('set','paused-eval-db10','ten')",
            b"0",
        ]),
        b"$-1\r\n"
    );
    assert!(
        wait_until(
            || key_in_db(&replica, b"0", b"paused-eval-db0") == b"$4\r\nzero\r\n"
                && key_in_db(&replica, b"10", b"paused-eval-db10") == b"$3\r\nten\r\n",
            Duration::from_secs(10),
        ),
        "paused EVAL effects did not reach the replica: db0={:?}, db10={:?}",
        key_in_db(&replica, b"0", b"paused-eval-db0"),
        key_in_db(&replica, b"10", b"paused-eval-db10")
    );

    let mut shutdown = primary.connect();
    shutdown.send(&[b"SHUTDOWN", b"NOSAVE"]);
    assert_eq!(primary.wait_exit(Duration::from_secs(5)), Some(0));
    let dir = primary.take_data_dir();
    let restarted = TestServer::start_in_dir(dir, &["--appendonly"]);
    assert_eq!(
        key_in_db(&restarted, b"0", b"paused-eval-db0"),
        b"$4\r\nzero\r\n"
    );
    assert_eq!(
        key_in_db(&restarted, b"10", b"paused-eval-db10"),
        b"$3\r\nten\r\n"
    );
}

#[test]
fn exec_after_aof_rewrite_sequences_from_the_aof_cursor() {
    let mut primary = TestServer::start(&["--appendonly"]);
    let replica = TestServer::start_default();
    attach_replica(&primary, &replica);
    let mut client = primary.connect();

    assert_eq!(client.command(&[b"SELECT", b"9"]), b"+OK\r\n");
    assert_eq!(
        client.command(&[b"SET", b"rewrite-prime", b"1"]),
        b"+OK\r\n"
    );
    assert!(client
        .command(&[b"BGREWRITEAOF"])
        .starts_with(b"+Background"));
    wait_for_aof_rewrite(&mut client);

    assert_eq!(client.command(&[b"MULTI"]), b"+OK\r\n");
    assert_eq!(
        client.command(&[b"SET", b"rewrite-boundary", b"after"]),
        b"+QUEUED\r\n"
    );
    assert_eq!(client.command(&[b"EXEC"]), b"*1\r\n+OK\r\n");
    assert!(
        wait_until(
            || key_in_db(&replica, b"9", b"rewrite-boundary") == b"$5\r\nafter\r\n",
            Duration::from_secs(10),
        ),
        "replication cursor did not preserve DB 9"
    );

    let mut shutdown = primary.connect();
    shutdown.send(&[b"SHUTDOWN", b"NOSAVE"]);
    assert_eq!(primary.wait_exit(Duration::from_secs(5)), Some(0));
    let dir = primary.take_data_dir();
    let aof = active_incremental_aof(&dir);
    let select9 = aof
        .windows(b"SELECT\r\n$1\r\n9".len())
        .position(|window| window == b"SELECT\r\n$1\r\n9")
        .expect("independent AOF cursor must synthesize SELECT 9");
    let write = aof
        .windows(b"rewrite-boundary".len())
        .position(|window| window == b"rewrite-boundary")
        .expect("transaction write in incremental AOF");
    assert!(select9 < write);
    let _ = std::fs::remove_dir_all(dir);
}
