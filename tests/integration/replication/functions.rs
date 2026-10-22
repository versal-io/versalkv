use crate::common::TestServer;
use crate::replication::common::wait_until;
use std::time::Duration;

#[test]
fn function_load_propagates_to_replica_stream() {
    let master = TestServer::start_default();
    let replica = TestServer::start_default();
    let mut mc = master.connect();
    let mut rc = replica.connect();
    let port = master.port.to_string();

    assert_eq!(
        mc.command(&[b"CONFIG", b"SET", b"repl-ping-replica-period", b"3600"]),
        b"+OK\r\n"
    );
    assert_eq!(
        rc.command(&[b"CONFIG", b"SET", b"repl-ping-replica-period", b"3600"]),
        b"+OK\r\n"
    );
    assert_eq!(
        rc.command(&[b"REPLICAOF", b"127.0.0.1", port.as_bytes()]),
        b"+OK\r\n"
    );
    assert!(
        wait_until(
            || replica
                .connect()
                .command_str(&[b"INFO", b"replication"])
                .contains("master_link_status:up"),
            Duration::from_secs(10),
        ),
        "sync incomplete"
    );

    let mut sub = replica.connect();
    sub.send_raw(b"SYNC\r\n");
    sub.read_sync_rdb();

    let code = b"#!lua name=test\nserver.register_function('test', function() return 'hello2' end)";
    assert_eq!(mc.command(&[b"FUNCTION", b"LOAD", code]), b"$4\r\ntest\r\n");

    // The stream may contain bootstrap and control frames before the verbatim
    // FUNCTION frame.
    let got = loop {
        let frame = sub.read_repl_command();
        if frame[0].eq_ignore_ascii_case(b"PING")
            || frame[0].eq_ignore_ascii_case(b"SELECT")
            || (frame[0].eq_ignore_ascii_case(b"REPLCONF")
                && frame.len() >= 2
                && frame[1].eq_ignore_ascii_case(b"GETACK"))
        {
            continue;
        }
        break frame;
    };
    assert_eq!(got[0].to_ascii_lowercase(), b"function");
    assert_eq!(got[1].to_ascii_uppercase(), b"LOAD");
}

#[test]
fn full_sync_overwrites_replica_same_named_function() {
    let master = TestServer::start_default();
    let replica = TestServer::start_default();
    let mut mc = master.connect();
    let mut rc = replica.connect();
    let port = master.port.to_string();

    assert_eq!(
        rc.command(&[b"CONFIG", b"SET", b"replica-read-only", b"no"]),
        b"+OK\r\n"
    );

    let replica_code =
        b"#!lua name=test\nserver.register_function('test', function() return 'hello1' end)";
    assert_eq!(
        rc.command(&[b"FUNCTION", b"LOAD", replica_code]),
        b"$4\r\ntest\r\n"
    );
    assert_eq!(rc.command(&[b"FCALL", b"test", b"0"]), b"$6\r\nhello1\r\n");

    let master_code =
        b"#!lua name=test\nserver.register_function('test', function() return 'hello2' end)";
    assert_eq!(
        mc.command(&[b"FUNCTION", b"LOAD", master_code]),
        b"$4\r\ntest\r\n"
    );

    assert_eq!(
        rc.command(&[b"REPLICAOF", b"127.0.0.1", port.as_bytes()]),
        b"+OK\r\n"
    );
    assert!(
        wait_until(
            || replica
                .connect()
                .command_str(&[b"INFO", b"replication"])
                .contains("master_link_status:up"),
            Duration::from_secs(10),
        ),
        "sync incomplete"
    );

    assert!(
        wait_until(
            || replica.connect().command(&[b"FCALL", b"test", b"0"]) == b"$6\r\nhello2\r\n",
            Duration::from_secs(10),
        ),
        "replica function not overwritten with master's, got {:?}",
        String::from_utf8_lossy(&replica.connect().command(&[b"FCALL", b"test", b"0"]))
    );
}

// exposed after swapping`）
#[test]
fn full_sync_transfers_functions_to_replica() {
    let master = TestServer::start_default();
    let replica = TestServer::start_default();
    let mut mc = master.connect();
    let mut rc = replica.connect();
    let port = master.port.to_string();

    let code = b"#!lua name=test\nserver.register_function{function_name='test', callback=function() return 'hello2' end, flags={'no-writes'}}";
    assert_eq!(mc.command(&[b"FUNCTION", b"LOAD", code]), b"$4\r\ntest\r\n");

    assert_eq!(
        rc.command(&[b"REPLICAOF", b"127.0.0.1", port.as_bytes()]),
        b"+OK\r\n"
    );
    assert!(
        wait_until(
            || replica
                .connect()
                .command_str(&[b"INFO", b"replication"])
                .contains("master_link_status:up"),
            Duration::from_secs(10),
        ),
        "sync incomplete"
    );

    assert!(
        wait_until(
            || replica.connect().command(&[b"FCALL_RO", b"test", b"0"]) == b"$6\r\nhello2\r\n",
            Duration::from_secs(10),
        ),
        "replica missing master function"
    );
}
