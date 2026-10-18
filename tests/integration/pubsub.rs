use crate::common::TestServer;

#[test]
fn subscribe_confirms_per_channel() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();

    let r1 = c.command(&[b"SUBSCRIBE", b"chan1"]);
    assert_eq!(r1, b"*3\r\n$9\r\nsubscribe\r\n$5\r\nchan1\r\n:1\r\n");

    c.send(&[b"SUBSCRIBE", b"chan2", b"chan3"]);
    assert_eq!(
        c.read_reply(),
        b"*3\r\n$9\r\nsubscribe\r\n$5\r\nchan2\r\n:2\r\n"
    );
    assert_eq!(
        c.read_reply(),
        b"*3\r\n$9\r\nsubscribe\r\n$5\r\nchan3\r\n:3\r\n"
    );
}

#[test]
fn publish_delivers_message_to_subscriber() {
    let srv = TestServer::start_default();
    let mut sub = srv.connect();
    let mut pubc = srv.connect();

    sub.command(&[b"SUBSCRIBE", b"news"]);

    assert_eq!(pubc.command(&[b"PUBLISH", b"news", b"hello"]), b":1\r\n");

    assert_eq!(
        sub.read_reply(),
        b"*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$5\r\nhello\r\n"
    );
}

#[test]
fn publish_to_no_subscribers_returns_zero() {
    let srv = TestServer::start_default();
    let mut pubc = srv.connect();
    assert_eq!(pubc.command(&[b"PUBLISH", b"ghost", b"x"]), b":0\r\n");
}

#[test]
fn publish_fans_out_to_two_subscribers() {
    let srv = TestServer::start_default();
    let mut s1 = srv.connect();
    let mut s2 = srv.connect();
    let mut pubc = srv.connect();

    s1.command(&[b"SUBSCRIBE", b"room"]);
    s2.command(&[b"SUBSCRIBE", b"room"]);

    assert_eq!(pubc.command(&[b"PUBLISH", b"room", b"hi"]), b":2\r\n");
    let expected: &[u8] = b"*3\r\n$7\r\nmessage\r\n$4\r\nroom\r\n$2\r\nhi\r\n";
    assert_eq!(s1.read_reply(), expected);
    assert_eq!(s2.read_reply(), expected);
}

#[test]
fn unsubscribe_stops_delivery() {
    let srv = TestServer::start_default();
    let mut sub = srv.connect();
    let mut pubc = srv.connect();

    sub.command(&[b"SUBSCRIBE", b"c1"]);
    assert_eq!(
        sub.command(&[b"UNSUBSCRIBE", b"c1"]),
        b"*3\r\n$11\r\nunsubscribe\r\n$2\r\nc1\r\n:0\r\n"
    );
    assert_eq!(pubc.command(&[b"PUBLISH", b"c1", b"x"]), b":0\r\n");
}

#[test]
fn unsubscribe_without_args() {
    let srv = TestServer::start_default();

    let mut c1 = srv.connect();
    assert_eq!(
        c1.command(&[b"UNSUBSCRIBE"]),
        b"*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n"
    );

    let mut c2 = srv.connect();
    c2.command(&[b"SUBSCRIBE", b"only"]);
    assert_eq!(
        c2.command(&[b"UNSUBSCRIBE"]),
        b"*3\r\n$11\r\nunsubscribe\r\n$4\r\nonly\r\n:0\r\n"
    );
}

#[test]
fn subscribe_mode_blocks_normal_commands_resp2() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    c.command(&[b"SUBSCRIBE", b"c1"]);

    let err = c.command_str(&[b"GET", b"foo"]);
    assert!(
        err.starts_with("-ERR") && err.contains("SUBSCRIBE"),
        "got: {}",
        err
    );

    assert_eq!(c.command(&[b"PING"]), b"*2\r\n$4\r\npong\r\n$0\r\n\r\n");
    assert_eq!(
        c.command(&[b"PING", b"foo"]),
        b"*2\r\n$4\r\npong\r\n$3\r\nfoo\r\n"
    );
}

#[test]
fn disconnect_cleans_up_subscription() {
    let srv = TestServer::start_default();
    let mut pubc = srv.connect();
    {
        let mut sub = srv.connect();
        sub.command(&[b"SUBSCRIBE", b"ephemeral"]);
        assert_eq!(pubc.command(&[b"PUBLISH", b"ephemeral", b"x"]), b":1\r\n");
    }
    std::thread::sleep(std::time::Duration::from_millis(200));
    assert_eq!(pubc.command(&[b"PUBLISH", b"ephemeral", b"x"]), b":0\r\n");
}

#[test]
fn arity_errors() {
    let srv = TestServer::start_default();
    let mut c = srv.connect();
    assert!(c
        .command_str(&[b"SUBSCRIBE"])
        .starts_with("-ERR wrong number"));
    assert!(c
        .command_str(&[b"PUBLISH", b"onlych"])
        .starts_with("-ERR wrong number"));
}

#[test]
fn psubscribe_glob_matching_and_pmessage() {
    let srv = TestServer::start_default();
    let mut sub = srv.connect();
    let mut pubc = srv.connect();

    sub.command(&[b"PSUBSCRIBE", b"news.*"]);
    sub.command(&[b"PSUBSCRIBE", b"h[ae]llo"]);

    assert_eq!(pubc.command(&[b"PUBLISH", b"news.1", b"hi"]), b":1\r\n");
    assert_eq!(
        sub.read_reply(),
        b"*4\r\n$8\r\npmessage\r\n$6\r\nnews.*\r\n$6\r\nnews.1\r\n$2\r\nhi\r\n"
    );

    assert_eq!(pubc.command(&[b"PUBLISH", b"hello", b"x"]), b":1\r\n");
    assert_eq!(
        sub.read_reply(),
        b"*4\r\n$8\r\npmessage\r\n$8\r\nh[ae]llo\r\n$5\r\nhello\r\n$1\r\nx\r\n"
    );

    assert_eq!(pubc.command(&[b"PUBLISH", b"hbllo", b"y"]), b":0\r\n");
}

#[test]
fn spublish_delivers_only_to_shard_subscribers() {
    let srv = TestServer::start_default();
    let mut shard_sub = srv.connect();
    let mut normal_sub = srv.connect();
    let mut pubc = srv.connect();

    shard_sub.command(&[b"SSUBSCRIBE", b"chan"]);
    normal_sub.command(&[b"SUBSCRIBE", b"chan"]);

    assert_eq!(pubc.command(&[b"SPUBLISH", b"chan", b"smsg"]), b":1\r\n");
    assert_eq!(
        shard_sub.read_reply(),
        b"*3\r\n$8\r\nsmessage\r\n$4\r\nchan\r\n$4\r\nsmsg\r\n"
    );

    assert_eq!(pubc.command(&[b"PUBLISH", b"chan", b"nmsg"]), b":1\r\n");
    assert_eq!(
        normal_sub.read_reply(),
        b"*3\r\n$7\r\nmessage\r\n$4\r\nchan\r\n$4\r\nnmsg\r\n"
    );
}

#[test]
fn punsubscribe_stops_pattern_delivery() {
    let srv = TestServer::start_default();
    let mut sub = srv.connect();
    let mut pubc = srv.connect();

    sub.command(&[b"PSUBSCRIBE", b"pat*"]);
    assert_eq!(
        sub.command(&[b"PUNSUBSCRIBE", b"pat*"]),
        b"*3\r\n$12\r\npunsubscribe\r\n$4\r\npat*\r\n:0\r\n"
    );
    assert_eq!(pubc.command(&[b"PUBLISH", b"patx", b"x"]), b":0\r\n");
}
