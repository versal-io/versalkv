use super::*;
use bytes::Bytes;

#[test]
fn inline_ping() {
    let mut p = Parser::new();
    p.feed(b"PING\r\n");
    assert_eq!(
        p.try_parse().unwrap(),
        Some(vec![Bytes::from_static(b"PING")])
    );
}
#[test]
fn inline_echo() {
    let mut p = Parser::new();
    p.feed(b"ECHO hello\r\n");
    assert_eq!(
        p.try_parse().unwrap(),
        Some(vec![
            Bytes::from_static(b"ECHO"),
            Bytes::from_static(b"hello")
        ])
    );
}
#[test]
fn mbulk_ping() {
    let mut p = Parser::new();
    p.feed(b"*1\r\n$4\r\nPING\r\n");
    assert_eq!(
        p.try_parse().unwrap(),
        Some(vec![Bytes::from_static(b"PING")])
    );
}
#[test]
fn mbulk_echo() {
    let mut p = Parser::new();
    p.feed(b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n");
    assert_eq!(
        p.try_parse().unwrap(),
        Some(vec![
            Bytes::from_static(b"ECHO"),
            Bytes::from_static(b"hello")
        ])
    );
}
#[test]
fn partial_feed() {
    let mut p = Parser::new();
    p.feed(b"*2\r\n$4\r\nEC");
    assert_eq!(p.try_parse().unwrap(), None);
    p.feed(b"HO\r\n$5\r\nhe");
    assert_eq!(p.try_parse().unwrap(), None);
    p.feed(b"llo\r\n");
    assert_eq!(
        p.try_parse().unwrap(),
        Some(vec![
            Bytes::from_static(b"ECHO"),
            Bytes::from_static(b"hello")
        ])
    );
}
#[test]
fn multiple_cmds() {
    let mut p = Parser::new();
    p.feed(b"PING\r\n*1\r\n$4\r\nPING\r\nECHO hi\r\n");
    assert_eq!(
        p.try_parse().unwrap(),
        Some(vec![Bytes::from_static(b"PING")])
    );
    assert_eq!(
        p.try_parse().unwrap(),
        Some(vec![Bytes::from_static(b"PING")])
    );
    assert_eq!(
        p.try_parse().unwrap(),
        Some(vec![Bytes::from_static(b"ECHO"), Bytes::from_static(b"hi")])
    );
    assert_eq!(p.try_parse().unwrap(), None);
}
#[test]
fn empty_lines_skipped() {
    let mut p = Parser::new();
    p.feed(b"\r\n\r\nPING\r\n");
    assert_eq!(
        p.try_parse().unwrap(),
        Some(vec![Bytes::from_static(b"PING")])
    );
}
#[test]
fn inline_quotes() {
    let mut p = Parser::new();
    p.feed(b"\"tes\"t-'k'e\"y\"\r\n");
    let r = p.try_parse().unwrap().unwrap();
    assert_eq!(r.len(), 1);
    assert_eq!(r[0], Bytes::from_static(b"test-key"));
}

#[test]
fn inline_token_start_preserves_quoted_empty_argument() {
    let mut p = Parser::new();
    p.feed(b"ECHO \"\" tail\r\n");
    assert_eq!(
        p.try_parse().unwrap(),
        Some(vec![
            Bytes::from_static(b"ECHO"),
            Bytes::new(),
            Bytes::from_static(b"tail"),
        ])
    );
}

#[test]
fn inline_separators_match_sdssplitargs() {
    assert_eq!(
        parse_inline_args(b"one\x0b\x0ctwo\nthree\rfour").unwrap(),
        vec![
            Bytes::from_static(b"one\x0b\x0ctwo"),
            Bytes::from_static(b"three"),
            Bytes::from_static(b"four"),
        ]
    );
}

#[test]
fn inline_hex_escape_and_escaped_quotes_are_decoded() {
    assert_eq!(
        parse_inline_args(b"\"a\\x00\\xFF\\\"\" 'it\\'s'").unwrap(),
        vec![
            Bytes::from_static(b"a\0\xff\""),
            Bytes::from_static(b"it's"),
        ]
    );
}

#[test]
fn inline_malformed_quoted_input_returns_protocol_error() {
    assert!(parse_inline_args(b"\"unterminated").is_err());
    assert!(parse_inline_args(b"\"trailing\\").is_err());
}
#[test]
fn inline_unbalanced_quotes() {
    let mut p = Parser::new();
    p.feed(b"set foo 'b'a'r\r\n");
    assert!(p.try_parse().is_err());
}
#[test]
fn mbulk_zero_silent_discard() {
    let mut p = Parser::new();
    p.feed(b"*0\r\nPING\r\n");
    assert_eq!(
        p.try_parse().unwrap(),
        Some(vec![Bytes::from_static(b"PING")])
    );
}
#[test]
fn mbulk_negative_silent_discard() {
    let mut p = Parser::new();
    p.feed(b"*-1\r\nPING\r\n");
    assert_eq!(
        p.try_parse().unwrap(),
        Some(vec![Bytes::from_static(b"PING")])
    );
}

#[test]
fn parser_reset_and_metadata() {
    let mut p = Parser::new();
    p.feed(b"PING\r\n");
    assert!(p.has_pending_data());
    let _ = p.try_parse().unwrap();
    assert!(!p.has_pending_data());
    assert_eq!(p.last_cmd_len(), 6);

    p.feed(b"SET k v\r\n");
    let _ = p.try_parse().unwrap();
    assert_eq!(p.last_cmd_len(), 9);

    p.reset();
    assert!(!p.has_pending_data());
    assert_eq!(p.last_cmd_len(), 0);
}

#[test]
fn undecoded_len_covers_fresh_partial_and_completed_states() {
    let mut parser = Parser::new();
    assert_eq!(parser.undecoded_len(), 0);

    let partial = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nva";
    parser.feed(partial);
    assert!(parser.try_parse().unwrap().is_none());
    assert_eq!(parser.undecoded_len(), partial.len());
    assert!(parser.undecoded_len() >= parser.pending_len());

    parser.feed(b"lue\r\n");
    assert!(parser.try_parse().unwrap().is_some());
    assert_eq!(parser.undecoded_len(), 0);
}

#[test]
fn undecoded_len_tracks_partial_command_after_buffer_compaction() {
    let payload = vec![b'x'; 8 * 1024];
    let mut completed = format!("*2\r\n$4\r\nECHO\r\n${}\r\n", payload.len()).into_bytes();
    completed.extend_from_slice(&payload);
    completed.extend_from_slice(b"\r\n");
    let partial = b"*2\r\n$4\r\nPING\r\n$4\r\nPO";
    completed.extend_from_slice(partial);

    let mut parser = Parser::new();
    parser.feed(&completed);
    assert!(parser.try_parse().unwrap().is_some());
    assert!(parser.try_parse().unwrap().is_none());

    assert_eq!(parser.undecoded_len(), partial.len());
    assert!(parser.undecoded_len() >= parser.pending_len());
}

#[test]
fn completed_large_request_releases_query_buffer_capacity() {
    let mut parser = Parser::new();
    let value = vec![b'x'; 128 * 1024];
    let mut request = format!("*2\r\n$3\r\nSET\r\n${}\r\n", value.len()).into_bytes();
    request.extend_from_slice(&value);
    request.extend_from_slice(b"\r\n");

    assert!(parser.try_parse().unwrap().is_none());
    parser.feed(&request);
    assert!(parser.try_parse().unwrap().is_some());

    assert!(
        parser.buffer_capacity() <= 16 * 1024,
        "completed requests must not retain their peak query-buffer allocation"
    );
}

#[test]
fn incomplete_multibulk_accounts_consumed_query_and_partial_argv() {
    let mut parser = Parser::new();
    let mut request = b"*250\r\n".to_vec();
    for _ in 0..249 {
        request.extend_from_slice(b"$1000\r\n");
        request.extend_from_slice(&[b'x'; 1000]);
        request.extend_from_slice(b"\r\n");
    }

    parser.feed(&request);
    assert!(parser.try_parse().unwrap().is_none());
    assert_eq!(parser.pending_len(), 0);
    assert!(parser.has_inflight_request());

    let usage = parser.inflight_memory_usage();
    assert!(usage.query_buffer >= request.len());
    assert!(usage.partial_argv >= 249 * 1000);
    assert_eq!(usage.total(), usage.query_buffer + usage.partial_argv);

    parser.shrink_idle_buffer();
    assert!(parser.has_inflight_request());
    assert_eq!(parser.inflight_memory_usage(), usage);
}

#[test]
fn try_parse_limited_rejects_over_limit_multibulk_and_bulk() {
    let mut p = Parser::new();
    p.feed(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n");
    assert!(p.try_parse_limited(2, 0).is_err());

    let mut p = Parser::new();
    p.feed(b"*1\r\n$100\r\n");
    // 100-byte bulk over limit=10 → error
    assert!(p.try_parse_limited(0, 10).is_err());
}

#[test]
fn invalid_crlf_and_bulk_length_errors() {
    let mut p = Parser::new();
    p.feed(b"PING\rX\n");
    assert!(p.try_parse().is_err());

    let mut p = Parser::new();
    p.feed(b"*abc\r\n");
    assert!(p.try_parse().is_err());

    let mut p = Parser::new();
    p.feed(b"*1\r\n$abc\r\n");
    assert!(p.try_parse().is_err());

    let mut p = Parser::new();
    p.feed(b"*1\r\n$-1\r\n");
    assert!(p.try_parse().is_err());
}

#[test]
fn inline_escape_sequences_are_processed() {
    let mut p = Parser::new();
    p.feed(b"\"a\\nb\"\r\n");
    let r = p.try_parse().unwrap().unwrap();
    assert_eq!(r[0], Bytes::from_static(b"a\nb"));
}
