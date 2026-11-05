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
fn inline_line_endings_preserve_command_boundaries() {
    for ending in [b"\n".as_slice(), b"\r\n".as_slice()] {
        let mut parser = Parser::new();
        let mut request = b"ECHO hello".to_vec();
        request.extend_from_slice(ending);
        request.extend_from_slice(b"PING\r\nPING\n");
        parser.feed(&request);
        assert_eq!(
            parser.try_parse().unwrap(),
            Some(vec![
                Bytes::from_static(b"ECHO"),
                Bytes::from_static(b"hello"),
            ])
        );
        assert_eq!(parser.last_cmd_len(), 10 + ending.len());
        for expected_len in [6, 5] {
            assert_eq!(
                parser.try_parse().unwrap(),
                Some(vec![Bytes::from_static(b"PING")])
            );
            assert_eq!(parser.last_cmd_len(), expected_len);
        }
        assert_eq!(parser.try_parse().unwrap(), None);
        assert!(!parser.has_inflight_request());
    }
}

#[test]
fn inline_nul_is_rejected_only_after_its_line_is_complete() {
    for suffix in [b"\n".as_slice(), b"\r\n".as_slice()] {
        let mut parser = Parser::new();
        parser.feed(b"SET key value\0");
        assert_eq!(parser.try_parse().unwrap(), None);
        assert!(parser.has_inflight_request());
        if suffix == b"\r\n" {
            parser.feed(b"\r");
            assert_eq!(parser.try_parse().unwrap(), None);
        }
        parser.feed(b"\n");
        assert_eq!(
            parser.try_parse(),
            Err(ParseError::Protocol(
                "Protocol error: embedded NUL byte in inline request".into()
            ))
        );
        assert!(!parser.has_inflight_request());
        assert_eq!(parser.try_parse().unwrap(), None);
    }
}

#[test]
fn inline_nul_does_not_reject_preceding_mixed_lines() {
    for prefix in [b"PING\n".as_slice(), b"PING\r\n".as_slice()] {
        let mut parser = Parser::new();
        parser.feed(prefix);
        parser.feed(b"SET key value\0\nPING\r\n");
        assert_eq!(
            parser.try_parse().unwrap(),
            Some(vec![Bytes::from_static(b"PING")])
        );
        assert_eq!(parser.last_cmd_len(), prefix.len());
        assert_eq!(
            parser.try_parse(),
            Err(ParseError::Protocol(
                "Protocol error: embedded NUL byte in inline request".into()
            ))
        );
        assert!(!parser.has_inflight_request());
    }
}

#[test]
fn inline_lf_compaction_preserves_partial_next_line() {
    let mut parser = Parser::new();
    parser.feed(&b"PING\n".repeat(1000));
    parser.feed(b"ECHO tail\r");
    for _ in 0..1000 {
        assert_eq!(
            parser.try_parse().unwrap(),
            Some(vec![Bytes::from_static(b"PING")])
        );
    }
    assert_eq!(parser.try_parse().unwrap(), None);
    assert_eq!(parser.pending_len(), 10);
    parser.feed(b"\n");
    assert_eq!(
        parser.try_parse().unwrap(),
        Some(vec![
            Bytes::from_static(b"ECHO"),
            Bytes::from_static(b"tail")
        ])
    );
    assert_eq!(parser.last_cmd_len(), 11);
    assert!(!parser.has_inflight_request());
}

#[test]
fn inline_lf_does_not_relax_multibulk_crlf() {
    for request in [b"*1\n".as_slice(), b"*1\r\n$4\n".as_slice()] {
        let mut parser = Parser::new();
        parser.feed(request);
        assert_eq!(parser.try_parse().unwrap(), None);
        assert!(parser.has_inflight_request());
    }
    let mut parser = Parser::new();
    parser.feed(b"*1\r\n$4\r\nPING\nX");
    assert_eq!(
        parser.try_parse(),
        Err(ParseError::Protocol(
            "Protocol error: invalid CRLF in request".into()
        ))
    );

    let mut parser = Parser::new();
    parser.feed(b"PING\n*2\r\n$4\r\nECHO\r\n$7\r\nbar\0baz\r\nPING\n");
    assert_eq!(
        parser.try_parse().unwrap(),
        Some(vec![Bytes::from_static(b"PING")])
    );
    assert_eq!(
        parser.try_parse().unwrap(),
        Some(vec![
            Bytes::from_static(b"ECHO"),
            Bytes::from_static(b"bar\0baz"),
        ])
    );
    assert_eq!(
        parser.try_parse().unwrap(),
        Some(vec![Bytes::from_static(b"PING")])
    );
    assert_eq!(parser.try_parse().unwrap(), None);
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
fn multibulk_header_memory_is_bounded_by_initial_argument_limit() {
    for count in [1_000_000, i32::MAX] {
        let mut parser = Parser::new();
        parser.feed(format!("*{count}\r\n").as_bytes());

        assert!(parser
            .try_parse()
            .expect("valid multibulk header")
            .is_none());
        assert!(
            parser.inflight_memory_usage().partial_argv <= 1024 * std::mem::size_of::<Bytes>(),
            "a header without payload must not reserve the entire declared argument array"
        );
    }
}

#[test]
fn multibulk_arguments_grow_incrementally_across_reads() {
    let count = 2500;
    let mut parser = Parser::new();
    parser.feed(format!("*{count}\r\n").as_bytes());
    for index in 0..count {
        let argument = index.to_string();
        parser.feed(format!("${}\r\n{argument}\r\n", argument.len()).as_bytes());
        let result = parser.try_parse().expect("valid argument");
        if index + 1 == count {
            let arguments = result.expect("complete request");
            assert_eq!(arguments.len(), count);
            for (index, argument) in arguments.iter().enumerate() {
                assert_eq!(argument.as_ref(), index.to_string().as_bytes());
            }
        } else {
            assert!(result.is_none(), "incomplete request must remain buffered");
        }
    }
}

#[test]
fn invalid_crlf_and_bulk_length_errors() {
    let mut p = Parser::new();
    p.feed(b"*1\rX\n");
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
fn inline_internal_cr_remains_an_argument_separator() {
    let mut parser = Parser::new();
    parser.feed(b"PING\rX\n");
    assert_eq!(
        parser.try_parse().unwrap(),
        Some(vec![Bytes::from_static(b"PING"), Bytes::from_static(b"X")])
    );
}

#[test]
fn bulk_terminators_are_validated_at_every_length_and_packet_boundary() {
    for payload in [b"".as_slice(), b"a", b"ab"] {
        for terminator in [b"XX", b"\rX", b"X\n", b"\r\n"] {
            let mut request = format!("*2\r\n$4\r\nECHO\r\n${}\r\n", payload.len()).into_bytes();
            request.extend_from_slice(payload);
            request.extend_from_slice(terminator);
            for split in 0..=request.len() {
                let mut parser = Parser::new();
                parser.feed(&request[..split]);
                let first = parser.try_parse();
                let result = if split == request.len() {
                    first
                } else {
                    assert_eq!(first, Ok(None), "split at byte {split}");
                    parser.feed(&request[split..]);
                    parser.try_parse()
                };
                if terminator == b"\r\n" {
                    assert_eq!(
                        result,
                        Ok(Some(vec![
                            Bytes::from_static(b"ECHO"),
                            Bytes::copy_from_slice(payload)
                        ]))
                    );
                } else {
                    assert!(
                        matches!(result, Err(ParseError::Protocol(_))),
                        "invalid terminator accepted for length {} at split {split}",
                        payload.len()
                    );
                }
            }
        }
    }
}

#[test]
fn inline_escape_sequences_are_processed() {
    let mut p = Parser::new();
    p.feed(b"\"a\\nb\"\r\n");
    let r = p.try_parse().unwrap().unwrap();
    assert_eq!(r[0], Bytes::from_static(b"a\nb"));
}
