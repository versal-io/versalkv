use std::fs;
use std::path::Path;

#[derive(Debug, Clone)]
pub enum TestEvent {
    ReplyPong,
    ReplyOk,
    ReplyFullResync,
    SendRdbBulk(usize),
    SendRdbComplete,
    SendError(String),
    WaitTimeout(u64),
    #[allow(dead_code)]
    AssertState(String),
}

#[derive(Debug, Clone)]
pub struct TestSequence {
    pub name: String,
    pub events: Vec<TestEvent>,
}

pub fn load_test_sequences(path: &str) -> Vec<TestSequence> {
    let content = fs::read_to_string(Path::new(path)).unwrap_or_else(|_| String::new());
    if content.trim().is_empty() {
        return builtin_sequences();
    }
    builtin_sequences()
}

fn builtin_sequences() -> Vec<TestSequence> {
    vec![
        TestSequence {
            name: "normal_path".into(),
            events: vec![
                TestEvent::ReplyPong,
                TestEvent::ReplyOk,
                TestEvent::ReplyOk,
                TestEvent::ReplyFullResync,
                TestEvent::SendRdbBulk(100),
                TestEvent::SendRdbComplete,
            ],
        },
        TestSequence {
            name: "timeout_in_wait_pong".into(),
            events: vec![TestEvent::WaitTimeout(1000)],
        },
        TestSequence {
            name: "error_in_replconf_port".into(),
            events: vec![
                TestEvent::ReplyPong,
                TestEvent::SendError("ERR unknown REPLCONF option".into()),
            ],
        },
    ]
}

pub fn event_to_resp(event: &TestEvent) -> Vec<u8> {
    match event {
        TestEvent::ReplyPong => encode_simple_string("PONG"),
        TestEvent::ReplyOk => encode_simple_string("OK"),
        TestEvent::ReplyFullResync => {
            let mut buf = b"+FULLRESYNC ".to_vec();
            buf.extend_from_slice(b"0123456789abcdef0123456789abcdef01234567");
            buf.extend_from_slice(b" 0\r\n");
            buf
        }
        TestEvent::SendRdbBulk(size) => encode_rdb_bulk_header(*size),
        TestEvent::SendRdbComplete => encode_rdb_eof(),
        TestEvent::SendError(msg) => encode_error(msg),
        TestEvent::WaitTimeout(_ms) => Vec::new(),
        TestEvent::AssertState(_) => Vec::new(),
    }
}

fn encode_simple_string(s: &str) -> Vec<u8> {
    format!("+{}\r\n", s).into_bytes()
}

fn encode_error(msg: &str) -> Vec<u8> {
    format!("-{}\r\n", msg).into_bytes()
}

fn encode_rdb_bulk_header(size: usize) -> Vec<u8> {
    format!("${}\r\n", size).into_bytes()
}

fn encode_rdb_eof() -> Vec<u8> {
    b"$EOF:\r\n".to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_builtin_sequences_loaded_when_file_empty() {
        let sequences = load_test_sequences("/dev/null");
        assert_eq!(sequences.len(), 3);
        assert_eq!(sequences[0].name, "normal_path");
        assert_eq!(sequences[1].name, "timeout_in_wait_pong");
        assert_eq!(sequences[2].name, "error_in_replconf_port");
    }

    #[test]
    fn test_builtin_sequences_loaded_when_file_missing() {
        let sequences = load_test_sequences("/nonexistent/path.xml");
        assert_eq!(sequences.len(), 3);
    }

    #[test]
    fn test_normal_path_events() {
        let seq = &builtin_sequences()[0];
        assert_eq!(seq.events.len(), 6);

        // Verify first event is ReplyPong
        assert!(matches!(seq.events[0], TestEvent::ReplyPong));
        // Verify ReplyOk follows
        assert!(matches!(seq.events[1], TestEvent::ReplyOk));
        // Verify final event is SendRdbComplete
        assert!(matches!(seq.events[5], TestEvent::SendRdbComplete));
    }

    #[test]
    fn test_timeout_path_events() {
        let seq = &builtin_sequences()[1];
        assert_eq!(seq.events.len(), 1);
        assert!(matches!(seq.events[0], TestEvent::WaitTimeout(1000)));
    }

    #[test]
    fn test_error_path_events() {
        let seq = &builtin_sequences()[2];
        assert_eq!(seq.events.len(), 2);
        assert!(matches!(seq.events[0], TestEvent::ReplyPong));
        assert!(matches!(seq.events[1], TestEvent::SendError(_)));
    }

    #[test]
    fn test_reply_pong_resp() {
        let resp = event_to_resp(&TestEvent::ReplyPong);
        assert_eq!(resp, b"+PONG\r\n");
    }

    #[test]
    fn test_reply_ok_resp() {
        let resp = event_to_resp(&TestEvent::ReplyOk);
        assert_eq!(resp, b"+OK\r\n");
    }

    #[test]
    fn test_reply_fullresync_resp() {
        let resp = event_to_resp(&TestEvent::ReplyFullResync);
        let expected = b"+FULLRESYNC 0123456789abcdef0123456789abcdef01234567 0\r\n";
        assert_eq!(resp, expected);
    }

    #[test]
    fn test_rdb_bulk_header_resp() {
        let resp = event_to_resp(&TestEvent::SendRdbBulk(100));
        assert_eq!(resp, b"$100\r\n");
    }

    #[test]
    fn test_rdb_complete_resp() {
        let resp = event_to_resp(&TestEvent::SendRdbComplete);
        assert_eq!(resp, b"$EOF:\r\n");
    }

    #[test]
    fn test_send_error_resp() {
        let resp = event_to_resp(&TestEvent::SendError("ERR bad command".into()));
        assert_eq!(resp, b"-ERR bad command\r\n");
    }

    #[test]
    fn test_wait_timeout_resp_is_empty() {
        let resp = event_to_resp(&TestEvent::WaitTimeout(5000));
        assert!(resp.is_empty());
    }

    #[test]
    fn test_assert_state_resp_is_empty() {
        let resp = event_to_resp(&TestEvent::AssertState("connected".into()));
        assert!(resp.is_empty());
    }
}
