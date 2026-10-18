use super::slowlog::SlowLog;

pub const CAT_SLOW: &str = "slow";
pub const CAT_LARGE_REQUEST: &str = "large-request";
pub const CAT_LARGE_REPLY: &str = "large-reply";

pub struct CommandLog {
    pub slow: SlowLog,
    pub large_request: SlowLog,
    pub large_reply: SlowLog,
}

impl CommandLog {
    pub fn new(
        slow_thr: i64,
        slow_max: i64,
        req_thr: i64,
        req_max: i64,
        rep_thr: i64,
        rep_max: i64,
    ) -> Self {
        Self {
            slow: SlowLog::new(slow_thr, slow_max),
            large_request: SlowLog::new(req_thr, req_max),
            large_reply: SlowLog::new(rep_thr, rep_max),
        }
    }

    pub fn category(&mut self, cat: &str) -> Option<&mut SlowLog> {
        match cat {
            "slow" => Some(&mut self.slow),
            "large-request" => Some(&mut self.large_request),
            "large-reply" => Some(&mut self.large_reply),
            _ => None,
        }
    }

    pub fn reconfigure_all(
        &mut self,
        slow_thr: i64,
        slow_max: i64,
        req_thr: i64,
        req_max: i64,
        rep_thr: i64,
        rep_max: i64,
    ) {
        self.slow.reconfigure(slow_thr, slow_max);
        self.large_request.reconfigure(req_thr, req_max);
        self.large_reply.reconfigure(rep_thr, rep_max);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn categories_return_the_expected_log_and_unknown_returns_none() {
        let mut log = CommandLog::new(0, 10, 0, 10, 0, 10);

        log.category(CAT_SLOW).unwrap().push(
            vec![Bytes::from_static(b"SLOW")],
            1,
            "a".to_string(),
            None,
        );
        log.category(CAT_LARGE_REQUEST).unwrap().push(
            vec![Bytes::from_static(b"REQ")],
            2,
            "b".to_string(),
            None,
        );
        log.category(CAT_LARGE_REPLY).unwrap().push(
            vec![Bytes::from_static(b"REP")],
            3,
            "c".to_string(),
            None,
        );

        assert_eq!(log.slow.len(), 1);
        assert_eq!(log.large_request.len(), 1);
        assert_eq!(log.large_reply.len(), 1);
        assert!(log.category("unknown").is_none());
    }

    #[test]
    fn reconfigure_all_trims_each_category() {
        let mut log = CommandLog::new(0, 4, 0, 4, 0, 4);
        for i in 0..4 {
            let arg = Bytes::from(i.to_string());
            log.slow.push(vec![arg.clone()], 1, "a".to_string(), None);
            log.large_request
                .push(vec![arg.clone()], 1, "b".to_string(), None);
            log.large_reply.push(vec![arg], 1, "c".to_string(), None);
        }

        log.reconfigure_all(10, 1, 20, 2, 30, 3);

        assert_eq!(log.slow.threshold_us, 10);
        assert_eq!(log.large_request.threshold_us, 20);
        assert_eq!(log.large_reply.threshold_us, 30);
        assert_eq!(log.slow.len(), 1);
        assert_eq!(log.large_request.len(), 2);
        assert_eq!(log.large_reply.len(), 3);
    }
}
