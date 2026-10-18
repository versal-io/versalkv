use bytes::Bytes;
use std::collections::VecDeque;

const SLOWLOG_ENTRY_MAX_ARGC: usize = 32;
const SLOWLOG_ENTRY_MAX_STRING: usize = 128;

#[derive(Clone, Debug)]
pub struct SlowLogEntry {
    pub id: u64,
    pub ts_sec: u64,
    pub duration: i64,
    pub argv: Vec<Bytes>,
    pub client_addr: String,
    pub client_name: Option<Bytes>,
}

pub struct SlowLog {
    entries: VecDeque<SlowLogEntry>,
    next_id: u64,
    pub threshold_us: i64,
    pub max_len: i64,
}

impl SlowLog {
    pub fn new(threshold_us: i64, max_len: i64) -> Self {
        Self {
            entries: VecDeque::new(),
            next_id: 0,
            threshold_us,
            max_len,
        }
    }

    pub fn reconfigure(&mut self, threshold_us: i64, max_len: i64) {
        self.threshold_us = threshold_us;
        self.max_len = max_len;
        while self.max_len >= 0 && (self.entries.len() as i64) > self.max_len {
            self.entries.pop_front();
        }
    }

    pub fn push(
        &mut self,
        argv: Vec<Bytes>,
        duration_or_bytes: i64,
        client_addr: String,
        client_name: Option<Bytes>,
    ) {
        if self.max_len == 0 {
            return;
        }
        if self.threshold_us < 0 {
            return;
        }
        if self.threshold_us > 0 && duration_or_bytes < self.threshold_us {
            return;
        }
        let redacted = redact_sensitive(&argv);
        let truncated = truncate_argv(&redacted);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        let id = self.next_id;
        self.next_id += 1;
        let entry = SlowLogEntry {
            id,
            ts_sec: now.as_secs(),
            duration: duration_or_bytes,
            argv: truncated,
            client_addr,
            client_name,
        };
        self.entries.push_back(entry);
        while (self.entries.len() as i64) > self.max_len {
            self.entries.pop_front();
        }
    }

    pub fn get(&self, count: i64) -> Vec<SlowLogEntry> {
        if count == 0 {
            return Vec::new();
        }
        let n = if count < 0 {
            self.entries.len()
        } else {
            (count as usize).min(self.entries.len())
        };
        if n == 0 {
            return Vec::new();
        }
        let skip = self.entries.len() - n;
        self.entries.iter().skip(skip).rev().cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn reset(&mut self) {
        self.entries.clear();
    }
}

fn truncate_argv(argv: &[Bytes]) -> Vec<Bytes> {
    let argc = argv.len();
    if argc <= SLOWLOG_ENTRY_MAX_ARGC {
        let mut out: Vec<Bytes> = Vec::with_capacity(argc);
        for a in argv.iter() {
            if a.len() > SLOWLOG_ENTRY_MAX_STRING {
                let prefix = a.slice(0..SLOWLOG_ENTRY_MAX_STRING);
                let msg = format!("... ({} more bytes)", a.len() - SLOWLOG_ENTRY_MAX_STRING);
                let mut combined = Vec::with_capacity(SLOWLOG_ENTRY_MAX_STRING + msg.len());
                combined.extend_from_slice(&prefix);
                combined.extend_from_slice(msg.as_bytes());
                out.push(Bytes::from(combined));
            } else {
                out.push(a.clone());
            }
        }
        return out;
    }
    let mut out: Vec<Bytes> = Vec::with_capacity(SLOWLOG_ENTRY_MAX_ARGC);
    for (i, a) in argv.iter().enumerate().take(SLOWLOG_ENTRY_MAX_ARGC) {
        if i == SLOWLOG_ENTRY_MAX_ARGC - 1 {
            let more = argc - SLOWLOG_ENTRY_MAX_ARGC + 1;
            let msg = format!("... ({} more arguments)", more);
            out.push(Bytes::from(msg.into_bytes()));
            break;
        }
        if a.len() > SLOWLOG_ENTRY_MAX_STRING {
            let prefix = a.slice(0..SLOWLOG_ENTRY_MAX_STRING);
            let msg = format!("... ({} more bytes)", a.len() - SLOWLOG_ENTRY_MAX_STRING);
            let mut combined = Vec::with_capacity(SLOWLOG_ENTRY_MAX_STRING + msg.len());
            combined.extend_from_slice(&prefix);
            combined.extend_from_slice(msg.as_bytes());
            out.push(Bytes::from(combined));
        } else {
            out.push(a.clone());
        }
    }
    out
}

fn redact_sensitive(argv: &[Bytes]) -> Vec<Bytes> {
    let mut out: Vec<Bytes> = Vec::with_capacity(argv.len());
    if argv.is_empty() {
        return out;
    }
    let cmd = String::from_utf8_lossy(&argv[0]).to_ascii_lowercase();
    out.push(argv[0].clone());

    let is_sensitive = |key: &[u8]| -> bool {
        let k = String::from_utf8_lossy(key).to_ascii_lowercase();
        matches!(
            k.as_str(),
            "requirepass"
                | "primaryauth"
                | "masterauth"
                | "primaryuser"
                | "tls-key-file-pass"
                | "tls-client-key-file-pass"
                | "tls-client-key-file-password"
                | "tls-key-file-password"
        )
    };

    match cmd.as_str() {
        "auth" => {
            for _ in 1..argv.len() {
                out.push(Bytes::from_static(b"(redacted)"));
            }
        }
        "hello" => {
            let mut i = 1;
            while i < argv.len() {
                let sub = String::from_utf8_lossy(&argv[i]).to_ascii_lowercase();
                if sub == "auth" && i + 2 < argv.len() {
                    out.push(argv[i].clone());
                    out.push(argv[i + 1].clone());
                    out.push(Bytes::from_static(b"(redacted)"));
                    i += 3;
                } else {
                    out.push(argv[i].clone());
                    i += 1;
                }
            }
        }
        "config" => {
            if argv.len() >= 2 {
                let sub = String::from_utf8_lossy(&argv[1]).to_ascii_lowercase();
                if (sub == "set" || sub == "setname") && argv.len() >= 4 {
                    out.push(argv[1].clone());
                    let mut i = 2;
                    while i + 1 < argv.len() {
                        out.push(argv[i].clone());
                        if is_sensitive(&argv[i]) {
                            out.push(Bytes::from_static(b"(redacted)"));
                        } else {
                            out.push(argv[i + 1].clone());
                        }
                        i += 2;
                    }
                    return out;
                }
            }
            for a in argv.iter().skip(1) {
                out.push(a.clone());
            }
        }
        "migrate" => {
            let mut i = 1;
            while i < argv.len() {
                let sub = String::from_utf8_lossy(&argv[i]).to_ascii_lowercase();
                if (sub == "auth" || sub == "auth2") && i + 1 < argv.len() {
                    out.push(argv[i].clone());
                    if sub == "auth2" && i + 2 < argv.len() {
                        out.push(Bytes::from_static(b"(redacted)"));
                        out.push(Bytes::from_static(b"(redacted)"));
                        i += 3;
                    } else {
                        out.push(Bytes::from_static(b"(redacted)"));
                        i += 2;
                    }
                } else {
                    out.push(argv[i].clone());
                    i += 1;
                }
            }
        }
        "acl" => {
            if argv.len() >= 2 {
                let sub = String::from_utf8_lossy(&argv[1]).to_ascii_lowercase();
                match sub.as_str() {
                    "setuser" => {
                        out.push(argv[1].clone());
                        for _ in 2..argv.len() {
                            out.push(Bytes::from_static(b"(redacted)"));
                        }
                    }
                    "getuser" => {
                        out.push(argv[1].clone());
                        for a in argv.iter().skip(2) {
                            let _ = a;
                            out.push(Bytes::from_static(b"(redacted)"));
                        }
                    }
                    "deluser" => {
                        out.push(argv[1].clone());
                        for _ in 2..argv.len() {
                            out.push(Bytes::from_static(b"(redacted)"));
                        }
                    }
                    _ => {
                        for a in argv.iter().skip(1) {
                            out.push(a.clone());
                        }
                    }
                }
            }
        }
        _ => {
            for a in argv.iter().skip(1) {
                out.push(a.clone());
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redact_auth() {
        let argv = vec![Bytes::from_static(b"AUTH"), Bytes::from_static(b"secret")];
        let r = redact_sensitive(&argv);
        assert_eq!(r[1], Bytes::from_static(b"(redacted)"));
    }

    #[test]
    fn truncate_long_args() {
        let mut argv: Vec<Bytes> = vec![Bytes::from_static(b"SET")];
        for _ in 0..35 {
            argv.push(Bytes::from_static(b"v"));
        }
        let t = truncate_argv(&argv);
        assert!(t.len() <= 33);
        assert!(!t.last().unwrap().is_empty());
    }

    #[test]
    fn push_respects_threshold_disable_and_max_len() {
        let mut log = SlowLog::new(10, 2);
        log.push(vec![Bytes::from_static(b"GET")], 5, "a".to_string(), None);
        assert_eq!(log.len(), 0);

        log.push(
            vec![Bytes::from_static(b"GET"), Bytes::from_static(b"k1")],
            10,
            "a".to_string(),
            Some(Bytes::from_static(b"client-a")),
        );
        log.push(vec![Bytes::from_static(b"GET")], 11, "b".to_string(), None);
        log.push(vec![Bytes::from_static(b"SET")], 12, "c".to_string(), None);

        assert_eq!(log.len(), 2);
        let entries = log.get(-1);
        assert_eq!(entries[0].id, 2);
        assert_eq!(entries[0].client_addr, "c");
        assert_eq!(entries[1].id, 1);
        assert_eq!(entries[1].client_addr, "b");

        log.reconfigure(-1, 2);
        log.push(vec![Bytes::from_static(b"GET")], 100, "d".to_string(), None);
        assert_eq!(log.len(), 2);
        log.reconfigure(0, 0);
        assert_eq!(log.len(), 0);
        log.push(vec![Bytes::from_static(b"GET")], 100, "e".to_string(), None);
        assert_eq!(log.len(), 0);
    }

    #[test]
    fn get_zero_limited_and_reset_behave_like_slowlog_command() {
        let mut log = SlowLog::new(0, 10);
        log.push(vec![Bytes::from_static(b"GET")], 1, "a".to_string(), None);
        log.push(vec![Bytes::from_static(b"SET")], 2, "b".to_string(), None);

        assert!(log.get(0).is_empty());
        let newest = log.get(1);
        assert_eq!(newest.len(), 1);
        assert_eq!(newest[0].argv[0], Bytes::from_static(b"SET"));
        let all = log.get(-1);
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].argv[0], Bytes::from_static(b"SET"));
        assert_eq!(all[1].argv[0], Bytes::from_static(b"GET"));

        log.reset();
        assert_eq!(log.len(), 0);
        assert!(log.get(-1).is_empty());
    }

    #[test]
    fn truncate_long_bulk_argument_and_many_arguments() {
        let long = Bytes::from(vec![b'x'; SLOWLOG_ENTRY_MAX_STRING + 5]);
        let truncated = truncate_argv(&[Bytes::from_static(b"SET"), long]);

        assert_eq!(truncated.len(), 2);
        let rendered = String::from_utf8_lossy(&truncated[1]);
        assert!(rendered.starts_with(&"x".repeat(SLOWLOG_ENTRY_MAX_STRING)));
        assert!(rendered.ends_with("... (5 more bytes)"));

        let mut many = vec![Bytes::from_static(b"MSET")];
        for i in 0..40 {
            many.push(Bytes::from(i.to_string()));
        }
        let truncated = truncate_argv(&many);
        assert_eq!(truncated.len(), SLOWLOG_ENTRY_MAX_ARGC);
        assert!(String::from_utf8_lossy(truncated.last().unwrap()).contains("more arguments"));
    }

    #[test]
    fn redact_sensitive_variants_for_hello_config_migrate_and_acl() {
        let hello = redact_sensitive(&[
            Bytes::from_static(b"HELLO"),
            Bytes::from_static(b"3"),
            Bytes::from_static(b"AUTH"),
            Bytes::from_static(b"default"),
            Bytes::from_static(b"secret"),
        ]);
        assert_eq!(hello[4], Bytes::from_static(b"(redacted)"));

        let config = redact_sensitive(&[
            Bytes::from_static(b"CONFIG"),
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"requirepass"),
            Bytes::from_static(b"secret"),
            Bytes::from_static(b"maxmemory"),
            Bytes::from_static(b"1"),
        ]);
        assert_eq!(config[3], Bytes::from_static(b"(redacted)"));
        assert_eq!(config[5], Bytes::from_static(b"1"));

        let migrate = redact_sensitive(&[
            Bytes::from_static(b"MIGRATE"),
            Bytes::from_static(b"127.0.0.1"),
            Bytes::from_static(b"6379"),
            Bytes::from_static(b""),
            Bytes::from_static(b"0"),
            Bytes::from_static(b"1000"),
            Bytes::from_static(b"AUTH2"),
            Bytes::from_static(b"user"),
            Bytes::from_static(b"pass"),
        ]);
        assert_eq!(migrate[7], Bytes::from_static(b"(redacted)"));
        assert_eq!(migrate[8], Bytes::from_static(b"(redacted)"));

        let acl = redact_sensitive(&[
            Bytes::from_static(b"ACL"),
            Bytes::from_static(b"SETUSER"),
            Bytes::from_static(b"default"),
            Bytes::from_static(b">secret"),
        ]);
        assert_eq!(acl[2], Bytes::from_static(b"(redacted)"));
        assert_eq!(acl[3], Bytes::from_static(b"(redacted)"));
    }
}
