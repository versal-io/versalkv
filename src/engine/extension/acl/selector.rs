use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

// ──────────────────────────────────────────────
// ──────────────────────────────────────────────
pub(crate) const ACL_DENIED_DB: i32 = 1;
pub(crate) const ACL_DENIED_CMD: i32 = 2;
pub(crate) const ACL_DENIED_KEY: i32 = 3;
pub(crate) const ACL_DENIED_AUTH: i32 = 4;
pub(crate) const ACL_DENIED_CHANNEL: i32 = 5;
pub(crate) const ACL_DENIED_TLS_CERT: i32 = 6;

pub(crate) const ACL_LOG_CTX_TOPLEVEL: i32 = 0;
pub(crate) const ACL_LOG_CTX_LUA: i32 = 1;
pub(crate) const ACL_LOG_CTX_MULTI: i32 = 2;
pub(crate) const ACL_LOG_CTX_MODULE: i32 = 3;
pub(crate) const ACL_LOG_CTX_SCRIPT: i32 = 4;

pub(crate) static ACL_STATS_AUTH: AtomicU64 = AtomicU64::new(0);
pub(crate) static ACL_STATS_CMD: AtomicU64 = AtomicU64::new(0);
pub(crate) static ACL_STATS_KEY: AtomicU64 = AtomicU64::new(0);
pub(crate) static ACL_STATS_CHANNEL: AtomicU64 = AtomicU64::new(0);
pub(crate) static ACL_STATS_DB: AtomicU64 = AtomicU64::new(0);

const ACL_LOG_GROUPING_MAX_TIME_DELTA: i64 = 60000; // ms

pub(crate) static ACL_LOG_MAX_LEN: OnceLock<Mutex<usize>> = OnceLock::new();
fn acl_log_max_len() -> usize {
    *ACL_LOG_MAX_LEN
        .get_or_init(|| Mutex::new(128))
        .lock()
        .unwrap()
}
pub(crate) fn set_acl_log_max_len(len: usize) {
    *ACL_LOG_MAX_LEN
        .get_or_init(|| Mutex::new(128))
        .lock()
        .unwrap() = len;
}

#[derive(Clone)]
pub(crate) struct AclLogEntry {
    pub count: u64,
    pub reason: i32,
    pub context: i32,
    pub object: String,
    pub username: String,
    pub ctime: i64, // ms, last update time
    pub client_info: String,
    pub entry_id: u64,
    pub timestamp_created: i64, // ms
}

static ACL_LOG_ENTRY_COUNTER: OnceLock<Mutex<u64>> = OnceLock::new();
fn acl_log_entry_counter() -> &'static Mutex<u64> {
    ACL_LOG_ENTRY_COUNTER.get_or_init(|| Mutex::new(0))
}

pub(crate) fn acl_log() -> &'static Mutex<Vec<AclLogEntry>> {
    static LOG: OnceLock<Mutex<Vec<AclLogEntry>>> = OnceLock::new();
    LOG.get_or_init(|| Mutex::new(Vec::with_capacity(128)))
}

pub(crate) fn reason_str(reason: i32) -> &'static str {
    match reason {
        ACL_DENIED_DB => "database",
        ACL_DENIED_CMD => "command",
        ACL_DENIED_KEY => "key",
        ACL_DENIED_AUTH => "auth",
        ACL_DENIED_CHANNEL => "channel",
        ACL_DENIED_TLS_CERT => "tls-cert",
        _ => "unknown",
    }
}

pub(crate) fn context_str(context: i32) -> &'static str {
    match context {
        ACL_LOG_CTX_TOPLEVEL => "toplevel",
        ACL_LOG_CTX_LUA => "lua",
        ACL_LOG_CTX_MULTI => "multi",
        ACL_LOG_CTX_MODULE => "module",
        ACL_LOG_CTX_SCRIPT => "script",
        _ => "unknown",
    }
}

pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

pub(crate) fn add_acl_log_entry(
    reason: i32,
    context: i32,
    object: String,
    username: String,
    client_info: String,
) {
    // Increment ACL stats counters
    match reason {
        ACL_DENIED_AUTH => {
            ACL_STATS_AUTH.fetch_add(1, Ordering::Relaxed);
        }
        ACL_DENIED_CMD => {
            ACL_STATS_CMD.fetch_add(1, Ordering::Relaxed);
        }
        ACL_DENIED_KEY => {
            ACL_STATS_KEY.fetch_add(1, Ordering::Relaxed);
        }
        ACL_DENIED_CHANNEL => {
            ACL_STATS_CHANNEL.fetch_add(1, Ordering::Relaxed);
        }
        ACL_DENIED_DB => {
            ACL_STATS_DB.fetch_add(1, Ordering::Relaxed);
        }
        _ => {}
    }

    if acl_log_max_len() == 0 {
        acl_log().lock().unwrap().clear();
        return;
    }

    let mut log = acl_log().lock().unwrap();
    let now = now_ms();

    let toscan = 10.min(log.len());
    let mut match_idx = None;
    for i in 0..toscan {
        let entry = &log[i];
        if entry.reason == reason
            && entry.context == context
            && entry.object == object
            && entry.username == username
        {
            let delta = (now - entry.ctime).abs();
            if delta <= ACL_LOG_GROUPING_MAX_TIME_DELTA {
                match_idx = Some(i);
                break;
            }
        }
    }

    if let Some(idx) = match_idx {
        let mut entry = log.remove(idx);
        entry.count += 1;
        entry.ctime = now;
        entry.client_info = client_info;
        log.insert(0, entry);
    } else {
        let mut counter = acl_log_entry_counter().lock().unwrap();
        let entry_id = *counter;
        *counter += 1;

        let entry = AclLogEntry {
            count: 1,
            reason,
            context,
            object,
            username,
            ctime: now,
            client_info,
            entry_id,
            timestamp_created: now,
        };
        log.insert(0, entry);
        let max_len = acl_log_max_len();
        if log.len() > max_len {
            log.truncate(max_len);
        }
    }
}

pub(crate) fn reset_acl_log() {
    acl_log().lock().unwrap().clear();
}

pub(crate) fn reset_acl_stats() {
    for counter in [
        &ACL_STATS_AUTH,
        &ACL_STATS_CMD,
        &ACL_STATS_KEY,
        &ACL_STATS_CHANNEL,
        &ACL_STATS_DB,
    ] {
        counter.store(0, Ordering::Relaxed);
    }
}
