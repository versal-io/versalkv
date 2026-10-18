use crate::framework::CommandArgs;
use crate::engine::extension::acl::{now_ms, reason_str, context_str, AclLogEntry};
use crate::foundation::io::protocol::Value;
use bytes::Bytes;

fn acl_log_entries() -> std::sync::MutexGuard<'static, Vec<AclLogEntry>> {
    crate::engine::extension::acl::acl_log().lock().unwrap()
}

pub fn acl_log_list(args: &CommandArgs) -> Value {
    let log_guard = acl_log_entries();
    let count = if args.args.len() >= 2 {
        let sub = std::str::from_utf8(&args.args[1]).unwrap_or("");
        if sub.eq_ignore_ascii_case("reset") {
            drop(log_guard);
            crate::engine::extension::acl::reset_acl_log();
            return Value::ok();
        }
        args.parse::<usize>(1).unwrap_or(10)
    } else {
        10
    };
    let count = count.min(log_guard.len());
    let entries: Vec<AclLogEntry> = log_guard.iter().take(count).cloned().collect();
    drop(log_guard);
    let now = now_ms();
    let mut result = Vec::with_capacity(entries.len());
    for entry in &entries {
        let age_sec = (now - entry.ctime) as f64 / 1000.0;
        let fields = vec![
            Value::bulk(Bytes::from_static(b"count")),
            Value::integer(entry.count as i64),
            Value::bulk(Bytes::from_static(b"reason")),
            Value::bulk(Bytes::from(reason_str(entry.reason))),
            Value::bulk(Bytes::from_static(b"context")),
            Value::bulk(Bytes::from(context_str(entry.context))),
            Value::bulk(Bytes::from_static(b"object")),
            Value::bulk(Bytes::from(entry.object.clone())),
            Value::bulk(Bytes::from_static(b"username")),
            Value::bulk(Bytes::from(entry.username.clone())),
            Value::bulk(Bytes::from_static(b"age-seconds")),
            Value::double(age_sec),
            Value::bulk(Bytes::from_static(b"client-info")),
            Value::bulk(Bytes::from(entry.client_info.clone())),
            Value::bulk(Bytes::from_static(b"entry-id")),
            Value::integer(entry.entry_id as i64),
            Value::bulk(Bytes::from_static(b"timestamp-created")),
            Value::integer(entry.timestamp_created),
            Value::bulk(Bytes::from_static(b"timestamp-last-updated")),
            Value::integer(entry.ctime),
        ];
        result.push(Value::Array(fields));
    }
    Value::Array(result)
}
