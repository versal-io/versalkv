use crate::engine::command::context::CommandConn;
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::observability::keyspace::{self, NOTIFY_GENERIC};
use crate::framework::CommandError;
use bytes::Bytes;

pub(super) fn cluster_flushslot(
    args: &[Bytes],
    conn: &mut dyn CommandConn,
    ctx: &mut ServerContext,
) -> Value {
    if !(1..=2).contains(&args.len()) {
        return CommandError::wrong_arity("cluster|flushslot");
    }
    let slot = match std::str::from_utf8(&args[0])
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
    {
        Some(slot) if (0..16_384).contains(&slot) => slot as usize,
        _ => return Value::error("ERR Invalid or out of range slot"),
    };
    if args.get(1).is_some_and(|mode| {
        !mode.eq_ignore_ascii_case(b"sync") && !mode.eq_ignore_ascii_case(b"async")
    }) {
        return CommandError::syntax_error();
    }

    let deleted = ctx.take_slot_entries(slot);
    for entry in &deleted {
        conn.record_dirty_key((entry.db_id, Bytes::copy_from_slice(&entry.key)));
    }
    for entry in &deleted {
        keyspace::notify_keyspace_event(conn, ctx, NOTIFY_GENERIC, "del", entry.db_id, &entry.key);
    }
    ctx.slot_metrics_mut().reset_slot(slot);
    Value::ok()
}
