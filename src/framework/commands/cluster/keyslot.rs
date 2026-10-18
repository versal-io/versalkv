use crate::engine::command::context::CommandConn;
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::CommandError;
use bytes::Bytes;

/// Parse a slot argument, returning the RESP error on failure.
fn parse_slot_arg(arg: &Bytes) -> Result<usize, Value> {
    let s = std::str::from_utf8(arg)
        .ok()
        .and_then(|s| s.parse::<i64>().ok());
    match s {
        Some(v) if (0..16384).contains(&v) => Ok(v as usize),
        _ => Err(Value::error("ERR Invalid slot")),
    }
}
/// CLUSTER COUNTKEYSINSLOT <slot>: number of local keys hashing to `slot`.
pub(super) fn cluster_countkeysinslot(
    args: &[Bytes],
    conn: &dyn CommandConn,
    ctx: &ServerContext,
) -> Value {
    if args.len() != 1 {
        return CommandError::wrong_arity("cluster");
    }
    let slot = match parse_slot_arg(&args[0]) {
        Ok(s) => s,
        Err(e) => return e,
    };
    let count = ctx
        .db(conn.selected_db())
        .map(|db| db.scan_slot_key_count(slot))
        .unwrap_or_default()
        .saturating_add(ctx.slot_import_key_count(conn.selected_db(), slot));
    Value::integer(count as i64)
}

/// CLUSTER GETKEYSINSLOT <slot> <count>: up to `count` local keys in `slot`,
/// returned in sorted order (aligns with C, which walks the slot's key index).
pub(super) fn cluster_getkeysinslot(
    args: &[Bytes],
    conn: &dyn CommandConn,
    ctx: &ServerContext,
) -> Value {
    if args.len() != 2 {
        return CommandError::wrong_arity("cluster");
    }
    let slot = match parse_slot_arg(&args[0]) {
        Ok(s) => s,
        Err(e) => return e,
    };
    let count = match std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
    {
        Some(v) if v >= 0 => v as usize,
        _ => return Value::error("ERR Invalid count"),
    };
    let mut keys = ctx
        .db(conn.selected_db())
        .map(|db| db.get_keys_in_slot(slot, count))
        .unwrap_or_default();
    let mut import_keys = ctx.slot_import_keys(conn.selected_db(), slot);
    keys.append(&mut import_keys);
    keys.sort_unstable();
    keys.truncate(count);
    Value::Array(
        keys.into_iter()
            .map(|k| Value::bulk(Bytes::from(k)))
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::distributed::cluster::node::key_hash_slot;
    use crate::engine::command::context::CommandConn;
    use crate::foundation::io::store::Robj;
    use crate::framework::capabilities::connection::Connection;

    #[test]
    fn cluster_keyslot_commands_use_selected_database() {
        let mut context =
            ServerContext::new_with_slot_router(2, 14, |key| key_hash_slot(key) as usize);
        let mut connection = Connection::new_fake();
        let first = b"{shared}a".to_vec();
        let second = b"{shared}b".to_vec();
        let slot = key_hash_slot(&first);
        context
            .db_mut(0)
            .unwrap()
            .set(first.clone(), Robj::string("zero"));
        context
            .db_mut(1)
            .unwrap()
            .set(second.clone(), Robj::string("one"));
        let slot_arg = Bytes::from(slot.to_string());

        assert_eq!(
            cluster_countkeysinslot(
                std::slice::from_ref(&slot_arg),
                &connection as &dyn CommandConn,
                &context,
            ),
            Value::integer(1)
        );
        assert_eq!(
            cluster_getkeysinslot(
                &[slot_arg, Bytes::from_static(b"2")],
                &connection as &dyn CommandConn,
                &context,
            ),
            Value::Array(vec![Value::bulk(first)])
        );

        connection.selected_db = 1;
        assert_eq!(
            cluster_getkeysinslot(
                &[Bytes::from(slot.to_string()), Bytes::from_static(b"2")],
                &connection as &dyn CommandConn,
                &context,
            ),
            Value::Array(vec![Value::bulk(second)])
        );
    }
}
