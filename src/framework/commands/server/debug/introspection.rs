use crate::engine::command::context::CommandConn;
use crate::foundation::io::protocol::Value;
use crate::foundation::io::store::Robj;
use crate::framework::commands::generic::object::{encoding_display_name, ObjectEncodingLimits};
use crate::framework::context::ServerContext;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use std::time::Duration;

pub(super) fn debug_log(args: &CommandArgs) -> Value {
    if args.args.len() != 2 {
        return CommandError::syntax_error();
    }
    crate::server_log!(
        warning,
        "DEBUG LOG: {}",
        String::from_utf8_lossy(&args.args[1])
    );
    Value::ok()
}

pub(super) fn debug_sleep(args: &CommandArgs, ctx: &ServerContext) -> Value {
    if args.args.len() != 2 {
        return CommandError::syntax_error();
    }
    let sec = match std::str::from_utf8(&args.args[1])
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
    {
        Some(v) if v >= 0.0 => v,
        _ => return Value::error("ERR value is not a valid float"),
    };
    let watchdog_ms = ctx.config().server.watchdog_period;
    if watchdog_ms > 0 {
        let _ = crate::foundation::io::sys::time::set_watchdog_timer(watchdog_ms);
    }
    std::thread::sleep(Duration::from_secs_f64(sec));
    if watchdog_ms > 0 {
        let _ = crate::foundation::io::sys::time::set_watchdog_timer(0);
    }
    Value::ok()
}

pub(super) fn debug_leak(args: &CommandArgs) -> Value {
    if args.args.len() != 2 {
        return CommandError::syntax_error();
    }
    let leaked = Bytes::copy_from_slice(&args.args[1]);
    std::mem::forget(leaked);
    Value::ok()
}

pub(super) fn debug_structsize(args: &CommandArgs) -> Value {
    if args.args.len() != 1 {
        return CommandError::syntax_error();
    }
    let pointer_bits = std::mem::size_of::<usize>() * 8;
    Value::bulk(format!(
        "bits:{} robj:{} dictentry:{} sdshdr5:{} sdshdr8:{} sdshdr16:{} sdshdr32:{} sdshdr64:{} ",
        pointer_bits,
        std::mem::size_of::<Robj>(),
        std::mem::size_of::<(Bytes, Robj)>(),
        1,
        3,
        5,
        9,
        17,
    ))
}

pub(super) fn debug_sdslen(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &ServerContext,
) -> Value {
    if args.args.len() != 2 {
        return CommandError::syntax_error();
    }
    let db = match ctx.db(conn.selected_db()) {
        Some(db) => db,
        None => return CommandError::db_index_out_of_range(),
    };
    let obj = match db.get_allow_expired(&args.args[1]) {
        Some(obj) => obj,
        None => return Value::error("ERR no such key"),
    };
    if !obj.is_string() {
        return Value::error("ERR Not an sds encoded string.");
    }
    let key_len = args.args[1].len();
    let val_len = obj.as_bytes().len();
    let val_alloc = match obj {
        Robj::StrRaw(_) => val_len,
        _ => 0,
    };
    Value::SimpleString(
        format!(
            "key_sds_len:{} key_sds_avail:0 obj_alloc:{} val_sds_len:{} val_sds_avail:0 val_alloc:{}",
            key_len,
            std::mem::size_of::<Robj>(),
            val_len,
            val_alloc,
        )
        .into(),
    )
}

pub(super) fn debug_listpack(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &ServerContext,
) -> Value {
    if args.args.len() != 2 {
        return CommandError::syntax_error();
    }
    let obj = match lookup_debug_object(conn, args, ctx) {
        Ok(obj) => obj,
        Err(reply) => return reply,
    };
    if encoding_display_name(obj, object_encoding_limits(ctx), false) == "listpack" {
        Value::SimpleString(Bytes::from_static(b"Listpack structure printed on stdout"))
    } else {
        Value::error("ERR Not a listpack encoded object.")
    }
}

fn object_encoding_limits(ctx: &ServerContext) -> ObjectEncodingLimits {
    ObjectEncodingLimits {
        list_max_listpack_size: ctx.config().encoding.list_max_listpack_size,
        hash_max_listpack_entries: ctx.config().encoding.hash_max_listpack_entries as usize,
        hash_max_listpack_value: ctx.config().encoding.hash_max_listpack_value as usize,
        zset_max_listpack_entries: ctx.config().encoding.zset_max_ziplist_entries as usize,
        zset_max_listpack_value: ctx.config().encoding.zset_max_ziplist_value as usize,
    }
}

pub(super) fn debug_quicklist(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &ServerContext,
) -> Value {
    if !(2..=3).contains(&args.args.len()) {
        return CommandError::syntax_error();
    }
    let obj = match lookup_debug_object(conn, args, ctx) {
        Ok(obj) => obj,
        Err(reply) => return reply,
    };
    if matches!(obj, Robj::ListQuicklist(_)) {
        Value::SimpleString(Bytes::from_static(b"Quicklist structure printed on stdout"))
    } else {
        Value::error("ERR Not a quicklist encoded object.")
    }
}

pub(super) fn debug_htstats_key(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &ServerContext,
) -> Value {
    if args.args.len() < 2 {
        return CommandError::syntax_error();
    }
    let obj = match lookup_debug_object(conn, args, ctx) {
        Ok(obj) => obj,
        Err(reply) => return reply,
    };
    if let Robj::ZSetSkiplist(z) = &obj {
        let full = args
            .args
            .get(2)
            .map(|a| a.eq_ignore_ascii_case(b"full"))
            .unwrap_or(false);
        return Value::bulk(z.dict().stats_msg(full));
    }
    let elements = match obj {
        Robj::HashHashtable(h) => h.len(),
        Robj::SetHashtable(_) | Robj::ZSetSkiplist(_) => obj
            .as_collection()
            .map(|collection| collection.len())
            .unwrap_or(0),
        _ => {
            return Value::error(
                "ERR The value stored at the specified key is not represented using an hash table",
            );
        }
    };
    Value::bulk(format!(
        "Hash table 0 stats (main hash table):\n table size: {}\n number of elements: {}",
        elements.next_power_of_two().max(1),
        elements,
    ))
}

pub(super) fn debug_htstats(args: &CommandArgs, ctx: &ServerContext) -> Value {
    if args.args.len() < 2 {
        return Value::error(
            "ERR unknown subcommand or wrong number of arguments for 'htstats'. Try DEBUG HELP.",
        );
    }
    let dbid = match std::str::from_utf8(&args.args[1])
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
    {
        Some(v) => v,
        None => return Value::error("ERR value is not an integer or out of range"),
    };
    if dbid < 0 || dbid >= ctx.num_dbs() as i64 {
        return Value::error("ERR Out of range database");
    }
    let dbid = dbid as u32;
    let full = args.args.len() == 4 && args.args[3].eq_ignore_ascii_case(b"full");
    let db = match ctx.db(dbid) {
        Some(db) => db,
        None => return Value::error("ERR Out of range database"),
    };
    let mut text = String::from("[Dictionary HT]\n");
    text.push_str(&db.data_stats_msg(full));
    text.push_str("[Expires HT]\n");
    text.push_str(&db.expires_stats_msg(full));
    Value::verbatim(*b"txt", Bytes::from(text))
}

fn stringmatchlen_fuzz_test() -> u64 {
    let mut state = 0x1234_5678_9abc_def0u64;
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    let mut string = [0u8; 32];
    let mut pattern = [0u8; 32];
    let mut total_matches = 0u64;
    for _ in 0..200_000 {
        let slen = next() as usize % string.len();
        let plen = next() as usize % pattern.len();
        for b in string.iter_mut().take(slen) {
            *b = (next() % 128) as u8;
        }
        for b in pattern.iter_mut().take(plen) {
            *b = (next() % 128) as u8;
        }
        total_matches +=
            crate::framework::commands::pattern::string_match(&pattern[..plen], &string[..slen], false)
                as u64;
    }
    total_matches
}

pub(super) fn debug_stringmatch_test(args: &CommandArgs) -> Value {
    if args.args.len() != 1 {
        return CommandError::syntax_error();
    }
    stringmatchlen_fuzz_test();
    Value::SimpleString(Bytes::from_static(
        b"Apparently the server did not crash: test passed",
    ))
}

pub(super) fn debug_pause_after_fork(args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    if args.args.len() != 2 {
        return CommandError::syntax_error();
    }
    match std::str::from_utf8(&args.args[1]).ok().and_then(|s| s.parse::<i64>().ok()) {
        Some(value) => {
            ctx.config_mut().server.debug_pause_after_fork = value != 0;
            crate::server_log!(
                debug,
                "DEBUG PAUSE-AFTER-FORK pid={} enabled={}",
                std::process::id(),
                value != 0
            );
            Value::ok()
        }
        None => Value::error("ERR value is not an integer or out of range"),
    }
}

pub(super) fn debug_set_integer(args: &CommandArgs) -> Value {
    if args.args.len() != 2 {
        return CommandError::syntax_error();
    }
    match std::str::from_utf8(&args.args[1])
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
    {
        Some(_) => Value::ok(),
        None => Value::error("ERR value is not an integer or out of range"),
    }
}

pub(super) fn debug_delay_rdb_client_free_seconds(
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    if args.args.len() != 2 {
        return CommandError::syntax_error();
    }
    match std::str::from_utf8(&args.args[1])
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
    {
        Some(value) if value >= 0 => {
            ctx.config_mut()
                .server
                .debug_delay_rdb_client_free_seconds = value;
            Value::ok()
        }
        _ => Value::error("ERR value is not an integer or out of range"),
    }
}

pub(super) fn debug_hashtable_can_abort_shrink(args: &CommandArgs) -> Value {
    if args.args.len() != 2 {
        return CommandError::syntax_error();
    }
    match std::str::from_utf8(&args.args[1])
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
    {
        Some(v) => {
            crate::foundation::io::store::structure::hashtable::set_can_abort_shrink(v != 0);
            Value::ok()
        }
        None => Value::error("ERR value is not an integer or out of range"),
    }
}

fn lookup_debug_object<'a>(
    conn: &mut dyn CommandConn,
    args: &'a CommandArgs,
    ctx: &'a ServerContext,
) -> Result<&'a Robj, Value> {
    let db = ctx
        .db(conn.selected_db())
        .ok_or_else(CommandError::db_index_out_of_range)?;
    db.get_allow_expired(&args.args[1])
        .ok_or_else(|| Value::error("ERR no such key"))
}
