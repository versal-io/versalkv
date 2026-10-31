use crate::engine::distributed::cluster::node::{key_hash_slot, pattern_hash_slot};
use crate::engine::distributed::cluster::state::{
    ClusterRouteDecision, RouteRequest, CLUSTER_SLOTS,
};
use crate::engine::command::context::CommandConn;
use crate::engine::command::CMD_READONLY;
use crate::foundation::io::protocol::Value;
use crate::framework::commands::pattern::string_match;
use crate::framework::context::ServerContext;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[path = "clusterscan_slot_tags.rs"]
mod slot_tags;

use slot_tags::slot_tag;

const SLOT_BITS: u32 = 14;

#[derive(Clone, Copy)]
struct ParsedCursor {
    raw: u64,
    slot: Option<usize>,
}

#[command("CLUSTERSCAN")]
pub fn clusterscan(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    // CLUSTERSCAN cursor [MATCH pattern] [COUNT count] [TYPE type] [SLOT slot]
    if args.args.is_empty() {
        return CommandError::wrong_arity("clusterscan");
    }
    if !ctx.config().server.cluster_enabled || ctx.cluster_state().is_none() {
        return Value::error("ERR This instance has cluster support disabled");
    }
    ctx.reset_repl_seldb();

    let cursor = match parse_cursor(&args.args[0]) {
        Ok(cursor) => cursor,
        Err(error) => return error,
    };

    let mut count: usize = 10;
    let mut pattern: Option<&[u8]> = None;
    let mut type_filter: Option<Vec<u8>> = None;
    let mut slot_filter: Option<usize> = None;

    let mut i = 1;
    while i < args.args.len() {
        let opt = match std::str::from_utf8(&args.args[i]) {
            Ok(s) => s,
            Err(_) => return CommandError::syntax_error(),
        };
        if opt.eq_ignore_ascii_case("count") && i + 1 < args.args.len() {
            count = match parse_count(&args.args[i + 1]) {
                Ok(count) => count,
                Err(error) => return error,
            };
            i += 2;
        } else if opt.eq_ignore_ascii_case("match") && i + 1 < args.args.len() {
            pattern = Some(&args.args[i + 1]);
            i += 2;
        } else if opt.eq_ignore_ascii_case("type") && i + 1 < args.args.len() {
            let type_name = args.args[i + 1].to_vec();
            if !is_valid_type_name(&type_name) {
                let name = String::from_utf8_lossy(&type_name);
                return Value::error(format!("unknown type name '{}'", name).into_bytes());
            }
            type_filter = Some(type_name);
            i += 2;
        } else if opt.eq_ignore_ascii_case("slot") && i + 1 < args.args.len() {
            if slot_filter.is_some() {
                return Value::error("ERR SLOT option can only be specified once");
            }
            slot_filter = match parse_slot(&args.args[i + 1]) {
                Ok(slot) => Some(slot),
                Err(error) => return error,
            };
            i += 2;
        } else {
            return CommandError::syntax_error();
        }
    }

    if let (Some(cursor_slot), Some(option_slot)) = (cursor.slot, slot_filter) {
        if cursor_slot != option_slot {
            return Value::error("ERR Cursor slot mismatch");
        }
    }
    let match_slot = pattern.and_then(pattern_slot_tag);
    if let (Some((match_slot, _)), Some(option_slot)) = (match_slot.as_ref(), slot_filter) {
        if *match_slot != option_slot {
            return Value::Array(vec![Value::bulk("0"), Value::Array(Vec::new())]);
        }
    }
    if let Some((match_slot, tag)) = match_slot.as_ref() {
        if slot_filter.is_none() {
            match cursor.slot {
                None if cursor.raw == 0 => {
                    return Value::Array(vec![
                        Value::bulk(format!("0-{{{tag}}}-0")),
                        Value::Array(Vec::new()),
                    ]);
                }
                Some(cursor_slot) if cursor_slot < *match_slot => {
                    return Value::Array(vec![
                        Value::bulk(format!("0-{{{tag}}}-0")),
                        Value::Array(Vec::new()),
                    ]);
                }
                Some(cursor_slot) if cursor_slot > *match_slot => {
                    return Value::Array(vec![Value::bulk("0"), Value::Array(Vec::new())]);
                }
                _ => {}
            }
        }
    }
    let requested_slot = slot_filter.or(match_slot.as_ref().map(|(slot, _)| *slot));
    let start_slot = cursor.slot.or(requested_slot).unwrap_or(0);
    let single_slot_mode = requested_slot.is_some();

    if single_slot_mode {
        if let Some(slot) = requested_slot {
            if cursor.slot.is_some_and(|cursor_slot| cursor_slot != slot) {
                return Value::error("ERR Cursor slot mismatch");
            }
        }
    }
    let effective_slot_filter = requested_slot;

    if cursor.raw == 0 && cursor.slot.is_none() && slot_filter.is_none() && match_slot.is_none() {
        return Value::Array(vec![
            Value::bulk(format!("0-{{{}}}-0", slot_tag(0))),
            Value::Array(Vec::new()),
        ]);
    }
    if let Some(slot) = requested_slot {
        if cursor.raw == 0 && cursor.slot.is_none() && slot_filter.is_none() {
            return Value::Array(vec![
                Value::bulk(format!("0-{{{}}}-0", slot_tag(slot))),
                Value::Array(Vec::new()),
            ]);
        }
    }

    if start_slot >= CLUSTER_SLOTS {
        return Value::error("ERR Invalid cursor");
    }
    let local_range_end = {
        let Some(state) = ctx.cluster_state() else {
            return Value::error("ERR This instance has cluster support disabled");
        };
        let projection = state.projection();
        let key = format!("{{{}}}", slot_tag(start_slot));
        let preferred_endpoint = match ctx.config().server.cluster_preferred_endpoint_type.as_str()
        {
            "hostname" => crate::engine::distributed::cluster::PreferredEndpoint::Hostname,
            "unknown-endpoint" => crate::engine::distributed::cluster::PreferredEndpoint::Unknown,
            _ => crate::engine::distributed::cluster::PreferredEndpoint::Ip,
        };
        if !projection.has_declared_owner(start_slot) {
            return Value::error("CLUSTERDOWN Hash slot not served");
        }
        let redirect_decision = projection.decide_route(RouteRequest {
            keys: &[key.as_bytes()],
            command_flags: CMD_READONLY,
            existing_keys: None,
            allow_replica_read: conn.readonly(),
            preferred_endpoint,
            transport: crate::engine::distributed::cluster::ClientTransport::Tcp,
            prefer_ipv6: conn.peer_addr().is_ipv6(),
            require_cluster_ok: false,
            route_sharded_pubsub: false,
            redirect_keyless: false,
            client_readonly: conn.readonly(),
            client_asking: false,
            open_slot_policy: crate::engine::distributed::cluster::OpenSlotPolicy::Standard,
        });
        let redirect_error = (redirect_decision != ClusterRouteDecision::ExecuteLocal).then(|| {
            redirect_decision
                .error_message()
                .expect("non-local cluster route must have an error")
        });
        let locally_owned = requested_slot
            .or(cursor.slot)
            .map(|slot| projection.local_owns_slot(slot))
            .unwrap_or(false);
        if redirect_error
            .as_deref()
            .is_some_and(|error| error.contains("Hash slot not served"))
            && !locally_owned
        {
            return Value::error(redirect_error.unwrap());
        }
        if !projection.health_is_ok() {
            return Value::error("CLUSTERDOWN The cluster is down");
        }
        if let Some(error) = redirect_error {
            let clusterdown_like =
                error.starts_with("CLUSTERDOWN") || error.contains("Hash slot not served");
            if !clusterdown_like || !locally_owned {
                return Value::error(error);
            }
        }
        (!single_slot_mode)
            .then(|| projection.local_slot_range_end(start_slot))
            .flatten()
    };

    let no_lazy_expire = ctx.config().server.import_mode || conn.import_source();
    let match_all = pattern.is_none_or(|p| p == b"*");
    let (final_slot, advance_to_next_slot) = if let Some(slot) = effective_slot_filter {
        (slot, false)
    } else {
        (local_range_end.unwrap_or(start_slot), true)
    };
    let scan_options = ScanRangeOptions {
        count,
        type_filter: type_filter.as_deref(),
        pattern,
        match_all,
        no_lazy_expire,
        advance_to_next_slot,
    };

    ctx.with_db_mut(conn.selected_db(), |db| {
        db.set_no_lazy_expire(no_lazy_expire);
        let (next_cursor_text, out) =
            scan_cluster_range(db, start_slot, final_slot, cursor.raw, &scan_options);
        db.set_no_lazy_expire(false);
        Value::Array(vec![Value::bulk(next_cursor_text), Value::Array(out)])
    })
}

struct ScanRangeOptions<'a> {
    count: usize,
    type_filter: Option<&'a [u8]>,
    pattern: Option<&'a [u8]>,
    match_all: bool,
    no_lazy_expire: bool,
    advance_to_next_slot: bool,
}

fn scan_cluster_range(
    db: &mut crate::foundation::io::store::kv_engine::Db,
    first_slot: usize,
    final_slot: usize,
    cursor: u64,
    options: &ScanRangeOptions<'_>,
) -> (String, Vec<Value>) {
    let max_iterations = options.count.saturating_mul(10).max(1);
    let mut cursor = cursor;
    let mut sampled = 0usize;
    let mut iterations = 0usize;
    let mut out = Vec::new();

    loop {
        let (next_cursor, batch) =
            db.scan_range_step_include_expired(first_slot, final_slot, cursor);
        cursor = next_cursor;
        iterations += 1;
        let cursor_slot = (cursor != 0).then_some((cursor & ((1u64 << SLOT_BITS) - 1)) as usize);
        let slot_key_count = cursor_slot.map(|slot| db.scan_slot_key_count(slot));
        let sampled_in_cursor_slot = cursor_slot.map_or(0, |slot| {
            batch
                .iter()
                .filter(|key| key_hash_slot(key) as usize == slot)
                .count()
        });
        for key in batch {
            sampled += 1;
            if let Some(type_filter) = options.type_filter {
                let passes = db.get_allow_expired(&key).is_some_and(|value| {
                    value
                        .type_name()
                        .as_bytes()
                        .eq_ignore_ascii_case(type_filter)
                });
                if !passes {
                    continue;
                }
            }
            if !options.no_lazy_expire && db.is_expired(&key) {
                db.del(&key);
                db.expired_deletions.push(key);
                continue;
            }
            if let Some(pattern) = options.pattern {
                if !options.match_all && !string_match(pattern, &key, false) {
                    continue;
                }
            }
            out.push(Value::bulk(key));
        }
        if sampled >= options.count {
            if let (Some(slot), Some(key_count)) = (cursor_slot, slot_key_count) {
                if sampled_in_cursor_slot >= key_count {
                    cursor = db
                        .next_populated_slot(slot + 1, final_slot)
                        .map_or(0, |next_slot| next_slot as u64);
                }
            }
        }
        if cursor == 0 || sampled >= options.count || iterations >= max_iterations {
            break;
        }
    }

    let next_cursor =
        if cursor == 0 && options.advance_to_next_slot && final_slot + 1 < CLUSTER_SLOTS {
            format!("0-{{{}}}-0", slot_tag(final_slot + 1))
        } else {
            format_cursor(cursor)
        };
    (next_cursor, out)
}

fn parse_cursor(raw: &[u8]) -> Result<ParsedCursor, Value> {
    let raw = std::str::from_utf8(raw).map_err(|_| Value::error("ERR Invalid cursor"))?;
    if let Ok(cursor) = raw.parse::<u64>() {
        return Ok(ParsedCursor {
            raw: cursor,
            slot: None,
        });
    }

    let (range, rest) = raw
        .split_once("-{")
        .ok_or_else(|| Value::error("ERR Invalid cursor"))?;
    let (tag, dict_cursor) = rest
        .split_once("}-")
        .ok_or_else(|| Value::error("ERR Invalid cursor"))?;
    if tag.is_empty() {
        return Err(Value::error("ERR Invalid cursor"));
    }
    let dict_cursor = dict_cursor
        .parse::<u64>()
        .map_err(|_| Value::error("ERR Invalid cursor"))?;
    let slot = key_hash_slot(format!("{{{tag}}}").as_bytes()) as usize;
    let cursor = if range == clusterscan_fingerprint() {
        dict_cursor
    } else {
        slot as u64
    };
    Ok(ParsedCursor {
        raw: cursor,
        slot: Some(slot),
    })
}

fn format_cursor(cursor: u64) -> String {
    if cursor == 0 {
        return "0".to_string();
    }
    let slot = (cursor & ((1u64 << SLOT_BITS) - 1)) as usize;
    format!(
        "{}-{{{}}}-{}",
        clusterscan_fingerprint(),
        slot_tag(slot),
        cursor
    )
}

fn clusterscan_fingerprint() -> String {
    let mut fingerprint =
        crate::foundation::io::store::structure::hash_seed::process_seed_fingerprint();
    let mut encoded = [0u8; 6];
    for byte in encoded.iter_mut().rev() {
        *byte = b'0' + (fingerprint & 0x3f) as u8;
        fingerprint >>= 6;
    }
    String::from_utf8(encoded.to_vec()).expect("fingerprint alphabet is ASCII")
}

fn parse_count(raw: &[u8]) -> Result<usize, Value> {
    if raw.first() == Some(&b'-') {
        return Err(CommandError::syntax_error());
    }
    let count = std::str::from_utf8(raw)
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .ok_or_else(|| Value::error("ERR value is not an integer or out of range"))?;
    if count == 0 {
        return Err(CommandError::syntax_error());
    }
    Ok(count)
}

fn parse_slot(raw: &[u8]) -> Result<usize, Value> {
    let slot = std::str::from_utf8(raw)
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .ok_or_else(|| Value::error("ERR Invalid or out of range slot"))?;
    if slot >= CLUSTER_SLOTS {
        return Err(Value::error("ERR Invalid or out of range slot"));
    }
    Ok(slot)
}

const VALID_TYPE_NAMES: &[&str] = &[
    "string", "list", "hash", "set", "zset", "stream", "pathhash",
];

fn is_valid_type_name(name: &[u8]) -> bool {
    VALID_TYPE_NAMES
        .iter()
        .any(|t| name.eq_ignore_ascii_case(t.as_bytes()))
}

fn pattern_slot_tag(pattern: &[u8]) -> Option<(usize, String)> {
    let slot = pattern_hash_slot(pattern)? as usize;
    let start = pattern.iter().position(|&b| b == b'{')?;
    let end_rel = pattern[start + 1..].iter().position(|&b| b == b'}')?;
    if end_rel == 0 {
        return None;
    }
    let end = start + 1 + end_rel;
    let tag = std::str::from_utf8(&pattern[start + 1..end]).ok()?;
    Some((slot, tag.to_string()))
}

#[cfg(test)]
#[path = "clusterscan_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "clusterscan_failure_tests.rs"]
mod failure_tests;
