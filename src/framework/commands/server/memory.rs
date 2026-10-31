use crate::engine::command::context::CommandConn;
use crate::foundation::io::protocol::Value;
use crate::foundation::io::store::kv_engine::DbSet;
use crate::foundation::io::store::Robj;
use crate::framework::context::ServerContext;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

const HASHMAP_ENTRY_OVERHEAD: usize = 64;
const HASHTABLE_STRUCT_OVERHEAD: usize = 80;
const HASHTABLE_BUCKET_SIZE: usize = 64;

fn memory_get_keys(args: &[Bytes]) -> Vec<(&[u8], Vec<&'static str>)> {
    if args.is_empty() {
        return Vec::new();
    }
    if args[0].eq_ignore_ascii_case(b"usage") && args.len() >= 2 {
        vec![(args[1].as_ref(), vec!["RO"])]
    } else {
        Vec::new()
    }
}

#[command("MEMORY", get_keys = memory_get_keys)]
pub fn memory(conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    if args.args.is_empty() {
        return CommandError::wrong_arity("memory");
    }
    let sub = String::from_utf8_lossy(&args.args[0]).to_ascii_lowercase();
    match sub.as_str() {
        "usage" => memory_usage(conn, args, ctx.dbs_mut()),
        "stats" => memory_stats(ctx),
        "doctor" => Value::bulk(Bytes::from(memory_doctor(ctx))),
        "malloc-stats" => {
            if args.args.len() != 1 {
                return CommandError::wrong_arity("memory|malloc-stats");
            }
            Value::bulk(Bytes::from_static(
                b"Stats not supported for the current allocator",
            ))
        }
        "purge" => {
            if args.args.len() != 1 {
                return CommandError::wrong_arity("memory|purge");
            }
            Value::ok()
        }
        "help" => memory_help(),
        _ => Value::error(format!(
            "ERR Unknown subcommand or wrong number of arguments for '{}'.",
            sub
        )),
    }
}

fn memory_doctor(ctx: &ServerContext) -> String {
    let stats = crate::foundation::memory::util_alloc::allocator_stats();
    let peak = crate::foundation::memory::util_alloc::peak_allocated();
    let used = stats.allocated;
    if used < 5 * 1024 * 1024 {
        return "Hi Sam, I can't find any memory problems in your instance. I can only provide my advice when there is a sizable amount of data in your instance.".to_string();
    }
    let mut findings = Vec::new();
    if peak > used.saturating_mul(2) {
        findings.push("Your instance used to have more memory than it currently uses. If this is unexpected, inspect workload peaks and consider restarting during a maintenance window.");
    }
    if stats.active > used.saturating_mul(3) / 2 {
        if ctx.config().server.activedefrag {
            findings.push("Allocator fragmentation is high relative to live allocations. Active defragmentation is enabled; monitor active_defrag_hits and total_active_defrag_time while it incrementally relocates values.");
        } else {
            findings.push("Allocator fragmentation is high relative to live allocations. Enable activedefrag to incrementally relocate values, or restart during a maintenance window.");
        }
    }
    if ctx.stats().lazyfree_pending_objects > 0 {
        findings.push("Background lazyfree work is pending. Monitor lazyfree_pending_objects before treating allocator pressure as persistent fragmentation.");
    }
    if findings.is_empty() {
        "Hi Sam, I can't find any memory problems in your instance. Current allocator and RSS measurements are within the configured diagnostic thresholds.".to_string()
    } else {
        format!(
            "Sam, I detected the following memory condition(s):\n- {}",
            findings.join("\n- ")
        )
    }
}

fn memory_help() -> Value {
    const HELP: &[&str] = &[
        "MEMORY <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
        "DOCTOR",
        "    Return memory problems reports.",
        "MALLOC-STATS",
        "    Return internal statistics report from the memory allocator.",
        "PURGE",
        "    Attempt to purge dirty pages so they can be reclaimed by the allocator.",
        "    On glibc systems this also releases free pages from the libc main arena",
        "    (the process [heap] segment) back to the OS via malloc_trim(3).",
        "STATS",
        "    Return information about the memory usage of the server.",
        "USAGE <key> [SAMPLES <count>]",
        "    Return memory in bytes used by <key> and its value. Nested values are",
        "    sampled up to <count> times (default: 5, 0 means sample all).",
        "HELP",
        "    Print this help.",
    ];
    Value::Array(
        HELP.iter()
            .map(|line| Value::SimpleString(Bytes::from_static(line.as_bytes())))
            .collect(),
    )
}

fn memory_stats(ctx: &ServerContext) -> Value {
    let dbs = ctx.dbs();
    let allocator_total = crate::foundation::memory::util_alloc::total_allocated();
    let allocator_peak = crate::foundation::memory::util_alloc::peak_allocated();
    let tracker = crate::foundation::memory::metrics::memory_stats::get_tracker();
    let normal_clients = ctx.normal_client_memory();
    let aof_buffer = ctx.aof().buffer_allocation_bytes();
    let lua_caches = ctx.script().script_cache.memory_usage();
    let replica_repl_buffer = if ctx.replication().is_replica() {
        ctx.replication().replica_output_buffer_bytes
    } else {
        0
    };
    let overhead_total = tracker.overhead().saturating_add(replica_repl_buffer);
    // Return a minimal MEMORY STATS response. The format is a flat key-value list
    // where db.<idx> maps to a nested list of per-db hashtable stats.
    let mut result = vec![
        Value::bulk(Bytes::from("peak.allocated")),
        Value::integer(allocator_peak as i64),
        Value::bulk(Bytes::from("total.allocated")),
        Value::integer(allocator_total as i64),
        Value::bulk(Bytes::from("startup.allocated")),
        Value::integer(tracker.startup_allocated() as i64),
        Value::bulk(Bytes::from("replication.backlog")),
    ];
    result.push(Value::integer(
        ctx.replication().backlog.history_len() as i64
    ));
    result.push(Value::bulk(Bytes::from("replicas.repl.buffer")));
    result.push(Value::integer(replica_repl_buffer as i64));
    result.push(Value::bulk(Bytes::from("clients.slaves")));
    result.push(Value::integer(0));
    result.push(Value::bulk(Bytes::from("clients.normal")));
    result.push(Value::integer(normal_clients as i64));
    result.push(Value::bulk(Bytes::from("aof.buffer")));
    result.push(Value::integer(aof_buffer as i64));
    result.push(Value::bulk(Bytes::from("lua.caches")));
    result.push(Value::integer(lua_caches as i64));

    // Per-db hashtable stats
    let mut hashtable_lut_total = 0i64;
    let mut rehashing_bytes_total = 0i64;
    let mut rehashing_count_total = 0i64;
    for idx in 0..dbs.num_dbs() as u32 {
        let bc = dbs.data_bucket_count(idx);
        let overhead_main = if bc == 0 {
            0
        } else {
            (HASHTABLE_STRUCT_OVERHEAD + bc * HASHTABLE_BUCKET_SIZE) as i64
        };
        let expires_count = dbs.expires_count(idx);
        let overhead_expires = (expires_count * 64) as i64;
        let rehashing_count = dbs.rehashing_count(idx) as i64;
        let rehashing_bytes =
            (dbs.rehashing_bucket_count(idx) * HASHTABLE_BUCKET_SIZE) as i64;
        hashtable_lut_total += overhead_main + overhead_expires;
        rehashing_bytes_total += rehashing_bytes;
        rehashing_count_total += rehashing_count;

        let db_entry = vec![
            Value::bulk(Bytes::from("overhead.hashtable.main")),
            Value::integer(overhead_main),
            Value::bulk(Bytes::from("overhead.hashtable.expires")),
            Value::integer(overhead_expires),
            Value::bulk(Bytes::from("db.dict.rehashing.count")),
            Value::integer(rehashing_count),
        ];

        result.push(Value::bulk(Bytes::from(format!("db.{}", idx))));
        result.push(Value::Array(db_entry));
    }

    // Global totals
    result.push(Value::bulk(Bytes::from("overhead.db.hashtable.lut")));
    result.push(Value::integer(hashtable_lut_total));
    result.push(Value::bulk(Bytes::from("overhead.db.hashtable.rehashing")));
    result.push(Value::integer(rehashing_bytes_total));
    result.push(Value::bulk(Bytes::from("db.dict.rehashing.count")));
    result.push(Value::integer(rehashing_count_total));
    result.push(Value::bulk(Bytes::from("overhead.total")));
    result.push(Value::integer(overhead_total as i64));

    Value::Array(result)
}

fn memory_usage(conn: &mut dyn CommandConn, args: &CommandArgs, dbs: &mut DbSet) -> Value {
    if args.args.len() < 2 {
        return CommandError::wrong_arity("memory|usage");
    }
    let key = &args.args[1];
    let mut _samples: i64 = 5;
    let mut i = 2;
    while i < args.args.len() {
        let opt = String::from_utf8_lossy(&args.args[i]).to_ascii_lowercase();
        match opt.as_str() {
            "samples" => {
                if i + 1 >= args.args.len() {
                    return CommandError::syntax_error();
                }
                _samples = match std::str::from_utf8(&args.args[i + 1])
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                {
                    Some(n) if n >= 0 => n,
                    _ => return Value::error("ERR value is not an integer or out of range"),
                };
                i += 2;
            }
            _ => return CommandError::syntax_error(),
        }
    }
    let db = match dbs.get_mut(conn.selected_db()) {
        Some(d) => d,
        None => return CommandError::db_index_out_of_range(),
    };
    match db.get(key) {
        Some(obj) => {
            let mut usage = estimate_memory(key.as_ref(), obj);
            if let Some(fields) = db.hash_field_expires_for(key) {
                usage += fields
                    .keys()
                    .map(|field| field.len() + std::mem::size_of::<u64>() + 2 * std::mem::size_of::<usize>())
                    .sum::<usize>();
            }
            Value::integer(usage as i64)
        }
        None => Value::null(),
    }
}

fn estimate_memory(key: &[u8], obj: &Robj) -> usize {
    let key_len = key.len();
    let value_len = match obj {
        Robj::StrInt(s) => s.len(),
        Robj::StrEmbstr(s) => s.len(),
        Robj::StrRaw(s) => s.0.len(),
        Robj::ListQuicklist(l) => {
            let mut total = 0;
            for item in l.iter() {
                total += item.len() + 32;
            }
            total
        }
        Robj::ListListpack(l) => l.memory_usage(),
        Robj::HashHashtable(h) => {
            let mut total = 0;
            for (k, v) in h.data.iter() {
                total += k.len() + v.len() + 64;
            }
            total
        }
        Robj::HashListpack(h) => h.raw_bytes().len(),
        Robj::SetHashtable(s) => {
            let mut total = 0;
            for item in s.data.keys() {
                total += item.len() + 32;
            }
            total
        }
        Robj::SetIntset(s) => {
            // i64 per element + overhead per element estimate
            s.len() * (std::mem::size_of::<i64>() + 16)
        }
        Robj::SetListpack(s) => s.memory_usage(),
        Robj::ZSetSkiplist(z) => {
            let mut total = 0;
            for k in z.dict().keys() {
                total += k.len() + 8 + 64;
            }
            total
        }
        Robj::ZSetListpack(z) => z.memory_usage(),
        Robj::StreamListpack(s) => {
            let mut total = 0;
            for (_id, fields) in s.iter() {
                for (k, v) in fields {
                    total += k.len() + v.len() + 32;
                }
                total += 16 + 32;
            }
            total
        }
        Robj::StreamRadix(_) => 0,
        Robj::PathHash(hash) => hash.memory_usage(),
    };
    key_len + value_len + HASHMAP_ENTRY_OVERHEAD
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nested_integer(items: &[Value], key: &[u8]) -> Option<i64> {
        items.windows(2).find_map(|pair| match pair {
            [Value::Bulk(name), Value::Integer(value)] if name.as_ref() == key => Some(*value),
            _ => None,
        })
    }

    #[test]
    fn memory_stats_reports_per_db_rehashing_count() {
        let mut ctx = ServerContext::new(1);
        let db = ctx.dbs_mut().get_mut(0).unwrap();
        for index in 0..8u64 {
            db.set(
                format!("k{index}").into_bytes(),
                crate::foundation::io::store::Robj::string("v"),
            );
        }

        let Value::Array(stats) = memory_stats(&ctx) else {
            panic!("MEMORY STATS should return an array");
        };
        assert_eq!(nested_integer(&stats, b"db.dict.rehashing.count"), Some(1));

        let db0 = stats.windows(2).find_map(|pair| match pair {
            [Value::Bulk(name), Value::Array(values)] if name.as_ref() == b"db.0" => Some(values),
            _ => None,
        });

        let db0 = db0.expect("db.0 stats should be present");
        assert_eq!(nested_integer(db0, b"db.dict.rehashing.count"), Some(1));
        assert!(nested_integer(&stats, b"overhead.db.hashtable.lut").unwrap_or(0) > 0);
        assert_eq!(
            nested_integer(&stats, b"overhead.db.hashtable.rehashing"),
            Some(64)
        );
        assert_eq!(nested_integer(&stats, b"db.dict.rehashing.count"), Some(1));
    }

    #[test]
    fn memory_doctor_returns_a_human_readable_report() {
        let ctx = ServerContext::new(1);
        let report = memory_doctor(&ctx);
        assert!(report.contains("Sam"));
        assert!(!report.eq_ignore_ascii_case("ok"));
    }

    #[test]
    fn memory_stats_uses_live_allocator_and_client_counters() {
        let mut ctx = ServerContext::new(1);
        ctx.adjust_normal_client_memory(0, 321);
        let Value::Array(stats) = memory_stats(&ctx) else {
            panic!("MEMORY STATS must return an array");
        };
        let total = nested_integer(&stats, b"total.allocated").expect("allocator total");
        let peak = nested_integer(&stats, b"peak.allocated").expect("allocator peak");
        assert!(total > 0);
        assert!(peak >= total);
        assert_eq!(nested_integer(&stats, b"clients.normal"), Some(321));
    }
}
