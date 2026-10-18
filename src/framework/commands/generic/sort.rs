use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use crate::foundation::io::store::structure::list::ListValue;
use crate::foundation::io::store::Robj;
use bytes::Bytes;
use command_macros::command;

#[path = "sort/impls.rs"]
mod impls;
use impls::*;

fn sort_get_keys(args: &[Bytes]) -> Vec<(&[u8], Vec<&'static str>)> {
    let mut result = Vec::new();
    if args.is_empty() {
        return result;
    }
    result.push((args[0].as_ref(), vec!["RO", "access"]));
    let mut i = 1;
    let mut store_key: Option<&[u8]> = None;
    while i < args.len() {
        let tok = String::from_utf8_lossy(&args[i]).to_ascii_uppercase();
        if tok == "STORE" && i + 1 < args.len() {
            store_key = Some(args[i + 1].as_ref());
            i += 2;
        } else if tok == "LIMIT" {
            i += 3;
        } else if tok == "BY" || tok == "GET" {
            if i + 1 < args.len() {
                i += 2;
            } else {
                i += 1;
            }
        } else {
            i += 1;
        }
    }
    if let Some(key) = store_key {
        result.push((key, vec!["OW", "update"]));
    }
    result
}

fn sort_ro_get_keys(args: &[Bytes]) -> Vec<(&[u8], Vec<&'static str>)> {
    if args.is_empty() {
        return Vec::new();
    }
    vec![(args[0].as_ref(), vec!["RO", "access"])]
}

#[command("SORT", runtime, get_keys = sort_get_keys)]
pub fn sort(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    sort_generic(runtime, args, false)
}

#[command("SORT_RO", runtime, get_keys = sort_ro_get_keys)]
pub fn sort_ro(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    sort_generic(runtime, args, true)
}

fn sort_generic(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
    readonly: bool,
) -> CommandOutcome {
    let cmd_name = if readonly { "sort_ro" } else { "sort" };
    let full_key_access = match runtime.validate_sort_access(cmd_name, args) {
        Ok(value) => value,
        Err(error) => return CommandOutcome::reply(error),
    };
    let db_idx = runtime.selected_db();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };

    let key = &args.args[0];
    let opts = match parse_options(&args.args[1..], full_key_access) {
        Ok(opts) => opts,
        Err(e) => return CommandOutcome::reply(e),
    };
    if readonly && opts.store_key.is_some() {
        return CommandOutcome::reply(CommandError::syntax_error());
    }

    let elements = match db.get(key) {
        None => {
            if let Some(store_key) = &opts.store_key {
                db.del(store_key);
                return sort_store_outcome(Value::integer(0), db_idx, store_key);
            } else {
                return CommandOutcome::reply(Value::Array(Vec::new()));
            }
        }
        Some(obj) => extract_elements(obj),
    };

    let entries = match build_entries(db, &elements, &opts) {
        Ok(entries) => entries,
        Err(e) => return CommandOutcome::reply(e),
    };

    let mut sorted = entries;
    if !opts.by_nosort {
        sort_entries(&mut sorted, opts.alpha, opts.desc);
    } else if opts.desc {
        sorted.reverse();
    }

    let limited = apply_limit(&sorted, opts.limit_start, opts.limit_count);

    if let Some(store_key) = &opts.store_key {
        let result: Vec<Bytes> = limited
            .iter()
            .map(|e| Bytes::copy_from_slice(&e.element))
            .collect();
        if result.is_empty() {
            db.del(store_key);
        } else {
            db.set(
                store_key.to_vec(),
                Robj::ListQuicklist(ListValue::from_vecdeque(result.into())),
            );
        }
        sort_store_outcome(Value::integer(limited.len() as i64), db_idx, store_key)
    } else {
        let mut out = Vec::new();
        for entry in limited {
            if opts.get_patterns.is_empty() {
                out.push(Value::bulk(entry.element.clone()));
                continue;
            }
            for pattern in &opts.get_patterns {
                out.push(resolve_get_pattern(db, pattern, &entry.element));
            }
        }
        CommandOutcome::reply(Value::Array(out))
    }
}

fn sort_store_outcome(reply: Value, db_idx: u32, key: &Bytes) -> CommandOutcome {
    CommandOutcome {
        reply,
        effects: CommandEffects {
            changed_keys: vec![(db_idx, key.clone())],
            events: vec![CoreEvent {
                kind: "sortstore",
                key: key.clone(),
            }],
            ..CommandEffects::default()
        },
    }
}

fn parse_options(args: &[Bytes], full_key_access: bool) -> Result<SortOptions, Value> {
    let mut opts = SortOptions::default();
    let mut i = 0;
    while i < args.len() {
        let arg = std::str::from_utf8(&args[i]).unwrap_or("");
        let lower = arg.to_ascii_lowercase();
        match lower.as_str() {
            "asc" => opts.desc = false,
            "desc" => opts.desc = true,
            "alpha" => opts.alpha = true,
            "limit" => {
                if i + 2 >= args.len() {
                    return Err(CommandError::syntax_error());
                }
                opts.limit_start = std::str::from_utf8(&args[i + 1])
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(0);
                opts.limit_count = std::str::from_utf8(&args[i + 2])
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(-1);
                i += 2;
            }
            "by" => {
                if i + 1 < args.len() {
                    let by_str = std::str::from_utf8(&args[i + 1]).unwrap_or("");
                    if by_str.eq_ignore_ascii_case("nosort") {
                        opts.by_nosort = true;
                    } else {
                        if args[i + 1].contains(&b'*') && !full_key_access {
                            return Err(Value::error(
                                "ERR BY option of SORT denied due to insufficient ACL permissions.",
                            ));
                        }
                        opts.by_pattern = Some(args[i + 1].clone());
                    }
                    i += 1;
                } else {
                    return Err(CommandError::syntax_error());
                }
            }
            "store" => {
                if i + 1 < args.len() {
                    opts.store_key = Some(args[i + 1].clone());
                    i += 1;
                } else {
                    return Err(CommandError::syntax_error());
                }
            }
            "get" if i + 1 < args.len() => {
                if !full_key_access {
                    return Err(Value::error(
                        "ERR GET option of SORT denied due to insufficient ACL permissions.",
                    ));
                }
                opts.get_patterns.push(args[i + 1].clone());
                i += 1;
            }
            "get" => return Err(CommandError::syntax_error()),
            _ => return Err(CommandError::syntax_error()),
        }
        i += 1;
    }
    Ok(opts)
}
