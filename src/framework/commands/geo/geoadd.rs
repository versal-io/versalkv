use super::geohash_util::{
    geohash_encode, parse_double, GEO_LAT_MAX, GEO_LAT_MIN, GEO_LONG_MAX, GEO_LONG_MIN,
};
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("GEOADD", runtime)]
pub fn geoadd(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let mut nx = false;
    let mut xx = false;
    let mut ch = false;
    let mut i = 1;
    while i < args.args.len() {
        let arg = &args.args[i];
        if arg.eq_ignore_ascii_case(b"nx") {
            nx = true;
        } else if arg.eq_ignore_ascii_case(b"xx") {
            xx = true;
        } else if arg.eq_ignore_ascii_case(b"ch") {
            ch = true;
        } else {
            break;
        }
        i += 1;
    }
    if nx && xx {
        return CommandOutcome::reply(CommandError::syntax_error());
    }
    let remaining = args.args.len() - i;
    if remaining == 0 || !remaining.is_multiple_of(3) {
        return CommandOutcome::reply(CommandError::syntax_error());
    }
    let mut pairs = Vec::with_capacity(remaining / 3);
    let mut j = i;
    while j + 2 < args.args.len() {
        let lon = match parse_double(&args.args[j]) {
            Some(v) => v,
            None => return CommandOutcome::reply(Value::error("ERR valid longitude is required")),
        };
        let lat = match parse_double(&args.args[j + 1]) {
            Some(v) => v,
            None => return CommandOutcome::reply(Value::error("ERR valid latitude is required")),
        };
        if !(GEO_LONG_MIN..=GEO_LONG_MAX).contains(&lon) || !(GEO_LAT_MIN..=GEO_LAT_MAX).contains(&lat) {
            return CommandOutcome::reply(Value::error(format!(
                "ERR invalid longitude,latitude pair {:.6},{:.6}",
                lon, lat
            )));
        }
        let member = args.args[j + 2].clone();
        let score = match geohash_encode(lon, lat) {
            Some(s) => s as f64,
            None => {
                return CommandOutcome::reply(Value::error(format!(
                    "ERR invalid longitude,latitude pair {:.6},{:.6}",
                    lon, lat
                )))
            }
        };
        pairs.push((member, score));
        j += 3;
    }
    let db_idx = runtime.selected_db();
    let (max_entries, max_value_size) = runtime.zset_encoding_limits();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    let key = &args.args[0];
    let mut added = 0i64;
    let mut updated = 0i64;
    for (member, score) in pairs {
        let current = match db.get_zset_object(key) {
            Ok(Some(zset)) => zset.zset_score(&member),
            Ok(None) => None,
            Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
        };
        match current {
            Some(old_score) => {
                if nx {
                    continue;
                }
                if score != old_score {
                    if db.zset_add_with_limits(key, &member, score, max_entries, max_value_size).is_err() {
                        return CommandOutcome::reply(CommandError::wrong_type());
                    }
                    updated += 1;
                }
            }
            None => {
                if xx {
                    continue;
                }
                if db.zset_add_with_limits(key, &member, score, max_entries, max_value_size).is_err() {
                    return CommandOutcome::reply(CommandError::wrong_type());
                }
                added += 1;
            }
        }
    }
    let reply = if ch {
        Value::integer(added + updated)
    } else {
        Value::integer(added)
    };
    if added == 0 && updated == 0 {
        return CommandOutcome {
            reply,
            effects: CommandEffects {
                skip_propagate: true,
                ..CommandEffects::default()
            },
        };
    }
    CommandOutcome {
        reply,
        effects: CommandEffects {
            changed_keys: vec![(db_idx, key.clone())],
            events: vec![CoreEvent {
                kind: "zadd",
                key: key.clone(),
            }],
            ..CommandEffects::default()
        },
    }
}
