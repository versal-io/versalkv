use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::commands::zset::basic::{parse_score, score_value};
use command_macros::command;

#[command("ZADD", runtime)]
pub fn zadd(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let mut nx = false;
    let mut xx = false;
    let mut ch = false;
    let mut incr = false;
    let mut gt = false;
    let mut lt = false;
    let mut index = 1;
    while index < args.args.len() {
        let option = &args.args[index];
        if option.eq_ignore_ascii_case(b"nx") {
            nx = true;
        } else if option.eq_ignore_ascii_case(b"xx") {
            xx = true;
        } else if option.eq_ignore_ascii_case(b"ch") {
            ch = true;
        } else if option.eq_ignore_ascii_case(b"incr") {
            incr = true;
        } else if option.eq_ignore_ascii_case(b"gt") {
            gt = true;
        } else if option.eq_ignore_ascii_case(b"lt") {
            lt = true;
        } else {
            break;
        }
        index += 1;
    }
    if nx && xx {
        return CommandOutcome::reply(Value::error(
            "ERR NX and XX at the same time are not compatible",
        ));
    }
    if nx && (gt || lt) {
        return CommandOutcome::reply(Value::error("ERR GT and LT can't be used with NX"));
    }
    if gt && lt {
        return CommandOutcome::reply(Value::error(
            "ERR GT and LT at the same time are not compatible",
        ));
    }
    let remaining = args.args.len().saturating_sub(index);
    if (incr && remaining != 2) || (!incr && (remaining == 0 || !remaining.is_multiple_of(2))) {
        return CommandOutcome::reply(CommandError::syntax_error());
    }
    let pairs = args.args[index..]
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| parse_score(&pair[0]).map(|score| (score, pair[1].clone())))
        .collect::<Option<Vec<_>>>();
    let Some(pairs) = pairs else {
        return CommandOutcome::reply(Value::error("ERR value is not a valid float"));
    };
    let db_index = runtime.selected_db();
    let resp3 = runtime.resp3();
    let (max_entries, max_value_size) = runtime.zset_encoding_limits();
    let key = args.args[0].clone();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    let event = || CommandEffects {
        changed_keys: vec![(db_index, key.clone())],
        events: vec![CoreEvent {
            kind: "zadd",
            key: key.clone(),
        }],
        ..CommandEffects::default()
    };
    if incr {
        let (increment, member) = &pairs[0];
        let current = match db.get_zset_object(&key) {
            Ok(Some(zset)) => zset.zset_score(member),
            Ok(None) => None,
            Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
        };
        match current {
            Some(score) => {
                if nx {
                    return CommandOutcome::reply(Value::null());
                }
                let new_score = score + increment;
                if new_score.is_nan() {
                    return CommandOutcome::reply(Value::error(
                        "ERR resulting score is not a number (NaN)",
                    ));
                }
                if (gt && new_score <= score) || (lt && new_score >= score) {
                    return CommandOutcome::reply(Value::null());
                }
                let _ =
                    db.zset_add_with_limits(&key, member, new_score, max_entries, max_value_size);
                return CommandOutcome {
                    reply: score_value(new_score, resp3),
                    effects: event(),
                };
            }
            None => {
                if xx {
                    return CommandOutcome::reply(Value::null());
                }
                if db
                    .zset_add_with_limits(&key, member, *increment, max_entries, max_value_size)
                    .is_err()
                {
                    return CommandOutcome::reply(CommandError::wrong_type());
                }
                return CommandOutcome {
                    reply: score_value(*increment, resp3),
                    effects: event(),
                };
            }
        }
    }
    let mut added = 0i64;
    let mut updated = 0i64;
    for (score, member) in pairs {
        let current = match db.get_zset_object(&key) {
            Ok(Some(zset)) => zset.zset_score(&member),
            Ok(None) => None,
            Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
        };
        match current {
            Some(old_score) => {
                if nx || (gt && score <= old_score) || (lt && score >= old_score) {
                    continue;
                }
                if score != old_score {
                    if db
                        .zset_add_with_limits(&key, &member, score, max_entries, max_value_size)
                        .is_err()
                    {
                        return CommandOutcome::reply(CommandError::wrong_type());
                    }
                    updated += 1;
                }
            }
            None => {
                if xx {
                    continue;
                }
                if db
                    .zset_add_with_limits(&key, &member, score, max_entries, max_value_size)
                    .is_err()
                {
                    return CommandOutcome::reply(CommandError::wrong_type());
                }
                added += 1;
            }
        }
    }
    CommandOutcome {
        reply: Value::integer(if ch { added + updated } else { added }),
        effects: event(),
    }
}
