
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("MOVE", runtime)]
pub fn r#move(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    if args.args.len() < 2 {
        return CommandOutcome::reply(CommandError::wrong_arity("move"));
    }
    let db_idx = match std::str::from_utf8(&args.args[1])
        .ok()
        .and_then(|s| s.trim().parse::<u32>().ok())
    {
        Some(v) => v,
        None => {
            return CommandOutcome::reply(Value::error(
                "ERR value is not an integer or out of range",
            ))
        }
    };

    let src_db = runtime.selected_db();

    if src_db == db_idx {
        return CommandOutcome::reply(Value::error(
            "ERR source and destination objects are the same",
        ));
    }

    if let Err(error) = runtime.ensure_db_access(db_idx, "move") {
        return CommandOutcome::reply(error);
    }

    let key = &args.args[0];

    let replace = if args.args.len() >= 3 {
        let opt = match std::str::from_utf8(&args.args[2]) {
            Ok(s) => s,
            Err(_) => return CommandOutcome::reply(Value::error("ERR syntax error")),
        };
        if opt.eq_ignore_ascii_case("replace") {
            true
        } else {
            return CommandOutcome::reply(Value::error("ERR syntax error"));
        }
    } else {
        false
    };

    if args.args.len() > 3 {
        return CommandOutcome::reply(Value::error("ERR syntax error"));
    }

    if !runtime.db_mut_at(src_db).is_some_and(|db| db.exists(key)) {
        return skipped();
    }

    if runtime.db_mut_at(db_idx).is_some_and(|db| db.exists(key)) {
        if !replace {
            return skipped();
        }
        runtime.db_mut_at(db_idx).unwrap().del(key);
    }

    let (val, expire, hfe) = match runtime.db_mut_at(src_db).and_then(|db| db.take(key)) {
        Some(v) => v,
        None => return skipped(),
    };

    let target_db = runtime.db_mut_at(db_idx).unwrap();
    target_db.set(key.to_vec(), val);
    if let Some(exp) = expire {
        target_db.set_expire(key, exp);
    }
    target_db.set_hash_field_expires_map(key.to_vec(), hfe);

    CommandOutcome {
        reply: Value::integer(1),
        effects: CommandEffects {
            changed_keys: vec![(src_db, key.clone()), (db_idx, key.clone())],
            db_events: vec![
                (
                    src_db,
                    CoreEvent {
                        kind: "move_from",
                        key: key.clone(),
                    },
                ),
                (
                    db_idx,
                    CoreEvent {
                        kind: "move_to",
                        key: key.clone(),
                    },
                ),
            ],
            ..CommandEffects::default()
        },
    }
}

fn skipped() -> CommandOutcome {
    CommandOutcome {
        reply: Value::integer(0),
        effects: CommandEffects {
            skip_propagate: true,
            ..CommandEffects::default()
        },
    }
}
