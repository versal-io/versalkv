
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use command_macros::command;

fn copy_get_keys(args: &[Bytes]) -> Vec<(&[u8], Vec<&'static str>)> {
    let mut result = Vec::new();
    if !args.is_empty() {
        result.push((args[0].as_ref(), vec!["RO", "access"]));
    }
    if args.len() >= 2 {
        result.push((args[1].as_ref(), vec!["OW", "update"]));
    }
    result
}

#[command("COPY", runtime, get_keys = copy_get_keys)]
pub fn copy(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    if args.args.len() < 2 {
        return CommandOutcome::reply(CommandError::wrong_arity("copy"));
    }

    let mut replace = false;
    let mut db_idx = runtime.selected_db();

    let mut i = 2usize;
    while i < args.args.len() {
        match String::from_utf8_lossy(&args.args[i])
            .to_ascii_uppercase()
            .as_str()
        {
            "REPLACE" => replace = true,
            "DB" => {
                i += 1;
                if i >= args.args.len() {
                    return CommandOutcome::reply(CommandError::syntax_error());
                }
                db_idx = match std::str::from_utf8(&args.args[i])
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
            }
            _ => return CommandOutcome::reply(CommandError::syntax_error()),
        }
        i += 1;
    }

    let src_db = runtime.selected_db();
    if let Err(error) = runtime.ensure_db_access(db_idx, "copy") {
        return CommandOutcome::reply(error);
    }

    let key = &args.args[0];
    let dst_key = &args.args[1];

    let (val, expire, hfe) = {
        let src = runtime.db_mut_at(src_db);
        match src.and_then(|db| db.clone_entry(key)) {
            Some(entry) => entry,
            None => return skipped(),
        }
    };

    let target_db = runtime
        .db_mut_at(db_idx)
        .expect("database access was validated above");
    if !replace && target_db.exists(dst_key) {
        return skipped();
    }

    target_db.set(dst_key.to_vec(), val);
    if let Some(exp) = expire {
        target_db.set_expire(dst_key, exp);
    }
    for (field, ts) in &hfe {
        target_db.set_hash_field_expire(dst_key, field, *ts);
    }

    CommandOutcome {
        reply: Value::integer(1),
        effects: CommandEffects {
            changed_keys: vec![(db_idx, dst_key.clone())],
            events: vec![CoreEvent {
                kind: "copy_to",
                key: dst_key.clone(),
            }],
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
