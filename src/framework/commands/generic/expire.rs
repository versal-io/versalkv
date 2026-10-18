use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use command_macros::command;

#[command("EXPIRE", runtime)]
pub fn expire(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    expire_impl(runtime, args, 1_000, false, "expire")
}

pub(crate) fn expire_impl(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
    unit_ms: i64,
    absolute: bool,
    name: &'static str,
) -> CommandOutcome {
    let Some((key, rest)) = args.args.split_first() else {
        return CommandOutcome::reply(CommandError::wrong_arity(name));
    };
    let Some((timeout, flags)) = rest.split_first() else {
        return CommandOutcome::reply(CommandError::wrong_arity(name));
    };
    let Some(timeout) = std::str::from_utf8(timeout)
        .ok()
        .and_then(|value| value.trim().parse::<i64>().ok())
    else {
        return CommandOutcome::reply(Value::error("ERR value is not an integer or out of range"));
    };
    let mut nx = false;
    let mut xx = false;
    let mut gt = false;
    let mut lt = false;
    for flag in flags {
        if flag.eq_ignore_ascii_case(b"nx") {
            nx = true;
        } else if flag.eq_ignore_ascii_case(b"xx") {
            xx = true;
        } else if flag.eq_ignore_ascii_case(b"gt") {
            gt = true;
        } else if flag.eq_ignore_ascii_case(b"lt") {
            lt = true;
        } else {
            return CommandOutcome::reply(Value::error(format!(
                "ERR Unsupported option {}",
                String::from_utf8_lossy(flag)
            )));
        }
    }
    if nx && (xx || gt || lt) {
        return CommandOutcome::reply(Value::error(
            "ERR NX and XX, GT or LT options at the same time are not compatible",
        ));
    }
    if gt && lt {
        return CommandOutcome::reply(Value::error(
            "ERR GT and LT options at the same time are not compatible",
        ));
    }
    let now_ms = runtime.now_ms();
    let Some(scaled_timeout) = timeout.checked_mul(unit_ms) else {
        return CommandOutcome::reply(Value::error(format!(
            "ERR invalid expire time in '{}' command",
            name
        )));
    };
    let Some(deadline) = (if absolute {
        Some(scaled_timeout)
    } else {
        (now_ms as i64).checked_add(scaled_timeout)
    }) else {
        return CommandOutcome::reply(Value::error(format!(
            "ERR invalid expire time in '{}' command",
            name
        )));
    };
    let db_index = runtime.selected_db();
    let import_mode = runtime.suppress_lazy_expire();
    let mark_local_expire = runtime.should_mark_replica_local_expire();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    db.set_no_lazy_expire(import_mode);
    if !db.exists(key) {
        db.set_no_lazy_expire(false);
        return CommandOutcome::reply(Value::integer(0));
    }
    let current = db.get_expire(key);
    let allowed = if nx {
        current.is_none()
    } else if xx && current.is_none() {
        false
    } else if gt {
        current.is_some_and(|current| deadline > current as i64)
    } else if lt {
        current.is_none_or(|current| deadline < current as i64)
    } else {
        true
    };
    if !allowed {
        db.set_no_lazy_expire(false);
        return CommandOutcome::reply(Value::integer(0));
    }
    let expired = deadline <= now_ms as i64;
    if expired && !import_mode {
        db.del(key);
        db.expired_deletions.push(key.to_vec());
    } else {
        db.set_expire(
            key,
            if expired {
                now_ms.saturating_sub(1)
            } else {
                deadline as u64
            },
        );
        if mark_local_expire {
            db.mark_replica_local_expire(key);
        }
    }
    db.set_no_lazy_expire(false);
    let mut effects = CommandEffects {
        changed_keys: vec![(db_index, key.clone())],
        events: vec![CoreEvent {
            kind: if expired { "del" } else { "expire" },
            key: key.clone(),
        }],
        ..CommandEffects::default()
    };
    if expired && !import_mode {
        effects.skip_propagate = true;
    }
    if !expired {
        effects.custom_propagations.push(vec![
            bytes::Bytes::from_static(b"PEXPIREAT"),
            key.clone(),
            bytes::Bytes::from(deadline.to_string()),
        ]);
    }
    CommandOutcome {
        reply: Value::integer(1),
        effects,
    }
}
