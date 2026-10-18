use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use command_macros::command;

#[command("TTL", runtime)]
pub fn ttl(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    ttl_impl(runtime, args, false)
}

pub(crate) fn ttl_impl(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
    milliseconds: bool,
) -> CommandOutcome {
    let [key] = args.args.as_slice() else {
        return CommandOutcome::reply(CommandError::wrong_arity(if milliseconds {
            "pttl"
        } else {
            "ttl"
        }));
    };
    let read_expired_values = runtime.read_expired_values();
    let suppress_lazy_expire = runtime.suppress_lazy_expire();
    let now_ms = runtime.now_ms();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    db.set_no_lazy_expire(suppress_lazy_expire);
    let ttl_ms = if read_expired_values {
        match db.get_allow_expired(key) {
            Some(_) => db
                .get_expire(key)
                .map_or(-1, |deadline| deadline.saturating_sub(now_ms) as i64),
            None => -2,
        }
    } else {
        db.ttl_ms_at(key, now_ms)
    };
    db.set_no_lazy_expire(false);
    CommandOutcome::reply(Value::integer(if milliseconds || ttl_ms < 0 {
        ttl_ms
    } else {
        (ttl_ms + 500) / 1_000
    }))
}
