use crate::engine::command::{CommandArgs, CommandError};
use crate::engine::core::CommandOutcome;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::commands::zset::basic::score_value;
use command_macros::command;

#[command("ZSCORE", runtime)]
pub fn zscore(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let [key, member] = args.args.as_slice() else {
        return CommandOutcome::reply(CommandError::wrong_arity("zscore"));
    };
    let resp3 = runtime.resp3();
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    match db.get_zset_object(key) {
        Ok(None) => CommandOutcome::reply(crate::engine::core::Value::null()),
        Ok(Some(zset)) => CommandOutcome::reply(
            zset.zset_score(member)
                .map_or_else(crate::engine::core::Value::null, |score| {
                    score_value(score, resp3)
                }),
        ),
        Err(_) => CommandOutcome::reply(CommandError::wrong_type()),
    }
}
