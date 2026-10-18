use super::basic::parse_score;
use super::basic::score_value;
use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("ZINCRBY", runtime)]
pub fn zincrby(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let resp3 = runtime.resp3();
    let increment = match parse_score(&args.args[1]) {
        Some(s) => s,
        None => return CommandOutcome::reply(Value::error("ERR value is not a valid float")),
    };
    let (max_entries, max_value_size) = runtime.zset_encoding_limits();
    let db_idx = runtime.selected_db();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    let key = &args.args[0];
    let member = &args.args[2];
    let current = match db.get_zset_object(key) {
        Ok(Some(zset)) => zset.zset_score(member),
        Ok(None) => None,
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
    };
    let new_score = match current {
        Some(score) => score + increment,
        None => increment,
    };
    if new_score.is_nan() {
        return CommandOutcome::reply(Value::error(
            "ERR resulting score is not a number (NaN)",
        ));
    }
    if db
        .zset_add_with_limits(key, member, new_score, max_entries, max_value_size)
        .is_err()
    {
        return CommandOutcome::reply(CommandError::wrong_type());
    }
    CommandOutcome {
        reply: score_value(new_score, resp3),
        effects: CommandEffects {
            changed_keys: vec![(db_idx, key.clone())],
            events: vec![CoreEvent {
                kind: "zincr",
                key: key.clone(),
            }],
            ..CommandEffects::default()
        },
    }
}
