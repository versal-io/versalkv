use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("SISMEMBER", runtime)]
pub fn sismember(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let requires_existing = match args.args.as_slice() {
        [_, _] => false,
        [_, _, option] if option.eq_ignore_ascii_case(b"XX") => true,
        _ => return CommandOutcome::reply(CommandError::syntax_error()),
    };
    let Some(db) = runtime.db_mut() else {
        return CommandOutcome::reply(CommandError::db_index_out_of_range());
    };
    CommandOutcome::reply(match db.get_set(&args.args[0]) {
        Ok(Some(s)) => Value::integer(if s.data.contains_key(&args.args[1]) { 1 } else { 0 }),
        Ok(None) => Value::integer(if requires_existing { -1 } else { 0 }),
        Err(_) => CommandError::wrong_type(),
    })
}
