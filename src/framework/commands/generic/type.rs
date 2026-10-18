use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("TYPE", runtime)]
pub fn type_cmd(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    let name = match db.get(&args.args[0]) {
        Some(obj) => obj.type_name(),
        None => "none",
    };
    CommandOutcome::reply(Value::SimpleString(name.into()))
}
