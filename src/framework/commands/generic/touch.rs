use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::CommandArgs;
use command_macros::command;

#[command("TOUCH", runtime)]
pub fn touch(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(Value::error("ERR invalid DB")),
    };
    let mut count = 0i64;
    for key in &args.args {
        if db.touch_access_time(key) {
            count += 1;
        }
    }
    CommandOutcome::reply(Value::integer(count))
}
