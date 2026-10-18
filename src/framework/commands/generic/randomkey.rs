use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::CommandArgs;
use command_macros::command;

#[command("RANDOMKEY", runtime)]
pub fn randomkey(
    runtime: &mut dyn CommandRuntime,
    _args: &CommandArgs,
) -> CommandOutcome {
    let no_lazy_expire = runtime.suppress_lazy_expire();
    runtime.reset_replication_selected_db();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(Value::null()),
    };
    db.set_no_lazy_expire(no_lazy_expire);
    let result = match db.random_key() {
        Some(key) => Value::bulk(key.to_vec()),
        None => Value::null(),
    };
    if !no_lazy_expire {
        db.delete_expired_with_grace();
    }
    db.set_no_lazy_expire(false);
    CommandOutcome::reply(result)
}
