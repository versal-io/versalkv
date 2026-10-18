use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("HMSET", runtime)]
pub fn hmset(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    if args.args.len().is_multiple_of(2) {
        return CommandOutcome::reply(CommandError::wrong_arity("hmset"));
    }
    let db_idx = runtime.selected_db();
    let read_expired_values = runtime.read_expired_values();
    let (max_entries, max_value_size) = runtime.hash_encoding_limits();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    let key = &args.args[0];
    let mut i = 1;
    let mut expired_overwritten = 0u64;
    while i + 1 < args.args.len() {
        let field = &args.args[i];
        if !read_expired_values && db.clear_expired_hash_field_ttl(key, field) {
            expired_overwritten += 1;
        }
        if db
            .hash_set_with_limits(key, field, &args.args[i + 1], max_entries, max_value_size)
            .is_err()
        {
            return CommandOutcome::reply(CommandError::wrong_type());
        }
        db.remove_hash_field_expire(key, field);
        i += 2;
    }
    let mut events = Vec::new();
    if expired_overwritten > 0 {
        events.push(CoreEvent {
            kind: "hexpired",
            key: key.clone(),
        });
    }
    events.push(CoreEvent {
        kind: "hset",
        key: key.clone(),
    });
    CommandOutcome {
        reply: Value::ok(),
        effects: CommandEffects {
            changed_keys: vec![(db_idx, key.clone())],
            events,
            expired_hash_fields: expired_overwritten,
            ..CommandEffects::default()
        },
    }
}
