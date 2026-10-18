use crate::engine::core::{CommandEffects, CommandOutcome, CoreEvent, Value};
use crate::foundation::io::store::structure::string::encoding::int_encodable;
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("HGETDEL", runtime)]
pub fn hgetdel(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let db_idx = runtime.selected_db();
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandOutcome::reply(CommandError::db_index_out_of_range()),
    };
    let key = &args.args[0];

    let fields_token = std::str::from_utf8(&args.args[1]).unwrap_or("");
    if !fields_token.eq_ignore_ascii_case("fields") {
        return CommandOutcome::reply(CommandError::syntax_error());
    }

    let num_fields = match int_encodable(&args.args[2]) {
        Some(n) => n,
        None => {
            return CommandOutcome::reply(Value::error(
                "ERR value is not an integer or out of range",
            ))
        }
    };

    if num_fields <= 0 || num_fields as usize != args.args.len() - 3 {
        return CommandOutcome::reply(Value::error(
            "numfields should be greater than 0 and match the provided number of fields",
        ));
    }

    let num_fields = num_fields as usize;

    let (reply, deleted) = match db.get_hash_object_mut(key) {
        Err(_) => return CommandOutcome::reply(CommandError::wrong_type()),
        Ok(None) => {
            let mut out = Vec::with_capacity(num_fields);
            for _ in 0..num_fields {
                out.push(Value::null());
            }
            (Value::Array(out), 0)
        }
        Ok(Some(hash)) => {
            let mut out = Vec::with_capacity(num_fields);
            let mut deleted = 0;
            for i in 0..num_fields {
                let field = &args.args[3 + i];
                match hash.hash_get(field) {
                    Some(value) => {
                        hash.hash_del(field);
                        out.push(Value::bulk(value));
                        deleted += 1;
                    }
                    None => out.push(Value::null()),
                }
            }
            if hash.hash_is_empty() {
                db.del(key);
            }
            (Value::Array(out), deleted)
        }
    };
    if deleted == 0 {
        return CommandOutcome::reply(reply);
    }
    CommandOutcome {
        reply,
        effects: CommandEffects {
            changed_keys: vec![(db_idx, key.clone())],
            events: vec![CoreEvent {
                kind: "hdel",
                key: key.clone(),
            }],
            ..CommandEffects::default()
        },
    }
}
