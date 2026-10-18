use crate::engine::core::{CommandEffects, CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::commands::hyperloglog::common::{
    error_value, parse_hll, serialize_hll_as, HllEncoding, HLL_REGISTERS,
};
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("PFDEBUG", runtime)]
pub fn pfdebug(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    let mut effects = CommandEffects::default();
    let reply = pfdebug_reply(runtime, args, &mut effects);
    CommandOutcome { reply, effects }
}

fn pfdebug_reply(
    runtime: &mut dyn CommandRuntime,
    args: &CommandArgs,
    effects: &mut CommandEffects,
) -> Value {
    if args.args.is_empty() {
        return Value::error("ERR wrong number of arguments for 'pfdebug' command");
    }

    let subcmd = &args.args[0];
    let subcmd_str = match std::str::from_utf8(subcmd) {
        Ok(s) => s.to_uppercase(),
        Err(_) => return Value::error("ERR syntax error"),
    };

    match subcmd_str.as_str() {
        "ENCODING" => {
            if args.args.len() < 2 {
                return Value::error(
                    "ERR wrong number of arguments for 'pfdebug|encoding' command",
                );
            }
            let key = &args.args[1];
            let db = match runtime.db_mut() {
                Some(d) => d,
                None => return CommandError::db_index_out_of_range(),
            };
            match db.get_string(key) {
                Err(_) => CommandError::wrong_type(),
                Ok(Some(data)) => match parse_hll(data) {
                    Ok(state) if state.encoding == HllEncoding::Sparse => Value::bulk("sparse"),
                    Ok(_) => Value::bulk("dense"),
                    Err(err) => error_value(err),
                },
                Ok(None) => Value::error("ERR key does not exist"),
            }
        }
        "GETREG" => {
            if args.args.len() < 2 {
                return Value::error("ERR wrong number of arguments for 'pfdebug|getreg' command");
            }
            let key = &args.args[1];
            let db = match runtime.db_mut() {
                Some(d) => d,
                None => return CommandError::db_index_out_of_range(),
            };
            match db.get_string(key) {
                Err(_) => CommandError::wrong_type(),
                Ok(Some(data)) => match parse_hll(data) {
                    Ok(state) => Value::Array(
                        state
                            .registers()
                            .iter()
                            .map(|reg| Value::integer(*reg as i64))
                            .chain(std::iter::repeat(Value::integer(0)))
                            .take(HLL_REGISTERS)
                            .collect(),
                    ),
                    Err(err) => error_value(err),
                },
                Ok(None) => Value::error("ERR key does not exist"),
            }
        }
        "TODENSE" => {
            if args.args.len() < 2 {
                return Value::error("ERR wrong number of arguments for 'pfdebug|todense' command");
            }
            let key = &args.args[1];
            let db_idx = runtime.selected_db();
            let db = match runtime.db_mut() {
                Some(d) => d,
                None => return CommandError::db_index_out_of_range(),
            };
            let data = match db.get_string(key) {
                Err(_) => return CommandError::wrong_type(),
                Ok(Some(data)) => data,
                Ok(None) => return Value::error("ERR key does not exist"),
            };
            let state = match parse_hll(data) {
                Ok(state) => state,
                Err(err) => return error_value(err),
            };
            db.set(
                key.to_vec(),
                crate::foundation::io::store::Robj::string_raw(serialize_hll_as(&state, HllEncoding::Dense, true)),
            );
            effects.changed_keys.push((db_idx, key.clone()));
            Value::ok()
        }
        "SIMD" => {
            Value::ok()
        }
        _ => Value::error(format!(
            "ERR Unknown subcommand or wrong number of arguments for '{}' command",
            subcmd_str
        )),
    }
}
