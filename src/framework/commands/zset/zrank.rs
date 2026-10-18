use super::basic::score_value;
use crate::engine::core::{CommandOutcome, Value};
use crate::framework::commands::command_runtime::CommandRuntime;
use crate::framework::{CommandArgs, CommandError};
use command_macros::command;

#[command("ZRANK", runtime)]
pub fn zrank(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> CommandOutcome {
    CommandOutcome::reply(zrank_reply(runtime, args))
}

fn zrank_reply(runtime: &mut dyn CommandRuntime, args: &CommandArgs) -> Value {
    let resp3 = runtime.resp3();
    let mut withscore = false;
    if args.args.len() == 3 {
        if args.args[2].eq_ignore_ascii_case(b"withscore") {
            withscore = true;
        } else {
            return CommandError::syntax_error();
        }
    } else if args.args.len() != 2 {
        return CommandError::syntax_error();
    }
    let db = match runtime.db_mut() {
        Some(d) => d,
        None => return CommandError::db_index_out_of_range(),
    };
    let zset = match db.get_zset_object(&args.args[0]) {
        Ok(None) => {
            if withscore {
                return Value::null_array();
            } else {
                return Value::null();
            }
        }
        Ok(Some(zset)) => zset,
        Err(_) => return CommandError::wrong_type(),
    };
    let member = &args.args[1];
    let score = match zset.zset_score(member) {
        Some(s) => s,
        None => {
            if withscore {
                return Value::null_array();
            } else {
                return Value::null();
            }
        }
    };
    match zset.zset_rank(member) {
        Some(r) => {
            if withscore {
                Value::Array(vec![Value::integer(r as i64), score_value(score, resp3)])
            } else {
                Value::integer(r as i64)
            }
        }
        None => {
            if withscore {
                Value::null_array()
            } else {
                Value::null()
            }
        }
    }
}
