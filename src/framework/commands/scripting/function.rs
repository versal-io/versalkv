
use command_macros::command;

use crate::engine::command::context::CommandConn;
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::{CommandArgs, CommandError};

#[path = "function/load.rs"]
mod load;
#[path = "function/introspect.rs"]
mod introspect;
#[path = "function/admin.rs"]
mod admin;

use admin::{function_delete, function_flush, function_kill, function_restore};
use introspect::{function_dump, function_help, function_list, function_stats};
use load::function_load;

#[command("FUNCTION")]
pub fn function(conn: &mut dyn CommandConn, args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    if args.args.is_empty() {
        return CommandError::wrong_arity("function");
    }

    let subcmd = match std::str::from_utf8(&args.args[0]) {
        Ok(s) => s.to_uppercase(),
        Err(_) => return Value::error("ERR Subcommand must be a valid string"),
    };

    match subcmd.as_str() {
        "LOAD" => function_load(conn, args, ctx),
        "DELETE" => function_delete(conn, args, ctx),
        "LIST" => function_list(conn, args, ctx),
        "FLUSH" => function_flush(conn, args, ctx),
        "STATS" => function_stats(conn, args, ctx),
        "KILL" => function_kill(conn, args, ctx),
        "HELP" => function_help(conn, args, ctx),
        "DUMP" => function_dump(conn, args, ctx),
        "RESTORE" => function_restore(conn, args, ctx),
        _ => Value::error(format!(
            "ERR unknown subcommand '{}'. Try FUNCTION HELP.",
            String::from_utf8_lossy(&args.args[0])
        )),
    }
}

fn is_valid_lib_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
}
