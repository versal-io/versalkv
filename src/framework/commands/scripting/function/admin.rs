use crate::engine::extension::script::ScriptError;
use crate::engine::command::context::CommandConn;
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::{CommandArgs, CommandError};

/// FUNCTION DELETE <library_name>
pub(super) fn function_delete(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    if args.args.len() < 2 {
        return CommandError::wrong_arity("function|delete");
    }

    let lib_name = match std::str::from_utf8(&args.args[1]) {
        Ok(s) => s,
        Err(_) => return Value::error("ERR Library name must be a valid string"),
    };

    if ctx.script_mut().function_registry.delete_library(lib_name) {
        propagate_verbatim(conn, args);
        Value::ok()
    } else {
        Value::error("ERR Library not found".to_string())
    }
}

/// FUNCTION FLUSH [ASYNC | SYNC]
pub(super) fn function_flush(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    if args.args.len() > 1 {
        let opt = match std::str::from_utf8(&args.args[1]) {
            Ok(s) => s.to_ascii_uppercase(),
            Err(_) => return Value::error("ERR Unknown option"),
        };
        if opt != "ASYNC" && opt != "SYNC" {
            return Value::error("ERR FUNCTION FLUSH only supports SYNC|ASYNC option");
        }
        if args.args.len() > 2 {
            return Value::error(
                "ERR unknown subcommand or wrong number of arguments for 'flush'. Try FUNCTION HELP.",
            );
        }
    }
    ctx.script_mut().function_registry.flush();
    propagate_verbatim(conn, args);
    Value::ok()
}

fn propagate_verbatim(conn: &mut dyn CommandConn, args: &CommandArgs) {
    let mut parts: Vec<&[u8]> = Vec::with_capacity(args.args.len() + 1);
    parts.push(b"FUNCTION");
    for a in &args.args {
        parts.push(a.as_ref());
    }
    conn.record_custom_propagation(crate::engine::distributed::replication::protocol::encode_command(&parts));
}

/// FUNCTION KILL
pub(super) fn function_kill(
    _conn: &mut dyn CommandConn,
    _args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    super::super::script_cmd::request_script_kill(
        ctx,
        super::super::script_cmd::KillCommand::Function,
    )
}

/// FUNCTION RESTORE <payload> [FLUSH | APPEND | REPLACE]
pub(super) fn function_restore(
    _conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    if ctx.maxmemory_state().is_over_limit() {
        return Value::error("OOM command not allowed when used memory > 'maxmemory'.");
    }

    if args.args.len() < 2 || args.args.len() > 3 {
        return Value::error(
            "ERR unknown subcommand or wrong number of arguments for 'restore'. Try FUNCTION HELP.",
        );
    }

    let payload = &args.args[1];

    let mut flush_first = false;
    let mut replace_mode = false;
    if args.args.len() > 2 {
        let opt = match std::str::from_utf8(&args.args[2]) {
            Ok(s) => s.to_uppercase(),
            Err(_) => return Value::error("ERR Unknown option"),
        };
        if opt == "FLUSH" {
            flush_first = true;
        } else if opt == "REPLACE" {
            replace_mode = true;
        } else if opt != "APPEND" {
            return Value::error("ERR Unknown option");
        }
    }

    if flush_first {
        ctx.script_mut().function_registry.flush();
    }

    let mut replaced_libs: Vec<crate::engine::extension::function::FunctionLib> = Vec::new();
    if replace_mode {
        let payload_str = String::from_utf8_lossy(payload);
        let mut pos = 0;
        let bytes = payload_str.as_bytes();
        while pos < bytes.len() {
            if bytes[pos] == b'\n' {
                pos += 1;
                continue;
            }
            let name_end = bytes[pos..]
                .iter()
                .position(|&b| b == b'\n')
                .unwrap_or(bytes.len() - pos);
            if name_end == 0 {
                break;
            }
            let lib_name = String::from_utf8_lossy(&bytes[pos..pos + name_end]).to_string();
            if let Some(lib) = ctx.script_mut().function_registry.take_library(&lib_name) {
                replaced_libs.push(lib);
            }
            pos += name_end + 1;
            let len_end = bytes[pos..]
                .iter()
                .position(|&b| b == b'\n')
                .unwrap_or(bytes.len() - pos);
            if len_end == 0 {
                break;
            }
            let len_str = String::from_utf8_lossy(&bytes[pos..pos + len_end]).to_string();
            let code_len: usize = len_str.parse().unwrap_or(0);
            pos += len_end + 1 + code_len;
            if pos < bytes.len() && bytes[pos] == b'\n' {
                pos += 1;
            }
        }
    }

    let result = ctx.script_mut().function_registry.restore(payload);
    match result {
        Ok(_count) => Value::ok(),
        Err(e) => {
            for lib in replaced_libs {
                ctx.script_mut().function_registry.restore_library(lib);
            }
            match e {
                ScriptError::Compile(msg) => {
                    Value::error(format!("ERR Error compiling function: {}", msg))
                }
                ScriptError::Restore(_) => {
                    Value::error("ERR DUMP payload version or checksum are wrong".to_string())
                }
                _ => e.into_value(),
            }
        }
    }
}
