use crate::engine::extension::function::parse_engine_name;
use crate::engine::extension::script::ScriptError;
use crate::engine::command::context::CommandConn;
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::{CommandArgs, CommandError};

use super::is_valid_lib_name;

/// FUNCTION LOAD [REPLACE] <library_code>
pub(super) fn function_load(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    if ctx.maxmemory_state().is_over_limit() {
        return Value::error("OOM command not allowed when used memory > 'maxmemory'.");
    }

    let argc = args.args.len();
    if argc < 2 {
        return CommandError::wrong_arity("function|load");
    }

    let replace = std::str::from_utf8(&args.args[1])
        .map(|s| s.eq_ignore_ascii_case("REPLACE"))
        .unwrap_or(false);

    let code_idx = if replace { 2 } else { 1 };
    if code_idx >= argc {
        return CommandError::wrong_arity("function|load");
    }

    if replace && argc > 3 {
        return Value::error("ERR Unknown option given".to_string());
    }
    if !replace && argc > 2 {
        return Value::error("ERR Unknown option given".to_string());
    }

    let code = match std::str::from_utf8(&args.args[code_idx]) {
        Ok(s) => s,
        Err(_) => return Value::error("ERR Library code must be a valid string"),
    };

    let lib_name = match crate::engine::extension::lua::shebang::extract_shebang_name(code) {
        Ok(name) => name,
        Err(e) => return Value::error(e),
    };

    if !is_valid_lib_name(&lib_name) {
        return Value::error(format!(
            "ERR Library names can only contain letters, numbers, or underscores(_), got '{}'",
            lib_name
        ));
    }

    let engine_name = parse_engine_name(code);
    if ctx
        .script_mut()
        .function_registry
        .engine_mgr()
        .get(&engine_name)
        .is_none()
    {
        return Value::error(format!("ERR Engine '{}' not found", engine_name));
    }

    if !replace
        && ctx
            .script_mut()
            .function_registry
            .get_library(&lib_name)
            .is_some()
    {
        return Value::error(format!(
            "ERR Library '{}' already exists. Use REPLACE to overwrite",
            lib_name
        ));
    }

    let old_lib = if replace {
        ctx.script_mut().function_registry.take_library(&lib_name)
    } else {
        None
    };

    match ctx
        .script_mut()
        .function_registry
        .load_library(&lib_name, code)
    {
        Ok(()) => {
            let mut parts: Vec<&[u8]> = Vec::with_capacity(args.args.len() + 1);
            parts.push(b"FUNCTION");
            for a in &args.args {
                parts.push(a.as_ref());
            }
            conn.record_custom_propagation(
                crate::engine::distributed::replication::protocol::encode_command(&parts),
            );
            Value::Bulk(bytes::Bytes::copy_from_slice(lib_name.as_bytes()))
        }
        Err(ScriptError::Compile(msg)) if msg.starts_with("Unknown engine: ") => {
            let engine = msg.trim_start_matches("Unknown engine: ");
            if let Some(old) = old_lib {
                ctx.script_mut().function_registry.restore_library(old);
            }
            Value::error(format!("ERR Engine '{}' not found", engine))
        }
        Err(ScriptError::Compile(msg)) => {
            if let Some(old) = old_lib {
                ctx.script_mut().function_registry.restore_library(old);
            }
            Value::error(format!("ERR Error compiling function: {}", msg))
        }
        Err(e) => {
            if let Some(old) = old_lib {
                ctx.script_mut().function_registry.restore_library(old);
            }
            e.into_value()
        }
    }
}
