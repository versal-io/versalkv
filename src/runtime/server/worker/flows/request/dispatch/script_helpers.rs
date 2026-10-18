use crate::engine::command::{CmdFlags, CommandArgs, CMD_MAY_REPLICATE, CMD_WRITE};
use crate::engine::command::{CMD_DENYOOM, CMD_STALE};
use crate::engine::extension::script::{
    SCRIPT_ALLOW_OOM, SCRIPT_ALLOW_STALE, SCRIPT_NO_WRITES, SCRIPT_READ_ONLY,
};

pub(crate) fn is_ro_script(
    name: &str,
    args: &CommandArgs,
    ctx: &crate::framework::context::ServerContext,
) -> bool {
    match name {
        "eval" | "evalsha" | "eval_ro" | "evalsha_ro" => {
            if args.args.is_empty() {
                return false;
            }
            let script_body = if matches!(name, "evalsha" | "evalsha_ro") {
                let sha = match std::str::from_utf8(&args.args[0]) {
                    Ok(s) => s,
                    Err(_) => return false,
                };
                match ctx.script().script_cache.peek(sha) {
                    Some(body) => body,
                    None => return false,
                }
            } else {
                match std::str::from_utf8(&args.args[0]) {
                    Ok(s) => s,
                    Err(_) => return false,
                }
            };
            script_has_no_writes_flag(script_body)
        }
        "fcall" | "fcall_ro" => {
            if args.args.is_empty() {
                return false;
            }
            let func_name = match std::str::from_utf8(&args.args[0]) {
                Ok(s) => s,
                Err(_) => return false,
            };
            match ctx.script().function_registry.find_function(func_name) {
                Some(func) => (func.flags & SCRIPT_NO_WRITES) != 0,
                None => false,
            }
        }
        _ => false,
    }
}

/// Apply invocation-dependent script flags, following C
/// `getCommandFlags()` plus `evalGetCommandFlags()`/`fcallGetCommandFlags()`.
/// Unknown scripts/functions and legacy EVAL bodies keep their static flags;
/// once declared flags are available, `no-writes` clears write propagation
/// while every other script is treated as write-capable.
pub(crate) fn effective_script_command_flags(
    name: &str,
    base_flags: CmdFlags,
    args: &CommandArgs,
    ctx: &crate::framework::context::ServerContext,
) -> CmdFlags {
    let name = name.to_ascii_lowercase();
    let script_flags = match name.as_str() {
        "eval" | "eval_ro" => args
            .args
            .first()
            .and_then(|body| std::str::from_utf8(body).ok())
            .and_then(extract_valid_shebang_flags),
        "evalsha" | "evalsha_ro" => args
            .args
            .first()
            .and_then(|sha| std::str::from_utf8(sha).ok())
            .and_then(|sha| ctx.script().script_cache.peek(sha))
            .and_then(extract_valid_shebang_flags),
        "fcall" | "fcall_ro" => args
            .args
            .first()
            .and_then(|func| std::str::from_utf8(func).ok())
            .and_then(|func| ctx.script().function_registry.find_function(func))
            .map(|func| func.flags),
        _ => None,
    };
    let Some(script_flags) = script_flags else {
        return base_flags;
    };

    let mut flags = base_flags & !(CMD_STALE | CMD_DENYOOM | CMD_WRITE | CMD_MAY_REPLICATE);
    let no_writes = script_flags & (SCRIPT_NO_WRITES | SCRIPT_READ_ONLY) != 0;
    if script_flags & SCRIPT_ALLOW_OOM == 0 && !no_writes {
        flags |= CMD_DENYOOM;
    }
    if !no_writes {
        flags |= CMD_WRITE;
    }
    if script_flags & SCRIPT_ALLOW_STALE != 0 {
        flags |= CMD_STALE;
    }
    flags
}

fn extract_valid_shebang_flags(
    body: &str,
) -> Option<crate::engine::extension::script::ScriptFlags> {
    let (_, flags, has_shebang) = crate::engine::extension::lua::parse_shebang(body, 0).ok()?;
    has_shebang.then_some(flags)
}

pub(crate) fn script_has_no_writes_flag(script: &str) -> bool {
    let Some(rest) = script.strip_prefix("#!") else {
        return false;
    };
    let shebang_line = match rest.find('\n') {
        Some(end) => &rest[..end],
        None => rest,
    };
    for part in shebang_line.split_whitespace() {
        if let Some(flags) = part.strip_prefix("flags=") {
            for flag in flags.split(',') {
                if flag == "no-writes" {
                    return true;
                }
            }
        }
    }
    false
}

pub(crate) fn is_eval_without_shebang(name: &str, args: &CommandArgs) -> bool {
    if !name.eq_ignore_ascii_case("eval") {
        return false;
    }
    match args
        .args
        .first()
        .and_then(|script| std::str::from_utf8(script).ok())
    {
        Some(script) => !script.starts_with("#!"),
        None => false,
    }
}
