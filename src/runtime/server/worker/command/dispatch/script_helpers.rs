use crate::engine::command::CommandArgs;
use crate::engine::extension::script::SCRIPT_NO_WRITES;

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
