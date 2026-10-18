use mlua::prelude::*;

use super::{ScriptRunCtx, ERR_RAISE_FIELD};

pub(super) fn has_err_code_prefix(msg: &str) -> bool {
    const KNOWN_CODES: &[&str] = &[
        "ERR ",
        "WRONGTYPE ",
        "NOSCRIPT ",
        "MOVED ",
        "CLUSTERDOWN ",
        "LOADING ",
        "READONLY ",
        "MASTERDOWN ",
        "NOAUTH ",
        "WRONGPASS ",
        "NOPERM ",
        "BUSY ",
        "NOPROTO ",
        "NOKEY ",
        "EXECABORT ",
        "NOSCRIPT ",
        "CROSSSLOT ",
        "TRYAGAIN ",
        "ASK ",
        "UNBLOCKED ",
        "OOM ",
        "NOREPLICAS ",
        "MISCONF ",
    ];
    KNOWN_CODES.iter().any(|code| msg.starts_with(code))
}

fn error_code_from_msg(msg: &str) -> String {
    msg.split_whitespace().next().unwrap_or("ERR").to_string()
}

pub(super) fn defer_pcall_error(script_ctx: &mut ScriptRunCtx, msg: &str) {
    script_ctx
        .deferred_error_replies
        .push(error_code_from_msg(msg));
}

fn sanitize_error_text(s: &str) -> String {
    s.chars()
        .map(|c| if c == '\r' || c == '\n' { ' ' } else { c })
        .collect()
}

pub(super) fn format_top_level_lua_error(raw: &str, script: &str) -> String {
    let mut msg = raw.trim().to_string();
    if msg.starts_with("Table(") || msg == "Nil" {
        msg = "unknown error".to_string();
    } else if let Some(rest) = msg.strip_prefix("user_script:") {
        if let Some((line, tail)) = rest.split_once(':') {
            if let Ok(line) = line.parse::<usize>() {
                let adjusted = line.saturating_sub(3).max(1);
                msg = format!("user_script:{}:{}", adjusted, tail);
            }
        }
    }

    let script = sanitize_error_text(script);
    if has_err_code_prefix(&msg) {
        format!("{} script: {}", sanitize_error_text(&msg), script)
    } else {
        format!("ERR {} script: {}", sanitize_error_text(&msg), script)
    }
}

pub(super) fn make_raise_err(lua: &Lua, msg: &str) -> Result<LuaValue, mlua::Error> {
    let full = if has_err_code_prefix(msg) {
        msg.to_string()
    } else {
        format!("ERR {}", msg)
    };
    let t = lua.create_table()?;
    t.set(ERR_RAISE_FIELD, true)?;
    t.set("err", full.as_str())?;
    Ok(LuaValue::Table(t))
}

pub(super) fn lua_value_to_err_string(val: &LuaValue) -> String {
    match val {
        LuaValue::Table(t) => {
            if let Ok(err_field) = t.get::<LuaValue>("err") {
                match err_field {
                    LuaValue::String(s) => s.to_str().map(|s| s.to_string()).unwrap_or_default(),
                    LuaValue::Integer(n) => n.to_string(),
                    _ => format!("{:?}", err_field),
                }
            } else {
                format!("{:?}", val)
            }
        }
        LuaValue::String(s) => s.to_str().map(|s| s.to_string()).unwrap_or_default(),
        _ => format!("{:?}", val),
    }
}
