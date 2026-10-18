use crate::engine::command::context::CommandConn;
use crate::foundation::io::protocol::Value;
use crate::foundation::io::sys::time::mstime;
use crate::framework::context::{PauseAction, ServerContext};
use crate::framework::{CommandArgs, CommandError};

pub(super) fn client_setname(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    if args.args.len() < 2 {
        return CommandError::wrong_arity("client");
    }
    if args.args[1].iter().any(|byte| byte.is_ascii_whitespace()) {
        return Value::error(
            "ERR Client names cannot contain spaces, but also no other special characters",
        );
    }
    conn.set_name(Some(args.args[1].clone()));
    if let Some(ci) = ctx.client_info_mut(conn.token().0) {
        ci.name = String::from_utf8_lossy(&args.args[1]).to_string();
    }
    Value::ok()
}

pub(super) fn client_no_evict(conn: &mut dyn CommandConn, args: &CommandArgs) -> Value {
    if args.args.len() < 2 {
        return CommandError::wrong_arity("client");
    }
    let on = String::from_utf8_lossy(&args.args[1]);
    match on.as_ref() {
        "on" => {
            conn.set_no_evict(true);
            Value::ok()
        }
        "off" => {
            conn.set_no_evict(false);
            Value::ok()
        }
        _ => Value::error("ERR syntax error"),
    }
}

pub(super) fn client_pause(args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    if args.args.len() < 2 {
        return CommandError::wrong_arity("client");
    }
    let timeout_ms = match String::from_utf8_lossy(&args.args[1]).parse::<i64>() {
        Ok(n) if n >= 0 => n,
        Ok(_) => return Value::error("ERR timeout is negative"),
        Err(_) => return Value::error("ERR timeout is not an integer or out of range"),
    };
    let is_all = if args.args.len() >= 3 {
        let mode = String::from_utf8_lossy(&args.args[2]);
        if mode.eq_ignore_ascii_case("write") {
            false
        } else if mode.eq_ignore_ascii_case("all") {
            true
        } else {
            return Value::error("ERR CLIENT PAUSE mode must be WRITE or ALL");
        }
    } else {
        true
    };
    if args.args.len() > 3 {
        return Value::error("ERR syntax error");
    }
    let action = if is_all {
        PauseAction::All
    } else {
        PauseAction::Write
    };
    let end = mstime() + timeout_ms;

    let p = ctx.client_pause_mut();
    p.reason = "client_pause";
    let prev_action = p.action;
    let was_active = p.end_time_ms > mstime();
    if end > p.end_time_ms || action == PauseAction::All {
        p.end_time_ms = end;
    }
    if action == PauseAction::All || p.action != PauseAction::All {
        p.action = action;
    }
    let now_active = action != PauseAction::None;
    let less_restrictive =
        was_active && now_active && (prev_action == PauseAction::All && action == PauseAction::Write);
    if less_restrictive {
        ctx.set_pending_unpause(true);
    }
    ctx.dbs_mut().set_pause_skip_expire_all(now_active);
    Value::ok()
}
