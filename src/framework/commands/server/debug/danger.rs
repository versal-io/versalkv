use crate::engine::command::context::CommandConn;
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::{CommandArgs, CommandError};

pub(super) fn debug_segfault(args: &CommandArgs) -> Value {
    if args.args.len() != 1 {
        return CommandError::syntax_error();
    }
    crate::foundation::io::sys::signal::crash_via_readonly_page();
    std::process::abort();
}

pub(super) fn flush_diagnostic_query_line(conn: &dyn CommandConn, ctx: &ServerContext) {
    let snapshot = conn.diagnostic_query_snapshot();
    let rendered = if ctx.config().server.hide_user_data_from_log {
        "redacted".to_string()
    } else {
        String::from_utf8_lossy(snapshot).replace(['\r', '\n'], " ")
    };
    let line = format!("Query buffer: {rendered}\n");
    let mut stdout = std::io::stdout().lock();
    let _ = std::io::Write::write_all(&mut stdout, line.as_bytes());
    let _ = std::io::Write::flush(&mut stdout);
}

pub(super) fn debug_panic(args: &CommandArgs) -> Value {
    if args.args.len() != 1 {
        return CommandError::syntax_error();
    }
    panic!("DEBUG PANIC requested");
}

pub(super) fn debug_assert(args: &CommandArgs) -> Value {
    if args.args.len() != 1 {
        return CommandError::syntax_error();
    }
    crate::server_log!(warning, "ASSERTION FAILED: DEBUG ASSERT");
    // Route assertions through the process panic hook so use-exit-on-panic
    // controls both C-style panic and assert terminations.
    panic!("DEBUG ASSERT requested");
}

pub(super) fn debug_oom(args: &CommandArgs) -> Value {
    if args.args.len() != 1 {
        return CommandError::syntax_error();
    }
    std::process::abort();
}

pub(super) fn debug_restart(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
    graceful: bool,
) -> Value {
    if args.args.len() > 2 {
        return CommandError::syntax_error();
    }
    let delay_ms = match args.args.get(1) {
        Some(raw) => match std::str::from_utf8(raw)
            .ok()
            .and_then(|s| s.parse::<i64>().ok())
        {
            Some(v) => v.max(0) as u64,
            None => return Value::error("ERR value is not an integer or out of range"),
        },
        None => 0,
    };
    let intent = crate::framework::context::RestartIntent {
        delay_ms,
        graceful,
        config_rewrite: graceful,
    };
    ctx.request_server_restart(intent);
    conn.set_custom_reply(true);
    Value::Null
}
