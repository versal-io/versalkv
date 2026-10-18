
use command_macros::command;

use crate::framework::{CommandArgs, CommandError};
use crate::framework::context::ServerContext;
use crate::engine::extension::lua::reset_lua_runtime;
use crate::foundation::io::protocol::Value;
use crate::framework::commands::scripting::script_cache::ScriptCache;
use crate::engine::command::context::CommandConn;
use std::sync::atomic::{AtomicBool, Ordering};

static SCRIPT_DEBUG_ENABLED: AtomicBool = AtomicBool::new(false);

pub(crate) fn script_debug_enabled() -> bool {
    SCRIPT_DEBUG_ENABLED.load(Ordering::Relaxed)
}

#[command("SCRIPT")]
pub fn script_cmd(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    if args.args.is_empty() {
        return CommandError::wrong_arity("script");
    }

    let subcmd = match std::str::from_utf8(&args.args[0]) {
        Ok(s) => s.to_lowercase(),
        Err(_) => return Value::error("ERR subcommand is not a valid UTF-8 string"),
    };

    match subcmd.as_str() {
        "load" => script_load(args, ctx),
        "exists" => script_exists(args, ctx),
        "flush" => script_flush(ctx),
        "show" => script_show(args, ctx),
        "kill" => script_kill(conn, ctx),
        "debug" => script_debug(args),
        _ => Value::error(format!("ERR Unknown subcommand '{}'", subcmd)),
    }
}

/// SCRIPT LOAD <script>
fn script_load(args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    if args.args.len() < 2 {
        return CommandError::wrong_arity("script|load");
    }

    let script = match std::str::from_utf8(&args.args[1]) {
        Ok(s) => s,
        Err(_) => return Value::error("ERR script is not a valid UTF-8 string"),
    };

    let sha1 = ScriptCache::compute_sha1(script);
    ctx.script_mut()
        .script_cache
        .store_persistent(sha1.clone(), script.to_string());

    Value::Bulk(bytes::Bytes::from(sha1))
}

/// SCRIPT EXISTS <sha1> [sha1 ...]
fn script_exists(args: &CommandArgs, ctx: &mut ServerContext) -> Value {
    if args.args.len() < 2 {
        return CommandError::wrong_arity("script|exists");
    }

    let mut results = Vec::with_capacity(args.args.len() - 1);
    for arg in &args.args[1..] {
        let sha1 = match std::str::from_utf8(arg) {
            Ok(s) => s,
            Err(_) => {
                results.push(Value::Integer(0));
                continue;
            }
        };
        if ctx.script_mut().script_cache.exists(sha1) {
            results.push(Value::Integer(1));
        } else {
            results.push(Value::Integer(0));
        }
    }

    Value::Array(results)
}

/// SCRIPT SHOW <sha1>
fn script_show(args: &CommandArgs, ctx: &ServerContext) -> Value {
    if args.args.len() != 2 {
        return CommandError::wrong_arity("script|show");
    }

    let sha1 = match std::str::from_utf8(&args.args[1]) {
        Ok(s) => s,
        Err(_) => return Value::error("NOSCRIPT No matching script. Please use EVAL."),
    };
    if sha1.len() != 40 || !sha1.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Value::error("NOSCRIPT No matching script. Please use EVAL.");
    }
    match ctx.script().script_cache.peek(sha1) {
        Some(script) => Value::Bulk(bytes::Bytes::copy_from_slice(script.as_bytes())),
        None => Value::error("NOSCRIPT No matching script. Please use EVAL."),
    }
}

/// SCRIPT FLUSH
fn script_flush(ctx: &mut ServerContext) -> Value {
    ctx.script_mut().script_cache.flush();
    reset_lua_runtime();
    Value::ok()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum KillCommand {
    Script,
    Function,
}

impl KillCommand {
    fn is_function(self) -> bool {
        self == Self::Function
    }

    fn source(self) -> crate::engine::extension::script::KillSource {
        match self {
            Self::Script => crate::engine::extension::script::KillSource::ScriptKill,
            Self::Function => crate::engine::extension::script::KillSource::FunctionKill,
        }
    }

    fn idle_reply(self) -> Value {
        match self {
            Self::Script => Value::error("ERR NOTBUSY No scripts in execution right now."),
            Self::Function => Value::error("NOTBUSY No scripts in execution"),
        }
    }
}

fn wrong_kind_reply(running_is_function: bool) -> Value {
    if running_is_function {
        Value::error(
            "BUSY Redis is busy running a script. You can only call FUNCTION KILL or SHUTDOWN NOSAVE.",
        )
    } else {
        Value::error(
            "BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.",
        )
    }
}

fn unkillable_reply(running_is_function: bool) -> Value {
    if running_is_function {
        Value::error("UNKILLABLE Sorry the function already executed write commands against the dataset. You can either wait the function termination or kill the server in a hard way using SHUTDOWN NOSAVE.")
    } else {
        Value::error("UNKILLABLE Sorry the script already executed write commands against the dataset. You can either wait the script termination or kill the server in a hard way using SHUTDOWN NOSAVE.")
    }
}

pub(super) fn request_script_kill(ctx: &mut ServerContext, command: KillCommand) -> Value {
    if let Some(control) = ctx.script().running_script.as_ref() {
        let running_is_function =
            control.kind() == crate::engine::extension::script::ScriptKind::Function;
        if running_is_function != command.is_function() {
            return wrong_kind_reply(running_is_function);
        }
        if control.is_write_dirty() {
            return unkillable_reply(running_is_function);
        }
        assert!(control.request_kill(command.source()));
        ctx.set_pending_script_kill(true);
        return Value::ok();
    }

    if ctx.is_script_busy() {
        let running_is_function = ctx.is_script_busy_function();
        if running_is_function != command.is_function() {
            return wrong_kind_reply(running_is_function);
        }
        if ctx.is_script_busy_write_dirty() {
            return unkillable_reply(running_is_function);
        }
        ctx.clear_script_busy();
        ctx.set_pending_script_kill(true);
        return Value::ok();
    }

    command.idle_reply()
}

/// SCRIPT KILL
fn script_kill(_conn: &mut dyn CommandConn, ctx: &mut ServerContext) -> Value {
    request_script_kill(ctx, KillCommand::Script)
}

/// SCRIPT DEBUG <mode>
/// Set the debugger mode. The Rust port does not expose the interactive
/// debugger, but keeps the command contract so scripts can run in compatibility
/// test suites.
fn script_debug(args: &CommandArgs) -> Value {
    if args.args.len() != 2 {
        return CommandError::wrong_arity("script|debug");
    }
    match args.args[1].as_ref() {
        b"yes" | b"sync" => {
            SCRIPT_DEBUG_ENABLED.store(true, Ordering::Relaxed);
            Value::ok()
        }
        b"no" => {
            SCRIPT_DEBUG_ENABLED.store(false, Ordering::Relaxed);
            Value::ok()
        }
        _ => Value::error("ERR Use SCRIPT DEBUG YES/SYNC/NO"),
    }
}

#[cfg(test)]
mod tests {
    use super::{request_script_kill, script_debug, KillCommand};
    use crate::engine::command::CommandArgs;
    use crate::engine::extension::script::{
        KillSource, ScriptRunCtx, SCRIPT_EVAL_MODE,
    };
    use crate::foundation::io::protocol::Value;
    use crate::framework::context::ServerContext;

    struct Case {
        name: &'static str,
        flags: u64,
        dirty: bool,
        command: KillCommand,
        expected: Value,
        killed: bool,
        source: KillSource,
    }

    #[test]
    fn request_script_kill_follows_running_kind_and_dirty_state() {
        let cases = [
            Case {
                name: "clean eval accepts script kill",
                flags: SCRIPT_EVAL_MODE,
                dirty: false,
                command: KillCommand::Script,
                expected: Value::ok(),
                killed: true,
                source: KillSource::ScriptKill,
            },
            Case {
                name: "dirty eval rejects script kill",
                flags: SCRIPT_EVAL_MODE,
                dirty: true,
                command: KillCommand::Script,
                expected: Value::error("UNKILLABLE Sorry the script already executed write commands against the dataset. You can either wait the script termination or kill the server in a hard way using SHUTDOWN NOSAVE."),
                killed: false,
                source: KillSource::None,
            },
            Case {
                name: "clean function accepts function kill",
                flags: 0,
                dirty: false,
                command: KillCommand::Function,
                expected: Value::ok(),
                killed: true,
                source: KillSource::FunctionKill,
            },
            Case {
                name: "dirty function rejects function kill",
                flags: 0,
                dirty: true,
                command: KillCommand::Function,
                expected: Value::error("UNKILLABLE Sorry the function already executed write commands against the dataset. You can either wait the function termination or kill the server in a hard way using SHUTDOWN NOSAVE."),
                killed: false,
                source: KillSource::None,
            },
            Case {
                name: "eval rejects function kill",
                flags: SCRIPT_EVAL_MODE,
                dirty: false,
                command: KillCommand::Function,
                expected: Value::error("BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE."),
                killed: false,
                source: KillSource::None,
            },
            Case {
                name: "function rejects script kill",
                flags: 0,
                dirty: false,
                command: KillCommand::Script,
                expected: Value::error("BUSY Redis is busy running a script. You can only call FUNCTION KILL or SHUTDOWN NOSAVE."),
                killed: false,
                source: KillSource::None,
            },
        ];

        for case in cases {
            let mut ctx = ServerContext::new(1);
            let run = ScriptRunCtx::new_bare_for_test(case.flags);
            if case.dirty {
                run.mark_write_dirty();
            }
            ctx.script_mut().running_script = Some(run.control.clone());
            ctx.set_script_busy(case.flags & SCRIPT_EVAL_MODE == 0, case.dirty);

            assert_eq!(request_script_kill(&mut ctx, case.command), case.expected, "{}", case.name);
            assert_eq!(run.is_killed(), case.killed, "{}", case.name);
            assert_eq!(run.control.kill_source(), case.source, "{}", case.name);
            assert!(ctx.is_script_busy(), "{}", case.name);
        }
    }

    #[test]
    fn request_script_kill_preserves_idle_error_shapes() {
        let mut ctx = ServerContext::new(1);
        assert_eq!(request_script_kill(&mut ctx, KillCommand::Script), Value::error("ERR NOTBUSY No scripts in execution right now."));
        assert_eq!(request_script_kill(&mut ctx, KillCommand::Function), Value::error("NOTBUSY No scripts in execution"));
    }

    #[test]
    fn script_debug_accepts_c_compatible_modes() {
        let _guard = crate::foundation::memory::metrics::memory_stats::lock_global_for_test();
        for mode in [b"yes".as_slice(), b"sync", b"no"] {
            let args = CommandArgs {
                name: bytes::Bytes::from_static(b"SCRIPT"),
                args: vec![bytes::Bytes::from_static(b"debug"), bytes::Bytes::copy_from_slice(mode)],
            };
            assert_eq!(script_debug(&args), Value::ok());
        }
        let args = CommandArgs {
            name: bytes::Bytes::from_static(b"SCRIPT"),
            args: vec![
                bytes::Bytes::from_static(b"debug"),
                bytes::Bytes::from_static(b"invalid"),
            ],
        };
        assert_eq!(
            script_debug(&args),
            Value::error("ERR Use SCRIPT DEBUG YES/SYNC/NO")
        );
    }
}
