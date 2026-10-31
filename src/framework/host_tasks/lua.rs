use bytes::Bytes;

use crate::engine::command::context::CommandConn;
use crate::engine::command::CommandArgs;
use crate::engine::extension::lua::{
    engine::{check_eval_preconditions, EvalPreconditions, ReadonlyErrorStyle},
    parse_shebang, LuaEngine,
};
use crate::engine::extension::script::{
    CompiledFunction, ScriptEngine, ScriptRunCtx, SCRIPT_ALLOW_CROSS_SLOT, SCRIPT_ALLOW_OOM,
    SCRIPT_ALLOW_STALE, SCRIPT_NO_CLUSTER, SCRIPT_NO_WRITES, SCRIPT_READ_ONLY,
};
use crate::engine::host::HostDispatcher;
use crate::foundation::io::protocol::Value;
use crate::framework::commands::scripting::script_cache::ScriptCache;
use crate::framework::context::ServerContext;

use super::PreparedHostTask;

pub(crate) struct LuaTask {
    program: LuaProgram,
    run: ScriptRunCtx,
    original_db: u32,
    propagation_frame: Option<Vec<u8>>,
}

enum LuaProgram {
    Eval {
        code: String,
        keys: Vec<Bytes>,
        args: Vec<Bytes>,
    },
    Function {
        engine: &'static dyn ScriptEngine,
        compiled: CompiledFunction,
        keys: Vec<Bytes>,
        args: Vec<Bytes>,
    },
}

impl LuaTask {
    pub(crate) fn control(&self) -> crate::engine::extension::script::ScriptControlHandle {
        self.run.control.clone()
    }

    pub(crate) fn run(&mut self, dispatcher: &mut HostDispatcher<'_>) -> Value {
        match &self.program {
            LuaProgram::Eval { code, keys, args } => {
                LuaEngine.eval_cached_with_host(code, &mut self.run, keys, args, dispatcher)
            }
            LuaProgram::Function {
                engine,
                compiled,
                keys,
                args,
            } => engine.call(compiled, &mut self.run, keys, args, dispatcher),
        }
        .unwrap_or_else(|error| error.into_value())
    }

    pub(crate) fn finish(
        mut self,
        result: Value,
        conn: &mut dyn CommandConn,
        ctx: &mut ServerContext,
    ) -> Value {
        if self.run.is_timed_out() {
            ctx.clear_script_busy();
        }
        if conn.selected_db() != self.original_db {
            conn.set_selected_db(self.original_db);
        }
        if !matches!(result, Value::Error(_)) {
            for error in self.run.deferred_error_replies.drain(..) {
                ctx.incr_error_reply(&error);
            }
        }

        let had_effects = !self.run.propagation_ops.is_empty();
        conn.extend_script_propagation(std::mem::take(&mut self.run.propagation_ops));
        if self.run.needs_propagation
            && self.run.propagate_dirty
            && !had_effects
            && !self.run.skip_propagate
        {
            if let Some(frame) = self.propagation_frame.take() {
                conn.record_custom_propagation(frame);
            }
        }
        result
    }
}

pub(crate) fn prepare_lua_task(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Result<PreparedHostTask, Value> {
    let name = String::from_utf8_lossy(&args.name).to_ascii_lowercase();
    ctx.script_mut().debug_session = None;
    match name.as_str() {
        "eval" => prepare_eval(conn, args, ctx, false, false),
        "eval_ro" => prepare_eval(conn, args, ctx, true, false),
        "evalsha" => prepare_eval(conn, args, ctx, false, true),
        "evalsha_ro" => prepare_eval(conn, args, ctx, true, true),
        "fcall" => prepare_fcall(conn, args, ctx, false),
        "fcall_ro" => prepare_fcall(conn, args, ctx, true),
        _ => Err(Value::error("ERR unsupported hosted Lua command")),
    }
}

pub(crate) fn run_lua_task_inline(
    task: LuaTask,
    conn: &mut dyn CommandConn,
    ctx: &mut ServerContext,
) -> Value {
    let mut task = task;
    ctx.enter_script(task.control());
    let result = crate::engine::extension::lua::redis_call::with_legacy_host_dispatcher(
        conn,
        ctx,
        |dispatcher| task.run(dispatcher),
    );
    ctx.leave_script();
    task.finish(result, conn, ctx)
}

fn prepare_eval(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
    readonly: bool,
    sha: bool,
) -> Result<PreparedHostTask, Value> {
    let command_name = String::from_utf8_lossy(&args.name).to_ascii_lowercase();
    if args.args.len() < 2 {
        return Err(crate::framework::CommandError::wrong_arity(&command_name));
    }
    conn.set_skip_propagate(true);

    let numkeys = parse_numkeys(&args.args[1])?;
    if args.args.len() < 2 + numkeys {
        return Err(Value::error(
            "ERR Number of keys can't be greater than number of args",
        ));
    }
    let script = if sha {
        let digest = std::str::from_utf8(&args.args[0])
            .map_err(|_| Value::error("ERR SHA1 is not a valid UTF-8 string"))?;
        ctx.script_mut()
            .script_cache
            .lookup(digest)
            .map(str::to_owned)
            .ok_or_else(|| Value::error("NOSCRIPT No matching script. Please use EVAL."))?
    } else {
        std::str::from_utf8(&args.args[0])
            .map_err(|_| Value::error("ERR script is not a valid UTF-8 string"))?
            .to_owned()
    };

    let digest = ScriptCache::compute_sha1(&script);
    let keys = args.args[2..2 + numkeys].to_vec();
    let argv = args.args[2 + numkeys..].to_vec();
    let mut run = ScriptRunCtx::new_eval_detached(
        conn.allow_oom(),
        true,
        <ServerContext as crate::engine::extension::script::ScriptRuntime>::has_enough_replicas_to_write(ctx),
        ctx.config().server.lua_enable_insecure_api,
    );
    if readonly {
        run.flags |= SCRIPT_READ_ONLY;
    }
    run.timeout_ms = ctx.config().server.lua_time_limit;

    let (_, flags, has_shebang) =
        parse_shebang(&script, run.flags).map_err(|error| error.into_value())?;
    run.flags = flags;
    check_eval_preconditions(EvalPreconditions {
        flags,
        has_shebang,
        has_maxmemory_limit: ctx.config().server.maxmemory > 0,
        has_enough_replicas_to_write:
            <ServerContext as crate::engine::extension::script::ScriptRuntime>::has_enough_replicas_to_write(ctx),
        is_readonly_replica:
            <ServerContext as crate::engine::extension::script::ScriptRuntime>::is_readonly_replica(ctx),
        is_stale_replica:
            <ServerContext as crate::engine::extension::script::ScriptRuntime>::is_stale_replica(ctx),
        has_persistence_write_error:
            <ServerContext as crate::engine::extension::script::ScriptRuntime>::has_persistence_write_error(ctx),
        readonly_error_style: if conn.file_descriptor() < 0 {
            ReadonlyErrorStyle::Valkey
        } else {
            ReadonlyErrorStyle::Legacy
        },
    })
    .map_err(|error| error.into_value())?;
    if ctx.config().server.cluster_enabled && run.flags & SCRIPT_NO_CLUSTER != 0 {
        return Err(Value::error(
            "ERR Can not run script on cluster, 'no-cluster' flag is set",
        ));
    }
    if ctx.config().server.cluster_enabled && has_shebang {
        run.script_slot = Some(u16::MAX);
    }
    run.check_cluster_slots(&keys).map_err(Value::error)?;
    LuaEngine
        .prepare_cached_eval(&digest, &script, run.lua_enable_insecure_api)
        .map_err(|error| error.into_value())?;
    ctx.register_eval_script(digest.clone(), script.clone());
    run.func_name = digest;
    run.start_timer();
    if crate::framework::commands::scripting::script_cmd::script_debug_enabled() {
        ctx.script_mut().debug_session = Some(conn.session_id());
    }

    let propagation_frame = (!readonly).then(|| {
        let mut parts: Vec<&[u8]> = Vec::with_capacity(3 + keys.len() + argv.len());
        parts.push(b"EVAL");
        parts.push(script.as_bytes());
        parts.push(args.args[1].as_ref());
        parts.extend(keys.iter().map(Bytes::as_ref));
        parts.extend(argv.iter().map(Bytes::as_ref));
        crate::engine::distributed::replication::protocol::encode_command(&parts)
    });

    Ok(PreparedHostTask::Lua(LuaTask {
        program: LuaProgram::Eval {
            code: script,
            keys,
            args: argv,
        },
        run,
        original_db: conn.selected_db(),
        propagation_frame,
    }))
}

fn prepare_fcall(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
    readonly: bool,
) -> Result<PreparedHostTask, Value> {
    if args.args.len() < 2 {
        return Err(crate::framework::CommandError::wrong_arity("fcall"));
    }
    conn.set_skip_propagate(true);
    let function_name = std::str::from_utf8(&args.args[0])
        .map_err(|_| Value::error("ERR Function name must be a valid string"))?
        .to_owned();
    let numkeys = std::str::from_utf8(&args.args[1])
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .ok_or_else(|| Value::error("ERR Bad number of keys provided"))?;
    if numkeys < 0 {
        return Err(Value::error("ERR Number of keys can't be negative"));
    }
    let numkeys = numkeys as usize;
    if args.args.len() < 2 + numkeys {
        return Err(Value::error(
            "ERR Number of keys can't be greater than number of args",
        ));
    }
    let keys = args.args[2..2 + numkeys].to_vec();
    let argv = args.args[2 + numkeys..].to_vec();
    let compiled = ctx
        .script()
        .function_registry
        .find_function(&function_name)
        .cloned()
        .ok_or_else(|| Value::error("ERR Function not found"))?;
    if compiled.flags & SCRIPT_NO_WRITES == 0
        && !<ServerContext as crate::engine::extension::script::ScriptRuntime>::has_enough_replicas_to_write(ctx)
    {
        return Err(Value::error("NOREPLICAS Not enough good replicas to write."));
    }
    let engine = ctx
        .script()
        .function_registry
        .function_engine(&compiled)
        .ok_or_else(|| Value::error("ERR Function engine not available"))?;
    if readonly && compiled.flags & SCRIPT_NO_WRITES == 0 {
        return Err(Value::error(
            "ERR Can not execute a script with write flag using *_ro command.",
        ));
    }

    let mut flags = 0;
    if readonly || compiled.flags & SCRIPT_NO_WRITES != 0 {
        flags |= SCRIPT_READ_ONLY;
    }
    if compiled.flags & SCRIPT_ALLOW_OOM != 0 {
        flags |= SCRIPT_ALLOW_OOM;
    }
    if compiled.flags & SCRIPT_ALLOW_CROSS_SLOT != 0 {
        flags |= SCRIPT_ALLOW_CROSS_SLOT;
    }
    if compiled.flags & SCRIPT_ALLOW_STALE != 0 {
        flags |= SCRIPT_ALLOW_STALE;
    }
    if compiled.flags & SCRIPT_NO_CLUSTER != 0 {
        flags |= SCRIPT_NO_CLUSTER;
    }

    let mut run = ScriptRunCtx::new_fcall_detached(
        flags,
        true,
        <ServerContext as crate::engine::extension::script::ScriptRuntime>::has_enough_replicas_to_write(ctx),
        ctx.config().server.lua_enable_insecure_api,
    );
    if ctx.config().server.cluster_enabled && flags & SCRIPT_NO_CLUSTER != 0 {
        return Err(Value::error(
            "ERR Can not run script on cluster, 'no-cluster' flag is set",
        ));
    }
    if ctx.config().server.cluster_enabled {
        run.script_slot = Some(u16::MAX);
    }
    run.check_cluster_slots(&keys).map_err(Value::error)?;
    run.timeout_ms = ctx.config().server.lua_time_limit;
    run.func_name = function_name.clone();
    run.caller_args = std::iter::once(args.name.to_vec())
        .chain(args.args.iter().map(|arg| arg.to_vec()))
        .collect();
    run.start_timer();

    let propagation_frame = (!readonly).then(|| {
        let mut parts: Vec<&[u8]> = Vec::with_capacity(3 + keys.len() + argv.len());
        parts.push(b"FCALL");
        parts.push(function_name.as_bytes());
        parts.push(args.args[1].as_ref());
        parts.extend(keys.iter().map(Bytes::as_ref));
        parts.extend(argv.iter().map(Bytes::as_ref));
        crate::engine::distributed::replication::protocol::encode_command(&parts)
    });

    Ok(PreparedHostTask::Lua(LuaTask {
        program: LuaProgram::Function {
            engine,
            compiled,
            keys,
            args: argv,
        },
        run,
        original_db: conn.selected_db(),
        propagation_frame,
    }))
}

fn parse_numkeys(bytes: &[u8]) -> Result<usize, Value> {
    let value = std::str::from_utf8(bytes)
        .ok()
        .and_then(|value| value.parse::<i64>().ok())
        .ok_or_else(|| Value::error("ERR value is not an integer or out of range"))?;
    if value < 0 {
        return Err(Value::error("ERR Number of keys can't be negative"));
    }
    Ok(value as usize)
}

#[cfg(test)]
mod tests;
