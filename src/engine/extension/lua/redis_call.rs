use bytes::Bytes;
use mlua::prelude::*;
use std::borrow::Cow;

use crate::engine::command::context::CommandConn;
use crate::engine::command::{
    CommandArgs, CommandDescriptor, CommandKeyIndices, ResolvedCommand, CMD_ALL_DBS, CMD_DENYOOM,
    CMD_MAY_REPLICATE, CMD_NOSCRIPT, CMD_READONLY, CMD_WRITE,
};
use crate::engine::extension::acl::{AclDenial, AclUser};
use crate::engine::extension::script::{ScriptRunCtx, ScriptRuntime, SCRIPT_WRITE_DIRTY};
use crate::engine::host::ScriptCallMode;
use crate::foundation::io::protocol::Value;

use super::convert;
use super::shebang::{is_blocked_subcmd, is_noscript_subcmd};

pub(crate) fn with_legacy_host_dispatcher<R>(
    conn: &mut dyn crate::engine::command::context::CommandConn,
    runtime: &mut dyn crate::engine::extension::script::ScriptRuntime,
    invoke: impl FnOnce(&mut crate::engine::host::HostDispatcher<'_>) -> R,
) -> R {
    let mut dispatcher = |request: crate::engine::host::HostRequest<'_>| match request {
        crate::engine::host::HostRequest::ExecuteCommand { args, origin } => match origin {
            crate::engine::host::HostCommandOrigin::Script { run, mode } => {
                Ok(crate::engine::host::HostReply::Command(
                    execute_script_command(run, mode, args, conn, runtime),
                ))
            }
            crate::engine::host::HostCommandOrigin::AofReplay => {
                Err(crate::engine::host::HostError::Invariant(
                    "AOF command reached the legacy Lua dispatcher",
                ))
            }
        },
        crate::engine::host::HostRequest::CheckAcl { args } => {
            check_acl_command(conn, runtime, args)
                .map(crate::engine::host::HostReply::Allowed)
                .ok_or_else(|| {
                    crate::engine::host::HostError::Runtime(
                        "Invalid command passed to server.acl_check_cmd()".to_string(),
                    )
                })
        }
        crate::engine::host::HostRequest::Yield(
            crate::engine::host::HostYield::ScriptCheckpoint { run },
        ) => {
            super::timeout::on_script_timeout_tick(run, runtime);
            Ok(crate::engine::host::HostReply::Continue)
        }
        crate::engine::host::HostRequest::Yield(
            crate::engine::host::HostYield::RdbProgress { .. }
            | crate::engine::host::HostYield::AofProgress { .. }
            | crate::engine::host::HostYield::ReplicationReplay { .. },
        ) => Err(crate::engine::host::HostError::Invariant(
            "persistence yield reached the legacy Lua dispatcher",
        )),
    };
    invoke(&mut dispatcher)
}

pub(crate) fn check_acl_command(
    conn: &dyn crate::engine::command::context::CommandConn,
    runtime: &dyn crate::engine::extension::script::ScriptRuntime,
    args: &CommandArgs,
) -> Option<bool> {
    let spec = runtime.lookup_command(args.name.as_ref())?.descriptor();
    let users = crate::engine::extension::acl::acl_users().lock().unwrap();
    let Some(user) = users.get(conn.username()) else {
        return Some(false);
    };
    if user.is_factory_default_privileged() {
        return Some(true);
    }
    let dbid = conn.selected_db() as i64;
    let num_dbs = runtime.num_dbs();
    let acl_categories = runtime.command_acl_categories(spec.clone(), args);
    let db_req = crate::engine::extension::acl::compute_db_requirement(
        spec.name.as_str(),
        &args.args,
        (spec.flags & CMD_ALL_DBS) != 0,
        crate::engine::extension::acl::should_restrict_cmd(
            spec.name.as_str(),
            acl_categories,
            spec.first_key != 0,
        ),
        dbid,
        num_dbs,
    );
    let required = if (spec.flags & CMD_WRITE) != 0 {
        crate::engine::extension::acl::ACL_WRITE_PERMISSION
    } else {
        crate::engine::extension::acl::ACL_READ_PERMISSION
    };
    let key_requirements = spec
        .key_indices(args)
        .into_iter()
        .flatten()
        .map(|index| (args.args[index].as_ref(), required != 0));
    let command = crate::engine::extension::acl::AclCommand::new(
        spec.name.as_str(),
        &args.args,
        acl_categories,
    );
    Some(user.can_execute_command_with_args_and_keys_db(
        command,
        &db_req,
        num_dbs,
        &key_requirements.collect::<Vec<_>>(),
    ))
}

pub(super) fn acl_check_impl(
    lua: &Lua,
    args: LuaMultiValue,
    dispatcher: &mut crate::engine::host::HostDispatcher<'_>,
) -> LuaResult<LuaValue> {
    let mut parts = Vec::with_capacity(args.len());
    for arg in args.iter() {
        match arg {
            LuaValue::String(value) => {
                parts.push(Bytes::copy_from_slice(value.as_bytes().as_ref()))
            }
            LuaValue::Integer(value) => parts.push(integer_bytes(*value)),
            LuaValue::Number(value) => parts.push(Bytes::from(value.to_string())),
            LuaValue::Boolean(value) => {
                parts.push(Bytes::from_static(if *value { b"1" } else { b"0" }))
            }
            LuaValue::Nil => parts.push(Bytes::new()),
            LuaValue::Table(table) => {
                for value in table.clone().sequence_values::<LuaValue>() {
                    match value? {
                        LuaValue::String(value) => {
                            parts.push(Bytes::copy_from_slice(value.as_bytes().as_ref()))
                        }
                        LuaValue::Integer(value) => parts.push(integer_bytes(value)),
                        LuaValue::Number(value) => parts.push(Bytes::from(value.to_string())),
                        LuaValue::Boolean(value) => {
                            parts.push(Bytes::from_static(if value { b"1" } else { b"0" }))
                        }
                        LuaValue::Nil => parts.push(Bytes::new()),
                        _ => {
                            return super::make_raise_err(
                                lua,
                                "Invalid command passed to server.acl_check_cmd()",
                            )
                        }
                    }
                }
            }
            _ => {
                return super::make_raise_err(
                    lua,
                    "Invalid command passed to server.acl_check_cmd()",
                )
            }
        }
    }
    if parts.is_empty() {
        return super::make_raise_err(lua, "Invalid command passed to server.acl_check_cmd()");
    }
    let command = CommandArgs::from_parts_reusing(parts);
    let allowed = crate::engine::host::check_acl(dispatcher, &command)
        .map_err(|error| mlua::Error::external(error.to_string()))?;
    Ok(LuaValue::Boolean(allowed))
}

fn make_lua_err(lua: &Lua, msg: &str) -> LuaResult<LuaValue> {
    super::make_raise_err(lua, msg).map_err(|e| mlua::Error::external(e.to_string()))
}

pub(super) fn is_noscript_denied(restrictions: ScriptCommandRestrictions) -> bool {
    restrictions.forced_block
        || ((restrictions.parent_noscript || restrictions.subcommand_noscript)
            && !restrictions.debug_override
            && !restrictions.safe_subcommand)
}

pub(super) struct ScriptCommandRestrictions {
    pub(super) parent_noscript: bool,
    pub(super) subcommand_noscript: bool,
    pub(super) forced_block: bool,
    pub(super) debug_override: bool,
    pub(super) safe_subcommand: bool,
}

pub(super) struct CompletedCommandEffect<'a> {
    pub(super) db_id: u32,
    pub(super) args: &'a CommandArgs,
    pub(super) result: &'a Value,
    pub(super) may_write_or_replicate: bool,
    pub(super) skip_propagate: bool,
    pub(super) custom_frames: Vec<Vec<u8>>,
}

pub(super) fn _capture_propagation_effect(
    script_ctx: &mut ScriptRunCtx,
    effect: CompletedCommandEffect<'_>,
) {
    let has_custom_frames = !effect.custom_frames.is_empty();
    let command_mutated =
        !matches!(effect.result, Value::Error(_)) && (!effect.skip_propagate || has_custom_frames);
    if !effect.may_write_or_replicate || !command_mutated {
        return;
    }

    script_ctx.propagate_dirty = true;
    if !script_ctx.needs_propagation {
        return;
    }
    if has_custom_frames {
        for frame in effect.custom_frames {
            script_ctx.append_propagation_frame(
                effect.db_id,
                script_ctx.propagation_targets,
                frame,
            );
        }
        return;
    }

    script_ctx.append_propagation_command(
        effect.db_id,
        script_ctx.propagation_targets,
        effect.args,
    );
}

pub(super) fn redis_call_impl(
    lua: &Lua,
    args: LuaMultiValue,
    pcall_mode: bool,
    script_ctx: &mut ScriptRunCtx,
    dispatcher: &mut crate::engine::host::HostDispatcher<'_>,
) -> LuaResult<LuaValue> {
    let cmd_args = match command_args_from_lua(&args, script_ctx) {
        Ok(args) => args,
        Err(message) => return lua_argument_error(lua, &message, pcall_mode),
    };
    let result = dispatch_script_invocation(dispatcher, &cmd_args, script_ctx, pcall_mode);
    let mut parts = cmd_args.into_parts_reusing();
    parts.clear();
    script_ctx.argv_scratch = parts;
    script_result_to_lua(lua, result?, script_ctx, pcall_mode)
}

fn command_args_from_lua(
    args: &LuaMultiValue,
    script_ctx: &mut ScriptRunCtx,
) -> Result<CommandArgs, String> {
    let mut parts = std::mem::take(&mut script_ctx.argv_scratch);
    parts.clear();
    parts.reserve(args.len());
    for argument in args.iter() {
        match argument {
            LuaValue::Table(table) => append_lua_table_args(table, script_ctx, &mut parts)?,
            _ => parts.push(lua_argument_bytes(argument, script_ctx)?),
        }
    }
    if parts.is_empty() {
        return Err("ERR Please specify at least one argument for this call".to_string());
    }
    Ok(CommandArgs::from_parts_reusing(parts))
}

fn append_lua_table_args(
    table: &LuaTable,
    script_ctx: &mut ScriptRunCtx,
    parts: &mut Vec<Bytes>,
) -> Result<(), String> {
    if let Ok(value) = table.get::<LuaValue>("err") {
        if !matches!(value, LuaValue::Nil) {
            return Err(lua_error_value_message(value));
        }
    }
    let unpack = matches!(
        table.raw_get::<LuaValue>("__vky_unpack_args"),
        Ok(LuaValue::Boolean(true))
    );
    if !unpack {
        return Err(invalid_lua_argument_message());
    }
    let _ = table.raw_set("__vky_unpack_args", LuaValue::Nil);
    for value in table.clone().sequence_values::<LuaValue>() {
        parts.push(lua_argument_bytes(
            &value.map_err(|_| invalid_lua_argument_message())?,
            script_ctx,
        )?);
    }
    Ok(())
}

fn lua_argument_bytes(value: &LuaValue, script_ctx: &mut ScriptRunCtx) -> Result<Bytes, String> {
    match value {
        LuaValue::String(value) => Ok(script_ctx.intern_lua_argument(value.as_bytes().as_ref())),
        LuaValue::Integer(value) => Ok(integer_bytes(*value)),
        LuaValue::Number(value) => Ok(Bytes::from(value.to_string())),
        LuaValue::Boolean(value) => Ok(Bytes::from_static(if *value { b"1" } else { b"0" })),
        LuaValue::Nil => Ok(Bytes::new()),
        _ => Err(invalid_lua_argument_message()),
    }
}

fn invalid_lua_argument_message() -> String {
    "ERR Command arguments must be strings or integers".to_string()
}

fn lua_error_value_message(value: LuaValue) -> String {
    match value {
        LuaValue::String(value) => value
            .to_str()
            .map(|value| value.to_string())
            .unwrap_or_default(),
        _ => "Unknown error".to_string(),
    }
}

fn lua_argument_error(lua: &Lua, message: &str, protected: bool) -> LuaResult<LuaValue> {
    if protected {
        let table = lua.create_table()?;
        table.set("err", message)?;
        Ok(LuaValue::Table(table))
    } else {
        make_lua_err(lua, message)
    }
}

fn dispatch_script_invocation(
    dispatcher: &mut crate::engine::host::HostDispatcher<'_>,
    args: &CommandArgs,
    script_ctx: &mut ScriptRunCtx,
    protected: bool,
) -> LuaResult<Value> {
    let mode = if protected {
        crate::engine::host::ScriptCallMode::ProtectedCall
    } else {
        crate::engine::host::ScriptCallMode::Call
    };
    crate::engine::host::dispatch_command(
        dispatcher,
        args,
        crate::engine::host::HostCommandOrigin::Script {
            run: script_ctx,
            mode,
        },
    )
    .map_err(|error| mlua::Error::external(error.to_string()))
}

fn script_result_to_lua(
    lua: &Lua,
    result: Value,
    script_ctx: &ScriptRunCtx,
    protected: bool,
) -> LuaResult<LuaValue> {
    if let Value::Error(error) = &result {
        let message = String::from_utf8_lossy(error).into_owned();
        return lua_argument_error(lua, &message, protected);
    }
    let result = convert::value_for_script_resp(&result, script_ctx.resp);
    convert::value_to_lua_resp(lua, &result, script_ctx.resp)
}

fn integer_bytes(value: i64) -> Bytes {
    let mut digits = [0_u8; 20];
    let mut start = digits.len();
    let mut magnitude = value.unsigned_abs();
    loop {
        start -= 1;
        digits[start] = b'0' + (magnitude % 10) as u8;
        magnitude /= 10;
        if magnitude == 0 {
            break;
        }
    }
    if value < 0 {
        start -= 1;
        digits[start] = b'-';
    }
    Bytes::copy_from_slice(&digits[start..])
}

struct ScriptCommandInvocation<'a> {
    args: &'a CommandArgs,
    command: ResolvedCommand,
    spec: CommandDescriptor,
    key_indices: Option<CommandKeyIndices>,
    has_subcommands: bool,
    protected: bool,
    is_write: bool,
    may_write_or_replicate: bool,
}

impl<'a> ScriptCommandInvocation<'a> {
    fn prepare(
        args: &'a CommandArgs,
        mode: ScriptCallMode,
        runtime: &dyn ScriptRuntime,
    ) -> Result<Self, Value> {
        let command = runtime
            .lookup_command(args.name.as_ref())
            .ok_or_else(|| Value::error("ERR Unknown command called from script"))?;
        let spec = command.descriptor();
        let has_subcommands = crate::engine::command::is_subcommand_container(spec.name.as_str());
        validate_script_command(&spec, args, runtime, has_subcommands)?;
        let key_indices = spec.key_indices(args);
        let is_write = spec.flags & CMD_WRITE != 0;
        let may_write_or_replicate = spec.flags & (CMD_WRITE | CMD_MAY_REPLICATE) != 0;
        Ok(Self {
            args,
            command,
            spec,
            key_indices,
            has_subcommands,
            protected: mode == ScriptCallMode::ProtectedCall,
            is_write,
            may_write_or_replicate,
        })
    }

    fn resolve_stats_name(&self, runtime: &dyn ScriptRuntime) -> Cow<'static, str> {
        if self.has_subcommands {
            Cow::Owned(runtime.command_stats_name(self.spec.clone(), self.args))
        } else {
            match &self.spec.name {
                crate::engine::command::CommandName::Static(name) => Cow::Borrowed(name),
                crate::engine::command::CommandName::Shared(name) => Cow::Owned(name.to_string()),
            }
        }
    }
}

fn validate_script_command(
    spec: &CommandDescriptor,
    args: &CommandArgs,
    runtime: &dyn ScriptRuntime,
    has_subcommands: bool,
) -> Result<(), Value> {
    if is_noscript_denied(script_command_restrictions(
        spec,
        args,
        runtime,
        has_subcommands,
    )) {
        Err(Value::error(
            "ERR This Valkey command is not allowed in scripts",
        ))
    } else {
        Ok(())
    }
}

fn script_command_restrictions(
    spec: &CommandDescriptor,
    args: &CommandArgs,
    runtime: &dyn ScriptRuntime,
    has_subcommands: bool,
) -> ScriptCommandRestrictions {
    let subcommand = has_subcommands
        .then(|| args.args.first())
        .flatten()
        .map(|value| String::from_utf8_lossy(value).to_ascii_lowercase());
    let subcmd_noscript = subcommand.as_deref().is_some_and(|subcommand| {
        let name = format!("{}-{}", spec.name.as_str(), subcommand);
        runtime
            .lookup_command(name.as_bytes())
            .is_some_and(|command| command.descriptor().flags & CMD_NOSCRIPT != 0)
    });
    let forced = subcommand
        .as_deref()
        .is_some_and(|subcommand| is_blocked_subcmd(spec.name.as_str(), subcommand));
    let safe = subcommand
        .as_deref()
        .is_some_and(|subcommand| is_noscript_subcmd(spec.name.as_str(), subcommand));
    ScriptCommandRestrictions {
        parent_noscript: spec.flags & CMD_NOSCRIPT != 0,
        subcommand_noscript: subcmd_noscript,
        forced_block: forced,
        debug_override: runtime.script_disable_deny_scripts(),
        safe_subcommand: safe,
    }
}

enum ScriptCallDecision {
    Execute,
    Reject {
        message: Cow<'static, str>,
        protected_message: Option<&'static str>,
    },
}

fn decide_script_call(
    invocation: &ScriptCommandInvocation<'_>,
    script_ctx: &ScriptRunCtx,
) -> ScriptCallDecision {
    let env = script_ctx.env_snapshot();
    let read_only = script_ctx.flags & crate::engine::extension::script::SCRIPT_READ_ONLY != 0;
    if read_only && invocation.may_write_or_replicate {
        return reject(
            format!(
                "ERR Write commands are not allowed from read-only scripts. script: {}",
                invocation.spec.name.as_str()
            ),
            Some("ERR Write commands are not allowed from read-only scripts."),
        );
    }
    if invocation.is_write && !read_only && env.persistence_write_error {
        return reject("MISCONF Errors writing to the RDB file, writes are disabled because stop-writes-on-bgsave-error is enabled.", None);
    }
    if invocation.is_write && !read_only && !script_ctx.has_enough_replicas_to_write {
        return reject("NOREPLICAS Not enough good replicas to write.", None);
    }
    if invocation.is_write && !read_only && !script_ctx.is_write_dirty() && env.readonly_replica {
        return reject(
            "READONLY You can't write against a read only replica.",
            None,
        );
    }
    decide_replica_and_oom(invocation, script_ctx, env)
}

fn decide_replica_and_oom(
    invocation: &ScriptCommandInvocation<'_>,
    script_ctx: &ScriptRunCtx,
    env: crate::engine::extension::script::ScriptEnvSnapshot,
) -> ScriptCallDecision {
    if env.stale_replica
        && ((script_ctx.flags & crate::engine::extension::script::SCRIPT_ALLOW_STALE) == 0
            || (invocation.spec.flags & crate::engine::command::CMD_STALE) == 0)
    {
        let message =
            if script_ctx.flags & crate::engine::extension::script::SCRIPT_ALLOW_STALE != 0 {
                "Can not execute the command on a stale replica"
            } else {
                "MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'."
            };
        return reject(message, None);
    }
    let deny_oom = env.maxmemory_limit
        && invocation.spec.flags & CMD_DENYOOM != 0
        && script_ctx.flags & crate::engine::extension::script::SCRIPT_ALLOW_OOM == 0
        && script_ctx.flags & crate::engine::extension::script::SCRIPT_READ_ONLY == 0
        && !script_ctx.is_write_dirty();
    if deny_oom {
        reject(
            "OOM command not allowed when used memory > 'maxmemory'.",
            None,
        )
    } else {
        ScriptCallDecision::Execute
    }
}

fn reject(
    message: impl Into<Cow<'static, str>>,
    protected_message: Option<&'static str>,
) -> ScriptCallDecision {
    ScriptCallDecision::Reject {
        message: message.into(),
        protected_message,
    }
}

fn apply_script_call_decision(
    decision: ScriptCallDecision,
    invocation: &ScriptCommandInvocation<'_>,
    script_ctx: &mut ScriptRunCtx,
    runtime: &mut dyn ScriptRuntime,
) -> Result<(), Value> {
    let ScriptCallDecision::Reject {
        message,
        protected_message,
    } = decision
    else {
        return Ok(());
    };
    runtime.end_command(invocation.spec.name.as_str(), true);
    let reply = if invocation.protected {
        let reply = protected_message.map(Cow::Borrowed).unwrap_or(message);
        super::defer_pcall_error(script_ctx, reply.as_ref());
        reply
    } else {
        message
    };
    Err(error_value_from_cow(reply))
}

fn error_value_from_cow(message: Cow<'static, str>) -> Value {
    match message {
        Cow::Borrowed(message) => Value::error(Bytes::from_static(message.as_bytes())),
        Cow::Owned(message) => Value::error(message),
    }
}

struct ScriptCommandOutcome {
    result: Value,
    stats_name: Cow<'static, str>,
    duration_us: u64,
    db_id: u32,
    skip_propagate: bool,
    custom_propagation: Vec<Vec<u8>>,
}

fn run_script_command(
    invocation: &ScriptCommandInvocation<'_>,
    script_ctx: &mut ScriptRunCtx,
    conn: &mut dyn CommandConn,
    runtime: &mut dyn ScriptRuntime,
    stats_name: Cow<'static, str>,
) -> ScriptCommandOutcome {
    let outer_skip = conn.skip_propagate();
    let custom_start = conn.custom_propagation().len();
    conn.set_skip_propagate(false);
    if invocation.is_write {
        script_ctx.mark_write_dirty();
        script_ctx.flags |= SCRIPT_WRITE_DIRTY;
    }
    // Mirror C's CMD_CALL_SLOWLOG: read the clock only when slowlog,
    // commandlog or MONITOR can actually consume the duration.
    let started = script_ctx
        .env_snapshot()
        .log_gate
        .logging_active()
        .then(std::time::Instant::now);
    let result = runtime.execute_command(invocation.command.clone(), conn, invocation.args);
    let outcome = ScriptCommandOutcome {
        result,
        stats_name,
        duration_us: started
            .map(|started| started.elapsed().as_micros() as u64)
            .unwrap_or(0),
        db_id: conn.selected_db(),
        skip_propagate: conn.skip_propagate(),
        custom_propagation: conn.take_custom_propagation_since(custom_start),
    };
    conn.set_skip_propagate(outer_skip);
    outcome
}

struct ScriptAclDenial {
    message: String,
    reason: i32,
    object: String,
}

fn find_script_acl_denial(
    invocation: &ScriptCommandInvocation<'_>,
    conn: &dyn CommandConn,
    runtime: &dyn ScriptRuntime,
) -> Option<ScriptAclDenial> {
    let users = crate::engine::extension::acl::acl_users().lock().unwrap();
    let user = users.get(conn.username())?;
    if user.is_factory_default_privileged() {
        return None;
    }
    command_acl_denial(invocation, user, conn, runtime)
        .or_else(|| channel_acl_denial(invocation, user))
}

fn command_acl_denial(
    invocation: &ScriptCommandInvocation<'_>,
    user: &AclUser,
    conn: &dyn CommandConn,
    runtime: &dyn ScriptRuntime,
) -> Option<ScriptAclDenial> {
    let spec = &invocation.spec;
    let categories = runtime.command_acl_categories(spec.clone(), invocation.args);
    let restrict = crate::engine::extension::acl::should_restrict_cmd(
        spec.name.as_str(),
        categories,
        spec.first_key != 0,
    );
    let requirement = crate::engine::extension::acl::compute_db_requirement(
        spec.name.as_str(),
        &invocation.args.args,
        spec.flags & CMD_ALL_DBS != 0,
        restrict,
        conn.selected_db() as i64,
        runtime.num_dbs(),
    );
    let permission = if invocation.is_write {
        crate::engine::extension::acl::ACL_WRITE_PERMISSION
    } else {
        crate::engine::extension::acl::ACL_READ_PERMISSION
    };
    let keys = invocation
        .key_indices
        .into_iter()
        .flatten()
        .map(|index| (invocation.args.args[index].as_ref(), permission));
    let command = crate::engine::extension::acl::AclCommand::new(
        spec.name.as_str(),
        &invocation.args.args,
        categories,
    );
    user.check_command_perm_iter(command, &requirement, runtime.num_dbs(), keys)
        .map(|denial| map_command_acl_denial(invocation, conn.username(), denial))
}

fn map_command_acl_denial(
    invocation: &ScriptCommandInvocation<'_>,
    username: &str,
    denial: AclDenial,
) -> ScriptAclDenial {
    match denial {
        AclDenial::Db { arg_index, .. } => ScriptAclDenial {
            message: "No permissions to access database".to_string(),
            reason: crate::engine::extension::acl::ACL_DENIED_DB,
            object: arg_index
                .and_then(|index| invocation.args.args.get(index))
                .map(|value| String::from_utf8_lossy(value).to_string())
                .unwrap_or_else(|| invocation.spec.name.as_str().to_ascii_lowercase()),
        },
        AclDenial::Command => ScriptAclDenial {
            message: format!(
                "User {} has no permissions to run the '{}' command",
                username,
                invocation.spec.name.as_str().to_ascii_lowercase()
            ),
            reason: crate::engine::extension::acl::ACL_DENIED_CMD,
            object: invocation.spec.name.as_str().to_ascii_lowercase(),
        },
        AclDenial::Key(index) => ScriptAclDenial {
            message: "No permissions to access a key".to_string(),
            reason: crate::engine::extension::acl::ACL_DENIED_KEY,
            object: invocation
                .key_indices
                .and_then(|mut indices| indices.nth(index))
                .and_then(|index| invocation.args.args.get(index))
                .map(|key| String::from_utf8_lossy(key).to_string())
                .unwrap_or_default(),
        },
    }
}

fn channel_acl_denial(
    invocation: &ScriptCommandInvocation<'_>,
    user: &AclUser,
) -> Option<ScriptAclDenial> {
    const CHANNEL_COMMANDS: &[&str] = &[
        "publish",
        "spublish",
        "subscribe",
        "ssubscribe",
        "psubscribe",
    ];
    let command = invocation.spec.name.as_str();
    let checks_channels = CHANNEL_COMMANDS
        .iter()
        .any(|name| command.eq_ignore_ascii_case(name));
    if !checks_channels || user.can_access_channel_for_command(command, &invocation.args.args) {
        return None;
    }
    let object = invocation
        .args
        .args
        .first()
        .and_then(|channel| std::str::from_utf8(channel).ok())
        .unwrap_or("<binary>")
        .to_string();
    Some(ScriptAclDenial {
        message: "No permissions to access a channel".to_string(),
        reason: crate::engine::extension::acl::ACL_DENIED_CHANNEL,
        object,
    })
}

fn apply_acl_denial(
    denial: ScriptAclDenial,
    invocation: &ScriptCommandInvocation<'_>,
    script_ctx: &mut ScriptRunCtx,
    conn: &dyn CommandConn,
    runtime: &mut dyn ScriptRuntime,
) -> Value {
    crate::engine::extension::acl::add_acl_log_entry(
        denial.reason,
        crate::engine::extension::acl::ACL_LOG_CTX_LUA,
        denial.object,
        conn.username().to_string(),
        format!("addr={} cmd=eval", conn.peer_addr()),
    );
    runtime.end_command(invocation.spec.name.as_str(), true);
    let message = format!("ACL failure in script: {}", denial.message);
    if invocation.protected {
        super::defer_pcall_error(script_ctx, &message);
        Value::error(message)
    } else {
        Value::error(format!("ERR {message}"))
    }
}

fn validate_script_admission(
    invocation: &ScriptCommandInvocation<'_>,
    script_ctx: &mut ScriptRunCtx,
    conn: &mut dyn CommandConn,
    runtime: &dyn ScriptRuntime,
) -> Result<(), Value> {
    validate_script_arity(invocation)?;
    validate_script_slots(invocation, script_ctx)?;
    validate_script_cluster_state(invocation, script_ctx, conn, runtime)?;
    if is_client_list(invocation.args) {
        return Err(Value::error("ERR CLIENT LIST requires a runtime host"));
    }
    Ok(())
}

fn validate_script_arity(invocation: &ScriptCommandInvocation<'_>) -> Result<(), Value> {
    let argc = invocation.args.args.len() as i32 + 1;
    let expected = invocation.spec.arity;
    let valid = if expected > 0 {
        argc == expected
    } else {
        argc >= -expected
    };
    if valid {
        Ok(())
    } else {
        Err(Value::error(
            "ERR Wrong number of args calling command from script",
        ))
    }
}

fn validate_script_slots(
    invocation: &ScriptCommandInvocation<'_>,
    script_ctx: &mut ScriptRunCtx,
) -> Result<(), Value> {
    let Some(indices) = invocation.key_indices else {
        return Ok(());
    };
    script_ctx
        .check_cluster_arg_slots(invocation.args, indices)
        .map_err(|error| {
            if invocation.protected {
                super::defer_pcall_error(script_ctx, error);
            }
            Value::error(error)
        })
}

fn validate_script_cluster_state(
    invocation: &ScriptCommandInvocation<'_>,
    script_ctx: &mut ScriptRunCtx,
    conn: &mut dyn CommandConn,
    runtime: &dyn ScriptRuntime,
) -> Result<(), Value> {
    let Some(indices) = invocation.key_indices else {
        return Ok(());
    };
    let Some(error) =
        runtime.script_cluster_error(invocation.spec.clone(), conn, invocation.args, indices)
    else {
        return Ok(());
    };
    let Value::Error(message) = error else {
        unreachable!("script cluster validation must return an error reply");
    };
    let message = String::from_utf8_lossy(&message).into_owned();
    if invocation.protected {
        super::defer_pcall_error(script_ctx, &message);
    }
    Err(Value::error(message))
}

fn is_client_list(args: &CommandArgs) -> bool {
    args.name.eq_ignore_ascii_case(b"client")
        && args.args.len() == 1
        && args.args[0].as_ref().eq_ignore_ascii_case(b"list")
}

impl ScriptCommandOutcome {
    fn command_mutated(&self) -> bool {
        !matches!(self.result, Value::Error(_))
            && (!self.skip_propagate || !self.custom_propagation.is_empty())
    }
}

fn commit_script_outcome(
    invocation: &ScriptCommandInvocation<'_>,
    mut outcome: ScriptCommandOutcome,
    script_ctx: &mut ScriptRunCtx,
    conn: &mut dyn CommandConn,
    runtime: &mut dyn ScriptRuntime,
) -> Value {
    record_nested_tracking(invocation, conn, runtime);
    runtime.record_command_call(outcome.stats_name.as_ref());
    conn.set_cmd_count(conn.cmd_count() + 1);
    record_nested_dirty_keys(invocation, &outcome, script_ctx, conn);
    capture_nested_effect(invocation, &mut outcome, script_ctx);
    record_nested_observability(invocation, &outcome, script_ctx, conn, runtime);
    record_nested_failure(invocation, &outcome, script_ctx, runtime);
    outcome.result
}

fn record_nested_tracking(
    invocation: &ScriptCommandInvocation<'_>,
    conn: &mut dyn CommandConn,
    runtime: &mut dyn ScriptRuntime,
) {
    if invocation.may_write_or_replicate || invocation.spec.flags & CMD_READONLY == 0 {
        return;
    }
    let should_track = conn.tracking()
        && !conn.tracking_bcast()
        && (!conn.tracking_optin() || conn.tracking_caching())
        && (!conn.tracking_optout() || !conn.tracking_caching());
    conn.set_tracking_caching(false);
    if should_track {
        let token = conn.token().0;
        for index in invocation.key_indices.into_iter().flatten() {
            runtime.remember_tracked_key(token, &invocation.args.args[index]);
        }
    }
}

fn record_nested_dirty_keys(
    invocation: &ScriptCommandInvocation<'_>,
    outcome: &ScriptCommandOutcome,
    script_ctx: &mut ScriptRunCtx,
    conn: &mut dyn CommandConn,
) {
    if !invocation.may_write_or_replicate || !outcome.command_mutated() {
        return;
    }
    for index in invocation.key_indices.into_iter().flatten() {
        let dirty_key = (outcome.db_id, invocation.args.args[index].clone());
        if script_ctx.dirty_keys_seen.insert(dirty_key.clone())
            && !conn.dirty_keys().contains(&dirty_key)
        {
            conn.record_dirty_key(dirty_key);
        }
    }
}

fn capture_nested_effect(
    invocation: &ScriptCommandInvocation<'_>,
    outcome: &mut ScriptCommandOutcome,
    script_ctx: &mut ScriptRunCtx,
) {
    _capture_propagation_effect(
        script_ctx,
        CompletedCommandEffect {
            db_id: outcome.db_id,
            args: invocation.args,
            result: &outcome.result,
            may_write_or_replicate: invocation.may_write_or_replicate,
            skip_propagate: outcome.skip_propagate,
            custom_frames: std::mem::take(&mut outcome.custom_propagation),
        },
    );
}

fn record_nested_observability(
    invocation: &ScriptCommandInvocation<'_>,
    outcome: &ScriptCommandOutcome,
    script_ctx: &ScriptRunCtx,
    conn: &dyn CommandConn,
    runtime: &mut dyn ScriptRuntime,
) {
    let gate = script_ctx.env_snapshot().log_gate;
    let record_slow = gate.should_record_slow(outcome.duration_us);
    let record_monitor = gate.monitor_present;
    if !record_slow && !record_monitor {
        return;
    }
    let argv = std::iter::once(invocation.args.name.clone())
        .chain(invocation.args.args.iter().cloned())
        .collect::<Vec<_>>();
    if record_slow {
        runtime.record_slow_command(
            argv.clone(),
            outcome.duration_us,
            &conn.peer_addr(),
            conn.name().cloned(),
        );
    }
    if record_monitor {
        runtime.record_internal_monitor(invocation.spec.clone(), conn.selected_db(), argv);
    }
}

fn record_nested_failure(
    invocation: &ScriptCommandInvocation<'_>,
    outcome: &ScriptCommandOutcome,
    script_ctx: &mut ScriptRunCtx,
    runtime: &mut dyn ScriptRuntime,
) {
    let Value::Error(message) = &outcome.result else {
        return;
    };
    runtime.record_command_failure(outcome.stats_name.as_ref());
    if invocation.protected {
        super::defer_pcall_error(script_ctx, &String::from_utf8_lossy(message));
    }
}

pub(crate) fn execute_script_command(
    script_ctx: &mut ScriptRunCtx,
    mode: ScriptCallMode,
    cmd_args: &CommandArgs,
    conn: &mut dyn CommandConn,
    runtime: &mut dyn ScriptRuntime,
) -> Value {
    script_ctx.ensure_env_snapshot(runtime, conn.username());
    let invocation = match ScriptCommandInvocation::prepare(cmd_args, mode, runtime) {
        Ok(invocation) => invocation,
        Err(error) => return error,
    };
    if let Err(error) = apply_script_call_decision(
        decide_script_call(&invocation, script_ctx),
        &invocation,
        script_ctx,
        runtime,
    ) {
        return error;
    }
    let stats_name = invocation.resolve_stats_name(runtime);
    // The factory-default privileged user was verified once when the snapshot
    // was captured; it skips the per-command ACL lock and permission walk.
    // Non-default users still run the full ACL check on every command.
    if !script_ctx.env_snapshot().privileged_user {
        if let Some(denial) = find_script_acl_denial(&invocation, conn, runtime) {
            return apply_acl_denial(denial, &invocation, script_ctx, conn, runtime);
        }
    }
    if let Err(error) = validate_script_admission(&invocation, script_ctx, conn, runtime) {
        return error;
    }
    let outcome = run_script_command(&invocation, script_ctx, conn, runtime, stats_name);
    commit_script_outcome(&invocation, outcome, script_ctx, conn, runtime)
}

#[cfg(test)]
mod allocation_tests {
    use super::integer_bytes;

    #[test]
    fn integer_arguments_are_encoded_without_intermediate_strings() {
        for value in [i64::MIN, -1, 0, 1, 42, i64::MAX] {
            assert_eq!(integer_bytes(value).as_ref(), value.to_string().as_bytes());
        }
    }
}
