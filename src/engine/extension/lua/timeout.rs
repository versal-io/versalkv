use mlua::prelude::*;
use std::cell::Cell;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::engine::extension::script::{
    ScriptError, ScriptRunCtx, ScriptRuntime, SCRIPT_EVAL_MODE,
};
thread_local! {
    pub(crate) static COMPILE_TIMEOUT_MS: Cell<i64> = const { Cell::new(0) };
    pub(crate) static COMPILE_START_MS: Cell<i64> = const { Cell::new(0) };
}

pub(crate) fn on_script_timeout_tick(
    script_ctx: &mut ScriptRunCtx,
    runtime: &mut dyn ScriptRuntime,
) {
    script_ctx.check_timeout();
    if script_ctx.is_killed() || !script_ctx.is_timed_out() {
        return;
    }

    if runtime.pending_script_kill() {
        return;
    }

    if !script_ctx.slow_log_emitted {
        crate::server_log!(warning, "Slow script detected");
        script_ctx.slow_log_emitted = true;
    }
    runtime.set_script_busy(
        script_ctx.flags & SCRIPT_EVAL_MODE == 0,
        script_ctx.is_write_dirty(),
    );
}

pub fn install_script_hook(lua: &Lua) -> Result<(), ScriptError> {
    lua.set_hook(
        mlua::HookTriggers::new().every_nth_instruction(10_000),
        |lua, _debug| {
            let compile_timeout = COMPILE_TIMEOUT_MS.with(|c| c.get());
            if compile_timeout > 0 {
                let compile_start = COMPILE_START_MS.with(|c| c.get());
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_millis() as i64)
                    .unwrap_or(0);
                if now - compile_start > compile_timeout {
                    return Err(mlua::Error::external("FUNCTION LOAD timeout"));
                }
            }

            let callback =
                lua.named_registry_value::<LuaFunction>(super::runtime::HOST_CALLBACK_REGISTRY_KEY);
            let Ok(callback) = callback else {
                return Ok(mlua::VmState::Continue);
            };
            let _: LuaValue = callback.call(5_i64)?;
            Ok(mlua::VmState::Continue)
        },
    )
    .map_err(|e| ScriptError::Runtime(format!("Failed to install script hook: {}", e)))
}
