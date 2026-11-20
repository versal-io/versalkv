use mlua::prelude::*;
use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::engine::extension::script::{
    ScriptError, ScriptRunCtx, ScriptRuntime, SCRIPT_EVAL_MODE,
};
use crate::engine::host::{ScriptDebugResume, ScriptDebugStopReason};
thread_local! {
    pub(crate) static COMPILE_TIMEOUT_MS: Cell<i64> = const { Cell::new(0) };
    pub(crate) static COMPILE_START_MS: Cell<i64> = const { Cell::new(0) };
    static DEBUGGER_STATE: Cell<DebuggerState> = const { Cell::new(DebuggerState::Disabled) };
}

const SCRIPT_CHECKPOINT_INSTRUCTIONS: u32 = 100_000;

#[derive(Clone, Copy, PartialEq, Eq)]
enum DebuggerState {
    Disabled,
    Continue,
    BreakNextLine,
    InitialStep,
    Step,
    Detached,
}

pub(crate) struct ScriptDebuggerGuard {
    previous: DebuggerState,
}

impl ScriptDebuggerGuard {
    pub(crate) fn enter(resume: ScriptDebugResume) -> Self {
        let state = match resume {
            ScriptDebugResume::Step => DebuggerState::InitialStep,
            ScriptDebugResume::Continue => DebuggerState::Continue,
        };
        Self {
            previous: DEBUGGER_STATE.with(|value| value.replace(state)),
        }
    }
}

impl Drop for ScriptDebuggerGuard {
    fn drop(&mut self) {
        DEBUGGER_STATE.with(|value| value.set(self.previous));
    }
}

pub(crate) fn request_debugger_breakpoint() -> bool {
    DEBUGGER_STATE.with(|state| match state.get() {
        DebuggerState::Disabled => false,
        DebuggerState::Detached => true,
        _ => {
            state.set(DebuggerState::BreakNextLine);
            true
        }
    })
}

pub(crate) fn resume_debugger_step() {
    DEBUGGER_STATE.with(|state| state.set(DebuggerState::Step));
}

pub(crate) fn detach_debugger() {
    DEBUGGER_STATE.with(|state| state.set(DebuggerState::Detached));
}

fn take_debugger_stop() -> Option<ScriptDebugStopReason> {
    DEBUGGER_STATE.with(|state| match state.get() {
        DebuggerState::BreakNextLine => {
            state.set(DebuggerState::Continue);
            Some(ScriptDebugStopReason::Breakpoint)
        }
        DebuggerState::InitialStep => {
            // The initial stop is emitted before the VM executes its first line.
            // Skip only that first event, including for an empty script.
            state.set(DebuggerState::Step);
            None
        }
        DebuggerState::Step => {
            state.set(DebuggerState::Continue);
            Some(ScriptDebugStopReason::StepOver)
        }
        _ => None,
    })
}

struct SharedHookState {
    error: RefCell<Option<LuaError>>,
}

impl SharedHookState {
    fn reset(&self) {
        *self.error.borrow_mut() = None;
    }

    fn terminate(self: &Rc<Self>, lua: &Lua, error: LuaError) -> LuaError {
        *self.error.borrow_mut() = Some(error.clone());
        let state = self.clone();
        // The native VM transfers this hook back through every resumer.
        // Protected calls cannot replenish the budget after termination.
        if let Err(hook_error) = lua.set_global_hook(
            mlua::HookTriggers::new().every_nth_instruction(1),
            move |_, _| {
                let error = state.error.borrow();
                match error.as_ref() {
                    Some(error) => Err(error.clone()),
                    None => Ok(mlua::VmState::Continue),
                }
            },
        ) {
            return hook_error;
        }
        error
    }
}

pub(super) struct ScriptHookGuard<'lua> {
    lua: &'lua Lua,
    state: Rc<SharedHookState>,
}

impl<'lua> ScriptHookGuard<'lua> {
    pub(super) fn enter(lua: &'lua Lua) -> LuaResult<Self> {
        let state = lua
            .app_data_ref::<Rc<SharedHookState>>()
            .ok_or_else(|| LuaError::external("Script hook state is unavailable"))?
            .clone();
        state.reset();
        set_checkpoint_hook(lua, state.clone())?;
        Ok(Self { lua, state })
    }
}

impl Drop for ScriptHookGuard<'_> {
    fn drop(&mut self) {
        self.lua.remove_global_hook();
        self.state.reset();
    }
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
    let state = Rc::new(SharedHookState {
        error: RefCell::new(None),
    });
    set_checkpoint_hook(lua, state.clone())
        .map_err(|e| ScriptError::Runtime(format!("Failed to install script hook: {}", e)))?;
    lua.set_app_data(state);
    Ok(())
}

fn set_checkpoint_hook(lua: &Lua, hook_state: Rc<SharedHookState>) -> LuaResult<()> {
    // Valkey's pinned Lua VM transfers the remaining count on resume and yield.
    // Normal scripts enter Rust only once per shared 100000-instruction budget.
    let mut triggers =
        mlua::HookTriggers::new().every_nth_instruction(SCRIPT_CHECKPOINT_INSTRUCTIONS);
    if DEBUGGER_STATE.with(|state| state.get() != DebuggerState::Disabled) {
        triggers = triggers.every_line();
    }
    lua.set_global_hook(triggers, move |lua, debug| {
        if debug.event() == mlua::DebugEvent::Line {
            let line = debug
                .current_line()
                .filter(|_| debug.source().source.as_deref() == Some("@user_script"));
            if let Some((line, reason)) =
                line.and_then(|line| take_debugger_stop().map(|reason| (line, reason)))
            {
                let callback: LuaFunction =
                    lua.named_registry_value(super::runtime::HOST_CALLBACK_REGISTRY_KEY)?;
                // Debugger input errors belong to the interrupted Lua frame.
                // They remain catchable and must not latch timeout termination.
                callback.call::<LuaValue>((6_i64, line, reason as i64))?;
            }
            return Ok(mlua::VmState::Continue);
        }
        let compile_timeout = COMPILE_TIMEOUT_MS.with(|c| c.get());
        if compile_timeout > 0 {
            let compile_start = COMPILE_START_MS.with(|c| c.get());
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            if now - compile_start > compile_timeout {
                return Err(hook_state.terminate(lua, LuaError::external("FUNCTION LOAD timeout")));
            }
        }

        let callback =
            lua.named_registry_value::<LuaFunction>(super::runtime::HOST_CALLBACK_REGISTRY_KEY);
        let Ok(callback) = callback else {
            return Ok(mlua::VmState::Continue);
        };
        // Once termination is requested, all resumers must fail immediately.
        // A coroutine.resume or pcall must not replenish the instruction budget.
        callback
            .call::<LuaValue>(5_i64)
            .map_err(|error| hook_state.terminate(lua, error))?;
        Ok(mlua::VmState::Continue)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn count_instructions(body: &str) -> u32 {
        let lua = Lua::new();
        let count = Rc::new(Cell::new(0));
        let hook_count = count.clone();
        lua.set_global_hook(
            mlua::HookTriggers::new().every_nth_instruction(1),
            move |_, _| {
                hook_count.set(hook_count.get() + 1);
                Ok(mlua::VmState::Continue)
            },
        )
        .unwrap();
        lua.load(body).exec().unwrap();
        count.get()
    }

    #[test]
    fn native_hook_preserves_exact_checkpoint_boundaries_across_coroutines() {
        for rounds in [1, 2] {
            for operation in [
                "coroutine.resume(coroutine.create(spin))",
                "coroutine.wrap(spin)()",
                "coroutine.resume(co)",
                "pcall(coroutine.wrap(function() spin(); error('ordinary') end))",
            ] {
                let prefix = format!(
                    "local x=0; local function spin() for i=1,49000 do end end; \
                     local co=coroutine.create(function() for i=1,{} do spin(); \
                     coroutine.yield() end end); for i=1,{} do {operation} end;",
                    2 * rounds,
                    2 * rounds,
                );
                let base = count_instructions(&format!("{prefix} return x"));
                assert_eq!(
                    count_instructions(&format!("{prefix} x=x+1; return x")),
                    base + 1
                );
                for instructions in [
                    rounds * SCRIPT_CHECKPOINT_INSTRUCTIONS - 1,
                    rounds * SCRIPT_CHECKPOINT_INSTRUCTIONS,
                    rounds * SCRIPT_CHECKPOINT_INSTRUCTIONS + 1,
                ] {
                    assert!(base < instructions);
                    let body = format!(
                        "{prefix}{} return x",
                        "x=x+1;".repeat((instructions - base) as usize)
                    );
                    assert_eq!(count_instructions(&body), instructions);
                    let lua = Lua::new();
                    install_script_hook(&lua).unwrap();
                    let checkpoints = Rc::new(Cell::new(0));
                    let observed = checkpoints.clone();
                    let callback = lua
                        .create_function(move |_, operation: i64| {
                            assert_eq!(operation, 5);
                            observed.set(observed.get() + 1);
                            Ok(())
                        })
                        .unwrap();
                    lua.set_named_registry_value(
                        super::super::runtime::HOST_CALLBACK_REGISTRY_KEY,
                        callback,
                    )
                    .unwrap();
                    let _guard = ScriptHookGuard::enter(&lua).unwrap();
                    lua.load(&body).exec().unwrap();
                    assert_eq!(
                        checkpoints.get(),
                        instructions / SCRIPT_CHECKPOINT_INSTRUCTIONS,
                        "checkpoint boundary for {operation}, {instructions} instructions"
                    );
                }
            }
        }
    }

    #[test]
    fn compile_timeout_escapes_protected_resumers_and_clears_on_same_vm() {
        let lua = Lua::new();
        install_script_hook(&lua).unwrap();
        COMPILE_TIMEOUT_MS.with(|value| value.set(1));
        COMPILE_START_MS.with(|value| value.set(0));
        {
            let _guard = ScriptHookGuard::enter(&lua).unwrap();
            let result = lua
                .load(
                    "local function spin() for i=1,200000 do end end; \
                     for i=1,4 do pcall(coroutine.wrap(function() \
                     pcall(coroutine.wrap(spin)) end)) end; return 42",
                )
                .eval::<i64>();
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("FUNCTION LOAD timeout"));
        }
        COMPILE_TIMEOUT_MS.with(|value| value.set(0));
        COMPILE_START_MS.with(|value| value.set(0));
        let _guard = ScriptHookGuard::enter(&lua).unwrap();
        assert_eq!(lua.load("return 42").eval::<i64>().unwrap(), 42);
    }
}
