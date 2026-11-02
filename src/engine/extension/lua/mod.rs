pub mod convert;
mod debugger;
pub mod engine;
mod errors;
#[macro_use]
pub mod libs;
pub mod redis_call;
pub mod runtime;
pub mod sandbox;
pub mod shebang;
pub mod timeout;

pub(crate) use debugger::{bind_eval_globals, eval_fragment, format_eval_global, LuaFormatOutput};
pub use engine::{LuaEngine, LUA_ENGINE};
#[cfg(test)]
use errors::has_err_code_prefix;
use errors::{
    defer_pcall_error, format_top_level_lua_error, lua_value_to_err_string, make_raise_err,
};
pub(crate) use runtime::reset_lua_runtime;
pub(crate) use runtime::{clear_eval_scripts, release_eval_script};
#[cfg(test)]
pub(crate) use runtime::{compiled_script_count, registered_function_count};
pub(crate) use shebang::parse_shebang;

#[cfg(test)]
use crate::engine::extension::script::ScriptError;
use crate::engine::extension::script::ScriptRunCtx;

#[cfg(test)]
pub(crate) fn with_lua_runtime<R>(
    enable_insecure_api: bool,
    f: impl FnOnce(&mlua::Lua) -> Result<R, ScriptError>,
) -> Result<R, ScriptError> {
    runtime::with_lua_runtime(enable_insecure_api, |runtime| f(&runtime.lua))
}

pub(super) const REGISTERED_FUNCS_TABLE: &str = "__valkey_functions_v1";

pub(super) const REGISTERED_FUNCS_PENDING_TABLE: &str = "__valkey_functions_pending_v1";

pub(super) const ERR_RAISE_FIELD: &str = "__err_raise";

pub(super) const VKY_SAVED_KEY: &str = "__vky_saved";

pub(super) const COMPILE_MODE_FLAG: &str = "__vky_compile_mode";

#[cfg(test)]
mod tests;
