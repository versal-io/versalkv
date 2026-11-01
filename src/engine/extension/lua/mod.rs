pub mod convert;
pub mod engine;
mod errors;
#[macro_use]
pub mod libs;
pub mod redis_call;
pub mod runtime;
pub mod sandbox;
pub mod shebang;
pub mod timeout;

pub use engine::{LuaEngine, LUA_ENGINE};
#[cfg(test)]
use errors::has_err_code_prefix;
use errors::{
    defer_pcall_error, format_top_level_lua_error, lua_value_to_err_string, make_raise_err,
};
#[cfg(test)]
pub(crate) use runtime::registered_function_count;
pub(crate) use runtime::reset_lua_runtime;
pub(crate) use runtime::{clear_eval_scripts, release_eval_script};
pub(crate) use shebang::parse_shebang;

use crate::engine::extension::script::ScriptRunCtx;

pub(super) const REGISTERED_FUNCS_TABLE: &str = "__valkey_functions_v1";

pub(super) const REGISTERED_FUNCS_PENDING_TABLE: &str = "__valkey_functions_pending_v1";

pub(super) const ERR_RAISE_FIELD: &str = "__err_raise";

pub(super) const VKY_SAVED_KEY: &str = "__vky_saved";

pub(super) const COMPILE_MODE_FLAG: &str = "__vky_compile_mode";

#[cfg(test)]
mod tests;
