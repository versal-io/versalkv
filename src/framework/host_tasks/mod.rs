mod aof;
mod lua;
mod rdb;

pub(crate) use aof::{
    apply_aof_truncation, install_rdb_preamble, AofFileResult, AofFileTaskError, AofReplayTask,
    AofTaskError, DebugAofTask,
};
pub(crate) use lua::{prepare_lua_task, run_lua_task_inline, LuaTask};
pub(crate) use rdb::{
    replace_function_libraries, DatasetVisibility, DebugRdbTask, RdbLoadTask, RdbTaskError,
};

pub(crate) enum PreparedHostTask {
    Lua(LuaTask),
    DebugRdb(DebugRdbTask),
    DebugAof(DebugAofTask),
}
