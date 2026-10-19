use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::host_tasks::{DebugAofTask, PreparedHostTask};

pub(super) fn prepare_debug_loadaof(
    ctx: &mut ServerContext,
) -> Result<PreparedHostTask, Value> {
    if let Some(journal) = ctx.aof_mut().journal.as_mut() {
        journal
            .flush(true)
            .map_err(|error| Value::error(format!("ERR {}", error)))?;
    }

    let persistence = &ctx.config().persistence;
    let paths = crate::foundation::io::persistence::resolve_aof_read_paths_checked(
        &persistence.dir,
        &persistence.aof_filename,
        &persistence.aof_appenddirname,
    )
    .map_err(aof_prepare_error)?;
    let mut inputs = Vec::new();
    for path in paths {
        match std::fs::read(&path) {
            Ok(bytes) => inputs.push((path, bytes)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(aof_prepare_error(error)),
        }
    }

    ctx.flush_all_dbs();
    ctx.script_mut().function_registry.flush();
    ctx.prepare_slot_migration_load();
    ctx.set_host_task_loading(true);
    ctx.config_mut().server.import_mode = true;
    Ok(PreparedHostTask::DebugAof(DebugAofTask::new(inputs)))
}

fn aof_prepare_error(error: impl std::fmt::Display) -> Value {
    Value::error(format!(
        "ERR Error trying to load the AOF files, check server logs: {}",
        error
    ))
}
