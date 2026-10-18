use super::{CommandRuntime, FlushMode};
use crate::engine::core::{CommandOutcome, Value};
use crate::foundation::io::store::kv_engine::{Db, DbSet};

pub(crate) struct EmbeddedCommandRuntime<'a> {
    dbs: &'a mut DbSet,
    selected_db: &'a mut u32,
    now_ms: u64,
}

impl<'a> EmbeddedCommandRuntime<'a> {
    pub(crate) fn new(dbs: &'a mut DbSet, selected_db: &'a mut u32, now_ms: u64) -> Self {
        Self {
            dbs,
            selected_db,
            now_ms,
        }
    }
}

impl CommandRuntime for EmbeddedCommandRuntime<'_> {
    fn command_catalog(&self) -> &dyn crate::framework::commands::introspection::CommandCatalog {
        &crate::framework::commands::server::command::static_catalog::STATIC_COMMAND_CATALOG
    }

    fn selected_db(&self) -> u32 {
        *self.selected_db
    }

    fn db_mut_at(&mut self, index: u32) -> Option<&mut Db> {
        self.dbs.get_mut(index)
    }

    fn num_dbs(&self) -> usize {
        self.dbs.num_dbs()
    }

    fn now_ms(&self) -> u64 {
        self.now_ms
    }

    fn flushdb(&mut self, _mode: FlushMode) -> CommandOutcome {
        let Some(db) = self.dbs.get_mut(*self.selected_db) else {
            return CommandOutcome::reply(
                crate::engine::command::CommandError::db_index_out_of_range(),
            );
        };
        db.flush();
        CommandOutcome::reply(Value::ok())
    }

    fn flushall(&mut self, _mode: FlushMode) -> CommandOutcome {
        self.dbs.flush_all();
        CommandOutcome::reply(Value::ok())
    }

    fn select_db(&mut self, index: u32) -> CommandOutcome {
        if index as usize >= self.dbs.num_dbs() {
            return CommandOutcome::reply(
                crate::engine::command::CommandError::db_index_out_of_range(),
            );
        }
        *self.selected_db = index;
        CommandOutcome::reply(Value::ok())
    }

    fn swapdb(&mut self, first: u32, second: u32) -> CommandOutcome {
        if first as usize >= self.dbs.num_dbs() || second as usize >= self.dbs.num_dbs() {
            return CommandOutcome::reply(Value::error("ERR DB index is out of range"));
        }
        self.dbs.swap(first, second);
        CommandOutcome::reply(Value::ok())
    }
}
