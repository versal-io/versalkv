use super::shared::{RuntimeLimits, RuntimePolicies};
use super::{CommandRuntime, FlushMode};
use crate::engine::core::{CommandOutcome, Value};
use crate::foundation::io::store::kv_engine::{Db, DbSet};

pub(crate) struct TestCommandRuntime {
    pub dbs: DbSet,
    pub selected_db: u32,
    pub now_ms: u64,
    pub read_expired_values: bool,
    pub touch_read_keys: bool,
    pub active_expire_enabled: bool,
    pub stream_node_limits: (usize, usize),
}

impl TestCommandRuntime {
    pub(crate) fn new(database_count: u32, now_ms: u64) -> Self {
        Self {
            dbs: DbSet::new(database_count),
            selected_db: 0,
            now_ms,
            read_expired_values: false,
            touch_read_keys: false,
            active_expire_enabled: true,
            stream_node_limits: (100, 4096),
        }
    }
}

impl CommandRuntime for TestCommandRuntime {
    fn command_catalog(&self) -> &dyn crate::framework::commands::introspection::CommandCatalog {
        &crate::framework::commands::server::command::static_catalog::STATIC_COMMAND_CATALOG
    }

    fn selected_db(&self) -> u32 {
        self.selected_db
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

    fn limits(&self) -> RuntimeLimits {
        RuntimeLimits {
            stream_node: self.stream_node_limits,
            ..RuntimeLimits::default()
        }
    }

    fn policies(&self) -> RuntimePolicies {
        RuntimePolicies {
            read_expired_values: self.read_expired_values,
            touch_read_keys: self.touch_read_keys,
            suppress_lazy_expire: self.read_expired_values,
            preserve_expired_keys: self.read_expired_values,
            active_expire: self.active_expire_enabled,
            ..RuntimePolicies::default()
        }
    }

    fn flushdb(&mut self, _mode: FlushMode) -> CommandOutcome {
        if let Some(db) = self.dbs.get_mut(self.selected_db) {
            db.flush();
        }
        CommandOutcome::reply(Value::ok())
    }

    fn flushall(&mut self, _mode: FlushMode) -> CommandOutcome {
        self.dbs.flush_all();
        CommandOutcome::reply(Value::ok())
    }

    fn select_db(&mut self, index: u32) -> CommandOutcome {
        self.selected_db = index;
        CommandOutcome::reply(Value::ok())
    }

    fn swapdb(&mut self, first: u32, second: u32) -> CommandOutcome {
        self.dbs.swap(first, second);
        CommandOutcome::reply(Value::ok())
    }
}
