use crate::engine::core::CommandOutcome;
use crate::engine::core::Value;
use crate::foundation::io::store::kv_engine::Db;
use bytes::Bytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushMode {
    Default,
    Sync,
    Async,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimeLimits {
    pub list_max_listpack_size: i64,
    pub set_encoding: (usize, usize, usize),
    pub hash_encoding: (usize, usize),
    pub zset_encoding: (usize, usize),
    pub stream_node: (usize, usize),
    pub proto_max_bulk_len: usize,
    pub normal_output_buffer_hard_limit: usize,
}

impl Default for RuntimeLimits {
    fn default() -> Self {
        Self {
            list_max_listpack_size: -2,
            set_encoding: (512, 128, 64),
            hash_encoding: (512, 64),
            zset_encoding: (128, 64),
            stream_node: (100, 4096),
            proto_max_bulk_len: 512 * 1024 * 1024,
            normal_output_buffer_hard_limit: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuntimePolicies {
    pub read_expired_values: bool,
    pub suppress_lazy_expire: bool,
    pub touch_read_keys: bool,
    pub mark_replica_local_expire: bool,
    pub extended_redis_compatibility: bool,
    pub hll_sparse_max_bytes: i64,
    pub preserve_expired_keys: bool,
    pub rdb_compression: bool,
    pub skip_checksum_validation: bool,
    pub rdb_version_check_relaxed: bool,
    pub lazyfree_server_del: bool,
    pub active_expire: bool,
    pub lazyfree_user_del: bool,
}

impl Default for RuntimePolicies {
    fn default() -> Self {
        Self {
            read_expired_values: false,
            suppress_lazy_expire: false,
            touch_read_keys: false,
            mark_replica_local_expire: false,
            extended_redis_compatibility: false,
            hll_sparse_max_bytes: 3_000,
            preserve_expired_keys: false,
            rdb_compression: true,
            skip_checksum_validation: false,
            rdb_version_check_relaxed: false,
            lazyfree_server_del: false,
            active_expire: true,
            lazyfree_user_del: false,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RuntimeSession {
    pub resp3: bool,
    pub output_buffer_memory_usage: usize,
}

pub trait CommandRuntime {
    fn command_catalog(&self) -> &dyn crate::framework::commands::introspection::CommandCatalog;
    fn selected_db(&self) -> u32;
    fn db_mut_at(&mut self, index: u32) -> Option<&mut Db>;
    fn num_dbs(&self) -> usize;
    fn now_ms(&self) -> u64;
    fn now_us(&self) -> u64 {
        self.now_ms().saturating_mul(1_000)
    }
    fn limits(&self) -> RuntimeLimits {
        RuntimeLimits::default()
    }
    fn policies(&self) -> RuntimePolicies {
        RuntimePolicies::default()
    }
    fn session(&self) -> RuntimeSession {
        RuntimeSession::default()
    }

    fn ensure_db_access(
        &mut self,
        index: u32,
        _command: &str,
    ) -> Result<(), crate::engine::core::Value> {
        if index as usize >= self.num_dbs() {
            Err(crate::engine::command::CommandError::db_index_out_of_range())
        } else {
            Ok(())
        }
    }

    fn validate_sort_access(
        &self,
        _command: &str,
        _args: &crate::engine::command::CommandArgs,
    ) -> Result<bool, crate::engine::core::Value> {
        Ok(true)
    }

    fn db_mut(&mut self) -> Option<&mut Db> {
        let index = self.selected_db();
        self.db_mut_at(index)
    }

    fn read_expired_values(&self) -> bool {
        self.policies().read_expired_values
    }
    fn suppress_lazy_expire(&self) -> bool {
        self.policies().suppress_lazy_expire
    }
    fn should_touch_read_keys(&self) -> bool {
        self.policies().touch_read_keys
    }
    fn should_mark_replica_local_expire(&self) -> bool {
        self.policies().mark_replica_local_expire
    }
    fn list_max_listpack_size(&self) -> i64 {
        self.limits().list_max_listpack_size
    }
    fn set_encoding_limits(&self) -> (usize, usize, usize) {
        self.limits().set_encoding
    }
    fn hash_encoding_limits(&self) -> (usize, usize) {
        self.limits().hash_encoding
    }
    fn zset_encoding_limits(&self) -> (usize, usize) {
        self.limits().zset_encoding
    }
    fn stream_node_limits(&self) -> (usize, usize) {
        self.limits().stream_node
    }
    fn proto_max_bulk_len(&self) -> usize {
        self.limits().proto_max_bulk_len
    }
    fn normal_output_buffer_hard_limit(&self) -> usize {
        self.limits().normal_output_buffer_hard_limit
    }
    fn output_buffer_memory_usage(&self) -> usize {
        self.session().output_buffer_memory_usage
    }
    fn extended_redis_compatibility(&self) -> bool {
        self.policies().extended_redis_compatibility
    }
    fn hll_sparse_max_bytes(&self) -> i64 {
        self.policies().hll_sparse_max_bytes
    }
    fn preserve_expired_keys(&self) -> bool {
        self.policies().preserve_expired_keys
    }
    fn rdb_compression(&self) -> bool {
        self.policies().rdb_compression
    }
    fn skip_checksum_validation(&self) -> bool {
        self.policies().skip_checksum_validation
    }
    fn rdb_version_check_relaxed(&self) -> bool {
        self.policies().rdb_version_check_relaxed
    }
    fn should_use_lazyfree_server_del(&self) -> bool {
        self.policies().lazyfree_server_del
    }
    fn active_expire_enabled(&self) -> bool {
        self.policies().active_expire
    }
    fn resp3(&self) -> bool {
        self.session().resp3
    }
    fn ping_response(&self, message: Option<&Bytes>) -> Value {
        match message {
            Some(message) => Value::bulk(message.clone()),
            None => Value::pong(),
        }
    }
    fn should_defer_lazyfree_user_del(&self) -> bool {
        self.policies().lazyfree_user_del
    }
    fn reset_replication_selected_db(&mut self) {}

    fn flushdb(&mut self, mode: FlushMode) -> CommandOutcome;
    fn flushall(&mut self, mode: FlushMode) -> CommandOutcome;
    fn select_db(&mut self, index: u32) -> CommandOutcome;
    fn swapdb(&mut self, first: u32, second: u32) -> CommandOutcome;
}
