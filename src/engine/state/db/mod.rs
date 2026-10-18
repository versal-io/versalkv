use crate::engine::state::blocking_registry::BlockingClientRegistry;
use crate::foundation::io::store::kv_engine::DbSet;
use std::collections::HashMap;
use std::time::Instant;

#[derive(Clone)]
pub struct ClientInfo {
    pub last_cmd: String,
    pub flags: String,
    pub db: u32,
    pub name: String,
    pub rbs: i64,
    pub watched_keys: usize,
    pub lib_name: String,
    pub lib_ver: String,
    pub last_interaction: Instant,
}

pub struct DbContext {
    pub dbs: DbSet,
    pub blocking_registry: BlockingClientRegistry,
    pub client_infos: HashMap<usize, ClientInfo>,
}

impl DbContext {
    pub fn new(num_dbs: u32) -> Self {
        Self::new_with_slot_router(num_dbs, 0, |_key| 0)
    }

    pub fn new_with_slot_router(
        num_dbs: u32,
        slot_bits: u32,
        slot_router: fn(&[u8]) -> usize,
    ) -> Self {
        Self {
            dbs: DbSet::new_with_slot_router(num_dbs, slot_bits, slot_router),
            blocking_registry: BlockingClientRegistry::new(),
            client_infos: HashMap::new(),
        }
    }
}
