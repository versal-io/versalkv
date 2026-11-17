use crate::engine::state::blocking_registry::BlockingClientRegistry;
use crate::foundation::io::store::kv_engine::DbSet;
use crate::foundation::memory::util_hash::FxHashMap;
use crate::foundation::memory::util_time::MonotonicInstant;
use std::borrow::Cow;

#[derive(Clone)]
pub struct ClientInfo {
    /// Connection flags string. Uses `Cow` so the common "N" (normal) case
    /// avoids a heap allocation; mutation sites call `to_mut()` to obtain an
    /// owned `String` only when a non-default flag is set.
    pub flags: Cow<'static, str>,
    pub db: u32,
    pub name: String,
    pub rbs: i64,
    pub watched_keys: usize,
    pub lib_name: String,
    pub lib_ver: String,
    pub last_interaction: MonotonicInstant,
}

pub struct DbContext {
    pub dbs: DbSet,
    pub blocking_registry: BlockingClientRegistry,
    pub client_infos: FxHashMap<usize, ClientInfo>,
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
            client_infos: FxHashMap::default(),
        }
    }
}
