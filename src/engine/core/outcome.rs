use bytes::Bytes;

use crate::foundation::io::protocol::Value;
use crate::foundation::io::store::Robj;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreEvent {
    pub kind: &'static str,
    pub key: Bytes,
}

#[derive(Default, Debug)]
pub struct CommandEffects {
    pub changed_keys: Vec<(u32, Bytes)>,
    pub events: Vec<CoreEvent>,
    pub db_events: Vec<(u32, CoreEvent)>,
    pub skip_propagate: bool,
    pub custom_propagations: Vec<Vec<Bytes>>,
    pub deferred_objects: Vec<Robj>,
    pub expired_hash_fields: u64,
    pub suppress_reply: bool,
    pub close_connection: bool,
}

pub struct CommandOutcome {
    pub reply: Value,
    pub effects: CommandEffects,
}

impl CommandOutcome {
    pub fn reply(reply: Value) -> Self {
        Self {
            reply,
            effects: CommandEffects::default(),
        }
    }
}
