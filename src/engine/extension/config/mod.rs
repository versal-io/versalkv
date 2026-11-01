pub mod encoding;
pub mod items;
pub mod persistence;
pub mod qos;
pub mod replication;
pub mod security;
pub mod server;
pub mod spec;
pub mod spec_registry;

pub use encoding::EncodingConfig;
pub use persistence::PersistenceConfig;
pub use replication::ReplicationConfig;
pub use security::SecurityConfig;
pub use server::ServerConfig;

#[derive(Clone)]
pub struct RuntimeConfig {
    pub replication: ReplicationConfig,
    pub persistence: PersistenceConfig,
    pub encoding: EncodingConfig,
    pub security: SecurityConfig,
    pub server: ServerConfig,
}

impl RuntimeConfig {
    pub fn new() -> Self {
        Self {
            replication: ReplicationConfig::new(),
            persistence: PersistenceConfig::new(),
            encoding: EncodingConfig::new(),
            security: SecurityConfig::new(),
            server: ServerConfig::new(),
        }
    }

    pub fn is_any_keyspace_event_enabled(&self) -> bool {
        !self.server.notify_keyspace_events.is_empty()
    }

    pub fn is_event_type_enabled(&self, event_type: char) -> bool {
        if self.server.notify_keyspace_events.contains('A')
            && !matches!(event_type, 'K' | 'E' | 'm' | 'n')
        {
            return true;
        }
        self.server.notify_keyspace_events.contains(event_type)
    }

    pub fn rdb_version_check_relaxed(&self) -> bool {
        self.persistence.rdb_version_check_relaxed
    }
}
