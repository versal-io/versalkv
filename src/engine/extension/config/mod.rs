pub mod encoding;
pub mod items;
pub mod persistence;
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
    pub fn validate_replication_durability(&self) -> Result<(), String> {
        if self.replication.repl_after_aof_fsync
            && (!self.persistence.aof_enabled
                || self.persistence.aof_fsync == crate::foundation::io::persistence::AofFsync::No
                || self.persistence.aof_no_fsync_on_rewrite)
        {
            return Err("ERR repl-after-aof-fsync requires appendonly yes, appendfsync always or everysec, and no-appendfsync-on-rewrite no".to_string());
        }
        Ok(())
    }

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

#[cfg(test)]
mod durability_tests {
    use super::*;

    #[test]
    fn durable_replication_requires_compatible_aof_configuration() {
        let registry = spec_registry::ConfigRegistry::new();
        let mut config = RuntimeConfig::new();
        assert!(config.validate_replication_durability().is_ok());
        registry
            .try_set(&mut config, "repl-after-aof-fsync", "yes")
            .expect("durability configuration is registered")
            .unwrap();
        assert!(config.validate_replication_durability().is_err());
        config.persistence.aof_enabled = true;
        assert!(config.validate_replication_durability().is_ok());
        config.persistence.aof_fsync = crate::foundation::io::persistence::AofFsync::No;
        assert!(config.validate_replication_durability().is_err());
        config.persistence.aof_fsync = crate::foundation::io::persistence::AofFsync::Always;
        config.persistence.aof_no_fsync_on_rewrite = true;
        assert!(config.validate_replication_durability().is_err());
    }
}
