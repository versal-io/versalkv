use crate::foundation::io::persistence::rdb::compression::RdbCompression;
use crate::foundation::io::persistence::AofFsync;

#[derive(Clone, Debug)]
pub struct SaveParam {
    pub seconds: i64,
    pub changes: i64,
}

pub const BGSAVE_RETRY_DELAY_SEC: i64 = 60;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum BgsaveMethod {
    #[default]
    Fork,
    Forkless,
}

impl BgsaveMethod {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Fork => "fork",
            Self::Forkless => "forkless",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        if value.eq_ignore_ascii_case("fork") {
            Some(Self::Fork)
        } else if value.eq_ignore_ascii_case("forkless") {
            Some(Self::Forkless)
        } else {
            None
        }
    }
}

pub fn validate_aof_basename(value: &str, setting: &str) -> Result<(), String> {
    if value.is_empty() {
        return Err(format!("{setting} can't be empty"));
    }
    if value.contains('/') || value.contains('\\') {
        let kind = if setting == "appendfilename" {
            "filename"
        } else {
            "dirname"
        };
        return Err(format!("{setting} can't be a path, just a {kind}"));
    }
    Ok(())
}

pub fn default_saveparams() -> Vec<SaveParam> {
    vec![
        SaveParam {
            seconds: 3600,
            changes: 1,
        },
        SaveParam {
            seconds: 300,
            changes: 100,
        },
        SaveParam {
            seconds: 60,
            changes: 10000,
        },
    ]
}

#[derive(Clone)]
pub struct PersistenceConfig {
    pub dir: String,
    pub dbfilename: String,
    pub forkless_infrastructure_enabled: bool,
    pub bgsave_default_method: BgsaveMethod,
    pub rdb_version_check_relaxed: bool,
    pub rdbcompression: RdbCompression,
    pub rdbchecksum: bool,
    pub rdb_save_incremental_fsync: bool,
    pub rdb_del_sync_files: bool,
    pub stop_writes_on_bgsave_error: bool,
    pub aof_enabled: bool,
    pub aof_filename: String,
    pub aof_appenddirname: String,
    pub aof_fsync: AofFsync,
    pub aof_no_fsync_on_rewrite: bool,
    pub aof_use_rdb_preamble: bool,
    pub aof_load_truncated: bool,
    pub aof_timestamp_enabled: bool,
    pub aof_disable_auto_gc: bool,
    pub aof_rewrite_incremental_fsync: bool,
    pub saveparams: Vec<SaveParam>,
    pub aof_rewrite_perc: i64,
    pub aof_rewrite_min_size: u64,
    /// Signed microsecond debug delay; negative values sample a one-us sleep.
    pub rdb_key_save_delay: i64,
    /// Signed microsecond debug delay; negative values sample a one-us sleep.
    pub key_load_delay: i64,
    pub loading_process_events_interval_bytes: u64,
}

impl PersistenceConfig {
    pub fn new() -> Self {
        Self {
            dir: "./".to_string(),
            dbfilename: "dump.rdb".to_string(),
            forkless_infrastructure_enabled: false,
            bgsave_default_method: BgsaveMethod::Fork,
            rdb_version_check_relaxed: false,
            rdbcompression: RdbCompression::Yes,
            rdbchecksum: true,
            rdb_save_incremental_fsync: true,
            rdb_del_sync_files: false,
            stop_writes_on_bgsave_error: true,
            aof_enabled: false,
            aof_filename: "appendonly.aof".to_string(),
            aof_appenddirname: "appendonlydir".to_string(),
            aof_fsync: AofFsync::EverySec,
            aof_no_fsync_on_rewrite: false,
            aof_use_rdb_preamble: true,
            aof_load_truncated: true,
            aof_timestamp_enabled: false,
            aof_disable_auto_gc: false,
            aof_rewrite_incremental_fsync: true,
            saveparams: default_saveparams(),
            aof_rewrite_perc: 100,
            aof_rewrite_min_size: 64 * 1024 * 1024,
            rdb_key_save_delay: 0,
            key_load_delay: 0,
            loading_process_events_interval_bytes: 2 * 1024 * 1024,
        }
    }

    pub fn validate_bgsave_method(&self) -> Result<(), String> {
        if self.bgsave_default_method == BgsaveMethod::Forkless
            && !self.forkless_infrastructure_enabled
        {
            return Err(
                "'bgsave-default-method forkless' can only be selected when the server was started with 'forkless-infrastructure-enabled yes'"
                    .to_string(),
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::PersistenceConfig;

    #[test]
    fn loading_event_interval_default_matches_valkey() {
        assert_eq!(
            PersistenceConfig::new().loading_process_events_interval_bytes,
            2 * 1024 * 1024
        );
    }
}
