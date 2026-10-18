use crate::engine::extension::config::PersistenceConfig;
use crate::foundation::io::persistence::AofFsync;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RdbSaveOptions {
    pub dir: PathBuf,
    pub filename: String,
    pub key_delay_us: i64,
    pub compression: bool,
    pub checksum: bool,
    pub incremental_fsync: bool,
}

impl RdbSaveOptions {
    pub fn from_config(config: &PersistenceConfig) -> Self {
        Self {
            dir: PathBuf::from(&config.dir),
            filename: config.dbfilename.clone(),
            key_delay_us: config.rdb_key_save_delay,
            compression: config.rdbcompression,
            checksum: config.rdbchecksum,
            incremental_fsync: config.rdb_save_incremental_fsync,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AofOptions {
    pub dir: PathBuf,
    pub filename: String,
    pub appenddirname: String,
    pub fsync: AofFsync,
    pub use_rdb_preamble: bool,
}

impl AofOptions {
    pub fn from_config(config: &PersistenceConfig) -> Self {
        Self {
            dir: PathBuf::from(&config.dir),
            filename: config.aof_filename.clone(),
            appenddirname: config.aof_appenddirname.clone(),
            fsync: config.aof_fsync,
            use_rdb_preamble: config.aof_use_rdb_preamble,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AofOptions, RdbSaveOptions};
    use crate::engine::extension::config::PersistenceConfig;
    use crate::foundation::io::persistence::AofFsync;
    use std::path::PathBuf;

    #[test]
    fn options_copy_only_operation_inputs_from_config() {
        let mut config = PersistenceConfig::new();
        config.dir = "/data".to_string();
        config.dbfilename = "snapshot.rdb".to_string();
        config.rdb_key_save_delay = 7;
        config.rdbcompression = false;
        config.rdbchecksum = false;
        config.aof_filename = "commands.aof".to_string();
        config.aof_appenddirname = "aof".to_string();
        config.aof_fsync = AofFsync::Always;
        config.aof_use_rdb_preamble = false;

        assert_eq!(
            RdbSaveOptions::from_config(&config),
            RdbSaveOptions {
                dir: PathBuf::from("/data"),
                filename: "snapshot.rdb".to_string(),
                key_delay_us: 7,
                compression: false,
                checksum: false,
                incremental_fsync: true,
            }
        );
        assert_eq!(
            AofOptions::from_config(&config),
            AofOptions {
                dir: PathBuf::from("/data"),
                filename: "commands.aof".to_string(),
                appenddirname: "aof".to_string(),
                fsync: AofFsync::Always,
                use_rdb_preamble: false,
            }
        );
    }
}
