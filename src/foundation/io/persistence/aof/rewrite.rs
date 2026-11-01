mod encode;
mod file;
mod slot;

pub use file::{finalize_rewrite, write_rewrite_temp};
pub use slot::write_filtered_command_snapshot;
use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AofRewriteFileOptions {
    pub dir: PathBuf,
    pub filename: String,
    pub appenddirname: String,
    pub use_rdb_preamble: bool,
    pub timestamp_enabled: bool,
    pub key_delay_us: i64,
    pub rdb_compression: crate::foundation::io::persistence::rdb::compression::RdbCompression,
    pub rdb_checksum: bool,
    pub incremental_fsync: bool,
}

impl AofRewriteFileOptions {
    pub fn aof_dir(&self) -> PathBuf {
        if self.appenddirname.is_empty() {
            self.dir.clone()
        } else {
            self.dir.join(&self.appenddirname)
        }
    }

    pub fn temp_path(&self, pid: i32) -> PathBuf {
        self.aof_dir()
            .join(format!("temp-rewriteaof-bg-{}.aof", pid))
    }
}
