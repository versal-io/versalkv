use super::Journal;
use crate::foundation::io::persistence::AofAppender;
use crate::foundation::io::persistence::AofCommandPlayer;
use crate::foundation::io::persistence::AofFsync;
use crate::foundation::io::persistence::AofLoadError;

pub struct AofJournal {
    appender: AofAppender,
    player: Option<Box<dyn AofCommandPlayer>>,
}

impl AofJournal {
    pub fn new(appender: AofAppender) -> Self {
        Self {
            appender,
            player: None,
        }
    }

    pub fn set_player(&mut self, player: Box<dyn AofCommandPlayer>) {
        self.player = Some(player);
    }

    /// Buffer a frame into the appender without flushing it to the backend.
    pub fn feed(&mut self, frame: &[u8]) {
        self.appender.feed(frame);
    }

    /// Flush buffered frames, forcing an fsync when `force` is set.
    pub fn flush(&mut self, force: bool) -> std::io::Result<()> {
        self.appender.flush(force)
    }

    /// Current on-disk size tracked by the appender.
    pub fn current_size(&self) -> u64 {
        self.appender.current_size()
    }

    /// Heap bytes reserved by the appender's pending buffer.
    pub fn buffer_allocation_bytes(&self) -> usize {
        self.appender.buffer_allocation_bytes()
    }

    /// Whether the most recent write/flush succeeded.
    pub fn last_write_ok(&self) -> bool {
        self.appender.last_write_ok()
    }

    /// Active fsync policy of the underlying appender.
    pub fn fsync_mode(&self) -> AofFsync {
        self.appender.fsync_mode()
    }

    /// Switch the appender's fsync policy.
    pub fn set_fsync_mode(&mut self, mode: AofFsync) -> std::io::Result<()> {
        self.appender.set_fsync_mode(mode)
    }

    /// Path of the active AOF file.
    pub fn path(&self) -> &str {
        self.appender.path()
    }

    /// Flush any pending data and stop the background fsync thread.
    pub fn shutdown(&mut self) -> std::io::Result<()> {
        self.appender.shutdown()
    }
}

impl Journal for AofJournal {
    type Error = std::io::Error;

    fn append(&mut self, entry: &[u8]) -> Result<(), Self::Error> {
        self.appender.feed(entry);
        self.appender.flush(false)
    }

    fn replay(&mut self) -> Result<(), Self::Error> {
        let path = self.appender.path().to_string();
        let bytes = std::fs::read(&path).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("AOF file not found at {}: {}", path, e),
            )
        })?;
        if bytes.is_empty() {
            return Ok(());
        }
        let player = self.player.as_deref_mut().ok_or_else(|| {
            std::io::Error::other("AofJournal::replay requires a player to be set via set_player()")
        })?;
        match crate::foundation::io::persistence::load_aof_bytes(&bytes, player) {
            Ok(_stats) => Ok(()),
            Err(AofLoadError::Corrupt) => Ok(()),
            Err(AofLoadError::Io(e)) => Err(e),
            Err(AofLoadError::ExecError(e)) => Err(std::io::Error::other(e)),
        }
    }

    fn rewrite(&mut self) -> Result<(), Self::Error> {
        let path = self.appender.path().to_string();
        let tmp_path = format!("{}.tmp", path);

        self.appender.flush(true)?;

        let content = std::fs::read(&path)?;

        std::fs::write(&tmp_path, &content)?;

        std::fs::rename(&tmp_path, path)?;

        Ok(())
    }

    fn truncate(&mut self, size: u64) -> Result<(), Self::Error> {
        self.appender.truncate(size)
    }
}

#[cfg(test)]
mod tests;
