use super::Journal;
use crate::foundation::io::persistence::AofAppender;
use crate::foundation::io::persistence::AofCommandPlayer;
use crate::foundation::io::persistence::AofLoadError;

pub struct AofJournal {
    pub appender: AofAppender,
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
