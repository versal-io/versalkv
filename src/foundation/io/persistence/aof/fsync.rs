use super::AofSyncHandle;
use std::fs::File;
use std::io;
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::thread::{self, JoinHandle};
use std::time::Duration;

pub(super) struct FsyncThread {
    handle: Option<JoinHandle<io::Result<()>>>,
    shutdown: Sender<bool>,
    stop_error: Option<io::ErrorKind>,
    progress: AofSyncHandle,
}

impl FsyncThread {
    pub(super) fn spawn(fd: File, progress: AofSyncHandle) -> io::Result<Self> {
        Self::spawn_with_sync(progress, move || fd.sync_data(), Duration::from_secs(1))
    }

    pub(super) fn spawn_with_sync(
        progress: AofSyncHandle,
        sync: impl Fn() -> io::Result<()> + Send + 'static,
        interval: Duration,
    ) -> io::Result<Self> {
        let (shutdown, receiver) = mpsc::channel();
        let th_progress = progress.clone();
        let handle = thread::Builder::new()
            .name("aof-fsync".to_string())
            .spawn(move || loop {
                match receiver.recv_timeout(interval) {
                    Ok(false) => return Ok(()),
                    Ok(true) | Err(RecvTimeoutError::Disconnected) => {
                        return th_progress.sync_with(&sync);
                    }
                    Err(RecvTimeoutError::Timeout) => {
                        // Completion records the error and wakes the observer before retrying.
                        if let Err(error) = th_progress.sync_with(&sync) {
                            log::warn!("Background AOF sync failed: {error}");
                        }
                    }
                }
            })?;
        Ok(Self {
            handle: Some(handle),
            shutdown,
            stop_error: None,
            progress,
        })
    }

    pub(super) fn stop(&mut self, final_sync: bool) -> io::Result<()> {
        let Some(handle) = self.handle.take() else {
            return self.stop_error.map_or(Ok(()), |kind| Err(kind.into()));
        };
        // A disconnected receiver means the worker has already exited; join is authoritative.
        let _ = self.shutdown.send(final_sync);
        let result = handle.join().unwrap_or_else(|_| {
            self.progress
                .sync_with(|| Err(io::Error::other("AOF sync thread panicked")))
        });
        self.stop_error = result.as_ref().err().map(io::Error::kind);
        result
    }
}

impl Drop for FsyncThread {
    fn drop(&mut self) {
        if self.handle.is_some() {
            if let Err(error) = self.stop(true) {
                log::error!("Final AOF sync failed while dropping worker: {error}");
            }
        }
    }
}
