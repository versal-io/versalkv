use std::io;
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread::JoinHandle;

enum Job {
    Save {
        path: PathBuf,
        content: String,
        do_fsync: bool,
    },
    Barrier(Sender<()>),
}

pub(super) struct SaveCompletion {
    pub result: io::Result<()>,
    pub finished_at_sec: i64,
}

/// One ordered I/O queue per server. Barriers prevent an older background
/// write from replacing a newer synchronous save.
pub(crate) struct ClusterConfigWriter {
    jobs: Option<Sender<Job>>,
    completions: Receiver<SaveCompletion>,
    thread: Option<JoinHandle<()>>,
}

impl ClusterConfigWriter {
    pub(super) fn new() -> io::Result<Self> {
        let (jobs, receiver) = mpsc::channel();
        let (completed, completions) = mpsc::channel();
        let thread = std::thread::Builder::new()
            .name("cluster-config-save".into())
            .spawn(move || {
                while let Ok(job) = receiver.recv() {
                    match job {
                        Job::Save {
                            path,
                            content,
                            do_fsync,
                        } => {
                            let result = super::cluster_config::persist_cluster_config_with_fsync(
                                &path, &content, do_fsync,
                            );
                            let _ = completed.send(SaveCompletion {
                                result,
                                finished_at_sec: crate::foundation::io::sys::time::mstime() / 1_000,
                            });
                        }
                        Job::Barrier(done) => {
                            let _ = done.send(());
                        }
                    }
                }
            })?;
        Ok(Self {
            jobs: Some(jobs),
            completions,
            thread: Some(thread),
        })
    }

    pub(super) fn submit(&self, path: PathBuf, content: String, do_fsync: bool) -> io::Result<()> {
        self.send(Job::Save {
            path,
            content,
            do_fsync,
        })
    }

    fn send(&self, job: Job) -> io::Result<()> {
        self.jobs
            .as_ref()
            .ok_or_else(worker_stopped)?
            .send(job)
            .map_err(|_| worker_stopped())
    }

    pub(super) fn drain(&self) -> io::Result<()> {
        let (done, wait) = mpsc::channel();
        self.send(Job::Barrier(done))?;
        wait.recv().map_err(|_| worker_stopped())
    }

    pub(super) fn take_completions(&self) -> Vec<SaveCompletion> {
        self.completions.try_iter().collect()
    }
}

fn worker_stopped() -> io::Error {
    io::Error::new(
        io::ErrorKind::BrokenPipe,
        "cluster config save worker stopped",
    )
}

impl Drop for ClusterConfigWriter {
    fn drop(&mut self) {
        self.jobs.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
