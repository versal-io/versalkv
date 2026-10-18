use crate::foundation::io::sys::time::mstime;
use std::collections::VecDeque;
use std::process::{Child, Command, Stdio};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScriptType {
    Notification,
    ClientReconfig,
}

impl ScriptType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ScriptType::Notification => "notification",
            ScriptType::ClientReconfig => "client-reconfig",
        }
    }
}

#[derive(Debug)]
pub struct ScriptJob {
    pub script_type: ScriptType,
    pub script_path: String,
    pub args: Vec<String>,
    pub start_time: i64,
    pub pid: Option<u32>,
    pub retry_count: u32,
    pub max_runtime_ms: i64,
}

#[derive(Debug)]
pub struct SentinelScriptEngine {
    pub pending: VecDeque<ScriptJob>,
    pub running: Vec<(ScriptJob, Child)>,
    pub debug_max_runtime_ms: i64,
    pub debug_retry_delay_ms: i64,
}

impl SentinelScriptEngine {
    pub fn new() -> Self {
        Self {
            pending: VecDeque::new(),
            running: Vec::new(),
            debug_max_runtime_ms: 60000,
            debug_retry_delay_ms: 30000,
        }
    }

    pub fn enqueue(&mut self, script_type: ScriptType, script_path: &str, args: Vec<String>) {
        self.pending.push_back(ScriptJob {
            script_type,
            script_path: script_path.to_string(),
            args,
            start_time: 0,
            pid: None,
            retry_count: 0,
            max_runtime_ms: self.debug_max_runtime_ms,
        });
    }

    pub fn run_pending(&mut self) {
        while !self.pending.is_empty() {
            if self.running.len() >= 4 {
                break;
            }
            let job = self.pending.pop_front().unwrap();
            let args: Vec<&str> = job.args.iter().map(|s| s.as_str()).collect();
            match build_script_command(&job.script_path, &args).spawn() {
                Ok(child) => {
                    let pid = child.id();
                    let running_job = ScriptJob {
                        pid: Some(pid),
                        start_time: mstime(),
                        ..job
                    };
                    self.running.push((running_job, child));
                }
                Err(e) => {
                    log::warn!(
                        "sentinel script '{}' failed to spawn: {}",
                        job.script_path,
                        e
                    );
                }
            }
        }
    }

    pub fn collect_terminated(&mut self) {
        let mut i = 0;
        while i < self.running.len() {
            let should_remove = {
                let (job, child) = &mut self.running[i];
                match child.try_wait() {
                    Ok(Some(status)) => {
                        if status.success() {
                            log::info!(
                                "sentinel script '{}' completed successfully",
                                job.script_path
                            );
                        } else {
                            log::warn!(
                                "sentinel script '{}' exited with {}",
                                job.script_path,
                                status
                            );
                        }
                        true
                    }
                    Ok(None) => false,
                    Err(e) => {
                        log::warn!("sentinel script '{}' wait error: {}", job.script_path, e);
                        true
                    }
                }
            };
            if should_remove {
                self.running.swap_remove(i);
            } else {
                i += 1;
            }
        }
    }

    pub fn kill_timedout(&mut self) {
        let now = mstime();
        let mut i = 0;
        while i < self.running.len() {
            let should_remove = {
                let (job, child) = &mut self.running[i];
                if now - job.start_time > job.max_runtime_ms {
                    log::warn!(
                        "sentinel script '{}' timed out (>{}ms), killing",
                        job.script_path,
                        job.max_runtime_ms
                    );
                    let _ = child.kill();
                    let _ = child.wait();
                    true
                } else {
                    false
                }
            };
            if should_remove {
                self.running.swap_remove(i);
            } else {
                i += 1;
            }
        }
    }
}

fn build_script_command(script_path: &str, args: &[&str]) -> Command {
    let mut command = Command::new(script_path);
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    command
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn script_engine_queues_and_runs() {
        let mut engine = SentinelScriptEngine::new();
        engine.enqueue(
            ScriptType::Notification,
            "/bin/sh",
            vec!["-c".to_string(), "exit 0".to_string()],
        );
        assert_eq!(engine.pending.len(), 1);
        engine.run_pending();
        let spawned = engine.running.len();
        wait_until_empty(&mut engine);
        assert_eq!(spawned, 1, "sentinel script must spawn successfully");
        assert!(engine.pending.is_empty());
    }

    #[test]
    fn script_engine_respects_max_concurrent() {
        let mut engine = SentinelScriptEngine::new();
        for _ in 0..6 {
            engine.enqueue(
                ScriptType::Notification,
                "/bin/sh",
                vec!["-c".to_string(), "exit 0".to_string()],
            );
        }
        assert_eq!(engine.pending.len(), 6);
        engine.run_pending();
        let first_spawned = engine.running.len();
        let first_pending = engine.pending.len();
        wait_until_empty(&mut engine);
        engine.run_pending();
        let second_spawned = engine.running.len();
        wait_until_empty(&mut engine);
        assert_eq!(first_spawned, 4);
        assert_eq!(first_pending, 2);
        assert_eq!(second_spawned, 2);
        assert!(engine.pending.is_empty());
    }

    #[test]
    fn script_engine_enqueue_with_args() {
        let mut engine = SentinelScriptEngine::new();
        let args = vec!["-c".to_string(), "exit 0".to_string()];
        engine.enqueue(ScriptType::ClientReconfig, "/bin/sh", args.clone());
        assert_eq!(engine.pending.len(), 1);
        let job = engine.pending.front().expect("enqueued script job");
        assert_eq!(job.script_type, ScriptType::ClientReconfig);
        assert_eq!(job.args, args);
        assert_eq!(job.script_path, "/bin/sh");
    }

    fn wait_until_empty(engine: &mut SentinelScriptEngine) {
        use std::time::{Duration, Instant};

        let mut deadline = Instant::now() + Duration::from_secs(5);
        let mut has_timed_out = false;
        let mut has_failed = false;
        while !engine.running.is_empty() {
            let mut has_all_exited = true;
            for (_, child) in &mut engine.running {
                match child.try_wait().expect("wait for sentinel script") {
                    Some(status) => has_failed |= !status.success(),
                    None => has_all_exited = false,
                }
            }
            if has_all_exited {
                engine.collect_terminated();
                assert!(engine.running.is_empty());
                break;
            }
            if Instant::now() >= deadline {
                assert!(!has_timed_out, "timed out reaping killed sentinel scripts");
                for (_, child) in &mut engine.running {
                    let _ = child.kill();
                }
                has_timed_out = true;
                deadline = Instant::now() + Duration::from_secs(5);
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(!has_timed_out, "sentinel script exceeded its deadline");
        assert!(!has_failed, "sentinel script exited unsuccessfully");
    }
}
