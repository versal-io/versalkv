mod commands;
mod connection;
mod mutator;
mod protocol;
mod rng;
mod run;
mod stats;
mod worker;

pub use run::run_fuzzer;

const COMMAND_TIMEOUT_SEC: u64 = 15;
const RECONNECT_MAX_ATTEMPTS: u32 = 5;
const RECONNECT_BASE_DELAY_MS: u64 = 100;
pub(super) const DEFAULT_NUM_KEYS: usize = 5;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LogLevel {
    None,
    Error,
    Info,
    Debug,
}

impl LogLevel {
    fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "none" => LogLevel::None,
            "error" => LogLevel::Error,
            "info" => LogLevel::Info,
            "debug" => LogLevel::Debug,
            _ => LogLevel::Info,
        }
    }
}

fn log_msg(level: LogLevel, current: LogLevel, msg: &str) {
    if (level as usize) > (current as usize) {
        return;
    }
    let level_str = match level {
        LogLevel::None => "",
        LogLevel::Error => "ERROR",
        LogLevel::Info => "INFO ",
        LogLevel::Debug => "DEBUG",
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs() % 86400;
    let h = (secs / 3600) % 24;
    let m = (secs / 60) % 60;
    let s = secs % 60;
    eprintln!("[{:02}:{:02}:{:02}] [{}] {}", h, m, s, level_str, msg);
}
