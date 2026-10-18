use log::LevelFilter;
use std::cell::RefCell;
use std::fs::File;
use std::io::Write;
use std::panic::PanicHookInfo;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU8, Ordering};
use std::sync::Once;
use std::sync::{Mutex, OnceLock};

static INIT: Once = Once::new();
static PANIC_HOOK_INIT: Once = Once::new();
static DEBUG_CONTEXT: OnceLock<Mutex<String>> = OnceLock::new();
static LOG_TIMESTAMP_FORMAT: AtomicU8 = AtomicU8::new(LogTimestampFormat::Legacy as u8);
static LOG_FORMAT: AtomicU8 = AtomicU8::new(LogFormat::Legacy as u8);
static USE_EXIT_ON_PANIC: AtomicBool = AtomicBool::new(false);
static CRASH_LOG_ENABLED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(true);
static SYSLOG_ENABLED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
static SYSLOG_IDENT: OnceLock<std::ffi::CString> = OnceLock::new();

thread_local! {
    static ACTIVE_COMMAND: RefCell<Option<CommandDiagnostic>> = const { RefCell::new(None) };
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandDiagnostic {
    pub client_id: u64,
    pub command_seq: u64,
    pub peer: String,
    pub db: u32,
    pub command: String,
    pub argument_count: usize,
    pub request_bytes: usize,
}

pub struct ActiveCommandGuard {
    previous: Option<CommandDiagnostic>,
}

impl Drop for ActiveCommandGuard {
    fn drop(&mut self) {
        let previous = self.previous.take();
        ACTIVE_COMMAND.with(|active| *active.borrow_mut() = previous);
    }
}

pub fn enter_active_command(command: CommandDiagnostic) -> ActiveCommandGuard {
    let previous = ACTIVE_COMMAND.with(|active| active.borrow_mut().replace(command));
    ActiveCommandGuard { previous }
}

fn panic_payload(info: &PanicHookInfo<'_>) -> String {
    if let Some(message) = info.payload().downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = info.payload().downcast_ref::<String>() {
        message.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

fn format_panic_report(info: &PanicHookInfo<'_>, command: Option<&CommandDiagnostic>) -> String {
    let location = info
        .location()
        .map(|location| {
            format!(
                "{}:{}:{}",
                location.file(),
                location.line(),
                location.column()
            )
        })
        .unwrap_or_else(|| "unknown".to_string());
    let mut report = format!("\npanic={}\nlocation={}", panic_payload(info), location);
    if let Some(command) = command {
        report.push_str(&format!(
            "\nclient_id={}\ncommand_seq={}\npeer={}\ndb={}\ncommand={}\nargument_count={}\nrequest_bytes={}",
            command.client_id,
            command.command_seq,
            command.peer,
            command.db,
            command.command,
            command.argument_count,
            command.request_bytes
        ));
    } else {
        report.push_str("\nactive_command=none");
    }
    if let Some(context) = DEBUG_CONTEXT
        .get_or_init(|| Mutex::new(String::new()))
        .lock()
        .ok()
        .filter(|context| !context.is_empty())
    {
        report.push_str(&format!("\ndebug_context={context}"));
    }
    report
}

pub fn set_debug_context(context: String) {
    let mut slot = DEBUG_CONTEXT
        .get_or_init(|| Mutex::new(String::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    *slot = context;
}

pub fn set_crash_log_enabled(enabled: bool) {
    CRASH_LOG_ENABLED.store(enabled, Ordering::Relaxed);
}

/// Select Valkey's panic terminal policy. `true` favors a plain exit for
/// Valgrind/ASAN runs; the default retains SIGABRT so debuggers and core-dump
/// tooling observe a crash rather than an ordinary process failure.
pub fn set_use_exit_on_panic(enabled: bool) {
    USE_EXIT_ON_PANIC.store(enabled, Ordering::Relaxed);
}

pub fn configure_syslog(enabled: bool, ident: &str, facility: &str) -> Result<(), String> {
    #[cfg(unix)]
    {
        let ident = std::ffi::CString::new(ident)
            .map_err(|_| "syslog-ident cannot contain NUL".to_string())?;
        let facility = match facility.to_ascii_lowercase().as_str() {
            "user" => libc::LOG_USER,
            "local0" => libc::LOG_LOCAL0,
            "local1" => libc::LOG_LOCAL1,
            "local2" => libc::LOG_LOCAL2,
            "local3" => libc::LOG_LOCAL3,
            "local4" => libc::LOG_LOCAL4,
            "local5" => libc::LOG_LOCAL5,
            "local6" => libc::LOG_LOCAL6,
            "local7" => libc::LOG_LOCAL7,
            _ => return Err("invalid syslog-facility".to_string()),
        };
        if !enabled {
            SYSLOG_ENABLED.store(false, Ordering::Relaxed);
            return Ok(());
        }
        let ident = SYSLOG_IDENT.get_or_init(|| ident);
        // SAFETY: ident is retained in OnceLock for the process lifetime and
        // facility is one of libc's accepted LOG_* constants.
        unsafe {
            libc::openlog(
                ident.as_ptr(),
                libc::LOG_PID | libc::LOG_NDELAY | libc::LOG_NOWAIT,
                facility,
            )
        };
        SYSLOG_ENABLED.store(true, Ordering::Relaxed);
        Ok(())
    }
    #[cfg(not(unix))]
    {
        let _ = (ident, facility);
        if enabled {
            Err("syslog is not supported on this platform".to_string())
        } else {
            SYSLOG_ENABLED.store(false, Ordering::Relaxed);
            Ok(())
        }
    }
}

fn emit_syslog(level: log::Level, message: &str) {
    if !SYSLOG_ENABLED.load(Ordering::Relaxed) {
        return;
    }
    #[cfg(unix)]
    {
        let priority = match level {
            log::Level::Error => libc::LOG_ERR,
            log::Level::Warn => libc::LOG_WARNING,
            log::Level::Info => libc::LOG_NOTICE,
            log::Level::Debug | log::Level::Trace => libc::LOG_DEBUG,
        };
        let message = message.replace('\0', "\\0");
        let Ok(message) = std::ffi::CString::new(message) else {
            return;
        };
        const FORMAT: &[u8] = b"%s\0";
        // SAFETY: FORMAT and message are NUL-terminated, and the vararg
        // matches the sole %s conversion in FORMAT.
        unsafe { libc::syslog(priority, FORMAT.as_ptr().cast(), message.as_ptr()) };
    }
}

pub fn install_panic_hook() {
    PANIC_HOOK_INIT.call_once(|| {
        let previous = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if CRASH_LOG_ENABLED.load(Ordering::Relaxed) {
                let report = ACTIVE_COMMAND
                    .with(|active| format_panic_report(info, active.borrow().as_ref()));
                // C debug.c emits logStackTrace before bugReportEnd selects
                // exit or abort. A panic hook is not a signal handler, so it can
                // capture real frames even when RUST_BACKTRACE is disabled.
                let backtrace = std::backtrace::Backtrace::force_capture();
                log::error!(
                    "{report}\n------ STACK TRACE ------\n{backtrace}\n------ STACK TRACE DONE ------"
                );
                log::logger().flush();
            }
            previous(info);
            if USE_EXIT_ON_PANIC.load(Ordering::Relaxed) {
                // C Valkey's use-exit-on-panic avoids an abort/core for
                // Valgrind-style runs. Rust process::exit likewise skips
                // Rust destructors while retaining the requested status.
                std::process::exit(1);
            }
            // As in C _serverAssert/_serverPanic, restore the terminal signal
            // disposition after reporting so abort cannot print a second trace.
            super::signal::abort_after_panic_report();
        }));
    });
}

/// Main-process pid recorded at logger init. RDB/AOF `fork()` children inherit
/// the parent's address space and role, but their logs must be tagged `C`
/// (mirroring C Valkey's `pid != server.pid` check in `serverLogRaw`).
static MAIN_PID: AtomicI32 = AtomicI32::new(0);
/// Legacy-log role char for the main process: `M` primary, `S` replica, `X` sentinel.
static LOG_ROLE_CHAR: AtomicU8 = AtomicU8::new(b'M');

const MONTH_ABBREV: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

pub fn set_log_role_primary() {
    LOG_ROLE_CHAR.store(b'M', Ordering::Relaxed);
}

pub fn set_log_role_replica() {
    LOG_ROLE_CHAR.store(b'S', Ordering::Relaxed);
}

fn current_role_char() -> char {
    let main_pid = MAIN_PID.load(Ordering::Relaxed);
    if main_pid != 0 && std::process::id() as i32 != main_pid {
        return 'C';
    }
    LOG_ROLE_CHAR.load(Ordering::Relaxed) as char
}

fn current_role_name() -> &'static str {
    match current_role_char() {
        'S' => "replica",
        'X' => "sentinel",
        'C' => "RDB/AOF",
        _ => "primary",
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LogTimestampFormat {
    Legacy = 0,
    Iso8601 = 1,
    Milliseconds = 2,
}

impl LogTimestampFormat {
    pub fn parse(value: &str) -> Self {
        match value.to_ascii_lowercase().as_str() {
            "iso8601" => Self::Iso8601,
            "milliseconds" => Self::Milliseconds,
            _ => Self::Legacy,
        }
    }
}

pub fn set_log_timestamp_format(format: LogTimestampFormat) {
    LOG_TIMESTAMP_FORMAT.store(format as u8, Ordering::Relaxed);
}

fn current_timestamp() -> String {
    // SAFETY: localtime_r writes into a stack-owned tm and gettimeofday into a
    // stack-owned timeval. Both functions are reentrant for distinct buffers.
    let (tm, sec, millis) = unsafe {
        let mut tv: libc::timeval = std::mem::zeroed();
        libc::gettimeofday(&mut tv, std::ptr::null_mut());
        let mut tm: libc::tm = std::mem::zeroed();
        libc::localtime_r(&tv.tv_sec, &mut tm);
        (tm, tv.tv_sec, (tv.tv_usec / 1000) as u32)
    };
    let month = MONTH_ABBREV
        .get(tm.tm_mon.clamp(0, 11) as usize)
        .copied()
        .unwrap_or("Jan");
    match LOG_TIMESTAMP_FORMAT.load(Ordering::Relaxed) {
        value if value == LogTimestampFormat::Iso8601 as u8 => {
            let offset_seconds = tm.tm_gmtoff;
            let sign = if offset_seconds < 0 { '-' } else { '+' };
            let offset_minutes = offset_seconds.unsigned_abs() / 60;
            format!(
                "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}.{:03}{}{:02}{:02}",
                tm.tm_year + 1900,
                tm.tm_mon + 1,
                tm.tm_mday,
                tm.tm_hour,
                tm.tm_min,
                tm.tm_sec,
                millis,
                sign,
                offset_minutes / 60,
                offset_minutes % 60
            )
        }
        value if value == LogTimestampFormat::Milliseconds as u8 => {
            format!("{}", sec * 1000 + i64::from(millis))
        }
        _ => format!(
            "{:02} {} {} {:02}:{:02}:{:02}.{:03}",
            tm.tm_mday,
            month,
            tm.tm_year + 1900,
            tm.tm_hour,
            tm.tm_min,
            tm.tm_sec,
            millis
        ),
    }
}

fn json_level(level: log::Level) -> &'static str {
    match level {
        log::Level::Error | log::Level::Warn => "warning",
        log::Level::Info => "notice",
        log::Level::Debug | log::Level::Trace => "debug",
    }
}

fn format_json_record(level: log::Level, message: &str) -> String {
    format!(
        "{{\"pid\":{},\"role\":\"{}\",\"timestamp\":\"{}\",\"level\":\"{}\",\"message\":\"{}\"}}",
        std::process::id(),
        current_role_name(),
        current_timestamp(),
        json_level(level),
        escape_json_string(message)
    )
}

fn format_logfmt_record(level: log::Level, message: &str) -> String {
    // Valkey's logfmt writer keeps the quoted message parseable by replacing
    // only the characters that could terminate its value. Keep backslashes
    // unchanged to match C's filterInvalidLogfmtChar exactly.
    let message = message
        .chars()
        .map(|ch| match ch {
            '"' => '\'',
            '\n' | '\r' => ' ',
            _ => ch,
        })
        .collect::<String>();
    format!(
        "pid={} role={} timestamp=\"{}\" level={} message=\"{}\"",
        std::process::id(),
        current_role_name(),
        current_timestamp(),
        json_level(level),
        message
    )
}

/// Legacy C-Valkey prefix: `{pid}:{role} {dd} {Mon} {yyyy} {HH:MM:SS}.{mmm} {level}`.
/// The Tcl harness parses replication/crash logs with the regexp
/// `^[0-9]+:[A-Z] [0-9]+ [A-z]+ [0-9]+ ([0-9:.]+) .*`, so legacy lines must
/// keep this exact shape (aligned with `serverLogRaw` in server.c).
fn format_legacy_prefix(level: log::Level) -> String {
    let level_char = match level {
        log::Level::Error | log::Level::Warn => '#',
        log::Level::Info => '*',
        log::Level::Debug | log::Level::Trace => '.',
    };
    format!(
        "{}:{} {} {}",
        std::process::id(),
        current_role_char(),
        current_timestamp(),
        level_char
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Debug,
    Verbose,
    Notice,
    Warning,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LogFormat {
    Legacy = 0,
    Logfmt = 1,
    Json = 2,
}

impl LogFormat {
    pub fn parse(value: &str) -> Self {
        match value.to_ascii_lowercase().as_str() {
            "logfmt" => Self::Logfmt,
            "json" => Self::Json,
            _ => Self::Legacy,
        }
    }
}

pub fn set_log_format(format: LogFormat) {
    LOG_FORMAT.store(format as u8, Ordering::Relaxed);
}

pub fn init_logger(level: LogLevel, logfile: Option<File>) {
    init_logger_with_format(
        level,
        logfile,
        LogFormat::Legacy,
        LogTimestampFormat::Legacy,
    );
}

pub fn init_logger_with_format(
    level: LogLevel,
    logfile: Option<File>,
    format: LogFormat,
    timestamp_format: LogTimestampFormat,
) {
    set_log_timestamp_format(timestamp_format);
    set_log_format(format);
    INIT.call_once(|| {
        MAIN_PID.store(std::process::id() as i32, Ordering::Relaxed);
        let filter = match level {
            LogLevel::Debug => LevelFilter::Debug,
            LogLevel::Verbose => LevelFilter::Debug,
            LogLevel::Notice => LevelFilter::Info,
            LogLevel::Warning => LevelFilter::Warn,
        };
        let mut builder = env_logger::Builder::new();
        let target = match logfile {
            Some(file) => env_logger::fmt::Target::Pipe(Box::new(file)),
            None => env_logger::fmt::Target::Stdout,
        };
        builder
            .target(target)
            .filter_level(filter)
            .format(move |buf, record| {
                emit_syslog(record.level(), &record.args().to_string());
                let message = record.args().to_string();
                match LOG_FORMAT.load(Ordering::Relaxed) {
                    value if value == LogFormat::Logfmt as u8 => {
                        writeln!(buf, "{}", format_logfmt_record(record.level(), &message))
                    }
                    value if value == LogFormat::Json as u8 => {
                        writeln!(buf, "{}", format_json_record(record.level(), &message))
                    }
                    _ => writeln!(buf, "{} {}", format_legacy_prefix(record.level()), message),
                }
            })
            .init();
    });
}

pub fn escape_json_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{000c}' => out.push_str("\\f"),
            '\u{0008}' => out.push_str("\\b"),
            c if c.is_control() => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

#[macro_export]
macro_rules! server_log {
    (debug, $($arg:tt)*) => { log::debug!($($arg)*) };
    (verbose, $($arg:tt)*) => { log::debug!($($arg)*) };
    (notice, $($arg:tt)*) => { log::info!($($arg)*) };
    (warning, $($arg:tt)*) => { log::warn!($($arg)*) };
}

#[cfg(test)]
mod tests {
    use super::{
        enter_active_command, escape_json_string, format_json_record, format_logfmt_record,
        CommandDiagnostic, ACTIVE_COMMAND,
    };

    #[test]
    fn escapes_json_control_characters_and_delimiters() {
        assert_eq!(
            escape_json_string("quotes\" \\ tab\t cr\r ff\u{000c} bs\u{0008}"),
            "quotes\\\" \\\\ tab\\t cr\\r ff\\f bs\\b"
        );
    }

    #[test]
    fn active_command_guard_restores_previous_context() {
        let outer = CommandDiagnostic {
            client_id: 7,
            command_seq: 3,
            peer: "127.0.0.1:6379".to_string(),
            db: 2,
            command: "get".to_string(),
            argument_count: 1,
            request_bytes: 22,
        };
        let guard = enter_active_command(outer.clone());
        ACTIVE_COMMAND.with(|active| assert_eq!(active.borrow().as_ref(), Some(&outer)));
        drop(guard);
        ACTIVE_COMMAND.with(|active| assert!(active.borrow().is_none()));
    }

    #[test]
    fn json_record_has_valkey_structured_fields() {
        let record = format_json_record(log::Level::Warn, "bad \"value\"");
        assert!(record.contains(&format!("\"pid\":{}", std::process::id())));
        assert!(
            record.contains("\"role\":\"primary\"")
                || record.contains("\"role\":\"replica\"")
                || record.contains("\"role\":\"RDB/AOF\"")
                || record.contains("\"role\":\"sentinel\"")
        );
        assert!(record.contains("\"timestamp\":\""));
        assert!(record.contains("\"level\":\"warning\""));
        assert!(record.contains("\"message\":\"bad \\\"value\\\"\""));
    }

    #[test]
    fn logfmt_record_matches_valkey_field_order_and_sanitizes_quotes() {
        let record = format_logfmt_record(log::Level::Warn, "bad \"value\"\nnext\rline");

        assert!(record.starts_with(&format!("pid={} role=", std::process::id())));
        assert!(record.contains(" level=warning "));
        assert!(record.ends_with("message=\"bad 'value' next line\""));
    }
}
