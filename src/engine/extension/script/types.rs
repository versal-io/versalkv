use crate::foundation::io::protocol::Value;
use bytes::Bytes;
use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;
use std::time::Instant;

// TODO(dead_code): sink-specific script producers land after the direct Both-sink migration.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PropagationTargets {
    None,
    Replication,
    Aof,
    Both,
}

impl PropagationTargets {
    pub fn includes_replication(self) -> bool {
        matches!(self, Self::Replication | Self::Both)
    }

    pub fn includes_aof(self) -> bool {
        matches!(self, Self::Aof | Self::Both)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PropagationBatch {
    pub db_id: u32,
    pub targets: PropagationTargets,
    pub bytes: Vec<u8>,
    command_count: usize,
}

impl PropagationBatch {
    pub fn new(db_id: u32, targets: PropagationTargets, bytes: Vec<u8>) -> Self {
        Self {
            db_id,
            targets,
            bytes,
            command_count: 1,
        }
    }

    pub fn both(db_id: u32, bytes: Vec<u8>) -> Self {
        Self::new(db_id, PropagationTargets::Both, bytes)
    }

    pub fn append_frame(&mut self, frame: Vec<u8>) {
        self.bytes.extend_from_slice(&frame);
        self.command_count = self.command_count.saturating_add(1);
    }

    pub(crate) fn from_command(
        db_id: u32,
        targets: PropagationTargets,
        args: &crate::engine::command::CommandArgs,
    ) -> Self {
        let mut batch = Self {
            db_id,
            targets,
            bytes: Vec::new(),
            command_count: 0,
        };
        batch.append_command(args);
        batch
    }

    pub(crate) fn append_command(&mut self, args: &crate::engine::command::CommandArgs) {
        self.bytes.push(b'*');
        append_usize(&mut self.bytes, args.args.len() + 1);
        self.bytes.extend_from_slice(b"\r\n");
        append_bulk(&mut self.bytes, &args.name);
        for arg in &args.args {
            append_bulk(&mut self.bytes, arg);
        }
        self.command_count = self.command_count.saturating_add(1);
    }

    pub fn command_count(&self) -> usize {
        self.command_count
    }
}

pub type PropagationOp = PropagationBatch;

fn append_bulk(output: &mut Vec<u8>, value: &[u8]) {
    output.push(b'$');
    append_usize(output, value.len());
    output.extend_from_slice(b"\r\n");
    output.extend_from_slice(value);
    output.extend_from_slice(b"\r\n");
}

fn append_usize(output: &mut Vec<u8>, mut value: usize) {
    let mut digits = [0_u8; 20];
    let mut start = digits.len();
    loop {
        start -= 1;
        digits[start] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            break;
        }
    }
    output.extend_from_slice(&digits[start..]);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KillSource {
    None,
    ScriptKill,
    FunctionKill,
    /// Accepted SHUTDOWN request interrupting a timed-out script.
    Shutdown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScriptDebugMode {
    Forked,
    Synchronous,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScriptKind {
    Eval,
    Function,
}

#[derive(Clone, Debug)]
struct ScriptControlState {
    timed_out: bool,
    killed: bool,
    write_dirty: bool,
    kill_source: KillSource,
    func_name: String,
    caller_args: Vec<Vec<u8>>,
    start_time: Option<Instant>,
}

#[derive(Clone, Debug)]
pub struct ScriptControlHandle {
    kind: ScriptKind,
    state: Rc<RefCell<ScriptControlState>>,
}

impl ScriptControlHandle {
    pub fn new(kind: ScriptKind) -> Self {
        Self {
            kind,
            state: Rc::new(RefCell::new(ScriptControlState {
                timed_out: false,
                killed: false,
                write_dirty: false,
                kill_source: KillSource::None,
                func_name: String::new(),
                caller_args: Vec::new(),
                start_time: None,
            })),
        }
    }

    pub fn kind(&self) -> ScriptKind {
        self.kind
    }

    pub fn is_timed_out(&self) -> bool {
        self.state.borrow().timed_out
    }

    pub fn mark_timed_out(&self) {
        self.state.borrow_mut().timed_out = true;
    }

    pub fn is_killed(&self) -> bool {
        self.state.borrow().killed
    }

    pub fn kill_source(&self) -> KillSource {
        self.state.borrow().kill_source
    }

    pub fn is_write_dirty(&self) -> bool {
        self.state.borrow().write_dirty
    }

    pub fn mark_write_dirty(&self) {
        self.state.borrow_mut().write_dirty = true;
    }

    pub fn request_kill(&self, source: KillSource) -> bool {
        let mut state = self.state.borrow_mut();
        if state.write_dirty && source != KillSource::Shutdown {
            return false;
        }
        state.killed = true;
        state.kill_source = source;
        true
    }

    pub fn set_invocation_info(
        &self,
        func_name: String,
        caller_args: Vec<Vec<u8>>,
        start_time: Option<Instant>,
    ) {
        let mut state = self.state.borrow_mut();
        state.func_name = func_name;
        state.caller_args = caller_args;
        state.start_time = start_time;
    }

    pub fn function_info(&self) -> Option<(String, Vec<Vec<u8>>, Option<Instant>)> {
        if self.kind != ScriptKind::Function || self.state.borrow().func_name.is_empty() {
            return None;
        }
        let state = self.state.borrow();
        Some((
            state.func_name.clone(),
            state.caller_args.clone(),
            state.start_time,
        ))
    }
}

#[derive(Clone)]
pub struct CompiledFunction {
    pub engine: &'static str,
    pub name: String,
    pub code: String,
    pub description: String,
    pub flags: ScriptFlags,
}

pub trait ScriptEngine: Send + Sync {
    fn name(&self) -> &'static str;

    fn eval(
        &self,
        code: &str,
        ctx: &mut ScriptRunCtx,
        dispatcher: &mut crate::engine::host::HostDispatcher<'_>,
    ) -> Result<Value, ScriptError>;

    fn compile(
        &self,
        code: &str,
        flags: ScriptFlags,
    ) -> Result<Vec<CompiledFunction>, ScriptError> {
        Ok(vec![CompiledFunction {
            engine: self.name(),
            name: String::new(),
            code: code.to_string(),
            description: String::new(),
            flags,
        }])
    }

    fn commit_compile(&self, _code: &str, _flags: ScriptFlags) -> Result<(), ScriptError> {
        Ok(())
    }

    fn begin_function_restore_transaction(&self) -> Result<(), ScriptError> {
        Ok(())
    }

    fn commit_function_restore_transaction(&self) -> Result<(), ScriptError> {
        Ok(())
    }

    fn rollback_function_restore_transaction(&self) -> Result<(), ScriptError> {
        Ok(())
    }

    fn clear_functions(&self) {}

    /// Release engine-owned callbacks and metadata for a deleted library.
    fn remove_functions(&self, _functions: &[CompiledFunction]) -> Result<(), ScriptError> {
        Ok(())
    }

    fn call(
        &self,
        func: &CompiledFunction,
        ctx: &mut ScriptRunCtx,
        keys: &[bytes::Bytes],
        args: &[bytes::Bytes],
        dispatcher: &mut crate::engine::host::HostDispatcher<'_>,
    ) -> Result<Value, ScriptError>;
}

/// Observation gates for nested-command slowlog, commandlog and MONITOR
/// delivery. Captured once per script invocation because the event loop never
/// services configuration or MONITOR changes while a script runs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ScriptLogGate {
    pub monitor_present: bool,
    pub slowlog_enabled: bool,
    pub slowlog_threshold_us: i64,
    pub commandlog_enabled: bool,
    pub commandlog_threshold_us: i64,
}

impl ScriptLogGate {
    pub fn logging_active(self) -> bool {
        self.monitor_present || self.slowlog_enabled || self.commandlog_enabled
    }

    pub fn should_record_slow(self, duration_us: u64) -> bool {
        let duration = duration_us as i64;
        (self.slowlog_enabled
            && (self.slowlog_threshold_us == 0 || duration >= self.slowlog_threshold_us))
            || (self.commandlog_enabled
                && (self.commandlog_threshold_us == 0 || duration >= self.commandlog_threshold_us))
    }
}

/// Server-side facts that stay constant for one script invocation. Nested
/// `redis.call` commands read this snapshot instead of issuing repeated trait
/// calls and taking the global ACL lock on every command.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ScriptEnvSnapshot {
    pub(crate) privileged_user: bool,
    pub(crate) readonly_replica: bool,
    pub(crate) stale_replica: bool,
    pub(crate) persistence_write_error: bool,
    pub(crate) maxmemory_limit: bool,
    pub(crate) log_gate: ScriptLogGate,
}

impl ScriptEnvSnapshot {
    /// Fail-closed fallback used only if a reader runs before
    /// `ensure_env_snapshot`. Admission facts default to the rejecting side,
    /// so a missing snapshot can never authorize a write that the live state
    /// would deny. The production path always captures the snapshot as the
    /// first step of nested-command execution.
    fn fail_closed() -> Self {
        Self {
            privileged_user: false,
            readonly_replica: true,
            stale_replica: true,
            persistence_write_error: true,
            maxmemory_limit: true,
            log_gate: ScriptLogGate {
                slowlog_enabled: true,
                slowlog_threshold_us: 0,
                commandlog_enabled: true,
                commandlog_threshold_us: 0,
                ..Default::default()
            },
        }
    }
}

pub struct ScriptRunCtx {
    pub flags: ScriptFlags,
    pub timeout_ms: u64,
    pub start_time: Option<Instant>,
    /// Invocation control shared with commands serviced while the script is busy.
    pub control: ScriptControlHandle,
    pub slow_log_emitted: bool,
    pub propagate_dirty: bool,
    /// Whether this invocation needs to retain propagation frames for AOF,
    /// replicas, or slot-migration export consumers.
    pub needs_propagation: bool,
    pub deferred_error_replies: Vec<String>,
    /// Reused storage for one nested redis.call argv vector.
    pub(crate) argv_scratch: Vec<Bytes>,
    /// Small bounded cache for repeated Lua string arguments such as command
    /// names and KEYS entries. Values remain owned by Bytes and never borrow
    /// from the Lua VM.
    pub(crate) lua_argument_cache: Vec<Bytes>,
    /// DB-aware propagation operations produced during script execution.
    pub propagation_ops: Vec<PropagationBatch>,
    /// Dirty keys already reported by nested commands in this invocation.
    pub dirty_keys_seen: HashSet<(u32, Bytes)>,
    /// Current sink selection for nested script effects.
    pub propagation_targets: PropagationTargets,
    /// Whether the script was allowed to write when execution started.
    pub has_enough_replicas_to_write: bool,
    /// Whether this script may use Lua's insecure compatibility APIs.
    pub lua_enable_insecure_api: bool,
    pub skip_propagate: bool,
    /// Script-internal RESP version (set via redis.setresp). Defaults to 2.
    pub resp: u8,
    pub func_name: String,
    pub caller_args: Vec<Vec<u8>>,
    pub script_slot: Option<u16>,
    /// Per-invocation server facts captured on the first nested command.
    pub(crate) env: Option<ScriptEnvSnapshot>,
}

impl ScriptRunCtx {
    /// Returns the captured invocation environment. Production callers always
    /// run `ensure_env_snapshot` before nested-command admission; the
    /// fail-closed fallback only protects direct unit-test helpers.
    pub(crate) fn env_snapshot(&self) -> ScriptEnvSnapshot {
        self.env.unwrap_or_else(ScriptEnvSnapshot::fail_closed)
    }
}

impl ScriptRunCtx {
    pub(crate) fn intern_lua_argument(&mut self, value: &[u8]) -> Bytes {
        const CACHE_ENTRIES: usize = 16;
        const MAX_CACHED_LEN: usize = 1024;

        if value.len() <= MAX_CACHED_LEN {
            if let Some(cached) = self
                .lua_argument_cache
                .iter()
                .find(|cached| cached.as_ref() == value)
            {
                return cached.clone();
            }
            if self.lua_argument_cache.len() < CACHE_ENTRIES {
                let cached = Bytes::copy_from_slice(value);
                self.lua_argument_cache.push(cached.clone());
                return cached;
            }
        }
        Bytes::copy_from_slice(value)
    }

    pub(crate) fn append_propagation_frame(
        &mut self,
        db_id: u32,
        targets: PropagationTargets,
        frame: Vec<u8>,
    ) {
        if let Some(batch) = self
            .propagation_ops
            .last_mut()
            .filter(|batch| batch.db_id == db_id && batch.targets == targets)
        {
            batch.append_frame(frame);
        } else {
            self.propagation_ops
                .push(PropagationBatch::new(db_id, targets, frame));
        }
    }

    pub(crate) fn append_propagation_command(
        &mut self,
        db_id: u32,
        targets: PropagationTargets,
        args: &crate::engine::command::CommandArgs,
    ) {
        if let Some(batch) = self
            .propagation_ops
            .last_mut()
            .filter(|batch| batch.db_id == db_id && batch.targets == targets)
        {
            batch.append_command(args);
        } else {
            self.propagation_ops
                .push(PropagationBatch::from_command(db_id, targets, args));
        }
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;

pub type ScriptFlags = u64;

pub const SCRIPT_WRITE_DIRTY: ScriptFlags = 1 << 0;
pub const SCRIPT_READ_ONLY: ScriptFlags = 1 << 5;
pub const SCRIPT_ALLOW_OOM: ScriptFlags = 1 << 6;
pub const SCRIPT_EVAL_MODE: ScriptFlags = 1 << 7;
pub const SCRIPT_ALLOW_CROSS_SLOT: ScriptFlags = 1 << 8;
pub const SCRIPT_NO_WRITES: ScriptFlags = 1 << 9;
pub const SCRIPT_NO_CLUSTER: ScriptFlags = 1 << 10;
pub const SCRIPT_ALLOW_STALE: ScriptFlags = 1 << 11;

#[derive(Debug)]
pub enum ScriptError {
    Compile(String),
    Runtime(String),
    KillError(&'static str),
    Restore(String),
}

/// Check if a message already has a RESP error code prefix (e.g. "ERR ", "WRONGTYPE ", "MOVED ").
fn has_error_code(msg: &str) -> bool {
    if let Some(space) = msg.find(' ') {
        let code = &msg[..space];
        code.len() >= 2
            && code.len() <= 16
            && code
                .chars()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
    } else {
        false
    }
}

impl ScriptError {
    pub fn into_value(self) -> Value {
        match self {
            ScriptError::Compile(msg) => {
                Value::error(format!("ERR Error compiling script: {}", msg))
            }
            ScriptError::Runtime(msg) => {
                // mlua wraps errors with "callback error: " or "runtime error: " prefix,
                // and appends "\nstack traceback:..." suffix. Strip these.
                // RESP simple errors don't allow embedded newlines (breaks line protocol).
                let mut inner = msg.as_str();
                if let Some(rest) = inner.strip_prefix("callback error: ") {
                    inner = rest;
                }
                if let Some(rest) = inner.strip_prefix("runtime error: ") {
                    inner = rest;
                }
                if let Some(idx) = inner.find('\n') {
                    inner = &inner[..idx];
                }
                if has_error_code(inner) {
                    Value::error(inner.to_string())
                } else {
                    Value::error(format!("ERR Error running script: {}", inner))
                }
            }
            ScriptError::KillError(msg) => Value::error(format!("ERR {}", msg)),
            ScriptError::Restore(msg) => {
                Value::error(format!("ERR Error restoring script: {}", msg))
            }
        }
    }
}

impl std::fmt::Display for ScriptError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScriptError::Compile(msg) => write!(f, "compile error: {}", msg),
            ScriptError::Runtime(msg) => write!(f, "runtime error: {}", msg),
            ScriptError::KillError(_) => write!(f, "script killed"),
            ScriptError::Restore(msg) => write!(f, "restore error: {}", msg),
        }
    }
}
