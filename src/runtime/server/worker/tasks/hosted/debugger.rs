//! Forked debugger sessions own their client socket and private dataset.

use std::io;
use std::time::Duration;

use bytes::Bytes;

use crate::engine::command::context::CommandConn;
use crate::engine::command::{CommandArgs, SessionId};
use crate::engine::host::{HostError, HostRequest, HostYield};
use crate::foundation::io::protocol::{
    parser::{ParseError, Parser},
    serializer::serialize_to_vec,
    Value,
};
use crate::foundation::io::sys::{os, process};
use crate::framework::capabilities::connection::ConnectionState;
use crate::framework::capabilities::io::Token;
use crate::framework::capabilities::process::{close_fd, fork_process, ForkBranch};
use crate::framework::host_tasks::LuaTask;
use crate::runtime::server::worker::reactor::sources::ChildOwner;
use crate::runtime::server::worker::ServerWorkerState;

use super::HostTaskScope;

const MAX_DEBUGGER_REPLY_DEPTH: usize = 16;
const MAX_DEBUGGER_REPLY_DEPTH_MESSAGE: &[u8] = b"<max reply depth reached>";
const MAX_DEBUGGER_LOG_FORMAT_BYTES: usize = 1024 * 1024;
const MAX_DEBUGGER_COMMAND_BYTES: usize = 1024 * 1024;
const MAX_DEBUGGER_SOURCE_LINES: usize = 4096;
const MAX_DEBUGGER_SOURCE_TOTAL_BYTES: usize = 1024 * 1024;
const DEBUGGER_SOURCE_TRUNCATION_MARKER: &[u8] = b"<source output truncated>";
const DEFAULT_DEBUGGER_LOG_MAX_LEN: usize = 256;
const MIN_DEBUGGER_LOG_MAX_LEN: usize = 60;
const DEBUGGER_LOG_TRIM_SUFFIX: &[u8] = b" ...";
const DEBUGGER_LOG_TRIM_HINT: &[u8] =
    b"<hint> The above reply was trimmed. Use 'maxlen 0' to disable trimming.";
const DEBUGGER_LOG_HARD_LIMIT_HINT: &[u8] = b"<hint> Reply reached debugger hard safety limit.";

struct DebuggerSessionState {
    max_len: usize,
    maxlen_hint_sent: bool,
    hard_limit_hint_sent: bool,
}

impl Default for DebuggerSessionState {
    fn default() -> Self {
        Self {
            max_len: DEFAULT_DEBUGGER_LOG_MAX_LEN,
            maxlen_hint_sent: false,
            hard_limit_hint_sent: false,
        }
    }
}

impl DebuggerSessionState {
    fn format_limit(&self) -> usize {
        if self.max_len == 0 || self.max_len >= MAX_DEBUGGER_LOG_FORMAT_BYTES {
            MAX_DEBUGGER_LOG_FORMAT_BYTES
        } else {
            self.max_len.saturating_add(1)
        }
    }

    fn set_max_len(&mut self, max_len: usize) {
        self.max_len = if max_len == 0 {
            0
        } else {
            max_len.max(MIN_DEBUGGER_LOG_MAX_LEN)
        };
        self.maxlen_hint_sent = true;
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DebuggerSessionMode {
    Forked,
    Synchronous,
}

impl ServerWorkerState {
    pub(super) fn run_forked_lua_debugger(&mut self, token: Token, task: LuaTask) -> Value {
        if !self.socket_owned_by_main_thread(token) {
            self.clear_lua_debugger_session(token);
            return Value::error("ERR debugger client socket is busy");
        }
        if self.transport.raw_fd(token).is_err() {
            self.clear_lua_debugger_session(token);
            return Value::error("ERR debugger requires a connected client");
        }
        match fork_process() {
            Err(error) => {
                self.clear_lua_debugger_session(token);
                Value::error(format!("ERR cannot fork debugger: {error}"))
            }
            Ok(ForkBranch::Parent(child)) => {
                self.register_child_process(ChildOwner::ScriptDebugger, child.into_pid());
                // Closing the parent's descriptor must not shutdown the child's socket.
                self.detach_debugger_connection(token);
                Value::Null
            }
            Ok(ForkBranch::Child) => {
                process::monitor_persistence_child_parent();
                let debugger_fd = match self.transport.raw_fd(token) {
                    Ok(fd) => fd,
                    Err(_) => process::exit_immediately(1),
                };
                if close_inherited_fds_except(debugger_fd).is_err() {
                    process::exit_immediately(1);
                }
                let session = self.connection_registry[token].session_id();
                let succeeded = self
                    .run_debugger_session(token, session, task, DebuggerSessionMode::Forked)
                    .is_ok();
                process::exit_immediately(if succeeded { 0 } else { 1 });
            }
        }
    }

    pub(super) fn run_synchronous_lua_debugger(&mut self, token: Token, task: LuaTask) -> Value {
        if !self.socket_owned_by_main_thread(token) {
            self.clear_lua_debugger_session(token);
            return Value::error("ERR debugger client socket is busy");
        }
        if self.transport.raw_fd(token).is_err() {
            self.clear_lua_debugger_session(token);
            return Value::error("ERR debugger requires a connected client");
        }
        let session = self.connection_registry[token].session_id();
        let previous_protected = self
            .connection_registry
            .get_mut(token)
            .filter(|connection| connection.session_id() == session)
            .map(|connection| std::mem::replace(&mut connection.protected, true));
        let result =
            self.run_debugger_session(token, session, task, DebuggerSessionMode::Synchronous);
        self.clear_lua_debugger_session(token);
        if let Some(connection) = self
            .connection_registry
            .get_mut(token)
            .filter(|connection| connection.session_id() == session)
        {
            connection.protected = previous_protected.unwrap_or(false);
            connection.state = ConnectionState::CloseAfterReply;
        }
        match result {
            Ok(Some(result)) => result,
            Ok(None) => Value::Null,
            Err(error) => Value::error(format!("ERR debugger I/O error: {error}")),
        }
    }

    fn clear_lua_debugger_session(&mut self, token: Token) {
        let Some(session) = self
            .connection_registry
            .get(token)
            .map(|connection| connection.session_id())
        else {
            return;
        };
        let script = self.ctx.script_mut();
        script.debug_modes.remove(&session);
        if script.debug_session == Some(session) {
            script.debug_session = None;
        }
    }

    fn run_debugger_session(
        &mut self,
        token: Token,
        session: SessionId,
        mut task: LuaTask,
        mode: DebuggerSessionMode,
    ) -> io::Result<Option<Value>> {
        let mut parser = std::mem::take(&mut self.connection_registry[token].parser);
        let pending = self.connection_registry[token]
            .take_threaded_write_snapshot()
            .into_frames();
        for frame in pending {
            self.write_debugger_bytes(token, &frame)?;
        }
        let mut source = task.debug_source().to_owned();
        let mut session_state = DebuggerSessionState::default();
        if let Err(error) = task.initialize_debugger() {
            if let Err(io_error) =
                self.write_debugger_reply(token, debugger_log_reply(format!("<error> {error}")))
            {
                let result = Value::error(format!("ERR debugger I/O error: {io_error}"));
                let _ = self.finish_interrupted_debugger_task(token, session, task, mode, result);
                return Err(io_error);
            }
            let result = task.finish(
                error.into_value(),
                &mut self.connection_registry[token],
                &mut self.ctx,
            );
            return self.finish_debugger_session(token, result, mode);
        }
        self.write_debugger_stopped(token, &source)?;
        let mut restart_requested = false;
        loop {
            process::exit_if_persistence_child_parent_changed();
            let parts = match parse_debugger_command(&mut parser) {
                Ok(Some(parts)) => parts,
                Ok(None) => {
                    let mut buffer = [0_u8; 4096];
                    match self.transport.read(token, &mut buffer) {
                        Ok(0) => {
                            return Ok(self.finish_interrupted_debugger_task(
                                token,
                                session,
                                task,
                                mode,
                                Value::Null,
                            ));
                        }
                        Ok(count) => parser.feed(&buffer[..count]),
                        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                            if let Err(error) = self.wait_debugger_socket(token, libc::POLLIN) {
                                let result =
                                    Value::error(format!("ERR debugger I/O error: {error}"));
                                let _ = self.finish_interrupted_debugger_task(
                                    token, session, task, mode, result,
                                );
                                return Err(error);
                            }
                        }
                        Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                        Err(error) => {
                            let result = Value::error(format!("ERR debugger I/O error: {error}"));
                            let _ = self.finish_interrupted_debugger_task(
                                token, session, task, mode, result,
                            );
                            return Err(error);
                        }
                    }
                    continue;
                }
                Err(error) => {
                    let message = match error {
                        ParseError::Incomplete => "Protocol error: incomplete request".to_string(),
                        ParseError::Protocol(message) => message,
                    };
                    if let Err(error) = self.write_debugger_reply(
                        token,
                        debugger_log_reply(format!("<error> {message}")),
                    ) {
                        let result = Value::error(format!("ERR debugger I/O error: {error}"));
                        let _ = self
                            .finish_interrupted_debugger_task(token, session, task, mode, result);
                        return Err(error);
                    }
                    let result = if mode == DebuggerSessionMode::Synchronous {
                        task.finish(
                            Value::Null,
                            &mut self.connection_registry[token],
                            &mut self.ctx,
                        )
                    } else {
                        Value::Null
                    };
                    return self.finish_debugger_session(token, result, mode);
                }
            };
            if debugger_command_exceeds_size_limit(&parts) {
                if let Err(error) = self.write_debugger_reply(
                    token,
                    debugger_log_reply("<error> debugger command exceeds 1 MiB"),
                ) {
                    let result = Value::error(format!("ERR debugger I/O error: {error}"));
                    let _ =
                        self.finish_interrupted_debugger_task(token, session, task, mode, result);
                    return Err(error);
                }
                let result = if mode == DebuggerSessionMode::Synchronous {
                    task.finish(
                        Value::Null,
                        &mut self.connection_registry[token],
                        &mut self.ctx,
                    )
                } else {
                    Value::Null
                };
                return self.finish_debugger_session(token, result, mode);
            }
            let Some(command) = parts.first() else {
                continue;
            };
            if mode == DebuggerSessionMode::Synchronous
                && command.eq_ignore_ascii_case(b"script")
                && parts
                    .get(1)
                    .is_some_and(|subcommand| subcommand.eq_ignore_ascii_case(b"debug"))
                && parts
                    .get(2)
                    .is_some_and(|setting| setting.eq_ignore_ascii_case(b"sync"))
            {
                let reply = match parts.as_slice() {
                    [_, _, _] => {
                        restart_requested = true;
                        Value::ok()
                    }
                    [_, _, _, engine] if engine.eq_ignore_ascii_case(b"lua") => {
                        restart_requested = true;
                        Value::ok()
                    }
                    [_, _, _, engine] => debugger_unknown_scripting_engine_reply(engine),
                    _ => debugger_wrong_arity_reply("script debug sync"),
                };
                if let Err(error) = self.write_debugger_reply(token, reply) {
                    let result = Value::error(format!("ERR debugger I/O error: {error}"));
                    let _ =
                        self.finish_interrupted_debugger_task(token, session, task, mode, result);
                    return Err(error);
                }
                continue;
            }
            if mode == DebuggerSessionMode::Synchronous
                && restart_requested
                && command.eq_ignore_ascii_case(b"eval")
            {
                let args = crate::engine::command::CommandArgs::from_parts(parts);
                let prepared = crate::framework::host_tasks::prepare_lua_task(
                    &mut self.connection_registry[token],
                    &args,
                    &mut self.ctx,
                );
                match prepared {
                    Ok(crate::framework::host_tasks::PreparedHostTask::Lua(mut new_task)) => {
                        restart_requested = false;
                        if let Err(error) = new_task.initialize_debugger() {
                            let message = format!("<error> {error}");
                            let _ = new_task.finish(
                                error.into_value(),
                                &mut self.connection_registry[token],
                                &mut self.ctx,
                            );
                            if let Err(io_error) =
                                self.write_debugger_reply(token, debugger_log_reply(message))
                            {
                                let result =
                                    Value::error(format!("ERR debugger I/O error: {io_error}"));
                                let _ = self.finish_interrupted_debugger_task(
                                    token, session, task, mode, result,
                                );
                                return Err(io_error);
                            }
                            continue;
                        }
                        let old_task = std::mem::replace(&mut task, new_task);
                        let _ = old_task.finish(
                            Value::Null,
                            &mut self.connection_registry[token],
                            &mut self.ctx,
                        );
                        task.rebase_debugger_db(self.connection_registry[token].selected_db());
                        source = task.debug_source().to_owned();
                        if let Err(error) = self.write_debugger_stopped(token, &source) {
                            let result = Value::error(format!("ERR debugger I/O error: {error}"));
                            let _ = self.finish_interrupted_debugger_task(
                                token, session, task, mode, result,
                            );
                            return Err(error);
                        }
                    }
                    Ok(_) => {
                        if let Err(error) = self.write_debugger_reply(
                            token,
                            Value::error("ERR unsupported debugger command"),
                        ) {
                            let result = Value::error(format!("ERR debugger I/O error: {error}"));
                            let _ = self.finish_interrupted_debugger_task(
                                token, session, task, mode, result,
                            );
                            return Err(error);
                        }
                    }
                    Err(error) => {
                        if let Err(io_error) = self.write_debugger_reply(token, error) {
                            let result =
                                Value::error(format!("ERR debugger I/O error: {io_error}"));
                            let _ = self.finish_interrupted_debugger_task(
                                token, session, task, mode, result,
                            );
                            return Err(io_error);
                        }
                    }
                }
                continue;
            }
            if command.eq_ignore_ascii_case(b"c") || command.eq_ignore_ascii_case(b"continue") {
                if parts.len() != 1 {
                    if let Err(error) =
                        self.write_debugger_reply(token, debugger_wrong_arity_reply("continue"))
                    {
                        let result = Value::error(format!("ERR debugger I/O error: {error}"));
                        let _ = self
                            .finish_interrupted_debugger_task(token, session, task, mode, result);
                        return Err(error);
                    }
                    continue;
                }
                task.restart_timer();
                // The debugger loop owns the connection until this execution
                // returns to either the fork child or the synchronous host path.
                let result = self.run_debugger_script(token, task);
                return self.finish_debugger_session(token, result, mode);
            }
            if command.eq_ignore_ascii_case(b"a") || command.eq_ignore_ascii_case(b"abort") {
                if parts.len() != 1 {
                    if let Err(error) =
                        self.write_debugger_reply(token, debugger_wrong_arity_reply("abort"))
                    {
                        let result = Value::error(format!("ERR debugger I/O error: {error}"));
                        let _ = self
                            .finish_interrupted_debugger_task(token, session, task, mode, result);
                        return Err(error);
                    }
                    continue;
                }
                let result = task.finish(
                    Value::error("ERR script aborted for user request"),
                    &mut self.connection_registry[token],
                    &mut self.ctx,
                );
                return self.finish_debugger_session(token, result, mode);
            }
            if is_debugger_maxlen_command(command) {
                let reply = debugger_maxlen_reply(&parts, &mut session_state);
                if let Err(error) = self.write_debugger_reply(token, reply) {
                    let result = Value::error(format!("ERR debugger I/O error: {error}"));
                    let _ =
                        self.finish_interrupted_debugger_task(token, session, task, mode, result);
                    return Err(error);
                }
                continue;
            }
            let mut trim_hints = Vec::new();
            let reply = if command.eq_ignore_ascii_case(b"eval")
                || command.eq_ignore_ascii_case(b"e")
            {
                let (log, was_truncated) = if parts.len() < 2 {
                    (
                        Bytes::from_static(b"<error> eval requires an expression"),
                        false,
                    )
                } else {
                    let fragment = join_debugger_parts(&parts[1..]);
                    let output = self.run_debugger_eval(
                        token,
                        &mut task,
                        &fragment,
                        session_state.format_limit(),
                    );
                    (output.bytes, output.truncated)
                };
                let (log, hint) = apply_debugger_log_limit(&mut session_state, log, was_truncated);
                trim_hints.extend(hint);
                debugger_log_reply(log)
            } else if command.eq_ignore_ascii_case(b"print") || command.eq_ignore_ascii_case(b"p") {
                let (log, was_truncated) = match parts.as_slice() {
                    [_, name] => task
                        .debug_variable(name, session_state.format_limit())
                        .map(|output| (output.bytes, output.truncated))
                        .unwrap_or_else(|| (Bytes::from_static(b"No such variable."), false)),
                    [_] => (
                        Bytes::from_static(b"<error> print requires a variable name"),
                        false,
                    ),
                    _ => (
                        Bytes::from_static(b"<error> Wrong number of arguments for 'print'."),
                        false,
                    ),
                };
                let (log, hint) = apply_debugger_log_limit(&mut session_state, log, was_truncated);
                trim_hints.extend(hint);
                debugger_log_reply(log)
            } else if is_debugger_command_alias(command) {
                if parts.len() < 2 {
                    debugger_log_reply("<error> debugger command requires a server command")
                } else {
                    let args = CommandArgs::from_parts(parts[1..].to_vec());
                    let result = self.run_debugger_command(token, &mut task, &args);
                    let (reply, hints) = debugger_command_logs(&args, result, &mut session_state);
                    trim_hints = hints;
                    reply
                }
            } else if command.eq_ignore_ascii_case(b"l") || command.eq_ignore_ascii_case(b"list") {
                if parts.len() != 1 {
                    debugger_wrong_arity_reply("list")
                } else {
                    Value::Array(bounded_debugger_source_lines(&source))
                }
            } else if command.eq_ignore_ascii_case(b"h") || command.eq_ignore_ascii_case(b"help") {
                if parts.len() != 1 {
                    debugger_wrong_arity_reply("help")
                } else {
                    Value::Array(vec![Value::SimpleString(Bytes::from_static(
                        b"continue (c), abort (a), eval (e), print (p), redis, valkey, server, list (l), help (h)",
                    ))])
                }
            } else {
                debugger_log_reply("<error> Unknown debugger command")
            };
            if let Err(error) = self.write_debugger_reply(token, reply) {
                let result = Value::error(format!("ERR debugger I/O error: {error}"));
                let _ = self.finish_interrupted_debugger_task(token, session, task, mode, result);
                return Err(error);
            }
            for hint in trim_hints {
                if let Err(error) =
                    self.write_debugger_reply(token, debugger_log_reply(Bytes::from_static(hint)))
                {
                    let result = Value::error(format!("ERR debugger I/O error: {error}"));
                    let _ =
                        self.finish_interrupted_debugger_task(token, session, task, mode, result);
                    return Err(error);
                }
            }
        }
    }

    fn finish_interrupted_debugger_task(
        &mut self,
        token: Token,
        session: SessionId,
        task: LuaTask,
        mode: DebuggerSessionMode,
        result: Value,
    ) -> Option<Value> {
        if mode != DebuggerSessionMode::Synchronous {
            return None;
        }
        let connection = self
            .connection_registry
            .get_mut(token)
            .filter(|connection| connection.session_id() == session)?;
        let result = task.finish(result, connection, &mut self.ctx);
        connection.mark_closing();
        connection.is_async_close_pending = true;
        Some(result)
    }

    fn run_debugger_eval(
        &mut self,
        token: Token,
        task: &mut LuaTask,
        fragment: &[u8],
        output_limit: usize,
    ) -> crate::engine::extension::lua::LuaFormatOutput {
        self.ctx.enter_script(task.control());
        let result = self.with_host_dispatcher(HostTaskScope::client(token), |dispatcher| {
            task.debug_eval(fragment, dispatcher, output_limit)
        });
        self.ctx.leave_script();
        result
    }

    fn run_debugger_command(
        &mut self,
        token: Token,
        task: &mut LuaTask,
        args: &CommandArgs,
    ) -> Result<Value, HostError> {
        self.ctx.enter_script(task.control());
        let result = self.with_host_dispatcher(HostTaskScope::client(token), |dispatcher| {
            task.debug_command(args, dispatcher)
        });
        self.ctx.leave_script();
        result
    }

    fn run_debugger_script(&mut self, token: Token, mut task: LuaTask) -> Value {
        self.ctx.enter_script(task.control());
        let result = self.with_host_dispatcher(HostTaskScope::client(token), |dispatcher| {
            task.run(&mut |request| {
                process::exit_if_persistence_child_parent_changed();
                if let HostRequest::Yield(HostYield::ScriptCheckpoint { run }) = &request {
                    if run.timeout_ms > 0
                        && run.start_time.is_some_and(|start| {
                            start.elapsed().as_millis() >= run.timeout_ms as u128
                        })
                    {
                        return Err(HostError::Runtime(
                            "Script timed out in debugger".to_string(),
                        ));
                    }
                }
                dispatcher(request)
            })
        });
        self.ctx.leave_script();
        task.finish(result, &mut self.connection_registry[token], &mut self.ctx)
    }

    fn write_debugger_reply(&mut self, token: Token, mut reply: Value) -> io::Result<()> {
        sanitize_debugger_reply_lines(&mut reply);
        self.write_debugger_bytes(token, &serialize_to_vec(&reply))
    }

    fn write_debugger_stopped(&mut self, token: Token, source: &str) -> io::Result<()> {
        self.write_debugger_reply(
            token,
            Value::Array(vec![
                Value::SimpleString(Bytes::from_static(
                    b"* Stopped at 1, stop reason = step over",
                )),
                Value::SimpleString(bounded_debugger_source_line(
                    b"-> 1 ",
                    source.lines().next().unwrap_or(""),
                )),
            ]),
        )
    }

    fn write_debugger_session_end(&mut self, token: Token, result: Value) -> io::Result<()> {
        for reply in debugger_session_end_replies(result) {
            self.write_debugger_reply(token, reply)?;
        }
        Ok(())
    }

    fn finish_debugger_session(
        &mut self,
        token: Token,
        result: Value,
        mode: DebuggerSessionMode,
    ) -> io::Result<Option<Value>> {
        if mode == DebuggerSessionMode::Forked {
            self.write_debugger_session_end(token, result)?;
            return Ok(None);
        }
        let [end_marker, result] = debugger_session_end_replies(result);
        self.write_debugger_reply(token, end_marker)?;
        Ok(Some(result))
    }

    fn write_debugger_bytes(&mut self, token: Token, mut bytes: &[u8]) -> io::Result<()> {
        while !bytes.is_empty() {
            process::exit_if_persistence_child_parent_changed();
            match self.transport.write(token, bytes) {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(count) => bytes = &bytes[count..],
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    self.wait_debugger_socket(token, libc::POLLOUT)?;
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    fn wait_debugger_socket(&self, token: Token, events: i16) -> io::Result<()> {
        let fd = self.transport.raw_fd(token)?;
        let mut descriptor = os::PollDescriptor::new(fd, events);
        os::poll_descriptors(
            std::slice::from_mut(&mut descriptor),
            Duration::from_secs(1),
        )
        .map(|_| ())
    }
}

fn debugger_session_end_replies(result: Value) -> [Value; 2] {
    [
        Value::Array(vec![Value::SimpleString(Bytes::from_static(
            b"<endsession>",
        ))]),
        result,
    ]
}

fn sanitize_debugger_reply_lines(value: &mut Value) {
    sanitize_debugger_reply_lines_at_depth(value, 0);
}

fn sanitize_debugger_reply_lines_at_depth(value: &mut Value, depth: usize) {
    if depth >= MAX_DEBUGGER_REPLY_DEPTH {
        *value = Value::SimpleString(Bytes::from_static(MAX_DEBUGGER_REPLY_DEPTH_MESSAGE));
        return;
    }
    match value {
        Value::SimpleString(bytes) | Value::Error(bytes) => {
            if bytes.iter().any(|byte| matches!(byte, b'\r' | b'\n')) {
                *bytes = Bytes::from(
                    bytes
                        .iter()
                        .map(|byte| {
                            if matches!(byte, b'\r' | b'\n') {
                                b' '
                            } else {
                                *byte
                            }
                        })
                        .collect::<Vec<_>>(),
                );
            }
        }
        Value::Array(values) | Value::Push(values) | Value::Set(values) => {
            for value in values {
                sanitize_debugger_reply_lines_at_depth(value, depth + 1);
            }
        }
        Value::Map(entries) => sanitize_debugger_reply_pairs(entries, depth + 1),
        Value::Attribute { attrs, inner } => {
            sanitize_debugger_reply_pairs(attrs, depth + 1);
            sanitize_debugger_reply_lines_at_depth(inner, depth + 1);
        }
        Value::Integer(_)
        | Value::Double(_)
        | Value::DecimalDouble(_)
        | Value::Bulk(_)
        | Value::Null
        | Value::NullArray
        | Value::Boolean(_)
        | Value::BigNumber(_)
        | Value::Verbatim { .. } => {}
    }
}

fn sanitize_debugger_reply_pairs(entries: &mut [(Value, Value)], depth: usize) {
    for (key, value) in entries {
        sanitize_debugger_reply_lines_at_depth(key, depth);
        sanitize_debugger_reply_lines_at_depth(value, depth);
    }
}

fn debugger_log_reply(log: impl Into<Bytes>) -> Value {
    Value::Array(vec![Value::SimpleString(log.into())])
}

fn debugger_wrong_arity_reply(command: &str) -> Value {
    debugger_log_reply(format!(
        "<error> Wrong number of arguments for '{command}'."
    ))
}

fn debugger_unknown_scripting_engine_reply(engine: &[u8]) -> Value {
    debugger_log_reply(format!(
        "<error> No scripting engine found with name '{}' to enable debug",
        String::from_utf8_lossy(engine)
    ))
}

fn bounded_debugger_source_line(prefix: &[u8], source_line: &str) -> Bytes {
    let source_line = source_line.as_bytes();
    let full_len = prefix.len().saturating_add(source_line.len());
    if full_len <= MAX_DEBUGGER_LOG_FORMAT_BYTES {
        let mut output = Vec::with_capacity(full_len);
        output.extend_from_slice(prefix);
        output.extend_from_slice(source_line);
        return Bytes::from(output);
    }

    let mut output =
        Vec::with_capacity(MAX_DEBUGGER_LOG_FORMAT_BYTES + DEBUGGER_LOG_TRIM_SUFFIX.len());
    let prefix_len = prefix.len().min(MAX_DEBUGGER_LOG_FORMAT_BYTES);
    output.extend_from_slice(&prefix[..prefix_len]);
    let source_len = source_line
        .len()
        .min(MAX_DEBUGGER_LOG_FORMAT_BYTES - prefix_len);
    output.extend_from_slice(&source_line[..source_len]);
    output.extend_from_slice(DEBUGGER_LOG_TRIM_SUFFIX);
    Bytes::from(output)
}

fn bounded_debugger_source_lines(source: &str) -> Vec<Value> {
    let mut lines = Vec::new();
    let mut total_bytes = 0_usize;
    for (index, source_line) in source.lines().enumerate() {
        if lines.len() >= MAX_DEBUGGER_SOURCE_LINES {
            lines.push(Value::SimpleString(Bytes::from_static(
                DEBUGGER_SOURCE_TRUNCATION_MARKER,
            )));
            break;
        }

        let prefix = format!("{} ", index + 1);
        let full_len = prefix.len().saturating_add(source_line.len());
        let bounded_len = if full_len <= MAX_DEBUGGER_LOG_FORMAT_BYTES {
            full_len
        } else {
            MAX_DEBUGGER_LOG_FORMAT_BYTES + DEBUGGER_LOG_TRIM_SUFFIX.len()
        };
        if total_bytes.saturating_add(bounded_len) > MAX_DEBUGGER_SOURCE_TOTAL_BYTES {
            lines.push(Value::SimpleString(Bytes::from_static(
                DEBUGGER_SOURCE_TRUNCATION_MARKER,
            )));
            break;
        }

        lines.push(Value::SimpleString(bounded_debugger_source_line(
            prefix.as_bytes(),
            source_line,
        )));
        total_bytes += bounded_len;
    }
    lines
}

fn debugger_maxlen_reply(parts: &[Bytes], state: &mut DebuggerSessionState) -> Value {
    let log = match parts {
        [_] => Bytes::from(format!("Current maxlen is {}", state.max_len)),
        [_, value] => {
            let parsed = std::str::from_utf8(value)
                .ok()
                .and_then(|value| value.parse::<i64>().ok())
                .and_then(|value| usize::try_from(value).ok());
            match parsed {
                Some(value) => {
                    state.set_max_len(value);
                    if value == 0 {
                        Bytes::from_static(b"<value> configured reply truncation is disabled.")
                    } else {
                        Bytes::from(format!(
                            "<value> replies are truncated at {} bytes.",
                            state.max_len
                        ))
                    }
                }
                None => Bytes::from_static(b"<error> Invalid maxlen value."),
            }
        }
        _ => Bytes::from_static(b"<error> Wrong number of arguments for 'maxlen'."),
    };
    debugger_log_reply(log)
}

fn apply_debugger_log_limit(
    state: &mut DebuggerSessionState,
    log: Bytes,
    formatter_truncated: bool,
) -> (Bytes, Option<&'static [u8]>) {
    if state.max_len > 0
        && state.max_len < MAX_DEBUGGER_LOG_FORMAT_BYTES
        && (log.len() > state.max_len || formatter_truncated)
    {
        let log = trim_debugger_log(log, state.max_len);
        let hint = (!state.maxlen_hint_sent).then_some(DEBUGGER_LOG_TRIM_HINT);
        state.maxlen_hint_sent = true;
        return (log, hint);
    }

    apply_debugger_hard_limit(state, log, formatter_truncated)
}

fn apply_debugger_hard_limit(
    state: &mut DebuggerSessionState,
    log: Bytes,
    formatter_truncated: bool,
) -> (Bytes, Option<&'static [u8]>) {
    if log.len() > MAX_DEBUGGER_LOG_FORMAT_BYTES || formatter_truncated {
        let log = trim_debugger_log(log, MAX_DEBUGGER_LOG_FORMAT_BYTES);
        let hint = (!state.hard_limit_hint_sent).then_some(DEBUGGER_LOG_HARD_LIMIT_HINT);
        state.hard_limit_hint_sent = true;
        return (log, hint);
    }

    (log, None)
}

fn trim_debugger_log(log: Bytes, limit: usize) -> Bytes {
    let prefix_len = log.len().min(limit);
    let mut trimmed = Vec::with_capacity(prefix_len + DEBUGGER_LOG_TRIM_SUFFIX.len());
    trimmed.extend_from_slice(&log[..prefix_len]);
    trimmed.extend_from_slice(DEBUGGER_LOG_TRIM_SUFFIX);
    Bytes::from(trimmed)
}

fn debugger_command_logs(
    args: &CommandArgs,
    result: Result<Value, HostError>,
    state: &mut DebuggerSessionState,
) -> (Value, Vec<&'static [u8]>) {
    let mut command = DebuggerReplyOutput::new(MAX_DEBUGGER_LOG_FORMAT_BYTES);
    command.push(b"<command> ");
    command.push(&args.name);
    for arg in &args.args {
        command.push(b" ");
        command.push(arg);
    }
    let (command, command_truncated) = command.finish();
    let (command, command_hint) = apply_debugger_hard_limit(state, command, command_truncated);

    let mut result_log = DebuggerReplyOutput::new(state.format_limit());
    match result {
        Ok(Value::Error(error)) => {
            result_log.push(b"<error> ");
            result_log.push(&error);
        }
        Ok(value) => {
            result_log.push(b"<reply> ");
            format_debugger_reply_at_depth(&value, 0, &mut result_log);
        }
        Err(error) => {
            result_log.push(b"<error> ");
            result_log.push_display(&error);
        }
    }
    let (result_log, formatter_truncated) = result_log.finish();
    let (result_log, result_hint) =
        apply_debugger_log_limit(state, result_log, formatter_truncated);
    let hints = command_hint.into_iter().chain(result_hint).collect();

    (
        Value::Array(vec![
            Value::SimpleString(command),
            Value::SimpleString(result_log),
        ]),
        hints,
    )
}

#[cfg(test)]
fn format_debugger_reply(value: &Value) -> String {
    String::from_utf8_lossy(&format_debugger_reply_with_limit(
        value,
        MAX_DEBUGGER_LOG_FORMAT_BYTES,
    ))
    .into_owned()
}

#[cfg(test)]
fn format_debugger_reply_with_limit(value: &Value, limit: usize) -> Bytes {
    let mut output = DebuggerReplyOutput::new(limit);
    format_debugger_reply_at_depth(value, 0, &mut output);
    output.finish().0
}

struct DebuggerReplyOutput {
    bytes: Vec<u8>,
    limit: usize,
    truncated: bool,
}

impl DebuggerReplyOutput {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::with_capacity(limit.min(256)),
            limit,
            truncated: false,
        }
    }

    fn push(&mut self, bytes: &[u8]) {
        let count = bytes.len().min(self.limit.saturating_sub(self.bytes.len()));
        self.bytes.extend_from_slice(&bytes[..count]);
        self.truncated |= count < bytes.len();
    }

    fn push_display(&mut self, value: &impl std::fmt::Display) {
        use std::fmt::Write;

        let _ = write!(self, "{value}");
    }

    fn is_full(&self) -> bool {
        self.bytes.len() >= self.limit
    }

    fn mark_truncated(&mut self) {
        self.truncated = true;
    }

    fn finish(self) -> (Bytes, bool) {
        (Bytes::from(self.bytes), self.truncated)
    }
}

impl std::fmt::Write for DebuggerReplyOutput {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        self.push(value.as_bytes());
        Ok(())
    }
}

fn format_debugger_reply_at_depth(value: &Value, depth: usize, output: &mut DebuggerReplyOutput) {
    if output.is_full() {
        output.mark_truncated();
        return;
    }
    if depth >= MAX_DEBUGGER_REPLY_DEPTH {
        output.push(MAX_DEBUGGER_REPLY_DEPTH_MESSAGE);
        return;
    }
    match value {
        Value::SimpleString(value)
        | Value::Error(value)
        | Value::Bulk(value)
        | Value::BigNumber(value) => output.push(value),
        Value::Integer(value) => output.push(value.to_string().as_bytes()),
        Value::Double(value) => output.push(format!("(double) {value}").as_bytes()),
        Value::DecimalDouble(value) => {
            output.push(b"(double) ");
            output.push(value.as_bytes());
        }
        Value::Null | Value::NullArray => output.push(b"(null)"),
        Value::Array(values) => format_debugger_sequence(values, b"[", b"]", depth, output),
        Value::Boolean(value) => output.push(format!("#{value}").as_bytes()),
        Value::Verbatim { data, .. } => output.push(data),
        Value::Attribute { attrs, inner } => {
            output.push(b"@");
            format_debugger_pairs(attrs, depth, output);
            output.push(b" ");
            format_debugger_reply_at_depth(inner, depth + 1, output);
        }
        Value::Push(values) => format_debugger_sequence(values, b">[", b"]", depth, output),
        Value::Map(entries) => format_debugger_pairs(entries, depth, output),
        Value::Set(values) => format_debugger_sequence(values, b"~(", b")", depth, output),
    }
}

fn format_debugger_sequence(
    values: &[Value],
    prefix: &[u8],
    suffix: &[u8],
    depth: usize,
    output: &mut DebuggerReplyOutput,
) {
    output.push(prefix);
    for (index, value) in values.iter().enumerate() {
        if output.is_full() {
            output.mark_truncated();
            return;
        }
        if index > 0 {
            output.push(b",");
        }
        format_debugger_reply_at_depth(value, depth + 1, output);
    }
    output.push(suffix);
}

fn format_debugger_pairs(
    entries: &[(Value, Value)],
    depth: usize,
    output: &mut DebuggerReplyOutput,
) {
    output.push(b"{");
    for (index, (key, value)) in entries.iter().enumerate() {
        if output.is_full() {
            output.mark_truncated();
            return;
        }
        if index > 0 {
            output.push(b",");
        }
        format_debugger_reply_at_depth(key, depth + 1, output);
        output.push(b" => ");
        format_debugger_reply_at_depth(value, depth + 1, output);
    }
    output.push(b"}");
}

fn join_debugger_parts(parts: &[Bytes]) -> Vec<u8> {
    let capacity = parts.iter().map(Bytes::len).sum::<usize>() + parts.len().saturating_sub(1);
    let mut joined = Vec::with_capacity(capacity);
    for (index, part) in parts.iter().enumerate() {
        if index > 0 {
            joined.push(b' ');
        }
        joined.extend_from_slice(part);
    }
    joined
}

fn debugger_command_exceeds_size_limit(parts: &[Bytes]) -> bool {
    parts
        .iter()
        .fold(0usize, |total, part| total.saturating_add(part.len()))
        > MAX_DEBUGGER_COMMAND_BYTES
}

fn parse_debugger_command(parser: &mut Parser) -> Result<Option<Vec<Bytes>>, ParseError> {
    parser.try_parse_limited(MAX_DEBUGGER_COMMAND_BYTES, MAX_DEBUGGER_COMMAND_BYTES)
}

fn is_debugger_command_alias(command: &[u8]) -> bool {
    command.eq_ignore_ascii_case(b"redis")
        || command.eq_ignore_ascii_case(b"valkey")
        || command.eq_ignore_ascii_case(b"server")
}

fn is_debugger_maxlen_command(command: &[u8]) -> bool {
    command.eq_ignore_ascii_case(b"maxlen")
        || command.eq_ignore_ascii_case(b"max")
        || command.eq_ignore_ascii_case(b"ml")
}

fn close_inherited_fds_except(keep_fd: i32) -> io::Result<()> {
    let entries = std::fs::read_dir("/dev/fd").or_else(|_| std::fs::read_dir("/proc/self/fd"))?;
    let fds = entries
        .filter_map(Result::ok)
        .filter_map(|entry| entry.file_name().to_string_lossy().parse::<i32>().ok())
        .filter(|fd| *fd > libc::STDERR_FILENO && *fd != keep_fd)
        .collect::<Vec<_>>();
    for fd in fds {
        close_fd(fd);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        debugger_command_exceeds_size_limit, debugger_command_logs, debugger_session_end_replies,
        format_debugger_reply, format_debugger_reply_with_limit, parse_debugger_command,
        sanitize_debugger_reply_lines, DebuggerSessionState, DEBUGGER_LOG_HARD_LIMIT_HINT,
        DEBUGGER_LOG_TRIM_SUFFIX, MAX_DEBUGGER_LOG_FORMAT_BYTES,
    };
    use crate::engine::command::CommandArgs;
    use crate::foundation::io::protocol::Value;
    use crate::framework::capabilities::connection::Connection;
    use crate::framework::capabilities::io::Token;
    use crate::runtime::server::worker::ServerWorkerState;
    use bytes::Bytes;

    const MAX_REPLY_DEPTH: usize = 16;
    const MAX_REPLY_DEPTH_MESSAGE: &[u8] = b"<max reply depth reached>";

    fn nested_array(depth: usize, leaf: Value) -> Value {
        (0..depth).fold(leaf, |value, _| Value::Array(vec![value]))
    }

    #[test]
    fn debugger_session_end_marker_precedes_abort_error() {
        let replies =
            debugger_session_end_replies(Value::error("ERR script aborted for user request"));

        assert_eq!(
            replies[0],
            Value::Array(vec![Value::SimpleString(Bytes::from_static(
                b"<endsession>",
            ))])
        );
        assert_eq!(
            replies[1],
            Value::error("ERR script aborted for user request")
        );
    }

    #[test]
    fn debugger_command_size_limit_counts_all_parts() {
        let part = Bytes::from(vec![b'x'; 1024]);
        let at_limit = vec![part; 1024];
        assert!(!debugger_command_exceeds_size_limit(&at_limit));

        let mut over_limit = at_limit;
        over_limit.push(Bytes::from_static(b"x"));
        assert!(debugger_command_exceeds_size_limit(&over_limit));
    }

    #[test]
    fn debugger_parser_accepts_bulk_arguments_above_legacy_one_kib_limit() {
        let mut command = b"*2\r\n$5\r\nredis\r\n$2048\r\n".to_vec();
        command.extend(std::iter::repeat_n(b'x', 2048));
        command.extend_from_slice(b"\r\n");
        let mut parser = crate::foundation::io::protocol::parser::Parser::new();
        parser.feed(&command);

        let parsed = parse_debugger_command(&mut parser)
            .expect("debugger command must parse without protocol errors")
            .expect("debugger command must be complete");

        assert_eq!(parsed[0].as_ref(), b"redis");
        assert_eq!(parsed[1].len(), 2048);
    }

    #[test]
    fn debugger_command_log_reports_hard_cap_truncation() {
        let argument = Bytes::from(vec![b'x'; 1024]);
        let mut parts = Vec::with_capacity(1024);
        parts.push(Bytes::from_static(b"ECHO"));
        parts.extend(std::iter::repeat_n(argument, 1023));
        let args = CommandArgs::from_parts(parts);
        let mut state = DebuggerSessionState::default();
        state.set_max_len(0);

        let (reply, hint) = debugger_command_logs(&args, Ok(Value::ok()), &mut state);

        let Value::Array(logs) = reply else {
            panic!("expected debugger command logs");
        };
        let Value::SimpleString(command_log) = &logs[0] else {
            panic!("expected debugger command log");
        };
        assert_eq!(
            command_log.len(),
            MAX_DEBUGGER_LOG_FORMAT_BYTES + DEBUGGER_LOG_TRIM_SUFFIX.len()
        );
        assert_eq!(hint, vec![DEBUGGER_LOG_HARD_LIMIT_HINT]);
    }

    #[test]
    fn debugger_session_cleanup_preserves_other_sessions() {
        let mut runtime = ServerWorkerState::new(1).unwrap();
        let current_token = Token(41);
        let other_token = Token(42);
        let mut current = Connection::new_fake();
        current.set_token_for_test(current_token);
        runtime.connection_registry.insert(current);
        let mut other = Connection::new_fake();
        other.set_token_for_test(other_token);
        runtime.connection_registry.insert(other);
        let current_session = runtime.connection_registry[current_token].session_id();
        let other_session = runtime.connection_registry[other_token].session_id();
        runtime.ctx.script_mut().debug_modes.insert(
            current_session,
            crate::engine::extension::script::ScriptDebugMode::Forked,
        );
        runtime.ctx.script_mut().debug_modes.insert(
            other_session,
            crate::engine::extension::script::ScriptDebugMode::Synchronous,
        );
        runtime.ctx.script_mut().debug_session = Some(other_session);

        runtime.clear_lua_debugger_session(current_token);

        assert!(!runtime
            .ctx
            .script()
            .debug_modes
            .contains_key(&current_session));
        assert_eq!(
            runtime.ctx.script().debug_modes.get(&other_session),
            Some(&crate::engine::extension::script::ScriptDebugMode::Synchronous)
        );
        assert_eq!(runtime.ctx.script().debug_session, Some(other_session));
    }

    #[test]
    fn debugger_reply_formatter_covers_resp_value_shapes() {
        assert_eq!(format_debugger_reply(&Value::ok()), "OK");
        assert_eq!(
            format_debugger_reply(&Value::bulk(Bytes::from_static(b"value"))),
            "value"
        );
        assert_eq!(format_debugger_reply(&Value::integer(7)), "7");
        assert_eq!(format_debugger_reply(&Value::Null), "(null)");
        assert_eq!(
            format_debugger_reply(&Value::Array(vec![
                Value::bulk(Bytes::from_static(b"one")),
                Value::integer(2),
            ])),
            "[one,2]"
        );
        assert_eq!(format_debugger_reply(&Value::Boolean(true)), "#true");
        assert_eq!(format_debugger_reply(&Value::Double(1.5)), "(double) 1.5");
        assert_eq!(
            format_debugger_reply(&Value::error("ERR failure")),
            "ERR failure"
        );
    }

    #[test]
    fn debugger_reply_formatter_stops_at_max_depth() {
        let reply = nested_array(32, Value::bulk(Bytes::from_static(b"leaf")));
        let formatted = format_debugger_reply(&reply);
        let expected = format!(
            "{}{}{}",
            "[".repeat(MAX_REPLY_DEPTH),
            String::from_utf8_lossy(MAX_REPLY_DEPTH_MESSAGE),
            "]".repeat(MAX_REPLY_DEPTH)
        );

        assert_eq!(formatted, expected);
        assert_eq!(
            formatted.len(),
            MAX_REPLY_DEPTH * 2 + MAX_REPLY_DEPTH_MESSAGE.len()
        );
    }

    #[test]
    fn debugger_reply_formatter_stops_at_output_budget() {
        let reply = Value::Array(vec![Value::bulk(Bytes::from_static(b"0123456789")); 128]);

        let formatted = format_debugger_reply_with_limit(&reply, 32);

        assert_eq!(formatted.len(), 32);
        assert_eq!(formatted, &b"[0123456789,0123456789,012345678"[..]);
    }

    #[test]
    fn debugger_reply_line_sanitizer_recurses_without_changing_bulk_data() {
        let bulk = Bytes::from_static(b"bulk\r\npayload");
        let mut reply = Value::Attribute {
            attrs: vec![(
                Value::SimpleString(Bytes::from_static(b"key\r\nline")),
                Value::Push(vec![
                    Value::Error(Bytes::from_static(b"error\nline")),
                    Value::Bulk(bulk.clone()),
                ]),
            )],
            inner: Box::new(Value::Map(vec![(
                Value::Array(vec![Value::SimpleString(Bytes::from_static(
                    b"array\rline",
                ))]),
                Value::Set(vec![Value::Error(Bytes::from_static(b"set\nline"))]),
            )])),
        };

        sanitize_debugger_reply_lines(&mut reply);

        assert_eq!(
            reply,
            Value::Attribute {
                attrs: vec![(
                    Value::SimpleString(Bytes::from_static(b"key  line")),
                    Value::Push(vec![
                        Value::Error(Bytes::from_static(b"error line")),
                        Value::Bulk(bulk),
                    ]),
                )],
                inner: Box::new(Value::Map(vec![(
                    Value::Array(vec![Value::SimpleString(
                        Bytes::from_static(b"array line",)
                    )]),
                    Value::Set(vec![Value::Error(Bytes::from_static(b"set line"))]),
                )])),
            }
        );
    }

    #[test]
    fn debugger_reply_line_sanitizer_stops_at_max_depth() {
        let mut reply = nested_array(32, Value::SimpleString(Bytes::from_static(b"deep\nline")));

        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            sanitize_debugger_reply_lines(&mut reply);
        }));

        assert!(outcome.is_ok(), "deep reply sanitization must not panic");
        assert_eq!(
            reply,
            nested_array(
                MAX_REPLY_DEPTH,
                Value::SimpleString(Bytes::from_static(MAX_REPLY_DEPTH_MESSAGE)),
            )
        );
    }
}
