//! Resume the active Lua frame after commands on its protected debugger socket.

use crate::engine::extension::script::ScriptRunCtx;
use crate::engine::host::{
    dispatch_command, HostCommandOrigin, HostReply, ScriptCallMode, ScriptDebugStopReason,
};

use super::*;

enum DebuggerInput {
    Command(Vec<Bytes>),
    Disconnected,
}

enum DebuggerDelivery {
    Sent,
    Disconnected,
}

impl ServerWorkerState {
    pub(super) fn run_debugger_breakpoint(
        &mut self,
        token: Token,
        parser: &mut Parser,
        source: &str,
        session_state: &mut DebuggerSessionState,
        run: &mut ScriptRunCtx,
        line: usize,
        reason: ScriptDebugStopReason,
    ) -> Result<HostReply, HostError> {
        let reason = match reason {
            ScriptDebugStopReason::Breakpoint => "server.breakpoint() called",
            ScriptDebugStopReason::StepOver => "step over",
        };
        if let DebuggerDelivery::Disconnected = self.write_breakpoint_reply(
            token,
            Value::Array(vec![
                Value::SimpleString(Bytes::from(format!(
                    "* Stopped at {line}, stop reason = {reason}"
                ))),
                Value::SimpleString(bounded_debugger_source_line(
                    format!("-> {line} ").as_bytes(),
                    source.lines().nth(line.saturating_sub(1)).unwrap_or(""),
                )),
            ]),
        )? {
            return Ok(resume_disconnected_script(run, parser));
        }
        loop {
            let parts = match self.read_breakpoint_command(token, parser)? {
                DebuggerInput::Command(parts) => parts,
                DebuggerInput::Disconnected => {
                    return Ok(resume_disconnected_script(run, parser));
                }
            };
            let Some(command) = parts.first() else {
                continue;
            };
            if matches_continue(command) && parts.len() == 1 {
                run.start_timer();
                return Ok(HostReply::Continue);
            }
            if (command.eq_ignore_ascii_case(b"s") || command.eq_ignore_ascii_case(b"step"))
                && parts.len() == 1
            {
                crate::engine::extension::lua::timeout::resume_debugger_step();
                run.start_timer();
                return Ok(HostReply::Continue);
            }
            if (command.eq_ignore_ascii_case(b"a") || command.eq_ignore_ascii_case(b"abort"))
                && parts.len() == 1
            {
                return Err(HostError::Runtime(
                    "script aborted for user request".to_string(),
                ));
            }
            let (reply, hints) = if is_debugger_command_alias(command) && parts.len() > 1 {
                let args = CommandArgs::from_parts(parts[1..].to_vec());
                let result = dispatch_command(
                    &mut |request| {
                        self.dispatch_host_request(HostTaskScope::client(token), request)
                    },
                    &args,
                    HostCommandOrigin::Script {
                        run,
                        mode: ScriptCallMode::Call,
                    },
                );
                debugger_command_logs(&args, result, session_state)
            } else if is_debugger_maxlen_command(command) {
                (debugger_maxlen_reply(&parts, session_state), Vec::new())
            } else if command.eq_ignore_ascii_case(b"l") || command.eq_ignore_ascii_case(b"list") {
                (
                    Value::Array(bounded_debugger_source_lines(source)),
                    Vec::new(),
                )
            } else {
                (
                    debugger_log_reply(
                        "<error> Unknown debugger command or wrong number of arguments.",
                    ),
                    Vec::new(),
                )
            };
            if let DebuggerDelivery::Disconnected = self.write_breakpoint_reply(token, reply)? {
                return Ok(resume_disconnected_script(run, parser));
            }
            for hint in hints {
                if let DebuggerDelivery::Disconnected = self
                    .write_breakpoint_reply(token, debugger_log_reply(Bytes::from_static(hint)))?
                {
                    return Ok(resume_disconnected_script(run, parser));
                }
            }
        }
    }

    fn read_breakpoint_command(
        &mut self,
        token: Token,
        parser: &mut Parser,
    ) -> Result<DebuggerInput, HostError> {
        loop {
            process::exit_if_persistence_child_parent_changed();
            if parser.undecoded_len() > MAX_DEBUGGER_COMMAND_BYTES {
                parser.reset();
                return Err(HostError::Runtime("max client buffer reached".to_string()));
            }
            match parse_debugger_command(parser) {
                Ok(Some(parts)) if !debugger_command_exceeds_size_limit(&parts) => {
                    return Ok(DebuggerInput::Command(parts))
                }
                Ok(Some(_)) => {
                    parser.reset();
                    return Err(HostError::Runtime("max client buffer reached".to_string()));
                }
                Err(error) => {
                    parser.reset();
                    return Err(HostError::Runtime(match error {
                        ParseError::Protocol(message) => message,
                        ParseError::Incomplete => "Protocol error: incomplete request".to_string(),
                    }));
                }
                Ok(None) => {}
            }
            // Do not consume bytes belonging to the next breakpoint beyond the limit.
            let mut buffer = [0_u8; 1024];
            match self.transport.read(token, &mut buffer) {
                Ok(0) => {
                    parser.reset();
                    return Ok(DebuggerInput::Disconnected);
                }
                Ok(count) => parser.feed(&buffer[..count]),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    self.wait_debugger_socket(token, libc::POLLIN)
                        .map_err(debugger_io_error)?;
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => {
                    parser.reset();
                    if is_debugger_disconnect(&error) {
                        return Ok(DebuggerInput::Disconnected);
                    }
                    return Err(debugger_io_error(error));
                }
            }
        }
    }

    fn write_breakpoint_reply(
        &mut self,
        token: Token,
        reply: Value,
    ) -> Result<DebuggerDelivery, HostError> {
        match self.write_debugger_reply(token, reply) {
            Ok(()) => Ok(DebuggerDelivery::Sent),
            Err(error) if is_debugger_disconnect(&error) => Ok(DebuggerDelivery::Disconnected),
            Err(error) => Err(debugger_io_error(error)),
        }
    }
}

fn matches_continue(command: &[u8]) -> bool {
    command.eq_ignore_ascii_case(b"c") || command.eq_ignore_ascii_case(b"continue")
}

fn debugger_io_error(error: io::Error) -> HostError {
    HostError::Runtime(format!("debugger I/O error: {error}"))
}

fn is_debugger_disconnect(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset | io::ErrorKind::NotConnected
    )
}

fn resume_disconnected_script(run: &mut ScriptRunCtx, parser: &mut Parser) -> HostReply {
    crate::engine::extension::lua::timeout::detach_debugger();
    parser.reset();
    run.start_timer();
    HostReply::Continue
}
