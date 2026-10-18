use super::super::super::ServerWorkerState;
use crate::framework::context::MonitorEvent;

impl ServerWorkerState {
    pub(crate) fn deliver_monitor(&mut self, mut events: Vec<MonitorEvent>) {
        events.sort_unstable_by_key(|event| event.sequence);
        for event in events {
            let frame = render_monitor_event(&event);
            let monitor_tokens: Vec<crate::framework::capabilities::io::Token> = event
                .recipients
                .iter()
                .filter_map(|client_id| self.connection_registry.token_for_client_id(*client_id))
                .collect();
            for token in monitor_tokens {
                let connection_token = match self
                    .connection_registry
                    .contains(crate::framework::capabilities::io::Token(token.0))
                    .then_some(token.0)
                {
                    Some(key) => key,
                    None => continue,
                };
                if self
                    .connection_registry
                    .contains(crate::framework::capabilities::io::Token(connection_token))
                {
                    self.queue_client_reply(token, frame.clone());
                }
            }
        }
    }
}

pub(crate) fn is_explicit_script_monitor_command(name: &str) -> bool {
    matches!(
        name,
        "eval" | "evalsha" | "eval_ro" | "evalsha_ro" | "fcall" | "fcall_ro"
    )
}

pub(super) fn render_monitor_event(event: &MonitorEvent) -> Vec<u8> {
    let mut frame = format!(
        "+{}.{:06} [{} {}] ",
        event.seconds, event.microseconds, event.db, event.source
    );
    for (index, arg) in event.argv.iter().enumerate() {
        if index > 0 {
            frame.push(' ');
        }
        let redact = monitor_argument_is_sensitive(&event.argv, index);
        frame.push('"');
        let bytes: &[u8] = if redact { b"(redacted)" } else { arg };
        for byte in bytes {
            match byte {
                b'\\' => frame.push_str("\\\\"),
                b'"' => frame.push_str("\\\""),
                b'\n' => frame.push_str("\\n"),
                b'\r' => frame.push_str("\\r"),
                b'\t' => frame.push_str("\\t"),
                b'\x07' => frame.push_str("\\a"),
                b'\x08' => frame.push_str("\\b"),
                0x20..=0x7e => frame.push(*byte as char),
                _ => frame.push_str(&format!("\\x{:02x}", byte)),
            }
        }
        frame.push('"');
    }
    frame.extend(['\r', '\n']);
    frame.into_bytes()
}

fn monitor_argument_is_sensitive(argv: &[bytes::Bytes], index: usize) -> bool {
    if index == 0 {
        return false;
    }
    let command = String::from_utf8_lossy(&argv[0]).to_ascii_lowercase();
    match command.as_str() {
        "auth" => true,
        "hello" => argv
            .iter()
            .position(|arg| arg.eq_ignore_ascii_case(b"auth"))
            .is_some_and(|auth| index == auth + 1 || index == auth + 2),
        "migrate" => argv
            .iter()
            .position(|arg| arg.eq_ignore_ascii_case(b"auth") || arg.eq_ignore_ascii_case(b"auth2"))
            .is_some_and(|auth| {
                index == auth + 1
                    || (argv[auth].eq_ignore_ascii_case(b"auth2") && index == auth + 2)
            }),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::render_monitor_event;
    use crate::engine::command::{CMD_ADMIN, CMD_SKIP_MONITOR};
    use crate::framework::context::{MonitorEvent, ServerContext};
    use bytes::Bytes;

    fn argv(parts: &[&'static [u8]]) -> Vec<Bytes> {
        parts.iter().copied().map(Bytes::from_static).collect()
    }

    #[test]
    fn monitor_eligibility_matches_loading_and_command_flags() {
        let mut ctx = ServerContext::new(1);
        let command = argv(&[b"SET", b"key", b"value"]);

        assert!(ctx
            .monitor_event(0, "127.0.0.1:1", command.clone(), 0, false)
            .is_none());
        ctx.register_monitor_client(1);
        assert!(ctx
            .monitor_event(0, "127.0.0.1:1", command.clone(), 0, false)
            .is_some());
        assert!(ctx
            .monitor_event(0, "127.0.0.1:1", command.clone(), CMD_ADMIN, false)
            .is_none());
        assert!(ctx
            .monitor_event(0, "127.0.0.1:1", command.clone(), CMD_SKIP_MONITOR, false,)
            .is_none());
        assert!(ctx
            .monitor_event(0, "127.0.0.1:1", command.clone(), CMD_SKIP_MONITOR, true)
            .is_some());
        ctx.set_loading(true);
        assert!(ctx
            .monitor_event(0, "127.0.0.1:1", command, 0, true)
            .is_none());
    }

    #[test]
    fn monitor_render_uses_capture_time_and_c_escaping() {
        let event = MonitorEvent::at(
            12,
            34,
            1,
            9,
            "127.0.0.1:42",
            argv(&[b"echo", b"quote\" slash\\ newline\n hex\x7f"]),
        );

        assert_eq!(
            render_monitor_event(&event),
            b"+12.000034 [9 127.0.0.1:42] \"echo\" \"quote\\\" slash\\\\ newline\\n hex\\x7f\"\r\n"
        );
    }

    #[test]
    fn monitor_redacts_only_credential_arguments() {
        let hello = MonitorEvent::at(
            1,
            2,
            1,
            0,
            "source",
            argv(&[
                b"hello",
                b"2",
                b"AUTH",
                b"user",
                b"password",
                b"SETNAME",
                b"visible",
            ]),
        );
        let migrate = MonitorEvent::at(
            1,
            2,
            2,
            0,
            "source",
            argv(&[
                b"migrate",
                b"host",
                b"1",
                b"",
                b"0",
                b"1",
                b"AUTH2",
                b"user",
                b"password",
                b"KEYS",
                b"visible",
            ]),
        );

        let hello = String::from_utf8(render_monitor_event(&hello)).unwrap();
        assert!(hello.contains("\"AUTH\" \"(redacted)\" \"(redacted)\" \"SETNAME\" \"visible\""));
        let migrate = String::from_utf8(render_monitor_event(&migrate)).unwrap();
        assert!(migrate.contains("\"AUTH2\" \"(redacted)\" \"(redacted)\" \"KEYS\" \"visible\""));
    }
}
