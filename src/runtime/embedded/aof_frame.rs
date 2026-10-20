//! AOF frame construction for the embedded runtime.
//!
//! Builds the RESP command frames that make up the AOF stream for a write
//! command. Frames are the same wire form the native replication/AOF path
//! produces: an array of bulk strings. A `SELECT` frame is prepended whenever
//! the command's database differs from the one the AOF stream last selected,
//! so replay restores keys into the correct database.
//!
//! This module stays host-neutral so it compiles on the `wasm` target.

use bytes::{Bytes, BytesMut};

use crate::engine::command::{CommandArgs, CMD_WRITE};
use crate::engine::core::{CommandEffects, Value};
use crate::foundation::io::protocol::serializer::serialize_command;
use crate::framework::commands::command_runtime::lookup_command_name;

/// Tracks which logical database the AOF stream last selected so a `SELECT`
/// frame is emitted only when the active database actually changes.
#[derive(Default)]
pub(super) struct AofFrameEncoder {
    /// Database id last written to the AOF stream, or `None` before the first
    /// frame (which forces an initial `SELECT`).
    selected_db: Option<u32>,
}

impl AofFrameEncoder {
    /// Build the durable AOF frame for a completed command, or `None` if the
    /// command does not need to be persisted.
    ///
    /// A command is persisted when it is a write that completed without error
    /// and did not opt out via `skip_propagate` (unless it supplied explicit
    /// `custom_propagations`). The returned bytes may contain a leading
    /// `SELECT` followed by one or more command frames.
    pub(super) fn frame_for(
        &mut self,
        selected_db: u32,
        args: &CommandArgs,
        reply: &Value,
        effects: &CommandEffects,
    ) -> Option<Bytes> {
        let has_custom = !effects.custom_propagations.is_empty();
        if !has_custom {
            if matches!(reply, Value::Error(_)) {
                return None;
            }
            if effects.skip_propagate || !self.is_write_command(args) {
                return None;
            }
        }

        let mut buffer = BytesMut::new();
        if self.selected_db != Some(selected_db) {
            serialize_command(
                &[b"SELECT", selected_db.to_string().as_bytes()],
                &mut buffer,
            );
            self.selected_db = Some(selected_db);
        }
        if has_custom {
            for propagation in &effects.custom_propagations {
                let parts = propagation.iter().map(Bytes::as_ref).collect::<Vec<_>>();
                serialize_command(&parts, &mut buffer);
            }
        } else {
            let mut parts = Vec::with_capacity(args.args.len() + 1);
            parts.push(args.name.as_ref());
            parts.extend(args.args.iter().map(Bytes::as_ref));
            serialize_command(&parts, &mut buffer);
        }
        Some(buffer.freeze())
    }

    fn is_write_command(&self, args: &CommandArgs) -> bool {
        std::str::from_utf8(&args.name)
            .ok()
            .and_then(lookup_command_name)
            .is_some_and(|spec| spec.meta.flags & CMD_WRITE != 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn args(parts: &[&[u8]]) -> CommandArgs {
        CommandArgs::from_parts(
            parts
                .iter()
                .map(|part| Bytes::copy_from_slice(part))
                .collect(),
        )
    }

    #[test]
    fn write_command_frame_includes_initial_select() {
        let mut encoder = AofFrameEncoder::default();
        let frame = encoder
            .frame_for(
                0,
                &args(&[b"SET", b"key", b"value"]),
                &Value::ok(),
                &CommandEffects::default(),
            )
            .expect("write command produces a frame");
        assert_eq!(
            frame.as_ref(),
            b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
        );
    }

    #[test]
    fn select_is_only_re_emitted_on_database_change() {
        let mut encoder = AofFrameEncoder::default();
        let _ = encoder
            .frame_for(
                0,
                &args(&[b"SET", b"a", b"1"]),
                &Value::ok(),
                &CommandEffects::default(),
            )
            .expect("first frame");
        let second = encoder
            .frame_for(
                0,
                &args(&[b"SET", b"b", b"2"]),
                &Value::ok(),
                &CommandEffects::default(),
            )
            .expect("second frame");
        assert_eq!(
            second.as_ref(),
            b"*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n"
        );

        let third = encoder
            .frame_for(
                1,
                &args(&[b"SET", b"c", b"3"]),
                &Value::ok(),
                &CommandEffects::default(),
            )
            .expect("third frame after db switch");
        assert_eq!(
            third.as_ref(),
            b"*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n*3\r\n$3\r\nSET\r\n$1\r\nc\r\n$1\r\n3\r\n"
        );
    }

    #[test]
    fn read_commands_are_not_persisted() {
        let mut encoder = AofFrameEncoder::default();
        assert!(encoder
            .frame_for(
                0,
                &args(&[b"GET", b"key"]),
                &Value::Null,
                &CommandEffects::default(),
            )
            .is_none());
    }

    #[test]
    fn failed_writes_are_not_persisted() {
        let mut encoder = AofFrameEncoder::default();
        assert!(encoder
            .frame_for(
                0,
                &args(&[b"SET", b"key"]),
                &Value::error("ERR wrong number of arguments"),
                &CommandEffects::default(),
            )
            .is_none());
    }

    #[test]
    fn skip_propagate_suppresses_persistence() {
        let mut encoder = AofFrameEncoder::default();
        let effects = CommandEffects {
            skip_propagate: true,
            ..CommandEffects::default()
        };
        assert!(encoder
            .frame_for(
                0,
                &args(&[b"SET", b"key", b"value"]),
                &Value::ok(),
                &effects
            )
            .is_none());
    }

    #[test]
    fn custom_propagations_override_the_raw_command() {
        let mut encoder = AofFrameEncoder::default();
        let effects = CommandEffects {
            skip_propagate: true,
            custom_propagations: vec![vec![
                Bytes::from_static(b"SET"),
                Bytes::from_static(b"key"),
                Bytes::from_static(b"rewritten"),
            ]],
            ..CommandEffects::default()
        };
        let frame = encoder
            .frame_for(0, &args(&[b"SPOP", b"key"]), &Value::ok(), &effects)
            .expect("custom propagation produces a frame");
        assert_eq!(
            frame.as_ref(),
            b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$9\r\nrewritten\r\n"
        );
    }
}
