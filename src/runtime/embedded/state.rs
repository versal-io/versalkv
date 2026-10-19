use std::collections::VecDeque;

use crate::engine::command::{CommandArgs, SessionId};
use crate::foundation::io::persistence::{is_rdb_magic, load_aof_bytes, load_rdb_bytes_checked};
use crate::foundation::io::store::kv_engine::DbSet;
use crate::runtime::driver::{RuntimeCapabilities, RuntimeClock, RuntimeInstant};

use super::aof_frame::AofFrameEncoder;
use super::effects::EmbeddedCommit;
use super::output::EmbeddedOutput;
use super::scheduler;
use super::session::{EmbeddedEvent, EmbeddedSessionState};

pub(super) struct EmbeddedState {
    pub(super) dbs: DbSet,
    pub(super) session: EmbeddedSessionState,
    pub(super) pending: VecDeque<EmbeddedEvent>,
    pub(super) aof_frames: AofFrameEncoder,
    pub(super) wake_pending: bool,
    pub(super) stopped: bool,
    pub(super) clock: RuntimeClock,
    pub(super) capabilities: RuntimeCapabilities,
}

impl Default for EmbeddedState {
    fn default() -> Self {
        Self {
            dbs: DbSet::new(16),
            session: EmbeddedSessionState::default(),
            pending: VecDeque::new(),
            aof_frames: AofFrameEncoder::default(),
            wake_pending: false,
            stopped: false,
            clock: RuntimeClock::new(0, 0),
            capabilities: RuntimeCapabilities::embedded(),
        }
    }
}

impl EmbeddedState {
    pub(super) fn delete_expired(&mut self) {
        scheduler::delete_expired(&mut self.dbs, self.clock);
    }

    pub(super) fn next_deadline(&self, now: RuntimeClock) -> Option<RuntimeInstant> {
        scheduler::next_deadline(&self.dbs, now)
    }

    /// Finish a completed command. When persistence is enabled and the command
    /// produced a durable AOF frame, emit a `DurableReply` so the host appends
    /// the frame before releasing the reply; otherwise reply immediately.
    pub(super) fn finish_command(
        &mut self,
        request_id: u32,
        _session: SessionId,
        selected_db: u32,
        args: &CommandArgs,
        commit: EmbeddedCommit,
    ) -> EmbeddedOutput {
        let EmbeddedCommit { reply, effects } = commit;
        if self.capabilities.persistence {
            if let Some(effects) = effects {
                if let Some(frame) = self
                    .aof_frames
                    .frame_for(selected_db, args, &reply, &effects)
                {
                    return EmbeddedOutput::DurableReply {
                        request_id,
                        value: reply,
                        frame,
                    };
                }
            }
        }
        EmbeddedOutput::Reply {
            request_id,
            value: reply,
        }
    }

    /// Load a base RDB preamble into the dataset during startup recovery.
    ///
    /// Returns the number of bytes consumed, or `0` when `bytes` is not an RDB
    /// image. Version checking is relaxed so a snapshot written by a newer or
    /// foreign build still loads in the browser. No AOF frames are produced.
    pub(super) fn load_base_rdb(&mut self, bytes: &[u8]) -> Result<usize, String> {
        if !is_rdb_magic(bytes) {
            return Ok(0);
        }
        load_rdb_bytes_checked(bytes, &mut self.dbs, false)
            .map(|(_loaded, consumed)| consumed)
            .map_err(|error| format!("{error:?}"))
    }

    /// Replay an incremental AOF byte stream into the dataset during startup
    /// recovery. Each decoded command is applied through the shared embedded
    /// command runtime; `MULTI`/`EXEC` transaction markers are unwrapped since
    /// the enclosed commands are already individually deterministic. Replay
    /// never emits AOF frames, so loading does not re-persist.
    ///
    /// Returns the number of commands applied (including `SELECT`; the
    /// `MULTI`/`EXEC` markers are skipped).
    pub(super) fn replay_aof(&mut self, bytes: &[u8]) -> Result<u64, String> {
        let now_ms = self.clock.unix_ms().max(0) as u64;
        // Replay uses its own database cursor so recovery does not move the
        // live session's selected database.
        let mut replay_db = 0u32;
        let mut player = AofReplaySink {
            dbs: &mut self.dbs,
            selected_db: &mut replay_db,
            now_ms,
            applied: 0,
        };
        load_aof_bytes(bytes, &mut player)
            .map(|_stats| player.applied)
            .map_err(|error| format!("{error:?}"))
    }
}

/// Replay sink that applies decoded AOF commands through the embedded command
/// runtime. Transaction markers are skipped so only the deterministic inner
/// commands mutate the dataset.
struct AofReplaySink<'a> {
    dbs: &'a mut DbSet,
    selected_db: &'a mut u32,
    now_ms: u64,
    applied: u64,
}

impl crate::foundation::io::persistence::AofCommandPlayer for AofReplaySink<'_> {
    fn select_db(&mut self, dbid: u32) {
        *self.selected_db = dbid;
    }

    fn execute(&mut self, args: &[&[u8]]) -> Result<(), String> {
        let Some(name) = args.first() else {
            return Ok(());
        };
        // `MULTI`/`EXEC` only frame a group of already-deterministic commands
        // in the AOF stream; applying the inner commands directly reproduces
        // the same dataset without a transaction context.
        if name.eq_ignore_ascii_case(b"multi") || name.eq_ignore_ascii_case(b"exec") {
            return Ok(());
        }
        let parts = args
            .iter()
            .map(|part| bytes::Bytes::copy_from_slice(part))
            .collect();
        let command = CommandArgs::from_parts(parts);
        let mut runtime = crate::framework::commands::command_runtime::EmbeddedCommandRuntime::new(
            self.dbs,
            self.selected_db,
            self.now_ms,
        );
        let outcome = crate::framework::commands::command_runtime::execute(&mut runtime, &command);
        self.applied += 1;
        match outcome.reply {
            crate::engine::core::Value::Error(message) => {
                Err(String::from_utf8_lossy(&message).into_owned())
            }
            _ => Ok(()),
        }
    }
}
