use std::collections::VecDeque;

use crate::engine::command::{CommandArgs, SessionId};
use crate::foundation::io::store::kv_engine::DbSet;
use crate::runtime::driver::{RuntimeCapabilities, RuntimeClock, RuntimeInstant};

use super::aof_frame::AofFrameEncoder;
use super::effects::EmbeddedCommit;
use super::output::EmbeddedOutput;
use super::persistence::EmbeddedPersistenceState;
use super::scheduler;
use super::session::{EmbeddedEvent, EmbeddedSessionState};

pub(super) struct EmbeddedState {
    pub(super) dbs: DbSet,
    pub(super) session: EmbeddedSessionState,
    pub(super) pending: VecDeque<EmbeddedEvent>,
    /// Outputs produced out-of-band from the pending command queue, such as
    /// durable replies released by a host completion.
    pub(super) completed: VecDeque<EmbeddedOutput>,
    pub(super) persistence: EmbeddedPersistenceState,
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
            completed: VecDeque::new(),
            persistence: EmbeddedPersistenceState::default(),
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

    /// Finish a completed command: either reply immediately or, when the
    /// command must be persisted and persistence is enabled, withhold the
    /// reply behind a durable host request.
    pub(super) fn finish_command(
        &mut self,
        request_id: u32,
        session: SessionId,
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
                    let append = self
                        .persistence
                        .enqueue_append(session, request_id, reply, frame);
                    return EmbeddedOutput::HostRequest {
                        operation: append.operation,
                        request: append.request,
                    };
                }
            }
        }
        EmbeddedOutput::Reply {
            request_id,
            value: reply,
        }
    }
}
