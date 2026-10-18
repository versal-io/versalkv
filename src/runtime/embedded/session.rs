use crate::engine::command::{CommandArgs, SessionId};

pub enum EmbeddedEvent {
    Execute {
        request_id: u32,
        session: SessionId,
        args: CommandArgs,
    },
}

#[derive(Default)]
pub(super) struct EmbeddedSessionState {
    pub(super) selected_db: u32,
}
