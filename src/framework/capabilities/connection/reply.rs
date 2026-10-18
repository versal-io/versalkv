//! Per-connection reply delivery policy.
//! CLIENT REPLY mode state advances at the command boundary.

#[derive(Default)]
pub(super) struct ReplyPolicy {
    pub(super) off: bool,
    pub(super) skip: bool,
    pub(super) skip_next: bool,
}

impl ReplyPolicy {
    pub(super) fn should_suppress_current(&self, command_suppressed: bool) -> bool {
        self.off || self.skip || command_suppressed
    }

    pub(super) fn finish_command(&mut self) {
        let skip_next = self.skip_next;
        self.skip = false;
        if skip_next {
            self.skip = true;
            self.skip_next = false;
        }
    }
}
