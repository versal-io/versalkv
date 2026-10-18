use super::{
    ExportPhase, ImportPhase, MigrationAction, MigrationError, MigrationEvent, MigrationJob,
    MigrationJobId, MigrationJobView, MigrationPhase, MigrationProtocolMessage, TerminalPhase,
};

#[derive(Default)]
pub struct MigrationRegistry {
    jobs: Vec<MigrationJob>,
}

impl MigrationRegistry {
    pub fn start(&mut self, job: MigrationJob) -> Result<Vec<MigrationAction>, MigrationError> {
        self.validate_start(&job, &[])?;
        let action = match job.operation {
            super::MigrationOperation::Export => Some(MigrationAction::Connect(job.id.clone())),
            super::MigrationOperation::Import => {
                Some(MigrationAction::CreateImport(job.id.clone()))
            }
        };
        // Match Valkey's listAddNodeHead: operator views are newest-first so a
        // completed historical job cannot shadow a newer job for the same slot.
        self.jobs.insert(0, job);
        Ok(action.into_iter().collect())
    }

    pub fn start_all(
        &mut self,
        jobs: Vec<MigrationJob>,
    ) -> Result<Vec<MigrationAction>, MigrationError> {
        for (index, job) in jobs.iter().enumerate() {
            self.validate_start(job, &jobs[..index])?;
        }
        let mut actions = Vec::with_capacity(jobs.len());
        for job in jobs {
            actions.extend(self.start(job)?);
        }
        Ok(actions)
    }

    fn validate_start(
        &self,
        job: &MigrationJob,
        pending: &[MigrationJob],
    ) -> Result<(), MigrationError> {
        if self
            .jobs
            .iter()
            .any(|current| current.is_active() && current.id == job.id)
            || pending
                .iter()
                .any(|current| current.is_active() && current.id == job.id)
        {
            return Err(MigrationError::DuplicateJob(job.id.clone()));
        }
        let overlapping_slot = self
            .jobs
            .iter()
            .filter(|current| current.is_active() && current.operation == job.operation)
            .chain(
                pending
                    .iter()
                    .filter(|current| current.operation == job.operation),
            )
            .find_map(|current| current.ranges.overlaps(&job.ranges));
        if let Some(slot) = overlapping_slot {
            return Err(MigrationError::ActiveSlotOverlap { slot });
        }
        Ok(())
    }

    pub fn transition(
        &mut self,
        id: &MigrationJobId,
        event: MigrationEvent,
        now: i64,
    ) -> Result<Vec<MigrationAction>, MigrationError> {
        let job = self
            .jobs
            .iter_mut()
            .find(|job| &job.id == id)
            .ok_or_else(|| MigrationError::UnknownJob(id.clone()))?;
        let (next, actions) = next_phase(job, event).ok_or(MigrationError::InvalidTransition {
            phase: job.phase.as_str(),
            event: event.as_str(),
        })?;
        job.phase = next;
        job.last_update_time = now;
        if event == MigrationEvent::Ack {
            job.last_ack_time = now.max(job.last_ack_time);
        }
        Ok(actions)
    }

    pub fn record_ack(
        &mut self,
        id: &MigrationJobId,
        now: i64,
    ) -> Result<Vec<MigrationAction>, MigrationError> {
        let should_transition = self
            .get(id)
            .ok_or_else(|| MigrationError::UnknownJob(id.clone()))?
            .phase
            == MigrationPhase::Import(ImportPhase::WaitingForAck);
        if should_transition {
            return self.transition(id, MigrationEvent::Ack, now);
        }
        let job = self
            .jobs
            .iter_mut()
            .find(|job| &job.id == id)
            .ok_or_else(|| MigrationError::UnknownJob(id.clone()))?;
        if job.is_active() {
            job.last_ack_time = now.max(job.last_ack_time);
        }
        Ok(Vec::new())
    }

    pub fn set_remaining_repl_size(&mut self, id: &MigrationJobId, bytes: usize) {
        if let Some(job) = self.jobs.iter_mut().find(|job| &job.id == id) {
            job.remaining_repl_size = bytes;
        }
    }

    pub fn finish(
        &mut self,
        id: &MigrationJobId,
        terminal: TerminalPhase,
        message: Option<String>,
        now: i64,
    ) -> Result<Vec<MigrationAction>, MigrationError> {
        let job = self
            .jobs
            .iter_mut()
            .find(|job| &job.id == id)
            .ok_or_else(|| MigrationError::UnknownJob(id.clone()))?;
        if !job.is_active() {
            return Ok(Vec::new());
        }
        job.phase = MigrationPhase::Terminal(terminal);
        job.last_update_time = now;
        job.message = message;
        job.remaining_repl_size = 0;
        Ok(vec![MigrationAction::Cleanup(id.clone())])
    }

    pub fn cancel_exports(&mut self, now: i64) -> Vec<MigrationAction> {
        let active: Vec<_> = self
            .jobs
            .iter()
            .filter(|job| job.is_active() && job.operation == super::MigrationOperation::Export)
            .map(|job| job.id.clone())
            .collect();
        active
            .into_iter()
            .flat_map(|id| {
                self.finish(&id, TerminalPhase::Cancelled, None, now)
                    .unwrap_or_default()
            })
            .collect()
    }

    pub fn fail_all(&mut self, message: &str, now: i64) -> Vec<MigrationAction> {
        let active = self
            .jobs
            .iter()
            .filter(|job| job.is_active())
            .map(|job| job.id.clone())
            .collect::<Vec<_>>();
        active
            .into_iter()
            .flat_map(|id| {
                self.finish(&id, TerminalPhase::Failed, Some(message.to_string()), now)
                    .unwrap_or_default()
            })
            .collect()
    }

    pub fn fail_imports(&mut self, message: &str, now: i64) -> Vec<MigrationAction> {
        let active = self
            .jobs
            .iter()
            .filter(|job| job.is_active() && job.operation == super::MigrationOperation::Import)
            .map(|job| job.id.clone())
            .collect::<Vec<_>>();
        active
            .into_iter()
            .flat_map(|id| {
                self.finish(&id, TerminalPhase::Failed, Some(message.to_string()), now)
                    .unwrap_or_default()
            })
            .collect()
    }

    pub fn track_imports_after_demotion(&mut self, now: i64) -> Vec<MigrationJobId> {
        let mut ids = Vec::new();
        for job in self.jobs.iter_mut().filter(|job| {
            job.is_active()
                && job.operation == super::MigrationOperation::Import
                && job.phase != MigrationPhase::Import(super::ImportPhase::OccurringOnPrimary)
        }) {
            job.phase = MigrationPhase::Import(super::ImportPhase::OccurringOnPrimary);
            job.last_update_time = now;
            ids.push(job.id.clone());
        }
        ids
    }

    pub fn trim_history(&mut self, max_len: usize) {
        let terminal_count = self.jobs.iter().filter(|job| !job.is_active()).count();
        let mut to_remove = terminal_count.saturating_sub(max_len);
        while to_remove != 0 {
            let Some(index) = self.jobs.iter().rposition(|job| !job.is_active()) else {
                break;
            };
            self.jobs.remove(index);
            to_remove -= 1;
        }
    }

    pub fn views(&self) -> Vec<MigrationJobView> {
        self.jobs.iter().map(MigrationJob::view).collect()
    }

    pub fn active_export_views(&self) -> Vec<MigrationJobView> {
        self.jobs
            .iter()
            .filter(|job| job.is_active() && job.operation == super::MigrationOperation::Export)
            .map(MigrationJob::view)
            .collect()
    }

    pub fn get(&self, id: &MigrationJobId) -> Option<&MigrationJob> {
        self.jobs.iter().find(|job| &job.id == id)
    }

    pub fn has_active_exports(&self) -> bool {
        self.jobs
            .iter()
            .any(|job| job.is_active() && job.operation == super::MigrationOperation::Export)
    }

    pub fn has_active_imports(&self) -> bool {
        self.jobs
            .iter()
            .any(|job| job.is_active() && job.operation == super::MigrationOperation::Import)
    }

    pub fn active_overlap(&self, ranges: &super::SlotRanges) -> Option<u16> {
        self.jobs
            .iter()
            .filter(|job| job.is_active())
            .find_map(|job| job.ranges.overlaps(ranges))
    }

    pub fn active_import_overlap(&self, ranges: &super::SlotRanges) -> Option<u16> {
        self.jobs
            .iter()
            .filter(|job| job.is_active_import())
            .find_map(|job| job.ranges.overlaps(ranges))
    }

    pub fn paused_export_for_slot(&self, slot: u16) -> Option<MigrationJobId> {
        self.jobs.iter().find_map(|job| {
            (job.operation == super::MigrationOperation::Export
                && matches!(
                    job.phase,
                    MigrationPhase::Export(
                        ExportPhase::FailoverPaused | ExportPhase::FailoverGranted
                    )
                )
                && job.ranges.contains(slot))
            .then(|| job.id.clone())
        })
    }
}

fn next_phase(
    job: &MigrationJob,
    event: MigrationEvent,
) -> Option<(MigrationPhase, Vec<MigrationAction>)> {
    use ExportPhase as E;
    use ImportPhase as I;
    use MigrationAction as A;
    use MigrationEvent as V;
    use MigrationPhase as P;
    use MigrationProtocolMessage as M;

    let id = || job.id.clone();
    match (job.phase, event) {
        (
            P::Export(E::Connecting),
            V::Connected {
                auth_required: true,
            },
        ) => Some((P::Export(E::SendingAuthCommand), vec![A::SendAuth(id())])),
        (
            P::Export(E::Connecting),
            V::Connected {
                auth_required: false,
            },
        ) => Some((
            P::Export(E::SendingEstablishCommand),
            vec![A::SendEstablish(id())],
        )),
        (P::Export(E::SendingAuthCommand), V::AuthCommandSent) => {
            Some((P::Export(E::ReadingAuthResponse), Vec::new()))
        }
        (P::Export(E::ReadingAuthResponse), V::AuthAccepted) => Some((
            P::Export(E::SendingEstablishCommand),
            vec![A::SendEstablish(id())],
        )),
        (P::Export(E::SendingEstablishCommand), V::EstablishCommandSent) => {
            Some((P::Export(E::ReadingEstablishResponse), Vec::new()))
        }
        (P::Export(E::ReadingEstablishResponse), V::Established) => Some((
            P::Export(E::WaitingToSnapshot),
            vec![A::Send(id(), M::Ack), A::StartSnapshot(id())],
        )),
        (P::Export(E::WaitingToSnapshot), V::SnapshotStarted) => {
            Some((P::Export(E::Snapshotting), Vec::new()))
        }
        (P::Export(E::Snapshotting), V::SnapshotFinished) => {
            Some((P::Export(E::Streaming), Vec::new()))
        }
        (P::Export(E::Snapshotting | E::Streaming), V::PauseRequested) => {
            Some((P::Export(E::WaitingToPause), Vec::new()))
        }
        (P::Export(E::WaitingToPause), V::OutputDrained) => {
            Some((P::Export(E::FailoverPaused), vec![A::Send(id(), M::Paused)]))
        }
        (P::Export(E::FailoverPaused), V::FailoverGranted) => Some((
            P::Export(E::FailoverGranted),
            vec![A::Send(id(), M::FailoverGranted)],
        )),
        (P::Export(E::FailoverGranted), V::OwnershipCommitted) => {
            Some((P::Terminal(TerminalPhase::Success), vec![A::Cleanup(id())]))
        }
        (P::Import(I::WaitingForAck), V::Ack) => {
            Some((P::Import(I::ReceivingSnapshot), Vec::new()))
        }
        (P::Import(I::ReceivingSnapshot), V::SnapshotEof) => Some((
            P::Import(I::WaitingForPaused),
            vec![A::Send(id(), M::RequestPause)],
        )),
        (P::Import(I::WaitingForPaused), V::Paused) => Some((
            P::Import(I::FailoverRequested),
            vec![A::Send(id(), M::RequestFailover)],
        )),
        (P::Import(I::FailoverRequested), V::FailoverGranted) => {
            Some((P::Import(I::FailoverGranted), Vec::new()))
        }
        (P::Import(I::FailoverGranted), V::OwnershipCommitted) => {
            Some((P::Terminal(TerminalPhase::Success), vec![A::Cleanup(id())]))
        }
        (P::Import(I::OccurringOnPrimary), V::OwnershipCommitted) => {
            Some((P::Terminal(TerminalPhase::Success), vec![A::Cleanup(id())]))
        }
        _ => None,
    }
}
