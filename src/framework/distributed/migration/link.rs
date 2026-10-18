use std::collections::HashMap;

use crate::engine::distributed::cluster::migration::MigrationJobId;
#[derive(Clone)]
pub enum SlotExportReply {
    StatusOk,
    Error(String),
}

pub struct SlotExportLink {
    pub job_id: MigrationJobId,
    pub connection_id: usize,
    response: Vec<u8>,
    selected_db: Option<u32>,
    deferred_mutations: Vec<Vec<u8>>,
    deferred_mutation_bytes: usize,
    deferred_mutation_allocation_bytes: usize,
    last_ack_sent: i64,
}

impl SlotExportLink {
    pub fn new(job_id: MigrationJobId, connection_id: usize) -> Self {
        Self {
            job_id,
            connection_id,
            response: Vec::new(),
            selected_db: None,
            deferred_mutations: Vec::new(),
            deferred_mutation_bytes: 0,
            deferred_mutation_allocation_bytes: 0,
            last_ack_sent: 0,
        }
    }

    pub fn queue_mutation(&mut self, db_id: u32, frame: Vec<u8>, defer: bool) -> Vec<Vec<u8>> {
        let mut frames = Vec::new();
        if self.selected_db != Some(db_id) {
            frames.push(
                crate::engine::distributed::replication::protocol::encode_command(&[
                    b"SELECT",
                    db_id.to_string().as_bytes(),
                ]),
            );
            self.selected_db = Some(db_id);
        }
        frames.push(frame);
        if defer {
            let container_capacity = self.deferred_mutations.capacity();
            let frame_allocation = frames.iter().map(Vec::capacity).sum::<usize>();
            self.deferred_mutation_bytes = self
                .deferred_mutation_bytes
                .saturating_add(frames.iter().map(Vec::len).sum::<usize>());
            self.deferred_mutations.extend(frames);
            let container_growth = self
                .deferred_mutations
                .capacity()
                .saturating_sub(container_capacity)
                .saturating_mul(std::mem::size_of::<Vec<u8>>());
            self.deferred_mutation_allocation_bytes = self
                .deferred_mutation_allocation_bytes
                .saturating_add(frame_allocation)
                .saturating_add(container_growth);
            Vec::new()
        } else {
            frames
        }
    }

    pub fn take_deferred_mutations(&mut self) -> Vec<Vec<u8>> {
        self.deferred_mutation_bytes = 0;
        self.deferred_mutation_allocation_bytes = 0;
        std::mem::take(&mut self.deferred_mutations)
    }

    pub fn deferred_reply_bytes(&self) -> usize {
        self.deferred_mutation_bytes
    }

    pub fn deferred_output_memory_usage(&self) -> usize {
        self.deferred_mutation_allocation_bytes
    }

    pub fn on_status_bytes(&mut self, bytes: &[u8]) -> Vec<SlotExportReply> {
        self.response.extend_from_slice(bytes);
        let Some(end) = self
            .response
            .windows(2)
            .position(|window| window == b"\r\n")
        else {
            return Vec::new();
        };
        let line = self.response.drain(..end + 2).collect::<Vec<_>>();
        let result = if line.first() == Some(&b'+') {
            SlotExportReply::StatusOk
        } else {
            SlotExportReply::Error(
                String::from_utf8_lossy(
                    line.get(1..line.len().saturating_sub(2))
                        .unwrap_or_default(),
                )
                .into_owned(),
            )
        };
        vec![result]
    }
}

#[derive(Default)]
pub struct SlotMigrationRuntime {
    exports: HashMap<MigrationJobId, SlotExportLink>,
    export_by_connection: HashMap<usize, MigrationJobId>,
}

impl SlotMigrationRuntime {
    pub fn register_export(&mut self, link: SlotExportLink) {
        self.export_by_connection
            .insert(link.connection_id, link.job_id.clone());
        self.exports.insert(link.job_id.clone(), link);
    }

    pub fn is_export_connection(&self, connection_id: usize) -> bool {
        self.export_by_connection.contains_key(&connection_id)
    }

    pub fn export_mut(&mut self, connection_id: usize) -> Option<&mut SlotExportLink> {
        let id = self.export_by_connection.get(&connection_id)?.clone();
        self.exports.get_mut(&id)
    }

    pub fn export_connection(&self, id: &MigrationJobId) -> Option<usize> {
        self.exports.get(id).map(|link| link.connection_id)
    }

    pub fn queue_export_mutation(
        &mut self,
        id: &MigrationJobId,
        db_id: u32,
        frame: Vec<u8>,
        defer: bool,
    ) -> Vec<Vec<u8>> {
        self.exports
            .get_mut(id)
            .map(|link| link.queue_mutation(db_id, frame, defer))
            .unwrap_or_default()
    }

    pub fn take_deferred_mutations(&mut self, id: &MigrationJobId) -> Vec<Vec<u8>> {
        self.exports
            .get_mut(id)
            .map(SlotExportLink::take_deferred_mutations)
            .unwrap_or_default()
    }

    pub fn deferred_reply_bytes(&self, id: &MigrationJobId) -> usize {
        self.exports
            .get(id)
            .map(SlotExportLink::deferred_reply_bytes)
            .unwrap_or_default()
    }

    pub fn deferred_output_memory_usage(&self, id: &MigrationJobId) -> usize {
        self.exports
            .get(id)
            .map(SlotExportLink::deferred_output_memory_usage)
            .unwrap_or_default()
    }

    pub fn remove_connection(&mut self, connection_id: usize) -> Option<MigrationJobId> {
        let id = self.export_by_connection.remove(&connection_id)?;
        self.exports.remove(&id);
        Some(id)
    }

    pub fn remove_job(&mut self, id: &MigrationJobId) -> Option<usize> {
        if let Some(link) = self.exports.remove(id) {
            self.export_by_connection.remove(&link.connection_id);
            Some(link.connection_id)
        } else {
            None
        }
    }

    pub fn ack_due(&mut self, id: &MigrationJobId, now: i64) -> bool {
        let Some(link) = self.exports.get_mut(id) else {
            return false;
        };
        if link.last_ack_sent >= now {
            return false;
        }
        link.last_ack_sent = now;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::{SlotExportLink, SlotExportReply, SlotMigrationRuntime};
    use crate::engine::distributed::cluster::migration::MigrationJobId;

    fn export_link() -> SlotExportLink {
        let id = MigrationJobId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
        SlotExportLink::new(id, 7)
    }

    #[test]
    fn fragmented_status_reply_is_reassembled() {
        let mut link = export_link();
        assert!(link.on_status_bytes(b"+O").is_empty());
        assert!(matches!(
            link.on_status_bytes(b"K\r\n").as_slice(),
            [SlotExportReply::StatusOk]
        ));
    }

    #[test]
    fn deferred_output_tracks_logical_and_memory_projections() {
        let mut link = export_link();
        let mut frame = Vec::with_capacity(4096);
        frame.extend_from_slice(b"mutation");

        assert!(link.queue_mutation(0, frame, true).is_empty());

        let select =
            crate::engine::distributed::replication::protocol::encode_command(&[b"SELECT", b"0"]);
        let select_capacity = link.deferred_mutations[0].capacity();
        let mutation_capacity = link.deferred_mutations[1].capacity();
        assert_eq!(
            link.deferred_output_memory_usage(),
            select_capacity
                + mutation_capacity
                + link.deferred_mutations.capacity() * std::mem::size_of::<Vec<u8>>()
        );
        assert_eq!(
            link.deferred_reply_bytes(),
            select.len() + b"mutation".len()
        );

        assert!(link.queue_mutation(0, b"next".to_vec(), true).is_empty());
        assert_eq!(link.deferred_mutations.len(), 3);
        let next_mutation_capacity = link.deferred_mutations[2].capacity();
        assert_eq!(
            link.deferred_output_memory_usage(),
            select_capacity
                + mutation_capacity
                + next_mutation_capacity
                + link.deferred_mutations.capacity() * std::mem::size_of::<Vec<u8>>()
        );
        assert_eq!(
            link.deferred_reply_bytes(),
            select.len() + b"mutation".len() + b"next".len()
        );
    }

    #[test]
    fn taking_deferred_mutations_moves_frames_and_resets_projections_idempotently() {
        let mut link = export_link();
        let mut frame = Vec::with_capacity(4096);
        frame.extend_from_slice(b"mutation");
        assert!(link.queue_mutation(0, frame, true).is_empty());

        let frames = link.take_deferred_mutations();
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[1], b"mutation");
        assert_eq!(link.deferred_output_memory_usage(), 0);
        assert_eq!(link.deferred_reply_bytes(), 0);

        assert!(link.take_deferred_mutations().is_empty());
        assert_eq!(link.deferred_output_memory_usage(), 0);
        assert_eq!(link.deferred_reply_bytes(), 0);
    }

    #[test]
    fn acknowledgement_cadence_is_owned_by_the_export_lifetime() {
        let mut runtime = SlotMigrationRuntime::default();
        let link = export_link();
        let id = link.job_id.clone();
        runtime.register_export(link);

        assert!(runtime.ack_due(&id, 10));
        assert!(!runtime.ack_due(&id, 10));
        assert!(runtime.ack_due(&id, 11));

        runtime.remove_job(&id);
        assert!(!runtime.ack_due(&id, 12));
    }
}
