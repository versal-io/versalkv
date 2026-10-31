use std::fmt;

use crate::engine::distributed::cluster::node::generate_node_id;
use crate::engine::distributed::cluster::{NodeId, NodeIdError};

use super::{ExportPhase, ImportPhase, MigrationPhase, SlotRanges};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MigrationJobId(String);

impl MigrationJobId {
    pub fn generate() -> Self {
        // Batch creation may outpace the clock granularity used by node IDs.
        static NEXT_SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let sequence = NEXT_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Self(format!("{}{:016x}", &generate_node_id()[..24], sequence))
    }

    pub fn parse(value: &[u8]) -> Result<Self, NodeIdError> {
        NodeId::parse(value).map(|id| Self(id.as_str().to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for MigrationJobId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MigrationOperation {
    Import,
    Export,
}

impl MigrationOperation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Import => "IMPORT",
            Self::Export => "EXPORT",
        }
    }
}

#[derive(PartialEq, Eq)]
pub struct MigrationAuth {
    username: Vec<u8>,
    password: Vec<u8>,
}

impl MigrationAuth {
    pub fn username(&self) -> &[u8] {
        &self.username
    }

    pub fn password(&self) -> &[u8] {
        &self.password
    }

    fn clear(&mut self) {
        for bytes in [&mut self.username, &mut self.password] {
            bytes.fill(0);
            // Keep the overwrite observable before releasing the allocation.
            std::hint::black_box(bytes.as_mut_slice());
        }
        std::sync::atomic::compiler_fence(std::sync::atomic::Ordering::SeqCst);
    }
}

impl fmt::Debug for MigrationAuth {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MigrationAuth((redacted))")
    }
}

impl Drop for MigrationAuth {
    fn drop(&mut self) {
        self.clear();
    }
}

// Only credential-free views are cloneable.
#[derive(Debug, PartialEq, Eq)]
pub struct MigrationJob {
    pub id: MigrationJobId,
    pub source: NodeId,
    pub target: NodeId,
    pub ranges: SlotRanges,
    pub operation: MigrationOperation,
    pub phase: MigrationPhase,
    pub create_time: i64,
    pub last_update_time: i64,
    pub last_ack_time: i64,
    pub message: Option<String>,
    pub cow_size: u64,
    pub remaining_repl_size: usize,
    auth: Option<MigrationAuth>,
}

impl MigrationJob {
    pub fn new(
        id: MigrationJobId,
        source: NodeId,
        target: NodeId,
        ranges: SlotRanges,
        operation: MigrationOperation,
        now: i64,
    ) -> Self {
        let phase = match operation {
            MigrationOperation::Import => MigrationPhase::Import(ImportPhase::WaitingForAck),
            MigrationOperation::Export => MigrationPhase::Export(ExportPhase::Connecting),
        };
        Self {
            id,
            source,
            target,
            ranges,
            operation,
            phase,
            create_time: now,
            last_update_time: now,
            last_ack_time: now,
            message: None,
            cow_size: 0,
            remaining_repl_size: 0,
            auth: None,
        }
    }

    pub fn set_auth(&mut self, username: &[u8], password: &[u8]) {
        self.auth = Some(MigrationAuth {
            username: username.to_vec(),
            password: password.to_vec(),
        });
    }

    pub fn has_auth(&self) -> bool {
        self.auth.is_some()
    }

    pub fn take_auth(&mut self) -> Option<MigrationAuth> {
        self.auth.take()
    }

    pub fn clear_auth(&mut self) {
        self.auth = None;
    }

    pub fn is_active(&self) -> bool {
        !self.phase.is_terminal()
    }

    /// Returns true for import jobs that are actively importing on this node.
    /// `OccurringOnPrimary` is a passive tracking state on replicas and is not
    /// considered importing for overlap/ownership purposes (matches C's
    /// `clusterIsSlotImporting` which excludes the occurring-on-primary state).
    pub fn is_active_import(&self) -> bool {
        self.operation == super::MigrationOperation::Import
            && self.is_active()
            && !matches!(
                self.phase,
                super::MigrationPhase::Import(super::ImportPhase::OccurringOnPrimary)
            )
    }

    pub fn new_tracking_import(
        id: MigrationJobId,
        source: NodeId,
        target: NodeId,
        ranges: SlotRanges,
        now: i64,
    ) -> Self {
        let mut job = Self::new(id, source, target, ranges, MigrationOperation::Import, now);
        job.phase = MigrationPhase::Import(ImportPhase::OccurringOnPrimary);
        job
    }

    pub fn view(&self) -> MigrationJobView {
        MigrationJobView {
            id: self.id.clone(),
            source: self.source.clone(),
            target: self.target.clone(),
            ranges: self.ranges.clone(),
            operation: self.operation,
            phase: self.phase,
            create_time: self.create_time,
            last_update_time: self.last_update_time,
            last_ack_time: self.last_ack_time,
            message: self.message.clone(),
            cow_size: self.cow_size,
            remaining_repl_size: self.remaining_repl_size,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MigrationJobView {
    pub id: MigrationJobId,
    pub source: NodeId,
    pub target: NodeId,
    pub ranges: SlotRanges,
    pub operation: MigrationOperation,
    pub phase: MigrationPhase,
    pub create_time: i64,
    pub last_update_time: i64,
    pub last_ack_time: i64,
    pub message: Option<String>,
    pub cow_size: u64,
    pub remaining_repl_size: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::distributed::cluster::SlotRange;

    fn job() -> MigrationJob {
        MigrationJob::new(
            MigrationJobId::generate(),
            NodeId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap(),
            NodeId::parse(b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap(),
            SlotRanges::new(vec![SlotRange::new(0, 0).unwrap()]).unwrap(),
            MigrationOperation::Export,
            1,
        )
    }

    #[test]
    fn auth_preserves_binary_bytes_and_explicitly_overwrites_both_allocations() {
        let mut job = job();
        job.set_auth(b"user\0\xff", b"password\r\n\0\xfe");
        let mut auth = job.take_auth().unwrap();
        assert_eq!(auth.username(), b"user\0\xff");
        assert_eq!(auth.password(), b"password\r\n\0\xfe");
        assert!(!job.has_auth());
        auth.clear();
        assert_eq!(auth.username(), &[0; 6]);
        assert_eq!(auth.password(), &[0; 12]);
    }

    #[test]
    fn debug_and_cloneable_views_exclude_credentials() {
        let mut job = job();
        job.set_auth(b"private-user", b"private-password");
        for text in [
            format!("{job:?}"),
            format!("{:?}", job.view().clone()),
            format!("{:?}", job.take_auth().unwrap()),
        ] {
            assert!(!text.contains("private-user"));
            assert!(!text.contains("private-password"));
            assert!(!text.contains("112, 114, 105"));
        }
    }

    #[test]
    fn replacing_and_clearing_auth_release_owned_credentials() {
        let mut job = job();
        job.set_auth(b"first", b"password1");
        job.set_auth(b"", b"");
        let auth = job.take_auth().unwrap();
        assert_eq!(auth.username(), b"");
        assert_eq!(auth.password(), b"");
        drop(auth);
        job.set_auth(b"second", b"password2");
        job.clear_auth();
        assert!(!job.has_auth());
        assert!(job.take_auth().is_none());
    }

    #[test]
    fn generated_batch_job_ids_are_distinct_and_parseable() {
        let mut ids = std::collections::HashSet::new();
        for _ in 0..1024 {
            let id = MigrationJobId::generate();
            assert_eq!(MigrationJobId::parse(id.as_str().as_bytes()).unwrap(), id);
            assert!(ids.insert(id));
        }
    }

    #[test]
    fn generated_job_ids_are_unique_across_concurrent_batches() {
        let workers: Vec<_> = (0..8)
            .map(|_| {
                std::thread::spawn(|| {
                    (0..256)
                        .map(|_| MigrationJobId::generate())
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        let mut ids = std::collections::HashSet::new();
        for worker in workers {
            for id in worker.join().unwrap() {
                assert!(ids.insert(id));
            }
        }
        assert_eq!(ids.len(), 2048);
    }
}
