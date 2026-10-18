use crate::engine::distributed::cluster::migration::{
    MigrationJobId, MigrationJobView, MigrationOperation, SlotRanges,
};
use crate::engine::distributed::cluster::NodeId;

pub(crate) const SLOT_IMPORT_AUX_KEY: &[u8] = b"cluster-slot-imports";

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SlotImportMetadata {
    pub id: MigrationJobId,
    pub source: NodeId,
    pub target: NodeId,
    pub ranges: SlotRanges,
}

impl SlotImportMetadata {
    pub fn from_job(job: &MigrationJobView) -> Option<Self> {
        (job.operation == MigrationOperation::Import && !job.phase.is_terminal()).then(|| Self {
            id: job.id.clone(),
            source: job.source.clone(),
            target: job.target.clone(),
            ranges: job.ranges.clone(),
        })
    }

    pub fn encode(&self) -> Vec<u8> {
        format!(
            "v1|{}|{}|{}|{}",
            self.id, self.source, self.target, self.ranges
        )
        .into_bytes()
    }

    pub fn decode(value: &[u8]) -> Result<Self, String> {
        let value = std::str::from_utf8(value).map_err(|_| "invalid slot import AUX")?;
        let mut fields = value.split('|');
        if fields.next() != Some("v1") {
            return Err("unsupported slot import AUX version".to_string());
        }
        let id = MigrationJobId::parse(
            fields
                .next()
                .ok_or("missing slot import job id")?
                .as_bytes(),
        )
        .map_err(|_| "invalid slot import job id")?;
        let source = NodeId::parse(
            fields
                .next()
                .ok_or("missing slot import source")?
                .as_bytes(),
        )
        .map_err(|_| "invalid slot import source")?;
        let target = NodeId::parse(
            fields
                .next()
                .ok_or("missing slot import target")?
                .as_bytes(),
        )
        .map_err(|_| "invalid slot import target")?;
        let ranges = fields.next().ok_or("missing slot import ranges")?;
        if fields.next().is_some() {
            return Err("unexpected slot import AUX field".to_string());
        }
        let mut pairs = Vec::new();
        for range in ranges.split_ascii_whitespace() {
            let (start, end) = range.split_once('-').ok_or("invalid slot import range")?;
            pairs.push(start.as_bytes());
            pairs.push(end.as_bytes());
        }
        let ranges = SlotRanges::parse_pairs(&pairs)
            .map_err(|_| "invalid slot import ranges".to_string())?;
        Ok(Self {
            id,
            source,
            target,
            ranges,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::SlotImportMetadata;
    use crate::engine::distributed::cluster::migration::{
        MigrationJob, MigrationJobId, MigrationOperation, SlotRanges,
    };
    use crate::engine::distributed::cluster::{NodeId, SlotRange};

    #[test]
    fn slot_import_aux_round_trips_multiple_ranges() {
        let job = MigrationJob::new(
            MigrationJobId::parse(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap(),
            NodeId::parse(b"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap(),
            NodeId::parse(b"cccccccccccccccccccccccccccccccccccccccc").unwrap(),
            SlotRanges::new(vec![
                SlotRange::new(1, 2).unwrap(),
                SlotRange::new(9, 11).unwrap(),
            ])
            .unwrap(),
            MigrationOperation::Import,
            1,
        );
        let metadata = SlotImportMetadata::from_job(&job.view()).unwrap();

        assert_eq!(SlotImportMetadata::decode(&metadata.encode()), Ok(metadata));
    }
}
