//! Owned point-in-time input for a background RDB serializer.

use super::serialize::{
    serialize_all_with_functions_and_replication_with_options, RdbReplicationInfo,
    RdbSerializeOptions,
};
use crate::foundation::io::persistence::snapshot::background::{FeedError, RdbSaveFeeder};
use crate::foundation::io::store::kv_engine::{DbSet, SnapshotValue};
use std::collections::{HashMap, HashSet, VecDeque};

pub struct FrozenRdb {
    header: Vec<u8>,
    databases: Vec<FrozenDb>,
    total_keys: usize,
}

const LARGE_VALUE_HANDOFF_THRESHOLD: usize = 64 * 1024;

struct FrozenDb {
    id: u32,
    dataset_id: Option<Vec<u8>>,
    entries: Vec<FrozenEntry>,
    total_entries: usize,
    expires: usize,
}

pub(crate) struct FrozenEntry {
    pub(crate) key: Vec<u8>,
    pub(crate) value: SnapshotValue,
    pub(crate) expire_ms: Option<u64>,
    pub(crate) hash_field_expires: Option<HashMap<Vec<u8>, u64>>,
    pub(crate) completion: Option<ForklessItemId>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ForklessItemId {
    pub(crate) dataset_id: Option<Vec<u8>>,
    pub(crate) db_id: u32,
    pub(crate) key: Vec<u8>,
}

pub(crate) enum FrozenWorkItem {
    Header(Vec<u8>),
    Database {
        id: u32,
        entries: usize,
        expires: usize,
    },
    Entry(FrozenEntry),
}

pub(crate) struct FrozenWorkItems {
    header: Option<Vec<u8>>,
    databases: std::vec::IntoIter<FrozenDb>,
    current_database: Option<FrozenDbWorkItems>,
}

struct FrozenDbWorkItems {
    id: u32,
    entries: std::vec::IntoIter<FrozenEntry>,
    entry_count: usize,
    expires: usize,
    is_header_pending: bool,
}

impl Iterator for FrozenWorkItems {
    type Item = FrozenWorkItem;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(header) = self.header.take() {
            return Some(FrozenWorkItem::Header(header));
        }
        loop {
            if let Some(database) = &mut self.current_database {
                if database.is_header_pending {
                    database.is_header_pending = false;
                    return Some(FrozenWorkItem::Database {
                        id: database.id,
                        entries: database.entry_count,
                        expires: database.expires,
                    });
                }
                if let Some(entry) = database.entries.next() {
                    return Some(FrozenWorkItem::Entry(entry));
                }
                self.current_database = None;
            }

            let database = self.databases.next()?;
            let entry_count = database.entries.len();
            self.current_database = Some(FrozenDbWorkItems {
                id: database.id,
                entries: database.entries.into_iter(),
                entry_count,
                expires: database.expires,
                is_header_pending: true,
            });
        }
    }
}

#[cfg(test)]
impl FrozenWorkItems {
    fn next_entry_storage_ptr(&self) -> Option<*const FrozenEntry> {
        self.current_database
            .as_ref()
            .map(|database| database.entries.as_slice())
            .or_else(|| {
                self.databases
                    .as_slice()
                    .first()
                    .map(|database| database.entries.as_slice())
            })
            .filter(|entries| !entries.is_empty())
            .map(<[FrozenEntry]>::as_ptr)
    }
}

impl FrozenRdb {
    pub(crate) fn capture_header(
        dbs: &DbSet,
        additional_datasets: &[(Vec<u8>, &DbSet)],
        function_codes: &[Vec<u8>],
        replication: Option<&RdbReplicationInfo>,
        options: RdbSerializeOptions,
    ) -> Self {
        let mut header = serialize_all_with_functions_and_replication_with_options(
            dbs,
            0,
            function_codes,
            false,
            replication,
            &[],
            options,
        );
        header.truncate(header.len() - 9);
        let mut databases = Vec::new();
        for (dataset_id, dataset) in std::iter::once((None, dbs)).chain(
            additional_datasets
                .iter()
                .map(|(id, dataset)| (Some(id.clone()), *dataset)),
        ) {
            databases.extend(dataset.iter_dbs().filter_map(|(id, db)| {
                let entries = db.dbsize();
                (entries > 0).then(|| FrozenDb {
                    id: id as u32,
                    dataset_id: dataset_id.clone(),
                    entries: Vec::new(),
                    total_entries: entries,
                    expires: db.expires_count(),
                })
            }));
        }
        Self {
            header,
            databases,
            total_keys: std::iter::once(dbs)
                .chain(additional_datasets.iter().map(|(_, dataset)| *dataset))
                .flat_map(|dataset| dataset.iter_dbs().map(|(_, db)| db.dbsize()))
                .sum(),
        }
    }

    pub(crate) fn into_incremental_feed(
        self,
        feeder: RdbSaveFeeder,
        capture_ms: u64,
        should_preserve_expired: bool,
    ) -> ForklessFeed {
        ForklessFeed {
            feeder,
            header: Some(self.header),
            databases: self
                .databases
                .into_iter()
                .map(|database| ForklessDatabaseFeed {
                    id: database.id,
                    current_id: database.id,
                    dataset_id: database.dataset_id,
                    total_entries: database.total_entries,
                    expires: database.expires,
                    cursor: 0,
                    keys: Vec::new(),
                    key_index: 0,
                    is_header_sent: false,
                    is_done: false,
                })
                .collect(),
            database_index: 0,
            pending: None,
            capture_ms,
            should_preserve_expired,
            queued_keys: Vec::new(),
            captured_keys: Vec::new(),
            resolved_keys: Vec::new(),
            priority_keys: HashMap::new(),
            priority_requested: HashSet::new(),
            scheduled_keys: HashSet::new(),
            ordinary_turn_pending: false,
            is_finished: false,
        }
    }

    pub(crate) fn into_work_items(self) -> FrozenWorkItems {
        let Self {
            header, databases, ..
        } = self;
        FrozenWorkItems {
            header: Some(header),
            databases: databases.into_iter(),
            current_database: None,
        }
    }
}

pub(crate) struct ForklessFeed {
    feeder: RdbSaveFeeder,
    header: Option<Vec<u8>>,
    databases: Vec<ForklessDatabaseFeed>,
    database_index: usize,
    pending: Option<FrozenWorkItem>,
    capture_ms: u64,
    should_preserve_expired: bool,
    queued_keys: Vec<ForklessItemId>,
    captured_keys: Vec<ForklessItemId>,
    resolved_keys: Vec<ForklessItemId>,
    priority_keys: HashMap<(Option<Vec<u8>>, u32), VecDeque<ForklessItemId>>,
    priority_requested: HashSet<ForklessItemId>,
    scheduled_keys: HashSet<ForklessItemId>,
    ordinary_turn_pending: bool,
    is_finished: bool,
}

struct ForklessDatabaseFeed {
    id: u32,
    current_id: u32,
    dataset_id: Option<Vec<u8>>,
    total_entries: usize,
    expires: usize,
    cursor: u64,
    keys: Vec<Vec<u8>>,
    key_index: usize,
    is_header_sent: bool,
    is_done: bool,
}

impl ForklessFeed {
    pub(crate) fn drive(
        &mut self,
        primary: &mut DbSet,
        additional_datasets: &mut [(Vec<u8>, &mut DbSet)],
        budget: usize,
    ) -> bool {
        let mut remaining = budget.max(1);
        let mut remaining_buckets = budget.max(1);
        while remaining > 0 {
            if let Some(item) = self.pending.take() {
                match self.feeder.try_send(item) {
                    Ok(()) => {}
                    Err(FeedError::Full(item)) => {
                        self.pending = Some(*item);
                        return false;
                    }
                    Err(FeedError::Closed) => return false,
                }
            }
            if let Some(header) = self.header.take() {
                match self.feeder.try_send(FrozenWorkItem::Header(header)) {
                    Ok(()) => {}
                    Err(FeedError::Full(item)) => {
                        self.pending = Some(*item);
                        return false;
                    }
                    Err(FeedError::Closed) => return false,
                }
                continue;
            }
            if !self.ordinary_turn_pending {
                if let Some((completion, current_db_id)) = self.pop_current_priority() {
                    remaining -= 1;
                    self.ordinary_turn_pending = true;
                    self.priority_requested.remove(&completion);
                    if self.scheduled_keys.remove(&completion) {
                        continue;
                    }
                    let Some(entry) = capture_forkless_entry(
                        primary,
                        additional_datasets,
                        &completion,
                        current_db_id,
                        self.capture_ms,
                        self.should_preserve_expired,
                    ) else {
                        self.scheduled_keys.insert(completion.clone());
                        self.captured_keys.push(completion.clone());
                        self.resolved_keys.push(completion);
                        continue;
                    };
                    self.scheduled_keys.insert(completion.clone());
                    self.captured_keys.push(completion.clone());
                    self.track_entry_handoff(completion, &entry.value);
                    match self.feeder.try_send(FrozenWorkItem::Entry(entry)) {
                        Ok(()) => {}
                        Err(FeedError::Full(item)) => {
                            self.pending = Some(*item);
                            return false;
                        }
                        Err(FeedError::Closed) => return false,
                    }
                    continue;
                }
            }
            let Some(database) = self.databases.get_mut(self.database_index) else {
                self.feeder.finish();
                self.is_finished = true;
                return true;
            };
            if !database.is_header_sent {
                let item = FrozenWorkItem::Database {
                    id: database.id,
                    entries: database.total_entries,
                    expires: database.expires,
                };
                match self.feeder.try_send(item) {
                    Ok(()) => database.is_header_sent = true,
                    Err(FeedError::Full(item)) => {
                        self.pending = Some(*item);
                        return false;
                    }
                    Err(FeedError::Closed) => return false,
                }
                continue;
            }
            if database.key_index == database.keys.len() && !database.is_done {
                if remaining_buckets == 0 {
                    return false;
                }
                let (cursor, keys) =
                    find_dataset(primary, additional_datasets, database.dataset_id.as_deref())
                        .and_then(|dataset| dataset.get(database.current_id))
                        .map_or((0, Vec::new()), |db| {
                            db.snapshot_scan_bucket(database.cursor)
                        });
                remaining_buckets -= 1;
                database.cursor = cursor;
                database.keys = keys;
                database.key_index = 0;
                database.is_done = cursor == 0;
                if self.ordinary_turn_pending {
                    self.ordinary_turn_pending = false;
                    remaining -= 1;
                    continue;
                }
            }
            if let Some(key) = database.keys.get(database.key_index).cloned() {
                database.key_index += 1;
                self.ordinary_turn_pending = false;
                remaining -= 1;
                let completion = ForklessItemId {
                    dataset_id: database.dataset_id.clone(),
                    db_id: database.id,
                    key: key.clone(),
                };
                let was_priority = self.priority_requested.remove(&completion);
                if self.scheduled_keys.remove(&completion) {
                    continue;
                }
                let Some(dataset) =
                    find_dataset_mut(primary, additional_datasets, database.dataset_id.as_deref())
                else {
                    continue;
                };
                let Some(db) = dataset.get_mut(database.current_id) else {
                    continue;
                };
                let expire_ms = db.get_expire(&key);
                if !self.should_preserve_expired
                    && expire_ms.is_some_and(|expire| expire <= self.capture_ms)
                {
                    continue;
                }
                let Some(value) =
                    db.snapshot_value_for_background(&key, LARGE_VALUE_HANDOFF_THRESHOLD)
                else {
                    continue;
                };
                let entry = FrozenEntry {
                    key: key.clone(),
                    value,
                    expire_ms,
                    hash_field_expires: db.hash_field_expires_for(&key).cloned(),
                    completion: Some(completion.clone()),
                };
                self.captured_keys.push(completion.clone());
                if was_priority {
                    self.scheduled_keys.insert(completion.clone());
                }
                // Ordinary iteration keeps the C-compatible in-use marker.
                // Only independent early copies may release waiting writers.
                self.queued_keys.push(completion);
                match self.feeder.try_send(FrozenWorkItem::Entry(entry)) {
                    Ok(()) => {}
                    Err(FeedError::Full(item)) => {
                        self.pending = Some(*item);
                        return false;
                    }
                    Err(FeedError::Closed) => return false,
                }
                continue;
            }
            if database.is_done {
                self.database_index += 1;
            }
        }
        false
    }

    fn track_entry_handoff(&mut self, item: ForklessItemId, value: &SnapshotValue) {
        // Shared values still require the serializer to release live storage.
        // Independent copies can release writers as soon as they are captured.
        match value {
            SnapshotValue::Shared(_) => self.queued_keys.push(item),
            SnapshotValue::Owned(_) => self.resolved_keys.push(item),
        }
    }

    pub(crate) fn take_queued_keys(&mut self) -> Vec<ForklessItemId> {
        std::mem::take(&mut self.queued_keys)
    }

    pub(crate) fn take_captured_keys(&mut self) -> Vec<ForklessItemId> {
        std::mem::take(&mut self.captured_keys)
    }

    pub(crate) fn take_resolved_keys(&mut self) -> Vec<ForklessItemId> {
        std::mem::take(&mut self.resolved_keys)
    }

    fn pop_current_priority(&mut self) -> Option<(ForklessItemId, u32)> {
        let database = self.databases.get(self.database_index)?;
        if !database.is_header_sent {
            return None;
        }
        let id = (database.dataset_id.clone(), database.id);
        let current_db_id = database.current_id;
        let completion = self.priority_keys.get_mut(&id)?.pop_front();
        if self.priority_keys.get(&id).is_some_and(VecDeque::is_empty) {
            self.priority_keys.remove(&id);
        }
        completion.map(|item| (item, current_db_id))
    }

    pub(crate) fn prioritize_keys(
        &mut self,
        db_id: u32,
        keys: impl IntoIterator<Item = Vec<u8>>,
    ) -> bool {
        let can_capture_now = self
            .databases
            .get(self.database_index)
            .is_some_and(|database| database.dataset_id.is_none() && database.id == db_id);
        let mut has_keys = false;
        for key in keys {
            has_keys = true;
            let item = ForklessItemId {
                dataset_id: None,
                db_id,
                key,
            };
            if !self.scheduled_keys.contains(&item) && self.priority_requested.insert(item.clone())
            {
                self.priority_keys
                    .entry((None, db_id))
                    .or_default()
                    .push_back(item);
            }
        }
        has_keys && can_capture_now
    }

    pub(crate) fn swap_databases(&mut self, first: u32, second: u32) {
        if first == second {
            return;
        }
        for database in &mut self.databases {
            if database.dataset_id.is_some() {
                continue;
            }
            if database.current_id == first {
                database.current_id = second;
            } else if database.current_id == second {
                database.current_id = first;
            }
        }
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.is_finished
    }
}

fn capture_forkless_entry(
    primary: &mut DbSet,
    additional_datasets: &mut [(Vec<u8>, &mut DbSet)],
    completion: &ForklessItemId,
    current_db_id: u32,
    capture_ms: u64,
    should_preserve_expired: bool,
) -> Option<FrozenEntry> {
    let dataset = find_dataset_mut(
        primary,
        additional_datasets,
        completion.dataset_id.as_deref(),
    )?;
    let db = dataset.get_mut(current_db_id)?;
    let expire_ms = db.get_expire(&completion.key);
    if !should_preserve_expired && expire_ms.is_some_and(|expire| expire <= capture_ms) {
        return None;
    }
    let value = db.snapshot_value_for_background(&completion.key, LARGE_VALUE_HANDOFF_THRESHOLD)?;
    Some(FrozenEntry {
        key: completion.key.clone(),
        value,
        expire_ms,
        hash_field_expires: db.hash_field_expires_for(&completion.key).cloned(),
        completion: Some(completion.clone()),
    })
}

impl FrozenRdb {
    /// Capture the primary dataset while promoting only large values at the
    /// point where they cross into the background save.
    pub fn capture_primary_with_background_handoff(
        dbs: &mut DbSet,
        function_codes: &[Vec<u8>],
        replication: Option<&RdbReplicationInfo>,
        options: RdbSerializeOptions,
        capture_ms: u64,
        should_preserve_expired: bool,
    ) -> Self {
        let mut frozen = Self {
            header: serialize_all_with_functions_and_replication_with_options(
                dbs,
                0,
                function_codes,
                false,
                replication,
                &[],
                options,
            ),
            databases: Vec::new(),
            total_keys: 0,
        };
        frozen.header.truncate(frozen.header.len() - 9);

        for (id, db) in dbs.iter_dbs_mut() {
            let mut entries = Vec::new();
            let mut expires = 0;
            let mut cursor = 0;
            loop {
                let (next, keys) = db.snapshot_scan_bucket(cursor);
                for key in keys {
                    let expire_ms = db.get_expire(&key);
                    if !should_preserve_expired && expire_ms.is_some_and(|t| t <= capture_ms) {
                        continue;
                    }
                    let Some(value) =
                        db.snapshot_value_for_background(&key, LARGE_VALUE_HANDOFF_THRESHOLD)
                    else {
                        continue;
                    };
                    expires += usize::from(expire_ms.is_some());
                    entries.push(FrozenEntry {
                        key: key.clone(),
                        value,
                        expire_ms,
                        hash_field_expires: db.hash_field_expires_for(&key).cloned(),
                        completion: None,
                    });
                }
                if next == 0 {
                    break;
                }
                cursor = next;
            }
            if !entries.is_empty() {
                let total_entries = entries.len();
                frozen.total_keys += total_entries;
                frozen.databases.push(FrozenDb {
                    id: id as u32,
                    dataset_id: None,
                    entries,
                    total_entries,
                    expires,
                });
            }
        }
        frozen
    }

    pub fn append_additional_datasets(
        &mut self,
        additional_datasets: &[&DbSet],
        capture_ms: u64,
        should_preserve_expired: bool,
    ) {
        for dataset in additional_datasets {
            for (id, db) in dataset.iter_dbs() {
                let mut entries = Vec::new();
                let mut expires = 0;
                db.for_each_entry_including_expired_with_hash_field_expires(
                    |key, value, expire_ms, fields| {
                        if !should_preserve_expired && expire_ms.is_some_and(|t| t <= capture_ms) {
                            return;
                        }
                        expires += usize::from(expire_ms.is_some());
                        entries.push(FrozenEntry {
                            key: key.to_vec(),
                            value: SnapshotValue::Owned(value.clone()),
                            expire_ms,
                            hash_field_expires: fields.cloned(),
                            completion: None,
                        });
                    },
                );
                if !entries.is_empty() {
                    let total_entries = entries.len();
                    self.total_keys += total_entries;
                    self.databases.push(FrozenDb {
                        id: id as u32,
                        dataset_id: Some(Vec::new()),
                        entries,
                        total_entries,
                        expires,
                    });
                }
            }
        }
    }

    pub fn capture(
        dbs: &DbSet,
        additional_datasets: &[&DbSet],
        function_codes: &[Vec<u8>],
        replication: Option<&RdbReplicationInfo>,
        options: RdbSerializeOptions,
        capture_ms: u64,
        should_preserve_expired: bool,
    ) -> Self {
        // The no-key serializer supplies the shared metadata/function header.
        // Its only remaining bytes are the EOF marker and eight-byte trailer.
        let mut header = serialize_all_with_functions_and_replication_with_options(
            dbs,
            0,
            function_codes,
            false,
            replication,
            &[],
            options,
        );
        header.truncate(header.len() - 9);
        let mut databases = Vec::new();
        let mut total_keys = 0;
        for dataset in std::iter::once(dbs).chain(additional_datasets.iter().copied()) {
            for (id, db) in dataset.iter_dbs() {
                let mut entries = Vec::new();
                let mut expires = 0;
                db.for_each_entry_including_expired_with_hash_field_expires(
                    |key, value, expire_ms, fields| {
                        if !should_preserve_expired && expire_ms.is_some_and(|t| t <= capture_ms) {
                            return;
                        }
                        expires += usize::from(expire_ms.is_some());
                        entries.push(FrozenEntry {
                            key: key.to_vec(),
                            value: SnapshotValue::Owned(value.clone()),
                            expire_ms,
                            hash_field_expires: fields.cloned(),
                            completion: None,
                        });
                    },
                );
                if !entries.is_empty() {
                    let total_entries = entries.len();
                    total_keys += total_entries;
                    databases.push(FrozenDb {
                        id: id as u32,
                        dataset_id: None,
                        entries,
                        total_entries,
                        expires,
                    });
                }
            }
        }
        Self {
            header,
            databases,
            total_keys,
        }
    }

    pub fn total_keys(&self) -> usize {
        self.total_keys
    }
}

fn find_dataset<'a>(
    primary: &'a DbSet,
    additional_datasets: &'a [(Vec<u8>, &mut DbSet)],
    dataset_id: Option<&[u8]>,
) -> Option<&'a DbSet> {
    match dataset_id {
        None => Some(primary),
        Some(id) => additional_datasets
            .iter()
            .find(|(candidate, _)| candidate.as_slice() == id)
            .map(|(_, dataset)| &**dataset),
    }
}

fn find_dataset_mut<'a>(
    primary: &'a mut DbSet,
    additional_datasets: &'a mut [(Vec<u8>, &mut DbSet)],
    dataset_id: Option<&[u8]>,
) -> Option<&'a mut DbSet> {
    match dataset_id {
        None => Some(primary),
        Some(id) => additional_datasets
            .iter_mut()
            .find(|(candidate, _)| candidate.as_slice() == id)
            .map(|(_, dataset)| &mut **dataset),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::io::persistence::rdb::compression::{
        RdbCompression, RdbSnapshotOptions,
    };
    use crate::foundation::io::persistence::snapshot::background::BackgroundRdbSave;
    use crate::foundation::io::store::structure::list::ListValue;
    use crate::foundation::io::store::Robj;
    use bytes::Bytes;

    #[test]
    fn work_items_are_materialized_lazily() {
        let first_entries = (0..1_024)
            .map(|index| FrozenEntry {
                key: index.to_string().into_bytes(),
                value: SnapshotValue::Owned(Robj::string_raw(Bytes::from_static(b"value"))),
                expire_ms: None,
                hash_field_expires: None,
                completion: None,
            })
            .collect();
        let second_entries = vec![FrozenEntry {
            key: b"tail".to_vec(),
            value: SnapshotValue::Owned(Robj::string_raw(Bytes::from_static(b"value"))),
            expire_ms: Some(42),
            hash_field_expires: None,
            completion: None,
        }];
        let frozen = FrozenRdb {
            header: vec![1, 2, 3],
            databases: vec![
                FrozenDb {
                    id: 7,
                    dataset_id: None,
                    entries: first_entries,
                    total_entries: 1_024,
                    expires: 0,
                },
                FrozenDb {
                    id: 9,
                    dataset_id: None,
                    entries: second_entries,
                    total_entries: 1,
                    expires: 1,
                },
            ],
            total_keys: 1_025,
        };
        let first_entry_storage = frozen.databases[0].entries.as_ptr();
        let second_entry_storage = frozen.databases[1].entries.as_ptr();

        let mut items = frozen.into_work_items();
        assert_eq!(items.next_entry_storage_ptr(), Some(first_entry_storage));
        assert!(matches!(items.next(), Some(FrozenWorkItem::Header(_))));
        assert!(matches!(
            items.next(),
            Some(FrozenWorkItem::Database {
                id: 7,
                entries: 1_024,
                expires: 0
            })
        ));
        assert_eq!(items.next_entry_storage_ptr(), Some(first_entry_storage));
        for index in 0..1_024 {
            let Some(FrozenWorkItem::Entry(entry)) = items.next() else {
                panic!("expected an entry");
            };
            assert_eq!(entry.key, index.to_string().into_bytes());
        }
        assert!(matches!(
            items.next(),
            Some(FrozenWorkItem::Database {
                id: 9,
                entries: 1,
                expires: 1
            })
        ));
        assert_eq!(items.next_entry_storage_ptr(), Some(second_entry_storage));
        let Some(FrozenWorkItem::Entry(entry)) = items.next() else {
            panic!("expected the final entry");
        };
        assert_eq!(entry.key, b"tail");
        assert!(items.next().is_none());
    }

    #[test]
    fn incremental_header_includes_slot_import_dataset_metadata() {
        let mut serving = DbSet::new(1);
        serving.get_mut(0).unwrap().set(
            b"serving".to_vec(),
            Robj::string_raw(Bytes::from_static(b"value")),
        );

        let mut importing = DbSet::new(1);
        importing.get_mut(0).unwrap().load_entry(
            b"imported".to_vec(),
            Robj::string_raw(Bytes::from_static(b"value")),
            Some(9_999_999_999_999),
        );

        let frozen = FrozenRdb::capture_header(
            &serving,
            &[(b"import".to_vec(), &importing)],
            &[],
            None,
            RdbSerializeOptions::default(),
        );

        assert_eq!(frozen.total_keys(), 2);
        assert_eq!(frozen.databases.len(), 2);
        assert_eq!(frozen.databases[0].dataset_id, None);
        assert_eq!(frozen.databases[0].total_entries, 1);
        assert_eq!(
            frozen.databases[1].dataset_id.as_deref(),
            Some(b"import".as_slice())
        );
        assert_eq!(frozen.databases[1].total_entries, 1);
        assert_eq!(frozen.databases[1].expires, 1);
    }

    #[test]
    fn incremental_feed_limits_dictionary_scanning_per_drive() {
        let mut dbs = DbSet::new(1);
        for index in 0..4_096 {
            dbs.get_mut(0).unwrap().set(
                format!("key-{index}").into_bytes(),
                Robj::string_raw(Bytes::from_static(b"value")),
            );
        }
        let frozen =
            FrozenRdb::capture_header(&dbs, &[], &[], None, RdbSerializeOptions::default());
        let dir = std::env::temp_dir().join(format!(
            "versal-forkless-scan-budget-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let (mut task, feeder) = BackgroundRdbSave::start_incremental(
            frozen.total_keys(),
            dir.clone(),
            "dump.rdb".to_string(),
            1,
            0,
            false,
            RdbSnapshotOptions {
                compression: RdbCompression::No,
                checksum: true,
            },
        )
        .unwrap();
        let mut feed = frozen.into_incremental_feed(feeder, 0, true);
        let mut additional_datasets = Vec::new();

        assert!(!feed.drive(&mut dbs, &mut additional_datasets, 1));
        assert!(
            !feed.databases[0].is_done,
            "one drive must not scan the complete dictionary"
        );

        task.control().cancel();
        let _ = task.finish();
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn ordinary_scan_does_not_retain_scheduled_key_copies() {
        let mut dbs = DbSet::new(1);
        dbs.get_mut(0).unwrap().set(
            b"ordinary".to_vec(),
            Robj::string_raw(Bytes::from_static(b"value")),
        );
        let frozen =
            FrozenRdb::capture_header(&dbs, &[], &[], None, RdbSerializeOptions::default());
        let dir = std::env::temp_dir().join(format!(
            "versal-forkless-ordinary-scheduled-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let (mut task, feeder) = BackgroundRdbSave::start_incremental(
            frozen.total_keys(),
            dir.clone(),
            "dump.rdb".to_string(),
            1,
            0,
            false,
            RdbSnapshotOptions {
                compression: RdbCompression::No,
                checksum: true,
            },
        )
        .unwrap();
        let mut feed = frozen.into_incremental_feed(feeder, 0, true);

        assert!(!feed.drive(&mut dbs, &mut [], 1));
        assert!(feed.scheduled_keys.is_empty());

        drop(feed);
        task.control().cancel();
        let _ = task.finish();
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn priority_keys_only_bypass_scanning_for_the_current_database() {
        let mut dbs = DbSet::new(2);
        dbs.get_mut(0).unwrap().set(
            b"current".to_vec(),
            Robj::string_raw(Bytes::from_static(b"value")),
        );
        dbs.get_mut(1).unwrap().set(
            b"later".to_vec(),
            Robj::string_raw(Bytes::from_static(b"value")),
        );
        let frozen =
            FrozenRdb::capture_header(&dbs, &[], &[], None, RdbSerializeOptions::default());
        let dir = std::env::temp_dir().join(format!(
            "versal-forkless-priority-db-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let (mut task, feeder) = BackgroundRdbSave::start_incremental(
            frozen.total_keys(),
            dir.clone(),
            "dump.rdb".to_string(),
            1,
            0,
            false,
            RdbSnapshotOptions {
                compression: RdbCompression::No,
                checksum: true,
            },
        )
        .unwrap();
        let mut feed = frozen.into_incremental_feed(feeder, 0, true);

        assert!(feed.prioritize_keys(0, [b"current".to_vec()]));
        assert!(!feed.prioritize_keys(1, [b"later".to_vec()]));
        assert!(!feed.prioritize_keys(0, std::iter::empty()));

        drop(feed);
        task.control().cancel();
        let _ = task.finish();
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn priority_queue_resolves_current_database_behind_future_database_request() {
        let mut dbs = DbSet::new(2);
        dbs.get_mut(0).unwrap().set(
            b"current-filler".to_vec(),
            Robj::string_raw(Bytes::from_static(b"value")),
        );
        dbs.get_mut(1).unwrap().set(
            b"future-filler".to_vec(),
            Robj::string_raw(Bytes::from_static(b"value")),
        );
        let frozen =
            FrozenRdb::capture_header(&dbs, &[], &[], None, RdbSerializeOptions::default());
        let dir = std::env::temp_dir().join(format!(
            "versal-forkless-priority-order-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let (mut task, feeder) = BackgroundRdbSave::start_incremental(
            frozen.total_keys(),
            dir.clone(),
            "dump.rdb".to_string(),
            1,
            0,
            false,
            RdbSnapshotOptions {
                compression: RdbCompression::No,
                checksum: true,
            },
        )
        .unwrap();
        let mut feed = frozen.into_incremental_feed(feeder, 0, true);

        assert!(!feed.drive(&mut dbs, &mut [], 1));
        let future = ForklessItemId {
            dataset_id: None,
            db_id: 1,
            key: b"future-missing".to_vec(),
        };
        let current = ForklessItemId {
            dataset_id: None,
            db_id: 0,
            key: b"current-missing".to_vec(),
        };
        assert!(!feed.prioritize_keys(1, [future.key.clone()]));
        assert!(feed.prioritize_keys(0, [current.key.clone()]));

        assert!(!feed.drive(&mut dbs, &mut [], 1));
        assert!(
            feed.take_resolved_keys().contains(&current),
            "a future-database request must not block a current-database priority key"
        );

        drop(feed);
        task.control().cancel();
        let _ = task.finish();
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn priority_keys_resolve_entries_absent_from_the_snapshot() {
        let mut dbs = DbSet::new(1);
        dbs.get_mut(0).unwrap().set(
            b"existing".to_vec(),
            Robj::string_raw(Bytes::from_static(b"value")),
        );
        let frozen =
            FrozenRdb::capture_header(&dbs, &[], &[], None, RdbSerializeOptions::default());
        let dir = std::env::temp_dir().join(format!(
            "versal-forkless-priority-missing-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let (mut task, feeder) = BackgroundRdbSave::start_incremental(
            frozen.total_keys(),
            dir.clone(),
            "dump.rdb".to_string(),
            1,
            0,
            false,
            RdbSnapshotOptions {
                compression: RdbCompression::No,
                checksum: true,
            },
        )
        .unwrap();
        let mut feed = frozen.into_incremental_feed(feeder, 0, true);
        let missing = ForklessItemId {
            dataset_id: None,
            db_id: 0,
            key: b"missing".to_vec(),
        };

        assert!(feed.prioritize_keys(0, [missing.key.clone()]));
        assert!(!feed.drive(&mut dbs, &mut [], 1));
        assert_eq!(feed.take_resolved_keys(), vec![missing.clone()]);
        assert!(feed.scheduled_keys.contains(&missing));

        drop(feed);
        task.control().cancel();
        let _ = task.finish();
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn priority_shared_key_waits_for_background_serialization() {
        let mut dbs = DbSet::new(1);
        dbs.get_mut(0).unwrap().set(
            b"owned".to_vec(),
            Robj::ListQuicklist(ListValue::from_vecdeque(
                (0..8_192).map(|_| Bytes::from_static(b"value")).collect(),
            )),
        );
        let frozen =
            FrozenRdb::capture_header(&dbs, &[], &[], None, RdbSerializeOptions::default());
        let dir = std::env::temp_dir().join(format!(
            "versal-forkless-priority-owned-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let (mut task, feeder) = BackgroundRdbSave::start_incremental(
            frozen.total_keys(),
            dir.clone(),
            "dump.rdb".to_string(),
            1,
            10_000_000,
            false,
            RdbSnapshotOptions {
                compression: RdbCompression::No,
                checksum: true,
            },
        )
        .unwrap();
        let mut feed = frozen.into_incremental_feed(feeder, 0, true);
        let owned = ForklessItemId {
            dataset_id: None,
            db_id: 0,
            key: b"owned".to_vec(),
        };

        assert!(feed.prioritize_keys(0, [owned.key.clone()]));
        assert!(!feed.drive(&mut dbs, &mut [], 1));
        assert!(feed.take_resolved_keys().is_empty());
        assert_eq!(feed.take_queued_keys(), vec![owned]);

        drop(feed);
        task.control().cancel();
        let _ = task.finish();
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn priority_owned_key_releases_writer_before_background_serialization() {
        let mut dbs = DbSet::new(1);
        dbs.get_mut(0).unwrap().set(
            b"owned".to_vec(),
            Robj::string_raw(Bytes::from_static(b"value")),
        );
        let frozen =
            FrozenRdb::capture_header(&dbs, &[], &[], None, RdbSerializeOptions::default());
        let dir = std::env::temp_dir().join(format!(
            "versal-forkless-priority-owned-release-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let (mut task, feeder) = BackgroundRdbSave::start_incremental(
            frozen.total_keys(),
            dir.clone(),
            "dump.rdb".to_string(),
            1,
            10_000_000,
            false,
            RdbSnapshotOptions {
                compression: RdbCompression::No,
                checksum: true,
            },
        )
        .unwrap();
        let mut feed = frozen.into_incremental_feed(feeder, 0, true);
        let owned = ForklessItemId {
            dataset_id: None,
            db_id: 0,
            key: b"owned".to_vec(),
        };

        assert!(feed.prioritize_keys(0, [owned.key.clone()]));
        assert!(!feed.drive(&mut dbs, &mut [], 1));
        let resolved = feed.take_resolved_keys();
        let queued = feed.take_queued_keys();
        let processed = task.control().progress().processed;
        drop(feed);
        task.control().cancel();
        let _ = task.finish();
        let _ = std::fs::remove_dir_all(dir);

        assert_eq!(resolved, vec![owned]);
        assert!(queued.is_empty());
        assert_eq!(processed, 0);
    }

    #[test]
    fn missing_priority_work_consumes_the_priority_budget() {
        let mut dbs = DbSet::new(1);
        dbs.get_mut(0).unwrap().set(
            b"ordinary".to_vec(),
            Robj::string_raw(Bytes::from_static(b"value")),
        );
        let frozen =
            FrozenRdb::capture_header(&dbs, &[], &[], None, RdbSerializeOptions::default());
        let dir = std::env::temp_dir().join(format!(
            "versal-forkless-priority-budget-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let (mut task, feeder) = BackgroundRdbSave::start_incremental(
            frozen.total_keys(),
            dir.clone(),
            "dump.rdb".to_string(),
            1,
            0,
            false,
            RdbSnapshotOptions {
                compression: RdbCompression::No,
                checksum: true,
            },
        )
        .unwrap();
        let mut feed = frozen.into_incremental_feed(feeder, 0, true);
        let missing_keys = (0..3).map(|index| format!("missing-{index}").into_bytes());

        assert!(feed.prioritize_keys(0, missing_keys));
        assert!(!feed.drive(&mut dbs, &mut [], 1));

        assert_eq!(feed.take_resolved_keys().len(), 1);
        assert_eq!(
            feed.priority_keys.get(&(None, 0)).map_or(0, VecDeque::len),
            2
        );

        drop(feed);
        task.control().cancel();
        let _ = task.finish();
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn full_priority_budget_still_advances_the_ordinary_scan() {
        let mut dbs = DbSet::new(1);
        let keys: Vec<Vec<u8>> = (0..4)
            .map(|index| format!("key-{index}").into_bytes())
            .collect();
        for key in &keys {
            dbs.get_mut(0)
                .unwrap()
                .set(key.clone(), Robj::string_raw(Bytes::from_static(b"value")));
        }
        let frozen =
            FrozenRdb::capture_header(&dbs, &[], &[], None, RdbSerializeOptions::default());
        let dir = std::env::temp_dir().join(format!(
            "versal-forkless-priority-fairness-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let (mut task, feeder) = BackgroundRdbSave::start_incremental(
            frozen.total_keys(),
            dir.clone(),
            "dump.rdb".to_string(),
            1,
            0,
            false,
            RdbSnapshotOptions {
                compression: RdbCompression::No,
                checksum: true,
            },
        )
        .unwrap();
        let mut feed = frozen.into_incremental_feed(feeder, 0, true);

        assert!(feed.prioritize_keys(0, keys));
        assert!(!feed.drive(&mut dbs, &mut [], 1));
        assert!(!feed.drive(&mut dbs, &mut [], 1));

        let database = &feed.databases[0];
        assert!(
            database.cursor != 0
                || !database.keys.is_empty()
                || database.key_index != 0
                || database.is_done,
            "priority capture must preserve ordinary scan progress"
        );

        drop(feed);
        task.control().cancel();
        let _ = task.finish();
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn ordinary_scan_does_not_recapture_a_queued_priority_key() {
        let mut dbs = DbSet::new(1);
        for index in 0..4 {
            dbs.get_mut(0).unwrap().set(
                format!("key-{index}").into_bytes(),
                Robj::string_raw(Bytes::from_static(b"value")),
            );
        }
        let frozen =
            FrozenRdb::capture_header(&dbs, &[], &[], None, RdbSerializeOptions::default());
        let dir = std::env::temp_dir().join(format!(
            "versal-forkless-priority-stale-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let (mut task, feeder) = BackgroundRdbSave::start_incremental(
            frozen.total_keys(),
            dir.clone(),
            "dump.rdb".to_string(),
            1,
            0,
            false,
            RdbSnapshotOptions {
                compression: RdbCompression::No,
                checksum: true,
            },
        )
        .unwrap();
        let mut feed = frozen.into_incremental_feed(feeder, 0, true);

        assert!(!feed.drive(&mut dbs, &mut [], 1));
        let database = &feed.databases[0];
        let ordinary_key = database.keys[database.key_index].clone();
        let first_priority = database.keys[database.key_index + 1].clone();
        assert!(feed.prioritize_keys(0, [first_priority.clone(), ordinary_key.clone()]));

        assert!(!feed.drive(&mut dbs, &mut [], 1));
        assert!(!feed.drive(&mut dbs, &mut [], 1));
        assert!(!feed.drive(&mut dbs, &mut [], 1));

        let captured = feed
            .take_captured_keys()
            .into_iter()
            .filter(|item| item.key == ordinary_key)
            .count();
        assert_eq!(captured, 1);

        drop(feed);
        task.control().cancel();
        let _ = task.finish();
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn background_handoff_promotes_large_values_without_copying_them() {
        let mut dbs = DbSet::new(1);
        dbs.get_mut(0).unwrap().set(
            b"large".to_vec(),
            Robj::ListQuicklist(ListValue::from_vecdeque(
                (0..8_192).map(|_| Bytes::from_static(b"value")).collect(),
            )),
        );
        let live_ptr = dbs.get(0).unwrap().get_allow_expired(b"large").unwrap();
        let Robj::ListQuicklist(live) = live_ptr else {
            panic!("expected a list")
        };
        let live_storage = live.as_slices().0.as_ptr();

        let frozen = FrozenRdb::capture_primary_with_background_handoff(
            &mut dbs,
            &[],
            None,
            RdbSerializeOptions::default(),
            0,
            true,
        );
        let entry = &frozen.databases[0].entries[0];
        assert!(entry.value.is_shared());
        let SnapshotValue::Shared(value) = &entry.value else {
            unreachable!("large value must use the shared handoff variant");
        };
        let Robj::ListQuicklist(saved) = value.as_ref() else {
            panic!("expected a list")
        };
        assert_eq!(live_storage, saved.as_slices().0.as_ptr());
    }
}
