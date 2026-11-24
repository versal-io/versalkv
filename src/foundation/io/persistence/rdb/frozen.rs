//! Owned point-in-time input for a background RDB serializer.

use super::serialize::{
    serialize_all_with_functions_and_replication_with_options, RdbReplicationInfo,
    RdbSerializeOptions,
};
use crate::foundation::io::persistence::snapshot::background::{FeedError, RdbSaveFeeder};
use crate::foundation::io::store::kv_engine::{DbSet, SnapshotValue};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

mod capture;
use capture::{EarlyCapture, EarlyCaptureReservation, FrozenCapture};

pub struct FrozenRdb {
    header: Vec<u8>,
    databases: Vec<FrozenDb>,
    total_keys: usize,
}

const LARGE_VALUE_HANDOFF_THRESHOLD: usize = 64 * 1024;

type ForklessDatabaseId = (Option<Vec<u8>>, u32);

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
    // Drop the reservation after the value and metadata, including on cancellation.
    _early_capture_reservation: Option<EarlyCaptureReservation>,
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
        let databases: Vec<_> = self
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
                early_captures: VecDeque::new(),
                is_header_sent: false,
                is_done: false,
            })
            .collect();
        let database_lookup = databases
            .iter()
            .enumerate()
            .map(|(index, database)| ((database.dataset_id.clone(), database.id), index))
            .collect();
        ForklessFeed {
            feeder,
            header: Some(self.header),
            databases,
            database_lookup,
            database_index: 0,
            pending: None,
            early_capture_budget: Arc::new(FrozenCapture::default()),
            is_output_backpressured: false,
            capture_ms,
            should_preserve_expired,
            queued_keys: Vec::new(),
            captured_keys: Vec::new(),
            resolved_keys: Vec::new(),
            priority_keys: HashMap::new(),
            priority_requested: HashSet::new(),
            priority_databases: VecDeque::new(),
            queued_priority_databases: HashSet::new(),
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
    database_lookup: HashMap<ForklessDatabaseId, usize>,
    database_index: usize,
    pending: Option<FrozenWorkItem>,
    early_capture_budget: Arc<FrozenCapture>,
    is_output_backpressured: bool,
    capture_ms: u64,
    should_preserve_expired: bool,
    queued_keys: Vec<ForklessItemId>,
    captured_keys: Vec<ForklessItemId>,
    resolved_keys: Vec<ForklessItemId>,
    priority_keys: HashMap<ForklessDatabaseId, VecDeque<ForklessItemId>>,
    priority_requested: HashSet<ForklessItemId>,
    priority_databases: VecDeque<ForklessDatabaseId>,
    queued_priority_databases: HashSet<ForklessDatabaseId>,
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
    early_captures: VecDeque<EarlyCapture>,
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
        if self.feeder.is_cancelled() {
            return false;
        }
        // Capture has a separate bounded quota so a full serializer queue
        // cannot prevent later databases from releasing their target writers.
        self.capture_future_priorities(primary, additional_datasets, budget.max(1));
        self.is_output_backpressured = false;
        let mut remaining = budget.max(1);
        let mut remaining_buckets = budget.max(1);
        while remaining > 0 {
            if let Some(item) = self.pending.take() {
                let is_database_header = matches!(item, FrozenWorkItem::Database { .. });
                match self.feeder.try_send(item) {
                    Ok(()) => {
                        if is_database_header {
                            self.databases[self.database_index].is_header_sent = true;
                        }
                    }
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
            if !self.ordinary_turn_pending {
                if let Some(mut capture) = database.early_captures.pop_front() {
                    remaining -= 1;
                    if let Some(entry) = capture.entry.take() {
                        match self.feeder.try_send(FrozenWorkItem::Entry(entry)) {
                            Ok(()) => {}
                            Err(FeedError::Full(item)) => {
                                let FrozenWorkItem::Entry(entry) = *item else {
                                    unreachable!("early capture only sends entries");
                                };
                                capture.entry = Some(entry);
                                database.early_captures.push_front(capture);
                                self.is_output_backpressured = true;
                                return false;
                            }
                            Err(FeedError::Closed) => return false,
                        }
                    }
                    self.ordinary_turn_pending = true;
                    continue;
                }
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
                    _early_capture_reservation: None,
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
                if database.early_captures.is_empty() {
                    self.database_index += 1;
                } else {
                    self.ordinary_turn_pending = false;
                }
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
                let id = (None, db_id);
                self.priority_keys
                    .entry(id.clone())
                    .or_default()
                    .push_back(item);
                self.queue_priority_database(id);
            }
        }
        has_keys
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

    pub(crate) fn is_backpressured(&self) -> bool {
        self.pending.is_some() || self.is_output_backpressured
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
        _early_capture_reservation: None,
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
                        _early_capture_reservation: None,
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
                            _early_capture_reservation: None,
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
                            _early_capture_reservation: None,
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
mod tests;
