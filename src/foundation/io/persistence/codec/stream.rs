use super::CodecLoad;
use crate::foundation::io::persistence::codec::decoder;
use crate::foundation::io::persistence::codec::decoder_compact::listpack_decode;
use crate::foundation::io::persistence::codec::types::{
    load_len, RDB_TYPE_STREAM_LISTPACKS, RDB_TYPE_STREAM_LISTPACKS_2, RDB_TYPE_STREAM_LISTPACKS_3,
};
use crate::foundation::io::persistence::codec::RdbWriter;
use crate::foundation::io::store::structure::stream::groups::{
    RestoredStreamConsumer, RestoredStreamGroup, RestoredStreamPendingEntry,
};
use crate::foundation::io::store::structure::stream::{StreamID, ValkeyStream};
use crate::foundation::io::store::structure::string::encoding::int_encodable;
use crate::foundation::io::store::Robj;
use bytes::Bytes;
use std::collections::{HashMap, HashSet};

const STREAM_DUMP_MARKER_V1: &[u8] = b"VALKEY_RUST_STREAM_V1";
const STREAM_DUMP_MARKER_V2: &[u8] = b"VALKEY_RUST_STREAM_V2";

pub struct StreamCodec;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StreamLoadError {
    DuplicateConsumer,
    DuplicateConsumerPel,
    Invalid,
}

impl CodecLoad for StreamCodec {
    fn load_dispatch(buf: &[u8], pos: &mut usize, type_byte: u8) -> Option<Robj> {
        let stream = if Self::is_legacy_encoding(buf, *pos, type_byte) {
            StreamCodec::load_legacy_listpacks(buf, pos, type_byte)?
        } else {
            StreamCodec::load(buf, pos)?
        };
        Some(Robj::StreamListpack(Box::new(stream)))
    }
}

impl StreamCodec {
    pub(crate) fn is_legacy_encoding(buf: &[u8], pos: usize, type_byte: u8) -> bool {
        if type_byte == RDB_TYPE_STREAM_LISTPACKS || type_byte == RDB_TYPE_STREAM_LISTPACKS_2 {
            return true;
        }
        if type_byte != RDB_TYPE_STREAM_LISTPACKS_3 {
            return false;
        }
        let has_owned_marker = buf.get(pos) == Some(&(STREAM_DUMP_MARKER_V2.len() as u8))
            && buf
                .get(pos.saturating_add(1)..pos.saturating_add(1 + STREAM_DUMP_MARKER_V2.len()))
                .is_some_and(|marker| {
                    marker == STREAM_DUMP_MARKER_V1 || marker == STREAM_DUMP_MARKER_V2
                });
        !has_owned_marker
    }

    pub fn save<W: RdbWriter>(stream: &ValkeyStream, w: &mut W) {
        w.write_string(STREAM_DUMP_MARKER_V2);
        w.write_len(stream.len() as u64);
        for (id, fields) in stream.iter() {
            w.write_string(&encode_stream_id(id));
            w.write_len(fields.len() as u64);
            for (field, value) in fields {
                w.write_string(field);
                w.write_string(value);
            }
        }

        w.write_len(stream.groups().count() as u64);
        for group in stream.groups() {
            w.write_string(&group.name);
            w.write_string(&encode_stream_id(&group.last_id));
            write_optional_u64(w, group.entries_read);

            w.write_len(group.consumers.len() as u64);
            for consumer in group.consumers.values() {
                w.write_string(&consumer.name);
                w.write_signed_len(consumer.seen_time_ms);
                write_optional_i64(w, consumer.active_time_ms);
                w.write_len(consumer.pending.len() as u64);
                for id in &consumer.pending {
                    w.write_string(&encode_stream_id(id));
                }
            }

            w.write_len(group.pending.len() as u64);
            for (id, pending) in &group.pending {
                w.write_string(&encode_stream_id(id));
                w.write_string(&pending.consumer);
                w.write_signed_len(pending.delivery_time_ms);
                w.write_len(pending.delivery_count);
            }
        }
    }

    fn load(buf: &[u8], pos: &mut usize) -> Option<ValkeyStream> {
        let marker = decoder::load_string(buf, pos)?;
        if marker != STREAM_DUMP_MARKER_V1 && marker != STREAM_DUMP_MARKER_V2 {
            return None;
        }

        let (entry_count, encoded) = load_len(buf, pos)?;
        if encoded {
            return None;
        }

        let mut stream = ValkeyStream::new();
        let mut entry_ids = HashSet::with_capacity(entry_count as usize);
        for _ in 0..entry_count {
            let id = decode_stream_id(&decoder::load_string(buf, pos)?)?;
            if !entry_ids.insert(id) {
                return None;
            }

            let (field_count, encoded) = load_len(buf, pos)?;
            if encoded {
                return None;
            }
            let mut fields = Vec::with_capacity(field_count as usize);
            for _ in 0..field_count {
                let field = Bytes::from(decoder::load_string(buf, pos)?);
                let value = Bytes::from(decoder::load_string(buf, pos)?);
                fields.push((field, value));
            }
            stream.append(id, fields, true, false, 0).ok()?;
        }

        if marker == STREAM_DUMP_MARKER_V1 {
            load_v1_pending_ids(buf, pos, &entry_ids)?;
            return Some(stream);
        }

        let (group_count, encoded) = load_len(buf, pos)?;
        if encoded {
            return None;
        }
        for _ in 0..group_count {
            let group = load_group(buf, pos)?;
            stream.restore_group(group).ok()?;
        }

        Some(stream)
    }

    fn load_legacy_listpacks(buf: &[u8], pos: &mut usize, type_byte: u8) -> Option<ValkeyStream> {
        Self::load_legacy_listpacks_detailed(buf, pos, type_byte).ok()
    }

    pub(crate) fn load_legacy_listpacks_detailed(
        buf: &[u8],
        pos: &mut usize,
        type_byte: u8,
    ) -> Result<ValkeyStream, StreamLoadError> {
        let has_offsets = type_byte >= RDB_TYPE_STREAM_LISTPACKS_2;
        let mut cursor = LegacyStreamCursor::new(buf, *pos).ok_or(StreamLoadError::Invalid)?;
        let mut builder = StreamRestoreBuilder::new();
        decode_legacy_listpack_nodes(&mut cursor, &mut builder).ok_or(StreamLoadError::Invalid)?;
        decode_legacy_metadata(&mut cursor, &mut builder, has_offsets)
            .ok_or(StreamLoadError::Invalid)?;
        decode_legacy_groups(
            &mut cursor,
            &mut builder,
            has_offsets,
            type_byte >= RDB_TYPE_STREAM_LISTPACKS_3,
        )?;
        builder
            .validate_cross_references()
            .ok_or(StreamLoadError::Invalid)?;
        let stream = builder.commit().ok_or(StreamLoadError::Invalid)?;
        *pos = cursor.position();
        Ok(stream)
    }
}

struct LegacyStreamCursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> LegacyStreamCursor<'a> {
    fn new(buf: &'a [u8], pos: usize) -> Option<Self> {
        (pos <= buf.len()).then_some(Self { buf, pos })
    }

    fn position(&self) -> usize {
        self.pos
    }

    fn read_plain_len(&mut self) -> Option<u64> {
        load_plain_len(self.buf, &mut self.pos)
    }

    fn read_rdb_string(&mut self) -> Option<Vec<u8>> {
        decoder::load_string(self.buf, &mut self.pos)
    }

    fn read_plain_stream_id(&mut self) -> Option<StreamID> {
        Some(StreamID::new(
            self.read_plain_len()?,
            self.read_plain_len()?,
        ))
    }

    fn read_stream_id_raw(&mut self) -> Option<StreamID> {
        let end = self.pos.checked_add(16)?;
        let raw = self.buf.get(self.pos..end)?;
        self.pos = end;
        decode_stream_id(raw)
    }

    fn read_i64_raw(&mut self) -> Option<i64> {
        let end = self.pos.checked_add(8)?;
        let raw: [u8; 8] = self.buf.get(self.pos..end)?.try_into().ok()?;
        self.pos = end;
        Some(i64::from_le_bytes(raw))
    }
}

struct LegacyStreamEntry {
    id: StreamID,
    fields: Vec<(Bytes, Bytes)>,
}

struct DecodedLegacyListpack {
    entry_ids: Vec<StreamID>,
    live_entries: Vec<LegacyStreamEntry>,
}

struct LegacyStreamMetadata {
    length: u64,
    last_id: StreamID,
    max_deleted_entry_id: StreamID,
    entries_added: u64,
}

struct LegacyPendingEntry {
    id: StreamID,
    delivery_time_ms: i64,
    delivery_count: u64,
    consumer: Option<Bytes>,
}

struct LegacyConsumer {
    name: Bytes,
    seen_time_ms: i64,
    active_time_ms: i64,
    pending: Vec<StreamID>,
}

struct LegacyGroup {
    name: Bytes,
    last_id: StreamID,
    entries_read: Option<u64>,
    consumers: Vec<LegacyConsumer>,
    pending: Vec<LegacyPendingEntry>,
}

struct StreamRestoreBuilder {
    node_ids: HashSet<StreamID>,
    entry_ids: HashSet<StreamID>,
    entries: Vec<LegacyStreamEntry>,
    metadata: Option<LegacyStreamMetadata>,
    group_names: HashSet<Bytes>,
    groups: Vec<LegacyGroup>,
}

impl StreamRestoreBuilder {
    fn new() -> Self {
        Self {
            node_ids: HashSet::new(),
            entry_ids: HashSet::new(),
            entries: Vec::new(),
            metadata: None,
            group_names: HashSet::new(),
            groups: Vec::new(),
        }
    }

    fn add_node(&mut self, node_id: StreamID, node: DecodedLegacyListpack) -> Option<()> {
        if !self.node_ids.insert(node_id) {
            return None;
        }
        for id in node.entry_ids {
            if !self.entry_ids.insert(id) {
                return None;
            }
        }
        self.entries.extend(node.live_entries);
        Some(())
    }

    fn set_metadata(&mut self, metadata: LegacyStreamMetadata) -> Option<()> {
        if metadata.length != self.entries.len() as u64 {
            return None;
        }
        if self.metadata.replace(metadata).is_some() {
            return None;
        }
        Some(())
    }

    fn add_group(&mut self, group: LegacyGroup) -> Option<()> {
        if !self.group_names.insert(group.name.clone()) {
            return None;
        }
        self.groups.push(group);
        Some(())
    }

    fn legacy_entries_read(&self, last_id: StreamID) -> u64 {
        self.entries
            .iter()
            .filter(|entry| entry.id <= last_id)
            .count() as u64
    }

    fn validate_cross_references(&mut self) -> Option<()> {
        for group in &mut self.groups {
            validate_legacy_group_cross_references(group)?;
        }
        Some(())
    }

    fn commit(self) -> Option<ValkeyStream> {
        let metadata = self.metadata?;
        let mut stream = ValkeyStream::new();
        for entry in self.entries {
            stream.append(entry.id, entry.fields, true, false, 0).ok()?;
        }
        stream.last_id = metadata.last_id;
        stream.max_deleted_entry_id = metadata.max_deleted_entry_id;
        stream.entries_added = metadata.entries_added;
        for group in self.groups {
            stream.restore_group(group.into_restored()?).ok()?;
        }
        Some(stream)
    }
}

impl LegacyGroup {
    fn into_restored(self) -> Option<RestoredStreamGroup> {
        let consumers = self
            .consumers
            .into_iter()
            .map(LegacyConsumer::into_restored)
            .collect();
        let pending = self
            .pending
            .into_iter()
            .map(LegacyPendingEntry::into_restored)
            .collect::<Option<Vec<_>>>()?;
        Some(RestoredStreamGroup {
            name: self.name,
            last_id: self.last_id,
            entries_read: self.entries_read,
            consumers,
            pending,
        })
    }
}

impl LegacyConsumer {
    fn into_restored(self) -> RestoredStreamConsumer {
        RestoredStreamConsumer {
            name: self.name,
            seen_time_ms: self.seen_time_ms,
            active_time_ms: Some(self.active_time_ms),
            pending: self.pending,
        }
    }
}

impl LegacyPendingEntry {
    fn into_restored(self) -> Option<RestoredStreamPendingEntry> {
        Some(RestoredStreamPendingEntry {
            id: self.id,
            consumer: self.consumer?,
            delivery_time_ms: self.delivery_time_ms,
            delivery_count: self.delivery_count,
        })
    }
}

fn decode_legacy_listpack_nodes(
    cursor: &mut LegacyStreamCursor<'_>,
    builder: &mut StreamRestoreBuilder,
) -> Option<()> {
    let listpack_count = cursor.read_plain_len()?;
    for _ in 0..listpack_count {
        let node_id = decode_stream_id(&cursor.read_rdb_string()?)?;
        let listpack = cursor.read_rdb_string()?;
        let decoded = decode_legacy_stream_listpack(node_id, &listpack)?;
        builder.add_node(node_id, decoded)?;
    }
    Some(())
}

fn decode_legacy_metadata(
    cursor: &mut LegacyStreamCursor<'_>,
    builder: &mut StreamRestoreBuilder,
    has_offsets: bool,
) -> Option<()> {
    let length = cursor.read_plain_len()?;
    let last_id = cursor.read_plain_stream_id()?;
    let (max_deleted_entry_id, entries_added) = if has_offsets {
        let _first_id = cursor.read_plain_stream_id()?;
        (cursor.read_plain_stream_id()?, cursor.read_plain_len()?)
    } else {
        (StreamID::zero(), length)
    };
    builder.set_metadata(LegacyStreamMetadata {
        length,
        last_id,
        max_deleted_entry_id,
        entries_added,
    })
}

fn decode_legacy_groups(
    cursor: &mut LegacyStreamCursor<'_>,
    builder: &mut StreamRestoreBuilder,
    has_offsets: bool,
    has_active_time: bool,
) -> Result<(), StreamLoadError> {
    let group_count = cursor.read_plain_len().ok_or(StreamLoadError::Invalid)?;
    for _ in 0..group_count {
        decode_legacy_group(cursor, builder, has_offsets, has_active_time)?;
    }
    Ok(())
}

fn decode_legacy_group(
    cursor: &mut LegacyStreamCursor<'_>,
    builder: &mut StreamRestoreBuilder,
    has_offsets: bool,
    has_active_time: bool,
) -> Result<(), StreamLoadError> {
    let name = Bytes::from(cursor.read_rdb_string().ok_or(StreamLoadError::Invalid)?);
    let last_id = cursor
        .read_plain_stream_id()
        .ok_or(StreamLoadError::Invalid)?;
    let entries_read = if has_offsets {
        Some(cursor.read_plain_len().ok_or(StreamLoadError::Invalid)?)
    } else {
        Some(builder.legacy_entries_read(last_id))
    };
    let pending = decode_legacy_global_pel(cursor).ok_or(StreamLoadError::Invalid)?;
    let consumers = decode_legacy_consumers(cursor, has_active_time)?;
    builder
        .add_group(LegacyGroup {
            name,
            last_id,
            entries_read,
            consumers,
            pending,
        })
        .ok_or(StreamLoadError::Invalid)
}

fn decode_legacy_global_pel(
    cursor: &mut LegacyStreamCursor<'_>,
) -> Option<Vec<LegacyPendingEntry>> {
    let pending_count = cursor.read_plain_len()?;
    let mut pending = Vec::new();
    let mut pending_ids = HashSet::new();
    for _ in 0..pending_count {
        let id = cursor.read_stream_id_raw()?;
        if !pending_ids.insert(id) {
            return None;
        }
        pending.push(LegacyPendingEntry {
            id,
            delivery_time_ms: cursor.read_i64_raw()?,
            delivery_count: cursor.read_plain_len()?,
            consumer: None,
        });
    }
    Some(pending)
}

fn decode_legacy_consumers(
    cursor: &mut LegacyStreamCursor<'_>,
    has_active_time: bool,
) -> Result<Vec<LegacyConsumer>, StreamLoadError> {
    let consumer_count = cursor.read_plain_len().ok_or(StreamLoadError::Invalid)?;
    let mut consumers = Vec::new();
    let mut consumer_names = HashSet::new();
    for _ in 0..consumer_count {
        let name = Bytes::from(cursor.read_rdb_string().ok_or(StreamLoadError::Invalid)?);
        if !consumer_names.insert(name.clone()) {
            return Err(StreamLoadError::DuplicateConsumer);
        }
        let seen_time_ms = cursor.read_i64_raw().ok_or(StreamLoadError::Invalid)?;
        let active_time_ms = if has_active_time {
            cursor.read_i64_raw().ok_or(StreamLoadError::Invalid)?
        } else {
            seen_time_ms
        };
        let pending_count = cursor.read_plain_len().ok_or(StreamLoadError::Invalid)?;
        let mut pending = Vec::new();
        let mut pending_ids = HashSet::new();
        for _ in 0..pending_count {
            let id = cursor
                .read_stream_id_raw()
                .ok_or(StreamLoadError::Invalid)?;
            if !pending_ids.insert(id) {
                return Err(StreamLoadError::DuplicateConsumerPel);
            }
            pending.push(id);
        }
        consumers.push(LegacyConsumer {
            name,
            seen_time_ms,
            active_time_ms,
            pending,
        });
    }
    Ok(consumers)
}

fn validate_legacy_group_cross_references(group: &mut LegacyGroup) -> Option<()> {
    let global_ids: HashSet<StreamID> = group.pending.iter().map(|entry| entry.id).collect();
    if global_ids.len() != group.pending.len() {
        return None;
    }
    let mut owners = HashMap::new();
    for consumer in &group.consumers {
        for id in &consumer.pending {
            if !global_ids.contains(id) || owners.insert(*id, consumer.name.clone()).is_some() {
                return None;
            }
        }
    }
    for pending in &mut group.pending {
        pending.consumer = Some(owners.remove(&pending.id)?);
    }
    owners.is_empty().then_some(())
}

trait StreamRdbWriterExt: RdbWriter {
    fn write_signed_len(&mut self, value: i64) {
        self.write_string(&value.to_be_bytes());
    }
}

impl<T: RdbWriter + ?Sized> StreamRdbWriterExt for T {}

fn write_optional_u64<W: RdbWriter>(w: &mut W, value: Option<u64>) {
    match value {
        Some(v) => {
            w.write_len(1);
            w.write_len(v);
        }
        None => w.write_len(0),
    }
}

fn write_optional_i64<W: RdbWriter>(w: &mut W, value: Option<i64>) {
    match value {
        Some(v) => {
            w.write_len(1);
            w.write_signed_len(v);
        }
        None => w.write_len(0),
    }
}

fn load_optional_u64(buf: &[u8], pos: &mut usize) -> Option<Option<u64>> {
    match load_plain_len(buf, pos)? {
        0 => Some(None),
        1 => Some(Some(load_plain_len(buf, pos)?)),
        _ => None,
    }
}

fn load_optional_i64(buf: &[u8], pos: &mut usize) -> Option<Option<i64>> {
    match load_plain_len(buf, pos)? {
        0 => Some(None),
        1 => Some(Some(load_i64(buf, pos)?)),
        _ => None,
    }
}

fn load_plain_len(buf: &[u8], pos: &mut usize) -> Option<u64> {
    let (value, encoded) = load_len(buf, pos)?;
    if encoded {
        return None;
    }
    Some(value)
}

fn load_i64(buf: &[u8], pos: &mut usize) -> Option<i64> {
    let raw = decoder::load_string(buf, pos)?;
    let bytes: [u8; 8] = raw.as_slice().try_into().ok()?;
    Some(i64::from_be_bytes(bytes))
}

fn decode_legacy_stream_listpack(primary_id: StreamID, lp: &[u8]) -> Option<DecodedLegacyListpack> {
    let mut cursor = LegacyListpackCursor::new(listpack_decode(lp)?);
    let entry_count = cursor.read_nonnegative_usize()?;
    let deleted_count = cursor.read_nonnegative_usize()?;
    let total_count = entry_count.checked_add(deleted_count)?;
    let primary_fields_count = cursor.read_nonnegative_usize()?;
    let primary_fields = cursor.take_items(primary_fields_count)?;
    if cursor.read_i64()? != 0 {
        return None;
    }
    let mut entry_ids = Vec::new();
    let mut live_entries = Vec::new();
    for _ in 0..total_count {
        let (id, entry) = decode_legacy_listpack_entry(&mut cursor, primary_id, &primary_fields)?;
        entry_ids.push(id);
        live_entries.extend(entry);
    }
    (cursor.is_done() && live_entries.len() == entry_count).then_some(DecodedLegacyListpack {
        entry_ids,
        live_entries,
    })
}

fn parse_i64_item(raw: &[u8]) -> Option<i64> {
    int_encodable(raw)
}

struct LegacyListpackCursor {
    items: Vec<Vec<u8>>,
    index: usize,
}

impl LegacyListpackCursor {
    fn new(items: Vec<Vec<u8>>) -> Self {
        Self { items, index: 0 }
    }

    fn position(&self) -> usize {
        self.index
    }

    fn read_i64(&mut self) -> Option<i64> {
        let value = parse_i64_item(self.items.get(self.index)?)?;
        self.index += 1;
        Some(value)
    }

    fn read_nonnegative_usize(&mut self) -> Option<usize> {
        usize::try_from(self.read_i64()?).ok()
    }

    fn take_items(&mut self, count: usize) -> Option<Vec<Vec<u8>>> {
        let end = self.index.checked_add(count)?;
        let values = self.items.get(self.index..end)?.to_vec();
        self.index = end;
        Some(values)
    }

    fn is_done(&self) -> bool {
        self.index == self.items.len()
    }
}

fn decode_legacy_listpack_entry(
    cursor: &mut LegacyListpackCursor,
    primary_id: StreamID,
    primary_fields: &[Vec<u8>],
) -> Option<(StreamID, Option<LegacyStreamEntry>)> {
    let start = cursor.position();
    let flags = cursor.read_i64()?;
    let ms_delta = cursor.read_i64()?;
    let seq_delta = cursor.read_i64()?;
    let id = StreamID::new(
        primary_id.ms.wrapping_add(ms_delta as u64),
        primary_id.seq.wrapping_add(seq_delta as u64),
    );
    let field_names = if flags & 2 != 0 {
        primary_fields.to_vec()
    } else {
        let field_count = cursor.read_nonnegative_usize()?;
        cursor.take_items(field_count)?
    };
    let values = cursor.take_items(field_names.len())?;
    let fields = field_names
        .into_iter()
        .zip(values)
        .map(|(field, value)| (Bytes::from(field), Bytes::from(value)))
        .collect();
    let logical_count = cursor.position().checked_sub(start)?;
    if cursor.read_nonnegative_usize()? != logical_count {
        return None;
    }
    let entry = (flags & 1 == 0).then_some(LegacyStreamEntry { id, fields });
    Some((id, entry))
}

fn load_v1_pending_ids(buf: &[u8], pos: &mut usize, entry_ids: &HashSet<StreamID>) -> Option<()> {
    let pending_count = load_plain_len(buf, pos)?;
    let mut pending_ids = HashSet::with_capacity(pending_count as usize);
    for _ in 0..pending_count {
        let id = decode_stream_id(&decoder::load_string(buf, pos)?)?;
        if !entry_ids.contains(&id) || !pending_ids.insert(id) {
            return None;
        }
    }
    Some(())
}

fn load_group(buf: &[u8], pos: &mut usize) -> Option<RestoredStreamGroup> {
    let name = Bytes::from(decoder::load_string(buf, pos)?);
    let last_id = decode_stream_id(&decoder::load_string(buf, pos)?)?;
    let entries_read = load_optional_u64(buf, pos)?;

    let consumer_count = load_plain_len(buf, pos)?;
    let mut consumers = Vec::with_capacity(consumer_count as usize);
    let mut consumer_names = HashSet::with_capacity(consumer_count as usize);
    for _ in 0..consumer_count {
        let consumer_name = Bytes::from(decoder::load_string(buf, pos)?);
        if !consumer_names.insert(consumer_name.clone()) {
            log::warn!("Duplicate stream consumer detected");
            return None;
        }
        let seen_time_ms = load_i64(buf, pos)?;
        let active_time_ms = load_optional_i64(buf, pos)?;
        let pending_count = load_plain_len(buf, pos)?;
        let mut pending = Vec::with_capacity(pending_count as usize);
        let mut pending_ids = HashSet::with_capacity(pending_count as usize);
        for _ in 0..pending_count {
            let id = decode_stream_id(&decoder::load_string(buf, pos)?)?;
            if !pending_ids.insert(id) {
                log::warn!("Duplicated consumer PEL entry");
                return None;
            }
            pending.push(id);
        }
        consumers.push(RestoredStreamConsumer {
            name: consumer_name,
            seen_time_ms,
            active_time_ms,
            pending,
        });
    }

    let pending_count = load_plain_len(buf, pos)?;
    let mut pending = Vec::with_capacity(pending_count as usize);
    for _ in 0..pending_count {
        let id = decode_stream_id(&decoder::load_string(buf, pos)?)?;
        let consumer = Bytes::from(decoder::load_string(buf, pos)?);
        let delivery_time_ms = load_i64(buf, pos)?;
        let delivery_count = load_plain_len(buf, pos)?;
        pending.push(RestoredStreamPendingEntry {
            id,
            consumer,
            delivery_time_ms,
            delivery_count,
        });
    }

    Some(RestoredStreamGroup {
        name,
        last_id,
        entries_read,
        consumers,
        pending,
    })
}

fn encode_stream_id(id: &StreamID) -> [u8; 16] {
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&id.ms.to_be_bytes());
    out[8..].copy_from_slice(&id.seq.to_be_bytes());
    out
}

fn decode_stream_id(raw: &[u8]) -> Option<StreamID> {
    if raw.len() != 16 {
        return None;
    }
    let ms = u64::from_be_bytes(raw[..8].try_into().ok()?);
    let seq = u64::from_be_bytes(raw[8..].try_into().ok()?);
    Some(StreamID::new(ms, seq))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::io::persistence::codec::decoder_compact::listpack_encode;
    use crate::foundation::io::persistence::codec::types::save_len;

    struct LegacyFixture {
        body: Vec<u8>,
        phase_ends: Vec<usize>,
    }

    #[derive(Clone)]
    struct LegacyEntryFixture {
        id: StreamID,
        value: Vec<u8>,
        is_deleted: bool,
    }

    #[derive(Clone)]
    struct LegacyNodeFixture {
        primary_id: StreamID,
        entries: Vec<LegacyEntryFixture>,
        trailing_items: Vec<Vec<u8>>,
    }

    #[derive(Clone)]
    struct LegacyPendingFixture {
        id: StreamID,
        delivery_time_ms: i64,
        delivery_count: u64,
    }

    #[derive(Clone)]
    struct LegacyConsumerFixture {
        name: Vec<u8>,
        seen_time_ms: i64,
        pending: Vec<StreamID>,
    }

    #[derive(Clone)]
    struct LegacyGroupFixture {
        name: Vec<u8>,
        last_id: StreamID,
        entries_read: u64,
        pending: Vec<LegacyPendingFixture>,
        consumers: Vec<LegacyConsumerFixture>,
    }

    #[derive(Clone)]
    struct LegacyMetadataFixture {
        length: u64,
        last_id: StreamID,
        first_id: StreamID,
        max_deleted_entry_id: StreamID,
        entries_added: u64,
    }

    #[derive(Clone)]
    struct LegacyFixtureSpec {
        type_byte: u8,
        nodes: Vec<LegacyNodeFixture>,
        metadata: LegacyMetadataFixture,
        groups: Vec<LegacyGroupFixture>,
    }

    fn append_rdb_string(out: &mut Vec<u8>, value: &[u8]) {
        save_len(out, value.len() as u64);
        out.extend_from_slice(value);
    }

    fn minimal_legacy_listpack_items() -> Vec<Vec<u8>> {
        vec![
            b"1".to_vec(),
            b"0".to_vec(),
            b"1".to_vec(),
            b"field".to_vec(),
            b"0".to_vec(),
            b"2".to_vec(),
            b"0".to_vec(),
            b"0".to_vec(),
            b"value".to_vec(),
            b"4".to_vec(),
        ]
    }

    fn minimal_legacy_node() -> LegacyNodeFixture {
        LegacyNodeFixture {
            primary_id: StreamID::new(1, 1),
            entries: vec![LegacyEntryFixture {
                id: StreamID::new(1, 1),
                value: b"value".to_vec(),
                is_deleted: false,
            }],
            trailing_items: Vec::new(),
        }
    }

    fn minimal_legacy_group(pending_id: StreamID) -> LegacyGroupFixture {
        LegacyGroupFixture {
            name: b"group".to_vec(),
            last_id: StreamID::new(1, 1),
            entries_read: 1,
            pending: vec![LegacyPendingFixture {
                id: pending_id,
                delivery_time_ms: 1234,
                delivery_count: 7,
            }],
            consumers: vec![LegacyConsumerFixture {
                name: b"alice".to_vec(),
                seen_time_ms: 100,
                pending: vec![pending_id],
            }],
        }
    }

    fn legacy_fixture_spec(type_byte: u8, pending_id: StreamID) -> LegacyFixtureSpec {
        LegacyFixtureSpec {
            type_byte,
            nodes: vec![minimal_legacy_node()],
            metadata: LegacyMetadataFixture {
                length: 1,
                last_id: StreamID::new(1, 1),
                first_id: StreamID::new(1, 1),
                max_deleted_entry_id: StreamID::zero(),
                entries_added: 1,
            },
            groups: vec![minimal_legacy_group(pending_id)],
        }
    }

    fn sequence_reset_fixture_spec(type_byte: u8) -> LegacyFixtureSpec {
        let pending_id = StreamID::new(1, 0);
        let mut spec = legacy_fixture_spec(type_byte, pending_id);
        spec.nodes[0].entries.push(LegacyEntryFixture {
            id: StreamID::new(2, 0),
            value: b"two".to_vec(),
            is_deleted: false,
        });
        spec.nodes[0].entries[0].value = b"one".to_vec();
        spec.metadata.length = 2;
        spec.metadata.last_id = StreamID::new(2, 0);
        spec.metadata.max_deleted_entry_id = pending_id;
        spec.metadata.entries_added = 3;
        spec.groups[0].last_id = StreamID::new(2, 0);
        spec.groups[0].entries_read = 2;
        spec
    }

    fn signed_delta(value: u64, base: u64) -> i64 {
        value.wrapping_sub(base) as i64
    }

    fn append_legacy_node(out: &mut Vec<u8>, node: &LegacyNodeFixture) {
        append_rdb_string(out, &encode_stream_id(&node.primary_id));
        let live = node
            .entries
            .iter()
            .filter(|entry| !entry.is_deleted)
            .count();
        let deleted = node.entries.len() - live;
        let mut items = vec![
            live.to_string().into_bytes(),
            deleted.to_string().into_bytes(),
            b"1".to_vec(),
            b"field".to_vec(),
            b"0".to_vec(),
        ];
        for entry in &node.entries {
            let flags = 2 | usize::from(entry.is_deleted);
            items.push(flags.to_string().into_bytes());
            items.push(
                signed_delta(entry.id.ms, node.primary_id.ms)
                    .to_string()
                    .into_bytes(),
            );
            items.push(
                signed_delta(entry.id.seq, node.primary_id.seq)
                    .to_string()
                    .into_bytes(),
            );
            items.push(entry.value.clone());
            items.push(b"4".to_vec());
        }
        items.extend(node.trailing_items.clone());
        append_rdb_string(out, &listpack_encode(&items));
    }

    fn append_legacy_metadata(out: &mut Vec<u8>, spec: &LegacyFixtureSpec) {
        save_len(out, spec.metadata.length);
        save_len(out, spec.metadata.last_id.ms);
        save_len(out, spec.metadata.last_id.seq);
        if spec.type_byte >= RDB_TYPE_STREAM_LISTPACKS_2 {
            save_len(out, spec.metadata.first_id.ms);
            save_len(out, spec.metadata.first_id.seq);
            save_len(out, spec.metadata.max_deleted_entry_id.ms);
            save_len(out, spec.metadata.max_deleted_entry_id.seq);
            save_len(out, spec.metadata.entries_added);
        }
        save_len(out, spec.groups.len() as u64);
    }

    fn append_legacy_group_header(out: &mut Vec<u8>, type_byte: u8, group: &LegacyGroupFixture) {
        append_rdb_string(out, &group.name);
        save_len(out, group.last_id.ms);
        save_len(out, group.last_id.seq);
        if type_byte >= RDB_TYPE_STREAM_LISTPACKS_2 {
            save_len(out, group.entries_read);
        }
    }

    fn append_legacy_global_pel(out: &mut Vec<u8>, group: &LegacyGroupFixture) {
        save_len(out, group.pending.len() as u64);
        for pending in &group.pending {
            out.extend_from_slice(&encode_stream_id(&pending.id));
            out.extend_from_slice(&pending.delivery_time_ms.to_le_bytes());
            save_len(out, pending.delivery_count);
        }
    }

    fn append_legacy_consumers(out: &mut Vec<u8>, type_byte: u8, group: &LegacyGroupFixture) {
        save_len(out, group.consumers.len() as u64);
        for consumer in &group.consumers {
            append_rdb_string(out, &consumer.name);
            out.extend_from_slice(&consumer.seen_time_ms.to_le_bytes());
            if type_byte >= RDB_TYPE_STREAM_LISTPACKS_3 {
                out.extend_from_slice(&(consumer.seen_time_ms - 5).to_le_bytes());
            }
            save_len(out, consumer.pending.len() as u64);
            for id in &consumer.pending {
                out.extend_from_slice(&encode_stream_id(id));
            }
        }
    }

    fn encode_legacy_fixture(spec: &LegacyFixtureSpec) -> LegacyFixture {
        let mut body = Vec::new();
        save_len(&mut body, spec.nodes.len() as u64);
        for node in &spec.nodes {
            append_legacy_node(&mut body, node);
        }
        let mut phase_ends = vec![body.len()];
        append_legacy_metadata(&mut body, spec);
        phase_ends.push(body.len());
        for group in &spec.groups {
            append_legacy_group_header(&mut body, spec.type_byte, group);
            phase_ends.push(body.len());
            append_legacy_global_pel(&mut body, group);
            phase_ends.push(body.len());
            append_legacy_consumers(&mut body, spec.type_byte, group);
        }
        LegacyFixture { body, phase_ends }
    }

    fn load_legacy_fixture(fixture: &LegacyFixture, type_byte: u8) -> Option<ValkeyStream> {
        let mut pos = 0;
        let Robj::StreamListpack(stream) =
            StreamCodec::load_dispatch(&fixture.body, &mut pos, type_byte)?
        else {
            return None;
        };
        (pos == fixture.body.len()).then_some(*stream)
    }

    fn stream_with_two_entries() -> ValkeyStream {
        let mut stream = ValkeyStream::new();
        stream
            .append(
                StreamID::new(1, 1),
                vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v"))],
                true,
                false,
                0,
            )
            .unwrap();
        stream
            .append(
                StreamID::new(2, 1),
                vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v"))],
                true,
                false,
                0,
            )
            .unwrap();
        stream
    }

    #[test]
    fn third_version_stream_restores_consumer_active_time() {
        let spec = legacy_fixture_spec(RDB_TYPE_STREAM_LISTPACKS_3, StreamID::new(1, 1));
        let fixture = encode_legacy_fixture(&spec);
        let stream = load_legacy_fixture(&fixture, RDB_TYPE_STREAM_LISTPACKS_3)
            .expect("valid third-version stream");
        let consumer = stream
            .group(b"group")
            .expect("group")
            .consumers
            .get(b"alice" as &[u8])
            .expect("consumer");
        assert_eq!(consumer.seen_time_ms, 100);
        assert_eq!(consumer.active_time_ms, Some(95));
    }

    #[test]
    fn legacy_listpack_rejects_live_deleted_split_mismatch() {
        let mut items = minimal_legacy_listpack_items();
        items[0] = b"0".to_vec();
        items[1] = b"1".to_vec();
        let listpack = listpack_encode(&items);
        assert!(decode_legacy_stream_listpack(StreamID::new(1, 1), &listpack).is_none());
    }

    #[test]
    fn stream_dump_round_trips_entries() {
        let mut buf = Vec::new();
        StreamCodec::save(&stream_with_two_entries(), &mut buf);

        let mut pos = 0;
        let restored = StreamCodec::load(&buf, &mut pos).unwrap();

        assert_eq!(restored.len(), 2);
        assert!(restored.get_entry(&StreamID::new(1, 1)).is_some());
        assert!(restored.get_entry(&StreamID::new(2, 1)).is_some());
        assert_eq!(pos, buf.len());
    }

    #[test]
    fn stream_dump_round_trips_consumer_groups() {
        let mut stream = stream_with_two_entries();
        stream
            .create_group(Bytes::from_static(b"group"), StreamID::zero(), Some(0))
            .unwrap();
        stream
            .read_group_new(b"group", b"alice", 1, false, 10_000)
            .unwrap();
        stream
            .read_group_new(b"group", b"bob", 1, false, 11_000)
            .unwrap();

        let mut buf = Vec::new();
        StreamCodec::save(&stream, &mut buf);

        let mut pos = 0;
        let restored = StreamCodec::load(&buf, &mut pos).unwrap();
        let group = restored.group(b"group").unwrap();
        assert_eq!(group.last_id, StreamID::new(2, 1));
        assert_eq!(group.entries_read, Some(2));
        assert_eq!(group.pending.len(), 2);
        let alice = group.consumers.get(b"alice" as &[u8]).unwrap();
        assert_eq!(alice.seen_time_ms, 10_000);
        assert_eq!(alice.active_time_ms, Some(10_000));
        assert!(alice.pending.contains(&StreamID::new(1, 1)));
        let bob = group.consumers.get(b"bob" as &[u8]).unwrap();
        assert_eq!(bob.seen_time_ms, 11_000);
        assert_eq!(bob.active_time_ms, Some(11_000));
        assert!(bob.pending.contains(&StreamID::new(2, 1)));
        assert_eq!(pos, buf.len());
    }

    #[test]
    fn stream_restore_rejects_shared_pending_ids() {
        let mut buf = Vec::new();
        StreamCodec::save(&stream_with_two_entries(), &mut buf);

        let id_2_1 = encode_stream_id(&StreamID::new(2, 1));
        let id_1_1 = encode_stream_id(&StreamID::new(1, 1));
        let last = buf
            .windows(id_2_1.len())
            .rposition(|window| window == id_2_1)
            .unwrap();
        buf[last..last + id_1_1.len()].copy_from_slice(&id_1_1);

        let mut pos = 0;
        assert!(StreamCodec::load(&buf, &mut pos).is_none());
    }

    #[test]
    fn stream_restore_rejects_duplicate_consumer_pending_ids() {
        let mut stream = stream_with_two_entries();
        stream
            .create_group(Bytes::from_static(b"group"), StreamID::zero(), Some(0))
            .unwrap();
        stream
            .read_group_new(b"group", b"alice", 2, false, 10_000)
            .unwrap();

        let mut buf = Vec::new();
        StreamCodec::save(&stream, &mut buf);

        let id_2_1 = encode_stream_id(&StreamID::new(2, 1));
        let id_1_1 = encode_stream_id(&StreamID::new(1, 1));
        let first = buf
            .windows(id_1_1.len())
            .position(|window| window == id_1_1)
            .unwrap();
        let duplicate = buf[first + id_1_1.len()..]
            .windows(id_2_1.len())
            .position(|window| window == id_2_1)
            .map(|offset| first + id_1_1.len() + offset)
            .unwrap();
        buf[duplicate..duplicate + id_1_1.len()].copy_from_slice(&id_1_1);

        let mut pos = 0;
        assert!(StreamCodec::load(&buf, &mut pos).is_none());
    }

    #[test]
    fn stream_restore_rejects_duplicate_consumers() {
        let mut stream = stream_with_two_entries();
        stream
            .create_group(Bytes::from_static(b"group"), StreamID::zero(), Some(0))
            .unwrap();
        stream
            .read_group_new(b"group", b"alice", 1, false, 10_000)
            .unwrap();
        stream
            .read_group_new(b"group", b"bob", 1, false, 11_000)
            .unwrap();

        let mut buf = Vec::new();
        StreamCodec::save(&stream, &mut buf);

        let bob = b"\x03bob";
        let alice = b"\x05alice";
        let pos = buf
            .windows(bob.len())
            .position(|window| window == bob)
            .unwrap();
        buf.splice(pos..pos + bob.len(), alice.iter().copied());

        let mut pos = 0;
        assert!(StreamCodec::load(&buf, &mut pos).is_none());
    }

    #[test]
    fn legacy_stream_rejects_global_pel_shared_by_consumers() {
        let pending_id = StreamID::new(1, 1);
        let mut spec = legacy_fixture_spec(RDB_TYPE_STREAM_LISTPACKS_2, pending_id);
        let mut bob = spec.groups[0].consumers[0].clone();
        bob.name = b"bob".to_vec();
        spec.groups[0].consumers.push(bob);
        let fixture = encode_legacy_fixture(&spec);
        let mut buf = vec![0xaa, 0xbb];
        buf.extend_from_slice(&fixture.body);
        let mut pos = 2;

        assert!(StreamCodec::load_dispatch(&buf, &mut pos, RDB_TYPE_STREAM_LISTPACKS_2).is_none());
        assert_eq!(pos, 2);
    }

    #[test]
    fn legacy_stream_rejects_trailing_logical_listpack_item() {
        let mut spec = legacy_fixture_spec(RDB_TYPE_STREAM_LISTPACKS_2, StreamID::new(1, 1));
        spec.nodes[0].trailing_items.push(b"trailing".to_vec());
        let fixture = encode_legacy_fixture(&spec);
        let mut pos = 0;

        assert!(
            StreamCodec::load_dispatch(&fixture.body, &mut pos, RDB_TYPE_STREAM_LISTPACKS_2)
                .is_none()
        );
        assert_eq!(pos, 0);
    }

    #[test]
    fn legacy_stream_rejects_negative_counts() {
        let negative_count =
            listpack_encode(&[b"-1".to_vec(), b"1".to_vec(), b"0".to_vec(), b"0".to_vec()]);
        assert!(decode_legacy_stream_listpack(StreamID::new(1, 1), &negative_count).is_none());
    }

    #[test]
    fn legacy_stream_rejects_noncanonical_numeric_items() {
        let mut plus_count = minimal_legacy_listpack_items();
        plus_count[0] = b"+1".to_vec();
        let mut leading_zero_count = minimal_legacy_listpack_items();
        leading_zero_count[0] = b"01".to_vec();
        let negative_zero_count = vec![b"-0".to_vec(), b"0".to_vec(), b"0".to_vec(), b"0".to_vec()];
        let mut plus_delta = minimal_legacy_listpack_items();
        plus_delta[6] = b"+0".to_vec();
        let mut leading_zero_lp_count = minimal_legacy_listpack_items();
        leading_zero_lp_count[9] = b"04".to_vec();
        let cases = [
            ("+1 entry count", plus_count),
            ("01 entry count", leading_zero_count),
            ("-0 entry count", negative_zero_count),
            ("+0 ID delta", plus_delta),
            ("04 lp-count", leading_zero_lp_count),
        ];
        let accepted: Vec<_> = cases
            .into_iter()
            .filter_map(|(label, items)| {
                let listpack = listpack_encode(&items);
                decode_legacy_stream_listpack(StreamID::new(1, 1), &listpack).map(|_| label)
            })
            .collect();
        assert!(
            accepted.is_empty(),
            "accepted non-canonical items: {accepted:?}"
        );
    }

    #[test]
    fn legacy_stream_numeric_parser_keeps_canonical_i64_boundaries() {
        for value in [i64::MIN, -1, 0, i64::MAX] {
            assert_eq!(parse_i64_item(value.to_string().as_bytes()), Some(value));
        }
    }

    #[test]
    fn legacy_stream_rejects_duplicate_listpack_nodes_and_entry_ids() {
        let mut duplicate_node_spec =
            legacy_fixture_spec(RDB_TYPE_STREAM_LISTPACKS_2, StreamID::new(1, 1));
        duplicate_node_spec
            .nodes
            .push(duplicate_node_spec.nodes[0].clone());
        let duplicate_node = encode_legacy_fixture(&duplicate_node_spec);
        assert!(load_legacy_fixture(&duplicate_node, RDB_TYPE_STREAM_LISTPACKS_2).is_none());

        let mut duplicate_entry_spec =
            legacy_fixture_spec(RDB_TYPE_STREAM_LISTPACKS_2, StreamID::new(1, 1));
        let mut second_node = minimal_legacy_node();
        second_node.primary_id = StreamID::new(1, 0);
        duplicate_entry_spec.nodes.push(second_node);
        let duplicate_entry = encode_legacy_fixture(&duplicate_entry_spec);
        assert!(load_legacy_fixture(&duplicate_entry, RDB_TYPE_STREAM_LISTPACKS_2).is_none());
    }

    #[test]
    fn legacy_stream_rejects_duplicate_groups_and_consumers() {
        let mut duplicate_group_spec =
            legacy_fixture_spec(RDB_TYPE_STREAM_LISTPACKS_2, StreamID::new(1, 1));
        duplicate_group_spec
            .groups
            .push(duplicate_group_spec.groups[0].clone());
        let duplicate_group = encode_legacy_fixture(&duplicate_group_spec);
        assert!(load_legacy_fixture(&duplicate_group, RDB_TYPE_STREAM_LISTPACKS_2).is_none());

        let mut duplicate_consumer_spec =
            legacy_fixture_spec(RDB_TYPE_STREAM_LISTPACKS_2, StreamID::new(1, 1));
        let consumer = duplicate_consumer_spec.groups[0].consumers[0].clone();
        duplicate_consumer_spec.groups[0].consumers.push(consumer);
        let duplicate_consumer = encode_legacy_fixture(&duplicate_consumer_spec);
        assert!(load_legacy_fixture(&duplicate_consumer, RDB_TYPE_STREAM_LISTPACKS_2).is_none());

        let mut pos = 0;
        let error = StreamCodec::load_legacy_listpacks_detailed(
            &duplicate_consumer.body,
            &mut pos,
            RDB_TYPE_STREAM_LISTPACKS_2,
        )
        .expect_err("duplicate consumer must retain its specific load error");
        assert_eq!(error, StreamLoadError::DuplicateConsumer);
    }

    #[test]
    fn legacy_stream_rejects_duplicate_global_pel_ids() {
        let mut spec = legacy_fixture_spec(RDB_TYPE_STREAM_LISTPACKS_2, StreamID::new(1, 1));
        let pending = spec.groups[0].pending[0].clone();
        spec.groups[0].pending.push(pending);
        let duplicate_pel = encode_legacy_fixture(&spec);
        assert!(load_legacy_fixture(&duplicate_pel, RDB_TYPE_STREAM_LISTPACKS_2).is_none());

        let mut consumer_spec =
            legacy_fixture_spec(RDB_TYPE_STREAM_LISTPACKS_2, StreamID::new(1, 1));
        let pending = consumer_spec.groups[0].consumers[0].pending[0];
        consumer_spec.groups[0].consumers[0].pending.push(pending);
        let duplicate_consumer_pel = encode_legacy_fixture(&consumer_spec);
        let mut pos = 0;
        let error = StreamCodec::load_legacy_listpacks_detailed(
            &duplicate_consumer_pel.body,
            &mut pos,
            RDB_TYPE_STREAM_LISTPACKS_2,
        )
        .expect_err("duplicate consumer PEL must retain its specific load error");
        assert_eq!(error, StreamLoadError::DuplicateConsumerPel);
    }

    #[test]
    fn legacy_stream_rejects_invalid_local_pel_cross_references() {
        let mut missing_spec =
            legacy_fixture_spec(RDB_TYPE_STREAM_LISTPACKS_2, StreamID::new(1, 1));
        missing_spec.groups[0].consumers[0].pending[0] = StreamID::new(2, 2);
        let missing = encode_legacy_fixture(&missing_spec);
        assert!(load_legacy_fixture(&missing, RDB_TYPE_STREAM_LISTPACKS_2).is_none());

        let mut duplicate_spec =
            legacy_fixture_spec(RDB_TYPE_STREAM_LISTPACKS_2, StreamID::new(1, 1));
        let pending_id = duplicate_spec.groups[0].consumers[0].pending[0];
        duplicate_spec.groups[0].consumers[0]
            .pending
            .push(pending_id);
        let duplicate = encode_legacy_fixture(&duplicate_spec);
        assert!(load_legacy_fixture(&duplicate, RDB_TYPE_STREAM_LISTPACKS_2).is_none());

        let mut unassigned_spec =
            legacy_fixture_spec(RDB_TYPE_STREAM_LISTPACKS_2, StreamID::new(1, 1));
        unassigned_spec.groups[0].consumers[0].pending.clear();
        let unassigned = encode_legacy_fixture(&unassigned_spec);
        assert!(load_legacy_fixture(&unassigned, RDB_TYPE_STREAM_LISTPACKS_2).is_none());
    }

    #[test]
    fn legacy_stream_truncation_does_not_advance_position() {
        let spec = legacy_fixture_spec(RDB_TYPE_STREAM_LISTPACKS_2, StreamID::new(1, 1));
        let fixture = encode_legacy_fixture(&spec);
        let cuts = std::iter::once(1)
            .chain(fixture.phase_ends.iter().copied())
            .chain(std::iter::once(fixture.body.len() - 1));
        for cut in cuts {
            let mut pos = 0;
            assert!(StreamCodec::load_dispatch(
                &fixture.body[..cut],
                &mut pos,
                RDB_TYPE_STREAM_LISTPACKS_2
            )
            .is_none());
            assert_eq!(pos, 0, "position advanced for truncation at byte {cut}");
        }
    }

    #[test]
    fn legacy_stream_v1_and_v2_preserve_group_metadata() {
        for type_byte in [RDB_TYPE_STREAM_LISTPACKS, RDB_TYPE_STREAM_LISTPACKS_2] {
            let pending_id = StreamID::new(9, 9);
            let spec = legacy_fixture_spec(type_byte, pending_id);
            let fixture = encode_legacy_fixture(&spec);
            let stream = load_legacy_fixture(&fixture, type_byte).expect("valid legacy stream");
            assert_eq!(stream.len(), 1);
            assert_eq!(stream.last_id, StreamID::new(1, 1));
            assert_eq!(stream.entries_added, 1);
            assert_eq!(stream.max_deleted_entry_id, StreamID::zero());
            assert!(stream.get_entry(&pending_id).is_none());
            let group = stream.group(b"group").expect("restored group");
            assert_eq!(group.last_id, StreamID::new(1, 1));
            assert_eq!(group.entries_read, Some(1));
            let pending = group.pending.get(&pending_id).unwrap();
            assert_eq!(pending.consumer, Bytes::from_static(b"alice"));
            assert_eq!(pending.delivery_time_ms, 1234);
            assert_eq!(pending.delivery_count, 7);
            let consumer = group.consumers.get(b"alice" as &[u8]).unwrap();
            assert_eq!(consumer.seen_time_ms, 100);
            assert_eq!(consumer.active_time_ms, Some(100));
        }
    }

    #[test]
    fn legacy_stream_v1_and_v2_restore_signed_sequence_delta() {
        for type_byte in [RDB_TYPE_STREAM_LISTPACKS, RDB_TYPE_STREAM_LISTPACKS_2] {
            let spec = sequence_reset_fixture_spec(type_byte);
            let fixture = encode_legacy_fixture(&spec);
            let mut buf = vec![0xaa, 0xbb];
            buf.extend_from_slice(&fixture.body);
            let mut pos = 2;
            let Robj::StreamListpack(stream) =
                StreamCodec::load_dispatch(&buf, &mut pos, type_byte).expect("valid signed delta")
            else {
                panic!("wrong restored object type");
            };
            assert_eq!(pos, buf.len());
            assert!(stream.get_entry(&StreamID::new(1, 1)).is_some());
            assert!(stream.get_entry(&StreamID::new(2, 0)).is_some());
            assert!(stream.get_entry(&StreamID::new(1, 0)).is_none());
            let group = stream.group(b"group").unwrap();
            assert!(group.pending.contains_key(&StreamID::new(1, 0)));
            if type_byte == RDB_TYPE_STREAM_LISTPACKS_2 {
                assert_eq!(stream.max_deleted_entry_id, StreamID::new(1, 0));
                assert_eq!(stream.entries_added, 3);
            }
        }
    }
}
