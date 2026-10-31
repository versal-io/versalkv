use crate::engine::command::context::CommandConn;
use crate::foundation::io::persistence::codec::decoder_compact;
use crate::foundation::io::persistence::codec::types::QUICKLIST_NODE_CONTAINER_PACKED;
use crate::foundation::io::persistence::codec::RdbWriter;
use crate::foundation::io::persistence::rdb::save_raw_string;
use crate::foundation::io::persistence::save_robj;
use crate::foundation::io::protocol::Value;
use crate::foundation::io::store::structure::list::ListValue;
use crate::foundation::io::store::structure::stream::groups::StreamGroup;
use crate::foundation::io::store::structure::stream::{StreamID, ValkeyStream};
use crate::foundation::io::store::Robj;
use crate::framework::commands::generic::object::{encoding_display_name, ObjectEncodingLimits};
use crate::framework::context::ServerContext;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use std::collections::HashMap;

use super::dataset::fpconv_format;

pub(super) fn debug_object(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    if !matches!(args.args.len(), 2 | 3) {
        return CommandError::syntax_error();
    }
    let fast = args.args.len() == 3 && args.args[2].eq_ignore_ascii_case(b"fast");

    let key = args.args[1].clone();
    let db_idx = conn.selected_db();
    let (obj, field_expires, eviction, hash_has_string_refs) = {
        let db = match ctx.db(db_idx) {
            Some(d) => d,
            None => return Value::error("ERR DB index is out of range"),
        };
        let entry = match db.clone_entry_allow_expired(&key) {
            None => return Value::error("ERR no such key"),
            Some(entry) => entry,
        };
        let eviction = if is_lfu_policy(ctx) {
            EvictionView::Lfu {
                freq: db.get_lfu_freq(&key),
                minutes: db.get_lfu_access_minutes(&key).unwrap_or(0),
            }
        } else {
            EvictionView::Lru {
                raw: db.get_lru_raw(&key),
                idle: db.get_lru_idle(&key),
            }
        };
        (
            entry.0,
            entry.2,
            eviction,
            db.hash_key_has_string_refs(&key),
        )
    };
    let hash_requires_hashtable = !field_expires.is_empty() || hash_has_string_refs;
    let enc = encoding_display_name(&obj, object_encoding_limits(ctx), hash_requires_hashtable);

    let mut line = format!("Value at:0x0000000000000000 refcount:1 encoding:{}", enc);
    if !fast {
        let fill = ctx.config().encoding.list_max_listpack_size;
        let compress = ctx.config().persistence.rdbcompression.uses_lzf();
        let stream_max_entries = ctx.config().encoding.stream_node_max_entries;
        let length = serialized_length(
            &obj,
            enc,
            &field_expires,
            fill,
            compress,
            stream_max_entries,
        );
        line.push_str(&format!(" serializedlength:{}", length));
    }
    match eviction {
        EvictionView::Lfu { freq, minutes } => {
            line.push_str(&format!(
                " lfu_freq:{} lfu_access_time_minutes:{}",
                freq, minutes
            ));
        }
        EvictionView::Lru { raw, idle } => {
            line.push_str(&format!(" lru:{} lru_seconds_idle:{}", raw, idle));
        }
    }
    if matches!(obj, Robj::ListQuicklist(_)) && enc == "quicklist" {
        line.push_str(&quicklist_extras(&obj, ctx, fast));
    }
    Value::SimpleString(line.into())
}

fn object_encoding_limits(ctx: &ServerContext) -> ObjectEncodingLimits {
    ObjectEncodingLimits {
        list_max_listpack_size: ctx.config().encoding.list_max_listpack_size,
        hash_max_listpack_entries: ctx.config().encoding.hash_max_listpack_entries as usize,
        hash_max_listpack_value: ctx.config().encoding.hash_max_listpack_value as usize,
    }
}

enum EvictionView {
    Lru { raw: i64, idle: i64 },
    Lfu { freq: u8, minutes: u32 },
}

fn is_lfu_policy(ctx: &ServerContext) -> bool {
    crate::foundation::io::store::kv_engine::EvictionPolicy::from_name(
        &ctx.config().server.maxmemory_policy,
    )
    .is_some_and(|policy| policy.uses_lfu())
}

struct DebugRdbWriter {
    buf: Vec<u8>,
    compress: bool,
}

impl DebugRdbWriter {
    fn new(compress: bool) -> Self {
        Self {
            buf: Vec::new(),
            compress,
        }
    }
}

impl RdbWriter for DebugRdbWriter {
    fn write_len(&mut self, len: u64) {
        self.buf.write_len(len);
    }
    fn write_string(&mut self, s: &[u8]) {
        save_raw_string(&mut self.buf, s, self.compress);
    }
    fn write_binary_double(&mut self, val: f64) {
        self.buf.write_binary_double(val);
    }
    fn write_millisecond_time(&mut self, t: i64) {
        self.buf.write_millisecond_time(t);
    }
    fn write_raw(&mut self, bytes: &[u8]) {
        self.buf.write_raw(bytes);
    }
}

fn serialized_length(
    obj: &Robj,
    enc: &str,
    field_expires: &HashMap<Vec<u8>, u64>,
    fill: i64,
    compress: bool,
    stream_max_entries: i64,
) -> usize {
    let mut w = DebugRdbWriter::new(compress);
    match obj {
        Robj::ListQuicklist(list) if enc == "listpack" => save_listpack_string(list, &mut w),
        Robj::ListQuicklist(list) if enc == "quicklist" => save_quicklist2_body(list, fill, &mut w),
        Robj::ListListpack(list) => {
            let items = list.all_elements();
            save_list_listpack(&items, &mut w);
        }
        Robj::HashHashtable(_) if !field_expires.is_empty() => {
            crate::foundation::io::persistence::save_robj_with_hash_field_expires(
                obj,
                Some(field_expires),
                &mut w,
            );
        }
        Robj::HashHashtable(hash) if enc == "listpack" => {
            let items: Vec<Vec<u8>> = hash
                .data
                .iter()
                .flat_map(|(field, value)| [field.to_vec(), value.to_vec()])
                .collect();
            w.write_string(&decoder_compact::listpack_encode(&items));
        }
        Robj::HashListpack(hash) => {
            let items: Vec<Vec<u8>> = hash
                .all_pairs()
                .into_iter()
                .flat_map(|(field, value)| [field.to_vec(), value.to_vec()])
                .collect();
            w.write_string(&decoder_compact::listpack_encode(&items));
        }
        Robj::ZSetSkiplist(zset) if enc == "listpack" => {
            save_zset_listpack(zset.sorted_entries(), &mut w)
        }
        Robj::ZSetListpack(zset) => save_zset_listpack(zset.all_pairs(), &mut w),
        Robj::StreamListpack(stream) => save_stream_body(stream, stream_max_entries, &mut w),
        _ => save_robj(obj, &mut w),
    }
    w.buf.len()
}

/// `[node-count=1][container=PACKED][rawstring(listpack)]`
fn save_list_listpack<W: RdbWriter>(items: &[Vec<u8>], w: &mut W) {
    w.write_len(1);
    w.write_len(QUICKLIST_NODE_CONTAINER_PACKED);
    w.write_string(&decoder_compact::listpack_encode(items));
}

fn save_listpack_string<W: RdbWriter>(list: &ListValue, w: &mut W) {
    let items: Vec<Vec<u8>> = list.iter().map(|b| b.to_vec()).collect();
    save_list_listpack(&items, w);
}

fn save_zset_listpack<W, I, B>(entries: I, w: &mut W)
where
    W: RdbWriter,
    I: IntoIterator<Item = (B, f64)>,
    B: AsRef<[u8]>,
{
    let items: Vec<Vec<u8>> = entries
        .into_iter()
        .flat_map(|(member, score)| [member.as_ref().to_vec(), fpconv_format(score).into_bytes()])
        .collect();
    w.write_string(&decoder_compact::listpack_encode(&items));
}

#[derive(Clone, Copy, PartialEq)]
enum Container {
    Packed,
    Plain,
}

struct QlNode {
    container: Container,
    items: Vec<Vec<u8>>,
    sz: usize,
}

impl QlNode {
    fn payload(&self) -> Vec<u8> {
        if self.container == Container::Plain {
            self.items[0].clone()
        } else {
            decoder_compact::listpack_encode(&self.items)
        }
    }
}

fn pack_quicklist(list: &ListValue, fill: i64) -> Vec<QlNode> {
    let mut nodes: Vec<QlNode> = Vec::new();
    for ele in list.iter() {
        let length = ele.len();
        if is_large_element(length, fill) {
            nodes.push(QlNode {
                container: Container::Plain,
                items: vec![ele.to_vec()],
                sz: length,
            });
            continue;
        }
        let can_append = match nodes.last() {
            Some(node) if node.container == Container::Packed => {
                !exceeds_limit(fill, node.sz + length + 8, node.items.len() + 1)
            }
            _ => false,
        };
        if can_append {
            let node = nodes.last_mut().expect("packed tail exists");
            node.items.push(ele.to_vec());
            node.sz = decoder_compact::listpack_encode(&node.items).len();
        } else {
            let items = vec![ele.to_vec()];
            let sz = decoder_compact::listpack_encode(&items).len();
            nodes.push(QlNode {
                container: Container::Packed,
                items,
                sz,
            });
        }
    }
    nodes
}

/// C quicklistNodeExceedsLimit
fn exceeds_limit(fill: i64, new_sz: usize, new_count: usize) -> bool {
    if fill < 0 {
        return new_sz > neg_fill_limit(fill);
    }
    new_sz > 8192 || new_count > fill.max(1) as usize
}

fn is_large_element(length: usize, fill: i64) -> bool {
    if fill < 0 {
        length > neg_fill_limit(fill)
    } else {
        length > 8192
    }
}

/// C quicklistNodeNegFillLimit：-1→4KB … -5→64KB
fn neg_fill_limit(fill: i64) -> usize {
    const LEVELS: [usize; 5] = [4096, 8192, 16384, 32768, 65536];
    let offset = ((-fill) - 1).clamp(0, LEVELS.len() as i64 - 1) as usize;
    LEVELS[offset]
}

/// `[node-count]` per node `[container][payload-len][payload]`
fn save_quicklist2_body<W: RdbWriter>(list: &ListValue, fill: i64, w: &mut W) {
    let nodes = pack_quicklist(list, fill);
    w.write_len(nodes.len() as u64);
    for node in &nodes {
        let container = match node.container {
            Container::Plain => 1,  // QUICKLIST_NODE_CONTAINER_PLAIN
            Container::Packed => 2, // QUICKLIST_NODE_CONTAINER_PACKED
        };
        w.write_len(container);
        let payload = node.payload();
        w.write_string(&payload);
    }
}

fn quicklist_extras(obj: &Robj, ctx: &ServerContext, fast: bool) -> String {
    let Robj::ListQuicklist(list) = obj else {
        return String::new();
    };
    let fill = ctx.config().encoding.list_max_listpack_size;
    let nodes = pack_quicklist(list, fill);
    let total = list.len();
    let avg = total as f64 / nodes.len().max(1) as f64;
    let compressed = i32::from(ctx.config().encoding.list_compress_depth != 0);

    let mut extra = format!(
        " ql_nodes:{} ql_avg_node:{:.2} ql_listpack_max:{} ql_compressed:{}",
        nodes.len(),
        avg,
        fill,
        compressed
    );
    if !fast {
        let size: usize = nodes.iter().map(|node| node.sz).sum();
        extra.push_str(&format!(" ql_uncompressed_size:{}", size));
    }
    extra
}

const STREAM_NODE_MAX_BYTES: usize = 4096;

fn save_stream_body<W: RdbWriter>(stream: &ValkeyStream, max_entries: i64, w: &mut W) {
    let nodes = split_stream_nodes(stream, max_entries);
    w.write_len(nodes.len() as u64);
    for node in &nodes {
        w.write_string(&node.key);
        w.write_string(&node.lp);
    }
    w.write_len(stream.len() as u64);
    w.write_len(stream.last_id.ms);
    w.write_len(stream.last_id.seq);
    let first = stream.first_id().unwrap_or_else(StreamID::zero);
    w.write_len(first.ms);
    w.write_len(first.seq);
    w.write_len(stream.max_deleted_entry_id.ms);
    w.write_len(stream.max_deleted_entry_id.seq);
    w.write_len(stream.entries_added);
    w.write_len(stream.groups.len() as u64);
    for group in stream.groups.values() {
        save_stream_group(group, w);
    }
}

fn split_stream_nodes(stream: &ValkeyStream, max_entries: i64) -> Vec<MacroNode> {
    let mut nodes: Vec<MacroNode> = Vec::new();
    for (id, fields) in stream.iter() {
        let raw_len: usize = fields.iter().map(|(f, v)| f.len() + v.len()).sum();
        let need_new = match nodes.last() {
            None => true,
            Some(node) => {
                node.lp.len() + raw_len >= STREAM_NODE_MAX_BYTES
                    || (max_entries > 0 && node.entries.len() as i64 >= max_entries)
            }
        };
        if need_new {
            nodes.push(MacroNode {
                key: encode_stream_id(id).to_vec(),
                entries: vec![(*id, fields.clone())],
                lp: Vec::new(),
            });
        } else {
            nodes
                .last_mut()
                .expect("tail node")
                .entries
                .push((*id, fields.clone()));
        }
    }
    for node in &mut nodes {
        let primary = node.entries[0].0;
        node.lp = build_node_lp(primary, &node.entries);
    }
    nodes
}

struct MacroNode {
    key: Vec<u8>,
    entries: Vec<(StreamID, Vec<(Bytes, Bytes)>)>,
    lp: Vec<u8>,
}

fn encode_stream_id(id: &StreamID) -> [u8; 16] {
    let mut buf = [0u8; 16];
    buf[0..8].copy_from_slice(&id.ms.to_be_bytes());
    buf[8..16].copy_from_slice(&id.seq.to_be_bytes());
    buf
}

fn build_node_lp(primary: StreamID, entries: &[(StreamID, Vec<(Bytes, Bytes)>)]) -> Vec<u8> {
    let master = &entries[0].1;
    let mut items: Vec<Vec<u8>> = Vec::new();
    items.push(entries.len().to_string().into_bytes());
    items.push(b"0".to_vec());
    items.push(master.len().to_string().into_bytes());
    for (field, _) in master {
        items.push(field.to_vec());
    }
    items.push(b"0".to_vec());

    for (id, fields) in entries {
        let same = fields_same(master, fields);
        items.push(if same { b"2".to_vec() } else { b"0".to_vec() });
        items.push((id.ms - primary.ms).to_string().into_bytes());
        items.push((id.seq - primary.seq).to_string().into_bytes());
        if same {
            for (_, value) in fields {
                items.push(value.to_vec());
            }
        } else {
            items.push(fields.len().to_string().into_bytes());
            for (field, value) in fields {
                items.push(field.to_vec());
                items.push(value.to_vec());
            }
        }
        let lp_count = if same {
            3 + fields.len()
        } else {
            3 + 1 + fields.len() * 2
        };
        items.push(lp_count.to_string().into_bytes());
    }
    decoder_compact::listpack_encode(&items)
}

fn fields_same(master: &[(Bytes, Bytes)], fields: &[(Bytes, Bytes)]) -> bool {
    master.len() == fields.len() && master.iter().zip(fields).all(|((a, _), (b, _))| a == b)
}

fn save_stream_group<W: RdbWriter>(group: &StreamGroup, w: &mut W) {
    w.write_string(&group.name);
    w.write_len(group.last_id.ms);
    w.write_len(group.last_id.seq);
    w.write_len(group.entries_read.unwrap_or(u64::MAX));

    w.write_len(group.pending.len() as u64);
    for (id, nack) in &group.pending {
        write_raw_id(id, w);
        w.write_millisecond_time(nack.delivery_time_ms);
        w.write_len(nack.delivery_count);
    }

    w.write_len(group.consumers.len() as u64);
    for consumer in group.consumers.values() {
        w.write_string(&consumer.name);
        w.write_millisecond_time(consumer.seen_time_ms);
        w.write_millisecond_time(consumer.active_time_ms.unwrap_or(-1));
        w.write_len(consumer.pending.len() as u64);
        for id in &consumer.pending {
            write_raw_id(id, w);
        }
    }
}

fn write_raw_id<W: RdbWriter>(id: &StreamID, w: &mut W) {
    w.write_raw(&encode_stream_id(id));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::framework::capabilities::connection::Connection;

    #[test]
    fn debug_object_reports_stringref_hashtable_with_and_without_fast_mode() {
        let mut ctx = ServerContext::new(1);
        let db = ctx.db_mut(0).unwrap();
        db.hash_set_with_limits(b"hash", b"field", b"value", 512, 64)
            .unwrap();
        assert!(db.set_hash_string_ref(b"hash", b"field", b"value"));
        assert!(!db.hash_key_has_volatile_fields(b"hash"));
        let mut conn = Connection::new_fake();
        for is_fast in [false, true] {
            let mut parts = vec!["DEBUG".into(), "OBJECT".into(), "hash".into()];
            if is_fast {
                parts.push("FAST".into());
            }
            let result = debug_object(&mut conn, &CommandArgs::from_parts(parts), &mut ctx);
            let Value::SimpleString(line) = result else {
                panic!("expected a debug object description");
            };
            assert!(std::str::from_utf8(&line)
                .unwrap()
                .contains("encoding:hashtable"));
        }
    }
}
