use super::CodecLoad;
use crate::foundation::io::persistence::codec::decoder;
use crate::foundation::io::persistence::codec::decoder_compact;
use crate::foundation::io::persistence::codec::types::{
    load_len, QUICKLIST_NODE_CONTAINER_PACKED, QUICKLIST_NODE_CONTAINER_PLAIN, RDB_TYPE_LIST,
    RDB_TYPE_LIST_QUICKLIST, RDB_TYPE_LIST_QUICKLIST_2, RDB_TYPE_LIST_ZIPLIST,
};
use crate::foundation::io::persistence::codec::RdbWriter;
use crate::foundation::io::store::structure::list::{ListLp, ListValue};
use crate::foundation::io::store::Robj;
use crate::foundation::memory::value::ListOps;
use bytes::Bytes;
use std::collections::VecDeque;

pub struct ListCodec;

impl CodecLoad for ListCodec {
    fn load_dispatch(buf: &[u8], pos: &mut usize, type_byte: u8) -> Option<Robj> {
        if type_byte == RDB_TYPE_LIST_ZIPLIST {
            let data = decoder::load_string(buf, pos)?;
            let items = decoder_compact::ziplist_decode(&data)?;
            let mut list = ListLp::new();
            for item in items {
                list.push_back_with_threshold(&item, usize::MAX).ok()?;
            }
            return Some(Robj::ListListpack(list));
        }
        let list = ListCodec::load(buf, pos, type_byte)?;
        Some(Robj::ListQuicklist(ListValue::from_vecdeque(list)))
    }
}

impl ListCodec {
    pub fn save<W: RdbWriter>(val: &ListValue, w: &mut W) {
        w.write_len(val.len() as u64);
        for ele in val.iter() {
            w.write_string(ele);
        }
    }

    pub fn load(buf: &[u8], pos: &mut usize, type_byte: u8) -> Option<VecDeque<Bytes>> {
        match type_byte {
            RDB_TYPE_LIST => {
                let (len, encoded) = load_len(buf, pos)?;
                if encoded {
                    return None;
                }
                let mut list = VecDeque::with_capacity(len as usize);
                for _ in 0..len {
                    list.push_back(Bytes::from(decoder::load_string(buf, pos)?));
                }
                Some(list)
            }
            RDB_TYPE_LIST_ZIPLIST => {
                let data = decoder::load_string(buf, pos)?;
                let items = decoder_compact::ziplist_decode(&data)?;
                Some(items.into_iter().map(Bytes::from).collect())
            }
            RDB_TYPE_LIST_QUICKLIST | RDB_TYPE_LIST_QUICKLIST_2 => {
                load_quicklist(buf, pos, type_byte == RDB_TYPE_LIST_QUICKLIST_2)
            }
            _ => None,
        }
    }
}

fn load_quicklist(buf: &[u8], pos: &mut usize, v2: bool) -> Option<VecDeque<Bytes>> {
    let (nodes, enc) = load_len(buf, pos)?;
    if enc {
        return None;
    }
    let mut list = VecDeque::new();
    for _ in 0..nodes {
        let container = if v2 {
            let (c, ce) = load_len(buf, pos)?;
            if ce {
                return None;
            }
            c
        } else {
            QUICKLIST_NODE_CONTAINER_PACKED
        };
        let data = load_quicklist_node(buf, pos)?;
        if container == QUICKLIST_NODE_CONTAINER_PLAIN {
            list.push_back(Bytes::from(data));
            continue;
        }
        let items = if v2 {
            decoder_compact::listpack_decode(&data)?
        } else {
            decoder_compact::ziplist_decode(&data)?
        };
        for item in items {
            list.push_back(Bytes::from(item));
        }
    }
    Some(list)
}

fn load_quicklist_node(buf: &[u8], pos: &mut usize) -> Option<Vec<u8>> {
    let (len, encoded) = load_len(buf, pos)?;
    if encoded || len == 0 {
        return None;
    }
    let end = pos.checked_add(len as usize)?;
    let data = buf.get(*pos..end)?.to_vec();
    *pos = end;
    Some(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::io::persistence::codec::types::save_len;

    fn enc_string(s: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        save_len(&mut out, s.len() as u64);
        out.extend_from_slice(s);
        out
    }

    #[test]
    fn load_list_rejects_encoded_length() {
        let buf = vec![0xC0u8];
        let mut pos = 0;
        assert!(ListCodec::load(&buf, &mut pos, RDB_TYPE_LIST).is_none());
    }

    #[test]
    fn load_quicklist_v2_plain_node_passes_through_bytes() {
        // v2: 1 node, container=PLAIN, plain string
        let mut buf = Vec::new();
        save_len(&mut buf, 1); // 1 node
        save_len(&mut buf, QUICKLIST_NODE_CONTAINER_PLAIN); // container type
        buf.extend_from_slice(&enc_string(b"plain"));
        let mut pos = 0;
        let list = ListCodec::load(&buf, &mut pos, RDB_TYPE_LIST_QUICKLIST_2).unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0], Bytes::from_static(b"plain"));
    }

    #[test]
    fn load_quicklist_v2_packed_node_decodes_listpack() {
        let items: Vec<Vec<u8>> = vec![b"x".to_vec(), b"y".to_vec()];
        let lp = decoder_compact::listpack_encode(&items);
        let mut buf = Vec::new();
        save_len(&mut buf, 1); // 1 node
        save_len(&mut buf, QUICKLIST_NODE_CONTAINER_PACKED); // container type
        buf.extend_from_slice(&enc_string(&lp));
        let mut pos = 0;
        let list = ListCodec::load(&buf, &mut pos, RDB_TYPE_LIST_QUICKLIST_2).unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list[0], Bytes::from_static(b"x"));
    }

    #[test]
    fn load_quicklist_v2_rejects_zero_length_node() {
        let mut buf = Vec::new();
        save_len(&mut buf, 1);
        save_len(&mut buf, QUICKLIST_NODE_CONTAINER_PLAIN);
        save_len(&mut buf, 0);
        let mut pos = 0;
        assert!(ListCodec::load(&buf, &mut pos, RDB_TYPE_LIST_QUICKLIST_2).is_none());
    }

    #[test]
    fn load_quicklist_v2_rejects_encoded_node_length() {
        let mut buf = Vec::new();
        save_len(&mut buf, 1);
        save_len(&mut buf, QUICKLIST_NODE_CONTAINER_PLAIN);
        buf.push(
            (crate::foundation::io::persistence::codec::types::RDB_ENCVAL << 6)
                | crate::foundation::io::persistence::codec::types::RDB_ENC_INT8,
        );
        buf.push(1);
        let mut pos = 0;
        assert!(ListCodec::load(&buf, &mut pos, RDB_TYPE_LIST_QUICKLIST_2).is_none());
    }

    #[test]
    fn load_list_unknown_type_byte_returns_none() {
        let mut pos = 0;
        assert!(ListCodec::load(&[], &mut pos, 99).is_none());
    }
}
