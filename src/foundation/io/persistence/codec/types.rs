use std::sync::OnceLock;

pub const RDB_VERSION: u16 = 81;

pub const RDB_TYPE_STRING: u8 = 0;
pub const RDB_TYPE_LIST: u8 = 1;
pub const RDB_TYPE_SET: u8 = 2;
pub const RDB_TYPE_ZSET: u8 = 3;
pub const RDB_TYPE_HASH: u8 = 4;
pub const RDB_TYPE_ZSET_2: u8 = 5;
pub const RDB_TYPE_HASH_ZIPMAP: u8 = 9;
pub const RDB_TYPE_LIST_ZIPLIST: u8 = 10;
pub const RDB_TYPE_SET_INTSET: u8 = 11;
pub const RDB_TYPE_ZSET_ZIPLIST: u8 = 12;
pub const RDB_TYPE_HASH_ZIPLIST: u8 = 13;
pub const RDB_TYPE_LIST_QUICKLIST: u8 = 14;
pub const RDB_TYPE_STREAM_LISTPACKS: u8 = 15;
pub const RDB_TYPE_HASH_LISTPACK: u8 = 16;
pub const RDB_TYPE_ZSET_LISTPACK: u8 = 17;
pub const RDB_TYPE_LIST_QUICKLIST_2: u8 = 18;
pub const RDB_TYPE_STREAM_LISTPACKS_2: u8 = 19;
pub const RDB_TYPE_SET_LISTPACK: u8 = 20;
pub const RDB_TYPE_STREAM_LISTPACKS_3: u8 = 21;
pub const RDB_TYPE_HASH_2: u8 = 22;
pub const RDB_TYPE_PATH_HASH: u8 = 23;

pub(crate) const QUICKLIST_NODE_CONTAINER_PLAIN: u64 = 1;
pub(crate) const QUICKLIST_NODE_CONTAINER_PACKED: u64 = 2;

pub(crate) const RDB_FOREIGN_VERSION_MIN: u16 = 12;
pub(crate) const RDB_FOREIGN_VERSION_MAX: u16 = 79;
#[cfg(not(feature = "wasm"))]
pub(crate) const RDB_FOREIGN_TYPE_MIN: u8 = 22;
#[cfg(not(feature = "wasm"))]
pub(crate) const RDB_FOREIGN_TYPE_MAX: u8 = 243;

pub const RDB_OPCODE_IDLE: u8 = 248;
pub const RDB_OPCODE_FREQ: u8 = 249;
pub const RDB_OPCODE_AUX: u8 = 250;
pub const RDB_OPCODE_FUNCTION2: u8 = 245;
pub const RDB_OPCODE_RESIZEDB: u8 = 251;
pub const RDB_OPCODE_EXPIRETIME_MS: u8 = 252;
pub const RDB_OPCODE_EXPIRETIME: u8 = 253;
pub const RDB_OPCODE_SELECTDB: u8 = 254;
pub const RDB_OPCODE_EOF: u8 = 255;

pub(crate) fn is_rdb_version_accepted(
    rdbver: i32,
    is_valkey: bool,
    is_redis: bool,
    strict: bool,
) -> bool {
    if rdbver < 1 {
        return false;
    }
    if is_valkey && rdbver <= RDB_FOREIGN_VERSION_MAX as i32 {
        return false;
    }
    if is_redis && rdbver > RDB_FOREIGN_VERSION_MAX as i32 {
        return false;
    }
    if strict
        && (rdbver > RDB_VERSION as i32
            || (RDB_FOREIGN_VERSION_MIN as i32..=RDB_FOREIGN_VERSION_MAX as i32).contains(&rdbver))
    {
        return false;
    }
    true
}

pub(crate) const RDB_6BITLEN: u8 = 0;
pub(crate) const RDB_14BITLEN: u8 = 1;
pub(crate) const RDB_32BITLEN: u8 = 0x80;
pub(crate) const RDB_64BITLEN: u8 = 0x81;
pub(crate) const RDB_ENCVAL: u8 = 3;

pub(crate) const RDB_ENC_INT8: u8 = 0;
pub(crate) const RDB_ENC_INT16: u8 = 1;
pub(crate) const RDB_ENC_INT32: u8 = 2;
pub(crate) const RDB_ENC_LZF: u8 = 3;

const CRC64_POLY_REV: u64 = 0x95ac9329ac4bc9b5;

fn crc64_table() -> &'static [u64; 256] {
    static TABLE: OnceLock<[u64; 256]> = OnceLock::new();
    TABLE.get_or_init(|| {
        let mut table = [0u64; 256];
        let mut i = 0;
        while i < 256 {
            let mut crc = i as u64;
            let mut j = 0;
            while j < 8 {
                if crc & 1 != 0 {
                    crc = (crc >> 1) ^ CRC64_POLY_REV;
                } else {
                    crc >>= 1;
                }
                j += 1;
            }
            table[i] = crc;
            i += 1;
        }
        table
    })
}

pub fn crc64(mut crc: u64, data: &[u8]) -> u64 {
    let table = crc64_table();
    let mut i = 0;
    let len = data.len();
    while i + 8 <= len {
        let b0 = data[i] as u64;
        let b1 = data[i + 1] as u64;
        let b2 = data[i + 2] as u64;
        let b3 = data[i + 3] as u64;
        let b4 = data[i + 4] as u64;
        let b5 = data[i + 5] as u64;
        let b6 = data[i + 6] as u64;
        let b7 = data[i + 7] as u64;
        crc = table[((crc ^ b0) & 0xff) as usize] ^ (crc >> 8);
        crc = table[((crc ^ b1) & 0xff) as usize] ^ (crc >> 8);
        crc = table[((crc ^ b2) & 0xff) as usize] ^ (crc >> 8);
        crc = table[((crc ^ b3) & 0xff) as usize] ^ (crc >> 8);
        crc = table[((crc ^ b4) & 0xff) as usize] ^ (crc >> 8);
        crc = table[((crc ^ b5) & 0xff) as usize] ^ (crc >> 8);
        crc = table[((crc ^ b6) & 0xff) as usize] ^ (crc >> 8);
        crc = table[((crc ^ b7) & 0xff) as usize] ^ (crc >> 8);
        i += 8;
    }
    while i < len {
        crc = table[((crc ^ data[i] as u64) & 0xff) as usize] ^ (crc >> 8);
        i += 1;
    }
    crc
}

pub fn save_len(out: &mut Vec<u8>, len: u64) {
    if len < (1 << 6) {
        out.push((len as u8) | (RDB_6BITLEN << 6));
    } else if len < (1 << 14) {
        let buf = [((len >> 8) as u8) | (RDB_14BITLEN << 6), (len & 0xff) as u8];
        out.extend_from_slice(&buf);
    } else if len <= u32::MAX as u64 {
        let mut buf = [0u8; 5];
        buf[0] = RDB_32BITLEN;
        buf[1..5].copy_from_slice(&(len as u32).to_be_bytes());
        out.extend_from_slice(&buf);
    } else {
        let mut buf = [0u8; 9];
        buf[0] = RDB_64BITLEN;
        buf[1..9].copy_from_slice(&len.to_be_bytes());
        out.extend_from_slice(&buf);
    }
}

pub fn load_len(buf: &[u8], pos: &mut usize) -> Option<(u64, bool)> {
    let b0 = *buf.get(*pos)?;
    *pos += 1;
    let kind = (b0 & 0xC0) >> 6;
    match kind {
        k if k == RDB_ENCVAL => Some(((b0 & 0x3F) as u64, true)),
        RDB_6BITLEN => Some(((b0 & 0x3F) as u64, false)),
        RDB_14BITLEN => {
            let b1 = *buf.get(*pos)?;
            *pos += 1;
            Some(((((b0 & 0x3F) as u64) << 8) | b1 as u64, false))
        }
        _ if b0 == RDB_32BITLEN => {
            let bytes: [u8; 4] = buf.get(*pos..*pos + 4)?.try_into().ok()?;
            *pos += 4;
            Some((u32::from_be_bytes(bytes) as u64, false))
        }
        _ if b0 == RDB_64BITLEN => {
            let bytes: [u8; 8] = buf.get(*pos..*pos + 8)?.try_into().ok()?;
            *pos += 8;
            Some((u64::from_be_bytes(bytes), false))
        }
        _ => None,
    }
}

pub(crate) fn parse_double(b: &[u8]) -> Option<f64> {
    let s = std::str::from_utf8(b).ok()?.trim();
    match s.to_ascii_lowercase().as_str() {
        "inf" | "+inf" | "infinity" | "+infinity" => Some(f64::INFINITY),
        "-inf" | "-infinity" => Some(f64::NEG_INFINITY),
        _ => s.parse::<f64>().ok(),
    }
}

#[cfg(any(not(feature = "wasm"), test))]
pub(crate) fn is_known_object_type(t: u8) -> bool {
    matches!(
        t,
        RDB_TYPE_STRING
            | RDB_TYPE_LIST
            | RDB_TYPE_SET
            | RDB_TYPE_ZSET
            | RDB_TYPE_HASH
            | RDB_TYPE_HASH_2
            | RDB_TYPE_PATH_HASH
            | RDB_TYPE_ZSET_2
            | RDB_TYPE_HASH_ZIPMAP
            | RDB_TYPE_LIST_ZIPLIST
            | RDB_TYPE_SET_INTSET
            | RDB_TYPE_ZSET_ZIPLIST
            | RDB_TYPE_HASH_ZIPLIST
            | RDB_TYPE_LIST_QUICKLIST
            | RDB_TYPE_STREAM_LISTPACKS
            | RDB_TYPE_HASH_LISTPACK
            | RDB_TYPE_ZSET_LISTPACK
            | RDB_TYPE_LIST_QUICKLIST_2
            | RDB_TYPE_STREAM_LISTPACKS_2
            | RDB_TYPE_SET_LISTPACK
            | RDB_TYPE_STREAM_LISTPACKS_3
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_rdb_types_are_known_object_types() {
        assert!(is_known_object_type(RDB_TYPE_STREAM_LISTPACKS));
        assert!(is_known_object_type(RDB_TYPE_STREAM_LISTPACKS_2));
        assert!(is_known_object_type(RDB_TYPE_STREAM_LISTPACKS_3));
    }
}
