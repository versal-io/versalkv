use crate::foundation::io::persistence::codec::types::{
    save_len, RDB_ENCVAL, RDB_ENC_INT16, RDB_ENC_INT32, RDB_ENC_INT8, RDB_ENC_LZF,
};
// Re-exported through `rdb` below, so the import itself must be crate-visible.
// It goes through the codec re-export so every LzfWorkspace path shares one entry.
pub(crate) use crate::foundation::io::persistence::codec::LzfWorkspace;
use crate::foundation::io::persistence::codec::RdbWriter;

fn try_save_integer(out: &mut Vec<u8>, s: &[u8]) -> bool {
    if s.is_empty() {
        return false;
    }
    let first = s[0];
    if first != b'-' && !first.is_ascii_digit() {
        return false;
    }
    let value = match crate::foundation::io::store::structure::string::encoding::int_encodable(s) {
        Some(v) => v,
        None => return false,
    };
    if (-(1 << 7)..=(1 << 7) - 1).contains(&value) {
        let buf = [(RDB_ENCVAL << 6) | RDB_ENC_INT8, (value & 0xff) as u8];
        out.extend_from_slice(&buf);
    } else if (-(1 << 15)..=(1 << 15) - 1).contains(&value) {
        let v = value as u16;
        let buf = [
            (RDB_ENCVAL << 6) | RDB_ENC_INT16,
            (v & 0xff) as u8,
            ((v >> 8) & 0xff) as u8,
        ];
        out.extend_from_slice(&buf);
    } else if (-(1 << 31)..=(1 << 31) - 1).contains(&value) {
        let v = value as u32;
        let mut buf = [0u8; 5];
        buf[0] = (RDB_ENCVAL << 6) | RDB_ENC_INT32;
        buf[1] = (v & 0xff) as u8;
        buf[2] = ((v >> 8) & 0xff) as u8;
        buf[3] = ((v >> 16) & 0xff) as u8;
        buf[4] = ((v >> 24) & 0xff) as u8;
        out.extend_from_slice(&buf);
    } else {
        return false;
    }
    true
}

fn write_raw_string(out: &mut Vec<u8>, s: &[u8]) {
    save_len(out, s.len() as u64);
    out.extend_from_slice(s);
}

pub(crate) fn save_raw_string(out: &mut Vec<u8>, s: &[u8], ws: &mut LzfWorkspace) {
    let len = s.len();
    if len <= 11 && try_save_integer(out, s) {
        return;
    }

    if len > 20 {
        if let Some(compressed) = ws.compress(s, len - 4) {
            if !compressed.is_empty() {
                out.push((RDB_ENCVAL << 6) | RDB_ENC_LZF);
                save_len(out, compressed.len() as u64);
                save_len(out, len as u64);
                out.extend_from_slice(compressed);
                return;
            }
        }
    }

    write_raw_string(out, s);
}

pub(crate) struct RdbVecWriter<'a> {
    out: &'a mut Vec<u8>,
    ws: &'a mut LzfWorkspace,
}

impl<'a> RdbVecWriter<'a> {
    pub(crate) fn new(out: &'a mut Vec<u8>, ws: &'a mut LzfWorkspace) -> Self {
        Self { out, ws }
    }
}

impl RdbWriter for RdbVecWriter<'_> {
    fn write_len(&mut self, len: u64) {
        save_len(self.out, len);
    }

    fn write_string(&mut self, s: &[u8]) {
        save_raw_string(self.out, s, self.ws);
    }

    fn write_binary_double(&mut self, val: f64) {
        save_binary_double(self.out, val);
    }

    fn write_millisecond_time(&mut self, t: i64) {
        save_millisecond_time(self.out, t);
    }

    fn write_raw(&mut self, bytes: &[u8]) {
        self.out.extend_from_slice(bytes);
    }
}

/// Unconfigured RDB string writes retain the historical uncompressed path.
/// Production snapshot owners use `RdbVecWriter` with an explicit workspace.
pub fn save_string(out: &mut Vec<u8>, s: &[u8]) {
    let len = s.len();
    if len <= 11 && try_save_integer(out, s) {
        return;
    }
    write_raw_string(out, s);
}

pub fn save_millisecond_time(out: &mut Vec<u8>, t: i64) {
    out.extend_from_slice(&t.to_le_bytes());
}

pub fn save_binary_double(out: &mut Vec<u8>, val: f64) {
    out.extend_from_slice(&val.to_le_bytes());
}

impl RdbWriter for Vec<u8> {
    fn write_len(&mut self, len: u64) {
        save_len(self, len);
    }

    fn write_string(&mut self, s: &[u8]) {
        save_string(self, s);
    }

    fn write_binary_double(&mut self, val: f64) {
        save_binary_double(self, val);
    }

    fn write_millisecond_time(&mut self, t: i64) {
        save_millisecond_time(self, t);
    }

    fn write_raw(&mut self, bytes: &[u8]) {
        self.extend_from_slice(bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn save_string_encodes_integers_and_falls_back_to_raw() {
        let mut buf = Vec::new();
        save_string(&mut buf, b"0");
        assert_eq!(buf, vec![(RDB_ENCVAL << 6) | RDB_ENC_INT8, 0]);

        buf.clear();
        save_string(&mut buf, b"127");
        assert_eq!(buf, vec![(RDB_ENCVAL << 6) | RDB_ENC_INT8, 127]);

        buf.clear();
        save_string(&mut buf, b"-128");
        assert_eq!(buf, vec![(RDB_ENCVAL << 6) | RDB_ENC_INT8, 0x80]);

        buf.clear();
        save_string(&mut buf, b"128");
        assert_eq!(buf[0], (RDB_ENCVAL << 6) | RDB_ENC_INT16);

        buf.clear();
        save_string(&mut buf, b"32768");
        assert_eq!(buf[0], (RDB_ENCVAL << 6) | RDB_ENC_INT32);

        // non-integer string → raw
        buf.clear();
        save_string(&mut buf, b"not-a-number");
        assert_eq!(buf[0], 12); // length prefix
        assert_eq!(&buf[1..], b"not-a-number");
    }

    #[test]
    fn save_millisecond_time_writes_little_endian() {
        let mut buf = Vec::new();
        save_millisecond_time(&mut buf, 0x0102030405060708);
        assert_eq!(buf, vec![0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01]);
    }

    #[test]
    fn save_binary_double_writes_ieee754_little_endian() {
        let mut buf = Vec::new();
        save_binary_double(&mut buf, 1.0);
        assert_eq!(buf.len(), 8);
        assert_eq!(f64::from_le_bytes(buf.try_into().unwrap()), 1.0);
    }

    #[test]
    fn rdb_writer_vec_impl_delegates_to_save_primitives() {
        let mut buf: Vec<u8> = Vec::new();
        RdbWriter::write_len(&mut buf, 5);
        assert_eq!(buf, vec![5]);

        buf.clear();
        RdbWriter::write_string(&mut buf, b"42");
        assert_eq!(buf, vec![(RDB_ENCVAL << 6) | RDB_ENC_INT8, 42]);

        buf.clear();
        RdbWriter::write_binary_double(&mut buf, 2.0);
        assert_eq!(buf.len(), 8);
    }
}
