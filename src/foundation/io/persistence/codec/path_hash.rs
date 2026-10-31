use super::{decoder, types::load_len, CodecLoad, RdbWriter};
use crate::foundation::io::store::structure::path_hash::{PathHash, PathHashPayload};
use crate::foundation::io::store::Robj;
use bytes::Bytes;

pub struct PathHashCodec;

impl PathHashCodec {
    pub fn save<W: RdbWriter>(hash: &PathHash, writer: &mut W) {
        writer.write_len(hash.len() as u64);
        for (path, payload) in hash.iter() {
            writer.write_string(path);
            writer.write_len(payload.len() as u64);
            for (field, value) in payload.entries() {
                writer.write_string(&field);
                writer.write_string(&value);
            }
        }
    }

    pub(super) fn load_with_limits(
        buf: &[u8],
        pos: &mut usize,
        max_entries: usize,
        max_value_size: usize,
    ) -> Option<PathHash> {
        let (path_count, encoded) = load_len(buf, pos)?;
        let path_count = usize::try_from(path_count).ok()?;
        // Every path requires a string header, field count, and at least one
        // field/value header. Validate before creating any payload.
        if encoded || path_count > buf.len().checked_sub(*pos)? / 4 {
            return None;
        }
        let mut hash = PathHash::new();
        for _ in 0..path_count {
            let path = Bytes::from(decoder::load_string(buf, pos)?);
            if hash.contains_path(&path) {
                return None;
            }
            let (field_count, encoded) = load_len(buf, pos)?;
            let field_count = usize::try_from(field_count).ok()?;
            if encoded || field_count == 0 || field_count > buf.len().checked_sub(*pos)? / 2 {
                return None;
            }
            let mut payload = PathHashPayload::default();
            for _ in 0..field_count {
                let field = Bytes::from(decoder::load_string(buf, pos)?);
                let value = Bytes::from(decoder::load_string(buf, pos)?);
                if payload.contains(&field) {
                    return None;
                }
                payload.set_pairs(&[(field, value)], max_entries, max_value_size);
            }
            if !hash.insert_checked(path, payload) {
                return None;
            }
        }
        Some(hash)
    }
}

impl CodecLoad for PathHashCodec {
    fn load_dispatch(buf: &[u8], pos: &mut usize, _type_byte: u8) -> Option<Robj> {
        Self::load_with_limits(buf, pos, 512, 64).map(|hash| Robj::PathHash(Box::new(hash)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_hash_load_builds_index_and_rejects_invalid_entries() {
        let pairs = [(Bytes::from_static(b"f"), Bytes::from_static(b"v"))];
        let mut hash = PathHash::new();
        for path in [b"".as_slice(), b"bb", b"b\0", b"bbbbb"] {
            hash.set_pairs(path, &pairs, 2, 64);
        }
        let mut bytes = Vec::new();
        PathHashCodec::save(&hash, &mut bytes);
        for max_entries in [0, 512] {
            let mut pos = 0;
            let mut loaded =
                PathHashCodec::load_with_limits(&bytes, &mut pos, max_entries, 64).unwrap();
            assert_eq!(pos, bytes.len());
            assert_eq!(
                loaded
                    .prefix_matches(b"bbbbbx", None)
                    .map(|(path, _)| path.to_vec())
                    .collect::<Vec<_>>(),
                vec![b"bbbbb".to_vec(), b"bb".to_vec(), Vec::new()]
            );
            assert_eq!(loaded.delete_fields(b"bb", &[Bytes::from_static(b"f")]), 1);
            let cloned = loaded.clone();
            assert_eq!(cloned.memory_usage(), loaded.memory_usage());
            assert_eq!(loaded.delete_prefix(b"b"), 2);
            assert_eq!(cloned.prefix_matches(b"bbbbbx", None).count(), 2);
        }
        let mut invalid = Vec::new();
        invalid.write_len(2);
        for _ in 0..2 {
            invalid.write_string(b"path");
            invalid.write_len(1);
            invalid.write_string(b"f");
            invalid.write_string(b"v");
        }
        assert!(PathHashCodec::load_with_limits(&invalid, &mut 0, 512, 64).is_none());
        let mut invalid = Vec::new();
        invalid.write_len(1);
        invalid.write_string(b"path");
        invalid.write_len(0);
        assert!(PathHashCodec::load_with_limits(&invalid, &mut 0, 512, 64).is_none());
        assert!(PathHashCodec::load_with_limits(&[0], &mut 0, 512, 64)
            .unwrap()
            .is_empty());
    }
}
