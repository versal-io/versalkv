pub fn int_encodable(b: &[u8]) -> Option<i64> {
    let s = std::str::from_utf8(b).ok()?;
    let n: i64 = s.parse().ok()?;
    if n.to_string().as_bytes() == b {
        Some(n)
    } else {
        None
    }
}

const SDS_HDR_5: usize = 1;
const SDS_HDR_8: usize = 3;

fn sds_req_type(len: usize) -> usize {
    if len < 1 << 5 {
        SDS_HDR_5
    } else {
        SDS_HDR_8
    }
}

fn sds_req_size(len: usize, hdr: usize) -> usize {
    len + hdr + 1
}

pub const EMBSTR_SIZE_LIMIT: usize = 116;

pub fn fits_embstr_with_key(key_len: usize, val_len: usize) -> bool {
    let mut size = 8usize;
    size += sds_req_size(key_len, sds_req_type(key_len)) + 1;
    size += sds_req_size(val_len, SDS_HDR_8);
    size <= 128
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StrEncoding {
    Int,
    Embstr,
    Raw,
}

pub fn infer_str_encoding(data: &[u8]) -> StrEncoding {
    if int_encodable(data).is_some() {
        StrEncoding::Int
    } else if data.len() <= EMBSTR_SIZE_LIMIT {
        StrEncoding::Embstr
    } else {
        StrEncoding::Raw
    }
}
