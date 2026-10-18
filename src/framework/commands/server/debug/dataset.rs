use crate::engine::command::context::CommandConn;
use crate::foundation::io::protocol::Value;
use crate::foundation::io::store::structure::stream::ValkeyStream;
use crate::foundation::io::store::Robj;
use crate::framework::context::ServerContext;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;
use sha1::{Digest, Sha1};


fn sha1_of(data: &[u8]) -> [u8; 20] {
    let mut hasher = Sha1::new();
    hasher.update(data);
    let mut out = [0u8; 20];
    out.copy_from_slice(&hasher.finalize());
    out
}

/// `digest ^= SHA1(data)`（C xorDigest）
fn xor_digest(digest: &mut [u8; 20], data: &[u8]) {
    let hash = sha1_of(data);
    for (byte, h) in digest.iter_mut().zip(hash) {
        *byte ^= h;
    }
}

fn mix_digest(digest: &mut [u8; 20], data: &[u8]) {
    xor_digest(digest, data);
    *digest = sha1_of(digest);
}

pub(super) fn debug_populate(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    if !(2..=4).contains(&args.args.len()) {
        return CommandError::syntax_error();
    }
    let count = match parse_positive_usize(&args.args[1]) {
        Some(v) => v,
        None => return Value::error("ERR value is not an integer or out of range"),
    };
    let prefix = args.args.get(2).map(Bytes::as_ref).unwrap_or(b"key");
    let value_size = match args.args.get(3) {
        Some(raw) => match parse_positive_usize(raw) {
            Some(v) => Some(v),
            None => return Value::error("ERR value is not an integer or out of range"),
        },
        None => None,
    };
    let db = match ctx.db_mut(conn.selected_db()) {
        Some(db) => db,
        None => return CommandError::db_index_out_of_range(),
    };
    for idx in 0..count {
        let mut key = Vec::with_capacity(prefix.len() + 24);
        key.extend_from_slice(prefix);
        key.extend_from_slice(format!(":{}", idx).as_bytes());
        if db.get_allow_expired(&key).is_some() {
            continue;
        }

        let base = format!("value:{}", idx).into_bytes();
        let value = match value_size {
            Some(size) => {
                let mut buf = vec![0; size];
                let copy_len = buf.len().min(base.len());
                buf[..copy_len].copy_from_slice(&base[..copy_len]);
                buf
            }
            None => base,
        };
        db.set(key, Robj::string_raw(value));
    }
    Value::ok()
}

pub(super) fn debug_digest(ctx: &ServerContext) -> Value {
    let mut final_digest = [0u8; 20];
    for (db_idx, db) in ctx.dbs().iter_dbs() {
        let mut seeded = false;
        db.for_each_entry_including_expired(|key, obj, expire| {
            if !seeded {
                mix_digest(&mut final_digest, &(db_idx as u32).to_be_bytes());
                seeded = true;
            }
            let mut key_digest = [0u8; 20];
            mix_digest(&mut key_digest, key);
            xor_object_digest(obj, expire, &mut key_digest);
            xor_digest(&mut final_digest, &key_digest);
        });
    }
    Value::SimpleString(Bytes::from(hex_bytes(&final_digest)))
}

pub(super) fn debug_digest_value(
    conn: &mut dyn CommandConn,
    args: &CommandArgs,
    ctx: &mut ServerContext,
) -> Value {
    let db = match ctx.db(conn.selected_db()) {
        Some(db) => db,
        None => return CommandError::db_index_out_of_range(),
    };
    let values = args
        .args
        .iter()
        .skip(1)
        .map(|key| {
            let mut digest = [0u8; 20];
            if let Some(obj) = db.get_allow_expired(key) {
                xor_object_digest(obj, db.expire_of(key), &mut digest);
            }
            Value::SimpleString(Bytes::from(hex_bytes(&digest)))
        })
        .collect();
    Value::Array(values)
}

fn logical_object_type(obj: &Robj) -> u8 {
    match obj {
        Robj::StrInt(_) | Robj::StrEmbstr(_) | Robj::StrRaw(_) => 0, // OBJ_STRING
        Robj::ListQuicklist(_) | Robj::ListListpack(_) => 1,         // OBJ_LIST
        Robj::SetHashtable(_) | Robj::SetIntset(_) | Robj::SetListpack(_) => 2, // OBJ_SET
        Robj::ZSetSkiplist(_) | Robj::ZSetListpack(_) => 3,          // OBJ_ZSET
        Robj::HashHashtable(_) | Robj::HashListpack(_) => 4,         // OBJ_HASH
        Robj::StreamListpack(_) | Robj::StreamRadix(_) => 6,         // OBJ_STREAM
    }
}

fn xor_object_digest(obj: &Robj, expire: Option<u64>, digest: &mut [u8; 20]) {
    let type_byte = logical_object_type(obj);
    mix_digest(digest, &(u32::from(type_byte)).to_be_bytes());

    match obj {
        Robj::StrInt(_) | Robj::StrEmbstr(_) | Robj::StrRaw(_) => {
            mix_digest(digest, obj.as_bytes());
        }
        Robj::ListQuicklist(list) => {
            for element in list.inner.iter() {
                mix_digest(digest, element);
            }
        }
        Robj::ListListpack(list) => {
            for element in list.all_elements() {
                mix_digest(digest, &element);
            }
        }
        Robj::SetHashtable(set) => {
            for member in set.data.keys() {
                xor_digest(digest, member);
            }
        }
        Robj::SetIntset(set) => {
            for number in set.iter() {
                xor_digest(digest, number.to_string().as_bytes());
            }
        }
        Robj::SetListpack(set) => {
            for member in set.all_members() {
                xor_digest(digest, &member);
            }
        }
        Robj::ZSetSkiplist(zset) => {
            for (member, score) in zset {
                zset_element_digest(digest, member, *score);
            }
        }
        Robj::ZSetListpack(zset) => {
            for (member, score) in zset.all_pairs() {
                zset_element_digest(digest, &member, score);
            }
        }
        Robj::HashHashtable(hash) => {
            for (field, value) in hash.data.iter() {
                hash_field_digest(digest, field, value);
            }
        }
        Robj::HashListpack(hash) => {
            for (field, value) in hash.all_pairs() {
                hash_field_digest(digest, &field, &value);
            }
        }
        Robj::StreamListpack(stream) => stream_digest(digest, stream),
        Robj::StreamRadix(_) => {}
    }

    if expire.is_some() {
        xor_digest(digest, b"!!expire!!");
    }
}

fn hash_field_digest(digest: &mut [u8; 20], field: &[u8], value: &[u8]) {
    let mut element = [0u8; 20];
    mix_digest(&mut element, field);
    mix_digest(&mut element, value);
    xor_digest(digest, &element);
}

fn zset_element_digest(digest: &mut [u8; 20], member: &[u8], score: f64) {
    let mut element = [0u8; 20];
    mix_digest(&mut element, member);
    let score_str = fpconv_format(score);
    mix_digest(&mut element, score_str.as_bytes());
    xor_digest(digest, &element);
}

fn stream_digest(digest: &mut [u8; 20], stream: &ValkeyStream) {
    for (id, fields) in stream.iter() {
        mix_digest(digest, format!("{}.{}", id.ms, id.seq).as_bytes());
        for (field, value) in fields {
            mix_digest(digest, field);
            mix_digest(digest, value);
        }
    }
}

pub(super) fn fpconv_format(value: f64) -> String {
    if value.is_nan() {
        return "nan".to_string();
    }
    if value == 0.0 {
        return if value.is_sign_negative() {
            "-0".to_string()
        } else {
            "0".to_string()
        };
    }
    if value.is_infinite() {
        return if value < 0.0 {
            "-inf".to_string()
        } else {
            "inf".to_string()
        };
    }
    let negative = value < 0.0;
    let scientific = format!("{:e}", value);
    let (mantissa, exp_part) = scientific
        .split_once('e')
        .expect("f64 scientific notation");
    let exponent: i64 = exp_part.parse().expect("scientific exponent");
    let digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
    let n = digits.len();
    // value = digits_coeff × 10^K，K = E - (n-1)
    let k = exponent - (n as i64 - 1);
    let emitted = emit_fpconv_digits(&digits, n, k, negative);
    if negative {
        format!("-{emitted}")
    } else {
        emitted
    }
}

fn emit_fpconv_digits(digits: &str, n: usize, k: i64, negative: bool) -> String {
    let magnitude = (k + n as i64 - 1).unsigned_abs();

    if k >= 0 && magnitude < n as u64 + 7 {
        let mut out = String::with_capacity(n + k as usize);
        out.push_str(digits);
        for _ in 0..k {
            out.push('0');
        }
        return out;
    }

    if k < 0 && (k > -7 || magnitude < 4) {
        let offset = n as i64 - k.unsigned_abs() as i64;
        if offset <= 0 {
            let mut out = String::from("0.");
            for _ in 0..(-offset) {
                out.push('0');
            }
            out.push_str(digits);
            return out;
        }
        let offset = offset as usize;
        let mut out = String::with_capacity(n + 1);
        out.push_str(&digits[..offset]);
        out.push('.');
        out.push_str(&digits[offset..]);
        return out;
    }

    let nt = n.min(18 - negative as usize);
    let mut out = String::new();
    out.push(digits.chars().next().expect("leading digit"));
    if nt > 1 {
        out.push('.');
        out.push_str(&digits[1..nt]);
    }
    out.push('e');
    let signed_exp = k + nt as i64 - 1;
    out.push(if signed_exp < 0 { '-' } else { '+' });
    let mut exp_value = magnitude;
    let mut cent = 0u64;
    if exp_value > 99 {
        cent = exp_value / 100;
        out.push((cent as u8 + b'0') as char);
        exp_value -= cent * 100;
    }
    if exp_value > 9 {
        out.push(((exp_value / 10) as u8 + b'0') as char);
        exp_value %= 10;
    } else if cent > 0 {
        out.push('0');
    }
    out.push((exp_value as u8 + b'0') as char);
    out
}

fn hex_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

fn parse_positive_usize(raw: &[u8]) -> Option<usize> {
    let n = std::str::from_utf8(raw).ok()?.parse::<usize>().ok()?;
    (n > 0).then_some(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fpconv_matches_c_special_and_shortest_values() {
        let cases = [
            (0.0, "0"),
            (-0.0, "-0"),
            (1.0, "1"),
            (-1.0, "-1"),
            (1.5, "1.5"),
            (0.1, "0.1"),
            (0.001, "0.001"),
            (0.0001, "0.0001"),
            (100.0, "100"),
            (12300.0, "12300"),
            (f64::INFINITY, "inf"),
            (f64::NEG_INFINITY, "-inf"),
            (f64::NAN, "nan"),
            (1e21, "1e+21"),
            (1e-7, "1e-7"),
            (1e-10, "1e-10"),
            (1.234_567_89e16, "1.23456789e+16"),
        ];
        for (input, expected) in cases {
            assert_eq!(fpconv_format(input), expected, "input {input}");
        }
    }

    #[test]
    fn digest_changes_when_container_content_changes() {
        use crate::foundation::io::store::structure::hash::HashLp;
        use crate::foundation::memory::value::HashOps;

        let mut ctx = ServerContext::new(1);
        let empty_hash = Robj::HashListpack(HashLp::new());
        let mut populated = HashLp::new();
        populated
            .set(b"a", b"1")
            .expect("listpack hash accepts field");

        ctx.db_mut(0).expect("db 0").set(b"h".to_vec(), empty_hash);
        let before = debug_digest(&ctx);
        ctx.db_mut(0)
            .expect("db 0")
            .set(b"h".to_vec(), Robj::HashListpack(populated));
        let after = debug_digest(&ctx);
        assert_ne!(before, after);
    }
}
