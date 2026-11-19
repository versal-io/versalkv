use super::Value;
use bytes::BufMut;

/// Write a non-negative `usize` into `buf` without heap allocation. Returns
/// the slice of `buf` containing the decimal digits. Used by the serializer
/// to avoid `format!("{}", n)` String allocations on the hot reply path
/// (e.g. every INCR integer reply and every bulk-string length prefix).
#[inline]
fn write_usize(buf: &mut [u8; 20], n: usize) -> &[u8] {
    if n == 0 {
        buf[0] = b'0';
        return &buf[..1];
    }
    let mut n = n;
    let mut pos = 20;
    while n > 0 {
        pos -= 1;
        buf[pos] = b'0' + (n % 10) as u8;
        n /= 10;
    }
    &buf[pos..]
}

/// Write a signed `i64` into `buf` without heap allocation. Returns the slice
/// of `buf` containing the decimal digits (including a leading `-` for
/// negatives).
#[inline]
fn write_i64(buf: &mut [u8; 20], n: i64) -> &[u8] {
    if n == i64::MIN {
        // i64::MIN cannot be negated; handle it explicitly.
        return b"-9223372036854775808";
    }
    if n < 0 {
        // Write digits of the absolute value first, then shift right and
        // prepend the sign. Use a local copy of the digit count to avoid
        // borrowing `buf` across the mutation.
        let abs = (-n) as usize;
        let digits = write_usize(buf, abs);
        let digit_len = digits.len();
        let start = 20 - digit_len - 1;
        // `digits` points into `buf[start+1..]`; copy within the same array
        // using a temporary to avoid overlapping borrow issues.
        let mut tmp = [0u8; 20];
        tmp[..digit_len].copy_from_slice(digits);
        buf[start] = b'-';
        buf[start + 1..start + 1 + digit_len].copy_from_slice(&tmp[..digit_len]);
        &buf[start..start + 1 + digit_len]
    } else {
        write_usize(buf, n as usize)
    }
}

pub fn format_double(v: f64) -> String {
    if v.is_nan() {
        return "nan".to_string();
    }
    if v.is_infinite() {
        return if v > 0.0 {
            "inf".to_string()
        } else {
            "-inf".to_string()
        };
    }
    if v == v.trunc() && v.abs() < 1e17 {
        return format!("{}", v as i64);
    }
    v.to_string()
}

pub fn serialize(value: &Value, buf: &mut impl BufMut) {
    serialize_resp2(value, buf)
}

pub fn serialize_command(parts: &[&[u8]], buf: &mut impl BufMut) {
    buf.put_u8(b'*');
    buf.put_slice(parts.len().to_string().as_bytes());
    buf.put_slice(b"\r\n");
    for part in parts {
        buf.put_u8(b'$');
        buf.put_slice(part.len().to_string().as_bytes());
        buf.put_slice(b"\r\n");
        buf.put_slice(part);
        buf.put_slice(b"\r\n");
    }
}

pub fn serialize_to_vec(value: &Value) -> Vec<u8> {
    let mut buf = Vec::with_capacity(256);
    serialize(value, &mut buf);
    buf
}

pub fn serialize_resp2(value: &Value, buf: &mut impl BufMut) {
    let mut num_buf = [0u8; 20];
    match value {
        Value::SimpleString(s) => {
            buf.put_u8(b'+');
            buf.put_slice(s);
            buf.put_slice(b"\r\n");
        }
        Value::Error(e) => {
            buf.put_u8(b'-');
            buf.put_slice(e);
            buf.put_slice(b"\r\n");
        }
        Value::Integer(n) => {
            buf.put_u8(b':');
            buf.put_slice(write_i64(&mut num_buf, *n));
            buf.put_slice(b"\r\n");
        }
        Value::Double(v) => {
            let s = format_double(*v);
            buf.put_u8(b'$');
            buf.put_slice(write_usize(&mut num_buf, s.len()));
            buf.put_slice(b"\r\n");
            buf.put_slice(s.as_bytes());
            buf.put_slice(b"\r\n");
        }
        Value::DecimalDouble(text) => {
            serialize_resp2(&Value::Bulk(text.bytes()), buf);
        }
        Value::Bulk(b) => {
            buf.put_u8(b'$');
            buf.put_slice(write_usize(&mut num_buf, b.len()));
            buf.put_slice(b"\r\n");
            buf.put_slice(b);
            buf.put_slice(b"\r\n");
        }
        Value::Null => buf.put_slice(b"$-1\r\n"),
        Value::NullArray => buf.put_slice(b"*-1\r\n"),
        Value::Boolean(v) => {
            if *v {
                buf.put_slice(b":1\r\n");
            } else {
                buf.put_slice(b":0\r\n");
            }
        }
        Value::BigNumber(b) => {
            buf.put_u8(b'$');
            buf.put_slice(write_usize(&mut num_buf, b.len()));
            buf.put_slice(b"\r\n");
            buf.put_slice(b);
            buf.put_slice(b"\r\n");
        }
        Value::Verbatim { data, .. } => {
            buf.put_u8(b'$');
            buf.put_slice(write_usize(&mut num_buf, data.len()));
            buf.put_slice(b"\r\n");
            buf.put_slice(data);
            buf.put_slice(b"\r\n");
        }
        Value::Attribute { inner, .. } => {
            serialize_resp2(inner, buf);
        }
        Value::Push(items) => {
            buf.put_u8(b'*');
            buf.put_slice(write_usize(&mut num_buf, items.len()));
            buf.put_slice(b"\r\n");
            for item in items {
                serialize_resp2(item, buf);
            }
        }
        Value::Array(items) => {
            buf.put_u8(b'*');
            buf.put_slice(write_usize(&mut num_buf, items.len()));
            buf.put_slice(b"\r\n");
            for item in items {
                serialize_resp2(item, buf);
            }
        }
        Value::Map(entries) => {
            buf.put_u8(b'*');
            buf.put_slice(write_usize(&mut num_buf, entries.len() * 2));
            buf.put_slice(b"\r\n");
            for (k, v) in entries {
                serialize_resp2(k, buf);
                serialize_resp2(v, buf);
            }
        }
        Value::Set(items) => {
            buf.put_u8(b'*');
            buf.put_slice(write_usize(&mut num_buf, items.len()));
            buf.put_slice(b"\r\n");
            for item in items {
                serialize_resp2(item, buf);
            }
        }
    }
}

pub fn serialize_resp3(value: &Value, buf: &mut impl BufMut) {
    let mut num_buf = [0u8; 20];
    match value {
        Value::SimpleString(s) => {
            buf.put_u8(b'+');
            buf.put_slice(s);
            buf.put_slice(b"\r\n");
        }
        Value::Error(e) => {
            buf.put_u8(b'-');
            buf.put_slice(e);
            buf.put_slice(b"\r\n");
        }
        Value::Integer(n) => {
            buf.put_u8(b':');
            buf.put_slice(write_i64(&mut num_buf, *n));
            buf.put_slice(b"\r\n");
        }
        Value::Double(v) => {
            buf.put_u8(b',');
            let s = format_double(*v);
            buf.put_slice(s.as_bytes());
            buf.put_slice(b"\r\n");
        }
        Value::DecimalDouble(text) => {
            buf.put_u8(b',');
            buf.put_slice(text.as_bytes());
            buf.put_slice(b"\r\n");
        }
        Value::Bulk(b) => {
            buf.put_u8(b'$');
            buf.put_slice(write_usize(&mut num_buf, b.len()));
            buf.put_slice(b"\r\n");
            buf.put_slice(b);
            buf.put_slice(b"\r\n");
        }
        Value::Null | Value::NullArray => buf.put_slice(b"_\r\n"),
        Value::Boolean(v) => {
            if *v {
                buf.put_slice(b"#t\r\n");
            } else {
                buf.put_slice(b"#f\r\n");
            }
        }
        Value::BigNumber(b) => {
            buf.put_u8(b'(');
            buf.put_slice(b);
            buf.put_slice(b"\r\n");
        }
        Value::Verbatim { format, data } => {
            buf.put_u8(b'=');
            buf.put_slice(write_usize(&mut num_buf, data.len() + 4));
            buf.put_slice(b"\r\n");
            buf.put_slice(format);
            buf.put_u8(b':');
            buf.put_slice(data);
            buf.put_slice(b"\r\n");
        }
        Value::Attribute { attrs, inner } => {
            buf.put_u8(b'|');
            buf.put_slice(write_usize(&mut num_buf, attrs.len()));
            buf.put_slice(b"\r\n");
            for (k, v) in attrs {
                serialize_resp3(k, buf);
                serialize_resp3(v, buf);
            }
            serialize_resp3(inner, buf);
        }
        Value::Push(items) => {
            buf.put_u8(b'>');
            buf.put_slice(write_usize(&mut num_buf, items.len()));
            buf.put_slice(b"\r\n");
            for item in items {
                serialize_resp3(item, buf);
            }
        }
        Value::Array(items) => {
            buf.put_u8(b'*');
            buf.put_slice(write_usize(&mut num_buf, items.len()));
            buf.put_slice(b"\r\n");
            for item in items {
                serialize_resp3(item, buf);
            }
        }
        Value::Map(entries) => {
            buf.put_u8(b'%');
            buf.put_slice(write_usize(&mut num_buf, entries.len()));
            buf.put_slice(b"\r\n");
            for (k, v) in entries {
                serialize_resp3(k, buf);
                serialize_resp3(v, buf);
            }
        }
        Value::Set(items) => {
            buf.put_u8(b'~');
            buf.put_slice(write_usize(&mut num_buf, items.len()));
            buf.put_slice(b"\r\n");
            for item in items {
                serialize_resp3(item, buf);
            }
        }
    }
}

pub fn serialize_to_vec_resp2(value: &Value) -> Vec<u8> {
    let mut buf = Vec::with_capacity(64);
    serialize_resp2(value, &mut buf);
    buf
}

pub fn serialize_to_vec_resp3(value: &Value) -> Vec<u8> {
    let mut buf = Vec::with_capacity(64);
    serialize_resp3(value, &mut buf);
    buf
}

#[cfg(test)]
#[path = "serializer_tests.rs"]
mod serializer_tests;
