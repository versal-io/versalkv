use super::Value;
use bytes::{BufMut, BytesMut};

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
    let mut buf = BytesMut::with_capacity(256);
    serialize(value, &mut buf);
    buf.to_vec()
}

pub fn serialize_resp2(value: &Value, buf: &mut impl BufMut) {
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
            let s = format!("{}", n);
            buf.put_slice(s.as_bytes());
            buf.put_slice(b"\r\n");
        }
        Value::Double(v) => {
            let s = format_double(*v);
            buf.put_u8(b'$');
            let len = format!("{}", s.len());
            buf.put_slice(len.as_bytes());
            buf.put_slice(b"\r\n");
            buf.put_slice(s.as_bytes());
            buf.put_slice(b"\r\n");
        }
        Value::DecimalDouble(text) => {
            serialize_resp2(&Value::Bulk(text.bytes()), buf);
        }
        Value::Bulk(b) => {
            buf.put_u8(b'$');
            let s = format!("{}", b.len());
            buf.put_slice(s.as_bytes());
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
            let s = format!("{}", b.len());
            buf.put_slice(s.as_bytes());
            buf.put_slice(b"\r\n");
            buf.put_slice(b);
            buf.put_slice(b"\r\n");
        }
        Value::Verbatim { data, .. } => {
            buf.put_u8(b'$');
            let s = format!("{}", data.len());
            buf.put_slice(s.as_bytes());
            buf.put_slice(b"\r\n");
            buf.put_slice(data);
            buf.put_slice(b"\r\n");
        }
        Value::Attribute { inner, .. } => {
            serialize_resp2(inner, buf);
        }
        Value::Push(items) => {
            buf.put_u8(b'*');
            let s = format!("{}", items.len());
            buf.put_slice(s.as_bytes());
            buf.put_slice(b"\r\n");
            for item in items {
                serialize_resp2(item, buf);
            }
        }
        Value::Array(items) => {
            buf.put_u8(b'*');
            let s = format!("{}", items.len());
            buf.put_slice(s.as_bytes());
            buf.put_slice(b"\r\n");
            for item in items {
                serialize_resp2(item, buf);
            }
        }
        Value::Map(entries) => {
            buf.put_u8(b'*');
            let n = entries.len() * 2;
            let s = format!("{}", n);
            buf.put_slice(s.as_bytes());
            buf.put_slice(b"\r\n");
            for (k, v) in entries {
                serialize_resp2(k, buf);
                serialize_resp2(v, buf);
            }
        }
        Value::Set(items) => {
            buf.put_u8(b'*');
            let s = format!("{}", items.len());
            buf.put_slice(s.as_bytes());
            buf.put_slice(b"\r\n");
            for item in items {
                serialize_resp2(item, buf);
            }
        }
    }
}

pub fn serialize_resp3(value: &Value, buf: &mut impl BufMut) {
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
            let s = format!("{}", n);
            buf.put_slice(s.as_bytes());
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
            let s = format!("{}", b.len());
            buf.put_slice(s.as_bytes());
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
            let total = data.len() + 4;
            let s = format!("{}", total);
            buf.put_slice(s.as_bytes());
            buf.put_slice(b"\r\n");
            buf.put_slice(format);
            buf.put_u8(b':');
            buf.put_slice(data);
            buf.put_slice(b"\r\n");
        }
        Value::Attribute { attrs, inner } => {
            buf.put_u8(b'|');
            let n = attrs.len();
            let s = format!("{}", n);
            buf.put_slice(s.as_bytes());
            buf.put_slice(b"\r\n");
            for (k, v) in attrs {
                serialize_resp3(k, buf);
                serialize_resp3(v, buf);
            }
            serialize_resp3(inner, buf);
        }
        Value::Push(items) => {
            buf.put_u8(b'>');
            let s = format!("{}", items.len());
            buf.put_slice(s.as_bytes());
            buf.put_slice(b"\r\n");
            for item in items {
                serialize_resp3(item, buf);
            }
        }
        Value::Array(items) => {
            buf.put_u8(b'*');
            let s = format!("{}", items.len());
            buf.put_slice(s.as_bytes());
            buf.put_slice(b"\r\n");
            for item in items {
                serialize_resp3(item, buf);
            }
        }
        Value::Map(entries) => {
            buf.put_u8(b'%');
            let s = format!("{}", entries.len());
            buf.put_slice(s.as_bytes());
            buf.put_slice(b"\r\n");
            for (k, v) in entries {
                serialize_resp3(k, buf);
                serialize_resp3(v, buf);
            }
        }
        Value::Set(items) => {
            buf.put_u8(b'~');
            let s = format!("{}", items.len());
            buf.put_slice(s.as_bytes());
            buf.put_slice(b"\r\n");
            for item in items {
                serialize_resp3(item, buf);
            }
        }
    }
}

pub fn serialize_to_vec_resp2(value: &Value) -> Vec<u8> {
    let mut buf = BytesMut::with_capacity(256);
    serialize_resp2(value, &mut buf);
    buf.to_vec()
}

pub fn serialize_to_vec_resp3(value: &Value) -> Vec<u8> {
    let mut buf = BytesMut::with_capacity(256);
    serialize_resp3(value, &mut buf);
    buf.to_vec()
}

#[cfg(test)]
#[path = "serializer_tests.rs"]
mod serializer_tests;
