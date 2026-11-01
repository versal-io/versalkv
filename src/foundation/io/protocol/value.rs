use super::{DecimalDoubleText, InvalidDecimalDouble};
use bytes::Bytes;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    SimpleString(Bytes),
    Error(Bytes),
    Integer(i64),
    Double(f64),
    DecimalDouble(DecimalDoubleText),
    Bulk(Bytes),
    Null,
    NullArray,
    Array(Vec<Value>),
    Boolean(bool),
    BigNumber(Bytes),
    Verbatim {
        format: [u8; 3],
        data: Bytes,
    },
    Attribute {
        attrs: Vec<(Value, Value)>,
        inner: Box<Value>,
    },
    Push(Vec<Value>),
    Map(Vec<(Value, Value)>),
    Set(Vec<Value>),
}

impl Value {
    pub fn ok() -> Self {
        Value::SimpleString(Bytes::from_static(b"OK"))
    }
    pub fn pong() -> Self {
        Value::SimpleString(Bytes::from_static(b"PONG"))
    }
    pub fn queued() -> Self {
        Value::SimpleString(Bytes::from_static(b"QUEUED"))
    }
    pub fn error(msg: impl Into<Bytes>) -> Self {
        Value::Error(msg.into())
    }
    pub fn bulk(data: impl Into<Bytes>) -> Self {
        Value::Bulk(data.into())
    }
    pub fn integer(n: i64) -> Self {
        Value::Integer(n)
    }
    pub fn double(v: f64) -> Self {
        Value::Double(v)
    }
    pub fn decimal_double(text: impl Into<Bytes>) -> Result<Self, InvalidDecimalDouble> {
        DecimalDoubleText::new(text).map(Value::DecimalDouble)
    }
    pub fn null() -> Self {
        Value::Null
    }
    pub fn null_array() -> Self {
        Value::NullArray
    }
    pub fn boolean(v: bool) -> Self {
        Value::Boolean(v)
    }
    pub fn bignum(data: impl Into<Bytes>) -> Self {
        Value::BigNumber(data.into())
    }
    pub fn verbatim(format: [u8; 3], data: impl Into<Bytes>) -> Self {
        Value::Verbatim {
            format,
            data: data.into(),
        }
    }
    pub fn attribute(attrs: Vec<(Value, Value)>, inner: Value) -> Self {
        Value::Attribute {
            attrs,
            inner: Box::new(inner),
        }
    }
    pub fn push(items: Vec<Value>) -> Self {
        Value::Push(items)
    }
    pub fn map(entries: Vec<(Value, Value)>) -> Self {
        Value::Map(entries)
    }
    pub fn set(items: Vec<Value>) -> Self {
        Value::Set(items)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_reply_constructors_return_expected_variants() {
        assert_eq!(Value::ok(), Value::SimpleString(Bytes::from_static(b"OK")));
        assert_eq!(
            Value::pong(),
            Value::SimpleString(Bytes::from_static(b"PONG"))
        );
        assert_eq!(
            Value::queued(),
            Value::SimpleString(Bytes::from_static(b"QUEUED"))
        );
        assert_eq!(
            Value::error("ERR x"),
            Value::Error(Bytes::from_static(b"ERR x"))
        );
        assert_eq!(
            Value::bulk("data"),
            Value::Bulk(Bytes::from_static(b"data"))
        );
        assert_eq!(Value::integer(-7), Value::Integer(-7));
        assert_eq!(Value::double(1.25), Value::Double(1.25));
        assert_eq!(Value::null(), Value::Null);
        assert_eq!(Value::null_array(), Value::NullArray);
        assert_eq!(Value::boolean(true), Value::Boolean(true));
    }

    #[test]
    fn resp3_container_constructors_preserve_payload_shape() {
        assert_eq!(
            Value::bignum("123"),
            Value::BigNumber(Bytes::from_static(b"123"))
        );
        assert_eq!(
            Value::verbatim(*b"txt", "hello"),
            Value::Verbatim {
                format: *b"txt",
                data: Bytes::from_static(b"hello"),
            }
        );

        let attrs = vec![(Value::bulk("ttl"), Value::integer(10))];
        let attribute = Value::attribute(attrs.clone(), Value::bulk("value"));
        assert_eq!(
            attribute,
            Value::Attribute {
                attrs,
                inner: Box::new(Value::bulk("value")),
            }
        );

        assert_eq!(
            Value::push(vec![Value::bulk("x")]),
            Value::Push(vec![Value::bulk("x")])
        );
        assert_eq!(
            Value::map(vec![(Value::bulk("k"), Value::bulk("v"))]),
            Value::Map(vec![(Value::bulk("k"), Value::bulk("v"))])
        );
        assert_eq!(
            Value::set(vec![Value::bulk("x")]),
            Value::Set(vec![Value::bulk("x")])
        );
    }
}
