use bytes::Bytes;

use crate::engine::core::Value;

#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "wasm", derive(serde::Serialize))]
#[cfg_attr(
    feature = "wasm",
    serde(tag = "type", content = "value", rename_all = "lowercase")
)]
pub enum Reply {
    Simple(String),
    Bulk(Option<String>),
    Integer(i64),
    Double(f64),
    Boolean(bool),
    Array(Vec<Option<String>>),
    Replies(Vec<Reply>),
    Map(Vec<(Reply, Reply)>),
    Error(String),
}

pub enum EmbeddedOutput {
    Reply {
        request_id: u32,
        value: Value,
    },
    /// A reply whose durable AOF frame must be written to the storage host
    /// before the reply is released to the client. The single-worker host
    /// appends `frame` synchronously, then delivers `value`.
    DurableReply {
        request_id: u32,
        value: Value,
        frame: Bytes,
    },
}

impl EmbeddedOutput {
    pub(crate) fn into_reply(self) -> Reply {
        match self {
            Self::Reply { value, .. } | Self::DurableReply { value, .. } => {
                Reply::from_value(value)
            }
        }
    }
}

impl Reply {
    pub(crate) fn from_value(value: Value) -> Self {
        match value {
            Value::SimpleString(value) => Self::Simple(lossy(&value)),
            Value::Error(value) => Self::Error(lossy(&value)),
            Value::Integer(value) => Self::Integer(value),
            Value::Double(value) => Self::Double(value),
            Value::Bulk(value) => Self::Bulk(Some(lossy(&value))),
            Value::Null | Value::NullArray => Self::Bulk(None),
            Value::Boolean(value) => Self::Boolean(value),
            Value::Array(values) | Value::Set(values) => Self::from_sequence(values),
            Value::BigNumber(value) => Self::Bulk(Some(lossy(&value))),
            Value::Attribute { inner, .. } => Self::from_value(*inner),
            Value::Push(values) => {
                Self::Replies(values.into_iter().map(Self::from_value).collect())
            }
            Value::Verbatim { data, .. } => Self::Bulk(Some(lossy(&data))),
            Value::Map(values) => Self::Map(
                values
                    .into_iter()
                    .map(|(key, value)| (Self::from_value(key), Self::from_value(value)))
                    .collect(),
            ),
        }
    }

    fn from_sequence(values: Vec<Value>) -> Self {
        if values.iter().all(|value| {
            matches!(
                value,
                Value::Bulk(_) | Value::SimpleString(_) | Value::Null | Value::NullArray
            )
        }) {
            return Self::Array(
                values
                    .into_iter()
                    .map(|value| match value {
                        Value::Bulk(value) | Value::SimpleString(value) => Some(lossy(&value)),
                        Value::Null | Value::NullArray => None,
                        _ => unreachable!("flat sequence was validated above"),
                    })
                    .collect(),
            );
        }
        Self::Replies(values.into_iter().map(Self::from_value).collect())
    }
}

fn lossy(value: &[u8]) -> String {
    String::from_utf8_lossy(value).into_owned()
}
