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
    #[cfg_attr(feature = "wasm", serde(rename = "decimal_double"))]
    DecimalDouble(String),
    Boolean(bool),
    Array(Vec<Option<String>>),
    Replies(Vec<Reply>),
    Map(Vec<(Reply, Reply)>),
    Error(String),
}

pub enum EmbeddedOutput {
    Reply { request_id: u32, value: Value },
}

impl EmbeddedOutput {
    pub(crate) fn into_reply(self) -> Reply {
        match self {
            Self::Reply { value, .. } => Reply::from_value(value),
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
            Value::DecimalDouble(value) => Self::DecimalDouble(lossy(value.as_bytes())),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::io::protocol::DecimalDoubleText;

    #[test]
    fn decimal_double_embedded_replies_preserve_numeric_identity_recursively() {
        for text in ["9007199254740993", "1e4932", "inf", "-inf"] {
            let scalar = Value::DecimalDouble(DecimalDoubleText::new(text.to_owned()).unwrap());
            let expected = Reply::DecimalDouble(text.to_owned());
            assert_eq!(Reply::from_value(scalar.clone()), expected);
            assert_eq!(
                Reply::from_value(Value::Array(vec![scalar.clone(), scalar.clone()])),
                Reply::Replies(vec![expected.clone(), expected.clone()])
            );
            assert_eq!(
                Reply::from_value(Value::Map(vec![(Value::bulk("key"), scalar.clone())])),
                Reply::Map(vec![(
                    Reply::Bulk(Some("key".to_owned())),
                    expected.clone()
                )])
            );
            assert_eq!(
                Reply::from_value(Value::attribute(vec![], scalar)),
                expected
            );
        }
    }
}
