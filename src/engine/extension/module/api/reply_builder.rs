use bytes::Bytes;

use crate::foundation::io::protocol::Value;

#[derive(Clone, Copy, PartialEq)]
enum ContainerType {
    Array,
    Map,
    Set,
    Attribute,
}

pub struct ReplyBuilder {
    stack: Vec<(Vec<Value>, Option<usize>, ContainerType)>,
    result: Option<Value>,
}

impl ReplyBuilder {
    pub fn new() -> Self {
        ReplyBuilder {
            stack: Vec::new(),
            result: None,
        }
    }

    fn append(&mut self, value: Value) {
        if self.stack.is_empty() {
            self.result = Some(value);
            return;
        }
        // Push to topmost container
        {
            let top = self.stack.last_mut().unwrap();
            top.0.push(value);
        }
        // Auto-close containers that are full
        while let Some((elements, expected, ctype)) = self.stack.pop() {
            if expected.is_none_or(|expected| elements.len() < expected) {
                // Not full yet, push back
                self.stack.push((elements, expected, ctype));
                break;
            }
            let built = Self::build_container(elements, ctype);
            if self.stack.is_empty() {
                self.result = Some(built);
                return;
            } else {
                self.stack.last_mut().unwrap().0.push(built);
            }
        }
    }

    fn build_container(elements: Vec<Value>, container_type: ContainerType) -> Value {
        match container_type {
            ContainerType::Array => Value::Array(elements),
            ContainerType::Map => {
                let mut entries = Vec::with_capacity(elements.len() / 2);
                let mut elements = elements.into_iter();
                while let (Some(key), Some(value)) = (elements.next(), elements.next()) {
                    entries.push((key, value));
                }
                Value::Map(entries)
            }
            ContainerType::Set => Value::Set(elements),
            ContainerType::Attribute => {
                let mut elements = elements.into_iter();
                let mut attrs = Vec::new();
                while elements.len() > 1 {
                    let key = elements.next().expect("attribute key must exist");
                    let value = elements.next().expect("attribute value must exist");
                    attrs.push((key, value));
                }
                let inner = elements.next().unwrap_or(Value::Null);
                Value::attribute(attrs, inner)
            }
        }
    }

    pub fn add_array(&mut self, len: i32) {
        if len < 0 {
            self.stack.push((Vec::new(), None, ContainerType::Array));
        } else {
            self.stack.push((
                Vec::with_capacity(len as usize),
                Some(len as usize),
                ContainerType::Array,
            ));
        }
    }

    pub fn add_map(&mut self, len: i32) {
        if len < 0 {
            self.append(Value::Null);
        } else {
            self.stack.push((
                Vec::with_capacity((len * 2) as usize),
                Some((len * 2) as usize),
                ContainerType::Map,
            ));
        }
    }

    pub fn add_set(&mut self, len: i32) {
        if len < 0 {
            self.append(Value::Null);
        } else {
            self.stack.push((
                Vec::with_capacity(len as usize),
                Some(len as usize),
                ContainerType::Set,
            ));
        }
    }

    pub fn add_attribute(&mut self, len: i32) {
        if len < 0 {
            self.append(Value::Null);
        } else {
            self.stack.push((
                Vec::with_capacity((len * 2 + 1) as usize),
                Some((len * 2 + 1) as usize),
                ContainerType::Attribute,
            ));
        }
    }

    pub fn set_array_length(&mut self, len: usize) -> bool {
        let Some((elements, expected, container_type)) = self.stack.last_mut() else {
            return false;
        };
        if *container_type != ContainerType::Array || expected.is_some() || elements.len() != len {
            return false;
        }
        *expected = Some(len);
        let (elements, _, container_type) = self.stack.pop().expect("checked above");
        let value = Self::build_container(elements, container_type);
        self.append(value);
        true
    }

    pub fn add_simple_string(&mut self, s: Bytes) {
        self.append(Value::SimpleString(s));
    }

    pub fn add_bulk_string(&mut self, s: Bytes) {
        self.append(Value::Bulk(s));
    }

    pub fn add_error(&mut self, s: Bytes) {
        self.append(Value::Error(s));
    }

    pub fn add_integer(&mut self, n: i64) {
        self.append(Value::Integer(n));
    }

    pub fn add_double(&mut self, d: f64) {
        self.append(Value::Double(d));
    }

    pub fn add_big_number(&mut self, value: Bytes) {
        self.append(Value::BigNumber(value));
    }

    pub fn add_verbatim(&mut self, format: [u8; 3], data: Bytes) {
        self.append(Value::Verbatim { format, data });
    }

    pub fn add_bool(&mut self, b: bool) {
        self.append(Value::Boolean(b));
    }

    pub fn add_null(&mut self) {
        self.append(Value::Null);
    }

    pub fn add_null_array(&mut self) {
        self.append(Value::NullArray);
    }

    pub fn into_value(mut self) -> Value {
        // If there are unfinished containers, build them as-is
        while let Some((elements, _, container_type)) = self.stack.pop() {
            let value = Self::build_container(elements, container_type);
            if self.stack.is_empty() {
                self.result = Some(value);
            } else {
                if let Some((container, _, _)) = self.stack.last_mut() {
                    container.push(value);
                }
            }
        }
        self.result.unwrap_or(Value::Null)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn map_and_set_preserve_resp3_container_types() {
        let mut map = ReplyBuilder::new();
        map.add_map(1);
        map.add_bulk_string(Bytes::from_static(b"key"));
        map.add_integer(7);
        assert_eq!(
            map.into_value(),
            Value::map(vec![(Value::bulk("key"), Value::integer(7))])
        );

        let mut set = ReplyBuilder::new();
        set.add_set(2);
        set.add_integer(1);
        set.add_integer(2);
        assert_eq!(
            set.into_value(),
            Value::set(vec![Value::integer(1), Value::integer(2)])
        );

        let mut scalar = ReplyBuilder::new();
        scalar.add_big_number(Bytes::from_static(b"12345678901234567890"));
        assert_eq!(scalar.into_value(), Value::bignum("12345678901234567890"));

        let mut verbatim = ReplyBuilder::new();
        verbatim.add_verbatim(*b"txt", Bytes::from_static(b"hello"));
        assert_eq!(verbatim.into_value(), Value::verbatim(*b"txt", "hello"));
    }

    #[test]
    fn attribute_wraps_the_following_reply() {
        let mut builder = ReplyBuilder::new();
        builder.add_attribute(1);
        builder.add_bulk_string(Bytes::from_static(b"ttl"));
        builder.add_integer(10);
        builder.add_simple_string(Bytes::from_static(b"OK"));

        assert_eq!(
            builder.into_value(),
            Value::attribute(
                vec![(Value::bulk("ttl"), Value::integer(10))],
                Value::SimpleString(Bytes::from_static(b"OK")),
            )
        );
    }

    #[test]
    fn postponed_array_closes_with_reported_length() {
        let mut builder = ReplyBuilder::new();
        builder.add_array(-1);
        builder.add_array(2);
        builder.add_bulk_string(Bytes::from_static(b"x"));
        builder.add_integer(1);
        builder.add_array(2);
        builder.add_bulk_string(Bytes::from_static(b"y"));
        builder.add_integer(2);
        assert!(builder.set_array_length(2));

        assert_eq!(
            builder.into_value(),
            Value::Array(vec![
                Value::Array(vec![Value::bulk("x"), Value::integer(1)]),
                Value::Array(vec![Value::bulk("y"), Value::integer(2)]),
            ])
        );
    }
}
