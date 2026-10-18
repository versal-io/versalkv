use crate::engine::command::{
    CMD_ADMIN, CMD_DENYOOM, CMD_FAST, CMD_LOADING, CMD_MODULE, CMD_MOVABLE_KEYS, CMD_NOSCRIPT,
    CMD_NO_AUTH, CMD_PUBSUB, CMD_READONLY, CMD_SKIP_SLOWLOG, CMD_SLOW, CMD_STALE, CMD_WRITE,
};
use crate::engine::core::Value;
use crate::framework::CommandMeta;
use bytes::Bytes;

command_macros::portable_command_support!();

pub(crate) fn flags_to_strings(flags: u64) -> Vec<&'static str> {
    let mut names = Vec::new();
    for (flag, name) in [
        (CMD_WRITE, "write"),
        (CMD_DENYOOM, "denyoom"),
        (CMD_FAST, "fast"),
        (CMD_READONLY, "readonly"),
        (CMD_ADMIN, "admin"),
        (CMD_LOADING, "loading"),
        (CMD_STALE, "stale"),
        (CMD_NOSCRIPT, "noscript"),
        (CMD_NO_AUTH, "no_auth"),
        (CMD_PUBSUB, "pubsub"),
        (CMD_SLOW, "slow"),
        (CMD_MOVABLE_KEYS, "movablekeys"),
        (CMD_MODULE, "module"),
        (CMD_SKIP_SLOWLOG, "skip_slowlog"),
    ] {
        if flags & flag != 0 {
            names.push(name);
        }
    }
    names
}

pub(crate) fn metadata_entry(meta: &CommandMeta, subcommands: Vec<Value>) -> Value {
    let flags = flags_to_strings(meta.flags)
        .into_iter()
        .map(|flag| Value::SimpleString(Bytes::from_static(flag.as_bytes())))
        .collect();
    let categories = meta
        .acl_categories
        .iter()
        .map(|category| {
            Value::SimpleString(Bytes::from(format!("@{}", category.to_ascii_lowercase())))
        })
        .collect();
    Value::Array(vec![
        Value::bulk(Bytes::from(metadata_name(meta))),
        Value::integer(meta.arity as i64),
        Value::set(flags),
        Value::integer(meta.first_key as i64),
        Value::integer(meta.last_key as i64),
        Value::integer(meta.key_step as i64),
        Value::set(categories),
        Value::set(Vec::new()),
        Value::set(metadata_key_specs(meta)),
        Value::Array(subcommands),
    ])
}

pub(crate) fn metadata_name(meta: &CommandMeta) -> String {
    let Some(container) = meta.container else {
        return meta.name.to_ascii_lowercase();
    };
    let subcommand = meta
        .name
        .strip_prefix(container)
        .and_then(|name| name.strip_prefix('-'))
        .unwrap_or(meta.name);
    format!("{}|{}", container.to_ascii_lowercase(), subcommand)
}

fn metadata_key_specs(meta: &CommandMeta) -> Vec<Value> {
    if meta.first_key <= 0 {
        return Vec::new();
    }
    vec![Value::Map(vec![
        (
            bulk("flags"),
            Value::Array(meta.key_flags.iter().copied().map(bulk).collect()),
        ),
        (
            bulk("begin_search"),
            typed_spec(
                "index",
                Value::Map(vec![(bulk("index"), Value::integer(meta.first_key as i64))]),
            ),
        ),
        (
            bulk("find_keys"),
            typed_spec(
                "range",
                Value::Map(vec![
                    (
                        bulk("lastkey"),
                        Value::integer(if meta.last_key < 0 {
                            meta.last_key as i64
                        } else {
                            (meta.last_key - meta.first_key) as i64
                        }),
                    ),
                    (bulk("keystep"), Value::integer(meta.key_step as i64)),
                    (bulk("limit"), Value::integer(0)),
                ]),
            ),
        ),
    ])]
}

fn typed_spec(kind: &'static str, spec: Value) -> Value {
    Value::Map(vec![(bulk("type"), bulk(kind)), (bulk("spec"), spec)])
}

fn bulk(value: &'static str) -> Value {
    Value::bulk(Bytes::from_static(value.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::flags_to_strings;
    use crate::engine::command::{CMD_FAST, CMD_WRITE};

    const DEPRECATED_RANDOM_BIT: u64 = 1 << 4;

    #[test]
    fn module_random_flag_is_not_rendered() {
        assert_eq!(
            flags_to_strings(CMD_WRITE | DEPRECATED_RANDOM_BIT | CMD_FAST),
            vec!["write", "fast"]
        );
    }
}
