use crate::engine::command::{
    CMD_ADMIN, CMD_DENYOOM, CMD_FAST, CMD_LOADING, CMD_MODULE, CMD_MOVABLE_KEYS, CMD_NOSCRIPT,
    CMD_NO_AUTH, CMD_PUBSUB, CMD_READONLY, CMD_SKIP_SLOWLOG, CMD_SLOW, CMD_STALE, CMD_WRITE,
};
use crate::engine::core::Value;
use crate::framework::CommandMeta;
use bytes::Bytes;

command_macros::portable_command_support!();

pub(super) fn builtin_doc(name: &str) -> Value {
    let metadata = crate::framework::generated::commands::catalog::COMMAND_DOC_META
        .iter()
        .find(|metadata| metadata.name.eq_ignore_ascii_case(name));
    let fields = [
        ("summary", metadata.map_or("", |metadata| metadata.summary)),
        ("since", metadata.map_or("", |metadata| metadata.since)),
        ("group", metadata.map_or("", |metadata| metadata.group)),
    ];
    Value::Map(fields.into_iter().map(|(name, value)| {
        (
            Value::bulk(Bytes::from_static(name.as_bytes())),
            Value::bulk(Bytes::from_static(value.as_bytes())),
        )
    }).collect())
}

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
    if let Some(key_specs) = rename_key_specs(meta.name) {
        return key_specs;
    }
    if let Some(key_specs) = geo_key_specs(meta.name) {
        return key_specs;
    }
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

pub(crate) fn rename_key_specs(name: &str) -> Option<Vec<Value>> {
    let destination_flags: &[&'static str] = if name.eq_ignore_ascii_case("rename") {
        &["OW", "update"]
    } else if name.eq_ignore_ascii_case("renamenx") {
        &["OW", "insert"]
    } else {
        return None;
    };
    Some(vec![
        single_key_spec(
            &["RW", "access", "delete"],
            typed_spec(
                "index",
                Value::Map(vec![(bulk("index"), Value::integer(1))]),
            ),
        ),
        single_key_spec(
            destination_flags,
            typed_spec(
                "index",
                Value::Map(vec![(bulk("index"), Value::integer(2))]),
            ),
        ),
    ])
}

/// Preserve variable destination specs instead of merging their write flags
/// into the read-only source spec.
pub(crate) fn geo_key_specs(name: &str) -> Option<Vec<Value>> {
    let start_from = if name.eq_ignore_ascii_case("georadius") {
        6
    } else if name.eq_ignore_ascii_case("georadiusbymember") {
        5
    } else {
        return None;
    };
    let source = single_key_spec(
        &["RO", "access"],
        typed_spec("index", Value::Map(vec![(bulk("index"), Value::integer(1))])),
    );
    let mut specs = vec![source];
    for keyword in ["STORE", "STOREDIST"] {
        specs.push(single_key_spec(
            &["OW", "update", "variable_flags"],
            typed_spec(
                "keyword",
                Value::Map(vec![
                    (bulk("keyword"), bulk(keyword)),
                    (bulk("startfrom"), Value::integer(start_from)),
                ]),
            ),
        ));
    }
    Some(specs)
}

fn single_key_spec(flags: &[&'static str], begin_search: Value) -> Value {
    Value::Map(vec![
        (
            bulk("flags"),
            Value::Array(
                flags
                    .iter()
                    .map(|flag| Value::SimpleString(Bytes::from_static(flag.as_bytes())))
                    .collect(),
            ),
        ),
        (bulk("begin_search"), begin_search),
        (
            bulk("find_keys"),
            typed_spec(
                "range",
                Value::Map(vec![
                    (bulk("lastkey"), Value::integer(0)),
                    (bulk("keystep"), Value::integer(1)),
                    (bulk("limit"), Value::integer(0)),
                ]),
            ),
        ),
    ])
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
