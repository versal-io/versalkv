use crate::engine::extension::module::registry::{
    ModuleKeyBeginSearch, ModuleKeyFindKeys, ModuleKeySpec,
};
use crate::engine::extension::module::ModuleRegistry;
use crate::foundation::io::protocol::Value;
use crate::framework::{CommandMeta, CommandRegistry, CommandSpec};
use bytes::Bytes;

use super::render::{flags_to_strings, metadata_entry};

pub(crate) fn make_entry(
    spec: &CommandSpec,
    registry: &CommandRegistry,
    modules: &ModuleRegistry,
) -> Value {
    let module_info = modules
        .lookup_cmd_func(spec.name.as_str())
        .and_then(|entry| entry.info.as_ref());
    let mut flag_strs = flags_to_strings(spec.flags);
    if let Some(info) = module_info {
        flag_strs.retain(|flag| *flag != "movablekeys");
        if info.getkeys_api
            || info.key_specs.iter().any(|key_spec| {
                !matches!(
                    key_spec.begin_search,
                    ModuleKeyBeginSearch::Index { .. }
                )
            })
            || info
                .key_specs
                .windows(2)
                .any(|pair| match (&pair[0].begin_search, &pair[1].begin_search) {
                    (
                        ModuleKeyBeginSearch::Index { pos: left },
                        ModuleKeyBeginSearch::Index { pos: right },
                    ) => *right != *left + 1,
                    _ => false,
                })
        {
            flag_strs.push("movablekeys");
        }
    }
    let flag_vals: Vec<Value> = flag_strs
        .iter()
        .map(|f| Value::SimpleString(Bytes::from_static(f.as_bytes())))
        .collect();
    let key_specs = module_info
        .map(module_key_specs)
        .unwrap_or_else(|| command_key_specs(spec));
    let acl_categories = module_info
        .filter(|info| !info.acl_categories.is_empty())
        .map(|info| category_values(info.acl_categories.iter().map(String::as_str)))
        .unwrap_or_else(|| category_values(spec.acl_categories.iter().copied()));
    let mut subcommands = crate::framework::generated::commands::ALL_COMMAND_META
        .iter()
        .filter(|meta| {
            meta.container
                .is_some_and(|container| container.eq_ignore_ascii_case(spec.name.as_str()))
        })
        .map(|meta| make_generated_entry(meta))
        .collect::<Vec<_>>();
    subcommands.extend(
        modules
        .subcommand_names(spec.name.as_str())
        .into_iter()
        .filter_map(|name| registry.lookup(name.as_bytes()))
        .map(|subcommand| make_entry(subcommand, registry, modules)),
    );
    Value::Array(vec![
        Value::bulk(Bytes::copy_from_slice(spec.name.as_str().as_bytes())), // 0: name
        Value::integer(spec.arity as i64),                         // 1: arity
        Value::set(flag_vals),                                     // 2: flags (set in RESP3)
        Value::integer(spec.first_key as i64),                     // 3: first_key
        Value::integer(spec.last_key as i64),                      // 4: last_key
        Value::integer(spec.key_step as i64),                      // 5: key_step
        Value::set(acl_categories),                                // 6: acl_categories
        Value::set(Vec::new()),                                    // 7: tips ( set)
        Value::set(key_specs),                                     // 8: key_specs
        Value::Array(subcommands),                                 // 9: subcommands
    ])
}

pub(crate) fn make_generated_entry(meta: &CommandMeta) -> Value {
    metadata_entry(meta, Vec::new())
}

fn category_values<'a>(categories: impl Iterator<Item = &'a str>) -> Vec<Value> {
    categories
        .map(|category| {
            Value::SimpleString(Bytes::from(format!(
                "@{}",
                category.to_ascii_lowercase()
            )))
        })
        .collect()
}

fn module_key_specs(info: &crate::engine::extension::module::ModuleCommandInfo) -> Vec<Value> {
    if !info.key_specs.is_empty() {
        return info.key_specs.iter().map(key_spec_value).collect();
    }
    let Some((first, last, step)) = info.legacy_keys else {
        return Vec::new();
    };
    if first <= 0 {
        return Vec::new();
    }
    vec![key_spec_value(&ModuleKeySpec {
        flags: {
            let mut flags = vec!["RW", "access", "update"];
            if info.getkeys_api {
                flags.push("variable_flags");
            }
            flags
        },
        begin_search: ModuleKeyBeginSearch::Index { pos: first },
        find_keys: ModuleKeyFindKeys::Range {
            last_key: if last < 0 { last } else { last - first },
            key_step: step,
            limit: 0,
        },
    })]
}

fn command_key_specs(spec: &CommandSpec) -> Vec<Value> {
    if let Some(key_specs) = super::render::rename_key_specs(spec.name.as_str()) {
        return key_specs;
    }
    if let Some(key_specs) = super::render::geo_key_specs(spec.name.as_str()) {
        return key_specs;
    }
    if spec.first_key <= 0 {
        return Vec::new();
    }
    vec![key_spec_value(&ModuleKeySpec {
        flags: spec.key_flags.to_vec(),
        begin_search: ModuleKeyBeginSearch::Index {
            pos: spec.first_key,
        },
        find_keys: ModuleKeyFindKeys::Range {
            last_key: if spec.last_key < 0 {
                spec.last_key
            } else {
                spec.last_key - spec.first_key
            },
            key_step: spec.key_step,
            limit: 0,
        },
    })]
}

fn key_spec_value(spec: &ModuleKeySpec) -> Value {
    Value::Map(vec![
        (
            bulk("flags"),
            Value::Array(
                spec.flags
                    .iter()
                    .map(|flag| Value::SimpleString(Bytes::from_static(flag.as_bytes())))
                    .collect(),
            ),
        ),
        (bulk("begin_search"), begin_search_value(&spec.begin_search)),
        (bulk("find_keys"), find_keys_value(&spec.find_keys)),
    ])
}

fn begin_search_value(begin_search: &ModuleKeyBeginSearch) -> Value {
    match begin_search {
        ModuleKeyBeginSearch::Unknown => typed_spec("unknown", Value::Map(Vec::new())),
        ModuleKeyBeginSearch::Index { pos } => typed_spec(
            "index",
            Value::Map(vec![(bulk("index"), Value::integer(i64::from(*pos)))]),
        ),
        ModuleKeyBeginSearch::Keyword {
            keyword,
            start_from,
        } => typed_spec(
            "keyword",
            Value::Map(vec![
                (bulk("keyword"), Value::bulk(keyword.clone())),
                (
                    bulk("startfrom"),
                    Value::integer(i64::from(*start_from)),
                ),
            ]),
        ),
    }
}

fn find_keys_value(find_keys: &ModuleKeyFindKeys) -> Value {
    match find_keys {
        ModuleKeyFindKeys::Unknown => typed_spec("unknown", Value::Map(Vec::new())),
        ModuleKeyFindKeys::Range {
            last_key,
            key_step,
            limit,
        } => typed_spec(
            "range",
            Value::Map(vec![
                (bulk("lastkey"), Value::integer(i64::from(*last_key))),
                (bulk("keystep"), Value::integer(i64::from(*key_step))),
                (bulk("limit"), Value::integer(i64::from(*limit))),
            ]),
        ),
        ModuleKeyFindKeys::KeyNum {
            key_num_idx,
            first_key,
            key_step,
        } => typed_spec(
            "keynum",
            Value::Map(vec![
                (
                    bulk("keynumidx"),
                    Value::integer(i64::from(*key_num_idx)),
                ),
                (bulk("firstkey"), Value::integer(i64::from(*first_key))),
                (bulk("keystep"), Value::integer(i64::from(*key_step))),
            ]),
        ),
    }
}

fn typed_spec(kind: &'static str, spec: Value) -> Value {
    Value::Map(vec![(bulk("type"), bulk(kind)), (bulk("spec"), spec)])
}

fn bulk(value: &'static str) -> Value {
    Value::bulk(Bytes::from_static(value.as_bytes()))
}

pub(crate) fn cmd_entry_name(entry: &Value) -> &[u8] {
    match entry {
        Value::Array(ref parts) => match &parts[0] {
            Value::Bulk(b) => b.as_ref(),
            _ => &[],
        },
        _ => &[],
    }
}

pub(crate) fn bulk_as_bytes(val: &Value) -> &[u8] {
    match val {
        Value::Bulk(b) => b.as_ref(),
        _ => &[],
    }
}
