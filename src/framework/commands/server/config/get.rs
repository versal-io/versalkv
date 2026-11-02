use crate::engine::extension::config::spec::ConfigValue;
use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::{CommandArgs, CommandError};
use bytes::Bytes;

use super::config_registry;

pub fn config_get(args: &CommandArgs, ctx: &ServerContext) -> Value {
    if args.args.len() < 2 {
        return CommandError::wrong_arity("config");
    }
    let patterns: Vec<&[u8]> = args.args[1..].iter().map(Bytes::as_ref).collect();
    let mut results = Vec::new();

    let pattern_matches = |pattern: &[u8], name: &str| -> bool {
        if pattern
            .iter()
            .any(|byte| matches!(byte, b'[' | b'*' | b'?'))
        {
            crate::framework::commands::pattern::string_match(pattern, name.as_bytes(), true)
        } else {
            pattern.eq_ignore_ascii_case(name.as_bytes())
        }
    };
    let r = ctx.config();
    let mut seen_keys: Vec<Vec<u8>> = Vec::new();
    let mut add_from_registry = |results: &mut Vec<Value>, name: &str, val: &ConfigValue| {
        let name_bytes = name.as_bytes().to_vec();
        if seen_keys.iter().any(|k| k == &name_bytes) {
            return;
        }
        seen_keys.push(name_bytes);
        let val_bytes = val.as_str().into_bytes();
        results.push(Value::bulk(Bytes::from(name.as_bytes().to_vec())));
        results.push(Value::bulk(Bytes::copy_from_slice(&val_bytes)));
    };
    for spec in config_registry().all_specs() {
        let name = spec.name;
        for pattern in &patterns {
            let has_glob = pattern
                .iter()
                .any(|byte| matches!(byte, b'[' | b'*' | b'?'));
            if spec.is_hidden() && has_glob {
                continue;
            }
            let name_match = pattern_matches(pattern, name);
            let alias_match = spec
                .alias
                .is_some_and(|alias| pattern_matches(pattern, alias));
            if !name_match && !alias_match {
                continue;
            }
            let val = (spec.get)(r);
            if name_match {
                add_from_registry(&mut results, name, &val);
            }
            if let Some(alias) = spec.alias.filter(|_| alias_match) {
                add_from_registry(&mut results, alias, &val);
            }
        }
    }

    let mut entries: Vec<(Vec<u8>, Value, Value)> = results
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let key = match &pair[0] {
                Value::Bulk(bytes) => bytes.to_vec(),
                _ => Vec::new(),
            };
            (key, pair[0].clone(), pair[1].clone())
        })
        .collect();
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    let mut sorted_results = Vec::with_capacity(results.len());
    for (_, key, value) in entries {
        sorted_results.push(key);
        sorted_results.push(value);
    }
    Value::Array(sorted_results)
}

#[cfg(test)]
mod tests {
    use super::config_get;
    use crate::engine::command::CommandArgs;
    use crate::foundation::io::protocol::Value;
    use crate::framework::context::ServerContext;
    use bytes::Bytes;

    #[test]
    fn config_get_matches_every_requested_pattern() {
        let ctx = ServerContext::new(1);
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"CONFIG"),
            Bytes::from_static(b"GET"),
            Bytes::from_static(b"maxmemory"),
            Bytes::from_static(b"maxmemory*"),
            Bytes::from_static(b"bind"),
            Bytes::from_static(b"*of"),
        ]);

        let Value::Array(values) = config_get(&args, &ctx) else {
            panic!("CONFIG GET must return an array");
        };
        let keys: Vec<_> = values
            .iter()
            .step_by(2)
            .filter_map(|value| match value {
                Value::Bulk(bytes) => Some(bytes.as_ref()),
                _ => None,
            })
            .collect();

        assert!(keys.contains(&b"maxmemory".as_slice()));
        assert!(keys.contains(&b"maxmemory-clients".as_slice()));
        assert!(keys.contains(&b"maxmemory-samples".as_slice()));
        assert!(keys.contains(&b"replicaof".as_slice()));
        assert!(keys.contains(&b"slaveof".as_slice()));
        assert!(keys.contains(&b"bind".as_slice()));
    }

    #[test]
    fn config_get_preserves_escaped_character_class_case() {
        let ctx = ServerContext::new(1);
        let escaped = CommandArgs::from_parts(vec![
            Bytes::from_static(b"CONFIG"),
            Bytes::from_static(b"GET"),
            Bytes::from_static(br"[\M]axmemory"),
        ]);
        let unescaped = CommandArgs::from_parts(vec![
            Bytes::from_static(b"CONFIG"),
            Bytes::from_static(b"GET"),
            Bytes::from_static(b"[M]axmemory"),
        ]);

        assert_eq!(config_get(&escaped, &ctx), Value::Array(Vec::new()));
        let Value::Array(values) = config_get(&unescaped, &ctx) else {
            panic!("CONFIG GET must return an array");
        };
        assert!(values.iter().step_by(2).any(
            |value| matches!(value, Value::Bulk(bytes) if bytes.as_ref() == b"maxmemory")
        ));
    }

    #[test]
    fn config_get_treats_backslash_as_literal_without_glob_metacharacters() {
        let ctx = ServerContext::new(1);
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"CONFIG"),
            Bytes::from_static(b"GET"),
            Bytes::from_static(br"\maxmemory"),
        ]);

        assert_eq!(config_get(&args, &ctx), Value::Array(Vec::new()));
    }

    #[test]
    fn config_get_exact_lookup_is_case_insensitive() {
        let ctx = ServerContext::new(1);
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"CONFIG"),
            Bytes::from_static(b"GET"),
            Bytes::from_static(b"MAXMEMORY"),
        ]);

        let Value::Array(values) = config_get(&args, &ctx) else {
            panic!("CONFIG GET must return an array");
        };
        let keys: Vec<_> = values
            .iter()
            .step_by(2)
            .filter_map(|value| match value {
                Value::Bulk(bytes) => Some(bytes.as_ref()),
                _ => None,
            })
            .collect();

        assert_eq!(keys, vec![b"maxmemory".as_slice()]);
    }

    #[test]
    fn config_get_hidden_alias_is_not_unlocked_for_other_patterns() {
        let ctx = ServerContext::new(1);
        let args = CommandArgs::from_parts(vec![
            Bytes::from_static(b"CONFIG"),
            Bytes::from_static(b"GET"),
            Bytes::from_static(b"lua-*"),
            Bytes::from_static(b"lua-enable-deprecated-api"),
        ]);

        let Value::Array(values) = config_get(&args, &ctx) else {
            panic!("CONFIG GET must return an array");
        };
        let keys: Vec<_> = values
            .iter()
            .step_by(2)
            .filter_map(|value| match value {
                Value::Bulk(bytes) => Some(bytes.as_ref()),
                _ => None,
            })
            .collect();

        assert_eq!(
            keys,
            vec![
                b"lua-enable-deprecated-api".as_slice(),
                b"lua-time-limit".as_slice(),
            ]
        );
    }
}
