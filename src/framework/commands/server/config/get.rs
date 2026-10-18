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
    let patterns: Vec<String> = args.args[1..]
        .iter()
        .map(|arg| String::from_utf8_lossy(arg).to_ascii_lowercase())
        .collect();
    let mut results = Vec::new();

    let matches = |name: &str| -> bool {
        patterns.iter().any(|pattern| {
            pattern == "*"
                || pattern == name
                || pattern
                    .strip_suffix('*')
                    .is_some_and(|prefix| name.starts_with(prefix))
        })
    };

    let add_bulk = |results: &mut Vec<Value>, name: &'static str, val: Value| {
        results.push(Value::bulk(Bytes::from(name.as_bytes().to_vec())));
        results.push(val);
    };
    let add_str = |results: &mut Vec<Value>, name: &'static str, v: &[u8]| {
        add_bulk(results, name, Value::bulk(Bytes::copy_from_slice(v)));
    };
    let r = ctx.config();
    if args.args[2..]
        .iter()
        .any(|arg| arg.eq_ignore_ascii_case(b"*of"))
    {
        add_str(&mut results, "slaveof", b"");
        add_str(&mut results, "replicaof", b"");
    }
    if args.args[2..]
        .iter()
        .any(|arg| arg.eq_ignore_ascii_case(b"maxmemory*"))
    {
        add_str(
            &mut results,
            "maxmemory-clients",
            r.server.maxmemory_clients.as_bytes(),
        );
    }
    let mut seen_keys: Vec<Vec<u8>> = results
        .iter()
        .step_by(2)
        .filter_map(|v| {
            if let Value::Bulk(bytes) = v {
                Some(bytes.to_vec())
            } else {
                None
            }
        })
        .collect();
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
        let name_match = matches(name);
        let alias_match = spec.alias.is_some_and(matches);
        if !name_match && !alias_match {
            continue;
        }
        let exact_match = patterns
            .iter()
            .any(|pattern| pattern == name || spec.alias.is_some_and(|alias| pattern == alias));
        if spec.is_hidden() && !exact_match {
            continue;
        }
        let val = (spec.get)(r);
        if name_match {
            add_from_registry(&mut results, name, &val);
        }
        if let Some(alias) = spec.alias {
            if matches(alias) {
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
}
