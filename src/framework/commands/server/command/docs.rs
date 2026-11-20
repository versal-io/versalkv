use crate::foundation::io::protocol::Value;
use crate::framework::context::ServerContext;
use crate::framework::CommandRegistry;
use crate::framework::CMD_SENTINEL;
use bytes::Bytes;

pub(crate) fn command_docs(reg: &CommandRegistry, ctx: &ServerContext, args: &[Bytes]) -> Value {
    if args.is_empty() {
        return Value::Map(Vec::new());
    }
    let is_sentinel = ctx.sentinel_state().sentinel_mode;
    let mut result = Vec::new();
    for name_bytes in args {
        let name = match std::str::from_utf8(name_bytes) {
            Ok(s) => s.to_ascii_lowercase(),
            Err(_) => continue,
        };
        let parent_name = name.split('|').next().unwrap_or(&name);
        let found = reg
            .iter()
            .find(|spec| spec.name.as_str().eq_ignore_ascii_case(&name))
            .or_else(|| {
                reg.iter()
                    .find(|spec| spec.name.as_str().eq_ignore_ascii_case(parent_name))
            });
        if let Some(spec) = found {
            if is_sentinel && (spec.flags & CMD_SENTINEL) == 0 {
                continue;
            }
            let doc = if let Some(module_name) = ctx
                .module_registry()
                .command_module_name(spec.name.as_str())
            {
                module_doc(ctx, spec.name.as_str(), module_name)
            } else {
                super::render::builtin_doc(&name)
            };
            result.push((Value::bulk(name_bytes.clone()), doc));
        }
    }
    if result.is_empty() {
        Value::null()
    } else {
        Value::Map(result)
    }
}

fn module_doc(ctx: &ServerContext, command_name: &str, module_name: &str) -> Value {
    let info = ctx
        .module_registry()
        .lookup_cmd_func(command_name)
        .and_then(|entry| entry.info.as_ref());
    let mut fields = vec![
        (
            bulk("summary"),
            Value::bulk(Bytes::copy_from_slice(
                info.and_then(|info| info.summary.as_deref())
                    .unwrap_or("")
                    .as_bytes(),
            )),
        ),
        (
            bulk("since"),
            Value::bulk(Bytes::copy_from_slice(
                info.and_then(|info| info.since.as_deref())
                    .unwrap_or("")
                    .as_bytes(),
            )),
        ),
        (bulk("group"), bulk("module")),
        (
            bulk("module"),
            Value::bulk(Bytes::copy_from_slice(module_name.as_bytes())),
        ),
    ];
    let subcommands: Vec<_> = ctx
        .module_registry()
        .subcommand_names(command_name)
        .into_iter()
        .flat_map(|name| {
            [
                Value::bulk(Bytes::copy_from_slice(name.as_bytes())),
                Value::Map(vec![
                    (bulk("group"), bulk("module")),
                    (
                        bulk("module"),
                        Value::bulk(Bytes::copy_from_slice(module_name.as_bytes())),
                    ),
                ]),
            ]
        })
        .collect();
    if !subcommands.is_empty() {
        fields.push((bulk("subcommands"), Value::Array(subcommands)));
    }
    Value::Map(fields)
}

fn bulk(value: &'static str) -> Value {
    Value::bulk(Bytes::from_static(value.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::extension::module::{ModuleCommandInfo, ModuleRef};

    fn field<'a>(value: &'a Value, name: &'static str) -> Option<&'a Value> {
        let Value::Map(fields) = value else {
            panic!("expected a documentation map, got {value:?}");
        };
        fields
            .iter()
            .find(|(key, _)| key == &bulk(name))
            .map(|(_, value)| value)
    }

    fn arguments(value: &Value) -> &[Value] {
        let Some(Value::Array(arguments)) = field(value, "arguments") else {
            panic!("expected a documentation argument array, got {value:?}");
        };
        arguments
    }

    fn builtin_command_doc(name: &'static str) -> Value {
        let mut registry = CommandRegistry::new();
        crate::framework::commands::register_all(&mut registry);
        let reply = command_docs(
            &registry,
            &ServerContext::new(1),
            &[Bytes::from_static(name.as_bytes())],
        );
        field(&reply, name).unwrap().clone()
    }

    fn optional_flags() -> Value {
        Value::set(vec![Value::SimpleString(Bytes::from_static(b"optional"))])
    }

    #[test]
    fn builtin_docs_preserve_sorted_set_double_arguments_in_source_order() {
        for (name, index, argument_name) in [
            ("zincrby", 1, "increment"),
            ("zinter", 2, "weight"),
            ("zinterstore", 3, "weight"),
            ("zunion", 2, "weight"),
            ("zunionstore", 3, "weight"),
        ] {
            let doc = builtin_command_doc(name);
            let argument = &arguments(&doc)[index];
            assert_eq!(field(argument, "name"), Some(&bulk(argument_name)));
            assert_eq!(field(argument, "type"), Some(&bulk("double")));
            assert_eq!(field(argument, "display_text"), Some(&bulk(argument_name)));
            assert_eq!(field(argument, "arguments"), None);
            if argument_name == "weight" {
                assert_eq!(field(argument, "token"), Some(&bulk("WEIGHTS")));
                assert_eq!(
                    field(argument, "flags"),
                    Some(&Value::set(vec![
                        Value::SimpleString(Bytes::from_static(b"optional")),
                        Value::SimpleString(Bytes::from_static(b"multiple")),
                    ]))
                );
            } else {
                assert_eq!(field(argument, "flags"), None);
            }
        }
        let doc = builtin_command_doc("zinterstore");
        assert_eq!(
            field(&arguments(&doc)[0], "name"),
            Some(&bulk("destination"))
        );
        assert_eq!(
            field(&arguments(&doc)[0], "key_spec_index"),
            Some(&Value::integer(0))
        );
        assert_eq!(
            field(&arguments(&doc)[2], "key_spec_index"),
            Some(&Value::integer(1))
        );
    }

    #[test]
    fn builtin_docs_preserve_nested_expiry_and_zadd_conditions() {
        for (name, index) in [
            ("expire", 2),
            ("expireat", 2),
            ("pexpire", 2),
            ("pexpireat", 2),
            ("hexpire", 2),
            ("hexpireat", 2),
            ("hpexpire", 2),
            ("hpexpireat", 2),
            ("zadd", 1),
        ] {
            let doc = builtin_command_doc(name);
            let condition = &arguments(&doc)[index];
            assert_eq!(field(condition, "type"), Some(&bulk("oneof")));
            assert_eq!(field(condition, "flags"), Some(&optional_flags()));
            assert_eq!(field(condition, "display_text"), None);
            let alternatives = arguments(condition);
            assert_eq!(field(&alternatives[0], "token"), Some(&bulk("NX")));
            let xx_comparison = &alternatives[1];
            assert_eq!(field(xx_comparison, "type"), Some(&bulk("block")));
            assert_eq!(field(xx_comparison, "display_text"), None);
            let options = arguments(xx_comparison);
            assert_eq!(field(&options[0], "token"), Some(&bulk("XX")));
            assert_eq!(field(&options[0], "flags"), Some(&optional_flags()));
            let comparison = &options[1];
            assert_eq!(field(comparison, "flags"), Some(&optional_flags()));
            let comparisons = arguments(comparison);
            assert_eq!(comparisons.len(), 2);
            for (option, token) in comparisons.iter().zip(["GT", "LT"]) {
                assert_eq!(field(option, "type"), Some(&bulk("pure-token")));
                assert_eq!(field(option, "token"), Some(&bulk(token)));
                assert_eq!(field(option, "flags"), None);
            }
        }
        for name in ["expireat", "pexpireat", "hexpireat", "hpexpireat"] {
            let doc = builtin_command_doc(name);
            assert_eq!(field(&arguments(&doc)[1], "type"), Some(&bulk("unix-time")));
        }
    }

    #[test]
    fn builtin_docs_preserve_mutually_exclusive_argument_blocks() {
        for (name, index, tokens, kind) in [
            ("restore", 5, ["IDLETIME", "FREQ"], "integer"),
            ("restore-asking", 5, ["IDLETIME", "FREQ"], "integer"),
            ("lcs", 2, ["LEN", "IDX"], "pure-token"),
        ] {
            let doc = builtin_command_doc(name);
            let selector = &arguments(&doc)[index];
            assert_eq!(field(selector, "type"), Some(&bulk("oneof")));
            assert_eq!(field(selector, "flags"), Some(&optional_flags()));
            assert_eq!(arguments(selector).len(), 2);
            for (option, token) in arguments(selector).iter().zip(tokens) {
                assert_eq!(field(option, "type"), Some(&bulk(kind)));
                assert_eq!(field(option, "token"), Some(&bulk(token)));
                assert_eq!(field(option, "flags"), None);
            }
        }
    }

    #[test]
    fn builtin_docs_preserve_leaf_metadata_and_normalize_string_aliases() {
        let doc = builtin_command_doc("cluster|setslot");
        let importing = &arguments(&arguments(&doc)[1])[0];
        assert_eq!(field(importing, "name"), Some(&bulk("importing")));
        assert_eq!(field(importing, "display_text"), Some(&bulk("node-id")));
        assert_eq!(field(importing, "token"), Some(&bulk("IMPORTING")));
        assert_eq!(field(&arguments(&doc)[2], "since"), Some(&bulk("8.0.0")));

        let doc = builtin_command_doc("client|kill");
        let old_format = &arguments(&arguments(&doc)[0])[0];
        assert_eq!(field(old_format, "display_text"), Some(&bulk("ip:port")));
        assert_eq!(field(old_format, "deprecated_since"), Some(&bulk("2.8.12")));
        let new_format = &arguments(&arguments(&doc)[0])[1];
        let client_type = &arguments(new_format)[1];
        assert_eq!(
            field(&arguments(client_type)[0], "token"),
            Some(&bulk("NORMAL"))
        );

        let doc = builtin_command_doc("set");
        let comparison = &arguments(&arguments(&doc)[2])[2];
        assert_eq!(field(comparison, "since"), Some(&bulk("8.1.0")));
        assert_eq!(
            field(comparison, "summary"),
            Some(&bulk("Sets the key's value only if the current value matches the specified comparison value."))
        );

        for (name, index) in [("hincrby", 1), ("zincrby", 2)] {
            let doc = builtin_command_doc(name);
            assert_eq!(
                field(&arguments(&doc)[index], "type"),
                Some(&bulk("string"))
            );
        }
        let doc = builtin_command_doc("xread");
        let id = &arguments(&arguments(&doc)[2])[1];
        assert_eq!(field(id, "name"), Some(&bulk("id")));
        assert_eq!(field(id, "display_text"), Some(&bulk("id")));

        let doc = builtin_command_doc("hexpire");
        let fields = arguments(arguments(&doc).last().unwrap());
        assert_eq!(field(&fields[0], "name"), Some(&bulk("numfields")));
        assert_eq!(field(&fields[0], "flags"), None);
        assert_eq!(
            field(&fields[1], "flags"),
            Some(&Value::set(vec![Value::SimpleString(Bytes::from_static(
                b"multiple"
            ))]))
        );
    }

    #[test]
    fn builtin_docs_render_repeated_token_flags_as_resp_sets() {
        let doc = builtin_command_doc("sort");
        let get = arguments(&doc)
            .iter()
            .find(|argument| field(argument, "token") == Some(&bulk("GET")))
            .unwrap();
        let flags = field(get, "flags").unwrap();
        assert_eq!(
            flags,
            &Value::set(vec![
                Value::SimpleString(Bytes::from_static(b"optional")),
                Value::SimpleString(Bytes::from_static(b"multiple")),
                Value::SimpleString(Bytes::from_static(b"multiple_token")),
            ])
        );
        use crate::foundation::io::protocol::serializer::{
            serialize_to_vec_resp2, serialize_to_vec_resp3,
        };
        assert_eq!(
            serialize_to_vec_resp2(flags),
            b"*3\r\n+optional\r\n+multiple\r\n+multiple_token\r\n"
        );
        assert_eq!(
            serialize_to_vec_resp3(flags),
            b"~3\r\n+optional\r\n+multiple\r\n+multiple_token\r\n"
        );
    }

    #[test]
    fn builtin_docs_preserve_upstream_string_repetition_flags() {
        use crate::foundation::io::protocol::serializer::{
            serialize_to_vec_resp2, serialize_to_vec_resp3,
        };

        let doc = builtin_command_doc("client|capa");
        let flags = field(&arguments(&doc)[0], "flags").expect("repeated capability argument");
        assert_eq!(
            flags,
            &Value::set(vec![Value::SimpleString(Bytes::from_static(b"multiple"))])
        );
        assert_eq!(serialize_to_vec_resp2(flags), b"*1\r\n+multiple\r\n");
        assert_eq!(serialize_to_vec_resp3(flags), b"~1\r\n+multiple\r\n");
    }

    #[test]
    fn builtin_docs_omit_arguments_for_commands_without_parameters() {
        let doc = builtin_command_doc("dbsize");
        assert_eq!(field(&doc, "arguments"), None);
        assert_eq!(field(&doc, "group"), Some(&bulk("server")));
    }

    #[test]
    fn portable_docs_share_generated_argument_rendering() {
        use crate::framework::commands::introspection::CommandCatalog;
        use crate::framework::commands::server::command::static_catalog::STATIC_COMMAND_CATALOG;

        for name in ["zincrby", "expire", "zadd", "object|encoding"] {
            let reply = STATIC_COMMAND_CATALOG.docs(&[Bytes::from_static(name.as_bytes())]);
            assert_eq!(field(&reply, name), Some(&builtin_command_doc(name)));
        }
        assert_eq!(
            STATIC_COMMAND_CATALOG.docs(&[Bytes::from_static(b"cluster|setslot")]),
            Value::Null
        );
    }

    #[test]
    fn module_docs_keep_exported_fields_after_acl_metadata_updates() {
        let mut ctx = ServerContext::new(1);
        ctx.module_registry_mut()
            .register(ModuleRef::new("docs-probe", std::ptr::null_mut(), 1, true))
            .unwrap();
        ctx.module_registry_mut()
            .register_command("docs-probe", "docs-probe.xadd", None)
            .unwrap();
        assert!(ctx.module_registry_mut().set_legacy_command_info(
            "docs-probe.xadd",
            (0, 0, 0),
            false,
            -5,
        ));
        let mut info = ModuleCommandInfo {
            summary: Some("Appends a message.".into()),
            since: Some("5.0.0".into()),
            key_specs: Vec::new(),
            acl_categories: Vec::new(),
            legacy_keys: None,
            getkeys_api: false,
            arity: None,
        };
        assert!(ctx
            .module_registry_mut()
            .set_command_info("docs-probe.xadd", info.clone()));
        info.summary = None;
        info.since = None;
        info.acl_categories.push("write".into());
        assert!(ctx
            .module_registry_mut()
            .set_command_info("docs-probe.xadd", info));
        assert_eq!(
            module_doc(&ctx, "docs-probe.xadd", "docs-probe"),
            Value::Map(vec![
                (bulk("summary"), Value::bulk("Appends a message.")),
                (bulk("since"), Value::bulk("5.0.0")),
                (bulk("group"), bulk("module")),
                (bulk("module"), Value::bulk("docs-probe")),
            ])
        );
    }
}
