use super::command_info::{cmd_entry_name, make_entry, make_generated_entry};
use crate::engine::extension::module::ModuleRegistry;
use crate::framework::CommandRegistry;
use crate::foundation::io::protocol::Value;
use bytes::Bytes;

pub(crate) fn command_info(
    reg: &CommandRegistry,
    modules: &ModuleRegistry,
    args: &[Bytes],
) -> Value {
    if args.is_empty() {
        let mut entries: Vec<Value> = reg
            .iter()
            .map(|spec| make_entry(spec, reg, modules))
            .collect();
        entries.sort_by(|a, b| {
            let a_name = cmd_entry_name(a);
            let b_name = cmd_entry_name(b);
            a_name.cmp(b_name)
        });
        return Value::Array(entries);
    }
    let mut results = Vec::new();
    for name_bytes in args {
        let name = match std::str::from_utf8(name_bytes) {
            Ok(s) => s.to_ascii_lowercase(),
            Err(_) => {
                results.push(Value::null());
                continue;
            }
        };
        let found = reg.iter().find(|spec| spec.name == name);
        match found {
            Some(spec) => results.push(make_entry(spec, reg, modules)),
            None => match crate::framework::generated::commands::acl_command_meta(&name) {
                Some(meta) => results.push(make_generated_entry(meta)),
                None => results.push(Value::null()),
            },
        }
    }
    Value::Array(results)
}

#[cfg(test)]
mod tests {
    use super::command_info;
    use crate::engine::extension::module::ModuleRegistry;
    use crate::foundation::io::protocol::Value;
    use crate::framework::CommandRegistry;
    use bytes::Bytes;

    fn category_names(entry: &Value) -> Vec<String> {
        let Value::Array(fields) = entry else {
            panic!("expected COMMAND INFO entry");
        };
        let Value::Set(categories) = &fields[6] else {
            panic!("expected ACL category set");
        };
        categories
            .iter()
            .map(|value| match value {
                Value::SimpleString(value) => String::from_utf8(value.to_vec()).unwrap(),
                _ => panic!("expected ACL category status string"),
            })
            .collect()
    }

    #[test]
    fn command_info_reports_generated_top_level_categories() {
        let mut registry = CommandRegistry::new();
        crate::framework::commands::register_all(&mut registry);
        let modules = ModuleRegistry::new();
        let Value::Array(entries) = command_info(
            &registry,
            &modules,
            &[Bytes::from_static(b"GET")],
        ) else {
            panic!("expected COMMAND INFO array");
        };

        assert_eq!(
            category_names(&entries[0]),
            vec!["@fast", "@read", "@string"]
        );
    }

    #[test]
    fn command_info_reports_generated_subcommand_categories() {
        let mut registry = CommandRegistry::new();
        crate::framework::commands::register_all(&mut registry);
        let modules = ModuleRegistry::new();
        let Value::Array(entries) = command_info(
            &registry,
            &modules,
            &[Bytes::from_static(b"CONFIG|GET")],
        ) else {
            panic!("expected COMMAND INFO array");
        };

        assert_eq!(
            category_names(&entries[0]),
            vec!["@admin", "@dangerous", "@slow"]
        );
    }
}
