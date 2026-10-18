use crate::engine::extension::config::spec::{ConfigSpec, ConfigType, ConfigValue};
use crate::engine::extension::config::spec_registry::ConfigRegistry;

pub(crate) fn register_security_configs(reg: &mut ConfigRegistry) {
    reg.register(&ConfigSpec {
        name: "requirepass",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &["sensitive"],
        alias: None,
        get: |rcfg| {
            ConfigValue::String(
                rcfg.security
                    .requirepass
                    .as_ref()
                    .map(|b| String::from_utf8_lossy(b).to_string())
                    .unwrap_or_default(),
            )
        },
        set: |rcfg, val| {
            if let ConfigValue::String(v) = val {
                rcfg.security.requirepass = if v.is_empty() {
                    None
                } else {
                    Some(v.as_bytes().to_vec())
                };
                Ok(())
            } else {
                Err("requirepass must be a string".to_string())
            }
        },
    });

    reg.register(&ConfigSpec {
        name: "primaryauth",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &["sensitive"],
        alias: Some("masterauth"),
        get: |rcfg| {
            ConfigValue::String(
                rcfg.security
                    .primaryauth
                    .as_ref()
                    .map(|b| String::from_utf8_lossy(b).to_string())
                    .unwrap_or_default(),
            )
        },
        set: |rcfg, val| {
            if let ConfigValue::String(v) = val {
                rcfg.security.primaryauth = if v.is_empty() {
                    None
                } else {
                    Some(v.as_bytes().to_vec())
                };
                Ok(())
            } else {
                Err("primaryauth must be a string".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "primaryuser",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &["sensitive"],
        alias: Some("masteruser"),
        get: |rcfg| {
            ConfigValue::String(
                rcfg.security
                    .primaryuser
                    .as_ref()
                    .map(|b| String::from_utf8_lossy(b).to_string())
                    .unwrap_or_default(),
            )
        },
        set: |rcfg, val| {
            if let ConfigValue::String(v) = val {
                rcfg.security.primaryuser = if v.is_empty() {
                    None
                } else {
                    Some(v.as_bytes().to_vec())
                };
                Ok(())
            } else {
                Err("primaryuser must be a string".to_string())
            }
        },
    });
}
