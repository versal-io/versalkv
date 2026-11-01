use crate::engine::extension::config::qos::parse_priority_subnets;
use crate::engine::extension::config::spec::{ConfigSpec, ConfigType, ConfigValue};
use crate::engine::extension::config::spec_registry::ConfigRegistry;

pub fn register_qos_configs(registry: &mut ConfigRegistry) {
    registry.register(&ConfigSpec {
        name: "maxclients-reserved",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: u32::MAX as i64,
        flags: &[],
        alias: None,
        get: |config| ConfigValue::Numeric(config.server.maxclients_reserved as i64),
        set: |config, value| {
            if let ConfigValue::Numeric(value) = value {
                config.server.maxclients_reserved = *value as u32;
            }
            Ok(())
        },
    });
    registry.register(&ConfigSpec {
        name: "priority-preemptive-poll-interval-us",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i32::MAX as i64,
        flags: &[],
        alias: None,
        get: |config| {
            ConfigValue::Numeric(config.server.priority_preemptive_poll_interval_us as i64)
        },
        set: |config, value| {
            if let ConfigValue::Numeric(value) = value {
                config.server.priority_preemptive_poll_interval_us = *value as u64;
            }
            Ok(())
        },
    });
    registry.register(&ConfigSpec {
        name: "priority-subnets",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |config| ConfigValue::String(config.server.priority_subnets.clone()),
        set: |config, value| {
            if let ConfigValue::String(value) = value {
                if config.server.priority_subnets == *value {
                    return Ok(());
                }
                let subnets = parse_priority_subnets(value)?;
                config.server.priority_subnets = value.clone();
                config.server.compiled_priority_subnets = subnets;
                config.server.priority_subnets_generation =
                    config.server.priority_subnets_generation.wrapping_add(1);
            }
            Ok(())
        },
    });
}
