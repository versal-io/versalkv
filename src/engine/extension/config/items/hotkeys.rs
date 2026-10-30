use crate::engine::extension::config::spec::{ConfigSpec, ConfigType, ConfigValue};
use crate::engine::extension::config::spec_registry::ConfigRegistry;

pub(crate) fn register_hotkeys_configs(reg: &mut ConfigRegistry) {
    reg.register(&ConfigSpec {
        name: "hotkeys-sampling-percentage",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 1,
        numeric_max: 100,
        flags: &[],
        alias: None,
        get: |config| ConfigValue::Numeric(i64::from(config.server.hotkeys_sampling_percentage)),
        set: |config, value| {
            let ConfigValue::Numeric(value) = value else {
                return Err("hotkeys-sampling-percentage must be numeric".to_string());
            };
            config.server.hotkeys_sampling_percentage = *value as u8;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "hotkeys-top-k",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 1000,
        flags: &[],
        alias: None,
        get: |config| ConfigValue::Numeric(config.server.hotkeys_top_k as i64),
        set: |config, value| {
            let ConfigValue::Numeric(value) = value else {
                return Err("hotkeys-top-k must be numeric".to_string());
            };
            config.server.hotkeys_top_k = *value as usize;
            Ok(())
        },
    });
    reg.register(&ConfigSpec {
        name: "hotkeys-window-seconds",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 1,
        numeric_max: 300,
        flags: &[],
        alias: None,
        get: |config| ConfigValue::Numeric(config.server.hotkeys_window_seconds as i64),
        set: |config, value| {
            let ConfigValue::Numeric(value) = value else {
                return Err("hotkeys-window-seconds must be numeric".to_string());
            };
            config.server.hotkeys_window_seconds = *value as u64;
            Ok(())
        },
    });
}
