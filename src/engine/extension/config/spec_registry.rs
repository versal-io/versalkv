use crate::engine::extension::config::spec::{ConfigSpec, ConfigType, ConfigValue};
use crate::engine::extension::config::RuntimeConfig;
use std::collections::HashMap;

fn parse_mem_unit(s: &str) -> Option<i64> {
    let s = s.trim().to_ascii_lowercase();
    if s.is_empty() {
        return None;
    }
    let mut num_str = String::new();
    let mut unit = String::new();
    for ch in s.chars() {
        if ch.is_ascii_digit() || ch == '-' {
            if unit.is_empty() {
                num_str.push(ch);
            } else {
                return None;
            }
        } else {
            unit.push(ch);
        }
    }
    let n: i64 = num_str.parse().ok()?;
    let mult: i64 = match unit.as_str() {
        "" | "b" => 1,
        "k" | "kb" => 1024,
        "m" | "mb" => 1024 * 1024,
        "g" | "gb" => 1024 * 1024 * 1024,
        _ => return None,
    };
    n.checked_mul(mult)
}

pub struct ConfigRegistry {
    specs: HashMap<&'static str, &'static ConfigSpec>,
    aliases: HashMap<&'static str, &'static str>, // alias → canonical name
}

impl ConfigRegistry {
    pub fn new() -> Self {
        let mut reg = Self {
            specs: HashMap::new(),
            aliases: HashMap::new(),
        };
        crate::engine::extension::config::items::register_all_configs(&mut reg);
        reg
    }

    pub(crate) fn register(&mut self, spec: &'static ConfigSpec) {
        let name = spec.name;
        assert!(
            !self.specs.contains_key(name) && !self.aliases.contains_key(name),
            "duplicate config name: {name}"
        );
        if let Some(alias) = spec.alias {
            assert!(
                alias != name
                    && !self.aliases.contains_key(alias)
                    && !self.specs.contains_key(alias),
                "duplicate config alias: {alias}"
            );
        }
        self.specs.insert(name, spec);
        if let Some(alias) = spec.alias {
            self.aliases.insert(alias, name);
        }
    }

    pub fn lookup(&self, name: &str) -> Option<&'static ConfigSpec> {
        if let Some(spec) = self.specs.get(name) {
            return Some(*spec);
        }
        if let Some(canonical) = self.aliases.get(name) {
            if let Some(spec) = self.specs.get(canonical) {
                return Some(*spec);
            }
        }
        None
    }

    pub fn all_specs(&self) -> Vec<&'static ConfigSpec> {
        let mut specs: Vec<_> = self.specs.values().copied().collect();
        specs.sort_by_key(|s| s.name);
        specs
    }

    pub fn try_set(
        &self,
        rcfg: &mut RuntimeConfig,
        name: &str,
        value: &str,
    ) -> Option<Result<(), String>> {
        let spec = self.lookup(name)?;
        // "CONFIG SET failed (possibly related to argument '%s') - can't set immutable config"
        if spec.is_immutable() {
            return Some(Err(format!(
                "ERR CONFIG SET failed (possibly related to argument '{}') - can't set immutable config",
                name
            )));
        }
        self.set_from_str(rcfg, spec, value)
    }

    pub fn try_set_startup(
        &self,
        rcfg: &mut RuntimeConfig,
        name: &str,
        value: &str,
    ) -> Option<Result<(), String>> {
        let spec = self.lookup(name)?;
        self.set_from_str(rcfg, spec, value)
    }

    fn set_from_str(
        &self,
        rcfg: &mut RuntimeConfig,
        spec: &'static ConfigSpec,
        value: &str,
    ) -> Option<Result<(), String>> {
        let cv = match spec.type_ {
            ConfigType::Bool => {
                let v = if value.eq_ignore_ascii_case("yes")
                    || value == "1"
                    || value.eq_ignore_ascii_case("true")
                {
                    true
                } else if value.eq_ignore_ascii_case("no")
                    || value == "0"
                    || value.eq_ignore_ascii_case("false")
                {
                    false
                } else {
                    return Some(Err(format!(
                        "ERR argument {} must be 'yes' or 'no'",
                        spec.name
                    )));
                };
                ConfigValue::Bool(v)
            }
            ConfigType::Numeric => {
                let parsed = value.parse::<i64>().ok().or_else(|| {
                    spec.accepts_memory_units()
                        .then(|| parse_mem_unit(value))
                        .flatten()
                });
                match parsed {
                    Some(n) => {
                        if n < spec.numeric_min || n > spec.numeric_max {
                            return Some(Err(format!(
                                "ERR value {} is out of range, must be between {} and {}",
                                n, spec.numeric_min, spec.numeric_max
                            )));
                        }
                        ConfigValue::Numeric(n)
                    }
                    None => {
                        return Some(Err(format!("ERR argument {} must be numeric", spec.name)));
                    }
                }
            }
            ConfigType::String => ConfigValue::String(value.to_string()),
            ConfigType::Enum => {
                if !spec
                    .enum_values
                    .iter()
                    .any(|allowed| allowed.eq_ignore_ascii_case(value))
                {
                    return Some(Err(format!(
                        "ERR argument {} must be one of: {}",
                        spec.name,
                        spec.enum_values.join(", ")
                    )));
                }
                ConfigValue::String(value.to_string())
            }
            ConfigType::Special => return None,
        };
        Some((spec.set)(rcfg, &cv))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static FIRST: ConfigSpec = ConfigSpec {
        name: "first",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: Some("legacy-first"),
        get: |_| ConfigValue::String(String::new()),
        set: |_, _| Ok(()),
    };
    static DUPLICATE_NAME: ConfigSpec = ConfigSpec {
        name: "first",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |_| ConfigValue::String(String::new()),
        set: |_, _| Ok(()),
    };
    static DUPLICATE_ALIAS: ConfigSpec = ConfigSpec {
        name: "second",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: Some("legacy-first"),
        get: |_| ConfigValue::String(String::new()),
        set: |_, _| Ok(()),
    };
    static ALIAS_COLLIDES_WITH_NAME: ConfigSpec = ConfigSpec {
        name: "second",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: Some("first"),
        get: |_| ConfigValue::String(String::new()),
        set: |_, _| Ok(()),
    };
    static NAME_COLLIDES_WITH_ALIAS: ConfigSpec = ConfigSpec {
        name: "legacy-first",
        type_: ConfigType::String,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: 0,
        flags: &[],
        alias: None,
        get: |_| ConfigValue::String(String::new()),
        set: |_, _| Ok(()),
    };

    fn empty_registry() -> ConfigRegistry {
        ConfigRegistry {
            specs: HashMap::new(),
            aliases: HashMap::new(),
        }
    }

    #[test]
    #[should_panic(expected = "duplicate config name")]
    fn duplicate_canonical_name_is_a_programmer_error() {
        let mut registry = empty_registry();
        registry.register(&FIRST);
        registry.register(&DUPLICATE_NAME);
    }

    #[test]
    #[should_panic(expected = "duplicate config alias")]
    fn duplicate_alias_is_a_programmer_error() {
        let mut registry = empty_registry();
        registry.register(&FIRST);
        registry.register(&DUPLICATE_ALIAS);
    }

    #[test]
    #[should_panic(expected = "duplicate config alias")]
    fn alias_cannot_shadow_a_canonical_name() {
        let mut registry = empty_registry();
        registry.register(&FIRST);
        registry.register(&ALIAS_COLLIDES_WITH_NAME);
    }

    #[test]
    #[should_panic(expected = "duplicate config name")]
    fn canonical_name_cannot_shadow_an_alias() {
        let mut registry = empty_registry();
        registry.register(&FIRST);
        registry.register(&NAME_COLLIDES_WITH_ALIAS);
    }
}
