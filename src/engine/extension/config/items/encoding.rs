use crate::engine::extension::config::spec::{ConfigSpec, ConfigType, ConfigValue};
use crate::engine::extension::config::spec_registry::ConfigRegistry;

pub(crate) fn register_encoding_configs(reg: &mut ConfigRegistry) {
    reg.register(&ConfigSpec {
        name: "zset-max-listpack-entries",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &[],
        alias: Some("zset-max-ziplist-entries"),
        get: |rcfg| ConfigValue::Numeric(rcfg.encoding.zset_max_ziplist_entries),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(v) = val {
                rcfg.encoding.zset_max_ziplist_entries = *v;
                Ok(())
            } else {
                Err("zset-max-listpack-entries must be numeric".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "zset-max-listpack-value",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &[],
        alias: Some("zset-max-ziplist-value"),
        get: |rcfg| ConfigValue::Numeric(rcfg.encoding.zset_max_ziplist_value),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(v) = val {
                rcfg.encoding.zset_max_ziplist_value = *v;
                Ok(())
            } else {
                Err("zset-max-listpack-value must be numeric".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "list-max-listpack-size",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: i64::MIN,
        numeric_max: i64::MAX,
        flags: &[],
        alias: Some("list-max-ziplist-size"),
        get: |rcfg| ConfigValue::Numeric(rcfg.encoding.list_max_listpack_size),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(v) = val {
                rcfg.encoding.list_max_listpack_size = *v;
                Ok(())
            } else {
                Err("list-max-listpack-size must be numeric".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "list-compress-depth",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.encoding.list_compress_depth),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(v) = val {
                rcfg.encoding.list_compress_depth = *v;
                Ok(())
            } else {
                Err("list-compress-depth must be numeric".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "hash-max-listpack-entries",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &[],
        alias: Some("hash-max-ziplist-entries"),
        get: |rcfg| ConfigValue::Numeric(rcfg.encoding.hash_max_listpack_entries),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(v) = val {
                rcfg.encoding.hash_max_listpack_entries = *v;
                Ok(())
            } else {
                Err("hash-max-listpack-entries must be numeric".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "hash-max-listpack-value",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &[],
        alias: Some("hash-max-ziplist-value"),
        get: |rcfg| ConfigValue::Numeric(rcfg.encoding.hash_max_listpack_value),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(v) = val {
                rcfg.encoding.hash_max_listpack_value = *v;
                Ok(())
            } else {
                Err("hash-max-listpack-value must be numeric".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "set-max-intset-entries",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.encoding.set_max_intset_entries),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(v) = val {
                rcfg.encoding.set_max_intset_entries = *v;
                Ok(())
            } else {
                Err("set-max-intset-entries must be numeric".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "set-max-listpack-entries",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.encoding.set_max_listpack_entries),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(v) = val {
                rcfg.encoding.set_max_listpack_entries = *v;
                Ok(())
            } else {
                Err("set-max-listpack-entries must be numeric".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "set-max-listpack-value",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.encoding.set_max_listpack_value),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(v) = val {
                rcfg.encoding.set_max_listpack_value = *v;
                Ok(())
            } else {
                Err("set-max-listpack-value must be numeric".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "stream-node-max-entries",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.encoding.stream_node_max_entries),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(v) = val {
                rcfg.encoding.stream_node_max_entries = *v;
                Ok(())
            } else {
                Err("stream-node-max-entries must be numeric".to_string())
            }
        },
    });
    reg.register(&ConfigSpec {
        name: "stream-node-max-bytes",
        type_: ConfigType::Numeric,
        enum_values: &[],
        numeric_min: 0,
        numeric_max: i64::MAX,
        flags: &[],
        alias: None,
        get: |rcfg| ConfigValue::Numeric(rcfg.encoding.stream_node_max_bytes),
        set: |rcfg, val| {
            if let ConfigValue::Numeric(v) = val {
                rcfg.encoding.stream_node_max_bytes = *v;
                Ok(())
            } else {
                Err("stream-node-max-bytes must be numeric".to_string())
            }
        },
    });
}
