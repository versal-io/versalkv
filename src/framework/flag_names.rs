#[allow(dead_code)]
pub(crate) fn flag_to_const(flag: &str) -> Option<&'static str> {
    match flag {
        "WRITE" => Some("CMD_WRITE"),
        "DENYOOM" => Some("CMD_DENYOOM"),
        "FAST" => Some("CMD_FAST"),
        "READONLY" => Some("CMD_READONLY"),
        "ADMIN" => Some("CMD_ADMIN"),
        "LOADING" => Some("CMD_LOADING"),
        "STALE" => Some("CMD_STALE"),
        "NOSCRIPT" => Some("CMD_NOSCRIPT"),
        "NO_AUTH" => Some("CMD_NO_AUTH"),
        "SKIP_MONITOR" => Some("CMD_SKIP_MONITOR"),
        "SKIP_SLOWLOG" => Some("CMD_SKIP_SLOWLOG"),
        "SKIP_COMMANDLOG" => Some("CMD_SKIP_COMMANDLOG"),
        "NO_ASYNC_LOADING" => Some("CMD_NO_ASYNC_LOADING"),
        "PUBSUB" => Some("CMD_PUBSUB"),
        "SLOW" => Some("CMD_SLOW"),
        "MOVABLE_KEYS" => Some("CMD_MOVABLE_KEYS"),
        "BLOCKING" => Some("CMD_BLOCKING"),
        "NO_MULTI" => Some("CMD_NO_MULTI"),
        "ALLOW_BUSY" => Some("CMD_ALLOW_BUSY"),
        "ALL_DBS" => Some("CMD_ALL_DBS"),
        "ASKING" => Some("CMD_ASKING"),
        "MAY_REPLICATE" => Some("CMD_MAY_REPLICATE"),
        "NO_MANDATORY_KEYS" => Some("CMD_NO_MANDATORY_KEYS"),
        "ONLY_SENTINEL" => Some("CMD_ONLY_SENTINEL"),
        "PROTECTED" => Some("CMD_PROTECTED"),
        "SENTINEL" => Some("CMD_SENTINEL"),
        "TOUCHES_ARBITRARY_KEYS" => Some("CMD_TOUCHES_ARBITRARY_KEYS"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::flag_to_const;

    #[test]
    fn maps_skip_flags_to_distinct_internal_bits() {
        assert_eq!(flag_to_const("SKIP_MONITOR"), Some("CMD_SKIP_MONITOR"));
        assert_eq!(flag_to_const("SKIP_SLOWLOG"), Some("CMD_SKIP_SLOWLOG"));
        assert_eq!(
            flag_to_const("SKIP_COMMANDLOG"),
            Some("CMD_SKIP_COMMANDLOG")
        );
        assert_eq!(flag_to_const("NOT_A_FLAG"), None);
    }

    #[test]
    fn deprecated_random_flag_is_not_mapped() {
        assert_eq!(flag_to_const("RANDOM"), None);
    }
}
