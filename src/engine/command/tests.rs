use super::*;
use crate::foundation::io::protocol::Value;
use bytes::Bytes;

fn args(parts: &[&'static [u8]]) -> CommandArgs {
    CommandArgs::from_parts(parts.iter().map(|p| Bytes::from_static(p)).collect())
}

fn error_text(value: Value) -> String {
    match value {
        Value::Error(bytes) => String::from_utf8_lossy(&bytes).to_string(),
        other => panic!("expected error, got {other:?}"),
    }
}

#[test]
fn command_args_split_name_and_arguments() {
    let parsed = args(&[b"SET", b"k", b"v"]);
    assert_eq!(parsed.name, Bytes::from_static(b"SET"));
    assert_eq!(
        parsed.args,
        vec![Bytes::from_static(b"k"), Bytes::from_static(b"v")]
    );
    assert_eq!(parsed.len(), 2);

    let empty = CommandArgs::from_parts(Vec::new());
    assert_eq!(empty.name, Bytes::from_static(b""));
    assert!(empty.args.is_empty());
}

#[test]
fn command_args_reusing_constructor_keeps_vec_allocation() {
    let mut parts = Vec::with_capacity(8);
    parts.extend([
        Bytes::from_static(b"SET"),
        Bytes::from_static(b"key"),
        Bytes::from_static(b"value"),
    ]);
    let allocation = parts.as_ptr();
    let capacity = parts.capacity();

    let parsed = CommandArgs::from_parts_reusing(parts);

    assert_eq!(parsed.name, Bytes::from_static(b"SET"));
    assert_eq!(
        parsed.args,
        [Bytes::from_static(b"key"), Bytes::from_static(b"value")]
    );
    assert_eq!(parsed.args.as_ptr(), allocation);
    assert_eq!(parsed.args.capacity(), capacity);
}

#[test]
fn command_args_round_trip_reuses_vec_allocation() {
    let mut parts = Vec::with_capacity(8);
    parts.extend([
        Bytes::from_static(b"SET"),
        Bytes::from_static(b"key"),
        Bytes::from_static(b"value"),
    ]);
    let allocation = parts.as_ptr();

    let parsed = CommandArgs::from_parts_reusing(parts);
    let returned = parsed.into_parts_reusing();

    assert_eq!(returned.as_ptr(), allocation);
    assert_eq!(
        returned,
        [
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"key"),
            Bytes::from_static(b"value")
        ]
    );
}

#[test]
fn parse_integer_positive_and_timeout_paths() {
    let parsed = args(&[b"CMD", b"42", b"0.25", b"-1", b"nan"]);

    assert_eq!(parsed.parse::<i64>(0).unwrap(), 42);
    assert_eq!(parsed.parse_positive::<u32>(0, "numkeys").unwrap(), 42);
    assert_eq!(parsed.parse_timeout_ms(1).unwrap(), 250);
    assert!(
        error_text(parsed.parse_positive::<i64>(2, "numkeys").unwrap_err())
            .contains("greater than 0")
    );
    assert!(error_text(parsed.parse::<i64>(3).unwrap_err()).contains("integer"));
    assert!(error_text(parsed.parse_timeout_ms(2).unwrap_err()).contains("float"));
    let too_large = args(&[b"CMD", b"2147484"]);
    assert!(error_text(too_large.parse_timeout_ms(0).unwrap_err()).contains("out of range"));
}

#[test]
fn parse_rejects_non_utf8_arguments() {
    let parsed = CommandArgs::from_parts(vec![
        Bytes::from_static(b"CMD"),
        Bytes::from(vec![0xff, 0xfe]),
    ]);

    assert!(error_text(parsed.parse::<i64>(0).unwrap_err()).contains("integer"));
    assert!(error_text(parsed.parse_timeout_ms(0).unwrap_err()).contains("float"));
}

#[test]
fn command_errors_match_resp_error_contracts() {
    assert!(error_text(CommandError::wrong_arity("get")).contains("'get' command"));
    assert!(error_text(CommandError::unknown(b"NOPE")).contains("NOPE"));
    assert_eq!(error_text(CommandError::syntax_error()), "ERR syntax error");
    assert_eq!(
        error_text(CommandError::db_index_out_of_range()),
        "ERR DB index is out of range"
    );
    assert_eq!(
        error_text(CommandError::noauth()),
        "NOAUTH Authentication required."
    );
    assert!(error_text(CommandError::wrong_type()).starts_with("WRONGTYPE"));
    assert!(error_text(CommandError::no_password_set()).contains("without any password"));
    assert!(error_text(CommandError::wrongpass()).starts_with("WRONGPASS"));
}

#[test]
fn dispatch_groups_are_stable_copyable_values() {
    let groups = [
        DispatchGroup::Default,
        DispatchGroup::Transport,
        DispatchGroup::PubSubControl,
        DispatchGroup::Transaction,
        DispatchGroup::ClientAdmin,
        DispatchGroup::Auth,
        DispatchGroup::Shutdown,
    ];

    assert_eq!(groups[0], DispatchGroup::Default);
    assert_eq!(groups[5] as u8, DispatchGroup::Auth as u8);
    assert_eq!(groups[6] as u8, DispatchGroup::Shutdown as u8);
}

#[test]
fn command_descriptor_carries_handler_independent_metadata() {
    let descriptor = CommandDescriptor {
        name: super::CommandName::Static("set"),
        arity: -3,
        flags: CMD_WRITE | CMD_DENYOOM,
        first_key: 1,
        last_key: 1,
        key_step: 1,
        container: None,
        acl_categories: &["STRING", "WRITE"],
    };

    assert_eq!(descriptor.name.as_str(), "set");
    assert_eq!(descriptor.arity, -3);
    assert_eq!(descriptor.flags, CMD_WRITE | CMD_DENYOOM);
    assert_eq!((descriptor.first_key, descriptor.last_key), (1, 1));
    assert_eq!(descriptor.acl_categories, &["STRING", "WRITE"]);
}
