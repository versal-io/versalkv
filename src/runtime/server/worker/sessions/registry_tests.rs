use super::ConnectionRegistry;
use crate::engine::command::SessionId;
use crate::framework::capabilities::connection::Connection;
use crate::framework::capabilities::io::Token;
use std::net::SocketAddr;

fn connection(token: usize, client_id: u64) -> Connection {
    Connection::new_with_client_id(
        Token(token),
        client_id,
        SocketAddr::from(([127, 0, 0, 1], 6379)),
        false,
    )
}

#[test]
fn registration_and_removal_update_every_identity_atomically() {
    let mut registry = ConnectionRegistry::default();
    registry.register(connection(7, 41)).unwrap();

    assert_eq!(
        registry.get(Token(7)).map(|conn| conn.client_id()),
        Some(41)
    );
    assert_eq!(registry.token_for_client_id(41), Some(Token(7)));
    assert_eq!(
        registry.token_for_session(SessionId::from_u64(41)),
        Some(Token(7))
    );

    let removed = registry.remove(Token(7)).expect("registered connection");
    assert_eq!(removed.client_id(), 41);
    assert!(registry.get(Token(7)).is_none());
    assert_eq!(registry.token_for_client_id(41), None);
}

#[test]
fn reused_token_never_resolves_a_stale_client_identity() {
    let mut registry = ConnectionRegistry::default();
    registry.register(connection(7, 41)).unwrap();
    registry.remove(Token(7)).unwrap();
    registry.register(connection(7, 42)).unwrap();

    assert_eq!(registry.token_for_client_id(41), None);
    assert_eq!(registry.token_for_client_id(42), Some(Token(7)));
    assert_eq!(registry.token_for_session(SessionId::from_u64(41)), None);
    assert_eq!(
        registry.token_for_session(SessionId::from_u64(42)),
        Some(Token(7))
    );
    assert_eq!(
        registry.get(Token(7)).map(|conn| conn.client_id()),
        Some(42)
    );
}

#[test]
fn conflicting_registration_leaves_existing_indexes_unchanged() {
    let mut registry = ConnectionRegistry::default();
    registry.register(connection(7, 41)).unwrap();

    let duplicate_token = registry.register(connection(7, 42));
    let duplicate_client = registry.register(connection(8, 41));

    assert!(duplicate_token.is_err());
    assert!(duplicate_client.is_err());
    assert_eq!(
        registry.get(Token(7)).map(|conn| conn.client_id()),
        Some(41)
    );
    assert!(registry.get(Token(8)).is_none());
    assert_eq!(registry.token_for_client_id(41), Some(Token(7)));
    assert_eq!(registry.token_for_client_id(42), None);
}

#[test]
fn iteration_exposes_typed_read_only_identities() {
    let mut registry = ConnectionRegistry::default();
    registry.register(connection(7, 41)).unwrap();

    let identities: Vec<(Token, Token, u64)> = registry
        .iter()
        .map(|(token, connection)| (token, connection.token(), connection.client_id()))
        .collect();

    assert_eq!(identities, vec![(Token(7), Token(7), 41)]);
}
