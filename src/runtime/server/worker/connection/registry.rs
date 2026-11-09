use crate::engine::command::SessionId;
use crate::framework::capabilities::connection::Connection;
use crate::framework::capabilities::io::Token;
use slab::Slab;
use std::collections::HashMap;
use std::ops::{Index, IndexMut};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ConnectionRegistryError {
    TokenInUse(Token),
    ClientIdInUse(u64),
}

/// Owns the complete set of same-lifecycle indexes for ordinary connections.
/// Slab positions never leave this type; runtime callers address connections
/// only by transport token or stable public client id.
#[derive(Default)]
pub(crate) struct ConnectionRegistry {
    connections: Slab<Connection>,
    token_to_slab: HashMap<Token, usize>,
    client_id_to_token: HashMap<u64, Token>,
}

impl ConnectionRegistry {
    pub(crate) fn register(
        &mut self,
        connection: Connection,
    ) -> Result<(), ConnectionRegistryError> {
        let token = connection.token();
        let client_id = connection.client_id();
        if self.token_to_slab.contains_key(&token) {
            return Err(ConnectionRegistryError::TokenInUse(token));
        }
        if self.client_id_to_token.contains_key(&client_id) {
            return Err(ConnectionRegistryError::ClientIdInUse(client_id));
        }
        let slab_key = self.connections.insert(connection);
        self.token_to_slab.insert(token, slab_key);
        self.client_id_to_token.insert(client_id, token);
        Ok(())
    }

    pub(crate) fn remove(&mut self, token: Token) -> Option<Connection> {
        let slab_key = self.token_to_slab.remove(&token)?;
        let connection = self.connections.remove(slab_key);
        debug_assert_eq!(connection.token(), token);
        self.client_id_to_token.remove(&connection.client_id());
        Some(connection)
    }

    pub(crate) fn get(&self, token: Token) -> Option<&Connection> {
        let slab_key = *self.token_to_slab.get(&token)?;
        self.connections
            .get(slab_key)
            .filter(|connection| connection.token() == token)
    }

    pub(crate) fn get_mut(&mut self, token: Token) -> Option<&mut Connection> {
        let slab_key = *self.token_to_slab.get(&token)?;
        self.connections
            .get_mut(slab_key)
            .filter(|connection| connection.token() == token)
    }

    pub(crate) fn contains(&self, token: Token) -> bool {
        self.get(token).is_some()
    }

    pub(crate) fn token_for_client_id(&self, client_id: u64) -> Option<Token> {
        let token = *self.client_id_to_token.get(&client_id)?;
        self.get(token)
            .filter(|connection| connection.client_id() == client_id)
            .map(|_| token)
    }

    pub(crate) fn token_for_session(&self, session: SessionId) -> Option<Token> {
        self.token_for_client_id(session.as_u64())
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = (Token, &Connection)> {
        self.connections
            .iter()
            .map(|(_, connection)| (connection.token(), connection))
    }

    pub(crate) fn iter_mut(&mut self) -> impl Iterator<Item = (Token, &mut Connection)> {
        self.connections
            .iter_mut()
            .map(|(_, connection)| (connection.token(), connection))
    }

    pub(crate) fn len(&self) -> usize {
        self.connections.len()
    }

    #[cfg(test)]
    pub(crate) fn register_for_test(&mut self, mut connection: Connection) -> Token {
        let token = connection.token();
        if self
            .client_id_to_token
            .contains_key(&connection.client_id())
        {
            connection.set_client_id_for_test(u64::MAX.saturating_sub(token.0 as u64));
        }
        self.register(connection)
            .expect("test connection identities must be unique");
        token
    }

    #[cfg(test)]
    pub(crate) fn insert(&mut self, connection: Connection) -> usize {
        self.register_for_test(connection).0
    }

    #[cfg(test)]
    pub(crate) fn replace_token_for_test(&mut self, old: usize, new: Token) -> usize {
        let mut connection = self.remove(Token(old)).expect("test connection must exist");
        connection.set_token_for_test(new);
        self.register_for_test(connection).0
    }
}

impl Index<Token> for ConnectionRegistry {
    type Output = Connection;

    fn index(&self, token: Token) -> &Self::Output {
        self.get(token)
            .expect("connection token must be registered")
    }
}

impl IndexMut<Token> for ConnectionRegistry {
    fn index_mut(&mut self, token: Token) -> &mut Self::Output {
        self.get_mut(token)
            .expect("connection token must be registered")
    }
}
