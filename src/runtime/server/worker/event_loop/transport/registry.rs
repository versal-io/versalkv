use crate::engine::command::context::Token;
use crate::foundation::io::transport::TransportId;
use std::collections::HashMap;

pub(crate) struct TransportRegistry {
    next_token: usize,
    transport_to_token: HashMap<TransportId, Token>,
    token_to_transport: HashMap<Token, TransportId>,
}

impl TransportRegistry {
    pub(crate) fn new() -> Self {
        Self {
            next_token: 0,
            transport_to_token: HashMap::new(),
            token_to_transport: HashMap::new(),
        }
    }

    pub(crate) fn register(&mut self, transport: TransportId) -> Token {
        self.next_token = self
            .next_token
            .checked_add(1)
            .expect("runtime token space exhausted");
        let token = Token(self.next_token);
        if let Some(previous) = self.transport_to_token.insert(transport, token) {
            self.token_to_transport.remove(&previous);
        }
        self.token_to_transport.insert(token, transport);
        token
    }

    pub(crate) fn runtime_token(&self, transport: TransportId) -> Option<Token> {
        self.transport_to_token.get(&transport).copied()
    }

    pub(crate) fn transport_id(&self, token: Token) -> Option<TransportId> {
        self.token_to_transport.get(&token).copied()
    }

    pub(crate) fn remove(&mut self, transport: TransportId) -> Option<Token> {
        let token = self.transport_to_token.remove(&transport)?;
        self.token_to_transport.remove(&token);
        Some(token)
    }
}

#[cfg(test)]
mod tests {
    use super::TransportRegistry;
    use crate::foundation::io::transport::TransportId;

    #[test]
    fn removing_a_transport_prevents_stale_event_routing_after_reuse() {
        let mut registry = TransportRegistry::new();
        let first = TransportId::new(1).unwrap();
        let first_token = registry.register(first);

        assert_eq!(registry.runtime_token(first), Some(first_token));
        assert_eq!(registry.transport_id(first_token), Some(first));

        assert_eq!(registry.remove(first), Some(first_token));
        assert_eq!(registry.runtime_token(first), None);

        let replacement = TransportId::new(2).unwrap();
        let replacement_token = registry.register(replacement);
        assert_ne!(first_token, replacement_token);
        assert_eq!(registry.runtime_token(replacement), Some(replacement_token));
        assert_eq!(registry.transport_id(replacement_token), Some(replacement));
    }

    #[test]
    fn reregistering_a_transport_replaces_its_previous_runtime_token() {
        let mut registry = TransportRegistry::new();
        let transport = TransportId::new(1).unwrap();
        let first = registry.register(transport);
        let second = registry.register(transport);

        assert_ne!(first, second);
        assert_eq!(registry.runtime_token(transport), Some(second));
        assert_eq!(registry.transport_id(first), None);
        assert_eq!(registry.transport_id(second), Some(transport));
    }
}
