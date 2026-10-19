//! Durable-persistence bookkeeping for the embedded (browser) runtime.
//!
//! The embedded runtime drives commands synchronously but persistence is an
//! asynchronous host capability: an append is handed to the storage host and
//! the client reply is withheld until the host acknowledges durability. This
//! module owns the continuations for those in-flight replies.
//!
//! It depends only on host-neutral runtime/driver types and `Value`, so it
//! compiles on the `wasm` target where the native AOF file stack is absent.
//!
//! Phase 2 lands the completion-routing core. The append side that registers
//! continuations and allocates operation ids is wired in a later phase.

use crate::engine::core::Value;
use crate::runtime::driver::{ContinuationRegistry, HostOperationResult, OwnedHostCompletion};

use super::output::EmbeddedOutput;

/// A client reply held back until its durable append is acknowledged.
pub(super) struct PendingDurableReply {
    pub(super) request_id: u32,
    pub(super) value: Value,
}

/// Embedded-runtime persistence state: the registry of client replies waiting
/// on durable host completions.
#[derive(Default)]
pub(super) struct EmbeddedPersistenceState {
    pending_replies: ContinuationRegistry<PendingDurableReply>,
}

impl EmbeddedPersistenceState {
    /// Complete a durable operation, returning the reply to emit if a matching
    /// continuation was waiting. An unmatched completion yields `None` so the
    /// caller can treat it as an unexpected host message.
    pub(super) fn complete(&mut self, completion: &OwnedHostCompletion) -> Option<EmbeddedOutput> {
        let suspended = self
            .pending_replies
            .complete_host_operation(completion.operation)?;
        let reply = match &completion.result {
            HostOperationResult::Data(_) | HostOperationResult::Ack => suspended.state.value,
            HostOperationResult::Error(message) => {
                Value::error(format!("MISCONF AOF durable write failed: {message}"))
            }
        };
        Some(EmbeddedOutput::Reply {
            request_id: suspended.state.request_id,
            value: reply,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::command::SessionId;
    use crate::runtime::driver::{HostOperationId, WaitCondition};

    fn completion(id: u64, result: HostOperationResult) -> OwnedHostCompletion {
        OwnedHostCompletion {
            operation: HostOperationId::from_u64(id),
            result,
        }
    }

    #[test]
    fn unmatched_completion_returns_none() {
        let mut state = EmbeddedPersistenceState::default();
        assert!(state
            .complete(&completion(1, HostOperationResult::Ack))
            .is_none());
    }

    #[test]
    fn ack_emits_the_withheld_reply_once() {
        let mut state = EmbeddedPersistenceState::default();
        let operation = HostOperationId::from_u64(7);
        state.pending_replies.register(
            SessionId(1),
            WaitCondition::HostOperation(operation),
            PendingDurableReply {
                request_id: 42,
                value: Value::ok(),
            },
        );

        let out = state
            .complete(&completion(7, HostOperationResult::Ack))
            .expect("matching completion emits a reply");
        let EmbeddedOutput::Reply { request_id, value } = out;
        assert_eq!(request_id, 42);
        assert_eq!(value, Value::ok());

        // A duplicate completion no longer matches.
        assert!(state
            .complete(&completion(7, HostOperationResult::Ack))
            .is_none());
    }

    #[test]
    fn error_completion_maps_to_misconf_reply() {
        let mut state = EmbeddedPersistenceState::default();
        let operation = HostOperationId::from_u64(9);
        state.pending_replies.register(
            SessionId(1),
            WaitCondition::HostOperation(operation),
            PendingDurableReply {
                request_id: 3,
                value: Value::ok(),
            },
        );
        let out = state
            .complete(&completion(
                9,
                HostOperationResult::Error("disk full".to_string()),
            ))
            .expect("error completion still emits a reply");
        let EmbeddedOutput::Reply { request_id, value } = out;
        assert_eq!(request_id, 3);
        assert_eq!(
            value,
            Value::error("MISCONF AOF durable write failed: disk full")
        );
    }
}
