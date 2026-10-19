//! Durable-persistence bookkeeping for the embedded (browser) runtime.
//!
//! The embedded runtime drives commands synchronously but persistence is an
//! asynchronous host capability: an append is handed to the storage host and
//! the client reply is withheld until the host acknowledges durability. This
//! module owns the continuations for those in-flight replies.
//!
//! It depends only on host-neutral runtime/driver types, the host-neutral AOF
//! I/O protocol, and `Value`, so it compiles on the `wasm` target where the
//! native AOF file stack is absent.

use bytes::Bytes;

use crate::engine::command::SessionId;
use crate::engine::core::Value;
use crate::foundation::io::persistence::{encode_aof_io_request, AofIoRequest};
use crate::runtime::driver::{
    ContinuationRegistry, HostOperationId, HostOperationResult, OwnedHostCompletion,
    OwnedHostRequest, WaitCondition,
};

use super::output::EmbeddedOutput;

/// Host service name for durable AOF operations (design §6.2).
const AOF_SERVICE: &[u8] = b"versal.aof";
/// Host operation name within the AOF service.
const AOF_OPERATION: &[u8] = b"request";

/// A client reply held back until its durable append is acknowledged.
pub(super) struct PendingDurableReply {
    pub(super) request_id: u32,
    pub(super) value: Value,
}

/// A durable host request plus the operation id that correlates its completion.
pub(super) struct DurableAppend {
    pub(super) operation: HostOperationId,
    pub(super) request: OwnedHostRequest,
}

/// Embedded-runtime persistence state: the registry of client replies waiting
/// on durable host completions, plus the sequence/operation-id allocators.
#[derive(Default)]
pub(super) struct EmbeddedPersistenceState {
    pending_replies: ContinuationRegistry<PendingDurableReply>,
    next_operation_id: u64,
    next_sequence: u64,
}

impl EmbeddedPersistenceState {
    /// Build a durable `Append` request for the given AOF frames and withhold
    /// the client reply until the host acknowledges it. Returns the host
    /// request to emit alongside its correlation id.
    pub(super) fn enqueue_append(
        &mut self,
        session: SessionId,
        request_id: u32,
        value: Value,
        frame: Bytes,
    ) -> DurableAppend {
        let operation = self.allocate_operation();
        let sequence = self.allocate_sequence();
        self.pending_replies.register(
            session,
            WaitCondition::HostOperation(operation),
            PendingDurableReply { request_id, value },
        );
        let payload = encode_aof_io_request(&AofIoRequest::Append {
            sequence,
            bytes: frame,
        });
        DurableAppend {
            operation,
            request: OwnedHostRequest::Call {
                service: Bytes::from_static(AOF_SERVICE),
                operation: Bytes::from_static(AOF_OPERATION),
                payload: Bytes::from(payload),
            },
        }
    }

    fn allocate_operation(&mut self) -> HostOperationId {
        self.next_operation_id = self.next_operation_id.saturating_add(1).max(1);
        HostOperationId::from_u64(self.next_operation_id)
    }

    fn allocate_sequence(&mut self) -> u64 {
        self.next_sequence = self.next_sequence.saturating_add(1).max(1);
        self.next_sequence
    }

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
    use crate::runtime::driver::HostOperationId;

    fn completion(id: u64, result: HostOperationResult) -> OwnedHostCompletion {
        OwnedHostCompletion {
            operation: HostOperationId::from_u64(id),
            result,
        }
    }

    fn expect_reply(output: EmbeddedOutput) -> (u32, Value) {
        match output {
            EmbeddedOutput::Reply { request_id, value } => (request_id, value),
            EmbeddedOutput::HostRequest { .. } => panic!("expected a reply, found a host request"),
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
    fn enqueue_append_builds_a_call_and_withholds_the_reply() {
        let mut state = EmbeddedPersistenceState::default();
        let append = state.enqueue_append(
            SessionId(1),
            42,
            Value::ok(),
            Bytes::from_static(b"*1\r\n$4\r\nPING\r\n"),
        );
        let OwnedHostRequest::Call {
            service,
            operation,
            payload,
        } = &append.request
        else {
            panic!("expected a Call host request");
        };
        assert_eq!(service.as_ref(), AOF_SERVICE);
        assert_eq!(operation.as_ref(), AOF_OPERATION);
        let decoded = crate::foundation::io::persistence::decode_aof_io_request(payload)
            .expect("payload decodes");
        assert_eq!(
            decoded,
            AofIoRequest::Append {
                sequence: 1,
                bytes: Bytes::from_static(b"*1\r\n$4\r\nPING\r\n"),
            }
        );

        // The reply is released only when the matching completion arrives.
        let out = state
            .complete(&OwnedHostCompletion {
                operation: append.operation,
                result: HostOperationResult::Ack,
            })
            .expect("matching completion emits a reply");
        let (request_id, value) = expect_reply(out);
        assert_eq!(request_id, 42);
        assert_eq!(value, Value::ok());
    }

    #[test]
    fn operation_and_sequence_ids_increase_monotonically() {
        let mut state = EmbeddedPersistenceState::default();
        let first = state.enqueue_append(SessionId(1), 1, Value::ok(), Bytes::from_static(b"a"));
        let second = state.enqueue_append(SessionId(1), 2, Value::ok(), Bytes::from_static(b"b"));
        assert_ne!(first.operation, second.operation);
        let decode = |request: &OwnedHostRequest| {
            let OwnedHostRequest::Call { payload, .. } = request else {
                panic!("expected a Call host request");
            };
            crate::foundation::io::persistence::decode_aof_io_request(payload).expect("decodes")
        };
        let AofIoRequest::Append {
            sequence: first_seq,
            ..
        } = decode(&first.request)
        else {
            panic!("expected Append");
        };
        let AofIoRequest::Append {
            sequence: second_seq,
            ..
        } = decode(&second.request)
        else {
            panic!("expected Append");
        };
        assert_eq!(first_seq, 1);
        assert_eq!(second_seq, 2);
    }

    #[test]
    fn ack_emits_the_withheld_reply_once() {
        let mut state = EmbeddedPersistenceState::default();
        let append =
            state.enqueue_append(SessionId(1), 42, Value::ok(), Bytes::from_static(b"frame"));

        let out = state
            .complete(&OwnedHostCompletion {
                operation: append.operation,
                result: HostOperationResult::Ack,
            })
            .expect("matching completion emits a reply");
        let (request_id, value) = expect_reply(out);
        assert_eq!(request_id, 42);
        assert_eq!(value, Value::ok());

        // A duplicate completion no longer matches.
        assert!(state
            .complete(&OwnedHostCompletion {
                operation: append.operation,
                result: HostOperationResult::Ack,
            })
            .is_none());
    }

    #[test]
    fn error_completion_maps_to_misconf_reply() {
        let mut state = EmbeddedPersistenceState::default();
        let append =
            state.enqueue_append(SessionId(1), 3, Value::ok(), Bytes::from_static(b"frame"));
        let out = state
            .complete(&OwnedHostCompletion {
                operation: append.operation,
                result: HostOperationResult::Error("disk full".to_string()),
            })
            .expect("error completion still emits a reply");
        let (request_id, value) = expect_reply(out);
        assert_eq!(request_id, 3);
        assert_eq!(
            value,
            Value::error("MISCONF AOF durable write failed: disk full")
        );
    }
}
