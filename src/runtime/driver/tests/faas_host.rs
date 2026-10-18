use std::collections::VecDeque;
use std::convert::Infallible;

use crate::engine::command::CommandArgs;
use crate::engine::command::SessionId;
use crate::engine::core::Value;
use crate::runtime::driver::{
    ContinuationRegistry, HostOperationId, HostOperationResult, OwnedHostCompletion,
    OwnedHostRequest, RuntimeClock, RuntimeDispatch, RuntimeDriver, RuntimeInput, RuntimeInstant,
    RuntimeKernel, RuntimeMachine, RuntimeProgress, RuntimeWait, WaitCondition,
};
use crate::runtime::embedded::{EmbeddedEvent, EmbeddedOutput, EmbeddedRuntime};
use bytes::Bytes;

enum Invocation {
    Increment {
        request_id: u32,
    },
    Read {
        request_id: u32,
    },
    Expire {
        request_id: u32,
        unix_ms: i64,
    },
    Suspend {
        request_id: u32,
        operation: HostOperationId,
    },
}

#[derive(Debug, PartialEq, Eq)]
enum InvocationOutput {
    Reply {
        request_id: u32,
        value: i64,
    },
    HostRequest {
        operation: HostOperationId,
        request: OwnedHostRequest,
    },
}

struct FaasContinuation {
    request_id: u32,
}

#[derive(Default)]
struct FaasMachine {
    value: i64,
    expires_at: Option<i64>,
    pending: VecDeque<Invocation>,
    outputs: VecDeque<InvocationOutput>,
    continuations: ContinuationRegistry<FaasContinuation>,
    wake_pending: bool,
    stopped: bool,
    clock: Option<RuntimeClock>,
}

impl RuntimeMachine for FaasMachine {
    type ExternalEvent = Invocation;
    type Output = InvocationOutput;
    type Error = Infallible;

    fn accept(
        &mut self,
        dispatch: RuntimeDispatch<Self::ExternalEvent>,
    ) -> Result<(), Self::Error> {
        self.clock = Some(dispatch.now);
        match dispatch.input {
            RuntimeInput::External(invocation) => self.pending.push_back(invocation),
            RuntimeInput::Wake => self.wake_pending = true,
            RuntimeInput::HostCompletion(completion) => self.complete_host_operation(completion),
            RuntimeInput::Shutdown => {
                self.continuations.cancel_all();
                self.stopped = true;
            }
        }
        Ok(())
    }

    fn advance(
        &mut self,
        _now: RuntimeClock,
    ) -> Result<RuntimeProgress<Self::Output>, Self::Error> {
        if self.stopped {
            return Ok(RuntimeProgress::Stopped);
        }
        if self.wake_pending {
            self.wake_pending = false;
            if self
                .clock
                .zip(self.expires_at)
                .is_some_and(|(clock, expires_at)| clock.unix_ms() >= expires_at)
            {
                self.value = 0;
                self.expires_at = None;
            }
            return Ok(RuntimeProgress::Progress);
        }
        if let Some(output) = self.outputs.pop_front() {
            return Ok(RuntimeProgress::Output(output));
        }
        let Some(invocation) = self.pending.pop_front() else {
            return Ok(RuntimeProgress::Pending);
        };
        match invocation {
            Invocation::Increment { request_id } => {
                self.value += 1;
                self.outputs.push_back(InvocationOutput::Reply {
                    request_id,
                    value: self.value,
                });
            }
            Invocation::Read { request_id } => self.outputs.push_back(InvocationOutput::Reply {
                request_id,
                value: self.value,
            }),
            Invocation::Expire {
                request_id,
                unix_ms,
            } => {
                self.expires_at = Some(unix_ms);
                self.outputs.push_back(InvocationOutput::Reply {
                    request_id,
                    value: self.value,
                });
            }
            Invocation::Suspend {
                request_id,
                operation,
            } => {
                self.continuations.register(
                    SessionId(1),
                    WaitCondition::HostOperation(operation),
                    FaasContinuation { request_id },
                );
                self.outputs.push_back(InvocationOutput::HostRequest {
                    operation,
                    request: OwnedHostRequest::Call {
                        service: Bytes::from_static(b"test-host"),
                        operation: Bytes::from_static(b"resume"),
                        payload: Bytes::new(),
                    },
                });
            }
        }
        Ok(RuntimeProgress::Progress)
    }

    fn next_deadline(&self, now: RuntimeClock) -> Option<RuntimeInstant> {
        self.expires_at
            .map(|expires_at| now.deadline_for_unix_ms(expires_at))
    }

    fn has_immediate_work(&self) -> bool {
        self.wake_pending || !self.pending.is_empty() || !self.outputs.is_empty()
    }

    fn is_stopped(&self) -> bool {
        self.stopped
    }
}

impl FaasMachine {
    fn complete_host_operation(&mut self, completion: OwnedHostCompletion) {
        let Some(suspended) = self
            .continuations
            .complete_host_operation(completion.operation)
        else {
            return;
        };
        let value = match completion.result {
            HostOperationResult::Data(data) => data.len() as i64,
            HostOperationResult::Ack => 1,
            HostOperationResult::Error(_) => -1,
        };
        self.outputs.push_back(InvocationOutput::Reply {
            request_id: suspended.state.request_id,
            value,
        });
    }
}

fn dispatch(
    kernel: &mut RuntimeKernel<FaasMachine>,
    now: RuntimeClock,
    input: RuntimeInput<Invocation>,
) -> crate::runtime::driver::RuntimeStep<InvocationOutput> {
    kernel.dispatch(RuntimeDispatch::new(now, input)).unwrap()
}

fn command(parts: &[&'static [u8]]) -> CommandArgs {
    CommandArgs::from_parts(parts.iter().copied().map(Bytes::from_static).collect())
}

fn invoke_embedded(
    runtime: &mut EmbeddedRuntime,
    request_id: u32,
    now: RuntimeClock,
    parts: &[&'static [u8]],
) -> Value {
    let step = runtime
        .dispatch(RuntimeDispatch::new(
            now,
            RuntimeInput::External(EmbeddedEvent::Execute {
                request_id,
                session: SessionId(1),
                args: command(parts),
            }),
        ))
        .unwrap();
    let EmbeddedOutput::Reply {
        request_id: output_id,
        value,
    } = step
        .outputs
        .into_iter()
        .next()
        .expect("FaaS command output");
    assert_eq!(output_id, request_id);
    value
}

#[test]
fn complete_commands_need_no_worker_or_native_poller() {
    let mut runtime = EmbeddedRuntime::new();
    assert_eq!(
        invoke_embedded(
            &mut runtime,
            1,
            RuntimeClock::new(10, 1_000),
            &[b"SET", b"key", b"value"],
        ),
        Value::ok()
    );
    assert_eq!(
        invoke_embedded(
            &mut runtime,
            2,
            RuntimeClock::new(11, 1_001),
            &[b"GET", b"key"],
        ),
        Value::bulk(Bytes::from_static(b"value"))
    );
}

#[test]
fn command_state_survives_warm_reuse_but_not_instance_discard() {
    let mut warm = EmbeddedRuntime::new();
    assert_eq!(
        invoke_embedded(
            &mut warm,
            1,
            RuntimeClock::new(10, 1_000),
            &[b"INCR", b"counter"],
        ),
        Value::integer(1)
    );
    assert_eq!(
        invoke_embedded(
            &mut warm,
            2,
            RuntimeClock::new(11, 1_001),
            &[b"INCR", b"counter"],
        ),
        Value::integer(2)
    );

    let mut cold = EmbeddedRuntime::new();
    assert_eq!(
        invoke_embedded(
            &mut cold,
            3,
            RuntimeClock::new(12, 1_002),
            &[b"GET", b"counter"],
        ),
        Value::Null
    );
}

#[test]
fn unsupported_blocking_is_a_typed_command_error() {
    let mut runtime = EmbeddedRuntime::new();
    assert_eq!(
        invoke_embedded(
            &mut runtime,
            1,
            RuntimeClock::new(10, 1_000),
            &[b"BLPOP", b"key", b"0"],
        ),
        Value::error("ERR command requires a blocking capability")
    );
}

#[test]
fn warm_instance_reuse_preserves_state() {
    let mut kernel = RuntimeKernel::with_default_limits(FaasMachine::default());
    let first = dispatch(
        &mut kernel,
        RuntimeClock::new(10, 1_000),
        RuntimeInput::External(Invocation::Increment { request_id: 1 }),
    );
    let second = dispatch(
        &mut kernel,
        RuntimeClock::new(11, 1_001),
        RuntimeInput::External(Invocation::Increment { request_id: 2 }),
    );
    assert_eq!(
        first.outputs[0],
        InvocationOutput::Reply {
            request_id: 1,
            value: 1
        }
    );
    assert_eq!(
        second.outputs[0],
        InvocationOutput::Reply {
            request_id: 2,
            value: 2
        }
    );

    let mut cold = RuntimeKernel::with_default_limits(FaasMachine::default());
    let cold_read = dispatch(
        &mut cold,
        RuntimeClock::new(12, 1_002),
        RuntimeInput::External(Invocation::Read { request_id: 3 }),
    );
    assert_eq!(
        cold_read.outputs[0],
        InvocationOutput::Reply {
            request_id: 3,
            value: 0
        }
    );
}

#[test]
fn later_invocation_can_deliver_expiration_wake() {
    let mut kernel = RuntimeKernel::with_default_limits(FaasMachine {
        value: 7,
        ..FaasMachine::default()
    });
    let expiry = dispatch(
        &mut kernel,
        RuntimeClock::new(100, 1_000),
        RuntimeInput::External(Invocation::Expire {
            request_id: 1,
            unix_ms: 1_050,
        }),
    );
    assert_eq!(
        expiry.wait,
        RuntimeWait::Event {
            deadline: Some(RuntimeInstant::from_millis(150))
        }
    );

    let wake = dispatch(
        &mut kernel,
        RuntimeClock::new(150, 1_050),
        RuntimeInput::Wake,
    );
    assert!(wake.outputs.is_empty());
    let read = dispatch(
        &mut kernel,
        RuntimeClock::new(151, 1_051),
        RuntimeInput::External(Invocation::Read { request_id: 2 }),
    );
    assert_eq!(
        read.outputs[0],
        InvocationOutput::Reply {
            request_id: 2,
            value: 0
        }
    );
}

#[test]
fn owned_host_completion_resumes_once() {
    let operation = HostOperationId::from_u64(8);
    let mut kernel = RuntimeKernel::with_default_limits(FaasMachine::default());
    let suspended = dispatch(
        &mut kernel,
        RuntimeClock::new(1, 1),
        RuntimeInput::External(Invocation::Suspend {
            request_id: 42,
            operation,
        }),
    );
    assert_eq!(
        suspended.outputs,
        vec![InvocationOutput::HostRequest {
            operation,
            request: OwnedHostRequest::Call {
                service: Bytes::from_static(b"test-host"),
                operation: Bytes::from_static(b"resume"),
                payload: Bytes::new(),
            },
        }]
    );
    assert_eq!(suspended.wait, RuntimeWait::Event { deadline: None });

    let completed = dispatch(
        &mut kernel,
        RuntimeClock::new(2, 2),
        RuntimeInput::HostCompletion(OwnedHostCompletion {
            operation,
            result: HostOperationResult::Ack,
        }),
    );
    assert_eq!(
        completed.outputs[0],
        InvocationOutput::Reply {
            request_id: 42,
            value: 1
        }
    );

    let duplicate = dispatch(
        &mut kernel,
        RuntimeClock::new(3, 3),
        RuntimeInput::HostCompletion(OwnedHostCompletion {
            operation,
            result: HostOperationResult::Ack,
        }),
    );
    assert!(duplicate.outputs.is_empty());
}

#[test]
fn shutdown_releases_owned_host_continuations() {
    let operation = HostOperationId::from_u64(9);
    let mut kernel = RuntimeKernel::with_default_limits(FaasMachine::default());
    dispatch(
        &mut kernel,
        RuntimeClock::new(1, 1),
        RuntimeInput::External(Invocation::Suspend {
            request_id: 43,
            operation,
        }),
    );
    assert_eq!(kernel.machine().continuations.len(), 1);

    let stopped = dispatch(&mut kernel, RuntimeClock::new(2, 2), RuntimeInput::Shutdown);
    assert_eq!(stopped.wait, RuntimeWait::Stopped);
    assert!(kernel.machine().continuations.is_empty());
}
