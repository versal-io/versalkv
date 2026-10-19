//! Browser-facing glue for the shared in-memory command runtime.

use bytes::Bytes;
use wasm_bindgen::prelude::*;

use crate::driver::{
    HostOperationId, HostOperationResult, OwnedHostCompletion, OwnedHostRequest, RuntimeClock,
    RuntimeDispatch, RuntimeInput, RuntimeWait,
};
use crate::embedded::{EmbeddedEvent, EmbeddedOutput, EmbeddedRuntime, Reply};
use crate::engine::command::SessionId;

#[derive(serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum BrowserInput {
    Execute {
        request_id: u32,
        argv: Vec<String>,
    },
    Wake,
    Shutdown,
    /// A durable host operation finished in the AOF I/O worker.
    HostCompletion {
        operation: u64,
        result: BrowserHostResult,
    },
}

/// Outcome of a durable host operation reported back by the AOF I/O worker.
#[derive(serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum BrowserHostResult {
    /// The operation succeeded with no payload (an append acknowledgement).
    Ack,
    /// The operation succeeded and returned payload bytes.
    Data { bytes: Vec<u8> },
    /// The operation failed with a host-provided message.
    Error { message: String },
}

#[derive(serde::Serialize)]
struct BrowserStep {
    outputs: Vec<BrowserOutput>,
    /// Durable host requests the AOF I/O worker must perform before the paired
    /// client reply is released.
    host_requests: Vec<BrowserHostRequest>,
    wait: BrowserWait,
}

#[derive(serde::Serialize)]
struct BrowserOutput {
    request_id: u32,
    value: Reply,
}

/// A durable host request serialized for the AOF I/O worker. `payload` carries
/// the MessagePack-encoded `AofIoRequest` as raw bytes.
#[derive(serde::Serialize)]
struct BrowserHostRequest {
    operation: u64,
    service: String,
    name: String,
    payload: Vec<u8>,
}

#[derive(serde::Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum BrowserWait {
    Immediate,
    Event { deadline_ms: Option<f64> },
    Stopped,
}

#[wasm_bindgen]
pub struct BrowserVersal {
    runtime: EmbeddedRuntime,
    input: Vec<u8>,
    output: Vec<u8>,
}

impl Default for BrowserVersal {
    fn default() -> Self {
        Self::new()
    }
}

#[wasm_bindgen]
impl BrowserVersal {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            runtime: EmbeddedRuntime::new(),
            input: Vec::new(),
            output: Vec::new(),
        }
    }

    /// Enable durable persistence. The host calls this once the AOF I/O worker
    /// is attached so subsequent write commands are persisted before their
    /// reply is released.
    #[wasm_bindgen]
    pub fn enable_persistence(&mut self) {
        self.runtime.enable_persistence();
    }

    #[wasm_bindgen]
    pub fn dispatch(
        &mut self,
        input: JsValue,
        monotonic_ms: f64,
        unix_ms: f64,
    ) -> Result<JsValue, JsValue> {
        let input: BrowserInput = serde_wasm_bindgen::from_value(input)
            .map_err(|error| JsValue::from_str(&format!("invalid runtime input: {error}")))?;
        let input = match input {
            BrowserInput::Execute { request_id, argv } => {
                let args = crate::engine::command::CommandArgs::from_parts(
                    argv.into_iter().map(Bytes::from).collect(),
                );
                RuntimeInput::External(EmbeddedEvent::Execute {
                    request_id,
                    session: SessionId(1),
                    args,
                })
            }
            BrowserInput::Wake => RuntimeInput::Wake,
            BrowserInput::Shutdown => RuntimeInput::Shutdown,
            BrowserInput::HostCompletion { operation, result } => {
                RuntimeInput::HostCompletion(OwnedHostCompletion {
                    operation: HostOperationId::from_u64(operation),
                    result: match result {
                        BrowserHostResult::Ack => HostOperationResult::Ack,
                        BrowserHostResult::Data { bytes } => {
                            HostOperationResult::Data(Bytes::from(bytes))
                        }
                        BrowserHostResult::Error { message } => HostOperationResult::Error(message),
                    },
                })
            }
        };
        let step = self
            .runtime
            .dispatch(RuntimeDispatch::new(
                RuntimeClock::from_micros(monotonic_clock_us(monotonic_ms), unix_clock_ms(unix_ms)),
                input,
            ))
            .map_err(|error| JsValue::from_str(&format!("runtime dispatch failed: {error}")))?;
        let mut outputs = Vec::new();
        let mut host_requests = Vec::new();
        for output in step.outputs {
            match output {
                EmbeddedOutput::Reply { request_id, value } => outputs.push(BrowserOutput {
                    request_id,
                    value: Reply::from_value(value),
                }),
                EmbeddedOutput::HostRequest { operation, request } => {
                    host_requests.push(encode_host_request(operation, request));
                }
            }
        }
        let wait = match step.wait {
            RuntimeWait::Immediate => BrowserWait::Immediate,
            RuntimeWait::Event { deadline } => BrowserWait::Event {
                deadline_ms: deadline.map(|value| value.as_millis_f64()),
            },
            RuntimeWait::Stopped => BrowserWait::Stopped,
        };
        serde_wasm_bindgen::to_value(&BrowserStep {
            outputs,
            host_requests,
            wait,
        })
        .map_err(|error| JsValue::from_str(&format!("failed to encode runtime step: {error}")))
    }

    #[wasm_bindgen]
    pub fn feed_resp(&mut self, bytes: Vec<u8>) -> Vec<u8> {
        self.feed_resp_at(bytes, 0.0)
    }

    #[wasm_bindgen]
    pub fn feed_resp_at(&mut self, bytes: Vec<u8>, now_ms: f64) -> Vec<u8> {
        self.input.extend_from_slice(&bytes);
        while let Some((command, consumed)) = parse_array(&self.input) {
            self.input.drain(..consumed);
            let reply = self.runtime.execute(&command, clock_ms(now_ms));
            serialize_reply(&reply, &mut self.output);
        }
        std::mem::take(&mut self.output)
    }
}

fn clock_ms(value: f64) -> u64 {
    value.max(0.0).min(u64::MAX as f64) as u64
}

fn encode_host_request(
    operation: HostOperationId,
    request: OwnedHostRequest,
) -> BrowserHostRequest {
    // The embedded runtime only issues durable AOF calls today; the read/write
    // request variants are carried through for forward compatibility.
    let (service, name, payload) = match request {
        OwnedHostRequest::Call {
            service,
            operation: name,
            payload,
        } => (service, name, payload),
        OwnedHostRequest::Read { resource } => (
            Bytes::from_static(b"versal.aof"),
            Bytes::from_static(b"read"),
            resource,
        ),
        OwnedHostRequest::Write { resource, payload } => {
            let mut combined = resource.to_vec();
            combined.extend_from_slice(&payload);
            (
                Bytes::from_static(b"versal.aof"),
                Bytes::from_static(b"write"),
                Bytes::from(combined),
            )
        }
    };
    BrowserHostRequest {
        operation: operation.as_u64(),
        service: String::from_utf8_lossy(&service).into_owned(),
        name: String::from_utf8_lossy(&name).into_owned(),
        payload: payload.to_vec(),
    }
}

fn monotonic_clock_us(value: f64) -> u64 {
    (value.max(0.0) * 1_000.0).min(u64::MAX as f64) as u64
}

fn unix_clock_ms(value: f64) -> i64 {
    value.max(0.0).min(i64::MAX as f64) as i64
}

fn parse_array(input: &[u8]) -> Option<(Vec<String>, usize)> {
    let mut cursor = 0;
    let count = parse_line(input, &mut cursor)?
        .strip_prefix('*')?
        .parse()
        .ok()?;
    let mut args = Vec::with_capacity(count);
    for _ in 0..count {
        let length = parse_line(input, &mut cursor)?
            .strip_prefix('$')?
            .parse::<usize>()
            .ok()?;
        let end = cursor.checked_add(length + 2)?;
        if input.get(cursor..end)?.get(length..) != Some(b"\r\n") {
            return None;
        }
        args.push(String::from_utf8_lossy(&input[cursor..cursor + length]).into_owned());
        cursor = end;
    }
    Some((args, cursor))
}

fn parse_line<'a>(input: &'a [u8], cursor: &mut usize) -> Option<&'a str> {
    let tail = input.get(*cursor..)?;
    let end = tail.windows(2).position(|bytes| bytes == b"\r\n")?;
    let line = std::str::from_utf8(&tail[..end]).ok()?;
    *cursor += end + 2;
    Some(line)
}

fn serialize_reply(reply: &Reply, output: &mut Vec<u8>) {
    match reply {
        Reply::Simple(value) => output.extend_from_slice(format!("+{value}\r\n").as_bytes()),
        Reply::Bulk(Some(value)) => {
            output.extend_from_slice(format!("${}\r\n{}\r\n", value.len(), value).as_bytes())
        }
        Reply::Bulk(None) => output.extend_from_slice(b"$-1\r\n"),
        Reply::Integer(value) => output.extend_from_slice(format!(":{value}\r\n").as_bytes()),
        Reply::Double(value) => output.extend_from_slice(format!(",{value}\r\n").as_bytes()),
        Reply::Boolean(value) => {
            output.extend_from_slice(if *value { b"#t\r\n" } else { b"#f\r\n" })
        }
        Reply::Array(values) => {
            output.extend_from_slice(format!("*{}\r\n", values.len()).as_bytes());
            for value in values {
                serialize_reply(&Reply::Bulk(value.clone()), output);
            }
        }
        Reply::Replies(replies) => {
            output.extend_from_slice(format!("*{}\r\n", replies.len()).as_bytes());
            for reply in replies {
                serialize_reply(reply, output);
            }
        }
        Reply::Map(values) => {
            output.extend_from_slice(format!("%{}\r\n", values.len()).as_bytes());
            for (key, value) in values {
                serialize_reply(key, output);
                serialize_reply(value, output);
            }
        }
        Reply::Error(value) => output.extend_from_slice(format!("-{value}\r\n").as_bytes()),
    }
}

#[cfg(test)]
mod tests {
    use super::{serialize_reply, Reply};

    #[test]
    fn serializes_recursive_command_replies() {
        let reply = Reply::Replies(vec![
            Reply::Bulk(Some("get".into())),
            Reply::Map(vec![(Reply::Bulk(Some("arity".into())), Reply::Integer(2))]),
        ]);
        let mut output = Vec::new();

        serialize_reply(&reply, &mut output);

        assert_eq!(output, b"*2\r\n$3\r\nget\r\n%1\r\n$5\r\narity\r\n:2\r\n");
    }
}
