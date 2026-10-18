use bytes::Bytes;

use super::HostOperationId;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OwnedHostRequest {
    Read {
        resource: Bytes,
    },
    Write {
        resource: Bytes,
        payload: Bytes,
    },
    Call {
        service: Bytes,
        operation: Bytes,
        payload: Bytes,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HostOperationResult {
    Data(Bytes),
    Ack,
    Error(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnedHostCompletion {
    pub operation: HostOperationId,
    pub result: HostOperationResult,
}
