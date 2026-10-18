mod endpoint;
mod lifecycle;
mod proto;
mod reply;

mod manager;

pub use manager::SentinelLinkManager;
pub use proto::RespFrame;
pub(super) use proto::{bulk_as_str, queue_request, SentinelLink, SentinelRequest};
