use crate::engine::command::context::Token;
use crate::engine::distributed::sentinel::SentinelInstanceId;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RespFrame {
    Simple(Vec<u8>),
    Error(Vec<u8>),
    Integer(i64),
    Bulk(Vec<u8>),
    Array(Vec<RespFrame>),
    Null,
}

impl RespFrame {
    pub fn take(input: &mut Vec<u8>) -> Option<Self> {
        let (frame, consumed) = parse_frame(input)?;
        input.drain(..consumed);
        Some(frame)
    }

    pub(super) fn is_complete(input: &[u8]) -> bool {
        parse_frame(input).is_some()
    }
}

fn parse_frame(input: &[u8]) -> Option<(RespFrame, usize)> {
    let marker = *input.first()?;
    match marker {
        b'+' | b'-' | b':' => {
            let end = find_crlf(input, 1)?;
            let body = input[1..end].to_vec();
            let frame = match marker {
                b'+' => RespFrame::Simple(body),
                b'-' => RespFrame::Error(body),
                _ => RespFrame::Integer(std::str::from_utf8(&body).ok()?.parse().ok()?),
            };
            Some((frame, end + 2))
        }
        b'$' => {
            let end = find_crlf(input, 1)?;
            let len: isize = std::str::from_utf8(&input[1..end]).ok()?.parse().ok()?;
            if len == -1 {
                return Some((RespFrame::Null, end + 2));
            }
            if len < 0 {
                return None;
            }
            let start = end + 2;
            let finish = start.checked_add(len as usize)?;
            if input.get(finish..finish + 2) != Some(b"\r\n") {
                return None;
            }
            Some((RespFrame::Bulk(input[start..finish].to_vec()), finish + 2))
        }
        b'*' => {
            let end = find_crlf(input, 1)?;
            let count: isize = std::str::from_utf8(&input[1..end]).ok()?.parse().ok()?;
            if count == -1 {
                return Some((RespFrame::Null, end + 2));
            }
            if count < 0 {
                return None;
            }
            let mut offset = end + 2;
            let mut frames = Vec::with_capacity(count as usize);
            for _ in 0..count {
                let (frame, consumed) = parse_frame(&input[offset..])?;
                offset += consumed;
                frames.push(frame);
            }
            Some((RespFrame::Array(frames), offset))
        }
        _ => None,
    }
}

fn find_crlf(input: &[u8], start: usize) -> Option<usize> {
    input[start..]
        .windows(2)
        .position(|window| window == b"\r\n")
        .map(|offset| start + offset)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SentinelLinkKind {
    Command,
    HelloSubscription,
}

#[derive(Debug)]
pub(crate) struct SentinelLink {
    pub token: Token,
    pub addr: String,
    pub kind: SentinelLinkKind,
    pub connected: bool,
    pub authenticated: bool,
    pub refcount: usize,
    pub close_pending: bool,
    pub input: Vec<u8>,
    pub output: Vec<Vec<u8>>,
    pub pending: Vec<SentinelRequest>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SentinelRequest {
    Auth,
    SetName,
    Ping(SentinelInstanceId),
    Info(SentinelInstanceId),
    PublishHello,
    Vote(SentinelInstanceId, SentinelInstanceId),
    SubscribeHello,
}

pub(super) fn encode_command(args: &[&str]) -> Vec<u8> {
    let mut frame = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        frame.extend_from_slice(format!("${}\r\n{}\r\n", arg.len(), arg).as_bytes());
    }
    frame
}

pub(crate) fn queue_request(link: &mut SentinelLink, request: SentinelRequest, args: &[&str]) {
    link.output.push(encode_command(args));
    link.pending.push(request);
}

pub(super) fn pop_pending_request(link: &mut SentinelLink) -> Option<SentinelRequest> {
    if link.pending.is_empty() {
        None
    } else {
        Some(link.pending.remove(0))
    }
}

pub(crate) fn bulk_as_str(frame: &RespFrame) -> Option<&str> {
    match frame {
        RespFrame::Bulk(data) => std::str::from_utf8(data).ok(),
        _ => None,
    }
}
