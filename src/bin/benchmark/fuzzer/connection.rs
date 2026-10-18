use std::io::Write;
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use super::protocol::{read_reply, serialize_resp_slices, ReplyType};
use super::COMMAND_TIMEOUT_SEC;

const CONNECT_TIMEOUT_MS: u64 = 1500;

pub(super) fn connect_to_server(host: &str, port: u16) -> Result<TcpStream, String> {
    let addr = format!("{}:{}", host, port);
    let addr = addr
        .to_socket_addrs()
        .map_err(|e| format!("resolve error: {}", e))?
        .next()
        .ok_or_else(|| "no address found".to_string())?;
    let stream = TcpStream::connect_timeout(&addr, Duration::from_millis(CONNECT_TIMEOUT_MS))
        .map_err(|e| format!("connect error: {}", e))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(COMMAND_TIMEOUT_SEC)))
        .map_err(|e| format!("set read timeout: {}", e))?;
    stream
        .set_write_timeout(Some(Duration::from_secs(COMMAND_TIMEOUT_SEC)))
        .map_err(|e| format!("set write timeout: {}", e))?;
    let mut s = stream;
    let ping_cmd = serialize_resp_slices(&[b"PING"]);
    s.write_all(&ping_cmd)
        .map_err(|e| format!("ping write: {}", e))?;
    let mut buf = vec![0u8; 1024];
    let mut pos = 0;
    let reply = match read_reply(&mut s, &mut buf, &mut pos) {
        Ok(r) => r,
        Err(_) => return Err("PING did not return valid reply".to_string()),
    };
    if reply.reply_type != ReplyType::Status && reply.reply_type != ReplyType::Bulk {
        return Err("PING did not return status".to_string());
    }
    Ok(s)
}

pub(super) fn reconnect_with_backoff(
    host: &str,
    port: u16,
    max_attempts: u32,
    base_delay_ms: u64,
) -> Option<TcpStream> {
    for attempt in 0..max_attempts {
        match connect_to_server(host, port) {
            Ok(s) => return Some(s),
            Err(_) => {
                let delay = base_delay_ms * (1u64 << attempt);
                std::thread::sleep(Duration::from_millis(delay));
            }
        }
    }
    None
}
