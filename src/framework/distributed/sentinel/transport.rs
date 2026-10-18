use std::io::{self, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use crate::engine::distributed::sentinel::split_sentinel_endpoint;

const CONNECT_TIMEOUT: Duration = Duration::from_millis(500);
const READ_TIMEOUT: Duration = Duration::from_millis(500);
const WRITE_TIMEOUT: Duration = Duration::from_millis(200);

pub(super) fn connect_endpoint(addr: &str) -> io::Result<TcpStream> {
    let endpoint = split_sentinel_endpoint(addr)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid Sentinel endpoint"))?;
    let resolved = (endpoint.host, endpoint.port).to_socket_addrs()?;
    let mut last_error = None;
    for socket_addr in resolved {
        match TcpStream::connect_timeout(&socket_addr, CONNECT_TIMEOUT) {
            Ok(stream) => return Ok(stream),
            Err(error) => last_error = Some(error),
        }
    }
    Err(last_error.unwrap_or_else(|| {
        io::Error::new(
            io::ErrorKind::AddrNotAvailable,
            format!("no address resolved for {addr}"),
        )
    }))
}

pub(super) fn connect_command_stream(addr: &str) -> Result<TcpStream, String> {
    let stream = connect_endpoint(addr).map_err(|error| format!("connect failed: {error}"))?;
    stream
        .set_read_timeout(Some(READ_TIMEOUT))
        .map_err(|error| format!("set_read_timeout failed: {error}"))?;
    stream
        .set_write_timeout(Some(WRITE_TIMEOUT))
        .map_err(|error| format!("set_write_timeout failed: {error}"))?;
    Ok(stream)
}

pub(super) fn authenticate_stream(
    stream: &mut TcpStream,
    user: Option<&str>,
    pass: Option<&str>,
) -> Result<(), String> {
    let args = match (user, pass) {
        (Some(user), Some(pass)) => vec!["AUTH", user, pass],
        (Some(user), None) => vec!["AUTH", user, ""],
        (None, Some(pass)) => vec!["AUTH", pass],
        (None, None) => return Ok(()),
    };
    write_command(stream, &args)?;
    let mut reply = [0_u8; 64];
    stream
        .read(&mut reply)
        .map_err(|error| format!("auth read failed: {error}"))?;
    Ok(())
}

pub(super) fn write_command(stream: &mut TcpStream, args: &[&str]) -> Result<(), String> {
    let mut request = format!("*{}\r\n", args.len()).into_bytes();
    for arg in args {
        request.extend_from_slice(format!("${}\r\n{}\r\n", arg.len(), arg).as_bytes());
    }
    stream
        .write_all(&request)
        .map_err(|error| format!("write failed: {error}"))
}

pub(super) fn read_ok(stream: &mut TcpStream) -> Result<(), String> {
    let mut reply = [0_u8; 64];
    let received = stream
        .read(&mut reply)
        .map_err(|error| format!("read failed: {error}"))?;
    if received >= 3 && &reply[..3] == b"+OK" {
        Ok(())
    } else {
        Err(format!("unexpected response: {:?}", &reply[..received]))
    }
}

pub(super) fn send_script_kill(addr: &str) -> Result<(), String> {
    let mut stream = connect_command_stream(addr)?;
    write_command(&mut stream, &["SCRIPT", "KILL"])?;
    read_ok(&mut stream)
}

pub(super) fn sentinel_kill_clients(addr: &str, auth_user: Option<&str>, auth_pass: Option<&str>) {
    let Ok(mut stream) = connect_command_stream(addr) else {
        return;
    };
    let mut request = Vec::new();
    if auth_user.is_some() || auth_pass.is_some() {
        let (user, pass) = (auth_user.unwrap_or(""), auth_pass.unwrap_or(""));
        let auth = if user.is_empty() {
            vec!["AUTH", pass]
        } else {
            vec!["AUTH", user, pass]
        };
        push_array(&mut request, &auth);
    }
    push_array(&mut request, &["MULTI"]);
    for command in CLIENT_HANDOFF_COMMANDS {
        push_array(&mut request, command);
    }
    push_array(&mut request, &["EXEC"]);
    let _ = stream.write_all(&request);
}

const CLIENT_HANDOFF_COMMANDS: &[&[&str]] = &[
    &["CONFIG", "REWRITE"],
    &["CLIENT", "KILL", "TYPE", "normal"],
    &["CLIENT", "KILL", "TYPE", "pubsub"],
    &["CLIENT", "UNPAUSE"],
];

fn push_array(buffer: &mut Vec<u8>, args: &[&str]) {
    buffer.extend_from_slice(format!("*{}\r\n", args.len()).as_bytes());
    for arg in args {
        buffer.extend_from_slice(format!("${}\r\n{}\r\n", arg.len(), arg).as_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::connect_endpoint;
    use std::net::TcpListener;

    #[test]
    fn connector_resolves_hostname_endpoint() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = format!("localhost:{}", listener.local_addr().unwrap().port());
        let stream = connect_endpoint(&addr).unwrap();
        assert!(stream.peer_addr().unwrap().ip().is_loopback());
    }
}
