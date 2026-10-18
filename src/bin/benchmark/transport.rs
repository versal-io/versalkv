use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::os::unix::net::UnixStream;
use std::time::Duration;

use super::cli::BenchConfig;
use super::protocol;

pub struct ConnectionOptions {
    pub host: String,
    pub port: u16,
    pub unix_socket: Option<String>,
    pub user: Option<String>,
    pub password: Option<String>,
    pub dbnum: Option<u32>,
    pub resp3: bool,
}

impl ConnectionOptions {
    pub fn from_config(config: &BenchConfig) -> Self {
        Self {
            host: config.host.clone(),
            port: config.port,
            unix_socket: config.unix_socket.clone(),
            user: config.user.clone(),
            password: config.password.clone(),
            dbnum: config.dbnum,
            resp3: config.resp3,
        }
    }
}

pub struct RespConnection {
    options: ConnectionOptions,
    stream: ConnectionStream,
    scratch: Vec<u8>,
}

impl RespConnection {
    pub fn connect(options: &ConnectionOptions) -> Result<Self, String> {
        if let Some(path) = options.unix_socket.as_deref() {
            let stream = UnixStream::connect(path).map_err(|err| err.to_string())?;
            stream
                .set_read_timeout(Some(Duration::from_secs(30)))
                .map_err(|err| err.to_string())?;
            stream
                .set_write_timeout(Some(Duration::from_secs(30)))
                .map_err(|err| err.to_string())?;
            return Self::from_stream(options, ConnectionStream::Unix(stream));
        }
        Self::connect_to(options, &options.host, options.port)
    }

    pub fn send_request(&mut self, command: &[Vec<u8>]) -> Result<Vec<u8>, String> {
        protocol::send_command(&mut self.stream, &mut self.scratch, command)
    }

    pub fn send_request_with_redirect(&mut self, command: &[Vec<u8>]) -> Result<Vec<u8>, String> {
        match self.send_request(command) {
            Ok(reply) => Ok(reply),
            Err(err) => {
                let Some((host, port)) = parse_moved_target(&err) else {
                    return Err(err);
                };
                let mut redirected = Self::connect_to(&self.options, &host, port)?;
                let reply = redirected.send_request(command)?;
                *self = redirected;
                Ok(reply)
            }
        }
    }

    pub fn write_all(&mut self, buf: &[u8]) -> Result<(), String> {
        self.stream.write_all(buf).map_err(|err| err.to_string())
    }

    pub fn read_one_reply(&mut self) -> Result<Vec<u8>, String> {
        protocol::read_one_reply(&mut self.stream, &mut self.scratch)
    }

    fn connect_to(options: &ConnectionOptions, host: &str, port: u16) -> Result<Self, String> {
        let addr = format!("{host}:{port}");
        let addr = addr
            .to_socket_addrs()
            .map_err(|err| err.to_string())?
            .next()
            .ok_or_else(|| format!("Unable to resolve {addr}"))?;
        let stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5))
            .map_err(|err| err.to_string())?;
        stream
            .set_read_timeout(Some(Duration::from_secs(30)))
            .map_err(|err| err.to_string())?;
        stream
            .set_write_timeout(Some(Duration::from_secs(30)))
            .map_err(|err| err.to_string())?;

        Self::from_stream(options, ConnectionStream::Tcp(stream))
    }

    fn from_stream(options: &ConnectionOptions, stream: ConnectionStream) -> Result<Self, String> {
        let mut connection = Self {
            options: ConnectionOptions {
                host: options.host.clone(),
                port: options.port,
                unix_socket: options.unix_socket.clone(),
                user: options.user.clone(),
                password: options.password.clone(),
                dbnum: options.dbnum,
                resp3: options.resp3,
            },
            stream,
            scratch: Vec::new(),
        };

        connection.initialize()?;
        Ok(connection)
    }

    fn initialize(&mut self) -> Result<(), String> {
        if self.options.resp3 {
            self.send_request(&[b"HELLO".to_vec(), b"3".to_vec()])?;
        }
        if let Some(password) = self.options.password.clone() {
            let auth = if let Some(user) = &self.options.user {
                vec![
                    b"AUTH".to_vec(),
                    user.as_bytes().to_vec(),
                    password.as_bytes().to_vec(),
                ]
            } else {
                vec![b"AUTH".to_vec(), password.as_bytes().to_vec()]
            };
            self.send_request(&auth)?;
        }
        if let Some(dbnum) = self.options.dbnum {
            self.send_request(&[b"SELECT".to_vec(), dbnum.to_string().into_bytes()])?;
        }
        Ok(())
    }
}

enum ConnectionStream {
    Tcp(TcpStream),
    Unix(UnixStream),
}

impl Read for ConnectionStream {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Tcp(stream) => stream.read(buffer),
            Self::Unix(stream) => stream.read(buffer),
        }
    }
}

impl Write for ConnectionStream {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Tcp(stream) => stream.write(buffer),
            Self::Unix(stream) => stream.write(buffer),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Tcp(stream) => stream.flush(),
            Self::Unix(stream) => stream.flush(),
        }
    }
}

fn parse_moved_target(err: &str) -> Option<(String, u16)> {
    let mut parts = err.split_whitespace();
    let kind = parts.next()?;
    if !kind.ends_with("MOVED") && kind != "-MOVED" && kind != "MOVED" {
        return None;
    }
    let _slot = parts.next()?;
    let target = parts.next()?;
    let (host, port) = target.rsplit_once(':')?;
    Some((host.to_string(), port.parse().ok()?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_moved_target() {
        assert_eq!(
            parse_moved_target("-MOVED 8338 127.0.0.1:21113"),
            Some(("127.0.0.1".to_string(), 21113))
        );
    }

    #[test]
    fn ignores_non_moved_errors() {
        assert_eq!(parse_moved_target("-ERR wrong number of arguments"), None);
    }
}
