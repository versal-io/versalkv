use std::io::{self, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

#[cfg(unix)]
use std::os::unix::net::UnixStream;

use super::resp::{RespParser, RespValue};

#[cfg(unix)]
pub enum StreamType {
    Tcp(TcpStream),
    Unix(UnixStream),
}

#[cfg(not(unix))]
pub enum StreamType {
    Tcp(TcpStream),
}

impl Read for StreamType {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            StreamType::Tcp(s) => s.read(buf),
            #[cfg(unix)]
            StreamType::Unix(s) => s.read(buf),
        }
    }
}

impl Write for StreamType {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            StreamType::Tcp(s) => s.write(buf),
            #[cfg(unix)]
            StreamType::Unix(s) => s.write(buf),
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        match self {
            StreamType::Tcp(s) => s.flush(),
            #[cfg(unix)]
            StreamType::Unix(s) => s.flush(),
        }
    }
}

impl StreamType {
    pub fn set_read_timeout(&self, dur: Option<Duration>) -> io::Result<()> {
        match self {
            StreamType::Tcp(s) => s.set_read_timeout(dur),
            #[cfg(unix)]
            StreamType::Unix(s) => s.set_read_timeout(dur),
        }
    }
    pub fn set_write_timeout(&self, dur: Option<Duration>) -> io::Result<()> {
        match self {
            StreamType::Tcp(s) => s.set_write_timeout(dur),
            #[cfg(unix)]
            StreamType::Unix(s) => s.set_write_timeout(dur),
        }
    }

    #[cfg(unix)]
    pub fn raw_fd(&self) -> std::os::unix::io::RawFd {
        use std::os::unix::io::AsRawFd;
        match self {
            StreamType::Tcp(s) => s.as_raw_fd(),
            StreamType::Unix(s) => s.as_raw_fd(),
        }
    }
}

pub struct Connection {
    pub stream: StreamType,
    parser: RespParser,
    buf: [u8; 8192],
    username: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicationRdbTransfer {
    Partial,
    FullLength,
    FullEof,
}

impl Connection {
    pub fn connect_with_defaults(host: &str, port: u16) -> Result<Self, String> {
        Connection::connect(host, port, None, None, 0)
    }

    pub fn connect(
        host: &str,
        port: u16,
        password: Option<&str>,
        username: Option<&str>,
        db: u8,
    ) -> Result<Self, String> {
        let addr = format!("{}:{}", host, port);
        let addresses = addr
            .to_socket_addrs()
            .map_err(|e| format!("Could not resolve host: {}", e))?;
        let mut last_error = None;
        let mut stream: Option<StreamType> = None;
        for address in addresses {
            match TcpStream::connect_timeout(&address, Duration::from_secs(5)) {
                Ok(connected) => {
                    connected
                        .set_nodelay(true)
                        .map_err(|error| format!("Could not disable TCP Nagle: {}", error))?;
                    stream = Some(StreamType::Tcp(connected));
                    break;
                }
                Err(error) => last_error = Some(error),
            }
        }
        let stream = stream.ok_or_else(|| {
            format!(
                "Could not connect: {}",
                last_error
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| "no resolved addresses".to_string())
            )
        })?;
        stream.set_read_timeout(Some(Duration::from_secs(30))).ok();
        stream.set_write_timeout(Some(Duration::from_secs(30))).ok();

        let mut conn = Connection {
            stream,
            parser: RespParser::new(),
            buf: [0u8; 8192],
            username: username.map(|s| s.to_string()),
        };

        if let Some(pwd) = password {
            let user = conn.username.clone();
            let resp = if let Some(ref user) = user {
                conn.send_command(&[b"AUTH", user.as_bytes(), pwd.as_bytes()])?
            } else {
                conn.send_command(&[b"AUTH", pwd.as_bytes()])?
            };
            match resp {
                RespValue::SimpleString(ref s) if s == "OK" => {}
                RespValue::Error(ref e) => return Err(format!("AUTH failed: {}", e)),
                _ => return Err(format!("Unexpected AUTH response: {:?}", resp)),
            }
        }

        if db > 0 {
            let resp = conn.send_command(&[b"SELECT", db.to_string().as_bytes()])?;
            match resp {
                RespValue::SimpleString(ref s) if s == "OK" => {}
                // A password-protected server rejects SELECT before AUTH. Keep
                // the interactive connection open so the user's first command
                // receives the server's normal NOAUTH reply.
                RespValue::Error(ref e) if e.starts_with("NOAUTH") => {}
                RespValue::Error(ref e) => return Err(format!("SELECT failed: {}", e)),
                _ => return Err(format!("Unexpected SELECT response: {:?}", resp)),
            }
        }

        Ok(conn)
    }

    #[cfg(unix)]
    pub fn connect_unix(
        path: &str,
        password: Option<&str>,
        username: Option<&str>,
        db: u8,
    ) -> Result<Self, String> {
        let unix_stream = UnixStream::connect(path)
            .map_err(|e| format!("Could not connect to unix socket {}: {}", path, e))?;
        unix_stream
            .set_read_timeout(Some(Duration::from_secs(30)))
            .ok();
        unix_stream
            .set_write_timeout(Some(Duration::from_secs(30)))
            .ok();

        let mut conn = Connection {
            stream: StreamType::Unix(unix_stream),
            parser: RespParser::new(),
            buf: [0u8; 8192],
            username: username.map(|s| s.to_string()),
        };

        if let Some(pwd) = password {
            let user = conn.username.clone();
            let resp = if let Some(ref user) = user {
                conn.send_command(&[b"AUTH", user.as_bytes(), pwd.as_bytes()])?
            } else {
                conn.send_command(&[b"AUTH", pwd.as_bytes()])?
            };
            match resp {
                RespValue::SimpleString(ref s) if s == "OK" => {}
                RespValue::Error(ref e) => return Err(format!("AUTH failed: {}", e)),
                _ => return Err(format!("Unexpected AUTH response: {:?}", resp)),
            }
        }

        if db > 0 {
            let resp = conn.send_command(&[b"SELECT", db.to_string().as_bytes()])?;
            match resp {
                RespValue::SimpleString(ref s) if s == "OK" => {}
                RespValue::Error(ref e) => return Err(format!("SELECT failed: {}", e)),
                _ => return Err(format!("Unexpected SELECT response: {:?}", resp)),
            }
        }

        Ok(conn)
    }

    pub fn send_command(&mut self, args: &[&[u8]]) -> Result<RespValue, String> {
        let data = serialize_resp_command(args);
        self.stream
            .write_all(&data)
            .map_err(|e| format!("Write error: {}", e))?;
        self.read_response()
    }

    #[cfg(unix)]
    pub fn raw_fd(&self) -> std::os::unix::io::RawFd {
        self.stream.raw_fd()
    }

    /// Switch the connection to RESP3. JSON output modes require RESP3 so that
    /// commands like HGETALL return a native map rather than a flat array.
    /// Failures are non-fatal when the switch is implicit (mirrors the C cli
    /// `config.resp3 == 2` behavior).
    pub fn switch_to_resp3(&mut self, required: bool) -> Result<(), String> {
        match self.send_command(&[b"HELLO", b"3"]) {
            Ok(RespValue::Error(e)) if !required => {
                eprintln!("HELLO 3 failed: {e}");
                Ok(())
            }
            Ok(RespValue::Error(e)) => Err(format!("HELLO 3 failed: {e}")),
            Ok(_) => Ok(()),
            Err(e) if required => Err(e),
            Err(e) => {
                eprintln!("HELLO 3 failed: {e}");
                Ok(())
            }
        }
    }

    /// Send a replication command whose reply is an RDB byte stream rather
    /// than a normal RESP value.
    pub fn send_command_without_reply(&mut self, args: &[&[u8]]) -> Result<(), String> {
        let data = serialize_resp_command(args);
        self.stream
            .write_all(&data)
            .map_err(|e| format!("Write error: {}", e))
    }

    /// Copy the RDB bulk transfer following SYNC/PSYNC to `writer`.
    /// A full resync can use either a fixed-length RESP bulk string or the
    /// diskless `$EOF:<marker>` framing. Neither is a normal command reply,
    /// so it must bypass `RespParser`.
    /// The returned framing mode lets replica clients send the terminal ACK
    /// required after an EOF-framed diskless transfer, matching C valkey-cli.
    pub fn read_replication_rdb<W: Write>(
        &mut self,
        writer: &mut W,
    ) -> Result<ReplicationRdbTransfer, String> {
        let mut header = self.read_replication_line()?;
        if header.starts_with(b"+FULLRESYNC") {
            header = self.read_replication_line()?;
        }
        if header.starts_with(b"+CONTINUE") {
            return Ok(ReplicationRdbTransfer::Partial);
        }
        if header.starts_with(b"-") {
            return Err(String::from_utf8_lossy(&header).to_string());
        }
        let bulk = header.strip_prefix(b"$").ok_or_else(|| {
            format!(
                "Unexpected replication reply: {}",
                String::from_utf8_lossy(&header)
            )
        })?;

        if let Some(marker) = bulk.strip_prefix(b"EOF:") {
            self.copy_until_marker(marker, writer)?;
            return Ok(ReplicationRdbTransfer::FullEof);
        }

        let mut remaining = std::str::from_utf8(bulk)
            .map_err(|_| "Invalid RDB bulk length".to_string())?
            .parse::<usize>()
            .map_err(|_| "Invalid RDB bulk length".to_string())?;
        let mut buf = [0u8; 8192];
        while remaining > 0 {
            let count = remaining.min(buf.len());
            self.stream
                .read_exact(&mut buf[..count])
                .map_err(|e| format!("Read RDB error: {}", e))?;
            writer
                .write_all(&buf[..count])
                .map_err(|e| format!("Write RDB error: {}", e))?;
            remaining -= count;
        }
        Ok(ReplicationRdbTransfer::FullLength)
    }

    fn read_replication_line(&mut self) -> Result<Vec<u8>, String> {
        let mut line = Vec::new();
        loop {
            let mut byte = [0u8; 1];
            self.stream
                .read_exact(&mut byte)
                .map_err(replication_header_read_error)?;
            if byte[0] == b'\r' {
                self.stream
                    .read_exact(&mut byte)
                    .map_err(replication_header_read_error)?;
                if byte[0] != b'\n' {
                    return Err("Invalid replication header".to_string());
                }
                return Ok(line);
            }
            line.push(byte[0]);
        }
    }

    fn copy_until_marker<W: Write>(&mut self, marker: &[u8], writer: &mut W) -> Result<(), String> {
        if marker.is_empty() {
            return Err("Invalid EOF marker".to_string());
        }
        let mut pending = Vec::new();
        let mut buf = [0u8; 8192];
        loop {
            let count = self
                .stream
                .read(&mut buf)
                .map_err(|e| format!("Read RDB error: {}", e))?;
            if count == 0 {
                return Err("Connection closed before RDB EOF marker".to_string());
            }
            pending.extend_from_slice(&buf[..count]);
            if let Some(position) = pending
                .windows(marker.len())
                .position(|window| window == marker)
            {
                writer
                    .write_all(&pending[..position])
                    .map_err(|e| format!("Write RDB error: {}", e))?;
                return Ok(());
            }
            let keep = marker.len().saturating_sub(1).min(pending.len());
            let write_len = pending.len() - keep;
            writer
                .write_all(&pending[..write_len])
                .map_err(|e| format!("Write RDB error: {}", e))?;
            pending.drain(..write_len);
        }
    }

    pub fn read_response(&mut self) -> Result<RespValue, String> {
        loop {
            if let Some(val) = self
                .parser
                .try_parse()
                .map_err(|e| format!("Parse error: {}", e))?
            {
                return Ok(val);
            }
            let n = self
                .stream
                .read(&mut self.buf)
                .map_err(|e| format!("Read error: {}", e))?;
            if n == 0 {
                return Err("Connection closed by server".to_string());
            }
            self.parser.feed(&self.buf[..n]);
        }
    }

    pub fn select_db(&mut self, db: u8) -> Result<(), String> {
        let resp = self.send_command(&[b"SELECT", db.to_string().as_bytes()])?;
        match resp {
            RespValue::SimpleString(ref s) if s == "OK" => Ok(()),
            RespValue::Error(ref e) => Err(format!("SELECT failed: {}", e)),
            _ => Err(format!("Unexpected SELECT response: {:?}", resp)),
        }
    }
}

fn replication_header_read_error(error: io::Error) -> String {
    if error.kind() == io::ErrorKind::UnexpectedEof {
        "Connection closed before replication header".to_string()
    } else {
        format!("Read replication header error: {}", error)
    }
}

pub fn serialize_resp_command(args: &[&[u8]]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(256);
    buf.extend_from_slice(b"*");
    buf.extend_from_slice(args.len().to_string().as_bytes());
    buf.extend_from_slice(b"\r\n");
    for arg in args {
        buf.extend_from_slice(b"$");
        buf.extend_from_slice(arg.len().to_string().as_bytes());
        buf.extend_from_slice(b"\r\n");
        buf.extend_from_slice(arg);
        buf.extend_from_slice(b"\r\n");
    }
    buf
}

#[cfg(test)]
mod tests {
    use super::{Connection, ReplicationRdbTransfer, StreamType};
    use std::io::Write;
    use std::net::TcpListener;

    #[test]
    fn tcp_connections_disable_nagle() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let accept = std::thread::spawn(move || listener.accept().unwrap().0);

        let connection = Connection::connect_with_defaults("127.0.0.1", port).unwrap();
        let _server = accept.join().unwrap();

        let StreamType::Tcp(stream) = connection.stream else {
            panic!("expected TCP connection");
        };
        assert!(stream.nodelay().unwrap());
    }

    #[test]
    fn replication_rdb_reports_eof_framing_for_terminal_ack() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let marker = b"0123456789012345678901234567890123456789";
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            stream
                .write_all(
                    b"+FULLRESYNC 0123456789012345678901234567890123456789 0\r\n\
$EOF:0123456789012345678901234567890123456789\r\n\
RDB0123456789012345678901234567890123456789",
                )
                .unwrap();
        });

        let mut connection = Connection::connect_with_defaults("127.0.0.1", port).unwrap();
        let mut rdb = Vec::new();
        let transfer = connection.read_replication_rdb(&mut rdb).unwrap();

        server.join().unwrap();
        assert_eq!(transfer, ReplicationRdbTransfer::FullEof);
        assert_eq!(rdb, b"RDB");
        assert_eq!(marker.len(), 40);
    }
}
