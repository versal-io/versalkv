use std::collections::HashMap;
use std::io::{self, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};

use super::reply::{read_reply, RetryPhase, TargetReply};

const MAX_SOCKETS: usize = 64;
const IDLE_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy)]
pub enum Auth<'a> {
    Password(&'a [u8]),
    UserPassword(&'a [u8], &'a [u8]),
}

pub struct RestoreItem<'a> {
    pub key: &'a [u8],
    pub ttl_ms: u64,
    pub payload: &'a [u8],
}

pub struct TransferRequest<'a> {
    pub host: &'a str,
    pub port: u16,
    pub db: i64,
    pub timeout: Duration,
    pub auth: Option<Auth<'a>>,
    pub replace: bool,
    pub items: &'a [RestoreItem<'a>],
}

pub struct TransferOutcome {
    pub acknowledged: Vec<bool>,
    pub target_error: Option<String>,
}

struct CachedSocket {
    stream: TcpStream,
    last_dbid: i64,
    last_use: Instant,
}

pub struct MigrateSocketCache {
    sockets: HashMap<String, CachedSocket>,
}

impl MigrateSocketCache {
    pub fn new() -> Self {
        Self {
            sockets: HashMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.sockets.len()
    }

    pub fn expire_idle(&mut self, now: Instant) {
        self.sockets
            .retain(|_, socket| now.saturating_duration_since(socket.last_use) <= IDLE_TIMEOUT);
    }

    pub fn transfer(&mut self, request: &TransferRequest<'_>) -> Result<TransferOutcome, String> {
        let key = format!("{}:{}", request.host, request.port);
        let deadline = Instant::now() + request.timeout;
        let mut retried = false;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(io_error_message(
                    io::Error::from(io::ErrorKind::TimedOut),
                    "connecting/reading",
                ));
            }
            let req_with_deadline = TransferRequest {
                host: request.host,
                port: request.port,
                db: request.db,
                timeout: remaining,
                auth: request.auth,
                replace: request.replace,
                items: request.items,
            };
            match self.transfer_once(&key, &req_with_deadline) {
                Ok(outcome) => return Ok(outcome),
                Err(AttemptError::Retryable {
                    phase: RetryPhase::PreRestore,
                    error,
                    ..
                }) if !retried && error.kind() != io::ErrorKind::TimedOut => {
                    retried = true;
                    self.sockets.remove(&key);
                }
                Err(AttemptError::Io {
                    error,
                    operation: phase,
                }) => {
                    return Err(io_error_message(error, phase));
                }
                Err(AttemptError::Retryable {
                    error,
                    operation: phase,
                    ..
                }) => {
                    return Err(io_error_message(error, phase));
                }
                Err(AttemptError::Protocol(error)) => return Err(error),
            }
        }
    }

    fn transfer_once(
        &mut self,
        key: &str,
        request: &TransferRequest<'_>,
    ) -> Result<TransferOutcome, AttemptError> {
        let cached = self.sockets.remove(key);
        let mut socket = match cached {
            Some(socket) => socket,
            None => CachedSocket {
                stream: connect(request)?,
                last_dbid: -1,
                last_use: Instant::now(),
            },
        };
        socket
            .stream
            .set_read_timeout(Some(request.timeout))
            .map_err(|error| AttemptError::Io {
                error,
                operation: "reading",
            })?;
        socket
            .stream
            .set_write_timeout(Some(request.timeout))
            .map_err(|error| AttemptError::Io {
                error,
                operation: "writing",
            })?;

        send_auth(&mut socket.stream, request.auth)?;
        if socket.last_dbid != request.db {
            send_select(&mut socket.stream, request.db)?;
            socket.last_dbid = request.db;
        }

        let result = send_restores(&mut socket.stream, request);
        socket.last_use = Instant::now();
        if result.is_ok() {
            if result
                .as_ref()
                .ok()
                .and_then(|outcome| outcome.target_error.as_ref())
                .is_some()
            {
                socket.last_dbid = -1;
            }
            self.insert_socket(key, socket);
        }
        result
    }

    fn insert_socket(&mut self, key: &str, socket: CachedSocket) {
        if self.sockets.len() >= MAX_SOCKETS && !self.sockets.contains_key(key) {
            if let Some(oldest) = self
                .sockets
                .iter()
                .min_by_key(|(_, socket)| socket.last_use)
                .map(|(key, _)| key.clone())
            {
                self.sockets.remove(&oldest);
            }
        }
        self.sockets.insert(key.to_owned(), socket);
    }
}

fn connect(request: &TransferRequest<'_>) -> Result<TcpStream, AttemptError> {
    let address = format!("{}:{}", request.host, request.port);
    let addr = address
        .to_socket_addrs()
        .map_err(|error| AttemptError::Io {
            error,
            operation: "connecting",
        })?
        .next()
        .ok_or_else(|| AttemptError::Protocol("ERR MIGRATE target address not found".into()))?;
    TcpStream::connect_timeout(&addr, request.timeout).map_err(|error| AttemptError::Io {
        error,
        operation: "connecting",
    })
}

fn send_auth(stream: &mut TcpStream, auth: Option<Auth<'_>>) -> Result<(), AttemptError> {
    let Some(auth) = auth else {
        return Ok(());
    };
    let frame = match auth {
        Auth::Password(password) => super::codec::encode_command(&[b"AUTH", password]),
        Auth::UserPassword(username, password) => {
            super::codec::encode_command(&[b"AUTH", username, password])
        }
    };
    send_and_expect_ok(stream, &frame)
}

fn send_select(stream: &mut TcpStream, db: i64) -> Result<(), AttemptError> {
    let db = db.to_string();
    let frame = super::codec::encode_command(&[b"SELECT", db.as_bytes()]);
    send_and_expect_ok(stream, &frame)
}

fn send_and_expect_ok(stream: &mut TcpStream, frame: &[u8]) -> Result<(), AttemptError> {
    stream
        .write_all(frame)
        .map_err(|error| AttemptError::Retryable {
            error,
            operation: "writing",
            phase: RetryPhase::PreRestore,
        })?;
    match read_reply(stream).map_err(|error| AttemptError::Retryable {
        error,
        operation: "reading",
        phase: RetryPhase::PreRestore,
    })? {
        TargetReply::Ok => Ok(()),
        TargetReply::Error(error) => Err(AttemptError::Protocol(format!(
            "Target instance replied with error: {}",
            error
        ))),
    }
}

fn send_restores(
    stream: &mut TcpStream,
    request: &TransferRequest<'_>,
) -> Result<TransferOutcome, AttemptError> {
    let mut acknowledged = Vec::with_capacity(request.items.len());
    let mut target_error = None;
    for item in request.items {
        let ttl = item.ttl_ms.to_string();
        let mut args: Vec<&[u8]> = vec![b"RESTORE-ASKING", item.key, ttl.as_bytes(), item.payload];
        if request.replace {
            args.push(b"REPLACE");
        }
        stream
            .write_all(&super::codec::encode_command(&args))
            .map_err(|error| AttemptError::Io {
                error,
                operation: "writing",
            })?;
        match read_reply(stream).map_err(|error| AttemptError::Io {
            error,
            operation: "reading",
        })? {
            TargetReply::Ok => acknowledged.push(true),
            TargetReply::Error(error) => {
                if target_error.is_none() {
                    target_error = Some(error);
                }
                acknowledged.push(false);
            }
        }
    }
    Ok(TransferOutcome {
        acknowledged,
        target_error,
    })
}

fn io_error_message(error: io::Error, phase: &'static str) -> String {
    if error.kind() == io::ErrorKind::TimedOut {
        format!("IOERR error or timeout {} to target instance", phase)
    } else {
        format!("IOERR error {} to target instance", phase)
    }
}

#[derive(Debug)]
enum AttemptError {
    Retryable {
        error: io::Error,
        operation: &'static str,
        phase: RetryPhase,
    },
    Io {
        error: io::Error,
        operation: &'static str,
    },
    Protocol(String),
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::net::{TcpListener, TcpStream};
    use std::thread;
    use std::time::{Duration, Instant};

    use super::*;

    #[test]
    fn retries_io_during_auth_before_restore() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let (mut first, _) = listener.accept().unwrap();
            read_command(&mut first);
            drop(first);

            let (mut second, _) = listener.accept().unwrap();
            read_command(&mut second);
            second.write_all(b"+OK\r\n").unwrap();
            read_command(&mut second);
            second.write_all(b"+OK\r\n").unwrap();
            let restore = read_command(&mut second);
            second.write_all(b"+OK\r\n").unwrap();
            restore
        });

        let items = [RestoreItem {
            key: b"key",
            ttl_ms: 0,
            payload: b"payload",
        }];
        let request = TransferRequest {
            host: "127.0.0.1",
            port,
            db: 0,
            timeout: Duration::from_secs(1),
            auth: Some(Auth::Password(b"secret")),
            replace: false,
            items: &items,
        };
        let mut cache = MigrateSocketCache::new();
        let outcome = cache.transfer(&request);
        let restore = server.join().unwrap();

        assert_eq!(outcome.unwrap().acknowledged, vec![true]);
        assert_eq!(restore, b"key");
    }

    #[test]
    fn does_not_retry_after_restore_was_written() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = thread::spawn(move || {
            let (mut first, _) = listener.accept().unwrap();
            read_command(&mut first);
            first.write_all(b"+OK\r\n").unwrap();
            let first_restore = read_command(&mut first);
            first.write_all(b"+OK\r\n").unwrap();
            let second_restore = read_command(&mut first);
            drop(first);

            listener.set_nonblocking(true).unwrap();
            thread::sleep(Duration::from_millis(50));
            let accepted_second_connection = listener.accept().is_ok();
            (first_restore, second_restore, accepted_second_connection)
        });

        let items = [
            RestoreItem {
                key: b"first",
                ttl_ms: 0,
                payload: b"payload-1",
            },
            RestoreItem {
                key: b"second",
                ttl_ms: 0,
                payload: b"payload-2",
            },
        ];
        let request = TransferRequest {
            host: "127.0.0.1",
            port,
            db: 0,
            timeout: Duration::from_secs(1),
            auth: None,
            replace: false,
            items: &items,
        };
        let mut cache = MigrateSocketCache::new();
        let outcome = cache.transfer(&request);
        let (first_restore, second_restore, accepted_second_connection) = server.join().unwrap();

        assert!(matches!(outcome, Err(ref error) if error.contains("IOERR")));
        assert_eq!(first_restore, b"first");
        assert_eq!(second_restore, b"second");
        assert!(!accepted_second_connection);
    }

    #[test]
    fn io_errors_report_their_operation_phase() {
        let error = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        assert_eq!(
            io_error_message(error, "connecting"),
            "IOERR error connecting to target instance"
        );
        let error = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset");
        assert_eq!(
            io_error_message(error, "writing"),
            "IOERR error writing to target instance"
        );
        let error = std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout");
        assert_eq!(
            io_error_message(error, "reading"),
            "IOERR error or timeout reading to target instance"
        );
    }

    #[test]
    fn cache_capacity_and_idle_expiration_are_bounded() {
        let mut cache = MigrateSocketCache::new();
        assert_eq!(MAX_SOCKETS, 64);
        assert_eq!(IDLE_TIMEOUT, Duration::from_secs(10));
        cache.expire_idle(Instant::now());
        assert_eq!(cache.len(), 0);
    }

    fn read_command(stream: &mut TcpStream) -> Vec<u8> {
        read_command_result(stream).unwrap()
    }

    fn read_command_result(stream: &mut TcpStream) -> std::io::Result<Vec<u8>> {
        let mut byte = [0u8; 1];
        stream.read_exact(&mut byte)?;
        assert_eq!(byte[0], b'*');
        let count = read_line(stream)?.parse::<usize>().unwrap();
        let mut first_arg = Vec::new();
        for index in 0..count {
            stream.read_exact(&mut byte)?;
            assert_eq!(byte[0], b'$');
            let len = read_line(stream)?.parse::<usize>().unwrap();
            let mut value = vec![0u8; len];
            stream.read_exact(&mut value)?;
            stream.read_exact(&mut [0u8; 2])?;
            if index == 1 {
                first_arg = value;
            }
        }
        Ok(first_arg)
    }

    fn read_line(stream: &mut TcpStream) -> std::io::Result<String> {
        let mut line = Vec::new();
        let mut byte = [0u8; 1];
        loop {
            stream.read_exact(&mut byte)?;
            if byte[0] == b'\r' {
                stream.read_exact(&mut byte)?;
                assert_eq!(byte[0], b'\n');
                return Ok(String::from_utf8(line).unwrap());
            }
            line.push(byte[0]);
        }
    }
}
