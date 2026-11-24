use super::*;
use std::io::Cursor;
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::{self, Receiver};

fn serve(
    peer: impl FnOnce(TcpStream) + Send + 'static,
) -> (Connection, std::thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let handle = std::thread::spawn(move || peer(listener.accept().unwrap().0));
    (
        Connection::connect_with_defaults("127.0.0.1", port).unwrap(),
        handle,
    )
}

fn echo_sentinel(stream: &mut TcpStream) {
    let prefix = b"*2\r\n$4\r\nECHO\r\n$20\r\n";
    let mut received = Vec::new();
    let mut buffer = [0; 128];
    let sentinel = loop {
        let count = stream.read(&mut buffer).unwrap();
        assert_ne!(count, 0);
        received.extend_from_slice(&buffer[..count]);
        if let Some(position) = received
            .windows(prefix.len())
            .position(|part| part == prefix)
        {
            let start = position + prefix.len();
            if received.len() >= start + 22 {
                break received[start..start + 20].to_vec();
            }
        }
    };
    stream.write_all(b"$20\r\n").unwrap();
    stream.write_all(&sentinel[..7]).unwrap();
    stream.write_all(&sentinel[7..]).unwrap();
    stream.write_all(b"\r\n").unwrap();
}

struct GatedInput {
    first: Option<Vec<u8>>,
    observed: Receiver<()>,
}

impl Read for GatedInput {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if let Some(bytes) = self.first.take() {
            buffer[..bytes.len()].copy_from_slice(&bytes);
            return Ok(bytes.len());
        }
        self.observed
            .recv_timeout(Duration::from_secs(2))
            .map_err(io::Error::other)?;
        Ok(0)
    }
}

#[test]
fn input_is_forwarded_before_eof_and_fragmented_sentinel_is_not_counted() {
    let command = serialize_resp_command(&[b"PING"]);
    let length = command.len();
    let (observed_tx, observed_rx) = mpsc::channel();
    let (mut conn, peer) = serve(move |mut stream| {
        let mut command = vec![0; length];
        stream.read_exact(&mut command).unwrap();
        stream.write_all(b"+PONG\r\n").unwrap();
        observed_tx.send(()).unwrap();
        echo_sentinel(&mut stream);
    });
    let stats = transfer(
        &mut conn,
        GatedInput {
            first: Some(command),
            observed: observed_rx,
        },
    )
    .unwrap();
    assert_eq!(stats.replies, 1);
    assert_eq!(stats.errors, 0);
    peer.join().unwrap();
}

#[test]
fn empty_input_completes_without_business_replies() {
    let (mut conn, peer) = serve(|mut stream| echo_sentinel(&mut stream));
    let stats = transfer(&mut conn, Cursor::new(Vec::new())).unwrap();
    assert_eq!(stats.replies, 0);
    assert_eq!(stats.errors, 0);
    peer.join().unwrap();
}

#[test]
fn business_errors_are_counted_and_premature_close_is_rejected() {
    let (mut conn, peer) = serve(|mut stream| {
        stream.write_all(b"-ERR business failure\r\n").unwrap();
        echo_sentinel(&mut stream);
    });
    let stats = transfer(&mut conn, Cursor::new(Vec::new())).unwrap();
    assert_eq!(stats.replies, 1);
    assert_eq!(stats.errors, 1);
    peer.join().unwrap();
    let (mut conn, peer) = serve(drop);
    assert!(transfer(&mut conn, Cursor::new(Vec::new())).is_err());
    peer.join().unwrap();
}
