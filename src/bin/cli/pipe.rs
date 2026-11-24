use super::conn::{serialize_resp_command, Connection, ReplyRead};
use super::RespValue;
use std::io::{self, Read, Write};
use std::sync::mpsc;
use std::time::{Duration, Instant};

#[cfg(test)]
#[path = "pipe_tests.rs"]
mod tests;

const REPLY_TIMEOUT: Duration = Duration::from_secs(5);

pub struct PipeStats {
    pub replies: usize,
    pub errors: usize,
}

pub fn transfer<R: Read + Send + 'static>(
    conn: &mut Connection,
    mut input: R,
) -> io::Result<PipeStats> {
    let mut sentinel = [0; 20];
    versal::foundation::io::sys::rand::fill_os_random_bytes(&mut sentinel)?;
    let mut writer = conn.stream.try_clone()?;
    writer.set_write_timeout(Some(REPLY_TIMEOUT))?;
    conn.stream
        .set_read_timeout(Some(Duration::from_millis(100)))?;
    let (completion_tx, completion_rx) = mpsc::channel();
    // One fixed-size input buffer bounds memory independently of stdin size.
    // Replies are drained concurrently, including while stdin remains open.
    std::thread::spawn(move || {
        let result = (|| {
            let mut buffer = [0; 65536];
            loop {
                let count = match input.read(&mut buffer) {
                    Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                    result => result?,
                };
                if count == 0 {
                    break;
                }
                writer.write_all(&buffer[..count])?;
            }
            writer.write_all(b"\r\n")?;
            writer.write_all(&serialize_resp_command(&[b"ECHO", &sentinel]))?;
            Ok::<_, io::Error>(())
        })();
        let _ = completion_tx.send(result);
    });

    let mut stats = PipeStats {
        replies: 0,
        errors: 0,
    };
    let mut finished_at = None;
    loop {
        if finished_at.is_none() {
            match completion_rx.try_recv() {
                Ok(result) => {
                    result?;
                    finished_at = Some(Instant::now());
                }
                Err(mpsc::TryRecvError::Empty) => {}
                Err(mpsc::TryRecvError::Disconnected) => {
                    return Err(io::Error::other("Pipe input worker stopped"));
                }
            }
        }
        match conn.read_reply_chunk()? {
            ReplyRead::Reply(RespValue::Bulk(reply)) if reply == sentinel => return Ok(stats),
            ReplyRead::Reply(reply) => {
                stats.replies += 1;
                if matches!(reply, RespValue::Error(_)) {
                    stats.errors += 1;
                }
                if finished_at.is_some() {
                    finished_at = Some(Instant::now());
                }
            }
            ReplyRead::Progress => {
                if finished_at.is_some() {
                    finished_at = Some(Instant::now());
                }
            }
            ReplyRead::Pending => {
                if finished_at.is_some_and(|last_progress| last_progress.elapsed() >= REPLY_TIMEOUT)
                {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "No reply received after pipe input completed",
                    ));
                }
            }
        }
    }
}
