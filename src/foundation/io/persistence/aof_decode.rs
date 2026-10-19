//! Host-neutral AOF decoding and replay.
//!
//! These types parse an AOF byte stream into RESP command frames and feed them
//! to a replay sink. They have no file-system or thread dependencies, so they
//! compile on both the native and `wasm` targets; the browser startup-recovery
//! path replays an increment file through [`AofDecoder`]/[`load_aof_bytes`].
//!
//! The native AOF module (`persistence::aof`) re-exports these so existing
//! callers keep their import paths unchanged.

/// Running counters produced while decoding an AOF stream.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AofLoadStats {
    pub cmd_count: u64,
    pub valid_up_to: usize,
}

/// A failure encountered while decoding or replaying an AOF stream.
#[derive(Debug)]
pub enum AofLoadError {
    Corrupt,
    ExecError(String),
    Io(std::io::Error),
}

/// Replay sink for decoded AOF commands.
pub trait AofCommandPlayer {
    fn select_db(&mut self, dbid: u32);
    fn execute(&mut self, args: &[&[u8]]) -> Result<(), String>;
}

/// One decode step over an AOF byte stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AofStep {
    Command(Vec<Vec<u8>>),
    Finished(AofLoadStats),
    Truncated(AofLoadStats),
}

/// Incremental RESP decoder for an AOF byte stream. A truncated tail is
/// reported (not an error) so a partially written final command can be
/// repaired against `valid_up_to`.
#[derive(Default)]
pub struct AofDecoder {
    pos: usize,
    stats: AofLoadStats,
    finished: bool,
}

impl AofDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn position(&self) -> usize {
        self.pos
    }

    pub fn step(&mut self, bytes: &[u8]) -> Result<AofStep, AofLoadError> {
        if self.finished {
            return Ok(AofStep::Finished(self.stats));
        }
        while self.pos < bytes.len() {
            while self.pos < bytes.len() && (bytes[self.pos] == b'\r' || bytes[self.pos] == b'\n') {
                self.pos += 1;
            }
            if self.pos >= bytes.len() {
                break;
            }
            if bytes[self.pos] == b'#' {
                while self.pos < bytes.len() && bytes[self.pos] != b'\n' {
                    self.pos += 1;
                }
                continue;
            }
            self.stats.valid_up_to = self.pos;
            if bytes[self.pos] != b'*' {
                return Err(AofLoadError::Corrupt);
            }
            let Some((array_len, mut cursor)) = parse_int_line(bytes, self.pos + 1) else {
                self.finished = true;
                return Ok(AofStep::Truncated(self.stats));
            };
            if array_len <= 0 {
                return Err(AofLoadError::Corrupt);
            }
            let mut parts = Vec::with_capacity(array_len as usize);
            for _ in 0..array_len {
                if cursor >= bytes.len() || bytes[cursor] != b'$' {
                    self.finished = true;
                    return Ok(AofStep::Truncated(self.stats));
                }
                let Some((bulk_len, next)) = parse_int_line(bytes, cursor + 1) else {
                    self.finished = true;
                    return Ok(AofStep::Truncated(self.stats));
                };
                if bulk_len < 0 {
                    self.finished = true;
                    return Ok(AofStep::Truncated(self.stats));
                }
                cursor = next;
                let end = cursor
                    .checked_add(bulk_len as usize)
                    .ok_or(AofLoadError::Corrupt)?;
                if end + 2 > bytes.len() {
                    self.finished = true;
                    return Ok(AofStep::Truncated(self.stats));
                }
                if bytes[end] != b'\r' || bytes[end + 1] != b'\n' {
                    return Err(AofLoadError::Corrupt);
                }
                parts.push(bytes[cursor..end].to_vec());
                cursor = end + 2;
            }
            self.pos = cursor;
            self.stats.cmd_count += 1;
            self.stats.valid_up_to = self.pos;
            return Ok(AofStep::Command(parts));
        }
        self.stats.valid_up_to = bytes.len();
        self.finished = true;
        Ok(AofStep::Finished(self.stats))
    }
}

/// Decode and replay a full in-memory AOF stream through `player`.
pub fn load_aof_bytes<P: AofCommandPlayer + ?Sized>(
    bytes: &[u8],
    player: &mut P,
) -> Result<AofLoadStats, AofLoadError> {
    let mut decoder = AofDecoder::new();
    loop {
        match decoder.step(bytes)? {
            AofStep::Command(parts) => {
                let args: Vec<&[u8]> = parts.iter().map(Vec::as_slice).collect();
                player.execute(&args).map_err(AofLoadError::ExecError)?;
            }
            AofStep::Finished(stats) | AofStep::Truncated(stats) => return Ok(stats),
        }
    }
}

fn parse_int_line(bytes: &[u8], start: usize) -> Option<(i64, usize)> {
    let mut pos = start;
    let mut sign: i64 = 1;
    let mut val: i64 = 0;
    if pos < bytes.len() && bytes[pos] == b'-' {
        sign = -1;
        pos += 1;
    }
    let mut any = false;
    while pos < bytes.len() && bytes[pos] != b'\r' {
        let c = bytes[pos];
        if !c.is_ascii_digit() {
            return None;
        }
        val = val.checked_mul(10)?.checked_add((c - b'0') as i64)?;
        any = true;
        pos += 1;
    }
    if !any {
        return None;
    }
    if pos + 2 > bytes.len() || bytes[pos] != b'\r' || bytes[pos + 1] != b'\n' {
        return None;
    }
    Some((val * sign, pos + 2))
}

#[cfg(test)]
mod tests {
    use super::*;

    struct CollectingPlayer {
        commands: Vec<Vec<Vec<u8>>>,
    }

    impl AofCommandPlayer for CollectingPlayer {
        fn select_db(&mut self, _dbid: u32) {}

        fn execute(&mut self, args: &[&[u8]]) -> Result<(), String> {
            self.commands
                .push(args.iter().map(|part| part.to_vec()).collect());
            Ok(())
        }
    }

    #[test]
    fn decodes_commands_and_tolerates_a_truncated_tail() {
        let mut player = CollectingPlayer {
            commands: Vec::new(),
        };
        // Two full commands followed by a truncated third.
        let buf = b"*1\r\n$4\r\nPING\r\n*2\r\n$3\r\nGET\r\n$1\r\nk\r\n*2\r\n$3\r\nSET";
        let stats = load_aof_bytes(buf, &mut player).unwrap();
        assert_eq!(stats.cmd_count, 2);
        assert_eq!(player.commands.len(), 2);
        assert_eq!(player.commands[0], vec![b"PING".to_vec()]);
    }

    #[test]
    fn rejects_a_non_array_leading_byte() {
        let mut player = CollectingPlayer {
            commands: Vec::new(),
        };
        assert!(matches!(
            load_aof_bytes(b"+OK\r\n", &mut player),
            Err(AofLoadError::Corrupt)
        ));
    }

    #[test]
    fn parse_int_line_rejects_missing_digits_crlf_and_overflow() {
        assert_eq!(parse_int_line(b"123\r\n", 0), Some((123, 5)));
        assert_eq!(parse_int_line(b"-5\r\n", 0), Some((-5, 4)));
        assert_eq!(parse_int_line(b"\r\n", 0), None);
        assert_eq!(parse_int_line(b"12x\r\n", 0), None);
        assert_eq!(parse_int_line(b"12\n", 0), None);
        assert_eq!(parse_int_line(b"9223372036854775808\r\n", 0), None);
    }
}
