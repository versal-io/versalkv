use sha2::{Digest, Sha256};
use versal::foundation::io::store::kv_engine::DbSet;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AofScanOptions {
    pub filename: String,
    pub has_rdb_preamble: bool,
    pub timestamp: Option<u64>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AofPreambleInfo {
    None,
    Only { size: u64 },
    Tail { offset: u64 },
    Invalid,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AofScanOutcome {
    Valid,
    Empty,
    Corrupt,
    TimestampBoundary { offset: u64, timestamp: u64 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AofScanSnapshot {
    pub original_size: u64,
    pub digest: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AofScanReport {
    pub outcome: AofScanOutcome,
    pub size: u64,
    pub ok_up_to: u64,
    pub line: u64,
    pub error: Option<String>,
    pub preamble: AofPreambleInfo,
    pub snapshot: AofScanSnapshot,
}

pub fn scan_aof_bytes(data: &[u8], options: &AofScanOptions) -> AofScanReport {
    AofScanner::new(data, options).scan()
}

struct AofScanner<'a> {
    data: &'a [u8],
    options: &'a AofScanOptions,
    pos: usize,
    ok_up_to: usize,
    line: u64,
    multi: i32,
    preamble: AofPreambleInfo,
    last_entry_start: usize,
}

impl<'a> AofScanner<'a> {
    fn new(data: &'a [u8], options: &'a AofScanOptions) -> Self {
        Self {
            data,
            options,
            pos: 0,
            ok_up_to: 0,
            line: 1,
            multi: 0,
            preamble: AofPreambleInfo::None,
            last_entry_start: 0,
        }
    }

    fn scan(mut self) -> AofScanReport {
        if self.data.is_empty() {
            return self.report(AofScanOutcome::Empty, None);
        }
        if let Err(error) = self.scan_preamble() {
            return self.report(AofScanOutcome::Corrupt, Some(error));
        }
        if matches!(self.preamble, AofPreambleInfo::Only { .. }) {
            return self.report(AofScanOutcome::Valid, None);
        }
        while self.pos < self.data.len() {
            if let Some(report) = self.scan_entry() {
                return report;
            }
        }
        if self.multi != 0 {
            return self.corrupt_at(
                self.last_entry_start,
                "Reached EOF before reading EXEC for MULTI".to_string(),
            );
        }
        if self.ok_up_to < self.data.len() {
            return self.report(AofScanOutcome::Corrupt, None);
        }
        self.report(AofScanOutcome::Valid, None)
    }

    fn scan_preamble(&mut self) -> Result<(), String> {
        if !self.options.has_rdb_preamble {
            return Ok(());
        }
        if self.data.len() < 9 || !has_rdb_signature(self.data) {
            self.preamble = AofPreambleInfo::Invalid;
            return Err("RDB preamble of AOF file is not sane, aborting.".to_string());
        }
        let (offset, parsed) = find_rdb_end(self.data);
        self.pos = offset;
        self.ok_up_to = offset;
        self.preamble = if offset == 0 && !parsed {
            AofPreambleInfo::Only {
                size: self.data.len() as u64,
            }
        } else {
            AofPreambleInfo::Tail {
                offset: offset as u64,
            }
        };
        Ok(())
    }

    fn scan_entry(&mut self) -> Option<AofScanReport> {
        self.last_entry_start = self.pos;
        match self.data[self.pos] {
            b'#' => self.scan_annotation(),
            b'*' => {
                let start = self.pos;
                if let Err(error) = self.scan_resp() {
                    return Some(self.corrupt_at(start, error));
                }
                if self.multi == 0 {
                    self.ok_up_to = self.pos;
                }
                None
            }
            b'\r' | b'\n' => {
                self.pos += 1;
                None
            }
            _ => Some(self.corrupt(format!(
                "AOF {} format error at byte {}",
                self.options.filename, self.pos
            ))),
        }
    }

    fn scan_annotation(&mut self) -> Option<AofScanReport> {
        let start = self.pos;
        while self.pos < self.data.len() && self.data[self.pos] != b'\n' {
            self.pos += 1;
        }
        if self.pos < self.data.len() {
            self.pos += 1;
        }
        let timestamp = annotation_timestamp(&self.data[start..self.pos]);
        if self.multi == 0 {
            self.ok_up_to = self.pos;
        }
        let (target, found) = self.options.timestamp.zip(timestamp)?;
        (found > target).then(|| {
            self.ok_up_to = start;
            self.report(
                AofScanOutcome::TimestampBoundary {
                    offset: start as u64,
                    timestamp: found,
                },
                None,
            )
        })
    }

    fn scan_resp(&mut self) -> Result<(), String> {
        let argc = self.read_number(b'*', "array")?;
        if argc < 0 {
            return Err("Negative array length in AOF".to_string());
        }
        for index in 0..argc as usize {
            let content = self.read_bulk()?;
            if index == 0 {
                self.update_multi(&content)?;
            }
        }
        self.line += 1 + 2 * argc as u64;
        Ok(())
    }

    fn read_bulk(&mut self) -> Result<Vec<u8>, String> {
        let len = self.read_number(b'$', "bulk string")?;
        if len < 0 {
            return Ok(Vec::new());
        }
        let len = len as usize;
        let end = self
            .pos
            .checked_add(len)
            .ok_or_else(|| "Truncated bulk string".to_string())?;
        if end + 2 > self.data.len() {
            return Err("Truncated bulk string".to_string());
        }
        let content = self.data[self.pos..end].to_vec();
        self.pos = end;
        self.consume_crlf("bulk string data")?;
        Ok(content)
    }

    fn read_number(&mut self, prefix: u8, kind: &str) -> Result<i64, String> {
        if self.data.get(self.pos) != Some(&prefix) {
            return Err(format!("Expected '{}' for {} length", prefix as char, kind));
        }
        self.pos += 1;
        let start = self.pos;
        while self.pos < self.data.len() && self.data[self.pos] != b'\r' {
            self.pos += 1;
        }
        let value = std::str::from_utf8(&self.data[start..self.pos])
            .map_err(|_| "Invalid UTF-8 in length".to_string())?;
        self.consume_crlf(&format!("{} length", kind))?;
        value
            .parse()
            .map_err(|_| format!("Bad {} length: {}", kind, value))
    }

    fn consume_crlf(&mut self, kind: &str) -> Result<(), String> {
        if self.data.get(self.pos..self.pos + 2) != Some(b"\r\n") {
            return Err(format!("Expected CRLF after {}", kind));
        }
        self.pos += 2;
        Ok(())
    }

    fn update_multi(&mut self, command: &[u8]) -> Result<(), String> {
        if command.eq_ignore_ascii_case(b"multi") {
            if self.multi != 0 {
                return Err(format!("Unexpected MULTI in AOF {}", self.options.filename));
            }
            self.multi += 1;
        } else if command.eq_ignore_ascii_case(b"exec") {
            self.multi -= 1;
            if self.multi != 0 {
                return Err(format!("Unexpected EXEC in AOF {}", self.options.filename));
            }
        }
        Ok(())
    }

    fn corrupt(&self, error: String) -> AofScanReport {
        self.corrupt_at(self.pos, error)
    }

    fn corrupt_at(&self, offset: usize, error: String) -> AofScanReport {
        self.report(
            AofScanOutcome::Corrupt,
            Some(format!("0x{:016x}: {}", offset, error)),
        )
    }

    fn report(&self, outcome: AofScanOutcome, error: Option<String>) -> AofScanReport {
        AofScanReport {
            outcome,
            size: self.data.len() as u64,
            ok_up_to: self.ok_up_to as u64,
            line: self.line,
            error,
            preamble: self.preamble,
            snapshot: AofScanSnapshot {
                original_size: self.data.len() as u64,
                digest: Sha256::digest(self.data).into(),
            },
        }
    }
}

fn has_rdb_signature(data: &[u8]) -> bool {
    data.starts_with(b"REDIS") || data.starts_with(b"VALKEY")
}

fn find_rdb_end(data: &[u8]) -> (usize, bool) {
    let mut databases = DbSet::new(16);
    match versal::foundation::io::persistence::load_rdb_bytes_checked(data, &mut databases, false) {
        Ok((_, consumed)) => (consumed, true),
        Err(_) => {
            let offset = data
                .iter()
                .enumerate()
                .skip(9)
                .find_map(|(index, byte)| matches!(byte, b'*' | b'#').then_some(index))
                .unwrap_or(0);
            (offset, false)
        }
    }
}

fn annotation_timestamp(line: &[u8]) -> Option<u64> {
    let text = std::str::from_utf8(line).ok()?;
    text.strip_prefix("#TS:")?.trim().parse().ok()
}
