use crate::constants::RDB_VERSION;
use crate::state::{state_now_ms, type_index, RdbState};
use versal::foundation::io::persistence::codec::types::{
    self, crc64, RDB_OPCODE_AUX, RDB_OPCODE_EOF, RDB_OPCODE_EXPIRETIME, RDB_OPCODE_EXPIRETIME_MS,
    RDB_OPCODE_FREQ, RDB_OPCODE_FUNCTION2, RDB_OPCODE_IDLE, RDB_OPCODE_RESIZEDB,
    RDB_OPCODE_SELECTDB,
};
use versal::foundation::io::persistence::codec::{
    decoder, load_robj, load_robj_with_hash_field_expires,
};
use versal::foundation::io::store::Robj;

const RDB_OPCODE_SLOT_INFO: u8 = 244;
const RDB_OPCODE_SLOT_IMPORT: u8 = 243;
const RDB_OPCODE_FUNCTION_PRE_GA: u8 = 246;
const RDB_OPCODE_MODULE_AUX: u8 = 247;
const RDB_FOREIGN_VERSION_MIN: u64 = 12;
const RDB_FOREIGN_VERSION_MAX: u64 = 79;
const RDB_FOREIGN_TYPE_MIN: u8 = 22;
const RDB_FOREIGN_TYPE_MAX: u8 = 243;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RdbCheckPhase {
    Header,
    ReadType,
    ReadExpire,
    ReadLength,
    ReadAux,
    ReadModuleAux,
    ReadFunctions,
    ReadKey,
    ReadObject,
    Checksum,
}

impl RdbCheckPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Header => "start",
            Self::ReadType => "read-type",
            Self::ReadExpire => "read-expire",
            Self::ReadLength => "read-len",
            Self::ReadAux => "read-aux",
            Self::ReadModuleAux => "read-module-aux",
            Self::ReadFunctions => "read-functions",
            Self::ReadKey => "read-key",
            Self::ReadObject => "read-object-value",
            Self::Checksum => "check-sum",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RdbNotice {
    pub offset: u64,
    pub physical_offset: Option<u64>,
    pub message: String,
}

#[derive(Debug)]
pub struct RdbCheckReport {
    pub state: RdbState,
    pub notices: Vec<RdbNotice>,
}

#[derive(Debug)]
pub struct RdbCheckFailure {
    pub state: Box<RdbState>,
    pub notices: Vec<RdbNotice>,
    pub phase: RdbCheckPhase,
    pub offset: u64,
    pub message: String,
}

pub type RdbCheckResult = Result<RdbCheckReport, RdbCheckFailure>;

pub fn scan_rdb(data: &[u8]) -> RdbCheckResult {
    scan_rdb_at(data, state_now_ms())
}

pub fn scan_rdb_at(data: &[u8], now_ms: i64) -> RdbCheckResult {
    use versal::foundation::io::persistence::rdb::compression::{
        decode_rdb_stream, CompressionErrorKind,
    };
    match decode_rdb_stream(data, false) {
        Ok(None) if !data.is_empty() && b"VCS".starts_with(data) => {
            scan_compressed_failure(data, now_ms, data.len())
        }
        Ok(None) => RdbScanner::new(data, now_ms).scan(),
        Ok(Some(decoded)) => {
            let mut scanner = RdbScanner::new(&decoded.bytes, now_ms);
            scanner.state.physical_offset = Some(decoded.physical_bytes as u64);
            scanner.scan()
        }
        Err(error) if error.kind == CompressionErrorKind::Incompatible => {
            let mut state = RdbState::new();
            state.physical_offset = Some(error.physical_offset as u64);
            Err(RdbCheckFailure {
                state: Box::new(state),
                notices: Vec::new(),
                phase: RdbCheckPhase::Header,
                offset: 0,
                message: error.message().to_string(),
            })
        }
        Err(error) => scan_compressed_failure(data, now_ms, error.physical_offset),
    }
}

fn scan_compressed_failure(data: &[u8], now_ms: i64, physical_offset: usize) -> RdbCheckResult {
    use versal::foundation::io::compression::{DecodeError, StreamDecoder, StreamKind};

    let mut decoder = StreamDecoder::new(StreamKind::Rdb, false);
    let mut logical = Vec::new();
    // Replay only failed inputs. Single-byte refills preserve each decoded
    // block before a later frame error can discard a feed's partial output.
    for byte in data {
        match decoder.feed(std::slice::from_ref(byte), usize::MAX) {
            Ok(bytes) => logical.extend(bytes),
            Err(_) => break,
        }
    }
    // C exposes complete raw bodies before reading their block checksum.
    // Recover that progress for diagnostics only, never after a codec error.
    if decoder.finish() == Err(DecodeError::Truncated) {
        if let Some(block) = truncated_raw_block(data, decoder.physical_bytes()) {
            logical.extend_from_slice(block);
        }
    }
    let mut scanner = RdbScanner::new(&logical, now_ms);
    scanner.state.physical_offset = Some(physical_offset as u64);
    let (phase, message) = if !decoder.is_compressed() {
        (
            RdbCheckPhase::Header,
            "Invalid or unsupported RDB stream envelope",
        )
    } else {
        match scanner.scan_logical_rdb() {
            Ok(()) => (
                RdbCheckPhase::Checksum,
                "Compressed RDB stream did not end cleanly",
            ),
            Err(issue) => (issue.phase, "Corrupt compressed RDB stream"),
        }
    };
    scanner.state.final_offset = scanner.pos as u64;
    Err(RdbCheckFailure {
        offset: scanner.pos as u64,
        state: Box::new(scanner.state),
        notices: scanner.notices,
        phase,
        message: message.to_string(),
    })
}

fn truncated_raw_block(data: &[u8], physical_bytes: usize) -> Option<&[u8]> {
    // Only recover a block after a frame header validated by the decoder.
    if physical_bytes < 14 || data.get(11)? & 0x10 == 0 {
        return None;
    }
    let block = data.get(physical_bytes..)?;
    let info = u32::from_le_bytes(block.get(..4)?.try_into().ok()?);
    let count = (info & 0x7fff_ffff) as usize;
    let block_size = match (data.get(12)? >> 4) & 7 {
        4 => 64 * 1024,
        5 => 256 * 1024,
        6 => 1024 * 1024,
        7 => 4 * 1024 * 1024,
        _ => return None,
    };
    if info & 0x8000_0000 == 0 || count == 0 || count > block_size || block.len() >= 8 + count {
        return None;
    }
    block.get(4..4 + count)
}

struct ScanIssue {
    phase: RdbCheckPhase,
    message: String,
}

struct RdbScanner<'a> {
    data: &'a [u8],
    pos: usize,
    state: RdbState,
    notices: Vec<RdbNotice>,
    dbid: u64,
    expiretime: i64,
    now_ms: i64,
}

impl<'a> RdbScanner<'a> {
    fn new(data: &'a [u8], now_ms: i64) -> Self {
        Self {
            data,
            pos: 0,
            state: RdbState::new(),
            notices: Vec::new(),
            dbid: 0,
            expiretime: -1,
            now_ms,
        }
    }

    fn scan(mut self) -> RdbCheckResult {
        match self.scan_inner() {
            Ok(()) => {
                self.state.final_offset = self.pos as u64;
                Ok(RdbCheckReport {
                    state: self.state,
                    notices: self.notices,
                })
            }
            Err(issue) => {
                self.state.final_offset = self.pos as u64;
                Err(RdbCheckFailure {
                    state: Box::new(self.state),
                    notices: self.notices,
                    phase: issue.phase,
                    offset: self.pos as u64,
                    message: issue.message,
                })
            }
        }
    }

    fn scan_inner(&mut self) -> Result<(), ScanIssue> {
        self.scan_logical_rdb()?;
        if self.state.physical_offset.is_some() && self.pos != self.data.len() {
            return self.fail(
                RdbCheckPhase::Checksum,
                "Compressed RDB stream did not end cleanly",
            );
        }
        Ok(())
    }

    fn scan_logical_rdb(&mut self) -> Result<(), ScanIssue> {
        self.scan_header()?;
        self.scan_opcodes()?;
        self.scan_checksum()
    }

    fn scan_header(&mut self) -> Result<(), ScanIssue> {
        let signature = self.take(9, RdbCheckPhase::Header, "File too short for RDB header")?;
        let is_valkey = signature.starts_with(b"VALKEY");
        let is_redis = signature.starts_with(b"REDIS");
        if !is_valkey && !is_redis {
            return self.fail(
                RdbCheckPhase::Header,
                "Wrong signature: expected 'REDIS' or 'VALKEY'",
            );
        }
        let version_start = if is_valkey { 6 } else { 5 };
        let text = std::str::from_utf8(&signature[version_start..])
            .map_err(|_| self.issue(RdbCheckPhase::Header, "Invalid version string"))?;
        let version = text.parse().map_err(|_| {
            self.issue(
                RdbCheckPhase::Header,
                format!("Invalid RDB version: {}", text),
            )
        })?;
        self.validate_version(version, is_redis)?;
        self.state.rdbver = version;
        Ok(())
    }

    fn validate_version(&mut self, version: u64, is_redis: bool) -> Result<(), ScanIssue> {
        let valid = if is_redis {
            (1..=RDB_FOREIGN_VERSION_MAX).contains(&version)
        } else {
            version > RDB_FOREIGN_VERSION_MAX
        };
        if !valid {
            return self.fail(
                RdbCheckPhase::Header,
                format!("Can't handle RDB format version {}", version),
            );
        }
        if is_foreign_version(version) {
            self.notice(format!("Foreign RDB version {} detected", version));
        } else if version > RDB_VERSION {
            self.notice(format!("Future RDB version {} detected", version));
        }
        Ok(())
    }

    fn scan_opcodes(&mut self) -> Result<(), ScanIssue> {
        loop {
            let opcode =
                self.read_byte(RdbCheckPhase::ReadType, "Unexpected EOF reading RDB file")?;
            if opcode == RDB_OPCODE_EOF {
                return Ok(());
            }
            if self.scan_expiry_opcode(opcode)? || self.scan_structure_opcode(opcode)? {
                continue;
            }
            if self.scan_aux_opcode(opcode)? {
                continue;
            }
            self.scan_object(opcode)?;
        }
    }

    fn scan_expiry_opcode(&mut self, opcode: u8) -> Result<bool, ScanIssue> {
        match opcode {
            RDB_OPCODE_EXPIRETIME => {
                let bytes = self.take(
                    4,
                    RdbCheckPhase::ReadExpire,
                    "Unexpected EOF reading expire time",
                )?;
                self.expiretime = i32::from_be_bytes(bytes.try_into().unwrap()) as i64 * 1000;
            }
            RDB_OPCODE_EXPIRETIME_MS => {
                let bytes = self.take(
                    8,
                    RdbCheckPhase::ReadExpire,
                    "Unexpected EOF reading expire time",
                )?;
                self.expiretime = i64::from_le_bytes(bytes.try_into().unwrap());
            }
            RDB_OPCODE_FREQ => {
                self.read_byte(RdbCheckPhase::ReadType, "Unexpected EOF in FREQ")?;
            }
            RDB_OPCODE_IDLE => {
                self.load_len(RdbCheckPhase::ReadLength, "Unexpected EOF reading IDLE")?;
            }
            _ => return Ok(false),
        }
        Ok(true)
    }

    fn scan_structure_opcode(&mut self, opcode: u8) -> Result<bool, ScanIssue> {
        match opcode {
            RDB_OPCODE_SELECTDB => self.select_database()?,
            RDB_OPCODE_RESIZEDB => {
                self.load_len(RdbCheckPhase::ReadLength, "Unexpected EOF reading RESIZEDB")?;
                self.load_len(RdbCheckPhase::ReadLength, "Unexpected EOF reading RESIZEDB")?;
            }
            RDB_OPCODE_SLOT_INFO => self.load_lengths(
                3,
                RdbCheckPhase::ReadLength,
                "Unexpected EOF reading SLOT INFO",
            )?,
            RDB_OPCODE_SLOT_IMPORT => {
                let job_name = self.load_string(
                    RdbCheckPhase::ReadLength,
                    "Unexpected EOF reading SLOT IMPORT",
                )?;
                if job_name.len() != 40 {
                    return self.fail(
                        RdbCheckPhase::ReadLength,
                        "Invalid slot import job name length in RDB",
                    );
                }
                let (num_slot_ranges, _) = self.load_len(
                    RdbCheckPhase::ReadLength,
                    "Unexpected EOF reading SLOT IMPORT",
                )?;
                if num_slot_ranges == u64::MAX {
                    return self.fail(
                        RdbCheckPhase::ReadLength,
                        "Unexpected EOF reading SLOT IMPORT",
                    );
                }
                for _ in 0..num_slot_ranges {
                    for _ in 0..2 {
                        let (slot, _) = self.load_len(
                            RdbCheckPhase::ReadLength,
                            "Unexpected EOF reading SLOT IMPORT",
                        )?;
                        if slot == u64::MAX {
                            return self.fail(
                                RdbCheckPhase::ReadLength,
                                "Unexpected EOF reading SLOT IMPORT",
                            );
                        }
                    }
                }
            }
            _ => return Ok(false),
        }
        Ok(true)
    }

    fn select_database(&mut self) -> Result<(), ScanIssue> {
        let (dbid, encoded) =
            self.load_len(RdbCheckPhase::ReadLength, "Unexpected EOF reading SELECTDB")?;
        if encoded {
            return self.fail(
                RdbCheckPhase::ReadLength,
                "Encoded length where DB id expected",
            );
        }
        self.dbid = dbid;
        self.state.ensure_db(dbid);
        self.state.databases = self.state.databases.max(dbid);
        self.notice(format!("Selecting DB ID {}", dbid));
        Ok(())
    }

    fn scan_aux_opcode(&mut self, opcode: u8) -> Result<bool, ScanIssue> {
        match opcode {
            RDB_OPCODE_AUX => self.scan_aux()?,
            RDB_OPCODE_MODULE_AUX => self.scan_module_aux()?,
            RDB_OPCODE_FUNCTION_PRE_GA => {
                return self.fail(
                    RdbCheckPhase::ReadFunctions,
                    "Pre-release function format not supported",
                );
            }
            RDB_OPCODE_FUNCTION2 => {
                self.load_string(
                    RdbCheckPhase::ReadFunctions,
                    "Unexpected EOF reading function library",
                )?;
                self.state.functions += 1;
            }
            _ => return Ok(false),
        }
        Ok(true)
    }

    fn scan_aux(&mut self) -> Result<(), ScanIssue> {
        let key = self.load_string(RdbCheckPhase::ReadAux, "Unexpected EOF reading AUX key")?;
        let value = self.load_string(RdbCheckPhase::ReadAux, "Unexpected EOF reading AUX value")?;
        if key.eq_ignore_ascii_case(b"lua") {
            self.state.lua_scripts += 1;
        }
        self.notice(format!(
            "AUX FIELD {} = '{}'",
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&value)
        ));
        Ok(())
    }

    fn scan_module_aux(&mut self) -> Result<(), ScanIssue> {
        self.load_lengths(
            3,
            RdbCheckPhase::ReadModuleAux,
            "Unexpected EOF reading MODULE AUX",
        )?;
        self.load_string(
            RdbCheckPhase::ReadModuleAux,
            "Unexpected EOF reading MODULE AUX value",
        )?;
        Ok(())
    }

    fn scan_object(&mut self, object_type: u8) -> Result<(), ScanIssue> {
        if !is_object_type(object_type) {
            let message = classify_unknown_type(object_type, self.state.rdbver);
            return self.fail(RdbCheckPhase::ReadType, message);
        }
        let key = self.load_string(RdbCheckPhase::ReadKey, "Unexpected EOF reading key")?;
        let value = self.load_object(object_type).ok_or_else(|| {
            self.issue(
                RdbCheckPhase::ReadObject,
                format!(
                    "Error reading key '{}': bad or unsupported value",
                    String::from_utf8_lossy(&key)
                ),
            )
        })?;
        self.record_object(&value);
        Ok(())
    }

    fn load_object(&mut self, object_type: u8) -> Option<Robj> {
        if object_type == 22 {
            load_robj_with_hash_field_expires(self.data, &mut self.pos, object_type, None)
                .map(|(object, _)| object)
        } else {
            load_robj(self.data, &mut self.pos, object_type)
        }
    }

    fn record_object(&mut self, value: &Robj) {
        self.state.keys += 1;
        self.state.ensure_db(self.dbid);
        let index = type_index(value.type_());
        self.state.type_counts[self.dbid as usize][index] += 1;
        if self.expiretime != -1 {
            self.state.expires += 1;
            if self.expiretime < self.now_ms {
                self.state.already_expired += 1;
            }
        }
        self.expiretime = -1;
    }

    fn scan_checksum(&mut self) -> Result<(), ScanIssue> {
        if self.state.rdbver < 5 {
            return Ok(());
        }
        let checksum_start = self.pos;
        let bytes = self.take(
            8,
            RdbCheckPhase::Checksum,
            "Unexpected EOF reading checksum",
        )?;
        let stored = u64::from_le_bytes(bytes.try_into().unwrap());
        if self.state.physical_offset.is_some() {
            self.notice("Logical RDB CRC64 skipped for streaming-compressed input");
            return Ok(());
        }
        let computed = crc64(0, &self.data[..checksum_start]);
        if stored == 0 {
            self.notice("RDB file was saved with checksum disabled: no check performed.");
        } else if stored != computed {
            return self.fail(RdbCheckPhase::Checksum, "RDB CRC error");
        } else {
            self.notice("Checksum OK");
        }
        Ok(())
    }

    fn take(
        &mut self,
        length: usize,
        phase: RdbCheckPhase,
        message: &str,
    ) -> Result<&'a [u8], ScanIssue> {
        let Some(end) = self.pos.checked_add(length) else {
            return self.fail(phase, message);
        };
        let Some(bytes) = self.data.get(self.pos..end) else {
            self.pos = self.data.len();
            return self.fail(phase, message);
        };
        self.pos = end;
        Ok(bytes)
    }

    fn read_byte(&mut self, phase: RdbCheckPhase, message: &str) -> Result<u8, ScanIssue> {
        Ok(self.take(1, phase, message)?[0])
    }

    fn load_len(&mut self, phase: RdbCheckPhase, message: &str) -> Result<(u64, bool), ScanIssue> {
        types::load_len(self.data, &mut self.pos).ok_or_else(|| self.issue(phase, message))
    }

    fn load_lengths(
        &mut self,
        count: usize,
        phase: RdbCheckPhase,
        message: &str,
    ) -> Result<(), ScanIssue> {
        for _ in 0..count {
            self.load_len(phase, message)?;
        }
        Ok(())
    }

    fn load_string(&mut self, phase: RdbCheckPhase, message: &str) -> Result<Vec<u8>, ScanIssue> {
        decoder::load_string(self.data, &mut self.pos).ok_or_else(|| self.issue(phase, message))
    }

    fn notice(&mut self, message: impl Into<String>) {
        self.notices.push(RdbNotice {
            offset: self.pos as u64,
            physical_offset: self.state.physical_offset,
            message: message.into(),
        });
    }

    fn issue(&self, phase: RdbCheckPhase, message: impl Into<String>) -> ScanIssue {
        ScanIssue {
            phase,
            message: message.into(),
        }
    }

    fn fail<T>(&self, phase: RdbCheckPhase, message: impl Into<String>) -> Result<T, ScanIssue> {
        Err(self.issue(phase, message))
    }
}

fn is_foreign_version(version: u64) -> bool {
    (RDB_FOREIGN_VERSION_MIN..=RDB_FOREIGN_VERSION_MAX).contains(&version)
}

fn is_object_type(object_type: u8) -> bool {
    object_type <= 7 || (9..=23).contains(&object_type)
}

fn classify_unknown_type(object_type: u8, version: u64) -> String {
    if is_foreign_version(version)
        && (RDB_FOREIGN_TYPE_MIN..=RDB_FOREIGN_TYPE_MAX).contains(&object_type)
    {
        format!(
            "Unknown object type {} in RDB file with foreign version {}",
            object_type, version
        )
    } else if version > RDB_VERSION {
        format!(
            "Unknown object type {} in RDB file with future version {}",
            object_type, version
        )
    } else {
        format!("Invalid object type: {}", object_type)
    }
}
