use bytes::Bytes;

#[path = "inline.rs"]
mod inline;
#[cfg(any(not(feature = "wasm"), test))]
pub(crate) use inline::parse_inline_args;
#[path = "numbers.rs"]
mod numbers;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    Incomplete,
    Protocol(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    DetectType,
    Inline,
    MultiBulkLen { remaining: i64 },
    MultiBulkBody { remaining: i64, bulk_len: usize },
}

const PROT_MAX_BULK: i64 = 512 * 1024 * 1024;
const PROT_INLINE_MAX: usize = 65536;
const PROT_MBULK_COUNT_MAX: usize = 65536;
const PROT_BULK_COUNT_MAX: usize = 65536;
const QUERY_BUFFER_RETAIN_CAPACITY: usize = 16 * 1024;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ParserMemoryUsage {
    pub query_buffer: usize,
    pub partial_argv: usize,
}

impl ParserMemoryUsage {
    pub fn total(self) -> usize {
        self.query_buffer.saturating_add(self.partial_argv)
    }
}

#[derive(Clone)]
pub struct Parser {
    state: State,
    buf: Vec<u8>,
    pos: usize,
    args: Vec<Bytes>,
    pending_mbulk_count: i64,
    cmd_start: usize,
    last_cmd_len: usize,
}

impl Default for Parser {
    fn default() -> Self {
        Self::new()
    }
}

impl Parser {
    pub fn new() -> Self {
        Self {
            state: State::DetectType,
            buf: Vec::with_capacity(4096),
            pos: 0,
            args: Vec::new(),
            pending_mbulk_count: 0,
            cmd_start: 0,
            last_cmd_len: 0,
        }
    }

    pub fn feed(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    pub fn reset(&mut self) {
        self.state = State::DetectType;
        self.buf.clear();
        self.pos = 0;
        self.args.clear();
        self.pending_mbulk_count = 0;
        self.cmd_start = 0;
        self.last_cmd_len = 0;
    }

    pub fn last_cmd_len(&self) -> usize {
        self.last_cmd_len
    }

    pub fn has_pending_data(&self) -> bool {
        self.pos < self.buf.len()
    }

    pub fn has_inflight_request(&self) -> bool {
        self.has_pending_data() || self.state != State::DetectType || !self.args.is_empty()
    }

    pub fn pending_len(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    /// Returns an upper bound for bytes that can contribute to commands not
    /// yet emitted by the parser. An incomplete multibulk keeps consumed
    /// headers and argv bytes in the query buffer until the command completes,
    /// so counting only bytes after `pos` would understate its eventual length.
    pub fn undecoded_len(&self) -> usize {
        let inflight_len = match self.state {
            State::DetectType => self.pending_len(),
            _ => self.buf.len().saturating_sub(self.cmd_start),
        };
        inflight_len.max(self.pending_len())
    }

    pub fn buffer_capacity(&self) -> usize {
        self.buf.capacity()
    }

    pub fn buffer_allocation_bytes(&self) -> usize {
        #[cfg(not(feature = "wasm"))]
        {
            crate::foundation::memory::util_alloc::vec_allocation_bytes(&self.buf)
        }
        #[cfg(feature = "wasm")]
        {
            self.buf.capacity()
        }
    }

    pub fn shrink_idle_buffer(&mut self) {
        if self.has_inflight_request() {
            self.buf.shrink_to(QUERY_BUFFER_RETAIN_CAPACITY);
            return;
        }
        self.buf.clear();
        self.buf.shrink_to(QUERY_BUFFER_RETAIN_CAPACITY);
        self.pos = 0;
        self.cmd_start = 0;
    }

    pub fn query_buffer_capacity_hint(&self) -> usize {
        self.inflight_memory_usage().query_buffer
    }

    pub fn inflight_memory_usage(&self) -> ParserMemoryUsage {
        if !self.has_inflight_request() {
            return ParserMemoryUsage::default();
        }
        let query_buffer = match self.state {
            State::MultiBulkBody { bulk_len, .. } => self.pending_len().max(bulk_len),
            _ if !self.args.is_empty() => self.buf.len(),
            _ => self.pending_len(),
        };
        // C retains the consumed query buffer until an incomplete multibulk is
        // complete and reserves the currently declared bulk payload. Use the
        // logical protocol commitment so Rust Vec growth slack cannot change
        // client eviction behavior.
        let partial_argv = self
            .args
            .capacity()
            .saturating_mul(std::mem::size_of::<Bytes>())
            .saturating_add(
                self.args
                    .iter()
                    .fold(0usize, |total, arg| total.saturating_add(arg.len())),
            );
        ParserMemoryUsage {
            query_buffer,
            partial_argv,
        }
    }

    pub fn try_parse(&mut self) -> Result<Option<Vec<Bytes>>, ParseError> {
        self.try_parse_limited(0, 0)
    }

    pub fn try_parse_limited(
        &mut self,
        max_multibulk: usize,
        max_bulk: usize,
    ) -> Result<Option<Vec<Bytes>>, ParseError> {
        use State::*;
        loop {
            match self.state {
                DetectType => {
                    if self.pos >= self.buf.len() {
                        return Ok(None);
                    }
                    self.cmd_start = self.pos;
                    match self.buf[self.pos] {
                        b'*' => {
                            let start = self.pos + 1;
                            match self.find_crlf(start) {
                                Ok(Some(end)) => {
                                    let line_end = end + 2;
                                    if line_end - self.pos > PROT_MBULK_COUNT_MAX {
                                        self.reset();
                                        return Err(ParseError::Protocol(
                                            "Protocol error: too big mbulk count string".into(),
                                        ));
                                    }
                                    let n = numbers::parse_mbulk_count(&self.buf, start, end)?;
                                    if n == 0 || n < -1 {
                                        self.pos = line_end;
                                        self.compact();
                                        self.state = DetectType;
                                        continue;
                                    }
                                    if n == -1 {
                                        self.pos = line_end;
                                        self.compact();
                                        self.state = DetectType;
                                        continue;
                                    }
                                    if max_multibulk > 0 && n as usize > max_multibulk {
                                        self.reset();
                                        return Err(ParseError::Protocol(
                                            "unauthenticated multibulk length".into(),
                                        ));
                                    }
                                    self.pos = line_end;
                                    self.args.clear();
                                    self.pending_mbulk_count = n;
                                    self.args = Vec::with_capacity(n as usize);
                                    self.state = MultiBulkLen { remaining: n };
                                }
                                Ok(None) => {
                                    if self.buf.len() - self.pos > PROT_INLINE_MAX {
                                        self.reset();
                                        return Err(ParseError::Protocol(
                                            "Protocol error: too big mbulk count string".into(),
                                        ));
                                    }
                                    return Ok(None);
                                }
                                Err(()) => {
                                    self.reset();
                                    return Err(ParseError::Protocol(
                                        "Protocol error: invalid CRLF in request".into(),
                                    ));
                                }
                            }
                        }
                        b'\r' | b'\n' => {
                            self.pos += 1;
                            continue;
                        }
                        _ => {
                            self.args.clear();
                            self.state = Inline;
                        }
                    }
                }
                Inline => {
                    let newline = match self.find_lf(self.pos) {
                        Some(newline) => newline,
                        None => {
                            if self.buf.len() - self.cmd_start > PROT_INLINE_MAX {
                                self.reset();
                                return Err(ParseError::Protocol(
                                    "Protocol error: too big inline request".into(),
                                ));
                            }
                            return Ok(None);
                        }
                    };
                    let end = if newline > self.pos && self.buf[newline - 1] == b'\r' {
                        newline - 1
                    } else {
                        newline
                    };
                    if self.buf[self.pos..end].contains(&0) {
                        self.reset();
                        return Err(ParseError::Protocol(
                            "Protocol error: embedded NUL byte in inline request".into(),
                        ));
                    }
                    self.args = match inline::parse_inline_args(&self.buf[self.pos..end]) {
                        Ok(args) => args,
                        Err(e) => {
                            self.reset();
                            return Err(e);
                        }
                    };
                    self.pos = newline + 1;
                    self.last_cmd_len = self.pos - self.cmd_start;
                    if self.args.is_empty() {
                        self.compact();
                        self.state = DetectType;
                        continue;
                    }
                    let result = std::mem::take(&mut self.args);
                    self.compact();
                    self.state = DetectType;
                    return Ok(Some(result));
                }
                MultiBulkLen { remaining } => {
                    if self.pos >= self.buf.len() {
                        return Ok(None);
                    }
                    if self.buf[self.pos] != b'$' {
                        let got = self.buf[self.pos] as char;
                        self.reset();
                        return Err(ParseError::Protocol(format!(
                            "Protocol error: expected '$', got '{}'",
                            got
                        )));
                    }
                    let start = self.pos + 1;
                    match self.find_crlf(start) {
                        Ok(Some(end)) => {
                            let line_end = end + 2;
                            if line_end - self.pos > PROT_BULK_COUNT_MAX {
                                self.reset();
                                return Err(ParseError::Protocol(
                                    "Protocol error: too big bulk count string".into(),
                                ));
                            }
                            let len = numbers::parse_bulk_len(&self.buf, start, end)?;
                            if max_bulk > 0 && len as usize > max_bulk {
                                self.reset();
                                return Err(ParseError::Protocol(
                                    "unauthenticated bulk length".into(),
                                ));
                            }
                            self.pos = line_end;
                            self.state = MultiBulkBody {
                                remaining,
                                bulk_len: len as usize,
                            };
                        }
                        Ok(None) => {
                            if self.buf.len() - self.pos > PROT_INLINE_MAX {
                                self.reset();
                                return Err(ParseError::Protocol(
                                    "Protocol error: too big bulk count string".into(),
                                ));
                            }
                            return Ok(None);
                        }
                        Err(()) => {
                            self.reset();
                            return Err(ParseError::Protocol(
                                "Protocol error: invalid CRLF in request".into(),
                            ));
                        }
                    }
                }
                MultiBulkBody {
                    remaining,
                    bulk_len,
                } => {
                    let total = self.pos + bulk_len + 2;
                    if total > self.buf.len() {
                        return Ok(None);
                    }
                    if self.buf[self.pos + bulk_len] != b'\r'
                        || self.buf[self.pos + bulk_len + 1] != b'\n'
                    {
                        self.reset();
                        return Err(ParseError::Protocol(
                            "Protocol error: invalid CRLF in request".into(),
                        ));
                    }
                    self.args.push(Bytes::copy_from_slice(
                        &self.buf[self.pos..self.pos + bulk_len],
                    ));
                    self.pos = total;
                    let new_remaining = remaining - 1;
                    if new_remaining > 0 {
                        self.state = MultiBulkLen {
                            remaining: new_remaining,
                        };
                    } else {
                        self.last_cmd_len = self.pos - self.cmd_start;
                        let result = std::mem::take(&mut self.args);
                        self.compact();
                        self.state = DetectType;
                        return Ok(Some(result));
                    }
                }
            }
        }
    }

    fn find_lf(&self, from: usize) -> Option<usize> {
        memchr::memchr(b'\n', &self.buf[from..]).map(|pos| from + pos)
    }

    fn find_crlf(&self, from: usize) -> Result<Option<usize>, ()> {
        let slice = &self.buf[from..];
        match memchr::memchr(b'\r', slice) {
            Some(pos) => {
                if pos + 1 >= slice.len() {
                    return Ok(None);
                }
                if slice[pos + 1] == b'\n' {
                    Ok(Some(from + pos))
                } else {
                    Err(())
                }
            }
            None => Ok(None),
        }
    }

    fn compact(&mut self) {
        if self.pos == 0 {
            return;
        }
        if self.pos == self.buf.len() {
            self.buf.clear();
            if self.buf.capacity() > QUERY_BUFFER_RETAIN_CAPACITY {
                self.buf.shrink_to(QUERY_BUFFER_RETAIN_CAPACITY);
            }
            self.cmd_start = 0;
            self.pos = 0;
            return;
        }
        if self.pos >= 4096 && self.pos * 2 >= self.buf.len() {
            self.buf.drain(0..self.pos);
            self.cmd_start = self.cmd_start.saturating_sub(self.pos);
            self.pos = 0;
        }
    }
}

#[cfg(test)]
#[path = "parser_tests.rs"]
mod parser_tests;
