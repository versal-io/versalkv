#[cfg(test)]
mod framing_tests {
    use super::{RespParser, RespValue};

    #[test]
    fn decimal_double_cli_preserves_fragmented_and_large_wire_text() {
        for text in [
            "9007199254740993",
            "1e4932",
            "0.00000000000000001",
            "inf",
            "-inf",
            "nan",
        ] {
            let mut parser = RespParser::new();
            parser.feed(format!(",{text}\r").as_bytes());
            assert!(parser.try_parse().unwrap().is_none());
            parser.feed(b"\n");
            let value = parser.try_parse().unwrap().unwrap();
            assert!(matches!(&value, RespValue::Double(value) if value == text));
            assert_eq!(value.to_string_raw(), text);
            assert_eq!(value.to_string_tty(), format!("(double) {text}"));
        }
        for text in ["1e", "1e+", "1tail", "1\0", "0x1"] {
            let mut parser = RespParser::new();
            parser.feed(format!(",{text}\r\n").as_bytes());
            assert!(parser.try_parse().is_err(), "{text}");
        }
    }

    #[test]
    fn bulk_body_waits_for_complete_trailing_crlf_before_next_reply() {
        let mut parser = RespParser::new();
        parser.feed(b"$2\r\nok");
        assert!(parser.try_parse().unwrap().is_none());
        parser.feed(b"\r");
        assert!(parser.try_parse().unwrap().is_none());
        parser.feed(b"\n+OK\r\n");
        assert!(
            matches!(parser.try_parse().unwrap(), Some(RespValue::Bulk(bytes)) if bytes == b"ok")
        );
        assert!(
            matches!(parser.try_parse().unwrap(), Some(RespValue::SimpleString(reply)) if reply == "OK")
        );
    }
}

#[derive(Debug, Clone)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    Bulk(Vec<u8>),
    Null,
    NullArray,
    Array(Vec<RespValue>),
    Boolean(bool),
    Double(String),
    BigNumber(String),
    Push(Vec<RespValue>),
    Verbatim { data: Vec<u8> },
    Map(Vec<(RespValue, RespValue)>),
    Set(Vec<RespValue>),
}

impl RespValue {
    /// Convert to a display-friendly string representation (non-interactive).
    pub fn to_string_raw(&self) -> String {
        match self {
            RespValue::SimpleString(s) => s.clone(),
            RespValue::Error(e) => e.clone(),
            RespValue::Integer(n) => format!("{}", n),
            RespValue::Bulk(b) => match std::str::from_utf8(b) {
                Ok(s) => s.to_string(),
                Err(_) => bytes_to_latin1(b),
            },
            RespValue::Null => String::new(),
            RespValue::NullArray => String::new(),
            RespValue::Array(items) => items
                .iter()
                .map(|v| v.to_string_raw())
                .collect::<Vec<_>>()
                .join("\n"),
            RespValue::Boolean(b) => (if *b { "true" } else { "false" }).to_string(),
            RespValue::Double(v) => v.clone(),
            RespValue::BigNumber(s) => s.clone(),
            RespValue::Push(items) => items
                .iter()
                .map(|v| v.to_string_raw())
                .collect::<Vec<_>>()
                .join("\n"),
            RespValue::Verbatim { data } => std::str::from_utf8(data).unwrap_or("").to_string(),
            RespValue::Map(items) => items
                .iter()
                .map(|(k, v)| format!("{} {}", k.to_string_raw(), v.to_string_raw()))
                .collect::<Vec<_>>()
                .join("\n"),
            RespValue::Set(items) => items
                .iter()
                .map(|v| v.to_string_raw())
                .collect::<Vec<_>>()
                .join("\n"),
        }
    }

    /// Convert to TTY-friendly output (interactive mode).
    pub fn to_string_tty(&self) -> String {
        match self {
            RespValue::SimpleString(s) => s.to_string(),
            RespValue::Error(e) => format!("(error) {}", e),
            RespValue::Integer(n) => format!("(integer) {}", n),
            RespValue::Bulk(b) => {
                let line_count = b.iter().filter(|&&c| c == b'\n').count();
                if line_count > 1 {
                    String::from_utf8_lossy(b).to_string()
                } else {
                    let escaped = escape_bulk_tty_bytes(b);
                    format!("\"{}\"", escaped)
                }
            }
            RespValue::Null => "(nil)".to_string(),
            RespValue::NullArray => "(empty list or set)".to_string(),
            RespValue::Array(items) => {
                let mut s = String::new();
                for (i, item) in items.iter().enumerate() {
                    s.push_str(&format!("{}) {}", i + 1, item.to_string_tty()));
                    if i + 1 < items.len() {
                        s.push('\n');
                    }
                }
                s
            }
            RespValue::Boolean(b) => if *b { "(integer) 1" } else { "(integer) 0" }.to_string(),
            RespValue::Double(v) => format!("(double) {}", v),
            RespValue::BigNumber(s) => s.clone(),
            RespValue::Push(items) => {
                let mut s = String::new();
                for (i, item) in items.iter().enumerate() {
                    s.push_str(&format!("{}) {}", i + 1, item.to_string_tty()));
                    if i + 1 < items.len() {
                        s.push('\n');
                    }
                }
                s
            }
            RespValue::Verbatim { data } => std::str::from_utf8(data).unwrap_or("").to_string(),
            RespValue::Map(items) => items
                .iter()
                .enumerate()
                .map(|(index, (k, v))| {
                    format!(
                        "{}# {} => {}",
                        index + 1,
                        k.to_string_tty(),
                        v.to_string_tty()
                    )
                })
                .collect::<Vec<_>>()
                .join("\n"),
            RespValue::Set(items) => items
                .iter()
                .map(|v| v.to_string_tty())
                .collect::<Vec<_>>()
                .join("\n"),
        }
    }
}

pub fn bytes_to_latin1(b: &[u8]) -> String {
    b.iter().map(|&byte| byte as char).collect()
}

pub fn escape_bulk_tty_bytes(b: &[u8]) -> String {
    let mut out = String::with_capacity(b.len());
    for &byte in b {
        match byte {
            b'\n' => out.push_str("\\n"),
            b'\r' => out.push_str("\\r"),
            b'\t' => out.push_str("\\t"),
            0x07 => out.push_str("\\a"),
            0x08 => out.push_str("\\b"),
            0x0b => out.push_str("\\v"),
            0x0c => out.push_str("\\f"),
            b'\\' => out.push_str("\\\\"),
            b'"' => out.push_str("\\\""),
            byte if byte.is_ascii_control() || byte >= 0x80 => {
                out.push_str(&format!("\\x{:02x}", byte));
            }
            byte => out.push(byte as char),
        }
    }
    out
}

pub struct RespParser {
    buf: Vec<u8>,
    pos: usize,
}

impl RespParser {
    pub fn new() -> Self {
        RespParser {
            buf: Vec::with_capacity(4096),
            pos: 0,
        }
    }

    pub fn feed(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    pub fn try_parse(&mut self) -> Result<Option<RespValue>, String> {
        if self.pos >= self.buf.len() {
            return Ok(None);
        }
        match self.parse_value() {
            Ok(val) => {
                self.compact();
                Ok(Some(val))
            }
            Err(e) if e == "incomplete" => {
                self.pos = 0;
                Ok(None)
            }
            Err(e) => {
                self.pos = 0;
                Err(e)
            }
        }
    }

    fn parse_value(&mut self) -> Result<RespValue, String> {
        if self.pos >= self.buf.len() {
            return Err("incomplete".to_string());
        }
        match self.buf[self.pos] {
            b'+' => self.parse_simple_string(),
            b'-' => self.parse_error(),
            b':' => self.parse_integer(),
            b'$' => self.parse_bulk(),
            b'*' => self.parse_array(),
            b'#' => self.parse_boolean(),
            b',' => self.parse_double(),
            b'(' => self.parse_bignumber(),
            b'>' => self.parse_push(),
            b'=' => self.parse_verbatim(),
            b'%' => self.parse_map(),
            b'~' => self.parse_set(),
            b'_' => {
                self.pos += 1;
                if self.pos + 1 < self.buf.len()
                    && self.buf[self.pos] == b'\r'
                    && self.buf[self.pos + 1] == b'\n'
                {
                    self.pos += 2;
                }
                Ok(RespValue::Null)
            }
            b'!' => {
                self.pos += 1;
                self.parse_bulk_inner()
                    .map(|v| RespValue::Error(String::from_utf8_lossy(&v).to_string()))
            }
            c => Err(format!("Protocol error: unexpected byte '{}'", c as char)),
        }
    }

    fn read_line(&mut self) -> Result<&[u8], String> {
        let start = self.pos;
        let mut i = start;
        while i < self.buf.len() {
            if self.buf[i] == b'\r' && i + 1 < self.buf.len() && self.buf[i + 1] == b'\n' {
                let line = &self.buf[start..i];
                self.pos = i + 2;
                return Ok(line);
            }
            i += 1;
        }
        Err("incomplete".to_string())
    }

    fn parse_simple_string(&mut self) -> Result<RespValue, String> {
        self.pos += 1;
        let line = self.read_line()?;
        Ok(RespValue::SimpleString(
            String::from_utf8_lossy(line).to_string(),
        ))
    }

    fn parse_error(&mut self) -> Result<RespValue, String> {
        self.pos += 1;
        let line = self.read_line()?;
        Ok(RespValue::Error(String::from_utf8_lossy(line).to_string()))
    }

    fn parse_integer(&mut self) -> Result<RespValue, String> {
        self.pos += 1;
        let line = self.read_line()?;
        let s = String::from_utf8_lossy(line);
        let n = s
            .parse::<i64>()
            .map_err(|e| format!("Invalid integer: {}", e))?;
        Ok(RespValue::Integer(n))
    }

    fn parse_bulk(&mut self) -> Result<RespValue, String> {
        self.pos += 1;
        let line = self.read_line()?;
        let s = String::from_utf8_lossy(line);
        let len: i64 = s
            .parse()
            .map_err(|e| format!("Invalid bulk length: {}", e))?;
        if len == -1 {
            return Ok(RespValue::Null);
        }
        self.parse_bulk_body(len as usize)
    }

    fn parse_bulk_inner(&mut self) -> Result<Vec<u8>, String> {
        let line = self.read_line()?;
        let s = String::from_utf8_lossy(line);
        let len: i64 = s
            .parse()
            .map_err(|e| format!("Invalid bulk length: {}", e))?;
        if len == -1 {
            return Ok(Vec::new());
        }
        self.read_exact(len as usize)
    }

    fn parse_bulk_body(&mut self, len: usize) -> Result<RespValue, String> {
        let data = self.read_exact(len)?;
        if self.buf.len().saturating_sub(self.pos) < 2 {
            return Err("incomplete".to_string());
        }
        if &self.buf[self.pos..self.pos + 2] != b"\r\n" {
            return Err("Protocol error: invalid bulk terminator".to_string());
        }
        self.pos += 2;
        Ok(RespValue::Bulk(data))
    }

    fn parse_array(&mut self) -> Result<RespValue, String> {
        self.pos += 1;
        let line = self.read_line()?;
        let s = String::from_utf8_lossy(line);
        let count: i64 = s
            .parse()
            .map_err(|e| format!("Invalid array length: {}", e))?;
        if count == -1 {
            return Ok(RespValue::NullArray);
        }
        let mut items = Vec::with_capacity(count as usize);
        for _ in 0..count {
            items.push(self.parse_value()?);
        }
        Ok(RespValue::Array(items))
    }

    fn parse_boolean(&mut self) -> Result<RespValue, String> {
        self.pos += 1;
        let line = self.read_line()?;
        if !line.is_empty() {
            Ok(RespValue::Boolean(line[0] == b't'))
        } else {
            Err("Invalid boolean".to_string())
        }
    }

    fn parse_double(&mut self) -> Result<RespValue, String> {
        self.pos += 1;
        let line = self.read_line()?;
        let text = std::str::from_utf8(line).map_err(|_| "Invalid double".to_owned())?;
        if text != "nan" {
            versal::foundation::io::protocol::DecimalDoubleText::new(line.to_vec())
                .map_err(|_| "Invalid double".to_owned())?;
        }
        Ok(RespValue::Double(text.to_owned()))
    }

    fn parse_bignumber(&mut self) -> Result<RespValue, String> {
        self.pos += 1;
        let line = self.read_line()?;
        Ok(RespValue::BigNumber(
            String::from_utf8_lossy(line).to_string(),
        ))
    }

    fn parse_push(&mut self) -> Result<RespValue, String> {
        self.pos += 1;
        let line = self.read_line()?;
        let s = String::from_utf8_lossy(line);
        let count: i64 = s
            .parse()
            .map_err(|e| format!("Invalid push count: {}", e))?;
        let mut items = Vec::with_capacity(count as usize);
        for _ in 0..count {
            items.push(self.parse_value()?);
        }
        Ok(RespValue::Push(items))
    }

    fn parse_verbatim(&mut self) -> Result<RespValue, String> {
        self.pos += 1;
        let line = self.read_line()?;
        let s = String::from_utf8_lossy(line);
        let len: i64 = s
            .parse()
            .map_err(|e| format!("Invalid verbatim length: {}", e))?;
        let data = self.read_exact(len as usize)?;
        if self.pos + 1 < self.buf.len()
            && self.buf[self.pos] == b'\r'
            && self.buf[self.pos + 1] == b'\n'
        {
            self.pos += 2;
        }
        let content = if data.len() > 4 {
            data[4..].to_vec()
        } else {
            Vec::new()
        };
        Ok(RespValue::Verbatim { data: content })
    }

    fn parse_map(&mut self) -> Result<RespValue, String> {
        self.pos += 1;
        let line = self.read_line()?;
        let s = String::from_utf8_lossy(line);
        let count: i64 = s
            .parse()
            .map_err(|e| format!("Invalid map length: {}", e))?;
        let mut items = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let key = self.parse_value()?;
            let value = self.parse_value()?;
            items.push((key, value));
        }
        Ok(RespValue::Map(items))
    }

    fn parse_set(&mut self) -> Result<RespValue, String> {
        self.pos += 1;
        let line = self.read_line()?;
        let s = String::from_utf8_lossy(line);
        let count: i64 = s
            .parse()
            .map_err(|e| format!("Invalid set length: {}", e))?;
        let mut items = Vec::with_capacity(count as usize);
        for _ in 0..count {
            items.push(self.parse_value()?);
        }
        Ok(RespValue::Set(items))
    }

    fn read_exact(&mut self, len: usize) -> Result<Vec<u8>, String> {
        if self.pos + len > self.buf.len() {
            return Err("incomplete".to_string());
        }
        let data = self.buf[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Ok(data)
    }

    fn compact(&mut self) {
        if self.pos > 0 {
            self.buf.drain(0..self.pos);
            self.pos = 0;
        }
    }
}
