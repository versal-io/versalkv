use super::resp::RespValue;
use super::CliOptions;

pub fn format_resp(val: &RespValue, opts: &CliOptions) -> String {
    if opts.json {
        format_json(val, false)
    } else if opts.quoted_json {
        format_json(val, true)
    } else if opts.csv {
        format_csv(val)
    } else {
        let mut output = val.to_string_raw();
        if opts.raw {
            output.push_str(opts.delimiter.as_deref().unwrap_or("\n"));
        }
        output
    }
}

fn format_csv(value: &RespValue) -> String {
    match value {
        RespValue::SimpleString(text) => {
            format!(
                "\"{}\"",
                super::resp::escape_bulk_tty_bytes(text.as_bytes())
            )
        }
        RespValue::Error(text) => format!(
            "ERROR,\"{}\"",
            super::resp::escape_bulk_tty_bytes(text.as_bytes())
        ),
        RespValue::Bulk(bytes) | RespValue::Verbatim { data: bytes } => {
            format!("\"{}\"", super::resp::escape_bulk_tty_bytes(bytes))
        }
        RespValue::Null | RespValue::NullArray => "NULL".to_string(),
        RespValue::Array(items) | RespValue::Set(items) | RespValue::Push(items) => {
            items.iter().map(format_csv).collect::<Vec<_>>().join(",")
        }
        RespValue::Map(items) => items
            .iter()
            .flat_map(|(key, value)| [format_csv(key), format_csv(value)])
            .collect::<Vec<_>>()
            .join(","),
        RespValue::Boolean(value) => value.to_string(),
        RespValue::Integer(value) => value.to_string(),
        RespValue::Double(value) | RespValue::BigNumber(value) => value.clone(),
    }
}

pub fn format_resp_for_command(val: &RespValue, opts: &CliOptions, _command: &[Vec<u8>]) -> String {
    format_resp(val, opts)
}

fn format_json(val: &RespValue, quoted: bool) -> String {
    match val {
        RespValue::SimpleString(s) => format!("\"{}\"", escape_json(s, quoted)),
        RespValue::Error(e) => format!("\"{}\"", escape_json(e, quoted)),
        RespValue::Integer(n) => n.to_string(),
        RespValue::Bulk(b) => format!("\"{}\"", escape_json_bytes(b, quoted)),
        RespValue::Null => "null".to_string(),
        RespValue::NullArray => "[]".to_string(),
        RespValue::Array(items) => {
            let inner: Vec<String> = items.iter().map(|v| format_json(v, quoted)).collect();
            format!("[{}]", inner.join(","))
        }
        RespValue::Boolean(b) => (if *b { "true" } else { "false" }).to_string(),
        RespValue::Double(v) => {
            if matches!(v.as_str(), "inf" | "-inf" | "nan") {
                format!("\"{v}\"")
            } else if !v.contains(['.', 'e', 'E']) {
                format!("{v}.0")
            } else {
                v.clone()
            }
        }
        RespValue::BigNumber(s) => s.clone(),
        RespValue::Push(items) => {
            let inner: Vec<String> = items.iter().map(|v| format_json(v, quoted)).collect();
            format!("[{}]", inner.join(","))
        }
        RespValue::Verbatim { data } => {
            let s = std::str::from_utf8(data).unwrap_or("");
            format!("\"{}\"", escape_json(s, quoted))
        }
        RespValue::Map(items) => {
            let inner: Vec<String> = items
                .iter()
                .map(|(k, v)| format!("{}:{}", format_json(k, quoted), format_json(v, quoted)))
                .collect();
            format!("{{{}}}", inner.join(","))
        }
        RespValue::Set(items) => {
            let inner: Vec<String> = items.iter().map(|v| format_json(v, quoted)).collect();
            format!("[{}]", inner.join(","))
        }
    }
}

fn escape_json(s: &str, quoted: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\x08' => out.push_str("\\b"),
            '\x0c' => out.push_str("\\f"),
            c if c.is_control() => {
                if quoted {
                    out.push_str(&format!("\\\\x{:02x}", c as u8));
                } else {
                    out.push_str(&format!("\\u{:04x}", c as u32));
                }
            }
            c if (c as u32) > 127 && quoted => {
                let code = c as u32;
                if code <= 0xff {
                    out.push_str(&format!("\\\\x{:02x}", code as u8));
                } else {
                    for b in c.to_string().as_bytes() {
                        out.push_str(&format!("\\\\x{:02x}", b));
                    }
                }
            }
            c => out.push(c),
        }
    }
    out
}

pub(super) fn escape_json_bytes(bytes: &[u8], quoted: bool) -> String {
    if let Ok(value) = std::str::from_utf8(bytes) {
        return if quoted {
            escape_json(value, true)
        } else {
            escape_json_utf8(value)
        };
    }
    let mut out = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        match byte {
            b'"' => out.push_str("\\\""),
            b'\\' => out.push_str("\\\\"),
            b'\n' => out.push_str("\\n"),
            b'\r' => out.push_str("\\r"),
            b'\t' => out.push_str("\\t"),
            0x08 => out.push_str("\\b"),
            0x0c => out.push_str("\\f"),
            byte if byte.is_ascii_control() => {
                if quoted {
                    out.push_str(&format!("\\\\x{byte:02x}"));
                } else {
                    out.push_str(&format!("\\u00{byte:02x}"));
                }
            }
            byte if byte >= 0x80 => {
                if quoted {
                    out.push_str(&format!("\\\\x{byte:02x}"));
                } else {
                    out.push_str(&format!("\\u00{byte:02x}"));
                }
            }
            byte => out.push(byte as char),
        }
    }
    out
}

pub fn json_bulk_bytes(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes.len() + 2);
    out.push(b'"');
    if let Ok(value) = std::str::from_utf8(bytes) {
        for c in value.chars() {
            match c {
                '"' => out.extend_from_slice(b"\\\""),
                '\\' => out.extend_from_slice(b"\\\\"),
                '\n' => out.extend_from_slice(b"\\n"),
                '\r' => out.extend_from_slice(b"\\r"),
                '\t' => out.extend_from_slice(b"\\t"),
                '\x08' => out.extend_from_slice(b"\\b"),
                '\x0c' => out.extend_from_slice(b"\\f"),
                c if c.is_control() => {
                    out.extend_from_slice(format!("\\u{:04x}", c as u32).as_bytes())
                }
                c => out.extend_from_slice(c.to_string().as_bytes()),
            }
        }
    } else {
        for &byte in bytes {
            match byte {
                b'"' => out.extend_from_slice(b"\\\""),
                b'\\' => out.extend_from_slice(b"\\\\"),
                b'\n' => out.extend_from_slice(b"\\n"),
                b'\r' => out.extend_from_slice(b"\\r"),
                b'\t' => out.extend_from_slice(b"\\t"),
                0x08 => out.extend_from_slice(b"\\b"),
                0x0c => out.extend_from_slice(b"\\f"),
                byte if byte.is_ascii_control() => {
                    out.extend_from_slice(format!("\\u00{byte:02x}").as_bytes())
                }
                byte => out.push(byte),
            }
        }
    }
    out.push(b'"');
    out
}

fn escape_json_utf8(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\x08' => out.push_str("\\b"),
            '\x0c' => out.push_str("\\f"),
            c if c.is_control() => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod decimal_tests {
    use super::*;

    #[test]
    fn decimal_double_json_keeps_finite_literals_and_quotes_special_values() {
        for (text, expected) in [
            ("9007199254740993", "9007199254740993.0"),
            ("1e4932", "1e4932"),
            ("0.00000000000000001", "0.00000000000000001"),
            ("inf", "\"inf\""),
            ("-inf", "\"-inf\""),
            ("nan", "\"nan\""),
        ] {
            assert_eq!(
                format_json(&RespValue::Double(text.to_owned()), false),
                expected
            );
            assert_eq!(
                format_json(&RespValue::Double(text.to_owned()), true),
                expected
            );
        }
    }
}
