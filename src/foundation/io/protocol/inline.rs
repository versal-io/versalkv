use bytes::Bytes;

use super::ParseError;

pub(crate) fn parse_inline_args(line: &[u8]) -> Result<Vec<Bytes>, ParseError> {
    let mut args = Vec::new();
    let mut cur: Vec<u8> = Vec::new();
    let mut token_started = false;
    let mut in_sgl = false;
    let mut in_dbl = false;
    let mut i = 0;
    while i < line.len() {
        let b = line[i];
        if in_sgl {
            if b == b'\'' {
                in_sgl = false;
            } else if b == b'\\' && line.get(i + 1) == Some(&b'\'') {
                cur.push(b'\'');
                i += 2;
                continue;
            } else {
                cur.push(b);
            }
            i += 1;
            continue;
        }
        if in_dbl {
            if b == b'"' {
                in_dbl = false;
            } else if b == b'\\' {
                let Some(next) = line.get(i + 1).copied() else {
                    return Err(unbalanced_quotes());
                };
                if next == b'x' && i + 3 < line.len() {
                    if let (Some(high), Some(low)) =
                        (hex_value(line[i + 2]), hex_value(line[i + 3]))
                    {
                        cur.push(high * 16 + low);
                        i += 4;
                        continue;
                    }
                }
                cur.push(escaped_double_quote_byte(next));
                i += 2;
                continue;
            } else {
                cur.push(b);
            }
            i += 1;
            continue;
        }
        match b {
            b' ' | b'\t' | b'\n' | b'\r' => {
                if token_started {
                    args.push(Bytes::copy_from_slice(&cur));
                    cur.clear();
                    token_started = false;
                }
            }
            b'\'' => {
                in_sgl = true;
                token_started = true;
            }
            b'"' => {
                in_dbl = true;
                token_started = true;
            }
            _ => {
                token_started = true;
                cur.push(b);
            }
        }
        i += 1;
    }
    if in_sgl || in_dbl {
        return Err(unbalanced_quotes());
    }
    if token_started {
        args.push(Bytes::copy_from_slice(&cur));
    }
    Ok(args)
}

fn unbalanced_quotes() -> ParseError {
    ParseError::Protocol("Protocol error: unbalanced quotes in request".into())
}

fn escaped_double_quote_byte(byte: u8) -> u8 {
    match byte {
        b'n' => b'\n',
        b'r' => b'\r',
        b't' => b'\t',
        b'a' => 0x07,
        b'b' => 0x08,
        b'\\' => b'\\',
        b'"' => b'"',
        b'\'' => b'\'',
        _ => byte,
    }
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}
