use std::io::{Read, Write};

pub fn serialize_resp(args: &[Vec<u8>]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(128);
    buf.extend_from_slice(b"*");
    buf.extend_from_slice(args.len().to_string().as_bytes());
    buf.extend_from_slice(b"\r\n");
    for arg in args {
        buf.extend_from_slice(b"$");
        buf.extend_from_slice(arg.len().to_string().as_bytes());
        buf.extend_from_slice(b"\r\n");
        buf.extend_from_slice(arg);
        buf.extend_from_slice(b"\r\n");
    }
    buf
}

pub fn read_one_reply(stream: &mut impl Read, scratch: &mut Vec<u8>) -> Result<Vec<u8>, String> {
    loop {
        match skip_resp(scratch) {
            Ok(consumed) => {
                let reply = scratch[..consumed].to_vec();
                scratch.drain(..consumed);
                if reply.first() == Some(&b'-') {
                    return Err(String::from_utf8_lossy(&reply).trim().to_string());
                }
                return Ok(reply);
            }
            Err(ParseState::Incomplete) => {
                let mut buf = [0u8; 8192];
                let n = stream.read(&mut buf).map_err(|err| err.to_string())?;
                if n == 0 {
                    return Err("connection closed".to_string());
                }
                scratch.extend_from_slice(&buf[..n]);
            }
            Err(ParseState::Invalid(err)) => return Err(err),
        }
    }
}

pub fn send_command(
    stream: &mut (impl Read + Write),
    scratch: &mut Vec<u8>,
    args: &[Vec<u8>],
) -> Result<Vec<u8>, String> {
    let buf = serialize_resp(args);
    stream.write_all(&buf).map_err(|err| err.to_string())?;
    read_one_reply(stream, scratch)
}

#[derive(Debug)]
enum ParseState {
    Incomplete,
    Invalid(String),
}

fn skip_resp(data: &[u8]) -> Result<usize, ParseState> {
    if data.is_empty() {
        return Err(ParseState::Incomplete);
    }
    match data[0] {
        b'+' | b'-' | b':' | b',' | b'#' => line_end(data),
        b'$' | b'=' | b'(' => skip_bulk(data),
        b'*' | b'>' | b'~' | b'%' => skip_array(data),
        b'_' => line_end(data),
        b'!' => skip_blob_error(data),
        other => Err(ParseState::Invalid(format!(
            "unknown RESP type: {}",
            other as char
        ))),
    }
}

fn line_end(data: &[u8]) -> Result<usize, ParseState> {
    data.windows(2)
        .position(|w| w == b"\r\n")
        .map(|pos| pos + 2)
        .ok_or(ParseState::Incomplete)
}

fn skip_bulk(data: &[u8]) -> Result<usize, ParseState> {
    let header = line_end(data)?;
    let len = parse_len(&data[1..header - 2])?;
    if len < 0 {
        return Ok(header);
    }
    let total = header + len as usize + 2;
    if total <= data.len() {
        Ok(total)
    } else {
        Err(ParseState::Incomplete)
    }
}

fn skip_blob_error(data: &[u8]) -> Result<usize, ParseState> {
    skip_bulk(data)
}

fn skip_array(data: &[u8]) -> Result<usize, ParseState> {
    let header = line_end(data)?;
    let count = parse_len(&data[1..header - 2])?;
    if count < 0 {
        return Ok(header);
    }
    let mut pos = header;
    for _ in 0..count {
        let consumed = skip_resp(&data[pos..])?;
        pos += consumed;
    }
    Ok(pos)
}

fn parse_len(bytes: &[u8]) -> Result<i64, ParseState> {
    let text = std::str::from_utf8(bytes).map_err(|err| ParseState::Invalid(err.to_string()))?;
    text.parse()
        .map_err(|_| ParseState::Invalid(format!("invalid RESP length: {text}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serializes_array_of_bulk_strings() {
        let args = vec![b"PING".to_vec(), b"hello".to_vec()];
        assert_eq!(
            serialize_resp(&args),
            b"*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n".to_vec()
        );
    }

    #[test]
    fn skips_nested_array_reply() {
        let data = b"*2\r\n$3\r\nfoo\r\n:42\r\ntrailing";
        assert_eq!(skip_resp(data).unwrap(), 18);
    }
}
