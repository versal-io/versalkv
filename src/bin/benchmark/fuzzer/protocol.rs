use std::io::Read;
use std::net::TcpStream;

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum ReplyType {
    Status,
    Error,
    Integer,
    Bulk,
    Array,
    Nil,
}

pub(super) struct Reply {
    pub reply_type: ReplyType,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum ReplyError {
    Io,
    Malformed,
}

pub(super) fn serialize_resp(args: &[Vec<u8>]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(256);
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

pub(super) fn serialize_resp_slices(args: &[&[u8]]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(256);
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

fn find_crlf(data: &[u8]) -> Option<usize> {
    data.windows(2).position(|w| w == b"\r\n")
}

fn skip_reply(data: &[u8]) -> Result<usize, ()> {
    if data.is_empty() {
        return Err(());
    }
    match data[0] {
        b'+' | b'-' | b':' | b',' | b'#' => find_crlf(data).map(|p| p + 2).ok_or(()),
        b'_' => find_crlf(data).map(|p| p + 2).ok_or(()),
        b'!' => find_crlf(data).map(|p| p + 2).ok_or(()),
        b'$' => {
            let crlf = find_crlf(data).ok_or(())?;
            let len_str = std::str::from_utf8(&data[1..crlf]).map_err(|_| ())?;
            let len: i64 = len_str.parse().map_err(|_| ())?;
            if len == -1 {
                return Ok(crlf + 2);
            }
            let total = crlf + 2 + len as usize + 2;
            if total > data.len() {
                return Err(());
            }
            Ok(total)
        }
        b'*' | b'>' | b'~' => {
            let crlf = find_crlf(data).ok_or(())?;
            let count_str = std::str::from_utf8(&data[1..crlf]).map_err(|_| ())?;
            let count: i64 = count_str.parse().map_err(|_| ())?;
            if count == -1 {
                return Ok(crlf + 2);
            }
            let mut pos = crlf + 2;
            for _ in 0..count {
                if pos >= data.len() {
                    return Err(());
                }
                let consumed = skip_reply(&data[pos..])?;
                pos += consumed;
            }
            Ok(pos)
        }
        b'%' => {
            let crlf = find_crlf(data).ok_or(())?;
            let count_str = std::str::from_utf8(&data[1..crlf]).map_err(|_| ())?;
            let count: i64 = count_str.parse().map_err(|_| ())?;
            let mut pos = crlf + 2;
            for _ in 0..count {
                if pos >= data.len() {
                    return Err(());
                }
                let k = skip_reply(&data[pos..])?;
                pos += k;
                if pos >= data.len() {
                    return Err(());
                }
                let v = skip_reply(&data[pos..])?;
                pos += v;
            }
            Ok(pos)
        }
        b'=' => {
            let crlf = find_crlf(data).ok_or(())?;
            let len_str = std::str::from_utf8(&data[1..crlf]).map_err(|_| ())?;
            let len: i64 = len_str.parse().map_err(|_| ())?;
            let total = crlf + 2 + len as usize + 2;
            if total > data.len() {
                return Err(());
            }
            Ok(total)
        }
        b'(' => {
            let crlf = find_crlf(data).ok_or(())?;
            Ok(crlf + 2)
        }
        b'|' => {
            let crlf = find_crlf(data).ok_or(())?;
            let count_str = std::str::from_utf8(&data[1..crlf]).map_err(|_| ())?;
            let count: i64 = count_str.parse().map_err(|_| ())?;
            let mut pos = crlf + 2;
            for _ in 0..count {
                if pos >= data.len() {
                    return Err(());
                }
                let k = skip_reply(&data[pos..])?;
                pos += k;
                if pos >= data.len() {
                    return Err(());
                }
                let v = skip_reply(&data[pos..])?;
                pos += v;
            }
            let inner = skip_reply(&data[pos..])?;
            Ok(pos + inner)
        }
        _ => Err(()),
    }
}

fn is_valid_resp_start(b: u8) -> bool {
    matches!(
        b,
        b'+' | b'-'
            | b':'
            | b','
            | b'#'
            | b'_'
            | b'!'
            | b'$'
            | b'*'
            | b'>'
            | b'~'
            | b'%'
            | b'='
            | b'('
            | b'|'
    )
}

pub(super) fn read_reply(
    stream: &mut TcpStream,
    read_buf: &mut Vec<u8>,
    read_pos: &mut usize,
) -> Result<Reply, ReplyError> {
    loop {
        if *read_pos > 0 {
            match skip_reply(&read_buf[..*read_pos]) {
                Ok(consumed) => {
                    let reply_type = match read_buf[0] {
                        b'+' => ReplyType::Status,
                        b'-' => ReplyType::Error,
                        b':' => ReplyType::Integer,
                        b'$' | b'=' | b'(' => ReplyType::Bulk,
                        b'*' | b'>' | b'~' | b'%' => ReplyType::Array,
                        b'_' | b'!' => ReplyType::Nil,
                        _ => ReplyType::Status,
                    };
                    let payload = read_buf[..consumed].to_vec();
                    read_buf.copy_within(consumed..*read_pos, 0);
                    *read_pos -= consumed;
                    return Ok(Reply {
                        reply_type,
                        data: payload,
                    });
                }
                Err(()) => {
                    if *read_pos > 0 && !is_valid_resp_start(read_buf[0]) {
                        return Err(ReplyError::Malformed);
                    }
                    if *read_pos > 1024 * 1024 {
                        return Err(ReplyError::Malformed);
                    }
                }
            }
        }
        if read_buf.len() < *read_pos + 4096 {
            read_buf.resize(read_buf.len().max(4096) * 2, 0);
        }
        match stream.read(&mut read_buf[*read_pos..]) {
            Ok(0) => return Err(ReplyError::Io),
            Ok(n) => *read_pos += n,
            Err(_) => return Err(ReplyError::Io),
        }
    }
}
