use std::io::{self, Read};

const MAX_RESP_REPLY_LENGTH: usize = 64 * 1024 * 1024;

#[derive(Debug, PartialEq, Eq)]
pub enum TargetReply {
    Ok,
    Error(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RetryPhase {
    PreRestore,
}

pub fn read_reply<R: Read>(reader: &mut R) -> io::Result<TargetReply> {
    let prefix = read_byte(reader)?;
    let line = read_line(reader)?;
    match prefix {
        b'+' if line == b"OK" => Ok(TargetReply::Ok),
        b'+' => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unexpected RESP simple string",
        )),
        b'-' => Ok(TargetReply::Error(
            String::from_utf8_lossy(&line).into_owned(),
        )),
        b'$' => {
            let len = parse_length(&line)?;
            if len >= 0 {
                read_bulk_body(reader, len)?;
            }
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected RESP bulk string",
            ))
        }
        b'*' | b'~' | b'>' => {
            let count = parse_length(&line)?;
            if count > MAX_RESP_REPLY_LENGTH as i64 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "RESP collection exceeds maximum length",
                ));
            }
            if count >= 0 {
                for _ in 0..count {
                    consume_reply(reader)?;
                }
            }
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected RESP collection",
            ))
        }
        b'%' | b'|' => {
            let count = parse_length(&line)?;
            if count > (MAX_RESP_REPLY_LENGTH / 2) as i64 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "RESP map exceeds maximum length",
                ));
            }
            if count >= 0 {
                let elements = count.checked_mul(2).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid RESP collection length")
                })?;
                for _ in 0..elements {
                    consume_reply(reader)?;
                }
            }
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected RESP map",
            ))
        }
        b':' | b'_' => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unexpected RESP reply type",
        )),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid RESP reply type",
        )),
    }
}

fn read_byte<R: Read>(reader: &mut R) -> io::Result<u8> {
    let mut byte = [0u8; 1];
    reader.read_exact(&mut byte)?;
    Ok(byte[0])
}

fn read_line<R: Read>(reader: &mut R) -> io::Result<Vec<u8>> {
    let mut line = Vec::new();
    loop {
        let byte = read_byte(reader)?;
        if byte == b'\r' {
            if read_byte(reader)? == b'\n' {
                return Ok(line);
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid RESP line ending",
            ));
        }
        line.push(byte);
    }
}

fn read_bulk_body<R: Read>(reader: &mut R, len: i64) -> io::Result<()> {
    let len = usize::try_from(len).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid RESP bulk string length",
        )
    })?;
    if len > MAX_RESP_REPLY_LENGTH {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "RESP bulk string exceeds maximum length",
        ));
    }
    let total = len.checked_add(2).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid RESP bulk string length",
        )
    })?;
    let mut buf = [0u8; 8192];
    let mut remaining = total;
    while remaining > 0 {
        let chunk = remaining.min(buf.len());
        reader.read_exact(&mut buf[..chunk])?;
        remaining -= chunk;
    }
    Ok(())
}

fn consume_reply<R: Read>(reader: &mut R) -> io::Result<()> {
    let prefix = read_byte(reader)?;
    let line = read_line(reader)?;
    match prefix {
        b'+' | b'-' | b':' | b'_' => Ok(()),
        b'$' => {
            let len = parse_length(&line)?;
            if len >= 0 {
                read_bulk_body(reader, len)?;
            }
            Ok(())
        }
        b'*' | b'~' | b'>' => {
            let count = parse_length(&line)?;
            if count > MAX_RESP_REPLY_LENGTH as i64 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "RESP collection exceeds maximum length",
                ));
            }
            if count >= 0 {
                for _ in 0..count {
                    consume_reply(reader)?;
                }
            }
            Ok(())
        }
        b'%' | b'|' => {
            let count = parse_length(&line)?;
            if count > (MAX_RESP_REPLY_LENGTH / 2) as i64 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "RESP map exceeds maximum length",
                ));
            }
            if count >= 0 {
                let elements = count.checked_mul(2).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid RESP collection length")
                })?;
                for _ in 0..elements {
                    consume_reply(reader)?;
                }
            }
            Ok(())
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid RESP reply type",
        )),
    }
}

fn parse_length(line: &[u8]) -> io::Result<i64> {
    std::str::from_utf8(line)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid RESP length"))?
        .parse()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid RESP length"))
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;

    #[test]
    fn target_error_preserves_message() {
        let mut input = Cursor::new(b"-BUSYKEY Target key name already exists.\r\n");
        assert_eq!(
            read_reply(&mut input).unwrap(),
            TargetReply::Error("BUSYKEY Target key name already exists.".into())
        );
    }

    #[test]
    fn reads_each_reply_in_a_batch() {
        let mut input = Cursor::new(b"+OK\r\n-BUSYKEY Target key name already exists.\r\n+OK\r\n");
        assert_eq!(read_reply(&mut input).unwrap(), TargetReply::Ok);
        assert_eq!(
            read_reply(&mut input).unwrap(),
            TargetReply::Error("BUSYKEY Target key name already exists.".into())
        );
        assert_eq!(read_reply(&mut input).unwrap(), TargetReply::Ok);
    }

    #[test]
    fn only_exact_ok_simple_string_is_success() {
        let mut input = Cursor::new(
            b"+PONG\r\n:1\r\n$2\r\nOK\r\n*1\r\n+OK\r\n%1\r\n+field\r\n+value\r\n+OK\r\n",
        );
        for _ in 0..5 {
            assert_eq!(
                read_reply(&mut input).unwrap_err().kind(),
                std::io::ErrorKind::InvalidData
            );
        }
    }

    #[test]
    fn reads_resp3_containers_before_rejecting_them() {
        let mut input = Cursor::new(b"%1\r\n+field\r\n+value\r\n|1\r\n+meta\r\n+value\r\n+OK\r\n");
        assert_eq!(
            read_reply(&mut input).unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );
        assert_eq!(
            read_reply(&mut input).unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );
        assert_eq!(read_reply(&mut input).unwrap(), TargetReply::Ok);
    }

    #[test]
    fn rejects_oversized_bulk_and_collection_lengths() {
        let oversized = format!("${}\r\n", MAX_RESP_REPLY_LENGTH + 1);
        let mut input = Cursor::new(oversized.into_bytes());
        assert_eq!(
            read_reply(&mut input).unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );

        let oversized = format!("*{}\r\n", MAX_RESP_REPLY_LENGTH + 1);
        let mut input = Cursor::new(oversized.into_bytes());
        assert_eq!(
            read_reply(&mut input).unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );

        let oversized = format!("%{}\r\n", (MAX_RESP_REPLY_LENGTH / 2) + 1);
        let mut input = Cursor::new(oversized.into_bytes());
        assert_eq!(
            read_reply(&mut input).unwrap_err().kind(),
            std::io::ErrorKind::InvalidData
        );
    }
}
