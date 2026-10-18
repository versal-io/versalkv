use std::io::{self, Write};

pub(crate) struct FailAfterWrites {
    pub(crate) successful_writes: usize,
    pub(crate) writes: usize,
    pub(crate) bytes: Vec<u8>,
}

impl FailAfterWrites {
    pub(crate) fn new(successful_writes: usize) -> Self {
        Self {
            successful_writes,
            writes: 0,
            bytes: Vec::new(),
        }
    }
}

impl Write for FailAfterWrites {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.writes >= self.successful_writes {
            return Err(io::Error::other("injected RDB write failure"));
        }
        self.writes += 1;
        let written = buf.len().min(7);
        self.bytes.extend_from_slice(&buf[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
