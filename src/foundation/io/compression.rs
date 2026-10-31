//! Incremental Valkey compressed streams, independent of socket ownership.

use bytes::{Buf, BytesMut};
use lz4_flex::frame::{BlockMode, BlockSize, FrameEncoder, FrameInfo};
use std::io::Write;
use xxhash_rust::xxh32::{xxh32, Xxh32};

pub const REPLICATION_BATCH_BYTES: usize = 1024 * 1024;
const MAGIC: &[u8; 4] = b"\x04\x22\x4d\x18";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamKind {
    Rdb = 1,
    Replication = 2,
}

#[derive(Debug)]
pub struct StreamEncoder {
    encoder: FrameEncoder<Vec<u8>>,
}

impl StreamEncoder {
    pub fn new(kind: StreamKind, block_checksum: bool, content_checksum: bool) -> Self {
        let info = FrameInfo::new()
            .block_mode(BlockMode::Linked)
            .block_size(BlockSize::Max64KB)
            .block_checksums(block_checksum)
            .content_checksum(content_checksum);
        Self {
            encoder: FrameEncoder::with_frame_info(
                info,
                vec![b'V', b'C', b'S', 1, 1, 0, kind as u8],
            ),
        }
    }

    pub fn flush_batch(&mut self, input: &[u8]) -> std::io::Result<Vec<u8>> {
        self.encoder.write_all(input)?;
        self.encoder.flush()?;
        Ok(std::mem::take(self.encoder.get_mut()))
    }

    pub fn finish(self) -> std::io::Result<Vec<u8>> {
        self.encoder.finish().map_err(std::io::Error::other)
    }

    pub fn memory_bytes(&self) -> usize {
        // FrameEncoder retains input/history, a compression table and output.
        256 * 1024 + self.encoder.get_ref().capacity()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DecodeError {
    Incompatible,
    Corrupt,
    Truncated,
}

#[derive(Clone, Copy)]
enum DecodeState {
    Probe,
    Plain,
    Header,
    Block,
    End,
    Done,
}

pub struct StreamDecoder {
    kind: StreamKind,
    skip_checksums: bool,
    state: DecodeState,
    input: BytesMut,
    output: Vec<u8>,
    output_pos: usize,
    dictionary: Vec<u8>,
    flags: u8,
    block_size: usize,
    content_size: Option<u64>,
    decoded_size: u64,
    checksum: Xxh32,
    physical_bytes: usize,
    is_compressed: bool,
    error: Option<DecodeError>,
}

impl StreamDecoder {
    pub fn new(kind: StreamKind, skip_checksums: bool) -> Self {
        Self {
            kind,
            skip_checksums,
            state: DecodeState::Probe,
            input: BytesMut::new(),
            output: Vec::new(),
            output_pos: 0,
            dictionary: Vec::new(),
            flags: 0,
            block_size: 0,
            content_size: None,
            decoded_size: 0,
            checksum: Xxh32::new(0),
            physical_bytes: 0,
            is_compressed: false,
            error: None,
        }
    }

    pub fn feed(&mut self, input: &[u8], budget: usize) -> Result<Vec<u8>, DecodeError> {
        if let Some(error) = self.error {
            return Err(error);
        }
        self.input.extend_from_slice(input);
        let result = self.drain(budget);
        if let Err(error) = result {
            self.error = Some(error);
        }
        result
    }

    fn consume(&mut self, count: usize) {
        self.input.advance(count);
        self.physical_bytes += count;
    }

    fn drain(&mut self, budget: usize) -> Result<Vec<u8>, DecodeError> {
        let mut result = Vec::new();
        while result.len() < budget {
            if self.output_pos < self.output.len() {
                let count = (budget - result.len()).min(self.output.len() - self.output_pos);
                result.extend_from_slice(&self.output[self.output_pos..self.output_pos + count]);
                self.output_pos += count;
                if self.output_pos == self.output.len() {
                    self.output.clear();
                    self.output_pos = 0;
                }
                continue;
            }
            match self.state {
                DecodeState::Probe => {
                    let count = self.input.len().min(3);
                    if count == 0 {
                        break;
                    }
                    if self.input[..count] != b"VCS"[..count] {
                        self.state = DecodeState::Plain;
                        continue;
                    }
                    if self.input.len() < 7 {
                        break;
                    }
                    if self.input[3..7] != [1, 1, 0, self.kind as u8] {
                        return Err(DecodeError::Incompatible);
                    }
                    self.consume(7);
                    self.is_compressed = true;
                    self.state = DecodeState::Header;
                }
                DecodeState::Plain => {
                    let count = self.input.len().min(budget - result.len());
                    if count == 0 {
                        break;
                    }
                    result.extend_from_slice(&self.input[..count]);
                    self.consume(count);
                }
                DecodeState::Header => {
                    if self.input.len() < 6 {
                        break;
                    }
                    if &self.input[..4] != MAGIC {
                        return Err(DecodeError::Corrupt);
                    }
                    let flags = self.input[4];
                    let descriptor = self.input[5];
                    if flags & 0xc2 != 0x40 || descriptor & 0x8f != 0 {
                        return Err(DecodeError::Corrupt);
                    }
                    if flags & 1 != 0 {
                        return Err(DecodeError::Incompatible);
                    }
                    let block_size = match (descriptor >> 4) & 7 {
                        4 => 64 * 1024,
                        5 => 256 * 1024,
                        6 => 1024 * 1024,
                        7 => 4 * 1024 * 1024,
                        _ => return Err(DecodeError::Corrupt),
                    };
                    let header_len = if flags & 8 != 0 { 15 } else { 7 };
                    if self.input.len() < header_len {
                        break;
                    }
                    if self.input[header_len - 1]
                        != (xxh32(&self.input[4..header_len - 1], 0) >> 8) as u8
                    {
                        return Err(DecodeError::Corrupt);
                    }
                    self.content_size = (flags & 8 != 0)
                        .then(|| u64::from_le_bytes(self.input[6..14].try_into().unwrap()));
                    self.flags = flags;
                    self.block_size = block_size;
                    self.dictionary.clear();
                    self.decoded_size = 0;
                    self.checksum = Xxh32::new(0);
                    self.consume(header_len);
                    self.state = DecodeState::Block;
                }
                DecodeState::Block => {
                    if self.input.len() < 4 {
                        break;
                    }
                    let info = u32::from_le_bytes(self.input[..4].try_into().unwrap());
                    if info == 0 {
                        self.consume(4);
                        self.state = DecodeState::End;
                        continue;
                    }
                    let count = (info & 0x7fff_ffff) as usize;
                    if count == 0 || count > self.block_size {
                        return Err(DecodeError::Corrupt);
                    }
                    let checksum_bytes = if self.flags & 0x10 != 0 { 4 } else { 0 };
                    if self.input.len() < 4 + count + checksum_bytes {
                        break;
                    }
                    let block = &self.input[4..4 + count];
                    if checksum_bytes != 0 && !self.skip_checksums {
                        let expected = u32::from_le_bytes(
                            self.input[4 + count..8 + count].try_into().unwrap(),
                        );
                        if xxh32(block, 0) != expected {
                            return Err(DecodeError::Corrupt);
                        }
                    }
                    if info & 0x8000_0000 != 0 {
                        self.output.extend_from_slice(block);
                    } else {
                        self.output.resize(self.block_size, 0);
                        let dictionary = if self.flags & 0x20 == 0 {
                            self.dictionary.as_slice()
                        } else {
                            &[]
                        };
                        let decoded = lz4_flex::block::decompress_into_with_dict(
                            block,
                            &mut self.output,
                            dictionary,
                        )
                        .map_err(|_| DecodeError::Corrupt)?;
                        self.output.truncate(decoded);
                    }
                    self.checksum.update(&self.output);
                    self.decoded_size += self.output.len() as u64;
                    if self.flags & 0x20 == 0 {
                        let keep: usize = 64 * 1024;
                        let old = keep
                            .saturating_sub(self.output.len())
                            .min(self.dictionary.len());
                        self.dictionary.drain(..self.dictionary.len() - old);
                        self.dictionary.extend_from_slice(
                            &self.output[self.output.len().saturating_sub(keep)..],
                        );
                    }
                    self.consume(4 + count + checksum_bytes);
                }
                DecodeState::End => {
                    if self.flags & 4 != 0 {
                        if self.input.len() < 4 {
                            break;
                        }
                        let expected = u32::from_le_bytes(self.input[..4].try_into().unwrap());
                        if !self.skip_checksums && self.checksum.digest() != expected {
                            return Err(DecodeError::Corrupt);
                        }
                        self.consume(4);
                    }
                    if self
                        .content_size
                        .is_some_and(|size| size != self.decoded_size)
                    {
                        return Err(DecodeError::Corrupt);
                    }
                    self.state = if self.kind == StreamKind::Replication {
                        DecodeState::Header
                    } else {
                        DecodeState::Done
                    };
                }
                DecodeState::Done => break,
            }
        }
        Ok(result)
    }

    /// Only ready work requires another turn; a socket fragment requires input.
    pub fn has_pending_decode(&self) -> bool {
        if self.output_pos < self.output.len() {
            return true;
        }
        match self.state {
            DecodeState::Probe => {
                !self.input.is_empty()
                    && (self.input.len() >= 7
                        || !b"VCS".starts_with(&self.input[..self.input.len().min(3)]))
            }
            DecodeState::Plain => !self.input.is_empty(),
            DecodeState::Header => {
                self.input.len() >= 7 && (self.input[4] & 8 == 0 || self.input.len() >= 15)
            }
            DecodeState::Block => {
                if self.input.len() < 4 {
                    return false;
                }
                let info = u32::from_le_bytes(self.input[..4].try_into().unwrap());
                let count = (info & 0x7fff_ffff) as usize;
                info == 0
                    || count > self.block_size
                    || self.input.len() >= 4 + count + if self.flags & 0x10 != 0 { 4 } else { 0 }
            }
            DecodeState::End => self.flags & 4 == 0 || self.input.len() >= 4,
            DecodeState::Done => false,
        }
    }

    pub fn is_compressed(&self) -> bool {
        self.is_compressed
    }

    pub fn is_plaintext(&self) -> bool {
        matches!(self.state, DecodeState::Plain)
    }
    pub fn is_plain(&self) -> bool {
        matches!(self.state, DecodeState::Plain)
    }
    pub fn is_done(&self) -> bool {
        matches!(self.state, DecodeState::Done)
    }
    pub fn physical_bytes(&self) -> usize {
        self.physical_bytes
    }
    pub fn memory_bytes(&self) -> usize {
        self.input.capacity() + self.output.capacity() + self.dictionary.capacity()
    }
    pub fn finish(&self) -> Result<(), DecodeError> {
        if let Some(error) = self.error {
            return Err(error);
        }
        if self.is_done() || self.is_plain() {
            Ok(())
        } else {
            Err(DecodeError::Truncated)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    #[test]
    fn incremental_linked_stream_flushes_without_finishing() {
        let payload: Vec<u8> = (0..180_000).map(|i| (i % 251) as u8).collect();
        let mut encoder = StreamEncoder::new(StreamKind::Replication, true, false);
        let mut decoder = StreamDecoder::new(StreamKind::Replication, false);
        for chunk in payload.chunks(30_000) {
            let wire = encoder.flush_batch(chunk).unwrap();
            let mut output = decoder.feed(&wire, 31).unwrap();
            while decoder.has_pending_decode() {
                output.extend(decoder.feed(&[], 31).unwrap());
            }
            assert_eq!(output, chunk);
        }
    }

    #[test]
    fn fragmented_rdb_frame_preserves_physical_endpoint() {
        let payload = vec![b'x'; 180_000];
        let mut encoder = StreamEncoder::new(StreamKind::Rdb, true, true);
        let mut wire = encoder.flush_batch(&payload).unwrap();
        wire.extend(encoder.finish().unwrap());
        let length = wire.len();
        wire.extend_from_slice(b"*1\r\n$4\r\nPING\r\n");
        let mut decoder = StreamDecoder::new(StreamKind::Rdb, false);
        let mut output = Vec::new();
        for fragment in wire.chunks(3) {
            output.extend(decoder.feed(fragment, 17).unwrap());
            while decoder.has_pending_decode() {
                output.extend(decoder.feed(&[], 17).unwrap());
            }
        }
        assert_eq!(output, payload);
        assert_eq!(decoder.physical_bytes(), length);
        decoder.finish().unwrap();
    }

    #[test]
    fn multiple_replication_frames_use_only_one_envelope() {
        let mut wire = Vec::new();
        for (index, chunk) in [b"first".as_slice(), b"second"].into_iter().enumerate() {
            let mut encoder = StreamEncoder::new(StreamKind::Replication, true, false);
            let mut frame = encoder.flush_batch(chunk).unwrap();
            frame.extend(encoder.finish().unwrap());
            wire.extend_from_slice(if index == 0 { &frame } else { &frame[7..] });
        }
        let mut decoder = StreamDecoder::new(StreamKind::Replication, false);
        let mut output = decoder.feed(&wire, 2).unwrap();
        while decoder.has_pending_decode() {
            output.extend(decoder.feed(&[], 2).unwrap());
        }
        assert_eq!(output, b"firstsecond");
        let mut standard = lz4_flex::frame::FrameDecoder::new(&wire[7..]);
        let mut first = Vec::new();
        standard.read_to_end(&mut first).unwrap();
        assert!(first.starts_with(b"first"));
    }

    #[test]
    fn plain_probe_is_replayed_and_invalid_envelopes_fail() {
        let mut plain = StreamDecoder::new(StreamKind::Replication, false);
        assert!(plain.feed(b"V", 2).unwrap().is_empty());
        assert_eq!(plain.feed(b"alkey", 2).unwrap(), b"Va");
        assert_eq!(plain.feed(&[], 99).unwrap(), b"lkey");
        for index in 3..7 {
            let mut wire = b"VCS\x01\x01\x00\x02".to_vec();
            wire[index] ^= 1;
            assert_eq!(
                StreamDecoder::new(StreamKind::Replication, false).feed(&wire, 99),
                Err(DecodeError::Incompatible)
            );
        }
    }
}
