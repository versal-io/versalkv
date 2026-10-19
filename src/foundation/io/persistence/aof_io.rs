//! Host-neutral AOF I/O protocol types.
//!
//! These types describe durable AOF operations without binding to any
//! particular backend. The native server completes them inline through an
//! `AofDataStore`; the browser host relays them to a dedicated OPFS worker and
//! replies through the runtime host-completion channel.
//!
//! This module must stay free of `std::fs`, `wasm_bindgen`, DOM, and runtime
//! layer types so it can compile on both the native and `wasm` targets.

use bytes::Bytes;

/// Identifies one of the two fixed manifest slots used for crash-consistent
/// generation commits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ManifestSlot {
    Zero,
    One,
}

impl ManifestSlot {
    /// The other slot, used when committing a new generation.
    pub fn other(self) -> Self {
        match self {
            ManifestSlot::Zero => ManifestSlot::One,
            ManifestSlot::One => ManifestSlot::Zero,
        }
    }

    /// Stable slot index used in file names and metrics.
    pub fn index(self) -> u8 {
        match self {
            ManifestSlot::Zero => 0,
            ManifestSlot::One => 1,
        }
    }
}

/// File names that make up a single AOF generation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RewriteFiles {
    /// RDB preamble base file name for this generation.
    pub base_filename: String,
    /// Incremental AOF file name for this generation.
    pub increment_filename: String,
}

/// Active files discovered from the selected manifest slot on startup.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AofCatalogSnapshot {
    /// Generation number of the selected (highest valid) slot, if any.
    pub generation: u64,
    /// Manifest slot the snapshot was loaded from, if a valid one existed.
    pub slot: Option<ManifestSlot>,
    /// Ordered load inputs: base first, then increment(s).
    pub files: Vec<AofCatalogFile>,
}

/// One file referenced by a manifest payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AofCatalogFile {
    pub filename: String,
    pub sequence: u64,
    pub kind: AofFileKind,
    pub size: u64,
}

/// Role of a catalog file within a generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AofFileKind {
    /// RDB preamble base.
    Base,
    /// Appendable incremental AOF.
    Increment,
}

/// A durable AOF operation issued by the runtime to the storage host.
///
/// Requests from a single runtime form a single-producer ordered stream; each
/// carries a monotonically increasing `sequence` where ordering matters.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AofIoRequest {
    /// Take the single-writer lock for this origin's AOF directory.
    AcquireLock,
    /// Read and validate both manifest slots and return the active catalog.
    LoadCatalog,
    /// Open (creating if needed) the active increment file for appending.
    OpenActiveIncrement { generation: u64, filename: String },
    /// Append encoded RESP frames to the active increment.
    Append { sequence: u64, bytes: Bytes },
    /// Persist previously appended bytes up to `sequence`.
    Flush { sequence: u64 },
    /// Create the base and increment files for a new generation.
    PrepareRewrite {
        generation: u64,
        files: RewriteFiles,
    },
    /// Write one chunk of the new generation's base file.
    WriteRewriteChunk {
        generation: u64,
        offset: u64,
        bytes: Bytes,
    },
    /// Flush and finalize the new generation's files.
    FinishRewriteFiles { generation: u64 },
    /// Atomically publish a new generation by writing an inactive manifest slot.
    CommitManifest { slot: ManifestSlot, envelope: Bytes },
    /// Discard an uncommitted generation's partial files.
    AbortRewrite { generation: u64 },
    /// Delete files no longer referenced by any retained generation.
    GarbageCollect { retained_generations: Vec<u64> },
    /// Flush, release the writer lock, and close all handles.
    Close,
}

/// The successful result of an `AofIoRequest`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AofIoResult {
    LockAcquired,
    CatalogLoaded(AofCatalogSnapshot),
    Opened { size: u64 },
    Appended { sequence: u64, size: u64 },
    Flushed { sequence: u64 },
    RewritePrepared,
    RewriteChunkWritten { next_offset: u64 },
    RewriteFilesFinished,
    ManifestCommitted { generation: u64 },
    RewriteAborted,
    GarbageCollected,
    Closed,
}

/// A durable AOF operation failure reported by the storage host.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AofIoError {
    /// The writer lock is held elsewhere (another tab or worker).
    LockUnavailable,
    /// Storage quota was exceeded.
    QuotaExceeded,
    /// Both manifest slots failed validation while the directory was non-empty.
    CatalogCorrupt,
    /// A referenced file was missing or truncated.
    FileMissing { filename: String },
    /// A lower-level backend failure, carrying a host-provided message.
    Backend { message: String },
}

impl core::fmt::Display for AofIoError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            AofIoError::LockUnavailable => formatter.write_str("AOF writer lock unavailable"),
            AofIoError::QuotaExceeded => formatter.write_str("AOF storage quota exceeded"),
            AofIoError::CatalogCorrupt => formatter.write_str("AOF manifest slots are corrupt"),
            AofIoError::FileMissing { filename } => {
                write!(formatter, "AOF file missing: {filename}")
            }
            AofIoError::Backend { message } => write!(formatter, "AOF backend error: {message}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_slot_other_alternates() {
        assert_eq!(ManifestSlot::Zero.other(), ManifestSlot::One);
        assert_eq!(ManifestSlot::One.other(), ManifestSlot::Zero);
        assert_eq!(ManifestSlot::Zero.other().other(), ManifestSlot::Zero);
    }

    #[test]
    fn manifest_slot_index_is_stable() {
        assert_eq!(ManifestSlot::Zero.index(), 0);
        assert_eq!(ManifestSlot::One.index(), 1);
    }

    #[test]
    fn catalog_snapshot_defaults_to_empty() {
        let snapshot = AofCatalogSnapshot::default();
        assert_eq!(snapshot.generation, 0);
        assert!(snapshot.slot.is_none());
        assert!(snapshot.files.is_empty());
    }

    #[test]
    fn io_error_display_is_descriptive() {
        assert_eq!(
            AofIoError::FileMissing {
                filename: "appendonly.aof.0000000002.incr.aof".to_string(),
            }
            .to_string(),
            "AOF file missing: appendonly.aof.0000000002.incr.aof"
        );
        assert_eq!(
            AofIoError::Backend {
                message: "disk full".to_string(),
            }
            .to_string(),
            "AOF backend error: disk full"
        );
    }
}
