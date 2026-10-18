//! Foundation layer split by abstractness.
//! `memory` contains pure in-memory types and helpers; `io` contains byte,
//! storage, protocol, and syscall-facing utilities.

pub mod io;
pub mod memory;
