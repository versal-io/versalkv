use crate::engine::command::context::Token;

/// IO interest for Sentinel TCP connections.
/// Simple types that don't depend on the concrete polling backend.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoInterest {
    Readable,
    ReadWritable,
}

/// Trait abstracting TCP IO for Sentinel link management.
/// This trait allows `SentinelLinkManager` (L2 engine layer) to perform
/// TCP connections and read/write operations without depending on a concrete
/// runtime transport. Readiness is owned and dispatched by the L5 runtime.
pub trait SentinelIo {
    /// Connect to a TCP address.
    /// `addr` is a string like "127.0.0.1:6379".
    fn connect_tcp(&mut self, addr: &str) -> Result<Token, String>;

    /// Set the interest for a given token.
    fn set_interest(&mut self, token: Token, interest: IoInterest) -> Result<(), String>;

    /// Remove a connection identified by token.
    fn remove(&mut self, token: Token) -> Result<(), String>;

    /// Check if a connection had an error during connect.
    /// Returns `Ok(None)` if the connection succeeded, `Ok(Some(error))` if
    /// there was a connect error, or `Err(msg)` if the token is invalid.
    fn take_connect_error(&mut self, token: Token) -> Result<Option<String>, String>;

    /// Read data from a connection.
    /// Returns `std::io::Result<usize>` to preserve `WouldBlock` semantics.
    fn read_stream(&mut self, token: Token, buf: &mut [u8]) -> std::io::Result<usize>;

    /// Write data to a connection.
    /// Returns `std::io::Result<usize>` to preserve `WouldBlock` semantics.
    fn write_stream(&mut self, token: Token, buf: &[u8]) -> std::io::Result<usize>;
}
