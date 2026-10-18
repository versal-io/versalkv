use std::io::{self, IoSlice, Read, Write};
use std::sync::Arc;

/// An owned duplicate of a registered stream for use by an I/O worker.
/// The original stream remains registered with the main-thread poller. Both
/// descriptors refer to the same nonblocking socket, but ownership of this
/// handle can move between the runtime and one worker without shared mutable
/// access to the transport slab.
pub enum ThreadIoHandle {
    Tcp(std::net::TcpStream),
    Unix(std::os::unix::net::UnixStream),
}

impl Read for ThreadIoHandle {
    fn read(&mut self, dst: &mut [u8]) -> io::Result<usize> {
        match self {
            Self::Tcp(stream) => stream.read(dst),
            Self::Unix(stream) => stream.read(dst),
        }
    }
}

impl Write for ThreadIoHandle {
    fn write(&mut self, src: &[u8]) -> io::Result<usize> {
        match self {
            Self::Tcp(stream) => stream.write(src),
            Self::Unix(stream) => stream.write(src),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn write_vectored(&mut self, src: &[IoSlice<'_>]) -> io::Result<usize> {
        match self {
            Self::Tcp(stream) => stream.write_vectored(src),
            Self::Unix(stream) => stream.write_vectored(src),
        }
    }
}

pub(crate) trait TransportWake: Send + Sync {
    fn wake(&self) -> io::Result<()>;
}

/// Backend-neutral handle used by I/O workers to wake the main poll loop.
#[derive(Clone)]
pub struct TransportWakeHandle(Arc<dyn TransportWake>);

impl TransportWakeHandle {
    pub(crate) fn new(wake: Arc<dyn TransportWake>) -> Self {
        Self(wake)
    }

    pub fn wake(&self) -> io::Result<()> {
        self.0.wake()
    }
}
