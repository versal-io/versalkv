//! Non-destructive Sentinel configuration save-capability checks.

use std::ffi::{CString, OsStr};
use std::fs::File;
use std::io;
#[cfg(not(target_os = "aix"))]
use std::os::fd::AsRawFd;
use std::os::fd::FromRawFd;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

pub(crate) fn probe_config_save(path: &Path) -> io::Result<()> {
    if path.as_os_str().is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Sentinel needs config file on disk to save state. Exiting...",
        ));
    }
    let config_path = CString::new(path.as_os_str().as_bytes()).map_err(|error| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "Sentinel config file path {} is invalid: {error}. Exiting...",
                path.display()
            ),
        )
    })?;
    // SAFETY: config_path is a valid NUL-terminated path, and access only
    // queries permissions using the real UID/GID without modifying the file.
    if unsafe { libc::access(config_path.as_ptr(), libc::W_OK) } == -1 {
        let error = io::Error::last_os_error();
        return Err(io::Error::new(
            error.kind(),
            format!(
                "Sentinel config file {} is not writable: {error}. Exiting...",
                path.display()
            ),
        ));
    }

    const TEMP_SUFFIX: &[u8] = b".XXXXXX";
    if config_path
        .as_bytes()
        .len()
        .saturating_add(TEMP_SUFFIX.len())
        >= libc::PATH_MAX as usize
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "Sentinel config file path {} is too long. Exiting...",
                path.display()
            ),
        ));
    }
    let directory = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let mut template = config_path.as_bytes().to_vec();
    template.extend_from_slice(TEMP_SUFFIX);
    template.push(0);
    #[cfg(target_os = "linux")]
    // SAFETY: template is writable, NUL-terminated, and ends in six X bytes.
    // mkostemp creates a new exclusive file and only modifies those X bytes.
    let fd = unsafe { libc::mkostemp(template.as_mut_ptr().cast(), libc::O_CLOEXEC) };
    #[cfg(not(target_os = "linux"))]
    // SAFETY: template is writable, NUL-terminated, and ends in six X bytes.
    // mkstemp creates a new exclusive file and only modifies those X bytes.
    let fd = unsafe { libc::mkstemp(template.as_mut_ptr().cast()) };
    if fd == -1 {
        let error = io::Error::last_os_error();
        return Err(io::Error::new(
            error.kind(),
            format!(
                "Sentinel config dir {} is not writable: {error}. Exiting...",
                directory.display()
            ),
        ));
    }
    // SAFETY: successful mkostemp/mkstemp returned a new owned descriptor.
    drop(unsafe { File::from_raw_fd(fd) });
    let probe_path = Path::new(OsStr::from_bytes(&template[..template.len() - 1]));
    std::fs::remove_file(probe_path).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "Sentinel config probe file {} cannot be removed: {error}. Exiting...",
                probe_path.display()
            ),
        )
    })?;
    sync_directory(directory).map_err(|error| {
        io::Error::new(
            error.kind(),
            format!(
                "Sentinel config dir {} cannot be synced: {error}. Exiting...",
                directory.display()
            ),
        )
    })
}

#[cfg(target_os = "aix")]
fn sync_directory(_directory: &Path) -> io::Result<()> {
    // Match fsyncFileDir: AIX cannot synchronize directories.
    Ok(())
}

#[cfg(not(target_os = "aix"))]
fn sync_directory(directory: &Path) -> io::Result<()> {
    let file = match File::open(directory) {
        Ok(file) => file,
        Err(error) if error.raw_os_error() == Some(libc::EISDIR) => return Ok(()),
        Err(error) => return Err(error),
    };
    #[cfg(target_os = "linux")]
    // SAFETY: file owns a live directory descriptor borrowed for fdatasync.
    let result = unsafe { libc::fdatasync(file.as_raw_fd()) };
    #[cfg(target_os = "macos")]
    // SAFETY: file owns a live descriptor; F_FULLFSYNC takes no extra argument.
    let result = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_FULLFSYNC) };
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    // SAFETY: file owns a live directory descriptor borrowed for fsync.
    let result = unsafe { libc::fsync(file.as_raw_fd()) };
    if result == -1 {
        let error = io::Error::last_os_error();
        if !matches!(error.raw_os_error(), Some(libc::EBADF | libc::EINVAL)) {
            return Err(error);
        }
    }
    Ok(())
}
