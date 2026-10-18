#[cfg(not(miri))]
use std::os::raw::{c_char, c_int};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::OnceLock;

static ARGV_START: AtomicUsize = AtomicUsize::new(0);
static ARGV_END: AtomicUsize = AtomicUsize::new(0);
static ARGV0: OnceLock<String> = OnceLock::new();

/// # Safety
#[used]
#[cfg(not(miri))]
#[cfg_attr(target_os = "linux", link_section = ".init_array")]
static CAPTURE_ARGV: extern "C" fn(c_int, *const *const c_char, *const *const c_char) =
    capture_argv;

#[cfg(not(miri))]
extern "C" fn capture_argv(argc: c_int, argv: *const *const c_char, envp: *const *const c_char) {
    if argv.is_null() || argc <= 0 {
        return;
    }
    // SAFETY: argv contains argc C string pointers and envp is null or a
    // null-terminated pointer array. This block only reads pointers and lengths.
    unsafe {
        let start = *argv.offset(0);
        if start.is_null() {
            return;
        }
        let mut end = start;
        for i in 0..argc as isize {
            let p = *argv.offset(i);
            if p.is_null() {
                break;
            }
            if p == end {
                end = end.add(libc::strlen(p) + 1);
            }
        }
        if !envp.is_null() {
            let mut i = 0isize;
            loop {
                let p = *envp.offset(i);
                if p.is_null() {
                    break;
                }
                if p == end {
                    end = end.add(libc::strlen(p) + 1);
                }
                i += 1;
            }
        }
        ARGV_START.store(start as usize, Ordering::Relaxed);
        ARGV_END.store(end as usize, Ordering::Relaxed);
        let cstr = std::ffi::CStr::from_ptr(start);
        let _ = ARGV0.set(cstr.to_string_lossy().into_owned());
    }
}

pub fn set_proc_title(title: &str) {
    let start = ARGV_START.load(Ordering::Relaxed);
    let end = ARGV_END.load(Ordering::Relaxed);
    if start == 0 || end <= start {
        return;
    }
    let capacity = end - start;
    let bytes = title.as_bytes();
    let n = bytes.len().min(capacity - 1);
    // SAFETY: capture_argv recorded [start, end) as contiguous writable process
    // argument storage. The copy is bounded and the remainder is zero-filled.
    unsafe {
        let dst = start as *mut u8;
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), dst, n);
        std::ptr::write_bytes(dst.add(n), 0, capacity - n);
    }
}

pub fn argv0() -> Option<String> {
    ARGV0.get().cloned()
}

#[derive(Default)]
pub struct ProcTitleVars {
    pub title: String,
    pub listen_addr: String,
    pub server_mode: String,
    pub config_file: String,
    pub port: String,
    pub tls_port: String,
    pub unixsocket: String,
}

pub fn expand_proc_title_template(template: &str, vars: &ProcTitleVars) -> Option<String> {
    let mut out = String::new();
    let mut chars = template.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '{' {
            out.push(c);
            continue;
        }
        if chars.peek() == Some(&'{') {
            chars.next();
            out.push('{');
            continue;
        }
        let mut name = String::new();
        let mut closed = false;
        for nc in chars.by_ref() {
            if nc == '}' {
                closed = true;
                break;
            }
            name.push(nc);
        }
        if !closed || name.is_empty() {
            return None;
        }
        match name.as_str() {
            "title" => out.push_str(&vars.title),
            "listen-addr" => out.push_str(&vars.listen_addr),
            "server-mode" => out.push_str(&vars.server_mode),
            "config-file" => out.push_str(&vars.config_file),
            "port" => out.push_str(&vars.port),
            "tls-port" => out.push_str(&vars.tls_port),
            "unixsocket" => out.push_str(&vars.unixsocket),
            _ => return None,
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expands_each_variable_distinctly() {
        let vars = ProcTitleVars {
            title: "bin".into(),
            listen_addr: "127.0.0.1:6379".into(),
            server_mode: String::new(),
            config_file: "cfg".into(),
            port: "6379".into(),
            tls_port: "0".into(),
            unixsocket: "/tmp/s".into(),
        };
        let out = expand_proc_title_template(
            "TEST {title} {listen-addr} {port} {tls-port} {unixsocket} {config-file}",
            &vars,
        )
        .unwrap();
        assert_eq!(out, "TEST bin 127.0.0.1:6379 6379 0 /tmp/s cfg");
    }

    #[test]
    fn rejects_unknown_variable() {
        assert!(expand_proc_title_template("{invalid-var}", &ProcTitleVars::default()).is_none());
    }
}
