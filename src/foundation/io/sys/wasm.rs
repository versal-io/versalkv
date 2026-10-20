#[path = "wasm/rand.rs"]
pub mod rand;
#[path = "wasm/time.rs"]
pub mod time;

// Minimal `console.log` binding so the shared code's `server_log!` calls surface
// in the browser devtools. The native `sys` module exports `server_log!` from
// `sys/log.rs`; that module is absent on wasm, so this target supplies a
// level-tagged equivalent routed through `console.log`.
//
// The `wasm` feature can be exercised on a native host (the `make wasm-check`
// library tests run on the build machine, not in a browser), where calling a
// wasm-bindgen import panics. Guard the actual console call on the real
// `wasm32` target and degrade to a no-op elsewhere.
#[cfg(target_arch = "wasm32")]
#[wasm_bindgen::prelude::wasm_bindgen]
extern "C" {
    #[wasm_bindgen::prelude::wasm_bindgen(js_namespace = console, js_name = log)]
    fn console_log(message: &str);
}

/// Emit a server log line to the browser console. Message formatting mirrors the
/// native logger's level prefixes so log output reads the same across hosts. On
/// non-`wasm32` hosts (library tests) this is a no-op.
pub fn log_line(level: &str, message: &str) {
    #[cfg(target_arch = "wasm32")]
    console_log(&format!("[{level}] {message}"));
    #[cfg(not(target_arch = "wasm32"))]
    {
        let _ = (level, message);
    }
}

/// Browser counterpart of the native `server_log!` macro. Accepts the same
/// level tags (`debug`, `verbose`, `notice`, `warning`) and format arguments,
/// routing every line through `console.log`.
#[macro_export]
macro_rules! server_log {
    (debug, $($arg:tt)*) => {
        $crate::foundation::io::sys::log_line("debug", &format!($($arg)*))
    };
    (verbose, $($arg:tt)*) => {
        $crate::foundation::io::sys::log_line("verbose", &format!($($arg)*))
    };
    (notice, $($arg:tt)*) => {
        $crate::foundation::io::sys::log_line("notice", &format!($($arg)*))
    };
    (warning, $($arg:tt)*) => {
        $crate::foundation::io::sys::log_line("warning", &format!($($arg)*))
    };
}
