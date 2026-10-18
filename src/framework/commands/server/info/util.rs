use std::fmt::Display;

pub(super) fn normalize_section(s: &str) -> String {
    s.to_ascii_lowercase().replace('-', "_")
}

pub(super) fn emit_line(s: &mut String, key: &str, value: impl Display) {
    s.push_str(&format!("{}:{}\r\n", key, value));
}

pub(super) fn format_bytes(n: usize) -> String {
    const UNITS: &[&str] = &["B", "K", "M", "G", "T"];
    let mut val = n as f64;
    let mut idx = 0;
    while val >= 1024.0 && idx < UNITS.len() - 1 {
        val /= 1024.0;
        idx += 1;
    }
    if idx == 0 {
        format!("{}{}", n, UNITS[idx])
    } else {
        format!("{:.2}{}", val, UNITS[idx])
    }
}

pub(super) fn total_system_memory() -> usize {
    crate::foundation::io::sys::os::total_system_memory().unwrap_or(0)
}
