//! Software long-double arithmetic and exact fixed-decimal formatting.

mod format;
mod parse;

pub use format::format_long_double_human;
pub use parse::{parse_long_double, NumericParseError};

use rustc_apfloat::{Float, Round, StatusAnd};

cfg_if::cfg_if! {
    if #[cfg(any(target_arch = "wasm32", target_arch = "wasm64"))] {
        pub type LongDouble = rustc_apfloat::ieee::Double;
    } else if #[cfg(all(target_os = "linux", target_arch = "aarch64"))] {
        pub type LongDouble = rustc_apfloat::ieee::Quad;
    } else if #[cfg(any(
        all(target_os = "linux", any(target_arch = "x86", target_arch = "x86_64")),
        all(target_os = "macos", target_arch = "x86_64")
    ))] {
        pub type LongDouble = rustc_apfloat::ieee::X87DoubleExtended;
    } else if #[cfg(any(
        all(target_os = "macos", target_arch = "aarch64"),
        all(target_os = "windows", target_env = "msvc",
            any(target_arch = "x86", target_arch = "x86_64", target_arch = "aarch64"))
    ))] {
        pub type LongDouble = rustc_apfloat::ieee::Double;
    } else {
        compile_error!("Unsupported default C long double ABI");
    }
}

pub fn add_long_double(left: LongDouble, right: LongDouble) -> StatusAnd<LongDouble> {
    left.add_r(right, Round::NearestTiesToEven)
}

pub fn sub_long_double(left: LongDouble, right: LongDouble) -> StatusAnd<LongDouble> {
    left.sub_r(right, Round::NearestTiesToEven)
}
