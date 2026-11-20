//! Build the unchanged Lua VM from the pinned Valkey submodule.

use std::env;
use std::path::{Path, PathBuf};

pub enum Version {
    Lua51,
}

pub use Version::Lua51;

#[derive(Default)]
pub struct Build;

impl Build {
    pub fn new() -> Self {
        Self
    }

    pub fn build(&self, version: Version) -> Artifacts {
        match version {
            Version::Lua51 => {}
        }
        let source = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../thirdparty/valkey/deps/lua/src")
            .canonicalize()
            .expect("The pinned Valkey Lua submodule must be initialized");
        // This provider is only called by Cargo's mlua-sys build script.
        let target = env::var("TARGET").expect("Cargo must provide TARGET");
        let output = PathBuf::from(env::var_os("OUT_DIR").expect("Cargo must provide OUT_DIR"));
        let mut compiler = cc::Build::new();
        compiler
            .warnings(false)
            .cargo_metadata(false)
            .include(&source)
            .out_dir(&output)
            .flag_if_supported("-fno-common");
        if target.contains("linux") || target.ends_with("bsd") || target.contains("apple-darwin") {
            compiler.define("LUA_USE_LINUX", None);
        } else if target.contains("apple-ios") {
            compiler.define("LUA_USE_POSIX", None);
        } else if target.contains("windows") {
            compiler.define("LUA_USE_WINDOWS", None);
            if target.contains("msvc") {
                compiler.define("strncasecmp", "_strnicmp");
            }
        } else {
            panic!("Unsupported native Lua target: {target}");
        }
        if cfg!(debug_assertions) {
            compiler.define("LUA_USE_APICHECK", None).debug(true);
        }
        // Match the library objects in the pinned Lua Makefile; exclude tools.
        for name in [
            "lapi",
            "lcode",
            "ldebug",
            "ldo",
            "ldump",
            "lfunc",
            "lgc",
            "llex",
            "lmem",
            "lobject",
            "lopcodes",
            "lparser",
            "lstate",
            "lstring",
            "ltable",
            "ltm",
            "lundump",
            "lvm",
            "lzio",
            "strbuf",
            "fpconv",
            "lauxlib",
            "lbaselib",
            "ldblib",
            "liolib",
            "lmathlib",
            "loslib",
            "ltablib",
            "lstrlib",
            "loadlib",
            "linit",
            "lua_cjson",
            "lua_struct",
            "lua_cmsgpack",
            "lua_bit",
        ] {
            compiler.file(source.join(format!("{name}.c")));
        }
        // mlua-sys owns the build script, so explicitly track these external
        // sources as well as headers and the provider when the gitlink changes.
        println!("cargo:rerun-if-changed={}", source.display());
        println!(
            "cargo:rerun-if-changed={}",
            source.join("../../../src/solarisfixes.h").display()
        );
        println!("cargo:rerun-if-changed={}", env!("CARGO_MANIFEST_DIR"));
        compiler.compile("lua5.1");
        Artifacts { output }
    }
}

pub struct Artifacts {
    output: PathBuf,
}

impl Artifacts {
    pub fn print_cargo_metadata(&self) {
        println!("cargo:rustc-link-search=native={}", self.output.display());
        println!("cargo:rustc-link-lib=static:-bundle=lua5.1");
    }
}
