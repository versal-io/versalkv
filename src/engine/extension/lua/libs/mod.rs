mod bit;
mod cjson;
mod cmsgpack;

pub use bit::setup_bit_table;
pub use cjson::setup_cjson_table;
pub use cmsgpack::setup_cmsgpack_table;

macro_rules! real_args {
    ($args:expr) => {{
        let mut _iter = $args.iter();
        let _first = _iter.next();
        match _first {
            Some(LuaValue::Table(_)) => _iter,
            _ => $args.iter(),
        }
    }};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_escape() {
        use cjson::json_escape;
        assert_eq!(json_escape("hello"), "\"hello\"");
        assert_eq!(json_escape("\"quoted\""), "\"\\\"quoted\\\"\"");
        assert_eq!(json_escape("\\backslash"), "\"\\\\backslash\"");
        assert_eq!(json_escape("line1\nline2"), "\"line1\\nline2\"");
        assert_eq!(json_escape("tab\there"), "\"tab\\there\"");
        assert_eq!(json_escape("return\rcar"), "\"return\\rcar\"");
        assert_eq!(json_escape("form\x0Cfeed"), "\"form\\u000cfeed\"");
        assert_eq!(json_escape("back\x08space"), "\"back\\u0008space\"");
    }
}
