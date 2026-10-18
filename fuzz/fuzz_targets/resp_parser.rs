#![no_main]

use libfuzzer_sys::fuzz_target;
use versal::foundation::io::protocol::parser::Parser;

fuzz_target!(|data: &[u8]| {
    let mut parser = Parser::new();
    parser.feed(data);
    let _ = parser.try_parse();
});
