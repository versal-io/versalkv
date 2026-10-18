use proptest::prelude::*;
use versal::foundation::io::protocol::parser::Parser;

proptest! {
    #[test]
    fn resp_command_bulk_argument_roundtrips(data in proptest::collection::vec(any::<u8>(), 0..4096)) {
        let mut encoded = format!("*1\r\n${}\r\n", data.len()).into_bytes();
        encoded.extend_from_slice(&data);
        encoded.extend_from_slice(b"\r\n");

        let mut parser = Parser::new();
        parser.feed(&encoded);
        let arguments = parser.try_parse().unwrap().unwrap();

        prop_assert_eq!(arguments.len(), 1);
        prop_assert_eq!(arguments[0].as_ref(), data.as_slice());
    }
}
