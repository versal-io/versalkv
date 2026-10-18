pub fn encode_command(args: &[&[u8]]) -> Vec<u8> {
    let mut frame = Vec::new();
    frame.extend_from_slice(b"*");
    frame.extend_from_slice(args.len().to_string().as_bytes());
    frame.extend_from_slice(b"\r\n");
    for arg in args {
        frame.extend_from_slice(b"$");
        frame.extend_from_slice(arg.len().to_string().as_bytes());
        frame.extend_from_slice(b"\r\n");
        frame.extend_from_slice(arg);
        frame.extend_from_slice(b"\r\n");
    }
    frame
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth2_is_encoded_as_auth_with_three_arguments() {
        let frame = encode_command(&[b"AUTH".as_slice(), b"user".as_slice(), b"secret".as_slice()]);
        assert_eq!(frame, b"*3\r\n$4\r\nAUTH\r\n$4\r\nuser\r\n$6\r\nsecret\r\n");
    }

    #[test]
    fn restore_batch_commands_are_encoded_without_concatenation_loss() {
        let frame = encode_command(&[
            b"RESTORE".as_slice(),
            b"key".as_slice(),
            b"1".as_slice(),
            b"payload".as_slice(),
            b"REPLACE".as_slice(),
        ]);
        assert_eq!(
            frame,
            b"*5\r\n$7\r\nRESTORE\r\n$3\r\nkey\r\n$1\r\n1\r\n$7\r\npayload\r\n$7\r\nREPLACE\r\n"
        );
    }
}
