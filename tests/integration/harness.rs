use crate::common::TestServer;

#[test]
fn default_servers_isolate_persistence_files() {
    let (first_dir, second_dir) = {
        let first = TestServer::start_default();
        let second = TestServer::start_default();
        assert_ne!(first.data_dir(), second.data_dir());

        let mut first_client = first.connect();
        let mut second_client = second.connect();
        assert!(first_client
            .command_str(&[b"SET", b"key", b"first"])
            .contains("OK"));
        assert!(second_client
            .command_str(&[b"SET", b"key", b"second"])
            .contains("OK"));
        assert!(first_client.command_str(&[b"SAVE"]).contains("OK"));
        assert!(second_client.command_str(&[b"SAVE"]).contains("OK"));

        assert!(first.data_dir().join("dump.rdb").exists());
        assert!(second.data_dir().join("dump.rdb").exists());
        (
            first.data_dir().to_path_buf(),
            second.data_dir().to_path_buf(),
        )
    };

    assert!(!first_dir.exists());
    assert!(!second_dir.exists());
}
