use super::{StreamID, ValkeyStream};
use bytes::Bytes;

fn append_id(stream: &mut ValkeyStream, ms: u64, seq: u64) -> StreamID {
    stream
        .append(
            StreamID::new(ms, seq),
            vec![(Bytes::from_static(b"f"), Bytes::from_static(b"v"))],
            true,
            false,
            0,
        )
        .unwrap()
}

#[test]
fn approximate_trim_honors_simulated_node_boundaries_and_limits() {
    let mut stream = ValkeyStream::new();
    for ms in 1..=1000 {
        append_id(&mut stream, ms, 0);
    }
    assert_eq!(stream.trim_by_length_limit(555, false, None, 100), 445);
    assert_eq!(stream.len(), 555);

    assert_eq!(stream.trim_by_length_limit(444, true, None, 100), 55);
    assert_eq!(stream.len(), 500);
    assert_eq!(stream.first_id(), Some(StreamID::new(501, 0)));

    assert_eq!(stream.trim_by_length_limit(400, true, None, 100), 100);
    assert_eq!(stream.len(), 400);
    assert_eq!(stream.first_id(), Some(StreamID::new(601, 0)));

    let mut limited = ValkeyStream::new();
    for ms in 1..=101 {
        append_id(&mut limited, ms, 0);
    }
    assert_eq!(limited.trim_by_length_limit(55, true, Some(30), 10), 30);
    assert_eq!(limited.len(), 71);
    append_id(&mut limited, 102, 0);
    assert_eq!(limited.trim_by_length_limit(55, true, Some(30), 10), 10);
    assert_eq!(limited.len(), 62);

    let mut tiny_nodes = ValkeyStream::new();
    for ms in 1..=102 {
        append_id(&mut tiny_nodes, ms, 0);
    }
    assert_eq!(tiny_nodes.trim_by_length_limit(1, true, None, 1), 100);
    assert_eq!(tiny_nodes.len(), 2);

    let mut restarted = ValkeyStream::new();
    for ms in 1..=3 {
        append_id(&mut restarted, ms, 0);
    }
    assert_eq!(restarted.trim_by_length_limit(0, false, None, 100), 3);
    for ms in 4..=20_003 {
        append_id(&mut restarted, ms, 0);
    }
    assert_eq!(restarted.trim_by_length_limit(0, false, None, 100), 20_000);
    for ms in 20_004..=40_003 {
        append_id(&mut restarted, ms, 0);
    }
    assert_eq!(restarted.trim_by_length_limit(2, true, None, 100), 10_000);
    assert_eq!(restarted.len(), 10_000);
}
