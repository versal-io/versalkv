use bytes::{Bytes, BytesMut};
use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;
use versal::foundation::io::protocol::parser::Parser;
use versal::foundation::io::protocol::serializer::{serialize, serialize_command};
use versal::foundation::io::protocol::Value;

fn bench_ping_parse(c: &mut Criterion) {
    c.bench_function("resp_parse_ping", |bench| {
        bench.iter(|| {
            let mut parser = Parser::new();
            parser.feed(b"*1\r\n$4\r\nPING\r\n");
            assert!(parser.try_parse().unwrap().is_some());
        });
    });
}

fn encode_command(parts: &[&[u8]]) -> Vec<u8> {
    let mut buf = BytesMut::new();
    serialize_command(parts, &mut buf);
    buf.to_vec()
}

fn bench_set_parse(c: &mut Criterion) {
    let value = vec![b'x'; 1024];
    let payload = encode_command(&[b"SET", b"key:000001", &value]);

    c.bench_function("resp_parse_set_1kb", |bench| {
        bench.iter(|| {
            let mut parser = Parser::new();
            parser.feed(black_box(&payload));
            black_box(parser.try_parse().unwrap().unwrap());
        });
    });
}

fn bench_pipeline_parse(c: &mut Criterion) {
    let mut payload = Vec::new();
    for index in 0..64 {
        let key = format!("key:{index}");
        let value = format!("value:{index}");
        payload.extend(encode_command(&[b"SET", key.as_bytes(), value.as_bytes()]));
    }

    c.bench_function("resp_parse_pipeline_64_set", |bench| {
        bench.iter(|| {
            let mut parser = Parser::new();
            parser.feed(black_box(&payload));
            let mut count = 0;
            while let Some(args) = parser.try_parse().unwrap() {
                black_box(args);
                count += 1;
            }
            assert_eq!(count, 64);
        });
    });
}

fn bench_mset_parse(c: &mut Criterion) {
    let owned: Vec<Vec<u8>> = std::iter::once(b"MSET".to_vec())
        .chain((0..200).map(|index| format!("field:{index}").into_bytes()))
        .collect();
    let parts: Vec<&[u8]> = owned.iter().map(Vec::as_slice).collect();
    let payload = encode_command(&parts);

    c.bench_function("resp_parse_mset_200_args", |bench| {
        bench.iter(|| {
            let mut parser = Parser::new();
            parser.feed(black_box(&payload));
            black_box(parser.try_parse().unwrap().unwrap());
        });
    });
}

fn bench_inline_parse(c: &mut Criterion) {
    c.bench_function("resp_parse_inline_set", |bench| {
        bench.iter(|| {
            let mut parser = Parser::new();
            parser.feed(black_box(b"SET key:000001 some-value\r\n"));
            black_box(parser.try_parse().unwrap().unwrap());
        });
    });
}

fn bench_serialize(c: &mut Criterion) {
    let array = Value::Array(
        (0..100)
            .map(|index| Value::Bulk(Bytes::from(format!("member:{index}"))))
            .collect(),
    );
    c.bench_function("resp_serialize_array_100_bulk", |bench| {
        let mut buf = BytesMut::with_capacity(4096);
        bench.iter(|| {
            buf.clear();
            serialize(black_box(&array), &mut buf);
            black_box(buf.len());
        });
    });

    let integers = Value::Array((0..100).map(|index| Value::Integer(index * 7919)).collect());
    c.bench_function("resp_serialize_array_100_integers", |bench| {
        let mut buf = BytesMut::with_capacity(4096);
        bench.iter(|| {
            buf.clear();
            serialize(black_box(&integers), &mut buf);
            black_box(buf.len());
        });
    });

    let map = Value::Map(
        (0..50)
            .map(|index| {
                (
                    Value::Bulk(Bytes::from(format!("field:{index}"))),
                    Value::Double(index as f64 * 1.5),
                )
            })
            .collect(),
    );
    c.bench_function("resp_serialize_map_50_doubles", |bench| {
        let mut buf = BytesMut::with_capacity(4096);
        bench.iter(|| {
            buf.clear();
            serialize(black_box(&map), &mut buf);
            black_box(buf.len());
        });
    });
}

criterion_group!(
    benches,
    bench_ping_parse,
    bench_set_parse,
    bench_pipeline_parse,
    bench_mset_parse,
    bench_inline_parse,
    bench_serialize
);
criterion_main!(benches);
