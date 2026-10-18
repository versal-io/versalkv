use criterion::{criterion_group, criterion_main, Criterion};
use versal::foundation::io::protocol::parser::Parser;

fn bench_ping_parse(c: &mut Criterion) {
    c.bench_function("resp_parse_ping", |bench| {
        bench.iter(|| {
            let mut parser = Parser::new();
            parser.feed(b"*1\r\n$4\r\nPING\r\n");
            assert!(parser.try_parse().unwrap().is_some());
        });
    });
}

criterion_group!(benches, bench_ping_parse);
criterion_main!(benches);
