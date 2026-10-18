use criterion::{criterion_group, criterion_main, Criterion};
use versal::foundation::io::store::structure::dict::Dict;

fn bench_lookup(c: &mut Criterion) {
    let mut dict = Dict::new();
    for index in 0..1_000 {
        dict.insert(format!("key:{index}"), index);
    }

    c.bench_function("dict_lookup_1000_keys", |bench| {
        bench.iter(|| {
            for index in 0..1_000 {
                assert_eq!(dict.get(format!("key:{index}").as_str()), Some(&index));
            }
        });
    });
}

criterion_group!(benches, bench_lookup);
criterion_main!(benches);
