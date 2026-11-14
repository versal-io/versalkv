use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use std::hint::black_box;
use versal::foundation::io::store::structure::hash::HashHt;
use versal::foundation::io::store::structure::list::ListValue;
use versal::foundation::io::store::structure::set::{IntSet, SetEncoding, ValkeySet};
use versal::foundation::io::store::structure::zset::ZSetSl;
use versal::foundation::memory::value::{HashOps, ListOps, SetOps, ZSetOps};

const SMALL: usize = 64;
const LARGE: usize = 1_000;

fn member(index: usize) -> Vec<u8> {
    format!("m{index:06}").into_bytes()
}

fn members(n: usize) -> Vec<Vec<u8>> {
    (0..n).map(member).collect()
}

fn build_zset(items: &[Vec<u8>]) -> ZSetSl {
    let mut zset = ZSetSl::new();
    for (index, item) in items.iter().enumerate() {
        let _ = zset.add(item, index as f64);
    }
    zset
}

fn build_hash(items: &[Vec<u8>]) -> HashHt {
    let mut hash = HashHt::new();
    for item in items {
        let _ = hash.set(item, item);
    }
    hash
}

fn build_list(items: &[Vec<u8>]) -> ListValue {
    let mut list = ListValue::new();
    for item in items {
        let _ = list.push_back(item);
    }
    list
}

fn bench_zset(c: &mut Criterion) {
    for n in [SMALL, LARGE] {
        let items = members(n);

        c.bench_function(&format!("zset_add_{n}"), |bench| {
            bench.iter(|| black_box(build_zset(black_box(&items))));
        });

        let zset = build_zset(&items);
        c.bench_function(&format!("zset_score_{n}"), |bench| {
            bench.iter(|| {
                for item in &items {
                    black_box(zset.score(black_box(item)));
                }
            });
        });

        c.bench_function(&format!("zset_remove_all_{n}"), |bench| {
            bench.iter_batched(
                || zset.clone(),
                |mut zset| {
                    for item in &items {
                        let _ = black_box(zset.remove(item));
                    }
                    zset
                },
                BatchSize::SmallInput,
            );
        });
    }
}

fn bench_hash(c: &mut Criterion) {
    for n in [SMALL, LARGE] {
        let items = members(n);

        c.bench_function(&format!("hash_set_{n}"), |bench| {
            bench.iter(|| black_box(build_hash(black_box(&items))));
        });

        let hash = build_hash(&items);
        c.bench_function(&format!("hash_get_{n}"), |bench| {
            bench.iter(|| {
                for item in &items {
                    black_box(hash.get(black_box(item)));
                }
            });
        });
    }
}

fn bench_list(c: &mut Criterion) {
    let items = members(LARGE);

    c.bench_function(&format!("list_push_back_{LARGE}"), |bench| {
        bench.iter(|| black_box(build_list(black_box(&items))));
    });

    let list = build_list(&items);
    c.bench_function(&format!("list_index_{LARGE}"), |bench| {
        bench.iter(|| {
            for index in (0..LARGE as isize).step_by(7) {
                black_box(ListOps::get(&list, black_box(index)));
            }
        });
    });

    c.bench_function(&format!("list_pop_front_{LARGE}"), |bench| {
        bench.iter_batched(
            || list.clone(),
            |mut list| {
                while let Ok(Some(value)) = list.pop_front() {
                    black_box(value);
                }
                list
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_set(c: &mut Criterion) {
    let int_members: Vec<Vec<u8>> = (0..LARGE).map(|i| i.to_string().into_bytes()).collect();

    c.bench_function(&format!("set_intset_add_{LARGE}"), |bench| {
        bench.iter(|| {
            let mut set = IntSet::new();
            for index in 0..LARGE {
                let _ = IntSet::add(&mut set, black_box(index as i64), usize::MAX);
            }
            black_box(set)
        });
    });

    let mut intset = IntSet::new();
    for index in 0..LARGE {
        let _ = IntSet::add(&mut intset, index as i64, usize::MAX);
    }
    c.bench_function(&format!("set_intset_contains_{LARGE}"), |bench| {
        bench.iter(|| {
            for item in &int_members {
                black_box(SetOps::contains(&intset, black_box(item)));
            }
        });
    });

    let items = members(SMALL);
    c.bench_function(&format!("set_listpack_add_{SMALL}"), |bench| {
        bench.iter(|| {
            let mut set = ValkeySet::new();
            set.encoding = SetEncoding::Listpack;
            for item in &items {
                let _ = SetOps::add(&mut set, black_box(item));
            }
            black_box(set)
        });
    });

    let mut listpack = ValkeySet::new();
    listpack.encoding = SetEncoding::Listpack;
    for item in &items {
        let _ = SetOps::add(&mut listpack, item);
    }
    c.bench_function(&format!("set_listpack_contains_{SMALL}"), |bench| {
        bench.iter(|| {
            for item in &items {
                black_box(SetOps::contains(&listpack, black_box(item)));
            }
        });
    });
}

criterion_group!(benches, bench_zset, bench_hash, bench_list, bench_set);
criterion_main!(benches);
