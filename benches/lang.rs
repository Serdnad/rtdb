use criterion::{black_box, Criterion, criterion_group, criterion_main};
use pprof::criterion::{PProfProfiler, Output};



use rtdb::lang::insert::parse_insert;
use rtdb::lang::query::parse_select;

fn criterion_benchmark(c: &mut Criterion) {
    // c.bench_function("parse simple select query 1", |b| {
    //     b.iter(|| {
    //         let mut query = black_box(String::from("SELECT test_series"));
    //         parse(&mut query).unwrap();
    //     })
    // });


    // c.bench_function("parse simple select query 2", |b| {
    //     b.iter(|| {
    //         let mut query = String::from("SELECT test_series[field1, field2]");
    //         parse(&mut query).unwrap();
    //     })
    // });


    c.bench_function("parse simple select query 1 alt", |b| {
        b.iter(|| {
            let mut query = black_box(String::from("SELECT test_series[ field1, field2,   field3,field4, field5, field6, field7  , field8 ]"));
            parse_select(&mut query);
        })
    });

    c.bench_function("parse short insert", |b| {
        b.iter(|| {
            let mut query = String::from("INSERT test_series,field1=1.0");
            parse_insert(&mut query);
        })
    });

    c.bench_function("parse longer insert", |b| {
        b.iter(|| {
            let mut query = String::from("INSERT test_series,field1=1.0, field2=0.5123 1663644227213092171");
            parse_insert(&mut query);
        })
    });
}


criterion_group!{
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = criterion_benchmark
}
criterion_main!(benches);
