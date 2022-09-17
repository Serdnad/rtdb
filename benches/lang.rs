use std::str::{from_utf8, from_utf8_unchecked};
use criterion::{black_box, Criterion, criterion_group, criterion_main};
use nom::bytes::complete::take_while1;
use nom::character::is_digit;

use rtdb::lang::{Action, parse, SelectQuery};
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

    c.bench_function("parse simple select query 3", |b| {
        b.iter(|| {
            let mut query = String::from("SELECT test_series[ field1, field2,   field3,field4, field5, field6, field7  , field8 ]");
            parse(&mut query).unwrap();
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
