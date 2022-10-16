use std::fs::File;

use criterion::{black_box, Criterion, criterion_group, criterion_main};
use pprof::criterion::{Output, PProfProfiler};

use rtdb::DataValue;
use rtdb::lang::SelectQuery;
use rtdb::storage::block_bool::{deserialize_bools, serialize_bools};
use rtdb::storage::field::{FieldEntry, FieldStorage};
use rtdb::storage::field_block::FieldStorageBlock;
use rtdb::storage::field_index::FieldStorageBlockSummary;
use rtdb::storage::series::{merge_records, SeriesEntry, SeriesStorage};
use rtdb::util::new_timestamp;

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("write field [single]", |b| {
        let mut s = FieldStorage::load("bench_tests", "field1");

        b.iter(|| {
            s.insert(FieldEntry { value: DataValue::from(123.0), time: 0 })
        })
    });

    c.bench_function("load block", |b| {
        let f = File::open("test_series_value1").unwrap();

        b.iter(|| {
            // let blocks: Vec<FieldStorageBlock> = (0..100).map(|offset| FieldStorageBlock::load(&f, offset)).collect();
            let _block = FieldStorageBlock::load(&f, 0);
        })
    });

    c.bench_function("load summaries", |b| {
        b.iter(|| {
            let _summaries = FieldStorageBlockSummary::load_all("test_series/value1_index");
        })
    });

    c.bench_function("read field", |b| {
        let s = FieldStorage::load("test_series", "value1");

        b.iter(|| {
            let _records = s.read(None, None);
        })
    });

    // MERGING RECORDS
    c.bench_function("numbers reference", |b| {
        b.iter(|| {
            black_box(
                if 3 > 1 {
                    if 2 < 4 {
                        let mut a = vec![1, 2, 3, 4];
                        a.reverse()
                    }
                });
        })
    });


    c.bench_function("bool_ser_ser big", |b| {
        let mut vals = black_box(vec![]);

        for i in 0..8000 {
            match i % 3 {
                2 => vals.push(Some(true)),
                1 => vals.push(Some(false)),
                _ => vals.push(None),
            }
        }

        b.iter(|| {
            let serialized = serialize_bools(&vals);
        })
    });

    c.bench_function("bool_ser_deser big", |b| {
        let mut vals = black_box(vec![]);

        for i in 0..8000 {
            match i % 3 {
                2 => vals.push(Some(true)),
                1 => vals.push(Some(false)),
                _ => vals.push(None),
            }
        }

        let serialized = serialize_bools(&vals);
        b.iter(|| {
            let deserialized = deserialize_bools(&serialized);
        })
    });

    // c.bench_function("merge baseline", |b| {
    //     b.iter(|| {
    //         let a: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i, value: 0.0 }).collect();
    //         let b: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i, value: 1.0 }).collect();
    //         let entries = vec![a, b];
    //     })
    // });

    // c.bench_function("merge aligned records", |b| {
    //     b.iter(|| {
    //         let a: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i, value: 0.0 }).collect();
    //         let b: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i, value: 1.0 }).collect();
    //         let entries = vec![a, b];
    //
    //         let records = merge_records(entries, vec!["field1", "field2"]);
    //     })
    // });

    c.bench_function("merge aligned records big", |b| {
        let a: Vec<_> = (0..10000).into_iter().map(|i| FieldEntry { time: i, value: DataValue::from(0.0) }).collect();
        let c: Vec<_> = (0..10000).into_iter().map(|i| FieldEntry { time: i, value: DataValue::from(1.0) }).collect();
        let entries = vec![a, c];

        b.iter(|| {
            let _records = merge_records(&entries, &vec!["field1", "field2"]);
        });
    });

    c.bench_function("merge aligned records", |b| {
        let a: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i, value: DataValue::from(0.0) }).collect();
        let c: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i, value: DataValue::from(1.0) }).collect();
        let entries = vec![a, c];

        b.iter(|| {
            let _records = merge_records(&entries, &vec!["field1", "field2"]);
        })
    });

    // c.bench_function("merge alternating records", |b| {
    //     b.iter(|| {
    //         let a: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i * 2, value: 0.0 }).collect();
    //         let b: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i * 2 + 1, value: 1.0 }).collect();
    //         let entries = vec![a, b];
    //
    //         let records = merge_records(entries, vec!["field1", "field2"]);
    //     })
    // });

    c.bench_function("merge alternating records", |b| {
        let a: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i * 2, value: DataValue::from(0.0) }).collect();
        let c: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i * 2 + 1, value: DataValue::from(1.0) }).collect();
        let entries = vec![a, c];

        b.iter(|| {
            let _records = merge_records(&entries, &vec!["field1", "field2"]);
        })
    });


    // c.bench_function("merge 4 baseline", |b| {
    //     b.iter(|| {
    //         let a: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i, value: 0.0 }).collect();
    //         let b: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i, value: 1.0 }).collect();
    //         let c: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i, value: 0.0 }).collect();
    //         let d: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i, value: 1.0 }).collect();
    //         let entries = vec![a, b, c, d];
    //     })
    // });

    // c.bench_function("merge2 4 aligned records", |b| {
    //     b.iter(|| {
    //         let a: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i, value: 0.0 }).collect();
    //         let b: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i, value: 1.0 }).collect();
    //         let c: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i, value: 1.0 }).collect();
    //         let d: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i, value: 1.0 }).collect();
    //         let entries = vec![a, b, c, d];
    //
    //         let records = merge_records(entries, vec!["field1", "field2", "field3", "field4"]);
    //     })
    // });


    c.bench_function("merge 4 aligned records", |b| {
        b.iter(|| {
            let a: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i, value: DataValue::from(0.0) }).collect();
            let b: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i, value: DataValue::from(1.0) }).collect();
            let c: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i, value: DataValue::from(1.0) }).collect();
            let d: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i, value: DataValue::from(1.0) }).collect();
            let entries = vec![a, b, c, d];

            let _records = merge_records(&entries, &vec!["field1", "field2", "field3", "field4"]);
        })
    });

    // c.bench_function("merge 4 alternating records", |b| {
    //     b.iter(|| {
    //         let a: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i * 4, value: 0.0 }).collect();
    //         let b: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i * 4 + 1, value: 1.0 }).collect();
    //         let c: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i * 4 + 2, value: 2.0 }).collect();
    //         let d: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i * 4 + 3, value: 3.0 }).collect();
    //         let entries = vec![a, b, c, d];
    //
    //         let records = merge_records(entries, vec!["field1", "field2", "field3", "field4"]);
    //     })
    // });

    c.bench_function("merge 4 alternating records", |b| {
        b.iter(|| {
            let a: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i * 4, value: DataValue::from(0.0) }).collect();
            let b: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i * 4 + 1, value: DataValue::from(1.0) }).collect();
            let c: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i * 4 + 2, value: DataValue::from(2.0) }).collect();
            let d: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i * 4 + 3, value: DataValue::from(3.0) }).collect();
            let entries = vec![a, b, c, d];

            let _records = merge_records(&entries, &vec!["field1", "field2", "field3", "field4"]);
        })
    });

    c.bench_function("series write", |b| {
        // fs::remove_dir("bench_test");
        let mut s = SeriesStorage::new("bench_test");

        b.iter(|| {
            s.insert(SeriesEntry {
                fields: vec![String::from("field1"), String::from("field2")],
                values: vec![DataValue::from(123.0), DataValue::from(false)],
                time: new_timestamp(),
            })
        })
    });

    c.bench_function("read series", |b| {
        let s = SeriesStorage::load("test_series");
        // s.insert(SeriesEntry { values: HashMap::from([("value1", 1.0), ("value2", 2.0)]), time: 1 });

        b.iter(|| {
            s.read(SelectQuery {
                series: "test_series",
                fields: vec![],
                start: None,
                end: None,
            });
        })
    });
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .with_profiler(
            PProfProfiler::new(100, Output::Flamegraph(None))
        );
    targets = criterion_benchmark
}

criterion_main!(benches);
