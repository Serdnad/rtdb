use std::collections::HashMap;
use std::fs::File;

use criterion::{black_box, Criterion, criterion_group, criterion_main};

use rtdb::storage::field::{FieldEntry, FieldStorage};
use rtdb::storage::field_block::FieldStorageBlock;

use rayon::prelude::*;
use rtdb::lang::SelectQuery;
use rtdb::storage::field_index::FieldStorageBlockSummary;
use rtdb::storage::series::{merge_records, merge_records2, merge_records3, SeriesEntry, SeriesStorage};
use rtdb::util::{arg_min_all, arg_min_all2};


fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("write field [single]", |b| {
        let mut s = FieldStorage::new("tests", "value");

        b.iter(|| {
            s.insert(FieldEntry { value: 123.0, time: 0 })
        })
    });

    c.bench_function("load block", |b| {
        let f = File::open("test_series_value1").unwrap();

        b.iter(|| {
            // let blocks: Vec<FieldStorageBlock> = (0..100).map(|offset| FieldStorageBlock::load(&f, offset)).collect();
            let block = FieldStorageBlock::load(&f, 0);
        })
    });

    c.bench_function("load summaries", |b| {
        b.iter(|| {
            let summaries = FieldStorageBlockSummary::load_all("test_series/value1_index");
        })
    });

    c.bench_function("read field", |b| {
        let s = FieldStorage::new("test_series", "value1");

        b.iter(|| {
            let records = s.read(None, None);
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

    c.bench_function("arg_min_all", |b| {
        b.iter(|| {
            black_box(
                arg_min_all(&Vec::<i64>::from([1, 0, 3, 1]))
            );
        })
    });

    c.bench_function("arg_min_all2", |b| {
        b.iter(|| {
            black_box(
                arg_min_all2(&Vec::<i64>::from([1, 0, 3, 1]))
            );
        })
    });

    c.bench_function("arg_min_all same", |b| {
        b.iter(|| {
            black_box(
                arg_min_all(&Vec::<i64>::from([1, 1, 1, 1]))
            );
        })
    });

    c.bench_function("arg_min_all2 same", |b| {
        b.iter(|| {
            black_box(
                arg_min_all2(&Vec::<i64>::from([1, 1, 1, 1]))
            );
        })
    });


    c.bench_function("arg_min_all same 2", |b| {
        b.iter(|| {
            black_box(
                arg_min_all(&Vec::<i64>::from([1, 1]))
            );
        })
    });

    c.bench_function("arg_min_all2 same 2", |b| {
        b.iter(|| {
            black_box(
                arg_min_all2(&Vec::<i64>::from([1, 1]))
            );
        })
    });


    // c.bench_function("merge baseline", |b| {
    //     b.iter(|| {
    //         let a: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i, value: 0.0 }).collect();
    //         let b: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i, value: 1.0 }).collect();
    //         let entries = vec![a, b];
    //     })
    // });

    c.bench_function("merge aligned records", |b| {
        b.iter(|| {
            let a: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i, value: 0.0 }).collect();
            let b: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i, value: 1.0 }).collect();
            let entries = vec![a, b];

            let records = merge_records(entries, vec!["field1", "field2"]);
        })
    });

    c.bench_function("merge2 aligned records", |b| {
        b.iter(|| {
            let a: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i, value: 0.0 }).collect();
            let b: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i, value: 1.0 }).collect();
            let entries = vec![a, b];

            let records = merge_records2(entries, vec!["field1", "field2"]);
        })
    });

    c.bench_function("merge3 aligned records big", |b| {
        b.iter(|| {
            let a: Vec<_> = (0..10000).into_iter().map(|i| FieldEntry { time: i, value: 0.0 }).collect();
            let b: Vec<_> = (0..10000).into_iter().map(|i| FieldEntry { time: i, value: 1.0 }).collect();
            let entries = vec![a, b];

            let records = merge_records3(entries, vec!["field1", "field2"]);
        })
    });

    c.bench_function("merge3 aligned records", |b| {
        b.iter(|| {
            let a: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i, value: 0.0 }).collect();
            let b: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i, value: 1.0 }).collect();
            let entries = vec![a, b];

            let records = merge_records3(entries, vec!["field1", "field2"]);
        })
    });

    c.bench_function("merge alternating records", |b| {
        b.iter(|| {
            let a: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i * 2, value: 0.0 }).collect();
            let b: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i * 2 + 1, value: 1.0 }).collect();
            let entries = vec![a, b];

            let records = merge_records(entries, vec!["field1", "field2"]);
        })
    });

    c.bench_function("merge2 alternating records", |b| {
        b.iter(|| {
            let a: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i * 2, value: 0.0 }).collect();
            let b: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i * 2 + 1, value: 1.0 }).collect();
            let entries = vec![a, b];

            let records = merge_records2(entries, vec!["field1", "field2"]);
        })
    });


    c.bench_function("merge3 alternating records", |b| {
        b.iter(|| {
            let a: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i * 2, value: 0.0 }).collect();
            let b: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i * 2 + 1, value: 1.0 }).collect();
            let entries = vec![a, b];

            let records = merge_records3(entries, vec!["field1", "field2"]);
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

    c.bench_function("merge 4 aligned records", |b| {
        b.iter(|| {
            let a: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i, value: 0.0 }).collect();
            let b: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i, value: 1.0 }).collect();
            let c: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i, value: 1.0 }).collect();
            let d: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i, value: 1.0 }).collect();
            let entries = vec![a, b, c, d];

            let records = merge_records(entries, vec!["field1", "field2", "field3", "field4"]);
        })
    });

    c.bench_function("merge2 4 aligned records", |b| {
        b.iter(|| {
            let a: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i, value: 0.0 }).collect();
            let b: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i, value: 1.0 }).collect();
            let c: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i, value: 1.0 }).collect();
            let d: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i, value: 1.0 }).collect();
            let entries = vec![a, b, c, d];

            let records = merge_records2(entries, vec!["field1", "field2", "field3", "field4"]);
        })
    });


    c.bench_function("merge3 4 aligned records", |b| {
        b.iter(|| {
            let a: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i, value: 0.0 }).collect();
            let b: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i, value: 1.0 }).collect();
            let c: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i, value: 1.0 }).collect();
            let d: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i, value: 1.0 }).collect();
            let entries = vec![a, b, c, d];

            let records = merge_records3(entries, vec!["field1", "field2", "field3", "field4"]);
        })
    });

    c.bench_function("merge 4 alternating records", |b| {
        b.iter(|| {
            let a: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i * 4, value: 0.0 }).collect();
            let b: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i * 4 + 1, value: 1.0 }).collect();
            let c: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i * 4 + 2, value: 2.0 }).collect();
            let d: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i * 4 + 3, value: 3.0 }).collect();
            let entries = vec![a, b, c, d];

            let records = merge_records(entries, vec!["field1", "field2", "field3", "field4"]);
        })
    });

    c.bench_function("merge2 4 alternating records", |b| {
        b.iter(|| {
            let a: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i * 4, value: 0.0 }).collect();
            let b: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i * 4 + 1, value: 1.0 }).collect();
            let c: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i * 4 + 2, value: 2.0 }).collect();
            let d: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i * 4 + 3, value: 3.0 }).collect();
            let entries = vec![a, b, c, d];

            let records = merge_records2(entries, vec!["field1", "field2", "field3", "field4"]);
        })
    });


    c.bench_function("merge3 4 alternating records", |b| {
        b.iter(|| {
            let a: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i * 4, value: 0.0 }).collect();
            let b: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i * 4 + 1, value: 1.0 }).collect();
            let c: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i * 4 + 2, value: 2.0 }).collect();
            let d: Vec<_> = (0..100).into_iter().map(|i| FieldEntry { time: i * 4 + 3, value: 3.0 }).collect();
            let entries = vec![a, b, c, d];

            let records = merge_records3(entries, vec!["field1", "field2", "field3", "field4"]);
        })
    });


    c.bench_function("read series", |b| {
        let mut s = SeriesStorage::load("test_series");
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


    //

    // c.bench_function("merge alternating records", |b| {
    //     b.iter(|| {
    //         let entries = vec![
    //             vec![FieldEntry { value: 1.0, time: 1 }, FieldEntry { value: 2.0, time: 3 }],
    //             vec![FieldEntry { value: 3.0, time: 2 }, FieldEntry { value: 4.0, time: 4 }],
    //         ];
    //
    //         let records = merge_records(entries, vec!["field1", "field2"]);
    //     })
    // });
    //
    // c.bench_function("merge mixed records", |b| {
    //     b.iter(|| {
    //         let entries = vec![
    //             vec![FieldEntry { value: 1.0, time: 1 }, FieldEntry { value: 2.0, time: 3 }, FieldEntry { value: 3.0, time: 4 }, FieldEntry { value: 4.0, time: 5 }],
    //             vec![FieldEntry { value: 3.0, time: 2 }, FieldEntry { value: 4.0, time: 4 }],
    //         ];
    //
    //         let records = merge_records(entries, vec!["field1", "field2"]);
    //     })
    // });

    // c.bench_function("write field [multiple unbatched]", |b| {
    //     let vals: Vec<f64> = (0..1000).map(|x| x as f64).collect();
    //
    //     b.iter(|| {
    //         for &val in &vals {
    //             s.insert(FieldEntry { value: 123.0, time: 0 })
    //         }
    //     })
    // });

    // c.bench_function("read", |b| b.iter(|| {
    //     s.read::<i32>();
    // }));

    // c.bench_function("read block", |b| {
    //     let bytes = read("test_series_value1").unwrap();
    //     b.iter(|| {
    //         // rkyv::check_archived_root::<Vec<FieldEntry>>(&bytes[..]).unwrap();
    //         black_box(1 + 1);
    //         let a = unsafe { rkyv::archived_root::<Vec<FieldEntry>>(&bytes[..]) };
    //     })
    // });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
