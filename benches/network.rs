#[macro_use]
extern crate criterion;

use criterion::{Criterion, criterion_group, criterion_main};

use rtdb::execution::QueryResult;
use rtdb::storage::series::{DataRow, RecordCollection};
use rtdb::wire_protocol::query::{build_query_result, ByteReader, parse_query_result};
use pprof::criterion::{PProfProfiler, Output};


fn all(c: &mut Criterion) {
    let result = QueryResult {
        count: 2,
        records: RecordCollection {
            fields: vec![String::from("field1"), String::from("field2")],
            rows: vec![DataRow { time: 1, elements: vec![Some(1.0), Some(2.0)] },
                       DataRow { time: 2, elements: vec![Some(1.0), Some(2.0)] },
                       DataRow { time: 3, elements: vec![Some(1.0), Some(2.0)] },
                       DataRow { time: 4, elements: vec![Some(1.0), Some(2.0)] }],
        },
    };

    c.bench_function("serialize query result", |b| {
        b.iter(|| {
            let _ = build_query_result(&result);
        })
    });

    c.bench_function("deserialize query result", |b| {
        let buffer = build_query_result(&result);

        b.iter(|| {
            let mut cursor = ByteReader::new(&buffer);
            let _ = parse_query_result(&mut cursor);
        })
    });


}

criterion_group!{
    name = benches;
    config = Criterion::default().with_profiler(PProfProfiler::new(100, Output::Flamegraph(None)));
    targets = all
}
criterion_main!(benches);
