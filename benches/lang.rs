use std::str::{from_utf8, from_utf8_unchecked};
use criterion::{black_box, Criterion, criterion_group, criterion_main};

use rtdb::lang::{Action, parse, SelectQuery};

#[inline]
fn advance_whitespace(s: &[u8], mut index: &mut usize) {
    let mut i = *index;
    while i < s.len() && s[i] == b' ' {
        i += 1;
    }

    *index = i;
}

#[inline]
fn parse_fields<'a>(s: &'a [u8], mut index: &mut usize, fields: &mut Vec<&'a str>) {
    let mut i = *index;

    if s[i] == b'[' {
        i += 1;
        advance_whitespace(s, &mut i);

        let mut start_index = i;
        while i < s.len() {
            match s[i] {
                b',' => {
                    let field = from_utf8(&s[start_index..i]).unwrap();
                    fields.push(field);

                    i += 1;
                    advance_whitespace(s, &mut i);
                    start_index = i;
                }
                b']' => {
                    let field = from_utf8(&s[start_index..i]).unwrap();
                    fields.push(field);
                    break;
                }
                b' ' => { // handle trailing whitespace
                    let field = from_utf8(&s[start_index..i]).unwrap();
                    fields.push(field);

                    advance_whitespace(s, &mut i);

                    if s[i] == b',' {
                        i += 1;
                        advance_whitespace(s, &mut i);
                    } else if s[i] == b']' {
                        break;
                    }

                    start_index = i;
                }
                _ => i += 1,
            }
        }
    }

    *index = i;
}

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
            query.make_ascii_lowercase();

            let mut index: usize = 0;
            if query.starts_with("select") {
                let query = query.as_bytes();

                index = 6;
                advance_whitespace(query, &mut index);

                // parse series name
                let start_index = index;
                while index < query.len() && query[index] != b'[' && query[index] != b' ' {
                    index += 1;
                }

                // I wonder if we could avoid this copy, since under the hood this is basically memcpy
                let series_name = from_utf8(&query[start_index..index]).unwrap();

                let mut q = SelectQuery { series: series_name, fields: vec![] };
                if index == query.len() {
                    // TODO: return
                    let a = Action::Select(q);
                    return;
                }

                parse_fields(query, &mut index, &mut q.fields);

                // dbg!(q);
            }
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
