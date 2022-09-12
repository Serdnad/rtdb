use std::cmp::min;
use std::collections::HashMap;

use crate::storage::field::{FieldEntry, FieldStorage};
use crate::storage::SupportedDataType;
use crate::util::arg_min_all;

enum DataType {
    Float,
}

/// A series entry is a collection of values, each corresponding to a different field under the
/// same series, all sharing the same timestamp.
#[derive(Debug, PartialEq)]
pub struct SeriesEntry<'a> {
    pub values: HashMap<&'a str, f64>,

    // Timestamp as nanoseconds since Unix epoch
    pub time: i64,
}

#[derive(Debug)]
pub struct FieldSummary<'a> {
    name: &'a str,
}

#[derive(Debug)]
pub struct SeriesStorage<'a> {
    pub(crate) series_name: &'a str,
    field_summaries: Vec<FieldSummary<'a>>,
    field_storages: HashMap<String, FieldStorage>,
}

impl SeriesStorage<'_> {
    pub fn new(series_name: &str) -> SeriesStorage {
        SeriesStorage { series_name, field_summaries: vec![], field_storages: HashMap::new() }
    }

    pub fn load(series_name: &str) -> SeriesStorage {
        // TODO: read indexes for field storages

        SeriesStorage {
            series_name,
            field_summaries: vec![],
            field_storages: HashMap::new(),
        }
    }

    pub fn read<'a>(&self, mut fields: Vec<&'a str>) -> Vec<SeriesEntry<'a>> {
        let records: Vec<Vec<FieldEntry>> = fields.iter().map(|&field| {
            match self.field_storages.get(field) {
                Some(storage) => storage.read(),
                None => {
                    println!("Field not found!! :(");
                    vec![]
                }
            }
        }).collect();

        dbg!(&records[0].len());

        merge_records(records, fields)
    }

    pub fn insert(&mut self, entry: SeriesEntry) {
        for (&field, &value) in entry.values.iter() {
            if !self.field_storages.contains_key(field) {
                println!("MAKE A NEW FIELD STORAGE");
                let new_storage = FieldStorage::new(self.series_name, &field);
                self.field_storages.insert(field.to_owned(), new_storage);
            }

            let mut field_storage = self.field_storages.get_mut(field).unwrap();
            field_storage.insert(FieldEntry { value, time: entry.time })
        }
    }
}


/// Given a vector of sorted field records, merge them into a single, sorted vector of SeriesEntry.
/// Records from different fields with identical timestamps should be placed into the same
/// SeriesEntry.
///
/// TODO: write unit tests (and benchmarks) for this, this is a core function
///
/// TODO: should this go into a util file?
pub fn merge_records(mut fields: Vec<Vec<FieldEntry>>, mut names: Vec<&str>) -> Vec<SeriesEntry> {
    let mut merged_records = vec![];
    let mut indices = vec![0; fields.len()];

    loop {
        let next_timestamps: Vec<i64> = indices.iter().enumerate().map(|(f, &i)| {
            fields[f][i].time
        }).collect();

        if next_timestamps.is_empty() {
            break;
        }

        let (min, next_field_indexes) = arg_min_all(&next_timestamps);
        let next_time = min.unwrap();

        let next_fields: HashMap<_, _> = next_field_indexes.iter().map(|&i| {
            let elem = (names[i], fields[i][indices[i]].value);
            indices[i] += 1;
            elem
        }).collect();

        // delete fields as their records become exhausted
        // delete backwards so we don't skip over any elements swapped with removed elements.
        for i in (0..indices.len()).into_iter().rev() {
            if indices[i] == fields[i].len() {
                fields.swap_remove(i);
                indices.swap_remove(i);
                names.swap_remove(i);
            }
        }

        let series_entry = SeriesEntry {
            values: next_fields,
            time: next_time,
        };

        merged_records.push(series_entry);
    }

    // dbg!(&merged_records);
    merged_records
}

// pub fn merge_records2(mut fields: Vec<Vec<FieldEntry>>, mut names: Vec<&str>) -> Vec<SeriesEntry> {
//     let mut merged_records = vec![];
//     let mut iters: Vec<_> = (0..fields.len()).map(|i| fields[i].iter().peekable()).collect();
//
//     //
//     // let nexts: Vec<_> = iters.iter().map(|mut iter| {
//     //     let v = iter.peek()
//     // }).collect();
//
//     loop {
//
//
//
//         // iters[0].peek()
//
//         let next_timestamps: Vec<u128> = indices.iter().enumerate().map(|(f, &i)| {
//             fields[f][i].time
//         }).collect();
//
//         if next_timestamps.is_empty() {
//             break;
//         }
//
//         let (min, next_field_indexes) = arg_min_all(&next_timestamps);
//         let next_time = min.unwrap();
//
//         let next_fields: HashMap<_, _> = next_field_indexes.iter().map(|&i| {
//             let elem = (names[i], fields[i][indices[i]].value);
//             indices[i] += 1;
//             elem
//         }).collect();
//
//         // delete fields as their records become exhausted
//         // delete backwards so we don't skip over any elements swapped with removed elements.
//         for i in (0..indices.len()).into_iter().rev() {
//             if indices[i] == fields[i].len() {
//                 fields.swap_remove(i);
//                 indices.swap_remove(i);
//                 names.swap_remove(i);
//             }
//         }
//
//         let series_entry = SeriesEntry {
//             values: next_fields,
//             time: next_time,
//         };
//
//         merged_records.push(series_entry);
//     }
//
//     // dbg!(&merged_records);
//     merged_records
// }


#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::{fs, time};
    use crate::storage::field::FieldEntry;

    use crate::storage::field_block::ENTRIES_PER_BLOCK;
    use crate::storage::series::{merge_records, SeriesEntry, SeriesStorage};

    #[test]
    fn it_writes_a_series_entry() {
        let mut s = SeriesStorage::new("test_series");

        for i in 0..20 {
            let entry1 = SeriesEntry {
                values: HashMap::from([("value1", 1.0), ("value2", 101.0)]),
                time: time::UNIX_EPOCH.elapsed().unwrap().as_nanos() as i64,
            };
            let entry2 = SeriesEntry {
                values: HashMap::from([("value1", 1.0), ("value2", 101.0)]),
                time: time::UNIX_EPOCH.elapsed().unwrap().as_nanos() as i64,
            };

            s.insert(entry1);
            s.insert(entry2);
        }
        // let mut s = SeriesFieldStorage::new("test_metric", "value1");
        //
        // let entry = SeriesEntry {
        //     series_name: String::from("test_metric"),
        //     values: HashMap::from([("value1", 1.0), ("value2", 101.0)]),
        //     time: time::UNIX_EPOCH.elapsed().unwrap().as_nanos(),
        // };


        // let values = s.read::<i64>();
        // dbg!(values);
        // s.insert(Entry { value: 123, time: time::UNIX_EPOCH.elapsed().unwrap().as_nanos() });
    }

    #[test]
    fn it_reads() {
        fs::remove_file("test_series_value1");
        fs::remove_file("test_series_value1_index");
        fs::remove_file("test_series_value2");
        fs::remove_file("test_series_value2_index");

        let mut s = SeriesStorage::new("test_series");
        for _ in 0..ENTRIES_PER_BLOCK + 1 {
            s.insert(SeriesEntry {
                values: HashMap::from([("value1", 1.0), ("value2", -1.0)]),
                time: time::UNIX_EPOCH.elapsed().unwrap().as_nanos() as i64,
            });
        }

        let r = s.read(vec!["value1", "value2"]);
        dbg!(r);
    }


    #[test]
    fn merge_aligned() {
        let entries = vec![
            vec![FieldEntry { value: 1.0, time: 1 }, FieldEntry { value: 2.0, time: 2 }],
            vec![FieldEntry { value: 3.0, time: 1 }, FieldEntry { value: 4.0, time: 2 }],
        ];

        let records = merge_records(entries, vec!["field1", "field2"]);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0], SeriesEntry { values: HashMap::from([("field1", 1.0), ("field2", 3.0)]), time: 1 });
        assert_eq!(records[1], SeriesEntry { values: HashMap::from([("field1", 2.0), ("field2", 4.0)]), time: 2 });
    }

    #[test]
    fn merge_alternating() {
        let entries = vec![
            vec![FieldEntry { value: 1.0, time: 1 }, FieldEntry { value: 2.0, time: 3 }],
            vec![FieldEntry { value: 3.0, time: 2 }, FieldEntry { value: 4.0, time: 4 }],
        ];

        let records = merge_records(entries, vec!["field1", "field2"]);
        assert_eq!(records.len(), 4);
        assert_eq!(records[0], SeriesEntry { values: HashMap::from([("field1", 1.0)]), time: 1 });
        assert_eq!(records[1], SeriesEntry { values: HashMap::from([("field2", 3.0)]), time: 2 });
        assert_eq!(records[2], SeriesEntry { values: HashMap::from([("field1", 2.0)]), time: 3 });
        assert_eq!(records[3], SeriesEntry { values: HashMap::from([("field2", 4.0)]), time: 4 });
    }

    #[test]
    fn merge_mixed() {
        let entries = vec![
            vec![FieldEntry { value: 1.0, time: 1 }, FieldEntry { value: 2.0, time: 3 }, FieldEntry { value: 3.0, time: 4 }, FieldEntry { value: 4.0, time: 5 }],
            vec![FieldEntry { value: 3.0, time: 2 }, FieldEntry { value: 4.0, time: 4 }],
        ];

        let records = merge_records(entries, vec!["field1", "field2"]);
        assert_eq!(records.len(), 5);
        assert_eq!(records[0], SeriesEntry { values: HashMap::from([("field1", 1.0)]), time: 1 });
        assert_eq!(records[1], SeriesEntry { values: HashMap::from([("field2", 3.0)]), time: 2 });
        assert_eq!(records[2], SeriesEntry { values: HashMap::from([("field1", 2.0)]), time: 3 });
        assert_eq!(records[3], SeriesEntry { values: HashMap::from([("field1", 3.0), ("field2", 4.0)]), time: 4 });
        assert_eq!(records[4], SeriesEntry { values: HashMap::from([("field1", 4.0)]), time: 5 });
    }
}