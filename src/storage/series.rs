use std::collections::HashMap;
use std::fs::{create_dir, read, read_dir};
use std::io::{Read, Write};
use std::io::ErrorKind::AlreadyExists;
use std::str;
use crate::lang::SelectQuery;

use crate::storage::field::{FieldEntry, FieldStorage};
use crate::storage::SupportedDataType;
use crate::util::arg_min_all;

enum DataType {
    Float,
}

/// A series entry is a collection of values, each corresponding to a different field under the
/// same series, all sharing the same timestamp.
#[derive(Debug, PartialEq, serde::Serialize)]
pub struct SeriesEntry {
    pub values: HashMap<String, f64>,

    // Timestamp as nanoseconds since Unix epoch
    pub time: i64,
}

#[derive(Debug, PartialEq)]
pub struct SeriesSummary<'a> {
    name: &'a str,
    fields: Vec<String>,
}

impl SeriesSummary<'_> {
    pub fn new(name: &str) -> SeriesSummary {
        let summary = SeriesSummary { name, fields: vec![] };
        summary.write();
        summary
    }

    pub fn load(name: &str) -> SeriesSummary {
        let bytes = read(name).unwrap();
        let data = str::from_utf8(bytes.as_slice()).unwrap();

        SeriesSummary {
            name,
            fields: data.split(" ").collect::<Vec<&str>>().iter().map(|&s| s.to_owned()).collect(),
        }


        // let mut bytes = read(name); // TODO: file name + path
        // let summary = rkyv::from_bytes::<SeriesSummary>(&bytes[..]).expect("failed to unarchive series summary");
        //
        // summary
    }

    /// Write series summary to disk
    fn write(&self) {
        // let mut f = OpenOptions::new()
        //     .create(true)
        //     .write(true)
        //     .open(self.name)
        //     .unwrap(); // tODO: keep handle and rename
        //
        // // let data = rkyv::to_bytes(&self).unwrap();
        // // f.write(&data[..]);
        //
        // f.write(self.fields.join(" ").into_bytes().as_slice());
    }
}


#[derive(Debug)]
pub struct SeriesStorage<'a> {
    pub(crate) series_name: &'a str,
    // summary: SeriesSummary<'a>,
    field_storages: HashMap<String, FieldStorage>,
}

impl SeriesStorage<'_> {
    pub fn new(series_name: &str) -> SeriesStorage {
        if let Err(err) = create_dir(series_name) {
            match err.kind() {
                AlreadyExists => {}
                _ => panic!("{}", err),
            }
        };

        SeriesStorage { series_name, field_storages: HashMap::new() }
    }

    pub fn load(series_name: &str) -> SeriesStorage {
        // TODO: read indexes for field storages

        let files: Vec<_> = read_dir(series_name).unwrap().collect();
        let mut fields = vec![];
        for entry in files {
            let file = entry.unwrap().file_name();
            let filename = file.to_str().unwrap();
            if !filename.ends_with("_index") {
                fields.push(filename.to_owned());
            }
        }

        let field_storages = fields.iter().map(|f| (f.to_owned(), FieldStorage::new(series_name, f))).collect();


        SeriesStorage {
            series_name,
            field_storages,
        }
    }

    pub fn read(&self, query: SelectQuery) -> Vec<SeriesEntry> {
        let fields = match query.fields.is_empty() {
            true => self.field_storages.keys().map(|s| s.as_str()).collect(), // TODO: we should just keep this around
            false => query.fields,
        };

        let records: Vec<Vec<FieldEntry>> = fields.iter().map(|&field| {
            match self.field_storages.get(field) {
                Some(storage) => {
                    storage.read(query.start, query.end)
                }
                None => {
                    println!("Field not found!! :(");
                    vec![]
                }
            }
        }).collect();

        merge_records(records, fields)
    }

    pub fn insert(&mut self, entry: SeriesEntry) {
        for (field, &value) in entry.values.iter() {
            if !self.field_storages.contains_key(field) {
                // println!("MAKE A NEW FIELD STORAGE");
                let new_storage = FieldStorage::new(self.series_name, &field);
                self.field_storages.insert(field.to_owned(), new_storage);
            }

            let field_storage = self.field_storages.get_mut(field).unwrap();
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

        let next_fields: HashMap<_, _> = next_field_indexes.iter().rev().map(|&i| {
            let elem = (names[i].to_owned(), fields[i][indices[i]].value);

            if indices[i] + 1 == fields[i].len() {
                fields.swap_remove(i);
                indices.swap_remove(i);
                names.swap_remove(i);
            } else {
                indices[i] += 1;
            }

            elem
        }).collect();

        let series_entry = SeriesEntry {
            values: next_fields,
            time: next_time,
        };

        merged_records.push(series_entry);
    }

    merged_records
}


#[cfg(test)]
mod tests {
    use std::{fs, time};
    use std::collections::HashMap;
    use crate::lang::SelectQuery;

    use crate::storage::field::FieldEntry;
    use crate::storage::field_block::ENTRIES_PER_BLOCK;
    use crate::storage::series::{merge_records, SeriesEntry, SeriesStorage};

    fn clear_tmp_files() {
        fs::remove_dir("test_series");
    }

    #[test]
    fn it_writes_a_series_entry() {
        clear_tmp_files();

        let mut s = SeriesStorage::new("test_series");

        for i in 0..ENTRIES_PER_BLOCK * 4 + 1 {
            let entry1 = SeriesEntry {
                values: HashMap::from([(String::from("value1"), 1.0), (String::from("value2"), 101.0)]),
                time: time::UNIX_EPOCH.elapsed().unwrap().as_nanos() as i64,
            };
            let entry2 = SeriesEntry {
                values: HashMap::from([(String::from("value1"), 1.0), (String::from("value2"), 101.0)]),
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
        // clear_tmp_files();

        let s = SeriesStorage::load("test_series");
        // for _ in 0..ENTRIES_PER_BLOCK * 10 + 1 {
        //     s.insert(SeriesEntry {
        //         values: HashMap::from([(String::from("value1"), 1.0), (String::from("value2"), -1.0)]),
        //         time: time::UNIX_EPOCH.elapsed().unwrap().as_nanos() as i64,
        //     });
        // }

        let r = s.read(SelectQuery { series: "test_series", fields: vec!["value1", "value2"], start: None, end: None });
        dbg!(r.len());
        dbg!(&s.field_storages.get("value1").unwrap().block_manager);

        let r = s.read(SelectQuery { series: "test_series", fields: vec!["value1", "value2"], start: None, end: None });
        dbg!(r.len());
        dbg!(&s.field_storages.get("value1").unwrap().block_manager);
    }


    #[test]
    fn merge_aligned() {
        let entries = vec![
            vec![FieldEntry { value: 1.0, time: 1 }, FieldEntry { value: 2.0, time: 2 }],
            vec![FieldEntry { value: 3.0, time: 1 }, FieldEntry { value: 4.0, time: 2 }],
        ];

        let records = merge_records(entries, vec!["field1", "field2"]);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0], SeriesEntry { values: HashMap::from([(String::from("field1"), 1.0), (String::from("field2"), 3.0)]), time: 1 });
        assert_eq!(records[1], SeriesEntry { values: HashMap::from([(String::from("field1"), 2.0), (String::from("field2"), 4.0)]), time: 2 });
    }

    #[test]
    fn merge_alternating() {
        let entries = vec![
            vec![FieldEntry { value: 1.0, time: 1 }, FieldEntry { value: 2.0, time: 3 }],
            vec![FieldEntry { value: 3.0, time: 2 }, FieldEntry { value: 4.0, time: 4 }],
        ];

        let records = merge_records(entries, vec!["field1", "field2"]);
        assert_eq!(records.len(), 4);
        assert_eq!(records[0], SeriesEntry { values: HashMap::from([(String::from("field1"), 1.0)]), time: 1 });
        assert_eq!(records[1], SeriesEntry { values: HashMap::from([(String::from("field2"), 3.0)]), time: 2 });
        assert_eq!(records[2], SeriesEntry { values: HashMap::from([(String::from("field1"), 2.0)]), time: 3 });
        assert_eq!(records[3], SeriesEntry { values: HashMap::from([(String::from("field2"), 4.0)]), time: 4 });
    }

    #[test]
    fn merge_mixed() {
        let entries = vec![
            vec![FieldEntry { value: 1.0, time: 1 }, FieldEntry { value: 2.0, time: 3 }, FieldEntry { value: 3.0, time: 4 }, FieldEntry { value: 4.0, time: 5 }],
            vec![FieldEntry { value: 3.0, time: 2 }, FieldEntry { value: 4.0, time: 4 }],
        ];

        let records = merge_records(entries, vec!["field1", "field2"]);
        assert_eq!(records.len(), 5);
        assert_eq!(records[0], SeriesEntry { values: HashMap::from([(String::from("field1"), 1.0)]), time: 1 });
        assert_eq!(records[1], SeriesEntry { values: HashMap::from([(String::from("field2"), 3.0)]), time: 2 });
        assert_eq!(records[2], SeriesEntry { values: HashMap::from([(String::from("field1"), 2.0)]), time: 3 });
        assert_eq!(records[3], SeriesEntry { values: HashMap::from([(String::from("field1"), 3.0), (String::from("field2"), 4.0)]), time: 4 });
        assert_eq!(records[4], SeriesEntry { values: HashMap::from([(String::from("field1"), 4.0)]), time: 5 });
    }
}