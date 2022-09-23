use std::collections::HashMap;
use std::fs::{create_dir, OpenOptions, read, read_dir};
use std::io::{Read, Write};
use std::io::ErrorKind::AlreadyExists;
use std::str;

use crate::lang::SelectQuery;
use crate::storage::field::{FieldEntry, FieldStorage};


enum DataType {
    Float,
}

/// A series entry is a collection of values, each corresponding to a different field under the
/// same series, all sharing the same timestamp.
#[derive(Debug, PartialEq, serde::Serialize)]
pub struct SeriesEntry {
    pub fields: Vec<String>,
    pub values: Vec<f64>,

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
    }

    /// Write series summary to disk
    fn write(&self) {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .open(self.name)
            .unwrap(); // tODO: keep handle and rename
        //
        // // let data = rkyv::to_bytes(&self).unwrap();
        // // f.write(&data[..]);
        //
        f.write(self.fields.join(" ").into_bytes().as_slice());
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
        let files = match read_dir(series_name) {
            Ok(files) => files,
            Err(_) => {
                println!("Create new series");
                return SeriesStorage::new(series_name);
            }
        };

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

    pub fn read(&self, query: SelectQuery) -> RecordCollection {
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

        merge_records3(records, fields)
    }

    pub fn insert(&mut self, entry: SeriesEntry) {
        for i in 0..entry.fields.len() {
            let field = &entry.fields[i];
            let value = entry.values[i];

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

// TODO: move
#[derive(Debug, serde::Serialize, PartialEq)]
pub struct RecordCollection {
    pub fields: Vec<String>,
    pub rows: Vec<DataRow>,
}

// TODO: move
#[derive(Debug, serde::Serialize, PartialEq)]
pub struct DataRow {
    pub time: i64,
    pub elements: Vec<Option<f64>>,
}

// TODO [urgent]: this doesn't include timestamps...
pub fn merge_records3(fields: Vec<Vec<FieldEntry>>, names: Vec<&str>) -> RecordCollection {
    let field_count = names.len();

    let mut rows = Vec::with_capacity(fields[0].len());

    let mut next_elems: Vec<_> = fields.iter().map(|f| &f[0]).collect();
    let mut indices = vec![0; fields.len()];

    let mut exhausted_count = 0;

    loop {
        if exhausted_count == field_count {
            break;
        }

        let mut earliest = next_elems[0].time;
        for &e in &next_elems[1..] {
            if e.time < earliest {
                earliest = e.time;
            }
        }

        // construct row, picking elements that match earliest, and filling None otherwise
        let mut elems = Vec::with_capacity(field_count);
        for i in 0..field_count {
            let entry = next_elems[i];

            if entry.time == earliest {
                elems.push(Some(entry.value));
                indices[i] += 1;

                if indices[i] == fields[i].len() {
                    exhausted_count += 1;
                    next_elems[i] = &FieldEntry { time: i64::MAX, value: 0.0 };
                } else {
                    next_elems[i] = &fields[i][indices[i]];
                }
            } else {
                elems.push(None);
            }
        }

        rows.push(DataRow { time: earliest, elements: elems });
    }

    RecordCollection { fields: names.iter().map(|&s| s.to_owned()).collect(), rows }
}


#[cfg(test)]
mod tests {
    use std::{fs};
    use crate::lang::SelectQuery;
    use crate::storage::field::FieldEntry;
    use crate::storage::field_block::ENTRIES_PER_BLOCK;
    use crate::storage::series::{DataRow, merge_records3, SeriesEntry, SeriesStorage};
    use crate::util::new_timestamp;

    fn clear_tmp_files() {
        fs::remove_dir("test_series");
    }

    // TODO: update these tests when we're including timestamps
    #[test]
    fn merge3() {
        let entries = vec![
            vec![FieldEntry { value: 1.0, time: 1 }, FieldEntry { value: 2.0, time: 2 }],
            vec![FieldEntry { value: 3.0, time: 1 }, FieldEntry { value: 4.0, time: 2 }],
        ];

        let records = merge_records3(entries, vec!["field1", "field2"]);
        assert_eq!(records.rows, vec![
            DataRow { time: 1, elements: vec![Some(1.0), Some(3.0)] },
            DataRow { time: 1, elements: vec![Some(2.0), Some(4.0)] },
        ]);
    }

    #[test]
    fn merge3_mixed() {
        let entries = vec![
            vec![FieldEntry { value: 1.0, time: 1 }, FieldEntry { value: 2.0, time: 2 }, FieldEntry { value: 5.0, time: 3 }],
            vec![FieldEntry { value: 3.0, time: 2 }, FieldEntry { value: 4.0, time: 4 }],
        ];

        let records = merge_records3(entries, vec!["field1", "field2"]);
        assert_eq!(records.rows, vec![
            DataRow { time: 1, elements: vec![Some(1.0), None] },
            DataRow { time: 2, elements: vec![Some(2.0), Some(3.0)] },
            DataRow { time: 3, elements: vec![Some(5.0), None] },
            DataRow { time: 4, elements: vec![None, Some(4.0)] },
        ]);
    }


    #[test]
    fn it_writes_a_series_entry() {
        clear_tmp_files();

        let mut s = SeriesStorage::new("test_series");

        for _i in 0..ENTRIES_PER_BLOCK * 5 + 1 {
            let entry1 = SeriesEntry {
                fields: vec!["field1".to_owned(), "field2".to_owned()],
                values: vec![1.0, 2.0],
                time: new_timestamp(),
            };
            let entry2 = SeriesEntry {
                fields: vec!["field1".to_owned(), "field2".to_owned()],
                values: vec![1.0, -1.0],
                time: new_timestamp(),
            };

            s.insert(entry1);
            s.insert(entry2);
        }
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

        dbg!(&s.field_storages.get("field1").unwrap());
        let r = s.read(SelectQuery { series: "test_series", fields: vec!["field1", "field2"], start: None, end: None });
        dbg!(r.rows.len());

        dbg!(&s.field_storages.get("field2").unwrap());
        let r = s.read(SelectQuery { series: "test_series", fields: vec!["field1", "field2"], start: None, end: None });
        dbg!(r.rows.len());
    }


    // TODO: we can delete these after we've updated merge3 tests
    // #[test]
    // fn merge_aligned() {
    //     let entries = vec![
    //         vec![FieldEntry { value: 1.0, time: 1 }, FieldEntry { value: 2.0, time: 2 }],
    //         vec![FieldEntry { value: 3.0, time: 1 }, FieldEntry { value: 4.0, time: 2 }],
    //     ];
    //
    //     let records = merge_records(entries, vec!["field1", "field2"]);
    //     assert_eq!(records.len(), 2);
    //     assert_eq!(records[0], SeriesEntry { values: HashMap::from([(String::from("field1"), 1.0), (String::from("field2"), 3.0)]), time: 1 });
    //     assert_eq!(records[1], SeriesEntry { values: HashMap::from([(String::from("field1"), 2.0), (String::from("field2"), 4.0)]), time: 2 });
    // }
    //
    // #[test]
    // fn merge_alternating() {
    //     let entries = vec![
    //         vec![FieldEntry { value: 1.0, time: 1 }, FieldEntry { value: 2.0, time: 3 }],
    //         vec![FieldEntry { value: 3.0, time: 2 }, FieldEntry { value: 4.0, time: 4 }],
    //     ];
    //
    //     let records = merge_records(entries, vec!["field1", "field2"]);
    //     assert_eq!(records.len(), 4);
    //     assert_eq!(records[0], SeriesEntry { values: HashMap::from([(String::from("field1"), 1.0)]), time: 1 });
    //     assert_eq!(records[1], SeriesEntry { values: HashMap::from([(String::from("field2"), 3.0)]), time: 2 });
    //     assert_eq!(records[2], SeriesEntry { values: HashMap::from([(String::from("field1"), 2.0)]), time: 3 });
    //     assert_eq!(records[3], SeriesEntry { values: HashMap::from([(String::from("field2"), 4.0)]), time: 4 });
    // }
    //
    // #[test]
    // fn merge_mixed() {
    //     let entries = vec![
    //         vec![FieldEntry { value: 1.0, time: 1 }, FieldEntry { value: 2.0, time: 3 }, FieldEntry { value: 3.0, time: 4 }, FieldEntry { value: 4.0, time: 5 }],
    //         vec![FieldEntry { value: 3.0, time: 2 }, FieldEntry { value: 4.0, time: 4 }],
    //     ];
    //
    //     let records = merge_records(entries, vec!["field1", "field2"]);
    //     assert_eq!(records.len(), 5);
    //     assert_eq!(records[0], SeriesEntry { values: HashMap::from([(String::from("field1"), 1.0)]), time: 1 });
    //     assert_eq!(records[1], SeriesEntry { values: HashMap::from([(String::from("field2"), 3.0)]), time: 2 });
    //     assert_eq!(records[2], SeriesEntry { values: HashMap::from([(String::from("field1"), 2.0)]), time: 3 });
    //     assert_eq!(records[3], SeriesEntry { values: HashMap::from([(String::from("field1"), 3.0), (String::from("field2"), 4.0)]), time: 4 });
    //     assert_eq!(records[4], SeriesEntry { values: HashMap::from([(String::from("field1"), 4.0)]), time: 5 });
    // }
}