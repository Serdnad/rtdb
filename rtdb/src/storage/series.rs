use std::collections::HashMap;
use std::fs::{create_dir, OpenOptions, read, read_dir};
use std::io::{Read, Write};
use std::io::ErrorKind::AlreadyExists;
use std::str;
use crate::DataValue;

use crate::lang::SelectQuery;
use crate::storage::field::{FieldEntry, FieldStorage};
use crate::wire_protocol::{DataType, FieldDescription};


/// A series entry is a collection of values, each corresponding to a different field under the
/// same series, all sharing the same timestamp.
#[derive(Debug, PartialEq, serde::Serialize)]
pub struct SeriesEntry {
    pub fields: Vec<String>,
    pub values: Vec<DataValue>,

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

        let field_storages = fields.iter().map(|f| (f.to_owned(), FieldStorage::load(series_name, f))).collect();

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

        merge_records(&records, &fields)
    }

    pub fn insert(&mut self, entry: SeriesEntry) {
        for i in 0..entry.fields.len() {
            let field = &entry.fields[i];
            let value = entry.values[i].clone();

            if !self.field_storages.contains_key(field) {
                let new_storage = FieldStorage::load(self.series_name, &field);
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
    pub fields: Vec<FieldDescription>, // TODO: maybe we should insert field types into here, or actually maybe we can get them from the series? hm
    pub rows: Vec<DataRow>,
}

// TODO: move
#[derive(Debug, serde::Serialize, PartialEq)]
pub struct DataRow {
    pub time: i64,
    pub elements: Vec<Option<DataValue>>,
}

// TODO [urgent]: this doesn't include timestamps...
pub fn merge_records(entries: &Vec<Vec<FieldEntry>>, fields: &Vec<&str>) -> RecordCollection {
    let field_count = fields.len();

    let max_min_rows = match entries.iter().map(|f| f.len()).min() {
        None => return RecordCollection { fields: vec![], rows: vec![] },
        Some(min) => min,
    };
    let mut rows = Vec::with_capacity(max_min_rows); // TODO: pick the max of all field lens

    let mut next_elems: Vec<_> = entries.iter().map(|f| &f[0]).collect();
    let mut indices = vec![0; entries.len()];

    let exhausted_field = FieldEntry { time: i64::MAX, value: DataValue::from(false) };
    let mut exhausted_count = 0;

    'outer: loop {
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

                if indices[i] == entries[i].len() {
                    exhausted_count += 1;
                    if exhausted_count == field_count {
                        rows.push(DataRow { time: earliest, elements: elems });
                        break 'outer;
                    }

                    next_elems[i] = &exhausted_field;
                } else {
                    next_elems[i] = &entries[i][indices[i]];
                }
            } else {
                elems.push(None);
            }
        }

        rows.push(DataRow { time: earliest, elements: elems });
    }

    // TODO: instead, we should be passing this in from above. the series should know what type each
    //  field is.
    let names: Vec<_> = fields.iter().map(|&s| s.to_owned()).collect();
    let fields = entries.iter().enumerate().map(|(i, field)| {
        let data_type = match field[0].value {
            DataValue::Bool(_) => DataType::Bool,
            DataValue::Float(_) => DataType::Float,
        };

        FieldDescription { name: names[i].to_owned(), data_type }
    }).collect();

    RecordCollection { fields , rows }
}


#[cfg(test)]
mod tests {
    use std::{fs};
    use crate::DataValue;
    use crate::lang::SelectQuery;
    use crate::storage::field::FieldEntry;
    use crate::storage::field_block::ENTRIES_PER_BLOCK;
    use crate::storage::series::{DataRow, merge_records, SeriesEntry, SeriesStorage};
    use crate::util::new_timestamp;

    fn clear_tmp_files() {
        fs::remove_dir("test_series");
    }

    // TODO: update these tests when we're including timestamps
    #[test]
    fn merge() {
        let entries = vec![
            vec![FieldEntry { value: DataValue::from(1.0), time: 1 }, FieldEntry { value: DataValue::from(2.0), time: 2 }],
            vec![FieldEntry { value: DataValue::from(3.0), time: 1 }, FieldEntry { value: DataValue::from(4.0), time: 2 }],
        ];

        let records = merge_records(&entries, &vec!["field1", "field2"]);
        assert_eq!(records.rows, vec![
            DataRow { time: 1, elements: vec![Some(DataValue::from(1.0)), Some(DataValue::from(3.0))] },
            DataRow { time: 2, elements: vec![Some(DataValue::from(2.0)), Some(DataValue::from(4.0))] },
        ]);
    }

    #[test]
    fn merge_mixed() {
        let entries = vec![
            vec![FieldEntry { value: DataValue::from(1.0), time: 1 }, FieldEntry { value: DataValue::from(2.0), time: 2 }, FieldEntry { value: DataValue::from(5.0), time: 3 }],
            vec![FieldEntry { value: DataValue::from(3.0), time: 2 }, FieldEntry { value: DataValue::from(4.0), time: 4 }],
        ];

        let records = merge_records(&entries, &vec!["field1", "field2"]);
        assert_eq!(records.rows, vec![
            DataRow { time: 1, elements: vec![Some(DataValue::from(1.0)), None] },
            DataRow { time: 2, elements: vec![Some(DataValue::from(2.0)), Some(DataValue::from(3.0))] },
            DataRow { time: 3, elements: vec![Some(DataValue::from(5.0)), None] },
            DataRow { time: 4, elements: vec![None, Some(DataValue::from(4.0))] },
        ]);
    }


    #[test]
    fn merge_3_mixed() {
        let entries = vec![
            vec![FieldEntry { value: DataValue::from(1.0), time: 1 }, FieldEntry { value: DataValue::from(2.0), time: 2 }, FieldEntry { value: DataValue::from(5.0), time: 3 }],
            vec![FieldEntry { value: DataValue::from(3.0), time: 2 }, FieldEntry { value: DataValue::from(4.0), time: 4 }],
            vec![FieldEntry { value: DataValue::from(3.0), time: 2 }, FieldEntry { value: DataValue::from(4.0), time: 4 }],
        ];

        let records = merge_records(&entries, &vec!["field1", "field2", "field3"]);
        assert_eq!(records.rows, vec![
            DataRow { time: 1, elements: vec![Some(DataValue::from(1.0)), None, None] },
            DataRow { time: 2, elements: vec![Some(DataValue::from(2.0)), Some(DataValue::from(3.0)), Some(DataValue::from(3.0))] },
            DataRow { time: 3, elements: vec![Some(DataValue::from(5.0)), None, None] },
            DataRow { time: 4, elements: vec![None, Some(DataValue::from(4.0)), Some(DataValue::from(4.0))] },
        ]);
    }


    #[test]
    fn it_writes_a_series_entry() {
        clear_tmp_files();

        let mut s = SeriesStorage::new("test_series");

        for _i in 0..ENTRIES_PER_BLOCK * 5 + 1 {
            let entry1 = SeriesEntry {
                fields: vec!["field1".to_owned(), "field2".to_owned()],
                values: vec![DataValue::from(1.0), DataValue::from(false)],
                time: new_timestamp(),
            };
            let entry2 = SeriesEntry {
                fields: vec!["field1".to_owned(), "field2".to_owned()],
                values: vec![DataValue::from(1.0), DataValue::from(true)],
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


    // TODO: we can delete these after we've updated merge tests
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