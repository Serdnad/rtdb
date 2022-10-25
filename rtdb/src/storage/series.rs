use std::fs::{create_dir_all, read_dir};
use std::io::{Read};
use std::io::ErrorKind::AlreadyExists;
use std::str;

use fnv::FnvHashMap;

use crate::{DataValue, RecordCollection};
use crate::lang::{Selection, SelectQuery};
use crate::storage::DEFAULT_DATA_DIR;
use crate::storage::field::{FieldEntry, FieldStorage};
use crate::wire_protocol::{DataType, FieldDescription};

/// A series entry is a collection of values, each corresponding to a different field under the
/// same series, all sharing the same timestamp.
#[derive(Debug, PartialEq, serde::Serialize)]
pub struct SeriesEntry {
    pub fields: Vec<String>,
    pub values: Vec<DataValue>,

    /// Timestamp as nanoseconds since Unix epoch
    pub time: i64,
}

#[derive(Debug, PartialEq)]
pub struct SeriesSummary<'a> {
    name: &'a str,
    fields: Vec<String>,
}

#[derive(Debug)]
pub struct SeriesStorage<'a> {
    pub(crate) series_name: &'a str,
    field_storages: FnvHashMap<String, FieldStorage>,
}

impl SeriesStorage<'_> {
    pub fn new(series_name: &str) -> SeriesStorage {
        if let Err(err) = create_dir_all(format!("{}/{}", DEFAULT_DATA_DIR, series_name)) {
            match err.kind() {
                AlreadyExists => {}
                _ => panic!("{}", err),
            }
        };

        SeriesStorage { series_name, field_storages: FnvHashMap::default() }
    }

    pub fn load(series_name: &str) -> SeriesStorage {
        let files = match read_dir(format!("{}/{}", DEFAULT_DATA_DIR, series_name)) {
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
                // TODO: figure out what data type a field is
                fields.push(FieldDescription { name: filename.to_owned(), data_type: DataType::Float });
            }
        }

        let field_storages = fields.iter().map(|f| (f.name.to_owned(), FieldStorage::load(series_name, &f.name))).collect();

        SeriesStorage {
            series_name,
            field_storages,
        }
    }

    pub fn read(&self, query: SelectQuery) -> RecordCollection {
        if Some(query.end) < Some(query.start) {
            return RecordCollection::empty();
        }


        let fields: Vec<_> = match query.selections.is_empty() {
            true => self.field_storages.values().collect(),
            false => {
                let field_names: Vec<_> = query.selections.iter().map(|f| {
                    match f {
                        Selection::Field(name) => { *name }
                        Selection::Expression(_) => { todo!() }
                    }
                }).collect();
                self.field_storages.values().filter(|&f| field_names.contains(&f.name.as_str())).collect()
            }
        };

        // if only the series is specified, select all fields unmodified
        let selections: Vec<_> = match query.selections.is_empty() {
            true => { self.field_storages.values().map(|f| Selection::Field(f.name.as_str())).collect() }
            false => { query.selections }
        };

        let records: Vec<Vec<FieldEntry>> = selections.iter().map(|selection| {
            match selection {

                // TODO: the selection should be passed down to the field storage, and that should
                //  be responsible for fetching its own data. I think...
                Selection::Field(field) => {
                    match self.field_storages.get(*field) {
                        Some(storage) => {
                            storage.read(query.start, query.end)
                        }
                        None => {
                            println!("Field not found!! :(");
                            vec![]
                        }
                    }
                }
                Selection::Expression(_) => { todo!() }
            }
        }).collect();

        merge_records(&records, &selections, &fields)
    }

    pub fn insert(&mut self, entry: SeriesEntry) {
        for i in 0..entry.fields.len() {
            let field = &entry.fields[i];
            let value = entry.values[i];

            match self.field_storages.get_mut(field) {
                None => {
                    let mut new_storage = FieldStorage::load(self.series_name, &field);
                    new_storage.insert(FieldEntry { value, time: entry.time });
                    self.field_storages.insert(field.to_owned(), new_storage);
                }
                Some(field_storage) => {
                    field_storage.insert(FieldEntry { value, time: entry.time })
                }
            }
        }
    }
}

/// Merge "columns" of fields into a single vector of records, sorting and matching entries by
/// their timestamp.
/// TODO: instead of outputting this into an intermediate result, these should get piped directly into the output
pub fn merge_records(entries: &Vec<Vec<FieldEntry>>, selections: &Vec<Selection>, fields: &Vec<&FieldStorage>) -> RecordCollection {
    // TODO: I don't this check does exactly what we want to do, but at some point we have to guard
    //  against empty results
    if entries.iter().all(|col| col.is_empty()) {
        return RecordCollection::empty();
    }

    let selection_count = selections.len();

    let max_min_rows = match entries.iter().map(|f| f.len()).min() {
        None => return RecordCollection::empty(),
        Some(min) => min,
    };

    // allocate a vector of (f + 1) * N elements, where f is field count and N is estimated row count
    let mut elements = Vec::with_capacity((selection_count + 1) * max_min_rows);

    let exhausted_field = FieldEntry { time: i64::MAX, value: DataValue::from(false) };
    let mut exhausted_count = 0;

    let mut next_elems: Vec<_> = entries.iter().map(|f| {
        let val = f.get(0);
        match val {
            Some(v) => v,
            None => {
                exhausted_count += 1;
                &exhausted_field
            }
        }
    }).collect();

    let mut indices = vec![0; entries.len()];


    'outer: loop {
        let mut earliest = next_elems[0].time;
        for &e in &next_elems[1..] {
            if e.time < earliest {
                earliest = e.time;
            }
        }

        elements.push(DataValue::Timestamp(earliest));

        for i in 0..selection_count {
            let entry = next_elems[i];

            if entry.time == earliest {
                elements.push(entry.value);
                indices[i] += 1;

                if indices[i] == entries[i].len() {
                    exhausted_count += 1;
                    if exhausted_count == selection_count {
                        break 'outer;
                    }

                    next_elems[i] = &exhausted_field;
                } else {
                    next_elems[i] = &entries[i][indices[i]];
                }
            } else {
                elements.push(DataValue::None);
            }
        }
    }

    let fields = fields.iter().map(|&field_storage| {
        FieldDescription { name: field_storage.name.clone(), data_type: field_storage.data_type.clone() }
    }).collect();

    RecordCollection { fields, elements }
}


#[cfg(test)]
mod tests {
    use std::fs;

    use crate::DataValue;
    use crate::lang::{Selection, SelectQuery};
    use crate::storage::field_block::ENTRIES_PER_BLOCK;
    use crate::storage::series::{SeriesEntry, SeriesStorage};
    use crate::util::new_timestamp;

    fn clear_tmp_files() {
        fs::remove_dir("test_series");
    }

    // TODO: update these tests when we're including timestamps
    // #[test]
    // fn merge_aligned() {
    //     let entries = vec![
    //         vec![FieldEntry { value: DataValue::from(1.0), time: 1 }, FieldEntry { value: DataValue::from(2.0), time: 2 }],
    //         vec![FieldEntry { value: DataValue::from(3.0), time: 1 }, FieldEntry { value: DataValue::from(4.0), time: 2 }],
    //     ];
    //
    //     let records = merge_records(&entries, &vec!["field1", "field2"]);
    //     assert_eq!(records.elements,
    //                vec![DataValue::Timestamp(1), DataValue::from(1.0), DataValue::from(3.0),
    //                     DataValue::Timestamp(2), DataValue::from(2.0), DataValue::from(4.0)]);
    // }
    //
    // #[test]
    // fn merge_mixed() {
    //     let entries = vec![
    //         vec![FieldEntry { value: DataValue::from(1.0), time: 1 }, FieldEntry { value: DataValue::from(2.0), time: 2 }, FieldEntry { value: DataValue::from(5.0), time: 3 }],
    //         vec![FieldEntry { value: DataValue::from(3.0), time: 2 }, FieldEntry { value: DataValue::from(4.0), time: 4 }],
    //     ];
    //
    //     let records = merge_records(&entries, &vec!["field1", "field2"]);
    //     assert_eq!(records.elements, vec![
    //         DataValue::Timestamp(1), DataValue::from(1.0), DataValue::None,
    //         DataValue::Timestamp(2), DataValue::from(2.0), DataValue::from(3.0),
    //         DataValue::Timestamp(3), DataValue::from(5.0), DataValue::None,
    //         DataValue::Timestamp(4), DataValue::None, DataValue::from(4.0),
    //     ]);
    // }
    //
    //
    // #[test]
    // fn merge_3_mixed() {
    //     let entries = vec![
    //         vec![FieldEntry { value: DataValue::from(1.0), time: 1 }, FieldEntry { value: DataValue::from(2.0), time: 2 }, FieldEntry { value: DataValue::from(5.0), time: 3 }],
    //         vec![FieldEntry { value: DataValue::from(3.0), time: 2 }, FieldEntry { value: DataValue::from(4.0), time: 4 }],
    //         vec![FieldEntry { value: DataValue::from(3.0), time: 2 }, FieldEntry { value: DataValue::from(4.0), time: 4 }],
    //     ];
    //
    //     let records = merge_records(&entries, &vec!["field1", "field2", "field3"]);
    //     assert_eq!(records.elements, vec![
    //         DataValue::Timestamp(1), DataValue::from(1.0), DataValue::None, DataValue::None,
    //         DataValue::Timestamp(2), DataValue::from(2.0), DataValue::from(3.0), DataValue::from(3.0),
    //         DataValue::Timestamp(3), DataValue::from(5.0), DataValue::None, DataValue::None,
    //         DataValue::Timestamp(4), DataValue::None, DataValue::from(4.0), DataValue::from(4.0),
    //     ]);
    // }


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
        let _r = s.read(SelectQuery {
            series: "test_series",
            selections: vec![Selection::Field("field1")],
            start: None,
            end: None,
        });
        // dbg!(r.rows.len());

        dbg!(&s.field_storages.get("field2").unwrap());
        let _r = s.read(SelectQuery {
            series: "test_series",
            selections: vec![Selection::Field("field2")],
            start: None,
            end: None,
        });
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