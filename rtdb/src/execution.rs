use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use fnv::FnvHashMap;
use serde::Serialize;

use crate::lang::{Action, SelectQuery};
use crate::lang::insert::Insertion;
use crate::{ClientRecordCollection, RecordCollection};
use crate::storage::series::SeriesStorage;

pub struct ExecutionEngine<'a> {
    series_storages: Arc<Mutex<FnvHashMap<String, SeriesStorage<'a>>>>,
}

#[derive(Serialize)]
pub enum ExecutionResult {
    Query(QueryResult),
    Insert(InsertionResult),
}

#[derive(Debug, Serialize)]
pub struct QueryResult {
    pub count: usize,
    // pub fields: Vec<String>,// already in record collection
    pub records: RecordCollection,
}


/// TODO: move and rename
#[derive(Debug, Serialize)]
pub struct ClientQueryResult {
    pub count: usize,
    pub records: ClientRecordCollection,
}

#[derive(Serialize)]
pub struct InsertionResult {
    pub success: bool,
    // pub error: Option<String>,
    // pub rows_inserted: u32,
}

impl ExecutionEngine<'_> {
    pub fn new<'a>() -> ExecutionEngine<'a> {
        ExecutionEngine { series_storages: Arc::new(Mutex::new(HashMap::default())) }
    }

    pub fn execute(&self, action: Action) -> ExecutionResult {
        match action {
            Action::Select(query) => self.execute_select(query),
            Action::Insert(insertion) => self.execute_insert(insertion),
        }
    }

    fn execute_select(&self, query: SelectQuery) -> ExecutionResult {
        // TODO: tmp
        let mut storages = self.series_storages.lock().unwrap();
        match storages.get(&query.series.to_owned()) {
            Some(storage) => {
                let records = storage.read(query);
                let count = records.len();
                ExecutionResult::Query(QueryResult { records, count })
            }
            None => {
                let series_name = (&query).series.to_owned();

                let storage = SeriesStorage::load("test_series"); // TODO: BAD
                let records = storage.read(query);
                let count = &records.len();
                let result = ExecutionResult::Query(QueryResult { records, count: *count });

                storages.insert(series_name, storage); // TODO: bad

                result
            }
        }
    }

    fn execute_insert(&self, insertion: Insertion) -> ExecutionResult {
        let mut storages = self.series_storages.lock().unwrap();
        if !storages.contains_key(&insertion.series.to_owned()) {
            println!("LOAD SERIES");
            storages.insert(insertion.series.to_owned(), SeriesStorage::load("test_series")); // TODO: bad
        }

        let storage = storages.get_mut(&insertion.series.to_owned()).unwrap();
        storage.insert(insertion.entry.into());

        ExecutionResult::Insert(InsertionResult { success: true })
    }
}
