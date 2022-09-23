use std::collections::HashMap;

use std::sync::{Arc, Mutex};

use serde::Serialize;


use crate::lang::{Action, SelectQuery};
use crate::lang::insert::Insertion;
use crate::storage::series::{RecordCollection, SeriesStorage};

mod query;

pub struct ExecutionEngine<'a> {
    series_storages: Arc<Mutex<HashMap<String, SeriesStorage<'a>>>>,
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

#[derive(Serialize)]
pub struct InsertionResult {
    // pub rows_inserted: u32,
}

impl ExecutionEngine<'_> {
    pub fn new<'a>() -> ExecutionEngine<'a> {
        ExecutionEngine { series_storages: Arc::new(Mutex::new(HashMap::new())) }
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
        if !storages.contains_key(&query.series.to_owned()) {
            println!("LOAD SERIES");
            storages.insert(query.series.to_owned(), SeriesStorage::load("test_series")); // TODO: bad
        }

        let storage = &storages[&query.series.to_owned()];
        let records = storage.read(query);
        let count = &records.rows.len();

        ExecutionResult::Query(QueryResult { records, count: *count })
    }

    fn execute_insert(&self, insertion: Insertion) -> ExecutionResult {
        let mut storages = self.series_storages.lock().unwrap();
        if !storages.contains_key(&insertion.series.to_owned()) {
            println!("LOAD SERIES");
            storages.insert(insertion.series.to_owned(), SeriesStorage::load("test_series")); // TODO: bad
        }

        let storage = storages.get_mut(&insertion.series.to_owned()).unwrap();
        storage.insert(insertion.entry.into());

        ExecutionResult::Insert(InsertionResult {})
    }
}
