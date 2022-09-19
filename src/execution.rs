use std::collections::HashMap;
use std::hash::Hash;

use serde::Serialize;
use tokio::time;

use crate::lang::{Action, Insertion, SelectQuery};
use crate::storage::series::{RecordCollection, SeriesEntry, SeriesStorage};

mod query;

pub struct ExecutionEngine<'a> {
    series_storages: HashMap<String, SeriesStorage<'a>>,
}

#[derive(Serialize)]
pub enum ExecutionResult {
    Query(QueryResult),
    Insert(InsertionResult),
}

#[derive(Serialize)]
pub struct QueryResult {
    pub count: usize,
    // pub fields: Vec<String>
    pub records: RecordCollection,
}

#[derive(Serialize)]
pub struct InsertionResult {
    // pub rows_inserted: u32,
}

impl ExecutionEngine<'_> {
    pub fn new<'a>() -> ExecutionEngine<'a> {
        ExecutionEngine { series_storages: HashMap::new() }
    }

    pub fn execute(&mut self, action: Action) -> ExecutionResult {
        match action {
            Action::Select(query) => self.execute_select(query),
            Action::Insert(insertion) => self.execute_insert(insertion),
        }
    }

    fn execute_select(&mut self, query: SelectQuery) -> ExecutionResult {
        // TODO: tmp
        if !self.series_storages.contains_key(&query.series.to_owned()) {
            println!("LOAD SERIES");
            self.series_storages.insert(query.series.to_owned(), SeriesStorage::load("test_series")); // TODO: bad
        }


        // TODO: keep series storages so we don't have to load them all the time
        let storage = &self.series_storages[&query.series.to_owned()];


        // let start = time::Instant::now();

        let records = storage.read(query);


        // let elapsed = start.elapsed();
        // println!("{}us", elapsed.as_micros());

        let count = &records.rows.len();
        ExecutionResult::Query(QueryResult { records, count: *count })
    }


    fn execute_insert(&self, query: Insertion) -> ExecutionResult {
        // TODO: keep series storages so we don't have to load them all the time
        let mut storage = SeriesStorage::load(query.series);
        storage.insert(query.into());

        ExecutionResult::Insert(InsertionResult {})
    }
}
