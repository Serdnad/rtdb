use serde::{Serialize};

use crate::lang::{Action, Insertion, SelectQuery};
use crate::lang::Action::Insert;
use crate::storage::series::{SeriesEntry, SeriesStorage};

mod query;

pub struct ExecutionEngine {}

#[derive(Serialize)]
pub enum ExecutionResult {
    Query(QueryResult),
    Insert(InsertionResult),
}

#[derive(Serialize)]
pub struct QueryResult {
    pub count: usize,
    pub records: Vec<SeriesEntry>,
}

#[derive(Serialize)]
pub struct InsertionResult {
    // pub rows_inserted: u32,
}

struct SeriesStorageCache {}

impl ExecutionEngine {
    pub fn execute(action: Action) -> ExecutionResult {
        match action {
            Action::Select(query) => ExecutionEngine::execute_select(query),
            Action::Insert(insertion) => ExecutionEngine::execute_insert(insertion),
        }
    }

    fn execute_select(query: SelectQuery) -> ExecutionResult {
        // TODO: keep series storages so we don't have to load them all the time
        let storage = SeriesStorage::load(query.series);
        let records = storage.read(query);

        let count = &records.len();
        ExecutionResult::Query(QueryResult { records, count: *count })
    }


    fn execute_insert(query: Insertion) -> ExecutionResult {
        // TODO: keep series storages so we don't have to load them all the time
        let mut storage = SeriesStorage::load(query.series);
        storage.insert(query.into());

        ExecutionResult::Insert(InsertionResult {})
    }
}
