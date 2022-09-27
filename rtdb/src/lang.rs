

use crate::lang::insert::Insertion;

mod util;
pub mod query;
pub mod insert;

#[derive(Debug, PartialEq)]
pub enum Action<'a> {
    Select(SelectQuery<'a>),
    Insert(Insertion),
}

#[derive(Debug, PartialEq)]
pub struct SelectQuery<'a> {
    pub series: &'a str,
    pub fields: Vec<&'a str>,

    pub start: Option<i64>,
    pub end: Option<i64>,

    // TODO: filters, group by
}