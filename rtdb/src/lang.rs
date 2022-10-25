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
    pub selections: Vec<Selection<'a>>,

    pub start: Option<i64>,
    pub end: Option<i64>,

    // TODO: filters, group by, sort by, where
}

#[derive(Debug, PartialEq)]
pub enum Aggregation {
    Mean,
    Last,
    First,
    Min,
    Max,
}

#[derive(Debug, PartialEq)]
pub enum Selection<'a> {
    Field(&'a str),
    Expression(Box<SelectExpression<'a>>),
}


#[derive(Debug, PartialEq)]
pub struct SelectExpression<'a> {
    pub expression: Selection<'a>,
    pub aggregator: Aggregation,
}
//
// #[derive(Debug, PartialEq)]
// pub struct FieldSelection<'a> {
//     pub name: &'a str,
//     pub aggregator: Aggregation,
// }