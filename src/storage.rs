use std::fmt::Debug;
use std::str::FromStr;

mod data_parser;
pub mod series;
pub mod field;
mod float_field;
pub mod field_block;
pub mod field_index;

pub trait SupportedDataType: FromStr + Debug {}

impl SupportedDataType for bool {}

impl SupportedDataType for i64 {}

impl SupportedDataType for f64 {}

impl SupportedDataType for String {}
