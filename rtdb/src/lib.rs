extern crate core;

use std::fmt::{Display, Formatter, write};


use bytecheck::CheckBytes;
use rkyv::{Archive, Deserialize, Serialize};
use crate::wire_protocol::FieldDescription;

pub mod storage;
pub mod execution;
pub mod lang;
pub mod util;
pub mod wire_protocol;
pub mod network;
pub mod users;


#[derive(Debug, serde::Serialize, PartialEq)]
pub struct RecordCollection {
    pub fields: Vec<FieldDescription>,

    // elements is effectively a 2D matrix of entries, stored as a 1D vector for performance reasons.
    // If a record collection is of N rows and M fields (not including timestamp), then elements will have length
    // N * (N + 1)
    pub elements: Vec<DataValue>,
}

impl RecordCollection {
    #[inline(always)]
    pub fn empty() -> RecordCollection {
        RecordCollection{ fields: vec![], elements: vec![] }
    }

    /// Return the number of rows in this record collection.
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.elements.len() / (self.fields.len() + 1)
    }
}

// TODO: move to client side and rename
#[derive(Debug, serde::Serialize, PartialEq)]
pub struct ClientRecordCollection {
    pub fields: Vec<FieldDescription>,

    pub rows: Vec<DataRow>,
}


#[derive(Debug, serde::Serialize, PartialEq, Clone)]
pub struct DataRow {
    pub time: i64,
    pub elements: Vec<DataValue>,
}

impl DataRow {
    pub fn with_capacity(num_elems: usize) -> DataRow {
        DataRow{ time: 0, elements: Vec::with_capacity(num_elems) }
    }
}

#[derive(Archive, Copy, Clone, Deserialize, Serialize, Debug, PartialEq, serde::Serialize)]
#[archive(compare(PartialEq))]
#[archive_attr(derive(CheckBytes, Debug))]
pub enum DataValue {
    None,
    Timestamp(i64),
    Bool(bool),
    Float(f64),
}

impl From<bool> for DataValue {
    fn from(b: bool) -> Self {
        DataValue::Bool(b)
    }
}

impl From<f64> for DataValue {
    fn from(f: f64) -> Self {
        DataValue::Float(f)
    }
}

impl DataValue {
    #[inline]
    pub fn to_be_bytes(self) -> Vec<u8> {
        match self {
            DataValue::None => vec![],
            DataValue::Timestamp(t) => t.to_be_bytes().to_vec(),
            DataValue::Bool(b) => vec![b as u8],
            DataValue::Float(f) => f.to_be_bytes().to_vec()
        }
    }
}

impl PartialEq<f64> for DataValue {
    fn eq(&self, other: &f64) -> bool {
        match self {
            DataValue::Float(f) => f == other,
            _ => false,
        }
    }
}

impl std::fmt::Display for DataValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            DataValue::None => write!(f, "NONE"),
            DataValue::Timestamp(t) => write!(f, "{}", t), // TODO: could format more nicely?
            DataValue::Bool(bool) => write!(f, "{}", bool),
            DataValue::Float(float) => write!(f, "{}", float)
        }
    }
}