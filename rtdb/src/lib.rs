extern crate core;

use std::fmt::{Display, Formatter};

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
    pub rows: Vec<DataRow>,
}

#[derive(Debug, serde::Serialize, PartialEq)]
pub struct DataRow {
    pub time: i64,
    pub elements: Vec<Option<DataValue>>,
}

#[derive(Archive, Copy, Clone, Deserialize, Serialize, Debug, PartialEq, serde::Serialize)]
#[archive(compare(PartialEq))]
#[archive_attr(derive(CheckBytes, Debug))]
pub enum DataValue {
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
            DataValue::Bool(bool) => write!(f, "{}", bool),
            DataValue::Float(float) => write!(f, "{}", float)
        }
    }
}