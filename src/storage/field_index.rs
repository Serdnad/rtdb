use core::mem::size_of;
use std::fs::read;
use std::io::Write;

use bytecheck::CheckBytes;
use nom::AsBytes;
use rkyv::{Archive, Deserialize, Serialize};

use crate::storage::field::FieldEntry;

#[derive(Archive, Clone, Deserialize, Serialize, Debug, PartialEq)]
#[archive(compare(PartialEq))]
#[archive_attr(derive(CheckBytes, Debug))]
pub struct FieldStorageBlockSummary {
    pub(crate) start_timestamp: u128,
    pub(crate) latest_timestamp: u128,
}

impl FieldStorageBlockSummary {
    pub fn from_entries(entries: &Vec<FieldEntry>) -> FieldStorageBlockSummary {
        let earliest = entries[0].time;
        let latest = entries[entries.len() - 1].time;

        FieldStorageBlockSummary {
            start_timestamp: earliest,
            latest_timestamp: latest,
        }
    }

    /// Load all block summaries from a given index file.
    /// TODO: there may? be a faster way to do this
    pub fn load_all(path: &str) -> Vec<FieldStorageBlockSummary> {
        let mut bytes = read(path).unwrap();

        // dbg!(&bytes);
        //
        let mut summaries = Vec::with_capacity(bytes.len() / size_of::<FieldStorageBlockSummary>());


        for offset in (0..bytes.len()).step_by(32) {
            let summary = rkyv::from_bytes::<FieldStorageBlockSummary>(&bytes[offset..offset + 32]).unwrap();
            summaries.push(summary);
        }

        // let summaries = rkyv::from_bytes::<Vec<FieldStorageBlockSummary>>(&bytes).unwrap();

        summaries
    }

    pub fn write<T: Write>(&self, out: &mut T) {
        let bytes = rkyv::to_bytes::<_, 4096>(self).expect("failed to serialize block summary");
        out.write(bytes.as_bytes());
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::field_index::FieldStorageBlockSummary;

    #[test]
    fn load() {
        // TODO: shim file
        let summaries = FieldStorageBlockSummary::load_all("test_series_value1_index");
        dbg!(summaries);
    }
}