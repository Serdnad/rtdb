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
    // TODO: We should also store min, max, and mean, for faster aggregating
    pub start_timestamp: i64,
    pub latest_timestamp: i64,
}

const SUMMARY_BLOCK_SIZE: usize = size_of::<FieldStorageBlockSummary>();

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
        let bytes = read(path).unwrap();

        let mut summaries = Vec::with_capacity(bytes.len() / SUMMARY_BLOCK_SIZE);

        for offset in (0..bytes.len()).step_by(SUMMARY_BLOCK_SIZE) {
            let summary = rkyv::from_bytes::<FieldStorageBlockSummary>(&bytes[offset..offset + SUMMARY_BLOCK_SIZE]).unwrap();
            summaries.push(summary);
        }

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