use std::fs::File;
use std::io::Write;
use std::mem::size_of;
use std::os::unix::fs::FileExt;

use nom::AsBytes;
use rkyv::{Deserialize};

use crate::storage::field::FieldEntry;
use crate::storage::field_index::FieldStorageBlockSummary;

const PTR_SIZE: usize = std::mem::size_of::<usize>();

/// The size of each block.
///
/// A block is simply a vector of field entries. As such, the size of a block is the number of
/// entries by the size of each entry, plus the size of a usize used to keep the vector's length.
///
/// We use certain powers of 2 to align with typical page sizes. In the future, this may be
/// configurable.
/// TODO: use at last 4096 after we're done testing.
const BLOCK_SIZE: usize = 2400 + PTR_SIZE;

/// The size of each entry in 8-bit bytes.
const ENTRY_SIZE: usize = size_of::<FieldEntry>();

/// The max number of entries recorded in a single block.
pub(crate) const ENTRIES_PER_BLOCK: usize = (BLOCK_SIZE - 8) / ENTRY_SIZE;

// block, with header and stuff
// inspiration: https://docs.influxdata.com/influxdb/v1.5/concepts/storage_engine/#compression
/// A block of measurements for a single field, under a single series.
/// Measurements are first stored in a block, and once a block fills, it is flushed to disk.
#[derive(Debug, PartialEq)]
pub struct FieldStorageBlock {
    pub(crate) entries: Vec<FieldEntry>,
}

impl FieldStorageBlock {
    pub fn new() -> FieldStorageBlock {
        FieldStorageBlock {
            entries: vec![],
        }
    }

    pub fn read(&self, from: Option<i64>, until: Option<i64>) -> Vec<FieldEntry> {
        if from.is_none() && until.is_none() {
            return self.entries[..].to_vec();
        }


        // TODO: validate input
        // if Some(from) > Some(until) {
        //     println!("UH OH!");
        //     return vec![];
        // }


        let mut start_index = 0;
        if let Some(from) = from {
            // TODO: check against block bounds

            // TODO: implement binary search for this
            let index = self.entries.iter().position(|entry| entry.time >= from);
            start_index = match index {
                None => 0,
                Some(index) => index,
            }
        }

        let mut end_index = self.entries.len();
        if let Some(until) = until {
            // TODO: check against block bounds

            // TODO: implement binary search for this
            let index = self.entries.iter().rposition(|entry| entry.time <= until);
            end_index = match index {
                None => self.entries.len(),
                Some(index) => index,
            }
        }

        // TODO: find start index,
        // TODO: find end index

        self.entries[start_index..end_index].to_vec()
    }

    /// Whether the block has space for accepting a new record.
    #[inline]
    pub fn has_space(&self) -> bool {
        self.entries.len() < ENTRIES_PER_BLOCK
    }

    /// Insert entry into collection.
    pub fn insert(&mut self, entry: FieldEntry) {
        // TODO: insert in order. check if it's latest, and if not use binary search
        self.entries.push(entry);
    }

    pub fn load(file: &File, block_offset: usize) -> FieldStorageBlock {
        let mut bytes = [0; BLOCK_SIZE];

        if let Err(_error) = file.read_exact_at(&mut bytes, (block_offset * BLOCK_SIZE) as u64) {
            dbg!(_error);
            return FieldStorageBlock { entries: vec![] };
        };

        let archived = rkyv::check_archived_root::<Vec<FieldEntry>>(&bytes).unwrap();
        let deserialized: Vec<FieldEntry> = archived.deserialize(&mut rkyv::Infallible).unwrap();
        FieldStorageBlock {
            entries: deserialized,
        }
    }

    /// Flush the block's data entries to out.
    pub fn write_data<T: Write>(&self, out: &mut T) {
        // TODO: at some point we need to optimize timestamps for storage, and data too

        let bytes = rkyv::to_bytes::<_, 4096>(&self.entries).expect("failed to serialize Field Entries");
        // let deserialized = rkyv::from_bytes::<Vec<FieldEntry>>(&bytes).expect("failed to deserialize vec");
        // dbg!(deserialized);


        out.write(bytes.as_bytes());
    }

    /// Flush the block's summary to out, typically an index file.
    pub fn write_summary<T: Write>(&self, out: &mut T) {
        let summary = FieldStorageBlockSummary::from_entries(&self.entries);
        summary.write(out);
    }
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use crate::DataValue;

    use crate::storage::field::{FieldEntry, FieldStorage};
    use crate::storage::field_block::FieldStorageBlock;

    #[test]
    fn it_reads_a_block() {
        let f = File::open("test_series/field2").unwrap();

        // TODO: use premade, fixed test shim files, and then just assert against slices
        let s = FieldStorageBlock::load(&f, 0);
        dbg!(&s);
        assert_eq!(s.entries.len(), 10);

        let values = s.read(None, None);
        // assert_eq!(values.len(), 100);
        assert_eq!(values[0].value, DataValue::from(123.0));
        assert_eq!(values[0].time, 1662352954755105835);

        let values = s.read(Some(1662352954755112508), None);
        // assert_eq!(values.len(), 4);
        assert_eq!(values[0].value, DataValue::from(5.0));
        assert_eq!(values[0].time, 1662352954755112508);

        let values = s.read(None, Some(1662352954755112608));
        assert_eq!(values.len(), 7);
        // assert_eq!(values[values.len() - 1].value, DataValue::from(5.0));
        assert_eq!(values[values.len() - 1].time, 1662352954755112508);

        let values = s.read(Some(1662352954755112508), Some(1662352954755113199));
        // assert_eq!(values.len(), 3);
        assert_eq!(values[values.len() - 1].value, 5.0);
        assert_eq!(values[values.len() - 1].time, 1662352954755112708);
    }

    #[test]
    fn it_reads2() {
        let f = File::open("test_series_value1").unwrap();

        let _s = FieldStorage::load("test_series", "value1");
        let s = FieldStorageBlock::load(&f, 0);
        dbg!(s);

        let s2 = FieldStorageBlock::load(&f, 1);
        dbg!(s2);
        // s.insert(Entry { value: 123, time: time::UNIX_EPOCH.elapsed().unwrap().as_nanos() });
    }
}