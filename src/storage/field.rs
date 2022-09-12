use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::fs::FileExt;

// bytecheck can be used to validate your data if you want
use bytecheck::CheckBytes;
use rkyv::{Archive, Deserialize, Serialize};

use crate::storage::field_block::FieldStorageBlock;
use crate::storage::field_index::FieldStorageBlockSummary;
use crate::storage::SupportedDataType;

#[derive(Archive, Clone, Deserialize, Serialize, Debug, PartialEq)]
#[archive(compare(PartialEq))]
#[archive_attr(derive(CheckBytes, Debug))]
pub struct FieldEntry {
    // Timestamp as nanoseconds since Unix epoch
    pub time: u128,

    // TODO: type tmp
    pub value: f64,
}


#[derive(Debug)]
pub struct FieldStorage {
    series_name: String,
    field_name: String,

    data_file_handle: File,
    index_file_handle: File,

    block_summaries: Vec<FieldStorageBlockSummary>,

    // buffer to be written as a block
    curr_block: FieldStorageBlock,
}

impl FieldStorage {
    pub fn new<'a>(series_name: &str, field_name: &str) -> FieldStorage {
        let (data_file, index_file) = FieldStorage::get_files(series_name, field_name, true);

        // TODO: reorganize, this is redundant
        let filename = format!("{}_{}", series_name, field_name);
        let index_filename = format!("{}_index", filename);
        let summaries = FieldStorageBlockSummary::load_all(&index_filename);

        FieldStorage {
            series_name: series_name.to_owned(),
            field_name: field_name.to_owned(),
            block_summaries: summaries,
            curr_block: FieldStorageBlock::new(),
            data_file_handle: data_file,
            index_file_handle: index_file,
        }
    }

    pub fn read(&self) -> Vec<FieldEntry> {
        // TODO: use a modified binary search to narrow down which blocks we scan, using summaries
        let start_block = 0;
        let end_block = self.block_summaries.len();

        let records = (start_block..end_block + 1).flat_map(|offset| {
            let block = FieldStorageBlock::load(&self.data_file_handle, offset);
            block.read(None, None)
        }).collect();

        records

        // for (i, summary) in self.block_summaries.iter().enumerate() {
        //     let block = FieldStorageBlock::load(&self.data_file_handle, i);
        //     let entries = block.read(None, None);
        //     dbg!(entries);
        //     // let summary = &self.block_summaries[i];
        //
        //     // FieldStorageBlock::load(self.data_file_handle)
        //     //
        //     //
        // }
        //
        // vec![]

        // let results = self.curr_block.read(None, None);
        // dbg!(results);
        // Ok(vec![])
    }

    pub fn insert(&mut self, entry: FieldEntry) {
        // we first attempt to write to the current block, and only write to disk if the block is
        // filled. TODO: what does this mean for data reliability?. TODO: move into curr block
        match self.curr_block.has_space() {
            true => self.curr_block.insert(entry),
            false => {
                // println!("WRITE");
                self.curr_block.write_data(&mut self.data_file_handle);
                self.curr_block.write_summary(&mut self.index_file_handle);

                self.curr_block = FieldStorageBlock::new();
                self.curr_block.insert(entry.clone());
            }
        };
    }

    /// Returns handles to a data file and an index file, respectively.
    fn get_files(series_name: &str, field_name: &str, append: bool) -> (File, File) {
        let filename = format!("{}_{}", series_name, field_name);
        let data_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(append)
            .open(&filename)
            .unwrap();

        let index_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .append(append)
            .open(format!("{}_index", filename))
            .unwrap();

        // TODO: error handling ^

        (data_file, index_file)
    }
}


#[cfg(test)]
mod tests {
    use std::{fs, time};

    use crate::storage::field::{FieldEntry, FieldStorage};
    use crate::storage::field_block::ENTRIES_PER_BLOCK;

    #[test]
    fn it_inserts() {
        fs::remove_file("test_series_value1");
        fs::remove_file("test_series_value1_index");
        let mut s = FieldStorage::new("test_series", "value1");

        for i in 0..ENTRIES_PER_BLOCK * 10 + 1 {
            s.insert(FieldEntry { value: i as f64, time: time::UNIX_EPOCH.elapsed().unwrap().as_nanos() });
        }
    }

    #[test]
    fn it_reads() {
        let s = FieldStorage::new("test_series", "value1");
        let records = s.read();
        dbg!(records);
    }
}