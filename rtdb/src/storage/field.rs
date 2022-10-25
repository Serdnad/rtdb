use std::fs::{File, OpenOptions};
use std::io::{Read};

use std::sync::{Arc, Mutex};

// bytecheck can be used to validate your data if you want
use bytecheck::CheckBytes;
use rkyv::{Archive, Deserialize, Serialize};
use crate::DataValue;
use crate::storage::block_manager::BlockManager;
use crate::storage::DEFAULT_DATA_DIR;

use crate::storage::field_block::FieldStorageBlock;
use crate::storage::field_index::FieldStorageBlockSummary;
use crate::wire_protocol::DataType;

// TODO: One idea is to make this generic, and have different implementations for each kind of supported
//  data type.
// TODO: Another idea is to not have the time in record, which can be really redundant in the common case
//  of multiple fields being written under the same series entry (i.e. with the same timestamp)
#[derive(Archive, Copy, Clone, Deserialize, Serialize, Debug, PartialEq)]
#[archive(compare(PartialEq))]
#[archive_attr(derive(CheckBytes, Debug))]
pub struct FieldEntry {
    /// Timestamp as nanoseconds since Unix epoch
    pub time: i64,

    pub value: DataValue,
}

#[derive(Debug)]
pub struct FieldStorage {
    pub name: String,
    pub data_type: DataType,

    data_file_handle: File,
    index_file_handle: File,

    block_summaries: Vec<FieldStorageBlockSummary>,

    // buffer to be written as a block
    curr_block: FieldStorageBlock,

    // todo
    block_manager: Arc<Mutex<BlockManager>>,
}

impl FieldStorage {
    // TODO: actually, should a lot of this work be moved to the block manager?
    // TODO: split new into load and new, and load summaries accordingly
    pub fn load<'a>(series_name: &str, field_name: &str) -> FieldStorage {
        let (data_file, index_file) = FieldStorage::get_files(series_name, field_name, true);

        // TODO: tmp
        let (data_file2, _) = FieldStorage::get_files(series_name, field_name, true);

        // TODO: reorganize, this is redundant
        let filename = format!("{}/{}/{}", DEFAULT_DATA_DIR, series_name, field_name);
        let index_filename = format!("{}_index", filename);
        let summaries = FieldStorageBlockSummary::load_all(&index_filename);

        if summaries.len() > 0 {
            // TODO: figure out datatype, maybe we should just save it, but that means saving a new file? or maybe as extension?
        }

        FieldStorage {
            data_type: DataType::Float, // TODO
            name: field_name.to_owned(),
            block_summaries: summaries,
            curr_block: FieldStorageBlock::new(),
            data_file_handle: data_file,
            index_file_handle: index_file,
            block_manager: Arc::new(Mutex::new(BlockManager::new(data_file2))),
        }
    }

    pub fn read(&self, start: Option<i64>, end: Option<i64>) -> Vec<FieldEntry> {
        // TODO: use a modified binary search to narrow down which blocks we scan, using summaries
        let start_block = 0;
        let end_block = self.block_summaries.len();

        let mut block_manager = self.block_manager.lock().unwrap();
        let mut records: Vec<_> = (start_block..end_block).flat_map(|offset| {
            let block = block_manager.load(offset);
            block.read(start, end)
        }).collect();
        records.extend(self.curr_block.read(start, end));

        records
    }

    pub fn insert(&mut self, entry: FieldEntry) {
        // we first attempt to write to the current block, and only write to disk if the block is
        // filled. TODO: what does this mean for data reliability?. TODO: move into curr block
        // TODO: this logic should probably be m/oved into block manager, right? maybe? would at least remove need for file handle

        // let block = self.curr_block;

        match self.curr_block.has_space() {
            true => self.curr_block.insert(entry),
            false => {
                self.curr_block.write_data(&mut self.data_file_handle);
                let summary = self.curr_block.write_summary(&mut self.index_file_handle);
                self.block_summaries.push(summary);

                let mut block_manager = self.block_manager.lock().unwrap();
                block_manager.blocks.push(self.curr_block.clone());

                self.curr_block = FieldStorageBlock::new();
                self.curr_block.insert(entry);
            }
        };
    }

    /// Returns handles to a data file and an index file, respectively.
    fn get_files(series_name: &str, field_name: &str, append: bool) -> (File, File) {
        let filename = format!("{}/{}/{}", DEFAULT_DATA_DIR, series_name, field_name);
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
    use crate::DataValue;

    use crate::storage::field::{FieldEntry, FieldStorage};
    use crate::storage::field_block::ENTRIES_PER_BLOCK;

    #[test]
    fn it_inserts() {
        fs::remove_dir("test_series");
        fs::create_dir("test_series");
        let mut s = FieldStorage::load("test_series", "field1");

        for i in 0..ENTRIES_PER_BLOCK * 10 + 1 {
            s.insert(FieldEntry { value: DataValue::Float(i as f64), time: time::UNIX_EPOCH.elapsed().unwrap().as_nanos() as i64 });
        }
    }

    #[test]
    fn it_reads() {
        let s = FieldStorage::load("test_series", "field1");
        let records = s.read(None, None);
        dbg!(records.len());
        dbg!(records);
    }
}