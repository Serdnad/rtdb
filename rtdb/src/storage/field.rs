use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read};
use std::os::unix::fs::FileExt;
use std::sync::{Arc, Mutex};

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
    pub time: i64,

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

    // todo
    block_manager: Arc<Mutex<BlockManager>>,
}

/// BlockManager is responsible for intelligently caching field storage blocks in memory, loading
/// them from disk when necessary.
#[derive(Debug)]
struct BlockManager {
    data_file: File,

    // key is the block index
    blocks: HashMap<usize, FieldStorageBlock>,
}

impl BlockManager {
    pub fn new(data_file: File) -> BlockManager {
        BlockManager {
            data_file,
            blocks: Default::default(),
        }
    }

    pub fn load(&mut self, block_offset: usize) -> &FieldStorageBlock {
        if self.blocks.contains_key(&block_offset) {
            return &self.blocks[&block_offset];
        }


        // match self.blocks.get(&block_offset) {
        //     Some(block) => block,
        //     None => {
        // println!("LOAD BLOCK!");
        let block = FieldStorageBlock::load(&self.data_file, block_offset);
        self.blocks.insert(block_offset, block);
        &self.blocks[&block_offset]

        // &block
        // }
    }
}


impl FieldStorage {
    // TODO: split new into load and new, and load summaries accordingly
    pub fn new<'a>(series_name: &str, field_name: &str) -> FieldStorage {
        let (data_file, index_file) = FieldStorage::get_files(series_name, field_name, true);

        // TODO: tmp
        let (data_file2, _) = FieldStorage::get_files(series_name, field_name, true);

        // TODO: reorganize, this is redundant
        let filename = format!("{}/{}", series_name, field_name);
        let index_filename = format!("{}_index", filename);
        let summaries = FieldStorageBlockSummary::load_all(&index_filename);

        FieldStorage {
            series_name: series_name.to_owned(),
            field_name: field_name.to_owned(),
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
        let records = (start_block..end_block + 1).flat_map(|offset| {
            let block = block_manager.load(offset);
            block.read(start, end)
        }).collect();

        records
    }

    pub fn insert(&mut self, entry: FieldEntry) {
        // we first attempt to write to the current block, and only write to disk if the block is
        // filled. TODO: what does this mean for data reliability?. TODO: move into curr block
        // TODO: this logic should probably be moved into block manager, right? maybe? would at least remove need for file handle
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
        let filename = format!("{}/{}", series_name, field_name);
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
        fs::remove_dir("test_series");
        fs::create_dir("test_series");
        let mut s = FieldStorage::new("test_series", "value1");

        for i in 0..ENTRIES_PER_BLOCK * 10 + 1 {
            s.insert(FieldEntry { value: i as f64, time: time::UNIX_EPOCH.elapsed().unwrap().as_nanos() as i64 });
        }
    }

    #[test]
    fn it_reads() {
        let s = FieldStorage::new("test_series", "value1");
        let records = s.read(None, None);
        dbg!(records.len());
        dbg!(records);
    }
}