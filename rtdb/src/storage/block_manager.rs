use std::fs::File;
use crate::storage::field_block::FieldStorageBlock;

/// BlockManager is responsible for intelligently caching field storage blocks in memory, loading
/// them from disk when necessary. TODO: that's not actually what it does right now, pretty far
#[derive(Debug)]
pub struct BlockManager {
    pub data_file: File,

    // key is the block index
    pub blocks: Vec<FieldStorageBlock>,
}

impl BlockManager {
    pub fn new(data_file: File) -> BlockManager {
        BlockManager {
            data_file,
            blocks: vec![],
        }
    }

    // pub fn

    pub fn load(&mut self, block_offset: usize) -> &FieldStorageBlock {
        if block_offset < self.blocks.len() {
            return &self.blocks[block_offset];
        }

        let block = FieldStorageBlock::load(&self.data_file, block_offset);
        self.blocks.push(block);
        &self.blocks[block_offset]
    }
}
