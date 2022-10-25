

trait BlockEncodable {
    fn encode(&self);
}

trait BlockDecodable<T> {
    fn decode(&self) -> Vec<T>;
}

impl BlockEncodable for Vec<f64> {
    fn encode(&self) {
        todo!()
    }
}

impl BlockDecodable<f64> for Vec<f64> {
    fn decode(&self) -> Vec<f64> {
        vec![]
    }
}

impl BlockEncodable for Vec<bool> {
    fn encode(&self) {
        todo!()
    }
}

impl BlockDecodable<f64> for Vec<bool> {
    fn decode(&self) -> Vec<f64> {
        vec![]
    }
}

fn deserialize_f64_block() -> Vec<f64> {
    vec![]
}

// fn deserialize_block<T: BlockDecodable<T>>() -> Vec<dyn BlockDecodable<T>> {
//     Vec::<T>::new()
// }

// trait Asd<T> {
//     fn a() -> T;
// }
//
// impl<T> Asd<T> for Vec<f64> {
//     fn a() -> Vec<f64> {
//         todo!()
//     }
// }

// impl Asd for bool {
//     fn a() -> Vec<T> {
//         todo!()
//     }
// }

// trait BlockEncodable<T> {
//     /// Load a block of stored data.
//     // fn load() -> Vec<T>;
//
//     /// Write a block of stored data.
//     fn write(&self);
// }

// struct StorageBlock<T: Asd<T>> {
//     pub index: u16,
//     pub values: Vec<T>,
// }

// impl<T: Asd> StorageBlock<T> {
//     pub fn new(index: u16) -> StorageBlock<T> {
//         StorageBlock { index, values: vec![] }
//     }
// }
//
//
// impl<T: Asd> BlockEncodable<T> for StorageBlock<T> {
//     // fn load() -> Vec<f64> {
//     //     todo!()
//     // }
//
//     fn write(&self) {
//         todo!()
//     }
// }

// impl BlockEncodable<bool> for StorageBlock<f64> {
//     // fn load() -> Vec<bool> {
//     //     todo!()
//     // }
//
//     fn write(&self) {
//         todo!()
//     }
// }


#[cfg(test)]
mod tests {
    use crate::storage::field_float::{BlockEncodable, deserialize_f64_block};

    #[test]
    fn t() {
        let a = deserialize_f64_block();
        a.encode();

        let b = vec![false];

        let _c: Vec<Box<dyn BlockEncodable>> = vec![Box::new(a), Box::new(b)];

        // let a: Vec<f64> = deserialize_block();
        // let b: Vec<bool> = deserialize_block();
        // let mut block = StorageBlock::new(0);
        // block.values.push(123.0);
        //
        // let mut block2 = StorageBlock::new(0);
        // block2.values.push(false);
        //
        // let a*/ = vec![Box::new(block), Box::new(block2)];

        // let d: Vec<Box<dyn BlockEncodable<dyn Asd>>> = vec![Box::new(block as (;
    }
}