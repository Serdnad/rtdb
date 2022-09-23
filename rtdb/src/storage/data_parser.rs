use std::fmt::Debug;
use std::str::FromStr;
use crate::storage::SupportedDataType;

pub struct DataParser {}

impl DataParser {
    // TODO: change to accept buffer
    // parse a, hm, series field file
    pub fn parse<T: SupportedDataType>(&mut self, buffer: String) -> Vec<T> where <T as FromStr>::Err: Debug {
        let mut values = Vec::with_capacity(4); // TODO: 4

        let mut index = 0;
        for (i, char) in buffer.char_indices() {
            if char == ',' {
                let value_str = buffer.get(index..i).unwrap().to_owned();

                // TODO: determine type by storing a small header in the file or metadata in a separate file
                let value: T = value_str.parse().unwrap();

                values.push(value);
                index = i + 1 // plus 1 to skip comma
            }

            // println!("{}", char);
        }
        // dbg!(&buffer);

        values
    }
}