/// Serialize values into a type-specific, compressed format.
pub fn serialize_bools(values: &Vec<Option<bool>>) -> Vec<u8> {
    let mut buf = vec![0; values.len() / 4];

    for i in (0..values.len()).step_by(4) {
        let mut byte = 0;
        for j in 0..3 {
            let val = values[i + j];
            match val {
                Some(true) => byte |= 0x3,
                Some(false) => byte |= 0x2,
                None => byte |= 0x0,
            }

            byte <<= 2;
        }

        // manually unroll the last iteration of the above loop to avoid needing a conditional on the
        // above bitshift.
        let val = values[i + 3];
        match val {
            Some(true) => byte |= 0x3,
            Some(false) => byte |= 0x2,
            None => byte |= 0x0,
        }

        buf[i / 4] = byte;
    }

    buf
}

pub fn serialize_floats(_values: &Vec<Option<f64>>) -> Vec<u8> {
    todo!()
}

type BoolSet = [Option<bool>; 4];

const BOOLS: [BoolSet; 256] = {
    const fn opt(val: u8, i: usize) -> Option<bool> {
        match (val << i * 2) & 0b1100_0000 {
            0b1100_0000 => Some(true),
            0b1000_0000 => Some(false),
            _ => None,
        }
    }
    const fn bool_set(b: u8) -> BoolSet {
        [
            opt(b, 0),
            opt(b, 1),
            opt(b, 2),
            opt(b, 3),
        ]
    }
    let mut v = [[None, None, None, None]; 256];
    let mut i = 0u8;
    while i < u8::MAX {
        v[i as usize] = bool_set(i);
        i += 1;
    }
    v
};

pub fn deserialize_1(raw: &[u8]) -> Vec<Option<bool>> {
    let mut values = Vec::with_capacity(raw.len() * 4); // 4 bools per byte

    for &b in raw {
        for i in 0..4 {
            let val = match (b << i * 2) & 0b1100_0000 {
                0b1100_0000 => Some(true),
                0b1000_0000 => Some(false),
                _ => None,
            };
            values.push(val);
        }
    }

    values
}

/// Deserialize booleans from a custom compressed format.
pub fn deserialize_bools(raw: &[u8]) -> Vec<Option<bool>> {
    raw.iter().flat_map(|v| BOOLS[*v as usize]).collect()
}

#[cfg(test)]
mod tests {
    use crate::storage::block_bool::{deserialize_bools, serialize_bools};

    #[test]
    fn full_loop() {
        let vals = vec![Some(true), Some(false), Some(false), None, Some(false), Some(true), Some(false), Some(true)];
        let serialized = serialize_bools(&vals);
        let deserialized = deserialize_bools(&serialized);

        assert_eq!(deserialized, vals);
    }
}