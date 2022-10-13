use std::arch::x86_64::*;
use std::mem::transmute;

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

pub fn serialize_bools2(values: &Vec<Option<bool>>) -> Vec<u8> {
    (0..values.len()).step_by(4).map(|i| {
        let mut byte = 0;

        for j in 0..3 {
            let val = values[i + j];
            match val {
                Some(true) => byte |= 0x3 << (2 * i),
                Some(false) => byte |= 0x2 << (2 * i),
                None => byte |= 0x0 << (2 * i),
            }
        }

        // manually unroll the last iteration of the above loop to avoid needing a conditional on the
        // above bitshift.
        let val = values[i + 3];
        match val {
            Some(true) => byte |= 0x3,
            Some(false) => byte |= 0x2,
            None => byte |= 0x0,
        }

        byte
    }).collect()
}

pub fn serialize_floats(values: &Vec<Option<f64>>) -> Vec<u8> {
    todo!()
}

/// Deserialize booleans from a custom compressed format.
pub fn deserialize(raw: &[u8]) -> Vec<Option<bool>> {
    let mut values = Vec::with_capacity(raw.len() * 4); // 4 bools per byte

    for &b in raw {
        values.extend((0..4).map(|i| {
            match (b << i * 2) & 0b1100_0000 {
                0b1100_0000 => Some(true),
                0b1000_0000 => Some(false),
                _ => None,
            }
        }));
    }

    values
}

pub fn deserialize_2(raw: &[u8]) -> Vec<Option<bool>> {
    let mut values = Vec::with_capacity(raw.len() * 4); // 4 bools per byte

    for &b in raw {
        values.extend((0..4).map(|i| {
            match (b << i * 2) & 0b1100_0000 {
                0b1100_0000 => Some(true),
                0b1000_0000 => Some(false),
                _ => None,
            }
        }));
    }

    values
}

struct Dsa<T: Asd> {
    data: Vec<Option<T>>,
}

pub trait Asd {
    type T;

    fn serialize(values: &Vec<Option<Self::T>>) -> Vec<u8>;
    fn deserialize(raw: &[u8]) -> Vec<Option<Self::T>>;
}

impl Asd for f64 {
    type T = f64;

    fn serialize(values: &Vec<Option<Self::T>>) -> Vec<u8> {
        values.iter().copied().flat_map(|f| f.unwrap().to_be_bytes()).collect()
    }

    fn deserialize(raw: &[u8]) -> Vec<Option<Self::T>> {
        todo!()
    }
}

impl Asd for bool {
    type T = bool;


    fn serialize(values: &Vec<Option<bool>>) -> Vec<u8> {
        let mut buf = Vec::with_capacity(values.len());

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

            buf.push(byte);
        }

        buf
    }

    fn deserialize(raw: &[u8]) -> Vec<Option<bool>> {
        let mut values = Vec::with_capacity(raw.len() * 4); // 4 bools per byte

        raw.iter()
            .copied()
            .for_each(|b| {
                values.extend((0..4).map(|i| {
                    match (b << i * 2) & 0b1100_0000 {
                        0b1100_0000 => Some(true),
                        0b1000_0000 => Some(false),
                        _ => None,
                    }
                }));
            });

        values
    }
}

pub fn deserialize_3(raw: &[u8]) -> Vec<Option<bool>> {
    raw.iter()
        .copied()
        .flat_map(|b| -> [_; 4] {
            std::array::from_fn(|i| match (b << i * 2) & 0b1100_0000 {
                0b1100_0000 => Some(true),
                0b1000_0000 => Some(false),
                _ => None,
            })
        })
        .collect()
}

pub fn deserialize_4(raw: &[u8]) -> Vec<Option<bool>> {
    let mut values = Vec::with_capacity(raw.len() * 4); // 4 bools per byte

    raw.iter()
        .copied()
        .for_each(|b| {
            values.extend((0..4).map(|i| {
                match (b << i * 2) & 0b1100_0000 {
                    0b1100_0000 => Some(true),
                    0b1000_0000 => Some(false),
                    _ => None,
                }
            }));
        });

    values
}


// deserialize_6
// pub fn deserialize_6(raw: &[u8]) -> Vec<Option<bool>> {
//     let mut v = vec![None;raw.len()*4];
//     for (i,arr) in raw.into_iter().zip(v.array_chunks_mut()){
//         *arr = BOOLS[*i as usize];
//     }
//     v
// }


#[inline(always)]
pub unsafe fn _mm256_shr4_epi8(a: __m256i) -> __m256i {
    let mask = _mm256_set1_epi8((0xff >> 4) as i8);
    _mm256_and_si256(_mm256_srli_epi16(a, 4), mask)
}

#[repr(align(32))]
struct Aligned32<T: Sized>(T);

const fn build_lut() -> Aligned32<[Option<bool>; 32]> {
    let mut lut = [None; 32];

    let mut i = 0;
    while i < 2 {
        let x = 16 * i;
        // only have to worry about bottom 4 bits,
        // since that is what we are looking up on

        // 11 => Some(true)
        // 10 => Some(false)
        // 01 => None
        // 00 => None

        lut[x + 0b11] = Some(true);
        lut[x + 0b10] = Some(false);
        lut[x + (0b11 << 2)] = Some(true);
        lut[x + (0b10 << 2)] = Some(false);

        i += 1;
    }

    Aligned32(lut)
}

static LUT: Aligned32<[Option<bool>; 32]> = build_lut();

#[inline(always)]
unsafe fn interleave_avx(m0: __m256i, m1: __m256i, m2: __m256i, m3: __m256i) -> [i8; 128] {
    let mut out = [0; 128];

    let ymm0 = m3;
    let ymm1 = m1;
    let ymm2 = m2;
    let ymm3 = m0;

    // vpunpcklbw      ymm4, ymm3, ymm1
    let ymm4 = _mm256_unpacklo_epi8(ymm3, ymm1);
    // vpunpckhbw      ymm1, ymm3, ymm1
    let ymm1 = _mm256_unpackhi_epi8(ymm3, ymm1);
    // vpunpcklbw      ymm3, ymm2, ymm0
    let ymm3 = _mm256_unpacklo_epi8(ymm2, ymm0);
    // vpunpckhbw      ymm0, ymm2, ymm0
    let ymm0 = _mm256_unpackhi_epi8(ymm2, ymm0);

    // vpunpcklwd      ymm2, ymm4, ymm3
    let ymm2 = _mm256_unpacklo_epi16(ymm4, ymm3);
    // vpunpckhwd      ymm3, ymm4, ymm3
    let ymm3 = _mm256_unpackhi_epi16(ymm4, ymm3);
    // vpunpcklwd      ymm4, ymm1, ymm0
    let ymm4 = _mm256_unpacklo_epi16(ymm1, ymm0);
    // vpunpckhwd      ymm0, ymm1, ymm0
    let ymm0 = _mm256_unpackhi_epi16(ymm1, ymm0);

    // vinserti128     ymm1, ymm2, xmm3, 1
    let xmm3 = _mm256_extracti128_si256(ymm3, 0);
    let ymm1 = _mm256_inserti128_si256(ymm2, xmm3, 1);
    // vinserti128     ymm5, ymm4, xmm0, 1
    let xmm0 = _mm256_extracti128_si256(ymm0, 0);
    let ymm5 = _mm256_inserti128_si256(ymm4, xmm0, 1);

    // vperm2i128      ymm2, ymm2, ymm3, 49
    let ymm2 = _mm256_permute2x128_si256(ymm2, ymm3, 49);
    // vperm2i128      ymm0, ymm4, ymm0, 49
    let ymm0 = _mm256_permute2x128_si256(ymm4, ymm0, 49);

    _mm256_storeu_si256(out.as_mut_ptr().cast::<__m256i>().add(0), ymm1);
    _mm256_storeu_si256(out.as_mut_ptr().cast::<__m256i>().add(1), ymm5);
    _mm256_storeu_si256(out.as_mut_ptr().cast::<__m256i>().add(2), ymm2);
    _mm256_storeu_si256(out.as_mut_ptr().cast::<__m256i>().add(3), ymm0);

    out
}

unsafe fn deserialize32_avx(bytes: &[u8; 32], out: &mut [Option<bool>; 128]) {
    // bits are in top 4 bits, need to shift and mask
    let mut m0 = _mm256_loadu_si256(bytes.as_ptr().cast());
    let mut m1 = _mm256_shr4_epi8(m0);

    // bottom 4 bits already contain the relevant information, we just have
    // to mask the other bits out, so one vpand.
    let mut m2 = m0;
    let mut m3 = m0;
    // copy over shifted values from m1
    m0 = m1;

    // select low 2 bits
    let mask1 = _mm256_set1_epi8(0b11);
    // select high 2 bits
    let mask2 = _mm256_set1_epi8(0b11 << 2);

    // m0   1100 0000
    // m1   0011 0000
    // m2   0000 1100
    // m3   0000 0011

    // m2 and m3 already contain bits in the bottom 4 bits
    m0 = _mm256_and_si256(m0, mask2);
    m1 = _mm256_and_si256(m1, mask1);

    m2 = _mm256_and_si256(m2, mask2);
    m3 = _mm256_and_si256(m3, mask1);

    // they all lookup from the same table
    let lut = _mm256_load_si256(LUT.0.as_ptr().cast());

    m0 = _mm256_shuffle_epi8(lut, m0);
    m1 = _mm256_shuffle_epi8(lut, m1);
    m2 = _mm256_shuffle_epi8(lut, m2);
    m3 = _mm256_shuffle_epi8(lut, m3);

    let interleaved = interleave_avx(m0, m1, m2, m3);

    *out = transmute(interleaved);
}

pub fn deserialize_avx(bytes: &[u8], out: &mut [Option<bool>]) {
    for (b, o) in bytes.chunks_exact(32).zip(out.chunks_exact_mut(128)) {
        unsafe {
            let b = &*(b.as_ptr() as *const [u8; 32]);
            let o = &mut *(o.as_mut_ptr() as *mut [Option<bool>; 128]);

            deserialize32_avx(b, o);
        }
    }
}

type BSET = [Option<bool>; 4];

pub const BOOLS: [BSET; 256] = {
    const fn opt(val: u8, i: usize) -> Option<bool> {
        match (val << i * 2) & 0b1100_0000 {
            0b1100_0000 => Some(true),
            0b1000_0000 => Some(false),
            _ => None,
        }
    }
    const fn bool_set(b: u8) -> BSET {
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