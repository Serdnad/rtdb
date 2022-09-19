use std::time;

/// Returns the current system time in nanoseconds as an i64.
pub fn timestamp_nanos() -> i64 {
    time::UNIX_EPOCH.elapsed().unwrap().as_nanos() as i64
}


/// Returns the index of the smallest value, returning multiple indexes in the case that the min
/// is equal to multiple values.
/// This version is custom built for our timestamp time, and assumes a non empty vector.
#[inline]
pub fn arg_min_all2(values: &[i64]) -> (i64, Vec<usize>) {
    // check for case that all values are equal, which is be extremely common for certain use cases.
    // TODO: would be cool to get actual numbers on how much this speeds up and slows down the 2 cases.
    //  If we really wanted to get sophisticated down the road, we could actually keep metrics on how
    //  frequently each path is the case, and make this adaptive. that's for way way later though.
    let first = values[0];
    if values.iter().all(|&v| v == first) {
        return (first, (0..values.len()).collect());
    }

    let mut min = values[0];
    for i in 1..values.len() {
        if values[i] < min {
            min = values[i];
        }
    }

    let indices = values.iter().enumerate().filter(|(_, &v)| v == min).map(|(i, _)| { i }).collect();

    (min, indices)
}

#[cfg(test)]
mod tests {
    use crate::util::{arg_min_all2};

    #[test]
    fn test_arg_min_all2() {
        // assert_eq!(arg_min_all2(&Vec::<i64>::new()), (None, vec![]));
        assert_eq!(arg_min_all2(&vec![1, 1, 1]), (1, vec![0, 1, 2]));
        assert_eq!(arg_min_all2(&vec![2, 1, 3]), (1, vec![1]));
        assert_eq!(arg_min_all2(&vec![1, 1, 3]), (1, vec![0, 1]));
    }
}