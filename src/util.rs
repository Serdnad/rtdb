use std::time;

/// Returns the current system time in nanoseconds as an i64.
pub fn timestamp_nanos() -> i64 {
    time::UNIX_EPOCH.elapsed().unwrap().as_nanos() as i64
}

/// Returns the index of the smallest value, returning multiple indexes in the case that the min
/// is equal to multiple values.
/// TODO: Right now this is a fairy naive implementation
#[inline]
pub fn arg_min_all<T: Ord + Copy>(values: &[T]) -> (Option<T>, Vec<usize>) {
    if values.is_empty() {
        return (None, vec![]);
    }

    // check for case that all values are equal, which is be extremely common for certain use cases.
    // TODO: would be cool to get actual numbers on how much this speeds up and slows down the 2 cases.
    //  If we really wanted to get sophisticated down the road, we could actually keep metrics on how
    //  frequently each path is the case, and make this adaptive. that's for way way later though.
    // if values.iter().all_equal() {
    //     return (Some(values[0]), (0..values.len()).collect());
    // }
    let first = values[0];
    if values.iter().all(|&v| v == first) {
        return (Some(first), (0..values.len()).collect());
    }

    let mut min = values[0];
    for i in 1..values.len() {
        if values[i] < min {
            min = values[i];
        }
    }

    let indices = values.iter().enumerate().filter(|(_, &v)| v == min).map(|(i, _)| { i }).collect();

    (Some(min), indices)
}

#[cfg(test)]
mod tests {
    use crate::util::arg_min_all;

    #[test]
    fn test_arg_min_all() {
        assert_eq!(arg_min_all(&Vec::<i64>::new()), (None, vec![]));
        assert_eq!(arg_min_all(&vec![1, 1, 1]), (Some(1), vec![0, 1, 2]));
        assert_eq!(arg_min_all(&vec![2, 1, 3]), (Some(1), vec![1]));
        assert_eq!(arg_min_all(&vec![1, 1, 3]), (Some(1), vec![0, 1]));
    }
}