/// Returns the index of the smallest value, returning multiple indexes in the case that the min
/// is equal to multiple values.
/// TODO: Right now this is a fairy naive implementation
pub fn arg_min_all<T: Ord + Copy>(mut values: &Vec<T>) -> (Option<T>, Vec<usize>) {
    // check for case that all values are equal, which is be extremely common for certain use cases.
    // TODO: would be cool to get actual numbers on how much this speeds up and slows down the 2 cases.
    //  If we really wanted to get sophisticated down the road, we could actually keep metrics on how
    //  frequently each path is the case, and make this adaptive. that's for way way later though.
    // let mut all_equal = true;
    // for i in 1..values.len() {
    //     if values[0] != values[i] {
    //         all_equal = false;
    //         break;
    //     }
    // }
    //
    // if all_equal {
    //     return (&values[0], (0..values.len()).collect());
    // }

    if values.is_empty() {
        return (None, vec![]);
    }

    let mut min = &values[0];
    for v in &values[1..] {
        if v < min {
            min = v;
        }
    }

    let mut indexes = vec![];
    for (i, v) in values.iter().enumerate() {
        if v == min {
            indexes.push(i);
        }
    }

    (Some(min.clone()), indexes)
}

#[cfg(test)]
mod tests {
    use crate::util::arg_min_all;

    #[test]
    fn test_arg_min_all() {
        assert_eq!(arg_min_all(&Vec::<i32>::new()), (None, vec![]));
        assert_eq!(arg_min_all(&vec![1, 1, 1]), (Some(1), vec![0, 1, 2]));
        assert_eq!(arg_min_all(&vec![2, 1, 3]), (Some(1), vec![1]));
        assert_eq!(arg_min_all(&vec![1, 1, 3]), (Some(1), vec![0, 1]));
    }
}