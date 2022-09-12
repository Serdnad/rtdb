// struct ResultSet {
//     rows_affected: u64,
// }


/*

ADD btc_price price=123.0,time=now()
 */

use std::collections::HashMap;
use std::time;
use crate::storage::field::FieldEntry;
use crate::storage::series::{SeriesEntry, SeriesStorage};
//
// struct InsertionRequest<'a> {
//     series_name: &'a str,
//
//     /// A collection of metrics to record. All keys added are assumed to be lowercase.
//     /// If time is not a key, then "now()" is assumed.
//     data: HashMap<&'a str, f64>,
// }
//
// impl InsertionRequest<'_> {
//
//
//     /// Assumes that the request is valid.
//     // pub fn get_fields(&self) -> HashMap<&str, FieldEntry> {
//     //     let time = match self.data.get("time") {
//     //         None => time::UNIX_EPOCH.elapsed().unwrap().as_nanos(),
//     //         Some(&t) => t as u128,
//     //     };
//     //
//     //
//     //
//     //     let entries: _ = self.data.iter()
//     //         .filter(|(&k, _)| k != "time")
//     //         .map(|(&k, &v)| (
//     //             k,
//     //             FieldEntry {
//     //                 value: v,
//     //                 time,
//     //             })
//     //         );
//     //     HashMap::from(entries)
//     // }
// }


// fn insert() {
//     let s = SeriesStorage::new("test_series");
// }


// #[cfg(test)]
// mod tests {
//     use std::collections::HashMap;
//     use std::time;
//     use crate::execution::query::InsertionRequest;
//
//     #[test]
//     fn creates_fields() {
//         let request = InsertionRequest {
//             series_name: "test_series",
//             data: HashMap::from([("cool!", 1.0), ("time", time::UNIX_EPOCH.elapsed().unwrap().as_nanos() as f64)]),
//         };
//
//         let a = request.get_fields();
//         dbg!(a);
//     }
// }