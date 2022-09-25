use std::time;
use postgres::{NoTls};
use rtdb_client::Client;

fn main() {
    let mut client = rtdb_client::Client::new("127.0.0.1:2345").unwrap();
    // let mut client = postgres::Client::connect("host=localhost user=postgres", NoTls).unwrap();

    let start = time::Instant::now();


    let N = 1000;
    for _ in 0..N {
        let r = client.execute("SELECT test_series[field1, field2]");
        // let r = client.query("SELECT * FROM playground", &[]).unwrap();
        // dbg!(r);
    }

    let elapsed = start.elapsed();
    println!("Elapsed: {}ms", elapsed.as_millis());

    test_postgres()
}

fn test_postgres() {


}