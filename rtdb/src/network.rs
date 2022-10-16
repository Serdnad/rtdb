use std::str::from_utf8;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use crate::network::connection::ConnectionPool;

mod tcp_handler;
pub mod connection;
pub mod server;

const PORT: &str = "2345";

pub async fn start_tcp_listener() {
    let address = format!("{}:{}", "127.0.0.1", PORT);
    let listener = tokio::net::TcpListener::bind(address).await.unwrap();

    let mut pool = ConnectionPool::new();

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                println!("new connection: {:?}", addr);

                pool.add(stream);
            }
            Err(e) => println!("couldn't get cli: {:?}", e),
        };
    }
}

/// Protocol action
pub const ACTION_AUTHENTICATE: u8 = 0x00;
pub const ACTION_QUERY: u8 = 0x01;
pub const ACTION_INSERT: u8 = 0x02;

// TODO: move this
/// Consumes a UCSD string, with a length specified as a u16.
#[inline]
async fn read_string(stream: &mut TcpStream) -> Option<String> {
    let len = match stream.read_u16().await {
        Ok(len) => len,
        Err(_) => return None,
    };
    let mut buffer = vec![0; len as usize];

    stream.read_exact(&mut buffer).await;
    Some(from_utf8(&buffer).unwrap().to_owned())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;
    use crate::network::{ACTION_QUERY, start_tcp_listener};
    use nom::AsBytes;

    #[tokio::test]
    async fn accepts_connection() {
        tokio::spawn(async {
            start_tcp_listener().await;
        });

        // give listener time to bind
        tokio::time::sleep(Duration::new(0, 1e6 as u32)).await;

        let msg = b"SELECT test_series";
        let len = msg.len() as u16;

        let mut c = tokio::net::TcpStream::connect("127.0.0.1:2345").await.unwrap();
        c.write_all(&[ACTION_QUERY]).await;
        c.write_all(len.to_be_bytes().as_bytes()).await;
        c.write_all(msg).await;
        c.flush().await;

        tokio::time::sleep(Duration::new(1, 1e7 as u32)).await;
    }
}
