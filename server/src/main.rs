use rtdb::network;


#[tokio::main]
async fn main() {
    // HttpServer::start().await;
    network::start_tcp_listener().await;
}
