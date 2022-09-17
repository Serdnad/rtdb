use axum::routing::post;

use rtdb::server::HttpServer;

#[tokio::main]
async fn main() {
    HttpServer::start().await;
}
