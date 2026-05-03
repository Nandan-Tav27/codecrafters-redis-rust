use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Decoder;

mod data_store;
mod operations;
mod resp;
use data_store::DataStore;
use resp::{RESPParser, RedisValueRef};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = DataStore::new();
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    loop {
        let store = store.clone();
        let (socket, _) = listener.accept().await?;
        println!("Accepted connection!");
        tokio::spawn(async move { handle_conn(socket, store).await });
    }
}

async fn handle_conn(socket: TcpStream, store: DataStore) {
    let mut transport = RESPParser.framed(socket);
    while let Some(result) = transport.next().await {
        match result {
            Ok(value) => {
                let response = match value {
                    RedisValueRef::Array(arr) => operations::execute(arr, store.clone()).await,
                    _ => RedisValueRef::Error(Bytes::from("ERR expected array")),
                };
                if transport.send(response).await.is_err() {
                    return;
                }
            }
            Err(e) => {
                eprintln!("Failed to parse frame: {:?}", e);
                continue;
            }
        }
    }
}
