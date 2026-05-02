use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::net::{TcpListener, TcpStream};
mod resp;
use resp::{RESPParser, RedisValueRef};
use tokio_util::codec::Decoder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    loop {
        let (socket, _) = listener.accept().await?;
        println!("Accepted connection!");
        tokio::spawn(async move { handle_conn(socket).await });
    }
}

async fn handle_conn(socket: TcpStream) {
    let mut transport = RESPParser::default().framed(socket);
    while let Some(result) = transport.next().await {
        match result {
            Ok(value) => {
                let response = handle_command(value);
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

fn handle_command(value: RedisValueRef) -> RedisValueRef {
    match value {
        RedisValueRef::Array(arr) => {
            let command = match &arr[0] {
                RedisValueRef::String(s) => s.to_ascii_uppercase(),
                _ => return RedisValueRef::Error(Bytes::from("ERR invalid command")),
            };
            match command.as_slice() {
                b"PING" => RedisValueRef::String(Bytes::from("PONG")),
                b"ECHO" => arr.get(1).cloned().unwrap_or(RedisValueRef::NullBulkString),
                _ => RedisValueRef::Error(Bytes::from("ERR unknown command")),
            }
        }
        _ => RedisValueRef::Error(Bytes::from("ERR expected array")),
    }
}
