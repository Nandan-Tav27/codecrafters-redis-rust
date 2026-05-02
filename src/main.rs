use std::{collections::HashMap, sync::Arc};

use bytes::Bytes;
use futures::{SinkExt, StreamExt, lock::Mutex};
use tokio::net::{TcpListener, TcpStream};
mod resp;
use resp::{RESPParser, RedisValueRef};
use tokio_util::codec::Decoder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(Mutex::new(HashMap::new()));
    let listener = TcpListener::bind("127.0.0.1:6379").await?;
    loop {
        let store = store.clone();
        let (socket, _) = listener.accept().await?;
        println!("Accepted connection!");
        tokio::spawn(async move { handle_conn(socket, store).await });
    }
}

async fn handle_conn(socket: TcpStream, store: Arc<Mutex<HashMap<String, String>>>) {
    let mut transport = RESPParser::default().framed(socket);
    while let Some(result) = transport.next().await {
        match result {
            Ok(value) => {
                let response = handle_command(value, store.clone()).await;
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

async fn handle_command(
    value: RedisValueRef,
    store: Arc<Mutex<HashMap<String, String>>>,
) -> RedisValueRef {
    match value {
        RedisValueRef::Array(arr) => {
            let command = match &arr[0] {
                RedisValueRef::String(s) => s.to_ascii_uppercase(),
                _ => return RedisValueRef::Error(Bytes::from("ERR invalid command")),
            };
            match command.as_slice() {
                b"PING" => RedisValueRef::SimpleString(Bytes::from("PONG")),
                b"ECHO" => arr.get(1).cloned().unwrap_or(RedisValueRef::NullBulkString),
                b"SET" => {
                    if arr.len() != 3 {
                        return RedisValueRef::Error(Bytes::from("ERR invalid 'SET' command"));
                    }
                    let key = match &arr[1] {
                        RedisValueRef::String(s) => String::from_utf8(s.to_vec()).unwrap(),
                        _ => {
                            return RedisValueRef::Error(Bytes::from(
                                "ERR invalid 'SET' command: expected bulk string",
                            ));
                        }
                    };
                    let val = match &arr[2] {
                        RedisValueRef::String(s) => String::from_utf8(s.to_vec()).unwrap(),
                        _ => {
                            return RedisValueRef::Error(Bytes::from(
                                "ERR 'SET' command: expected bulk string",
                            ));
                        }
                    };
                    let _ = store.lock().await.insert(key, val);
                    RedisValueRef::SimpleString(Bytes::from("OK"))
                }
                b"GET" => {
                    if arr.len() != 2 {
                        return RedisValueRef::Error(Bytes::from("ERR invalid 'GET' command"));
                    }
                    let key = match &arr[1] {
                        RedisValueRef::String(s) => String::from_utf8(s.to_vec()).unwrap(),
                        _ => {
                            return RedisValueRef::Error(Bytes::from(
                                "ERR invalid 'GET' command: expected bulk string",
                            ));
                        }
                    };
                    match store.lock().await.get(&key) {
                        Some(val) => RedisValueRef::String(Bytes::from(val.clone())),
                        None => RedisValueRef::NullBulkString,
                    }
                }
                _ => RedisValueRef::Error(Bytes::from("ERR unknown command")),
            }
        }
        _ => RedisValueRef::Error(Bytes::from("ERR expected array")),
    }
}
