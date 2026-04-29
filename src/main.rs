use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

mod resp;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (mut socket, _) = listener.accept().await?;

        tokio::spawn(async move {
            let mut in_buf = [0; 1024];
            let mut out_buf = [0; 1024];

            loop {
                match socket.read(&mut in_buf).await {
                    Ok(0) => return,
                    Ok(n) => {
                        let mut parser = resp::Parser::new(&in_buf[0..n]);
                        if let Some(input) = parser.decode_array().unwrap() {
                            if input[0].to_lowercase() == "ping" {
                                let _ = socket.write_all(b"+PONG\r\n").await;
                            } else if input[0].to_lowercase() == "echo" && input.len() >= 2 {
                                let mut encoder = resp::Encoder::new(&mut out_buf);
                                let len = encoder.encode_to_bulk_string(&input[1]).unwrap();
                                let _ = socket.write(&out_buf[..len]).await;
                            }
                        }
                    }
                    Err(e) => {
                        println!("Failed to read from socket; err = {:}", e);
                        return;
                    }
                };
            }
        });
    }
}
