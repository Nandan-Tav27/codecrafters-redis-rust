use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:6379").await?;

    loop {
        let (mut socket, _) = listener.accept().await?;

        tokio::spawn(async move {
            let mut buf = [0; 1024];

            loop {
                let _n = match socket.read(&mut buf).await {
                    Ok(0) => return,
                    Ok(_n) => {
                        println!("Reached here");
                        let _ = socket.write_all(b"+PONG\r\n").await;
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
