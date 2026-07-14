use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

// Raw tokio echo server: one spawned task per connection (the tokio equivalent of
// "one actor per connection"). Represents the IO ceiling of tokio-based actor
// frameworks (actix / ractor / kameo / xtra / coerce all run on tokio).
const READ_BUFFER_SIZE: usize = 1 << 16;

fn main() {
    let mut args = std::env::args().skip(1);
    let port: u16 = args.next().and_then(|v| v.parse().ok()).unwrap_or(5000);
    let worker_threads: usize = args
        .next()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8));

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_io()
        .build()
        .unwrap();

    println!("tokio echo server listening on :{port} (worker_threads={worker_threads})");

    runtime.block_on(async move {
        let listener = TcpListener::bind(("0.0.0.0", port)).await.unwrap();
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let _ = socket.set_nodelay(true);
            tokio::spawn(async move {
                let mut buffer = vec![0u8; READ_BUFFER_SIZE];
                loop {
                    match socket.read(&mut buffer).await {
                        Ok(0) => return,
                        Ok(count) => {
                            if socket.write_all(&buffer[..count]).await.is_err() {
                                return;
                            }
                        }
                        Err(_) => return,
                    }
                }
            });
        }
    });
}
