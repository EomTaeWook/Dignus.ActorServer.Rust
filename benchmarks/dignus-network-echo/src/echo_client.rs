use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const MSG_LEN: usize = 32;
const DEFAULT_WINDOW: usize = 1000;
const DEFAULT_DURATION_SECS: u64 = 10;
const READ_BUFFER_SIZE: usize = 1 << 16;

fn run_connection(addr: &str, window: usize, duration: Duration, total_bytes: &AtomicU64) {
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream.set_nodelay(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();

    let seed = vec![0u8; MSG_LEN * window];
    stream.write_all(&seed).unwrap();

    let mut buffer = vec![0u8; READ_BUFFER_SIZE];
    let mut local_bytes: u64 = 0;
    let start = Instant::now();

    while start.elapsed() < duration {
        match stream.read(&mut buffer) {
            Ok(0) => break,
            Ok(count) => {
                local_bytes += count as u64;
                stream.write_all(&buffer[..count]).unwrap();
            }
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock
                    || error.kind() == std::io::ErrorKind::TimedOut =>
            {
                continue
            }
            Err(error) => {
                eprintln!("read error: {error}");
                break;
            }
        }
    }

    total_bytes.fetch_add(local_bytes, Ordering::Relaxed);
}

fn main() {
    let mut args = std::env::args().skip(1);
    let addr = args
        .next()
        .unwrap_or_else(|| "127.0.0.1:5000".to_string());
    let connections: usize = args.next().and_then(|value| value.parse().ok()).unwrap_or(1);
    let window: usize = args
        .next()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_WINDOW);
    let duration_secs: u64 = args
        .next()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_DURATION_SECS);
    let duration = Duration::from_secs(duration_secs);

    println!(
        "echo client -> {addr} | connections={connections} window={window} msg_len={MSG_LEN} duration={duration_secs}s"
    );

    let total_bytes = Arc::new(AtomicU64::new(0));
    let start = Instant::now();

    let mut handles = Vec::new();
    for _ in 0..connections {
        let addr = addr.clone();
        let total_bytes = Arc::clone(&total_bytes);
        handles.push(std::thread::spawn(move || {
            run_connection(&addr, window, duration, &total_bytes);
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let seconds = start.elapsed().as_secs_f64();
    let bytes = total_bytes.load(Ordering::Relaxed);
    let messages = bytes / MSG_LEN as u64;

    println!("duration:      {seconds:.3} s");
    println!("total bytes:   {bytes}");
    println!("total msgs:    {messages}");
    println!(
        "throughput:    {:.0} msg/s ({:.2} MiB/s)",
        messages as f64 / seconds,
        (bytes as f64 / (1024.0 * 1024.0)) / seconds
    );
}
