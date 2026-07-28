use dignus_actor_server::{HostHandler, HostOptions, Session, TcpHost};
use std::sync::Arc;

const MAX_PENDING_SEND: usize = 1 << 26;

struct EchoHandler;

impl HostHandler for EchoHandler {
    fn on_data(&mut self, session: &Arc<Session>, data: &[u8]) {
        let _ = session.send(data);
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let port: u16 = args.next().and_then(|v| v.parse().ok()).unwrap_or(5000);
    let workers: usize = args
        .next()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8));

    let options = HostOptions::default()
        .with_worker_count(workers)
        .with_max_pending_send(MAX_PENDING_SEND);

    let host = TcpHost::bind(
        format!("0.0.0.0:{port}").parse().unwrap(),
        options,
        || EchoHandler,
    )
    .unwrap();

    println!("dignus RAW echo server (no actor) on :{port} (io-workers={workers})");
    host.run().unwrap();
}
