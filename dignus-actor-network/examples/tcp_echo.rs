use dignus_actor_server::{HostHandler, HostOptions, Session, TcpHost};
use std::io;
use std::sync::Arc;

struct Echo;

impl HostHandler for Echo {
    fn on_data(&mut self, session: &Arc<Session>, data: &[u8]) {
        let _ = session.send(data);
    }
}

fn main() -> io::Result<()> {
    let host = TcpHost::bind(
        "127.0.0.1:7000".parse().unwrap(),
        HostOptions::new().with_worker_count(2),
        || Echo,
    )?;

    println!("listening on {}", host.local_address());
    host.run()
}
