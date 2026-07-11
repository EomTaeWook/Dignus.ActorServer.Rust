use dignus_actor_network::{HostHandler, HostOptions, Session, TcpHost};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;

fn test_options() -> HostOptions {
    HostOptions::default()
        .with_worker_count(2)
        .with_max_pending_send(1 << 20)
}

struct EchoHandler;

impl HostHandler for EchoHandler {
    fn on_data(&mut self, session: &Arc<Session>, data: &[u8]) {
        let _ = session.send(data);
    }
}

#[test]
fn echo_roundtrip() {
    let host = TcpHost::bind("127.0.0.1:0".parse().unwrap(), test_options(), || EchoHandler).unwrap();
    let address = host.local_address();

    std::thread::spawn(move || {
        let _ = host.run();
    });

    let mut client = TcpStream::connect(address).unwrap();
    client.write_all(b"hello mio reactor").unwrap();

    let mut received = [0u8; 17];
    client.read_exact(&mut received).unwrap();

    assert_eq!(&received, b"hello mio reactor");
}

struct PushOnAcceptHandler;

impl HostHandler for PushOnAcceptHandler {
    fn on_accepted(&mut self, session: Arc<Session>) {
        std::thread::spawn(move || {
            let _ = session.send(b"server-push");
        });
    }

    fn on_data(&mut self, _session: &Arc<Session>, _data: &[u8]) {}
}

#[test]
fn cross_thread_send_roundtrip() {
    let host = TcpHost::bind("127.0.0.1:0".parse().unwrap(), test_options(), || {
        PushOnAcceptHandler
    })
    .unwrap();
    let address = host.local_address();

    std::thread::spawn(move || {
        let _ = host.run();
    });

    let mut client = TcpStream::connect(address).unwrap();

    let mut received = [0u8; 11];
    client.read_exact(&mut received).unwrap();

    assert_eq!(&received, b"server-push");
}
