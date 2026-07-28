use dignus_actor_server::{HostHandler, HostOptions, Session, TlsHost};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
use std::io::{Read, Write};
use std::sync::Arc;

struct EchoHandler;

impl HostHandler for EchoHandler {
    fn on_data(&mut self, session: &Arc<Session>, data: &[u8]) {
        let _ = session.send(data);
    }
}

#[test]
fn tls_echo_roundtrip() {
    let cert_key = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let cert_der = CertificateDer::from(cert_key.cert.der().to_vec());
    let key_der: PrivateKeyDer = PrivatePkcs8KeyDer::from(cert_key.key_pair.serialize_der()).into();

    let provider = Arc::new(rustls::crypto::ring::default_provider());

    let server_config = rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![cert_der.clone()], key_der)
        .unwrap();

    let host = TlsHost::bind(
        "127.0.0.1:0".parse().unwrap(),
        HostOptions::default()
            .with_worker_count(2)
            .with_max_pending_send(1 << 20),
        Arc::new(server_config),
        || EchoHandler,
    )
    .unwrap();
    let address = host.local_address();

    std::thread::spawn(move || {
        let _ = host.run();
    });

    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert_der).unwrap();

    let client_config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();

    let server_name = ServerName::try_from("localhost").unwrap().to_owned();
    let mut client_connection =
        rustls::ClientConnection::new(Arc::new(client_config), server_name).unwrap();
    let mut tcp = std::net::TcpStream::connect(address).unwrap();
    let mut tls = rustls::Stream::new(&mut client_connection, &mut tcp);

    tls.write_all(b"hello tls").unwrap();
    tls.flush().unwrap();

    let mut received = [0u8; 9];
    tls.read_exact(&mut received).unwrap();

    assert_eq!(&received, b"hello tls");
}
