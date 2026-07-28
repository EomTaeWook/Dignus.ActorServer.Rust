use crate::send_buffer::SendBuffer;
use crate::transport::{Transport, TransportOutcome};
use crate::transport_factory::TransportFactory;
use mio::net::TcpStream;
use rustls::ServerConnection;
use std::io::{self, Read, Write};
use std::sync::Arc;

pub struct TlsTransportFactory {
    config: Arc<rustls::ServerConfig>,
}

impl TlsTransportFactory {
    pub fn new(config: Arc<rustls::ServerConfig>) -> Self {
        Self { config }
    }
}

impl TransportFactory for TlsTransportFactory {
    fn create(&self) -> Box<dyn Transport> {
        let connection =
            ServerConnection::new(Arc::clone(&self.config)).expect("valid rustls server config");
        Box::new(TlsTransport::new(connection))
    }
}

pub struct TlsTransport {
    connection: ServerConnection,
}

impl TlsTransport {
    fn new(connection: ServerConnection) -> Self {
        Self { connection }
    }
}

impl Transport for TlsTransport {
    fn read(&mut self, socket: &mut TcpStream, inbound: &mut Vec<u8>) -> TransportOutcome {
        loop {
            match self.connection.read_tls(socket) {
                Ok(0) => return TransportOutcome::Closed,
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(_) => return TransportOutcome::Closed,
            }

            let io_state = match self.connection.process_new_packets() {
                Ok(io_state) => io_state,
                Err(_) => return TransportOutcome::Closed,
            };

            let plaintext_len = io_state.plaintext_bytes_to_read();
            if plaintext_len > 0 {
                let start = inbound.len();
                inbound.resize(start + plaintext_len, 0);
                if self
                    .connection
                    .reader()
                    .read_exact(&mut inbound[start..])
                    .is_err()
                {
                    return TransportOutcome::Closed;
                }
            }

            if io_state.peer_has_closed() {
                return TransportOutcome::Closed;
            }
        }

        TransportOutcome::Continue
    }

    fn write(&mut self, socket: &mut TcpStream, outbound: &mut SendBuffer) -> TransportOutcome {
        while outbound.has_pending() {
            match self.connection.writer().write(outbound.pending_slice()) {
                Ok(0) => break,
                Ok(count) => outbound.advance(count),
                Err(_) => return TransportOutcome::Closed,
            }
        }

        while self.connection.wants_write() {
            match self.connection.write_tls(socket) {
                Ok(0) => break,
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(_) => return TransportOutcome::Closed,
            }
        }

        TransportOutcome::Continue
    }

    fn wants_write(&self) -> bool {
        self.connection.wants_write()
    }
}
