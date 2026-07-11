use crate::send_buffer::SendBuffer;
use crate::transport::{Transport, TransportOutcome};
use mio::net::TcpStream;
use std::io::{self, Read, Write};

const READ_BUFFER_SIZE: usize = 8192;

pub struct TcpTransport;

impl Transport for TcpTransport {
    fn read(&mut self, socket: &mut TcpStream, inbound: &mut Vec<u8>) -> TransportOutcome {
        let mut buffer = [0u8; READ_BUFFER_SIZE];

        loop {
            match socket.read(&mut buffer) {
                Ok(0) => return TransportOutcome::Closed,
                Ok(count) => inbound.extend_from_slice(&buffer[..count]),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(_) => return TransportOutcome::Closed,
            }
        }

        TransportOutcome::Continue
    }

    fn write(&mut self, socket: &mut TcpStream, outbound: &mut SendBuffer) -> TransportOutcome {
        while outbound.has_pending() {
            match socket.write(outbound.pending_slice()) {
                Ok(0) => return TransportOutcome::Closed,
                Ok(count) => outbound.advance(count),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(_) => return TransportOutcome::Closed,
            }
        }

        TransportOutcome::Continue
    }

    fn wants_write(&self) -> bool {
        false
    }
}
