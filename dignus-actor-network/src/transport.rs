use crate::send_buffer::SendBuffer;
use mio::net::TcpStream;

pub enum TransportOutcome {
    Continue,
    Closed,
}

pub trait Transport: Send {
    fn read(&mut self, socket: &mut TcpStream, inbound: &mut Vec<u8>) -> TransportOutcome;
    fn write(&mut self, socket: &mut TcpStream, outbound: &mut SendBuffer) -> TransportOutcome;
    fn wants_write(&self) -> bool;
}
