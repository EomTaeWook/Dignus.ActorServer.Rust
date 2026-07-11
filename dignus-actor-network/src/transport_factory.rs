use crate::tcp_transport::TcpTransport;
use crate::transport::Transport;

pub trait TransportFactory: Send + Sync {
    fn create(&self) -> Box<dyn Transport>;
}

pub struct TcpTransportFactory;

impl TransportFactory for TcpTransportFactory {
    fn create(&self) -> Box<dyn Transport> {
        Box::new(TcpTransport)
    }
}
