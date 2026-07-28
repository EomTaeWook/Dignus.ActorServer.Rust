use crate::handler::HostHandler;
use crate::host_options::HostOptions;
use crate::reactor::Reactor;
use crate::tcp_host::{run_reactors, HostShutdown};
use crate::tls_transport::TlsTransportFactory;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

pub struct TlsHost<THandler: HostHandler> {
    reactors: Vec<Reactor<THandler>>,
    address: SocketAddr,
}

impl<THandler: HostHandler> TlsHost<THandler> {
    pub fn bind<TFactory>(
        address: SocketAddr,
        options: HostOptions,
        tls_config: Arc<rustls::ServerConfig>,
        handler_factory: TFactory,
    ) -> io::Result<Self>
    where
        TFactory: FnMut() -> THandler,
    {
        let (reactors, address) = Reactor::build_pool(
            address,
            options.worker_count,
            Arc::new(TlsTransportFactory::new(tls_config)),
            options.max_pending_send,
            handler_factory,
        )?;

        Ok(Self { reactors, address })
    }

    pub fn local_address(&self) -> SocketAddr {
        self.address
    }

    pub fn shutdown_handle(&self) -> HostShutdown {
        HostShutdown::new(
            self.reactors
                .iter()
                .map(|reactor| Arc::clone(&reactor.shared))
                .collect(),
        )
    }

    pub fn run(self) -> io::Result<()> {
        run_reactors(self.reactors)
    }
}
