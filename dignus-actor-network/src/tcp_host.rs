use crate::handler::HostHandler;
use crate::host_options::HostOptions;
use crate::reactor::Reactor;
use crate::transport_factory::TcpTransportFactory;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

pub struct TcpHost<THandler: HostHandler> {
    reactors: Vec<Reactor<THandler>>,
    address: SocketAddr,
}

impl<THandler: HostHandler> TcpHost<THandler> {
    pub fn bind<TFactory>(
        address: SocketAddr,
        options: HostOptions,
        handler_factory: TFactory,
    ) -> io::Result<Self>
    where
        TFactory: FnMut() -> THandler,
    {
        let (reactors, address) = Reactor::build_pool(
            address,
            options.worker_count,
            Arc::new(TcpTransportFactory),
            options.max_pending_send,
            handler_factory,
        )?;

        Ok(Self { reactors, address })
    }

    pub fn local_address(&self) -> SocketAddr {
        self.address
    }

    pub fn run(self) -> io::Result<()> {
        run_reactors(self.reactors)
    }
}

pub(crate) fn run_reactors<THandler: HostHandler>(
    reactors: Vec<Reactor<THandler>>,
) -> io::Result<()> {
    let mut handles = Vec::with_capacity(reactors.len());

    for reactor in reactors {
        handles.push(std::thread::spawn(move || reactor.run()));
    }

    for handle in handles {
        match handle.join() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => return Err(error),
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "reactor worker thread panicked",
                ))
            }
        }
    }

    Ok(())
}
