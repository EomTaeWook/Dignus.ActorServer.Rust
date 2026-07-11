use crate::handler::HostHandler;
use crate::session::{ReactorShared, Session};
use crate::transport::{Transport, TransportOutcome};
use crate::transport_factory::TransportFactory;
use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token, Waker};
use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

const LISTENER_TOKEN: Token = Token(0);
const WAKER_TOKEN: Token = Token(1);
const FIRST_CONNECTION_TOKEN: usize = 2;
const EVENT_CAPACITY: usize = 1024;

struct ManagedConnection {
    stream: TcpStream,
    session: Arc<Session>,
    transport: Box<dyn Transport>,
}

pub(crate) struct Reactor<THandler: HostHandler> {
    poll: Poll,
    listener: Option<TcpListener>,
    connections: HashMap<Token, ManagedConnection>,
    shared: Arc<ReactorShared>,
    peers: Vec<Arc<ReactorShared>>,
    handler: THandler,
    transport_factory: Arc<dyn TransportFactory>,
    next_token: usize,
    next_session_id: u64,
    next_worker: usize,
    max_pending_send: usize,
    read_buf: Vec<u8>,
}

impl<THandler: HostHandler> Reactor<THandler> {
    pub(crate) fn build_pool<TFactory>(
        address: SocketAddr,
        worker_count: usize,
        transport_factory: Arc<dyn TransportFactory>,
        max_pending_send: usize,
        mut handler_factory: TFactory,
    ) -> io::Result<(Vec<Reactor<THandler>>, SocketAddr)>
    where
        TFactory: FnMut() -> THandler,
    {
        let worker_count = worker_count.max(1);

        let mut polls = Vec::with_capacity(worker_count);
        let mut shareds = Vec::with_capacity(worker_count);

        for _ in 0..worker_count {
            let poll = Poll::new()?;
            let waker = Waker::new(poll.registry(), WAKER_TOKEN)?;
            let shared = Arc::new(ReactorShared {
                waker,
                pending_writes: Mutex::new(Vec::new()),
                pending_closes: Mutex::new(Vec::new()),
                incoming: Mutex::new(Vec::new()),
            });
            polls.push(poll);
            shareds.push(shared);
        }

        let mut listener = TcpListener::bind(address)?;
        let bound_address = listener.local_addr()?;
        polls[0]
            .registry()
            .register(&mut listener, LISTENER_TOKEN, Interest::READABLE)?;

        let peers = shareds.clone();

        let mut listener_slot = Some(listener);
        let mut reactors = Vec::with_capacity(worker_count);
        for (index, (poll, shared)) in polls.into_iter().zip(shareds.into_iter()).enumerate() {
            reactors.push(Reactor {
                poll,
                listener: if index == 0 { listener_slot.take() } else { None },
                connections: HashMap::new(),
                shared,
                peers: peers.clone(),
                handler: handler_factory(),
                transport_factory: Arc::clone(&transport_factory),
                next_token: FIRST_CONNECTION_TOKEN,
                next_session_id: 1,
                next_worker: 0,
                max_pending_send,
                read_buf: Vec::new(),
            });
        }

        Ok((reactors, bound_address))
    }

    pub(crate) fn run(mut self) -> io::Result<()> {
        let mut events = Events::with_capacity(EVENT_CAPACITY);

        loop {
            self.poll.poll(&mut events, None)?;

            for event in events.iter() {
                match event.token() {
                    LISTENER_TOKEN => self.accept()?,
                    WAKER_TOKEN => {
                        self.take_incoming();
                        self.flush_pending();
                        self.close_pending();
                    }
                    token => self.handle_connection(token, event.is_readable(), event.is_writable()),
                }
            }
        }
    }

    fn accept(&mut self) -> io::Result<()> {
        loop {
            let accepted = match self.listener.as_ref() {
                Some(listener) => listener.accept(),
                None => return Ok(()),
            };

            match accepted {
                Ok((stream, _address)) => {
                    let session_id = self.next_session_id;
                    self.next_session_id += 1;

                    let target = self.next_worker;
                    self.next_worker = (self.next_worker + 1) % self.peers.len();

                    let peer = &self.peers[target];
                    peer.incoming.lock().unwrap().push((stream, session_id));
                    let _ = peer.waker.wake();
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(error),
            }
        }
    }

    fn take_incoming(&mut self) {
        let incoming = {
            let mut queue = self.shared.incoming.lock().unwrap();
            std::mem::take(&mut *queue)
        };

        for (mut stream, session_id) in incoming {
            let token = Token(self.next_token);
            self.next_token += 1;

            if self
                .poll
                .registry()
                .register(&mut stream, token, Interest::READABLE)
                .is_err()
            {
                continue;
            }

            let session = Arc::new(Session::new(
                session_id,
                token,
                Arc::clone(&self.shared),
                self.max_pending_send,
            ));

            self.connections.insert(
                token,
                ManagedConnection {
                    stream,
                    session: Arc::clone(&session),
                    transport: self.transport_factory.create(),
                },
            );

            self.handler.on_accepted(session);
        }
    }

    fn flush_pending(&mut self) {
        let tokens = {
            let mut pending = self.shared.pending_writes.lock().unwrap();
            std::mem::take(&mut *pending)
        };

        for token in tokens {
            if self.flush(token) {
                self.close(token);
            }
        }
    }

    fn close_pending(&mut self) {
        let tokens = {
            let mut pending = self.shared.pending_closes.lock().unwrap();
            std::mem::take(&mut *pending)
        };

        for token in tokens {
            self.close(token);
        }
    }

    fn handle_connection(&mut self, token: Token, readable: bool, writable: bool) {
        let mut closed = false;

        if readable {
            closed = self.read(token);
        }

        if closed == false && writable {
            closed = self.flush(token);
        }

        if closed {
            self.close(token);
        }
    }

    fn read(&mut self, token: Token) -> bool {
        let session;
        let mut closed = false;

        // Reuse a per-reactor scratch buffer instead of allocating one per read event.
        let mut scratch = std::mem::take(&mut self.read_buf);
        scratch.clear();

        match self.connections.get_mut(&token) {
            Some(connection) => {
                session = Arc::clone(&connection.session);
                if let TransportOutcome::Closed =
                    connection.transport.read(&mut connection.stream, &mut scratch)
                {
                    closed = true;
                }
            }
            None => {
                self.read_buf = scratch;
                return false;
            }
        }

        if scratch.is_empty() == false {
            self.handler.on_data(&session, &scratch);
        }

        self.read_buf = scratch;

        if closed == false {
            closed = self.flush(token);
        }

        closed
    }

    fn flush(&mut self, token: Token) -> bool {
        let mut closed = false;

        if let Some(connection) = self.connections.get_mut(&token) {
            let still_pending;

            {
                let mut outbound = connection.session.outbound().lock().unwrap();

                if let TransportOutcome::Closed =
                    connection.transport.write(&mut connection.stream, &mut outbound)
                {
                    closed = true;
                }

                still_pending = outbound.has_pending() || connection.transport.wants_write();
            }

            if closed == false {
                let interest = if still_pending {
                    Interest::READABLE | Interest::WRITABLE
                } else {
                    Interest::READABLE
                };
                let _ = self
                    .poll
                    .registry()
                    .reregister(&mut connection.stream, token, interest);
            }
        }

        closed
    }

    fn close(&mut self, token: Token) {
        if let Some(mut connection) = self.connections.remove(&token) {
            connection.session.mark_disposed();
            let _ = self.poll.registry().deregister(&mut connection.stream);
            self.handler.on_disconnected(connection.session.id());
        }
    }
}
