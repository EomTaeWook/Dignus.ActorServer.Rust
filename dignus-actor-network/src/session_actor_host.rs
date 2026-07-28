use crate::codec::{DecodeResult, MessageDecoder};
use crate::frame_encoder::FrameEncoder;
use crate::handler::HostHandler;
use crate::network_session_ref::NetworkSessionRef;
use crate::session::Session;
use crate::session_sender::SessionSender;
use dignus_actor_core::actor_base::ActorBase;
use dignus_actor_core::actor_ref_trait::ActorRefTrait;
use dignus_actor_core::actor_system::ActorSystem;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

const DEFAULT_MAILBOX_CAPACITY: usize = 1024;

pub struct SessionHostOptions {
    pub mailbox_capacity: usize,
    pub dispatcher_index: Option<usize>,
}

impl Default for SessionHostOptions {
    fn default() -> Self {
        Self {
            mailbox_capacity: DEFAULT_MAILBOX_CAPACITY,
            dispatcher_index: None,
        }
    }
}

struct SessionEntry {
    network_session: NetworkSessionRef,
    inbound: Vec<u8>,
}

pub struct SessionActorHost<TActor, TFactory, TDecoder>
where
    TActor: ActorBase + 'static,
    TFactory: Fn(SessionSender) -> TActor + Send + 'static,
    TDecoder: MessageDecoder,
{
    system: Arc<ActorSystem>,
    factory: TFactory,
    decoder: TDecoder,
    encoder: Arc<dyn FrameEncoder>,
    options: SessionHostOptions,
    sessions: HashMap<u64, SessionEntry>,
    _marker: PhantomData<fn() -> TActor>,
}

impl<TActor, TFactory, TDecoder> SessionActorHost<TActor, TFactory, TDecoder>
where
    TActor: ActorBase + 'static,
    TFactory: Fn(SessionSender) -> TActor + Send + 'static,
    TDecoder: MessageDecoder,
{
    pub fn new<TEncoder>(
        system: Arc<ActorSystem>,
        factory: TFactory,
        decoder: TDecoder,
        encoder: TEncoder,
    ) -> Self
    where
        TEncoder: FrameEncoder,
    {
        Self::with_options(
            system,
            factory,
            decoder,
            encoder,
            SessionHostOptions::default(),
        )
    }

    pub fn with_options<TEncoder>(
        system: Arc<ActorSystem>,
        factory: TFactory,
        decoder: TDecoder,
        encoder: TEncoder,
        options: SessionHostOptions,
    ) -> Self
    where
        TEncoder: FrameEncoder,
    {
        Self {
            system,
            factory,
            decoder,
            encoder: Arc::new(encoder),
            options,
            sessions: HashMap::new(),
            _marker: PhantomData,
        }
    }
}

impl<TActor, TFactory, TDecoder> HostHandler for SessionActorHost<TActor, TFactory, TDecoder>
where
    TActor: ActorBase + 'static,
    TFactory: Fn(SessionSender) -> TActor + Send + 'static,
    TDecoder: MessageDecoder,
{
    fn on_accepted(&mut self, session: Arc<Session>) {
        let id = session.id();
        let sender = SessionSender::new(Arc::clone(&session), Arc::clone(&self.encoder));
        let factory = &self.factory;
        let mailbox_capacity = self.options.mailbox_capacity;

        let actor_ref = match self.options.dispatcher_index {
            Some(dispatcher_index) => self.system.spawn_on_dispatcher_with_factory_options(
                move || factory(sender),
                dispatcher_index,
                None,
                mailbox_capacity,
            ),
            None => self.system.spawn_with_factory_options(
                move || factory(sender),
                None,
                mailbox_capacity,
            ),
        };

        self.sessions.insert(
            id,
            SessionEntry {
                network_session: NetworkSessionRef::new(actor_ref, session),
                inbound: Vec::new(),
            },
        );
    }

    fn on_data(&mut self, session: &Arc<Session>, data: &[u8]) {
        let Some(entry) = self.sessions.get_mut(&session.id()) else {
            return;
        };

        if entry.inbound.is_empty() {
            let mut offset = 0;
            let mut corrupt = false;

            loop {
                match self.decoder.try_decode(&data[offset..]) {
                    DecodeResult::Incomplete => break,
                    DecodeResult::Corrupt => {
                        corrupt = true;
                        break;
                    }
                    DecodeResult::Frame { consumed, message } => {
                        offset += consumed;
                        if let Some(message) = message {
                            entry.network_session.post(message);
                        }
                    }
                }
            }

            if corrupt {
                entry.network_session.close();
            } else if offset < data.len() {
                entry.inbound.extend_from_slice(&data[offset..]);
            }

            return;
        }

        entry.inbound.extend_from_slice(data);

        let mut offset = 0;
        let mut corrupt = false;

        loop {
            match self.decoder.try_decode(&entry.inbound[offset..]) {
                DecodeResult::Incomplete => break,
                DecodeResult::Corrupt => {
                    corrupt = true;
                    break;
                }
                DecodeResult::Frame { consumed, message } => {
                    offset += consumed;
                    if let Some(message) = message {
                        entry.network_session.post(message);
                    }
                }
            }
        }

        if offset > 0 {
            entry.inbound.drain(..offset);
        }

        if corrupt {
            entry.network_session.close();
        }
    }

    fn on_disconnected(&mut self, session_id: u64) {
        if let Some(entry) = self.sessions.remove(&session_id) {
            ActorRefTrait::kill(entry.network_session.actor_ref().as_ref());
        }
    }
}
