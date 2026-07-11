use crate::send_result::SendResult;
use crate::session::Session;
use dignus_actor_core::actor_ref_trait::ActorRefTrait;
use dignus_actor_core::messages::actor_message_trait::ActorMessageTrait;
use dignus_actor_core::ActorRef;
use std::sync::Arc;

#[derive(Clone)]
pub struct NetworkSessionRef {
    actor_ref: Arc<ActorRef>,
    session: Arc<Session>,
}

impl NetworkSessionRef {
    pub(crate) fn new(actor_ref: Arc<ActorRef>, session: Arc<Session>) -> Self {
        Self { actor_ref, session }
    }

    pub fn id(&self) -> u64 {
        self.session.id()
    }

    pub fn actor_ref(&self) -> &Arc<ActorRef> {
        &self.actor_ref
    }

    pub fn send(&self, bytes: &[u8]) -> SendResult {
        self.session.send(bytes)
    }

    pub fn post(&self, message: Box<dyn ActorMessageTrait>) {
        ActorRefTrait::post(self.actor_ref.as_ref(), message, None);
    }

    pub fn close(&self) {
        self.session.close();
    }

    pub fn kill(&self) {
        self.session.close();
        ActorRefTrait::kill(self.actor_ref.as_ref());
    }
}
