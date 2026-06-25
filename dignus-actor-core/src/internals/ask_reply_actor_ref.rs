use crate::actor_ref_trait::ActorRefTrait;
use crate::internals::ask_awaiter_trait::AskAwaiterTrait;
use crate::internals::ask_system::AskSystem;
use crate::messages::actor_mail::ActorMail;
use crate::messages::actor_message_trait::ActorMessageTrait;

use std::sync::{Arc, Weak};

pub(crate) struct AskReplyActorRef {
    ask_system: Weak<AskSystem>,
    slot_index: usize,
    ask_awaiter: Weak<dyn AskAwaiterTrait>,
}

impl AskReplyActorRef {
    pub(crate) fn new(
        ask_system: Weak<AskSystem>,
        slot_index: usize,
        ask_awaiter: Weak<dyn AskAwaiterTrait>,
    ) -> Self {
        Self {
            ask_system,
            slot_index,
            ask_awaiter,
        }
    }
}

impl ActorRefTrait for AskReplyActorRef {
    fn post(&self, message: Box<dyn ActorMessageTrait>, _sender: Option<Arc<dyn ActorRefTrait>>) {
        if let Some(ask_system) = self.ask_system.upgrade() {
            ask_system.try_complete_response(self.slot_index, &self.ask_awaiter, message);
        }
    }

    fn post_mail(&self, actor_mail: ActorMail) {
        let (message, sender) = actor_mail.into_parts();
        self.post(message, sender);
    }

    fn kill(&self) {}
}
