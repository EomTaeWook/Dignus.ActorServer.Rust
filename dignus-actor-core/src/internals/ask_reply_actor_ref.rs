use crate::actor_ref_trait::ActorRefTrait;
use crate::internals::ask_awaiter_trait::AskAwaiterTrait;
use crate::messages::actor_mail::ActorMail;
use crate::messages::actor_message_trait::ActorMessageTrait;

use std::sync::{Arc, Weak};

pub(crate) struct AskReplyActorRef {
    ask_awaiter: Weak<dyn AskAwaiterTrait>,
}

impl AskReplyActorRef {
    pub(crate) fn new(ask_awaiter: Weak<dyn AskAwaiterTrait>) -> Self {
        Self { ask_awaiter }
    }
}

impl ActorRefTrait for AskReplyActorRef {
    fn post(&self, message: Box<dyn ActorMessageTrait>, _sender: Option<Arc<dyn ActorRefTrait>>) {
        if let Some(ask_awaiter) = self.ask_awaiter.upgrade() {
            ask_awaiter.set_response(message);
        }
    }

    fn post_mail(&self, actor_mail: ActorMail) {
        let (message, sender) = actor_mail.into_parts();
        self.post(message, sender);
    }

    fn kill(&self) {}
}
