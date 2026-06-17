use crate::{
    actor_ref_trait::ActorRefTrait,
    messages::actor_message_trait::ActorMessageTrait,
};

use std::sync::Arc;

pub struct ActorMail {
    message: Box<dyn ActorMessageTrait>,
    sender: Option<Arc<dyn ActorRefTrait>>,
}

impl ActorMail {
    pub fn new(message: Box<dyn ActorMessageTrait>, sender: Option<Arc<dyn ActorRefTrait>>) -> Self {
        Self {
            message,
            sender,
        }
    }

    pub fn message(&self) -> &dyn ActorMessageTrait {
        self.message.as_ref()
    }

    pub fn sender(&self) -> Option<&Arc<dyn ActorRefTrait>> {
        self.sender.as_ref()
    }

    pub(crate) fn into_parts(self) -> (Box<dyn ActorMessageTrait>, Option<Arc<dyn ActorRefTrait>>) {
        (self.message, self.sender)
    }
}