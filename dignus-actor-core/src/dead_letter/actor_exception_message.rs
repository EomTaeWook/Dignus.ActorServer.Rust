use crate::messages::actor_message_trait::ActorMessageTrait;

use std::error::Error;

pub struct ActorExceptionMessage {
    exception: Box<dyn Error + Send + Sync>,
}

impl ActorExceptionMessage {
    pub fn new(exception: Box<dyn Error + Send + Sync>) -> Self {
        Self { exception }
    }

    pub fn exception(&self) -> &(dyn Error + Send + Sync) {
        self.exception.as_ref()
    }

    pub fn into_exception(self) -> Box<dyn Error + Send + Sync> {
        self.exception
    }
}

impl ActorMessageTrait for ActorExceptionMessage {
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }
}
