use dignus_actor_core::messages::actor_message_trait::ActorMessageTrait;
use std::any::Any;

pub struct SessionReceived(pub Vec<u8>);

impl ActorMessageTrait for SessionReceived {
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}
