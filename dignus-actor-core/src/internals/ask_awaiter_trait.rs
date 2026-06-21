use crate::messages::actor_message_trait::ActorMessageTrait;

pub(crate) trait AskAwaitertrait: Send + Sync {
    fn set_response(&self, message: Box<dyn ActorMessageTrait>);
    fn set_timeout(&self);
}
