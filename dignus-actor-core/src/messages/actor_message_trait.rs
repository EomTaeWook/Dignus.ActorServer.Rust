use std::any::Any;

pub trait ActorMessageTrait: Send + 'static {
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
}
