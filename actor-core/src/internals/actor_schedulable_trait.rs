use std::sync::Arc;

pub(crate) trait ActorSchedulableTrait: Send + Sync {
    fn execute(self: Arc<Self>);
}