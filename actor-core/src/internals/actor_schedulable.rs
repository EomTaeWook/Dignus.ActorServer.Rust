use std::sync::Arc;

pub(crate) trait ActorSchedulable: Send + Sync {
    fn execute(self: Arc<Self>);
}