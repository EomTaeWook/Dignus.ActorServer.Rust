use crate::messages::actor_mail::ActorMail;
use crate::messages::actor_message_trait::ActorMessageTrait;

use std::sync::Arc;

pub trait ActorRefTrait: Send + Sync + 'static {
    fn post(&self, message: Box<dyn ActorMessageTrait>, sender: Option<Arc<dyn ActorRefTrait>>);

    fn post_mail(&self, actor_mail: ActorMail);

    fn kill(&self);
}