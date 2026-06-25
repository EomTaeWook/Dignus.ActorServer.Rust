use crate::{
    actor_ref_trait::ActorRefTrait,
    internals::{ask_awaiter::AskAwaiter, ask_system::AskSystem, registry::ActorRegistry},
    messages::{actor_mail::ActorMail, actor_message_trait::ActorMessageTrait},
};

use std::{sync::Arc, time::Duration};

#[derive(Clone)]
pub struct ActorRef {
    index: u32,
    generation: u32,
    alias: Option<String>,
    registry: Arc<ActorRegistry>,
    ask_system: Arc<AskSystem>,
}

impl ActorRef {
    pub(crate) fn new(
        index: u32,
        generation: u32,
        alias: Option<String>,
        registry: Arc<ActorRegistry>,
        ask_system: Arc<AskSystem>,
    ) -> Self {
        Self {
            index,
            generation,
            alias,
            registry,
            ask_system,
        }
    }

    pub(crate) fn index(&self) -> u32 {
        self.index
    }

    pub(crate) fn generation(&self) -> u32 {
        self.generation
    }

    pub(crate) fn alias(&self) -> Option<&str> {
        self.alias.as_deref()
    }

    pub fn ask<TResponse>(
        &self,
        message: Box<dyn ActorMessageTrait>,
        timeout: Duration,
    ) -> AskAwaiter<TResponse>
    where
        TResponse: ActorMessageTrait,
    {
        let (ask_awaiter, ask_reply_actor_ref) = self.ask_system.register::<TResponse>(timeout);
        let sender: Arc<dyn ActorRefTrait> = Arc::new(ask_reply_actor_ref);

        self.post(message, Some(sender));

        ask_awaiter
    }

    fn post(&self, message: Box<dyn ActorMessageTrait>, sender: Option<Arc<dyn ActorRefTrait>>) {
        self.registry
            .post(self.index, self.generation, ActorMail::new(message, sender));
    }

    fn post_mail(&self, actor_mail: ActorMail) {
        self.registry.post(self.index, self.generation, actor_mail);
    }

    fn kill(&self) {
        self.registry.kill(self.index, self.generation);
    }
}

impl ActorRefTrait for ActorRef {
    fn post(&self, message: Box<dyn ActorMessageTrait>, sender: Option<Arc<dyn ActorRefTrait>>) {
        ActorRef::post(self, message, sender);
    }

    fn post_mail(&self, actor_mail: ActorMail) {
        ActorRef::post_mail(self, actor_mail);
    }

    fn kill(&self) {
        ActorRef::kill(self);
    }
}
