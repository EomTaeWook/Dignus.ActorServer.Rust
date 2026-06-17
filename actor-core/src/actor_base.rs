use crate::{
    actor_ref_trait::ActorRefTrait,
    dispatcher::actor_dispatcher::ActorDispatcher,
    internals::actor_ref::ActorRef,
    messages::actor_message_trait::ActorMessageTrait,
};

use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, OnceLock},
};

pub type ActorReceiveFuture<'actor> =
    Pin<Box<dyn Future<Output = ()> + Send + 'actor>>;

pub enum ActorReceiveResult<'actor> {
    Done,
    Pending(ActorReceiveFuture<'actor>),
}

pub struct ActorContext {
    dispatcher: OnceLock<Arc<ActorDispatcher>>,
    self_ref: OnceLock<Arc<dyn ActorRefTrait>>,
}

impl ActorContext {
    pub fn new() -> Self {
        Self {
            dispatcher: OnceLock::new(),
            self_ref: OnceLock::new(),
        }
    }

    pub(crate) fn initialize(&self, actor_dispatcher: Arc<ActorDispatcher>, actor_ref: ActorRef) {
        if self.dispatcher.set(actor_dispatcher).is_err() {
            panic!("ActorContext dispatcher is already initialized.");
        }

        let self_ref_trait: Arc<dyn ActorRefTrait> = Arc::new(actor_ref);

        if self.self_ref.set(self_ref_trait).is_err() {
            panic!("ActorContext self_ref is already initialized.");
        }
    }

    pub(crate) fn dispatcher(&self) -> &Arc<ActorDispatcher> {
        self.dispatcher
            .get()
            .expect("ActorContext dispatcher is not initialized.")
    }

    pub(crate) fn self_ref_trait(&self) -> Arc<dyn ActorRefTrait> {
        Arc::clone(
            self.self_ref
                .get()
                .expect("ActorContext self_ref is not initialized."),
        )
    }

    pub(crate) fn post(&self, target_ref: &dyn ActorRefTrait, message: Box<dyn ActorMessageTrait>) {
        target_ref.post(message, Some(self.self_ref_trait()));
    }

    pub fn verify_context(&self) {
        let current_actor_dispatcher = ActorDispatcher::current_actor_dispatcher();

        let Some(current_actor_dispatcher) = current_actor_dispatcher else {
            panic!(
                "Actor is running outside its dispatcher context. Expected Dispatcher-{}",
                self.dispatcher().id()
            );
        };

        if current_actor_dispatcher.id() != self.dispatcher().id() {
            panic!(
                "Actor Dispatcher-{} vs Current Dispatcher-{}",
                self.dispatcher().id(),
                current_actor_dispatcher.id()
            );
        }
    }
}

impl Default for ActorContext {
    fn default() -> Self {
        Self::new()
    }
}

pub trait ActorBase: Send {
    fn actor_context(&self) -> &ActorContext;

    fn on_receive<'actor>(
        &'actor mut self,
        message: Box<dyn ActorMessageTrait>,
        sender: Option<Arc<dyn ActorRefTrait>>,
    ) -> ActorReceiveResult<'actor>;

    fn on_kill(&mut self) {
    }

    fn self_ref(&self) -> Arc<dyn ActorRefTrait> {
        self.actor_context().self_ref_trait()
    }

    fn post(&self, target_ref: &dyn ActorRefTrait, message: Box<dyn ActorMessageTrait>) {
        self.actor_context().post(target_ref, message);
    }

    fn verify_context(&self) {
        self.actor_context().verify_context();
    }
}