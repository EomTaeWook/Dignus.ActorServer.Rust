use crate::actor_base::ActorBase;
use crate::dispatcher::actor_dispatcher::ActorDispatcher;

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub struct ActorAwait;

impl ActorAwait {
    pub fn join(actor_base: &dyn ActorBase) -> ActorAwaiter {
        Self::join_dispatcher(Arc::clone(actor_base.actor_context().dispatcher()))
    }

    pub(crate) fn join_dispatcher(dispatcher: Arc<ActorDispatcher>) -> ActorAwaiter {
        ActorAwaiter::new(dispatcher)
    }
}

pub struct ActorAwaiter {
    dispatcher: Arc<ActorDispatcher>,
    is_scheduled: bool,
}

impl ActorAwaiter {
    fn new(dispatcher: Arc<ActorDispatcher>) -> Self {
        Self {
            dispatcher,
            is_scheduled: false,
        }
    }
}

impl Future for ActorAwaiter {
    type Output = ();

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let actor_awaiter = self.get_mut();

        if let Some(current_actor_dispatcher) = ActorDispatcher::current_actor_dispatcher() {
            if Arc::ptr_eq(&current_actor_dispatcher, &actor_awaiter.dispatcher) {
                return Poll::Ready(());
            }
        }

        if !actor_awaiter.is_scheduled {
            actor_awaiter.is_scheduled = true;

            let waker = context.waker().clone();

            actor_awaiter
                .dispatcher
                .enqueue_continuation(Box::new(move || {
                    waker.wake();
                }));
        }

        Poll::Pending
    }
}
