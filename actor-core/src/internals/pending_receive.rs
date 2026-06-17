use crate::actor_base::{ActorBase, ActorReceiveFuture, ActorReceiveResult};
use crate::internals::enums::ActorReceivePollResult;
use crate::messages::actor_mail::ActorMail;

use std::cell::UnsafeCell;
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::task::{Context, Poll, Waker};

type StoredActorReceiveFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

pub(crate) struct PendingReceive {
    is_active: AtomicBool,
    is_poll_scheduled: AtomicBool,
    receive_future: Mutex<Option<StoredActorReceiveFuture>>,
}

impl PendingReceive {
    pub(crate) fn new() -> Self {
        Self {
            is_active: AtomicBool::new(false),
            is_poll_scheduled: AtomicBool::new(false),
            receive_future: Mutex::new(None),
        }
    }

    pub(crate) fn is_active(&self) -> bool {
        self.is_active.load(Ordering::Acquire)
    }

    pub(crate) fn create_receive_future<'actor>(
        actor: &'actor UnsafeCell<Box<dyn ActorBase>>,
        actor_mail: ActorMail,
    ) -> ActorReceiveResult<'actor> {
        let (message, sender) = actor_mail.into_parts();

        unsafe {
            let actor = &mut *actor.get();
            actor.on_receive(message, sender)
        }
    }

    pub(crate) fn set<'actor>(&self, receive_future: ActorReceiveFuture<'actor>) {
        let stored_receive_future = unsafe {
            std::mem::transmute::<ActorReceiveFuture<'actor>, StoredActorReceiveFuture>(
                receive_future,
            )
        };

        {
            let mut receive_future_slot = self.receive_future.lock().unwrap();
            *receive_future_slot = Some(stored_receive_future);
        }

        self.is_active.store(true, Ordering::Release);
        self.is_poll_scheduled.store(false, Ordering::Release);
    }

    pub(crate) fn clear(&self) {
        {
            let mut receive_future_slot = self.receive_future.lock().unwrap();
            receive_future_slot.take();
        }

        self.is_active.store(false, Ordering::Release);
        self.is_poll_scheduled.store(false, Ordering::Release);
    }

    pub(crate) fn try_mark_poll_scheduled(&self) -> bool {
        if self.is_active() == false {
            return false;
        }

        self.is_poll_scheduled
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub(crate) fn poll(&self, waker: &Waker) -> ActorReceivePollResult {
        self.is_poll_scheduled.store(false, Ordering::Release);

        let mut context = Context::from_waker(waker);

        let poll_result = {
            let mut receive_future_slot = self.receive_future.lock().unwrap();

            let Some(receive_future) = receive_future_slot.as_mut() else {
                self.is_active.store(false, Ordering::Release);
                return ActorReceivePollResult::NoPending;
            };

            let poll_result = catch_unwind(AssertUnwindSafe(|| {
                receive_future.as_mut().poll(&mut context)
            }));

            match poll_result {
                Ok(Poll::Ready(())) => {
                    receive_future_slot.take();
                    Ok(Poll::Ready(()))
                }
                Ok(Poll::Pending) => Ok(Poll::Pending),
                Err(_) => {
                    receive_future_slot.take();
                    Err(())
                }
            }
        };

        match poll_result {
            Ok(Poll::Ready(())) => {
                self.is_active.store(false, Ordering::Release);
                self.is_poll_scheduled.store(false, Ordering::Release);
                ActorReceivePollResult::Ready
            }
            Ok(Poll::Pending) => ActorReceivePollResult::Pending,
            Err(_) => {
                self.is_active.store(false, Ordering::Release);
                self.is_poll_scheduled.store(false, Ordering::Release);
                ActorReceivePollResult::Failed
            }
        }
    }
}