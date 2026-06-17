use crate::actor_base::{ActorBase, ActorReceiveResult};
use crate::dispatcher::actor_dispatcher::ActorDispatcher;
use crate::internals::actor_ref::ActorRef;
use crate::internals::actor_schedulable_trait::ActorSchedulableTrait;
use crate::internals::enums::ActorReceivePollResult;
use crate::internals::pending_receive::PendingReceive;
use crate::messages::actor_mail::ActorMail;
use crate::queues::mpsc_bounded_queue::MpscBoundedQueue;

use std::cell::UnsafeCell;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::Arc;
use std::task::{Wake, Waker};

const LIFECYCLE_RUNNING: i32 = 0;
const LIFECYCLE_KILLING: i32 = 1;
const LIFECYCLE_STOPPED: i32 = 2;

pub(crate) struct ActorRunner {
    actor: UnsafeCell<Box<dyn ActorBase>>,
    actor_ref: ActorRef,
    dispatcher: Arc<ActorDispatcher>,
    on_finalize: Box<dyn Fn(u32, u32) + Send + Sync>,
    mailbox: MpscBoundedQueue<ActorMail>,

    is_scheduled: AtomicBool,
    lifecycle_state: AtomicI32,

    pending_receive: PendingReceive,
}

unsafe impl Send for ActorRunner {}
unsafe impl Sync for ActorRunner {}

impl ActorRunner {
    pub(crate) fn new(
        actor: Box<dyn ActorBase>,
        dispatcher: Arc<ActorDispatcher>,
        actor_ref: ActorRef,
        mailbox_capacity: usize,
        on_finalize: Box<dyn Fn(u32, u32) + Send + Sync>,
    ) -> Self {
        actor
            .actor_context()
            .initialize(Arc::clone(&dispatcher), actor_ref.clone());

        Self {
            actor: UnsafeCell::new(actor),
            actor_ref,
            dispatcher,
            on_finalize,
            mailbox: MpscBoundedQueue::new(mailbox_capacity),
            is_scheduled: AtomicBool::new(false),
            lifecycle_state: AtomicI32::new(LIFECYCLE_RUNNING),
            pending_receive: PendingReceive::new(),
        }
    }

    pub(crate) fn enqueue_only(&self, actor_mail: ActorMail) -> bool {
        if self.lifecycle_state.load(Ordering::Acquire) != LIFECYCLE_RUNNING {
            return false;
        }

        if self.mailbox.try_enqueue(actor_mail) == false {
            return false;
        }

        self.is_scheduled.load(Ordering::Acquire) == false
            && self.is_scheduled.swap(true, Ordering::AcqRel) == false
    }

    pub(crate) fn schedule_self(self: &Arc<Self>) {
        self.dispatcher.schedule(self.clone());
    }

    pub(crate) fn kill(self: &Arc<Self>) {
        if self
            .lifecycle_state
            .compare_exchange(
                LIFECYCLE_RUNNING,
                LIFECYCLE_KILLING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
        {
            if self.is_scheduled.swap(true, Ordering::AcqRel) == false {
                self.dispatcher.schedule(self.clone());
            }
        }
    }

    pub(crate) fn actor_alias(&self) -> Option<String> {
        self.actor_ref.alias().map(str::to_string)
    }

    fn create_waker(self: &Arc<Self>) -> Waker {
        let dispatcher = ActorDispatcher::current_actor_dispatcher()
            .unwrap_or_else(|| Arc::clone(&self.dispatcher));

        Waker::from(Arc::new(ActorRunnerWake {
            actor_runner: self.clone(),
            dispatcher,
        }))
    }

    fn poll_pending_receive(self: &Arc<Self>) -> ActorReceivePollResult {
        let waker = self.create_waker();
        self.pending_receive.poll(&waker)
    }

    fn execute_pending_receive(self: &Arc<Self>) -> bool {
        if self.pending_receive.is_active() == false {
            return true;
        }

        match self.poll_pending_receive() {
            ActorReceivePollResult::Ready => {
                self.dispatcher.schedule(self.clone());
                false
            }
            ActorReceivePollResult::Pending => false,
            ActorReceivePollResult::Failed => {
                self.kill();
                self.publish_execution_exception();
                self.dispatcher.schedule(self.clone());
                false
            }
            ActorReceivePollResult::NoPending => true,
        }
    }

    fn finalize_kill(&self) {
        if self
            .lifecycle_state
            .swap(LIFECYCLE_STOPPED, Ordering::AcqRel)
            == LIFECYCLE_STOPPED
        {
            return;
        }

        while self.mailbox.try_dequeue().is_some() {
        }

        self.pending_receive.clear();

        let actor = unsafe { &mut *self.actor.get() };
        actor.on_kill();

        (self.on_finalize)(self.actor_ref.index(), self.actor_ref.generation());
    }

    fn publish_execution_exception(&self) {
    }
}

impl ActorSchedulableTrait for ActorRunner {
    fn execute(self: Arc<Self>) {
        if self.execute_pending_receive() == false {
            return;
        }

        let lifecycle_state = self.lifecycle_state.load(Ordering::Acquire);

        if lifecycle_state == LIFECYCLE_KILLING {
            self.finalize_kill();
            return;
        }

        if lifecycle_state != LIFECYCLE_RUNNING {
            return;
        }

        while let Some(actor_mail) = self.mailbox.try_dequeue() {
            if self.lifecycle_state.load(Ordering::Acquire) != LIFECYCLE_RUNNING {
                break;
            }

            let receive_result = catch_unwind(AssertUnwindSafe(|| {
                PendingReceive::create_receive_future(&self.actor, actor_mail)
            }));

            let receive_result = match receive_result {
                Ok(receive_result) => receive_result,
                Err(_) => {
                    self.kill();
                    self.publish_execution_exception();
                    break;
                }
            };

            let receive_future = match receive_result {
                ActorReceiveResult::Done => continue,
                ActorReceiveResult::Pending(receive_future) => receive_future,
            };

            self.pending_receive.set(receive_future);

            match self.poll_pending_receive() {
                ActorReceivePollResult::Ready => {
                    continue;
                }
                ActorReceivePollResult::Pending => {
                    return;
                }
                ActorReceivePollResult::Failed => {
                    self.kill();
                    self.publish_execution_exception();
                    break;
                }
                ActorReceivePollResult::NoPending => {
                    continue;
                }
            }
        }

        if self.lifecycle_state.load(Ordering::Acquire) == LIFECYCLE_KILLING {
            self.finalize_kill();
            return;
        }

        self.is_scheduled.store(false, Ordering::Release);

        if self.mailbox.try_peek() {
            if self.is_scheduled.swap(true, Ordering::AcqRel) == false {
                self.dispatcher.schedule(self.clone());
            }
        }
    }
}

struct ActorRunnerWake {
    actor_runner: Arc<ActorRunner>,
    dispatcher: Arc<ActorDispatcher>,
}

impl Wake for ActorRunnerWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        let dispatcher = ActorDispatcher::current_actor_dispatcher()
            .unwrap_or_else(|| Arc::clone(&self.dispatcher));

        if self.actor_runner.pending_receive.try_mark_poll_scheduled() {
            dispatcher.schedule(self.actor_runner.clone());
        }
    }
}