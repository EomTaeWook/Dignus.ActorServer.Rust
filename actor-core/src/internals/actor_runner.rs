use crate::actor_base::{ActorBase, ActorReceiveResult};
use crate::dead_letter::{
    dead_letter_message::DeadLetterMessage,
    dead_letter_publisher_trait::DeadLetterPublisherTrait,
    dead_letter_reason::DeadLetterReason,
};
use crate::dispatcher::actor_dispatcher::ActorDispatcher;
use crate::internals::actor_ref::ActorRef;
use crate::internals::actor_schedulable_trait::ActorSchedulableTrait;
use crate::messages::actor_mail::ActorMail;
use crate::messages::actor_message_trait::ActorMessageTrait;
use crate::poll_driver::{CompletionCallback, PollDriver, PollOutcome};
use crate::queues::mpsc_bounded_queue::MpscBoundedQueue;

use std::cell::UnsafeCell;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Arc, Weak};
use std::task::{Wake, Waker};

const LIFECYCLE_RUNNING: i32 = 0;
const LIFECYCLE_KILLING: i32 = 1;
const LIFECYCLE_STOPPED: i32 = 2;

struct ActorExecutionExceptionMessage;

impl ActorMessageTrait for ActorExecutionExceptionMessage {}

fn create_receive_future<'actor>(
    actor: &'actor UnsafeCell<Box<dyn ActorBase>>,
    actor_mail: ActorMail,
) -> ActorReceiveResult<'actor> {
    let (message, sender) = actor_mail.into_parts();

    unsafe {
        let actor = &mut *actor.get();
        actor.on_receive(message, sender)
    }
}

pub(crate) struct ActorRunner {
    actor: UnsafeCell<Box<dyn ActorBase>>,
    actor_ref: ActorRef,
    dispatcher: Arc<ActorDispatcher>,
    dead_letter_publisher: Arc<dyn DeadLetterPublisherTrait + Send + Sync>,
    on_finalize: Box<dyn Fn(u32, u32) + Send + Sync>,
    mailbox: MpscBoundedQueue<ActorMail>,

    is_scheduled: AtomicBool,
    lifecycle_state: AtomicI32,

    poll_driver: PollDriver,
}

unsafe impl Send for ActorRunner {}
unsafe impl Sync for ActorRunner {}

impl ActorRunner {
    pub(crate) fn new(
        actor: Box<dyn ActorBase>,
        dispatcher: Arc<ActorDispatcher>,
        actor_ref: ActorRef,
        mailbox_capacity: usize,
        dead_letter_publisher: Arc<dyn DeadLetterPublisherTrait + Send + Sync>,
        on_finalize: Box<dyn Fn(u32, u32) + Send + Sync>,
    ) -> Self {
        actor
            .actor_context()
            .initialize(Arc::clone(&dispatcher), actor_ref.clone());

        Self {
            actor: UnsafeCell::new(actor),
            actor_ref,
            dispatcher,
            dead_letter_publisher,
            on_finalize,
            mailbox: MpscBoundedQueue::new(mailbox_capacity),
            is_scheduled: AtomicBool::new(false),
            lifecycle_state: AtomicI32::new(LIFECYCLE_RUNNING),
            poll_driver: PollDriver::new(),
        }
    }

    fn receive_completed_callback(self: &Arc<Self>) -> CompletionCallback {
        let weak_self: Weak<Self> = Arc::downgrade(self);

        Box::new(move || {
            if let Some(actor_runner) = weak_self.upgrade() {
                actor_runner.schedule_self();
            }
        })
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

    fn poll_pending_receive(self: &Arc<Self>) -> PollOutcome {
        let waker = self.create_waker();
        self.poll_driver.poll(&waker)
    }

    fn execute_pending_receive(self: &Arc<Self>) -> bool {
        if self.poll_driver.is_active() == false {
            return true;
        }

        match self.poll_pending_receive() {
            PollOutcome::Ready => false,
            PollOutcome::Pending => false,
            PollOutcome::Failed => {
                self.kill();
                self.publish_execution_exception();
                self.dispatcher.schedule(self.clone());
                false
            }
            PollOutcome::Idle => true,
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

        self.poll_driver.clear();

        let actor = unsafe { &mut *self.actor.get() };
        actor.on_kill();

        (self.on_finalize)(self.actor_ref.index(), self.actor_ref.generation());
    }

    fn publish_execution_exception(&self) {
        self.dead_letter_publisher.publish(DeadLetterMessage::new(
            Box::new(ActorExecutionExceptionMessage),
            None,
            self.actor_ref.index() as i64,
            DeadLetterReason::ExecutionException,
        ));
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
                create_receive_future(&self.actor, actor_mail)
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

            self.poll_driver
                .arm(receive_future, Some(self.receive_completed_callback()));

            match self.poll_pending_receive() {
                PollOutcome::Ready => return,
                PollOutcome::Pending => return,
                PollOutcome::Failed => {
                    self.kill();
                    self.publish_execution_exception();
                    break;
                }
                PollOutcome::Idle => continue,
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

        if self.actor_runner.poll_driver.try_mark_poll_scheduled() {
            dispatcher.schedule(self.actor_runner.clone());
        }
    }
}