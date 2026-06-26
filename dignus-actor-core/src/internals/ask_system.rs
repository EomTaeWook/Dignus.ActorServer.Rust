use crate::internals::ask_awaiter::AskAwaiter;
use crate::internals::ask_awaiter_trait::AskAwaiterTrait;
use crate::internals::ask_reply_actor_ref::AskReplyActorRef;
use crate::messages::actor_message_trait::ActorMessageTrait;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const SWEEP_CAP: Duration = Duration::from_millis(100);
const MIN_SLOT_COUNT: usize = 1 << 18;

struct AskSlot {
    ask_awaiter: Weak<dyn AskAwaiterTrait>,
    deadline: Instant,
}

pub(crate) struct AskSystem {
    slots: Box<[Mutex<Option<AskSlot>>]>,
    slot_mask: usize,
    is_stopped: AtomicBool,
    sweeper_lock: Mutex<()>,
    sweeper_signal: Condvar,
    sweeper: Mutex<Option<JoinHandle<()>>>,
}

impl AskSystem {
    pub(crate) fn new(slot_count_hint: usize) -> Arc<Self> {
        let slot_count = slot_count_hint
            .max(1)
            .next_power_of_two()
            .max(MIN_SLOT_COUNT);

        let mut slots = Vec::with_capacity(slot_count);

        for _ in 0..slot_count {
            slots.push(Mutex::new(None));
        }

        let ask_system = Arc::new(Self {
            slots: slots.into_boxed_slice(),
            slot_mask: slot_count - 1,
            is_stopped: AtomicBool::new(false),
            sweeper_lock: Mutex::new(()),
            sweeper_signal: Condvar::new(),
            sweeper: Mutex::new(None),
        });

        let weak_ask_system = Arc::downgrade(&ask_system);

        let sweeper = thread::Builder::new()
            .name("AskTimeoutSweeper".to_string())
            .spawn(move || Self::run_sweeper(weak_ask_system))
            .expect("failed to start ask timeout sweeper");

        *ask_system.sweeper.lock().unwrap() = Some(sweeper);

        ask_system
    }

    pub(crate) fn register<TResponse>(
        self: &Arc<Self>,
        timeout: Duration,
    ) -> (AskAwaiter<TResponse>, AskReplyActorRef)
    where
        TResponse: ActorMessageTrait,
    {
        if self.is_stopped.load(Ordering::Acquire) {
            panic!("AskSystem is stopped.");
        }

        let deadline = Instant::now() + timeout;
        let ask_awaiter = AskAwaiter::<TResponse>::new();
        let ask_awaiter_trait = ask_awaiter.ask_awaiter();

        let start_index =
            (Arc::as_ptr(&ask_awaiter_trait) as *const () as usize >> 5) & self.slot_mask;
        let weak_ask_awaiter = Arc::downgrade(&ask_awaiter_trait);

        self.reserve_slot(start_index, &weak_ask_awaiter, deadline);

        let ask_reply_actor_ref = AskReplyActorRef::new(weak_ask_awaiter);

        (ask_awaiter, ask_reply_actor_ref)
    }

    fn reserve_slot(
        &self,
        slot_index: usize,
        ask_awaiter: &Weak<dyn AskAwaiterTrait>,
        deadline: Instant,
    ) {
        let mut slot = self.slots[slot_index].lock().unwrap();
        *slot = Some(AskSlot {
            ask_awaiter: ask_awaiter.clone(),
            deadline,
        });
    }

    pub(crate) fn stop(&self) {
        if self.is_stopped.swap(true, Ordering::AcqRel) {
            return;
        }

        {
            let _sweeper_guard = self.sweeper_lock.lock().unwrap();
            self.sweeper_signal.notify_all();
        }

        let sweeper = self.sweeper.lock().unwrap().take();

        if let Some(sweeper) = sweeper {
            if sweeper.thread().id() != thread::current().id() {
                let _ = sweeper.join();
            }
        }

        for slot in self.slots.iter() {
            let entry = slot.lock().unwrap().take();

            if let Some(entry) = entry {
                if let Some(ask_awaiter) = entry.ask_awaiter.upgrade() {
                    ask_awaiter.set_timeout();
                }
            }
        }
    }

    fn run_sweeper(weak_ask_system: Weak<AskSystem>) {
        loop {
            let ask_system = match weak_ask_system.upgrade() {
                Some(ask_system) => ask_system,
                None => return,
            };

            if ask_system.is_stopped.load(Ordering::Acquire) {
                return;
            }

            let now = Instant::now();
            let mut expired_awaiters = Vec::new();
            let mut next_deadline: Option<Instant> = None;

            for slot in ask_system.slots.iter() {
                let expired_entry = {
                    let mut slot = slot.lock().unwrap();

                    let should_remove = match slot.as_ref() {
                        Some(entry) if entry.deadline <= now => true,
                        Some(entry) if entry.ask_awaiter.strong_count() == 0 => true,
                        Some(entry) => {
                            next_deadline = Some(
                                next_deadline.map_or(entry.deadline, |current| current.min(entry.deadline)),
                            );
                            false
                        }
                        None => false,
                    };

                    if should_remove {
                        slot.take()
                    } else {
                        None
                    }
                };

                if let Some(expired_entry) = expired_entry {
                    if let Some(ask_awaiter) = expired_entry.ask_awaiter.upgrade() {
                        expired_awaiters.push(ask_awaiter);
                    }
                }
            }

            for ask_awaiter in expired_awaiters {
                ask_awaiter.set_timeout();
            }

            let wait_duration = match next_deadline {
                Some(deadline) => deadline
                    .saturating_duration_since(Instant::now())
                    .min(SWEEP_CAP),
                None => SWEEP_CAP,
            };

            let sweeper_guard = ask_system.sweeper_lock.lock().unwrap();

            if ask_system.is_stopped.load(Ordering::Acquire) {
                return;
            }

            let _ = ask_system
                .sweeper_signal
                .wait_timeout(sweeper_guard, wait_duration)
                .unwrap();
        }
    }
}
