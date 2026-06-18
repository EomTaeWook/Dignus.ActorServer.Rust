use crate::dispatcher::actor_yield_task::SendOrPostCallback;
use crate::dispatcher::signal::Signal;
use crate::internals::actor_schedulable_trait::ActorSchedulableTrait;
use crate::object_pool::actor_yield_task_pool::ActorYieldTaskPool;

use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::{
    atomic::{AtomicBool, AtomicI32, Ordering},
    Arc, Mutex,
};
use std::thread::{self, JoinHandle};

thread_local! {
    static CURRENT_ACTOR_DISPATCHER: RefCell<Option<Arc<ActorDispatcher>>> = RefCell::new(None);
}

pub(crate) struct ActorDispatcher {
    dispatcher_id: i32,
    scheduled_actors: Mutex<VecDeque<Arc<dyn ActorSchedulableTrait>>>,
    yield_task_pool: Arc<ActorYieldTaskPool>,
    is_stopped: AtomicBool,
    worker_thread: Mutex<Option<JoinHandle<()>>>,
    signal: Signal,
    signal_pending: AtomicI32,
}

impl ActorDispatcher {
    pub(crate) fn new(dispatcher_id: i32) -> Arc<Self> {
        Arc::new(Self {
            dispatcher_id,
            scheduled_actors: Mutex::new(VecDeque::new()),
            yield_task_pool: ActorYieldTaskPool::new(),
            is_stopped: AtomicBool::new(false),
            worker_thread: Mutex::new(None),
            signal: Signal::new(),
            signal_pending: AtomicI32::new(0),
        })
    }

    pub(crate) fn id(&self) -> i32 {
        self.dispatcher_id
    }

    pub(crate) fn current_actor_dispatcher() -> Option<Arc<ActorDispatcher>> {
        CURRENT_ACTOR_DISPATCHER
            .with(|current_actor_dispatcher| current_actor_dispatcher.borrow().clone())
    }

    pub(crate) fn start(self: &Arc<Self>) {
        let actor_dispatcher = Arc::clone(self);

        let worker_thread = thread::Builder::new()
            .name(format!("ActorDispatcher-{}", self.dispatcher_id))
            .spawn(move || {
                actor_dispatcher.process_scheduled_actors();
            })
            .expect("failed to start actor dispatcher thread");

        *self.worker_thread.lock().unwrap() = Some(worker_thread);
    }

    fn process_scheduled_actors(self: Arc<Self>) {
        CURRENT_ACTOR_DISPATCHER.with(|current_actor_dispatcher| {
            *current_actor_dispatcher.borrow_mut() = Some(Arc::clone(&self));
        });

        loop {
            self.signal.wait();

            if self.is_stopped.load(Ordering::Acquire) {
                break;
            }

            self.signal_pending.store(0, Ordering::Release);

            loop {
                let mut batch = {
                    let mut scheduled_actors = self.scheduled_actors.lock().unwrap();

                    if scheduled_actors.is_empty() {
                        break;
                    }

                    std::mem::take(&mut *scheduled_actors)
                };

                let mut is_stopped = false;

                for actor_schedulable in batch.drain(..) {
                    actor_schedulable.execute();

                    if self.is_stopped.load(Ordering::Acquire) {
                        is_stopped = true;
                        break;
                    }
                }

                if is_stopped {
                    break;
                }
            }

            if self.is_stopped.load(Ordering::Acquire) {
                break;
            }
        }

        CURRENT_ACTOR_DISPATCHER.with(|current_actor_dispatcher| {
            *current_actor_dispatcher.borrow_mut() = None;
        });
    }

    pub(crate) fn dispose(&self) {
        self.is_stopped.store(true, Ordering::Release);
        self.signal.release();

        let worker_thread = self.worker_thread.lock().unwrap().take();

        if let Some(worker_thread) = worker_thread {
            if worker_thread.thread().id() != thread::current().id() {
                let _ = worker_thread.join();
            }
        }
    }

    pub(crate) fn schedule(&self, actor_schedulable: Arc<dyn ActorSchedulableTrait>) {
        if self.is_stopped.load(Ordering::Acquire) {
            return;
        }

        self.scheduled_actors
            .lock()
            .unwrap()
            .push_back(actor_schedulable);

        if self
            .signal_pending
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.signal.release();
        }
    }

    pub(crate) fn enqueue_continuation(&self, send_or_post_callback: SendOrPostCallback) {
        let actor_yield_task = self.yield_task_pool.pop();

        actor_yield_task.set(send_or_post_callback);
        self.schedule(actor_yield_task);
    }
}
