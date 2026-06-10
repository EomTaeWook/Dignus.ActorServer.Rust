use crate::object_pool::actor_yield_task_pool::ActorYieldTaskPool;
use crate::internals::actor_schedulable::ActorSchedulable;
use std::sync::{Arc, Mutex, Weak};

pub(crate) type SendOrPostCallback = Box<dyn FnOnce() + Send + 'static>;

pub(crate) struct ActorYieldTask {
    send_or_post_callback: Mutex<Option<SendOrPostCallback>>,
    pool: Weak<ActorYieldTaskPool>,
}

impl ActorYieldTask {
    pub(crate) fn new(pool: Weak<ActorYieldTaskPool>) -> Self {
        Self {
            send_or_post_callback: Mutex::new(None),
            pool,
        }
    }

    pub(crate) fn set(&self, send_or_post_callback: SendOrPostCallback) {
        *self.send_or_post_callback.lock().unwrap() = Some(send_or_post_callback);
    }

    fn recycle(self: Arc<Self>) {
        *self.send_or_post_callback.lock().unwrap() = None;

        if let Some(pool) = self.pool.upgrade() {
            pool.push(self);
        }
    }
}

impl ActorSchedulable for ActorYieldTask {
    fn execute(self: Arc<Self>) {
        let send_or_post_callback = self.send_or_post_callback.lock().unwrap().take();

        if let Some(send_or_post_callback) = send_or_post_callback {
            send_or_post_callback();
        }

        self.recycle();
    }
}