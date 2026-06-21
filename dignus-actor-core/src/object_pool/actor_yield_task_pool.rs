use crate::dispatcher::actor_yield_task::ActorYieldTask;
use std::sync::{Arc, Mutex};

pub(crate) struct ActorYieldTaskPool {
    inner_pool: Mutex<Vec<Arc<ActorYieldTask>>>,
}

impl ActorYieldTaskPool {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            inner_pool: Mutex::new(Vec::new()),
        })
    }

    pub(crate) fn pop(self: &Arc<Self>) -> Arc<ActorYieldTask> {
        let mut inner_pool = self.inner_pool.lock().unwrap();

        inner_pool
            .pop()
            .unwrap_or_else(|| Arc::new(ActorYieldTask::new(Arc::downgrade(self))))
    }

    pub(crate) fn push(&self, actor_yield_task: Arc<ActorYieldTask>) {
        let mut inner_pool = self.inner_pool.lock().unwrap();

        inner_pool.push(actor_yield_task);
    }
}
