use crate::internals::actor_runner::ActorRunner;
use crate::messages::actor_mail::ActorMail;

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

struct Slot {
    generation: AtomicU32,
    owner: Mutex<Option<Arc<ActorRunner>>>,
}

struct AllocState {
    free: Vec<u32>,
    next_unused: u32,
    live: usize,
}

pub(crate) struct ActorRegistry {
    slots: Box<[Slot]>,
    capacity: u32,
    alloc: Mutex<AllocState>,
}

impl ActorRegistry {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        let capacity = capacity.max(1).min(u32::MAX as usize - 1) as u32;

        let mut slots = Vec::with_capacity(capacity as usize);
        for _ in 0..capacity {
            slots.push(Slot {
                generation: AtomicU32::new(0),
                owner: Mutex::new(None),
            });
        }

        Self {
            slots: slots.into_boxed_slice(),
            capacity,
            alloc: Mutex::new(AllocState {
                free: Vec::new(),
                next_unused: 0,
                live: 0,
            }),
        }
    }

    pub(crate) fn reserve(&self) -> (u32, u32) {
        let mut alloc = self.alloc.lock().unwrap();

        let index = if let Some(index) = alloc.free.pop() {
            index
        } else {
            let index = alloc.next_unused;
            if index >= self.capacity {
                panic!("ActorRegistry capacity exceeded: {}", self.capacity);
            }
            alloc.next_unused = index + 1;
            index
        };

        let slot = &self.slots[index as usize];
        let generation = slot.generation.load(Ordering::Relaxed).wrapping_add(1);
        slot.generation.store(generation, Ordering::Release);

        alloc.live += 1;

        (index, generation)
    }

    pub(crate) fn commit(&self, index: u32, runner: Arc<ActorRunner>) {
        let slot = &self.slots[index as usize];
        *slot.owner.lock().unwrap() = Some(runner);
    }

    pub(crate) fn post(&self, index: u32, generation: u32, actor_mail: ActorMail) {
        if index >= self.capacity {
            return;
        }

        let slot = &self.slots[index as usize];

        if slot.generation.load(Ordering::Acquire) != generation {
            return;
        }

        let runner = {
            let owner = slot.owner.lock().unwrap();
            if slot.generation.load(Ordering::Acquire) != generation {
                return;
            }
            owner.as_ref().cloned()
        };

        if let Some(runner) = runner {
            if runner.enqueue_only(actor_mail) {
                runner.schedule_self();
            }
        }
    }

    pub(crate) fn kill(&self, index: u32, generation: u32) {
        if index >= self.capacity {
            return;
        }

        let slot = &self.slots[index as usize];

        if slot.generation.load(Ordering::Acquire) != generation {
            return;
        }

        let runner = {
            let owner = slot.owner.lock().unwrap();
            if slot.generation.load(Ordering::Acquire) != generation {
                return;
            }
            owner.as_ref().cloned()
        };

        if let Some(runner) = runner {
            runner.kill();
        }
    }

    pub(crate) fn remove(&self, index: u32, generation: u32) -> Option<Arc<ActorRunner>> {
        let mut alloc = self.alloc.lock().unwrap();

        if index >= self.capacity {
            return None;
        }

        let slot = &self.slots[index as usize];

        if slot.generation.load(Ordering::Relaxed) != generation {
            return None;
        }

        slot.generation
            .store(generation.wrapping_add(1), Ordering::Release);
        let removed = slot.owner.lock().unwrap().take();

        if removed.is_some() {
            alloc.free.push(index);
            alloc.live -= 1;
        }

        removed
    }

    pub(crate) fn snapshot_live(&self) -> Vec<Arc<ActorRunner>> {
        let alloc = self.alloc.lock().unwrap();

        let mut live = Vec::with_capacity(alloc.live);
        for index in 0..alloc.next_unused {
            if let Some(runner) = self.slots[index as usize].owner.lock().unwrap().as_ref() {
                live.push(Arc::clone(runner));
            }
        }

        live
    }

    pub(crate) fn live_count(&self) -> usize {
        self.alloc.lock().unwrap().live
    }
}
