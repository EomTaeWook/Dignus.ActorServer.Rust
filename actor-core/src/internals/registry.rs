use crate::internals::actor_runner::ActorRunner;
use crate::messages::actor_mail::ActorMail;

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicPtr, AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

struct Slot {
    generation: AtomicU32,
    runner_ptr: AtomicPtr<ActorRunner>,
    owner: UnsafeCell<Option<Arc<ActorRunner>>>,
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

unsafe impl Send for ActorRegistry {}
unsafe impl Sync for ActorRegistry {}

impl ActorRegistry {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        let capacity = capacity.max(1).min(u32::MAX as usize - 1) as u32;

        let mut slots = Vec::with_capacity(capacity as usize);
        for _ in 0..capacity {
            slots.push(Slot {
                generation: AtomicU32::new(0),
                runner_ptr: AtomicPtr::new(std::ptr::null_mut()),
                owner: UnsafeCell::new(None),
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
        let _alloc = self.alloc.lock().unwrap();

        let slot = &self.slots[index as usize];
        let runner_ptr = Arc::as_ptr(&runner) as *mut ActorRunner;

        unsafe {
            *slot.owner.get() = Some(runner);
        }
        slot.runner_ptr.store(runner_ptr, Ordering::Release);
    }

    pub(crate) fn post(&self, index: u32, generation: u32, actor_mail: ActorMail) {
        if index >= self.capacity {
            return;
        }

        let slot = &self.slots[index as usize];

        if slot.generation.load(Ordering::Acquire) != generation {
            return;
        }

        let runner_ptr = slot.runner_ptr.load(Ordering::Acquire);
        if runner_ptr.is_null() {
            return;
        }

        let runner: &ActorRunner = unsafe { &*runner_ptr };

        if runner.enqueue_only(actor_mail) {
            unsafe {
                Arc::increment_strong_count(runner_ptr as *const ActorRunner);
                let runner_arc = Arc::from_raw(runner_ptr as *const ActorRunner);
                runner_arc.schedule_self();
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

        let runner_ptr = slot.runner_ptr.load(Ordering::Acquire);
        if runner_ptr.is_null() {
            return;
        }

        unsafe {
            Arc::increment_strong_count(runner_ptr as *const ActorRunner);
            let runner_arc = Arc::from_raw(runner_ptr as *const ActorRunner);
            runner_arc.kill();
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

        slot.generation.store(generation.wrapping_add(1), Ordering::Release);
        slot.runner_ptr.store(std::ptr::null_mut(), Ordering::Release);

        let removed = unsafe { (*slot.owner.get()).take() };

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
            let slot = &self.slots[index as usize];
            if let Some(runner) = unsafe { (*slot.owner.get()).as_ref() } {
                live.push(Arc::clone(runner));
            }
        }

        live
    }

    pub(crate) fn live_count(&self) -> usize {
        self.alloc.lock().unwrap().live
    }
}