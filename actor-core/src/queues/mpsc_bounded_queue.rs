use std::{
    cell::UnsafeCell,
    mem::MaybeUninit,
    sync::atomic::{AtomicI64, Ordering},
};

struct Slot<T> {
    sequence: AtomicI64,
    data: UnsafeCell<MaybeUninit<T>>,
}

pub(crate) struct MpscBoundedQueue<T> {
    buffer: Vec<Slot<T>>,
    capacity: usize,
    producer_index: AtomicI64,
    consumer_index: UnsafeCell<i64>,
    index_mask: usize,
}

unsafe impl<T: Send> Send for MpscBoundedQueue<T> {}
unsafe impl<T: Send> Sync for MpscBoundedQueue<T> {}

impl<T> MpscBoundedQueue<T> {
    pub(crate) fn new(capacity: usize) -> Self {
        if capacity < 2 {
            panic!("capacity must be greater than 1.");
        }

        let normalized_capacity = Self::normalize_to_power_of_two(capacity);
        let index_mask = normalized_capacity - 1;

        let mut buffer = Vec::with_capacity(normalized_capacity);

        for slot_index in 0..normalized_capacity {
            buffer.push(Slot {
                sequence: AtomicI64::new(slot_index as i64),
                data: UnsafeCell::new(MaybeUninit::uninit()),
            });
        }

        Self {
            buffer,
            capacity: normalized_capacity,
            producer_index: AtomicI64::new(0),
            consumer_index: UnsafeCell::new(0),
            index_mask,
        }
    }

    pub(crate) fn try_enqueue(&self, item: T) -> bool {
        let mut item = Some(item);

        loop {
            let position = self.producer_index.load(Ordering::Acquire);
            let slot_index = position as usize & self.index_mask;
            let slot = &self.buffer[slot_index];

            let sequence = slot.sequence.load(Ordering::Acquire);
            let difference = sequence - position;

            if difference == 0 {
                if self
                    .producer_index
                    .compare_exchange(position, position + 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
                {
                    continue;
                }

                unsafe {
                    (*slot.data.get()).write(item.take().unwrap());
                }

                slot.sequence.store(position + 1, Ordering::Release);
                return true;
            }

            if difference < 0 {
                return false;
            }
        }
    }

    pub(crate) fn try_dequeue(&self) -> Option<T> {
        let position = unsafe { *self.consumer_index.get() };
        let slot_index = position as usize & self.index_mask;
        let slot = &self.buffer[slot_index];

        let sequence = slot.sequence.load(Ordering::Acquire);
        let expected_sequence = position + 1;
        let difference = sequence - expected_sequence;

        if difference == 0 {
            let item = unsafe { (*slot.data.get()).assume_init_read() };

            slot.sequence
                .store(position + self.capacity as i64, Ordering::Release);

            unsafe {
                *self.consumer_index.get() = position + 1;
            }

            return Some(item);
        }

        None
    }

    pub(crate) fn try_peek(&self) -> bool {
        let position = unsafe { *self.consumer_index.get() };
        let slot_index = position as usize & self.index_mask;
        let slot = &self.buffer[slot_index];

        let sequence = slot.sequence.load(Ordering::Acquire);
        let expected_sequence = position + 1;
        let difference = sequence - expected_sequence;

        difference == 0
    }

    fn normalize_to_power_of_two(capacity: usize) -> usize {
        let mut normalized_capacity = 1usize;

        while normalized_capacity < capacity {
            normalized_capacity = normalized_capacity
                .checked_mul(2)
                .expect("capacity overflow");
        }

        normalized_capacity
    }
}

impl<T> Drop for MpscBoundedQueue<T> {
    fn drop(&mut self) {
        while self.try_dequeue().is_some() {}
    }
}
