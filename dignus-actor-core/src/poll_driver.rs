use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;
use std::task::{Context, Poll, Waker};

pub type DriverFuture = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
pub type CompletionCallback = Box<dyn FnOnce() + Send + 'static>;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum PollOutcome {
    Ready,
    Pending,
    Failed,
    Idle,
}

pub struct PollDriver {
    is_active: AtomicBool,
    is_poll_scheduled: AtomicBool,
    slot: Mutex<Option<(DriverFuture, Option<CompletionCallback>)>>,
}

impl Default for PollDriver {
    fn default() -> Self {
        Self::new()
    }
}

impl PollDriver {
    pub fn new() -> Self {
        Self {
            is_active: AtomicBool::new(false),
            is_poll_scheduled: AtomicBool::new(false),
            slot: Mutex::new(None),
        }
    }

    pub fn is_active(&self) -> bool {
        self.is_active.load(Ordering::Acquire)
    }

    pub fn arm<'a>(
        &self,
        future: Pin<Box<dyn Future<Output = ()> + Send + 'a>>,
        on_complete: Option<CompletionCallback>,
    ) {
        let future = unsafe {
            std::mem::transmute::<Pin<Box<dyn Future<Output = ()> + Send + 'a>>, DriverFuture>(
                future,
            )
        };

        {
            let mut slot = self.slot.lock().unwrap();
            *slot = Some((future, on_complete));
        }

        self.is_active.store(true, Ordering::Release);
        self.is_poll_scheduled.store(false, Ordering::Release);
    }

    pub fn clear(&self) {
        {
            let mut slot = self.slot.lock().unwrap();
            slot.take();
        }

        self.is_active.store(false, Ordering::Release);
        self.is_poll_scheduled.store(false, Ordering::Release);
    }

    pub fn try_mark_poll_scheduled(&self) -> bool {
        if !self.is_active() {
            return false;
        }

        self.is_poll_scheduled
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub fn poll(&self, waker: &Waker) -> PollOutcome {
        self.is_poll_scheduled.store(false, Ordering::Release);

        let mut context = Context::from_waker(waker);

        let (outcome, completion_callback) = {
            let mut slot = self.slot.lock().unwrap();

            let Some((future, _)) = slot.as_mut() else {
                self.is_active.store(false, Ordering::Release);
                return PollOutcome::Idle;
            };

            let poll_result = catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(&mut context)));

            match poll_result {
                Ok(Poll::Pending) => (PollOutcome::Pending, None),
                Ok(Poll::Ready(())) => {
                    let (_future, on_complete) = slot.take().unwrap();
                    (PollOutcome::Ready, on_complete)
                }
                Err(_) => {
                    slot.take();
                    (PollOutcome::Failed, None)
                }
            }
        };

        match outcome {
            PollOutcome::Pending => PollOutcome::Pending,
            PollOutcome::Ready => {
                self.is_active.store(false, Ordering::Release);
                self.is_poll_scheduled.store(false, Ordering::Release);

                if let Some(completion_callback) = completion_callback {
                    completion_callback();
                }

                PollOutcome::Ready
            }
            PollOutcome::Failed => {
                self.is_active.store(false, Ordering::Release);
                self.is_poll_scheduled.store(false, Ordering::Release);
                PollOutcome::Failed
            }
            PollOutcome::Idle => PollOutcome::Idle,
        }
    }
}
