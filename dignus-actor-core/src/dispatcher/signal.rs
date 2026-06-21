use std::sync::{Condvar, Mutex};

pub(crate) struct Signal {
    is_signaled: Mutex<bool>,
    condition_variable: Condvar,
}

impl Signal {
    pub(crate) fn new() -> Self {
        Self {
            is_signaled: Mutex::new(false),
            condition_variable: Condvar::new(),
        }
    }

    pub(crate) fn wait(&self) {
        let mut is_signaled = self.is_signaled.lock().unwrap();

        while *is_signaled == false {
            is_signaled = self.condition_variable.wait(is_signaled).unwrap();
        }

        *is_signaled = false;
    }

    pub(crate) fn release(&self) {
        let mut is_signaled = self.is_signaled.lock().unwrap();

        *is_signaled = true;
        self.condition_variable.notify_one();
    }
}
