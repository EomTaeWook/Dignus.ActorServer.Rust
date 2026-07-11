const DEFAULT_MAX_PENDING_SEND: usize = 1 << 20;

#[derive(Clone, Copy, Debug)]
pub struct HostOptions {
    pub worker_count: usize,
    pub max_pending_send: usize,
}

impl HostOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_worker_count(mut self, worker_count: usize) -> Self {
        self.worker_count = worker_count.max(1);
        self
    }

    pub fn with_max_pending_send(mut self, max_pending_send: usize) -> Self {
        self.max_pending_send = max_pending_send;
        self
    }
}

impl Default for HostOptions {
    fn default() -> Self {
        let worker_count = std::thread::available_parallelism()
            .map(|value| value.get())
            .unwrap_or(1);

        Self {
            worker_count,
            max_pending_send: DEFAULT_MAX_PENDING_SEND,
        }
    }
}
