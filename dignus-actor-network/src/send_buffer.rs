pub struct SendBuffer {
    data: Vec<u8>,
    offset: usize,
    max_pending: usize,
}

impl SendBuffer {
    pub(crate) fn new(max_pending: usize) -> Self {
        Self {
            data: Vec::new(),
            offset: 0,
            max_pending,
        }
    }

    fn pending_len(&self) -> usize {
        self.data.len() - self.offset
    }

    pub(crate) fn can_write(&self, count: usize) -> bool {
        self.pending_len() + count <= self.max_pending
    }

    pub(crate) fn append(&mut self, bytes: &[u8]) {
        self.data.extend_from_slice(bytes);
    }

    pub fn has_pending(&self) -> bool {
        self.offset < self.data.len()
    }

    pub fn pending_slice(&self) -> &[u8] {
        &self.data[self.offset..]
    }

    pub fn advance(&mut self, count: usize) {
        self.offset += count;
        if self.offset >= self.data.len() {
            self.data.clear();
            self.offset = 0;
        }
    }
}
