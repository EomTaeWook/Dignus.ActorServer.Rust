use crate::send_buffer::SendBuffer;
use crate::send_result::SendResult;
use mio::net::TcpStream;
use mio::{Token, Waker};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

pub(crate) struct ReactorShared {
    pub(crate) waker: Waker,
    pub(crate) pending_writes: Mutex<Vec<Token>>,
    pub(crate) pending_closes: Mutex<Vec<Token>>,
    pub(crate) incoming: Mutex<Vec<(TcpStream, u64)>>,
}

pub struct Session {
    id: u64,
    token: Token,
    outbound: Mutex<SendBuffer>,
    disposed: AtomicBool,
    close_requested: AtomicBool,
    shared: Arc<ReactorShared>,
}

impl Session {
    pub(crate) fn new(
        id: u64,
        token: Token,
        shared: Arc<ReactorShared>,
        max_pending_send: usize,
    ) -> Self {
        Self {
            id,
            token,
            outbound: Mutex::new(SendBuffer::new(max_pending_send)),
            disposed: AtomicBool::new(false),
            close_requested: AtomicBool::new(false),
            shared,
        }
    }

    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn is_disposed(&self) -> bool {
        self.disposed.load(Ordering::Acquire)
    }

    pub fn send(&self, bytes: &[u8]) -> SendResult {
        if self.is_disposed() {
            return SendResult::Disposed;
        }

        {
            let mut outbound = self.outbound.lock().unwrap();
            if outbound.can_write(bytes.len()) == false {
                return SendResult::BufferFull;
            }
            outbound.append(bytes);
        }

        self.shared.pending_writes.lock().unwrap().push(self.token);
        let _ = self.shared.waker.wake();

        SendResult::Success
    }

    pub fn close(&self) {
        if self.is_disposed() {
            return;
        }

        if self.close_requested.swap(true, Ordering::AcqRel) {
            return;
        }

        self.shared.pending_closes.lock().unwrap().push(self.token);
        let _ = self.shared.waker.wake();
    }

    pub(crate) fn outbound(&self) -> &Mutex<SendBuffer> {
        &self.outbound
    }

    pub(crate) fn mark_disposed(&self) {
        self.disposed.store(true, Ordering::Release);
    }
}
