use crate::frame_encoder::FrameEncoder;
use crate::send_result::SendResult;
use crate::session::Session;
use std::sync::Arc;

#[derive(Clone)]
pub struct SessionSender {
    session: Arc<Session>,
    encoder: Arc<dyn FrameEncoder>,
}

impl SessionSender {
    pub(crate) fn new(session: Arc<Session>, encoder: Arc<dyn FrameEncoder>) -> Self {
        Self { session, encoder }
    }

    pub fn id(&self) -> u64 {
        self.session.id()
    }

    pub fn is_disposed(&self) -> bool {
        self.session.is_disposed()
    }

    pub fn send(&self, payload: &[u8]) -> SendResult {
        self.session.send(&self.encoder.encode(payload))
    }

    pub fn send_raw(&self, bytes: &[u8]) -> SendResult {
        self.session.send(bytes)
    }

    pub fn close(&self) {
        self.session.close();
    }

    pub fn session(&self) -> &Arc<Session> {
        &self.session
    }
}
