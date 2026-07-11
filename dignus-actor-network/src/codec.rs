use crate::messages::SessionReceived;
use dignus_actor_core::messages::actor_message_trait::ActorMessageTrait;

pub enum DecodeResult {
    Incomplete,
    Frame {
        consumed: usize,
        message: Option<Box<dyn ActorMessageTrait>>,
    },
    Corrupt,
}

pub trait MessageDecoder: Send + 'static {
    fn try_decode(&self, buffer: &[u8]) -> DecodeResult;
}

pub struct RawFrameDecoder;

impl MessageDecoder for RawFrameDecoder {
    fn try_decode(&self, buffer: &[u8]) -> DecodeResult {
        if buffer.is_empty() {
            return DecodeResult::Incomplete;
        }

        DecodeResult::Frame {
            consumed: buffer.len(),
            message: Some(Box::new(SessionReceived(buffer.to_vec()))),
        }
    }
}
