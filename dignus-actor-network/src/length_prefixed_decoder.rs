use crate::codec::{DecodeResult, MessageDecoder};
use dignus_actor_core::messages::actor_message_trait::ActorMessageTrait;

const LENGTH_PREFIX_SIZE: usize = 4;
const DEFAULT_MAX_FRAME_LEN: usize = 1 << 20;

pub fn length_prefixed_frame(payload: &[u8]) -> Vec<u8> {
    let mut buffer = Vec::with_capacity(LENGTH_PREFIX_SIZE + payload.len());
    buffer.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    buffer.extend_from_slice(payload);
    buffer
}

pub struct LengthPrefixedDecoder<F> {
    deserialize: F,
    max_frame_len: usize,
}

impl<F> LengthPrefixedDecoder<F>
where
    F: Fn(&[u8]) -> Option<Box<dyn ActorMessageTrait>> + Send + 'static,
{
    pub fn new(deserialize: F) -> Self {
        Self {
            deserialize,
            max_frame_len: DEFAULT_MAX_FRAME_LEN,
        }
    }

    pub fn with_max_frame_len(deserialize: F, max_frame_len: usize) -> Self {
        Self {
            deserialize,
            max_frame_len,
        }
    }
}

impl<F> MessageDecoder for LengthPrefixedDecoder<F>
where
    F: Fn(&[u8]) -> Option<Box<dyn ActorMessageTrait>> + Send + 'static,
{
    fn try_decode(&self, buffer: &[u8]) -> DecodeResult {
        if buffer.len() < LENGTH_PREFIX_SIZE {
            return DecodeResult::Incomplete;
        }

        let payload_len =
            u32::from_be_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]) as usize;

        if payload_len > self.max_frame_len {
            return DecodeResult::Corrupt;
        }

        let total = LENGTH_PREFIX_SIZE + payload_len;
        if buffer.len() < total {
            return DecodeResult::Incomplete;
        }

        let message = (self.deserialize)(&buffer[LENGTH_PREFIX_SIZE..total]);

        DecodeResult::Frame {
            consumed: total,
            message,
        }
    }
}
