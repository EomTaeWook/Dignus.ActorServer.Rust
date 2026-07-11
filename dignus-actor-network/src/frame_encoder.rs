use crate::length_prefixed_decoder::length_prefixed_frame;

pub trait FrameEncoder: Send + Sync + 'static {
    fn encode(&self, payload: &[u8]) -> Vec<u8>;
}

pub struct RawEncoder;

impl FrameEncoder for RawEncoder {
    fn encode(&self, payload: &[u8]) -> Vec<u8> {
        payload.to_vec()
    }
}

pub struct LengthPrefixedEncoder;

impl FrameEncoder for LengthPrefixedEncoder {
    fn encode(&self, payload: &[u8]) -> Vec<u8> {
        length_prefixed_frame(payload)
    }
}
