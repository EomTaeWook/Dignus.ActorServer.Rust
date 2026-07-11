#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendResult {
    Success,
    BufferFull,
    Disposed,
}
