#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeadLetterReason {
    Unknown = 0,
    MailboxFull,
    RecipientInvalidated,
    ActorStopped,
    ActorSystemDisposed,
    ExecutionException,
}
