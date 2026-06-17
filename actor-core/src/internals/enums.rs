pub(crate) enum ActorReceivePollResult {
    Ready,
    Pending,
    Failed,
    NoPending,
}