use crate::{actor_ref_trait::ActorRefTrait, messages::actor_message_trait::ActorMessageTrait};

use super::dead_letter_reason::DeadLetterReason;

use std::sync::Arc;
use std::time::SystemTime;

pub struct DeadLetterMessage {
    message: Box<dyn ActorMessageTrait>,
    sender: Option<Arc<dyn ActorRefTrait>>,
    recipient_actor_id: i64,
    detected_timestamp: SystemTime,
    reason: DeadLetterReason,
}

impl DeadLetterMessage {
    pub fn new(
        message: Box<dyn ActorMessageTrait>,
        sender: Option<Arc<dyn ActorRefTrait>>,
        recipient_actor_id: i64,
        reason: DeadLetterReason,
    ) -> Self {
        Self {
            message,
            sender,
            recipient_actor_id,
            detected_timestamp: SystemTime::now(),
            reason,
        }
    }

    pub fn message(&self) -> &dyn ActorMessageTrait {
        self.message.as_ref()
    }

    pub fn sender(&self) -> Option<&Arc<dyn ActorRefTrait>> {
        self.sender.as_ref()
    }

    pub fn recipient_actor_id(&self) -> i64 {
        self.recipient_actor_id
    }

    pub fn detected_timestamp(&self) -> SystemTime {
        self.detected_timestamp
    }

    pub fn reason(&self) -> DeadLetterReason {
        self.reason
    }

    pub fn into_message(self) -> Box<dyn ActorMessageTrait> {
        self.message
    }
}

impl ActorMessageTrait for DeadLetterMessage {}
