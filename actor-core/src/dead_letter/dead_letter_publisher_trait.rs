use crate::dead_letter::dead_letter_message::DeadLetterMessage;

pub trait DeadLetterPublisherTrait {
    fn publish(&self, dead_letter_message: DeadLetterMessage);
}
