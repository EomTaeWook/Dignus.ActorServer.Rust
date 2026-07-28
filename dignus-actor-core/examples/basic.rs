use dignus_actor_core::actor_base::{ActorBase, ActorContext, ActorReceiveResult};
use dignus_actor_core::actor_ref_trait::ActorRefTrait;
use dignus_actor_core::actor_system::ActorSystem;
use dignus_actor_core::messages::actor_message_trait::ActorMessageTrait;
use std::any::Any;
use std::sync::{mpsc, Arc};
use std::time::Duration;

struct Greet(String);

impl ActorMessageTrait for Greet {
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

struct Greeter {
    context: ActorContext,
    completed: mpsc::Sender<()>,
}

impl ActorBase for Greeter {
    fn actor_context(&self) -> &ActorContext {
        &self.context
    }

    fn on_receive<'actor>(
        &'actor mut self,
        message: Box<dyn ActorMessageTrait>,
        _sender: Option<Arc<dyn ActorRefTrait>>,
    ) -> ActorReceiveResult<'actor> {
        if let Ok(greet) = message.into_any().downcast::<Greet>() {
            println!("Hello, {}!", greet.0);
            let _ = self.completed.send(());
        }
        ActorReceiveResult::Done
    }
}

fn main() {
    let (completed_tx, completed_rx) = mpsc::channel();
    let system = ActorSystem::new(2);
    let greeter = system.spawn_with_factory(|| Greeter {
        context: ActorContext::new(),
        completed: completed_tx,
    });

    greeter.post(Box::new(Greet("actor".into())), None);
    completed_rx.recv().unwrap();
    system.shutdown_timeout(Duration::from_secs(5)).unwrap();
}
