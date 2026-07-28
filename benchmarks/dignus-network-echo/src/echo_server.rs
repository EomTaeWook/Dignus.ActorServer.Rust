use dignus_actor_core::actor_base::{ActorBase, ActorContext, ActorReceiveResult};
use dignus_actor_core::actor_ref_trait::ActorRefTrait;
use dignus_actor_core::actor_system::ActorSystem;
use dignus_actor_core::messages::actor_message_trait::ActorMessageTrait;
use dignus_actor_server::{
    HostOptions, RawEncoder, RawFrameDecoder, SessionActorHost, SessionReceived, SessionSender,
    TcpHost,
};
use std::sync::Arc;

const MAX_PENDING_SEND: usize = 1 << 26;

struct EchoActor {
    context: ActorContext,
    sender: SessionSender,
}

impl EchoActor {
    fn new(sender: SessionSender) -> Self {
        Self {
            context: ActorContext::new(),
            sender,
        }
    }
}

impl ActorBase for EchoActor {
    fn actor_context(&self) -> &ActorContext {
        &self.context
    }

    fn on_receive<'actor>(
        &'actor mut self,
        message: Box<dyn ActorMessageTrait>,
        _sender: Option<Arc<dyn ActorRefTrait>>,
    ) -> ActorReceiveResult<'actor> {
        if let Ok(received) = message.into_any().downcast::<SessionReceived>() {
            let _ = self.sender.send_raw(&received.0);
        }
        ActorReceiveResult::Done
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let port: u16 = args.next().and_then(|value| value.parse().ok()).unwrap_or(5000);
    let dispatchers: usize = args.next().and_then(|value| value.parse().ok()).unwrap_or(32);
    let workers: usize = args.next().and_then(|value| value.parse().ok()).unwrap_or(8);

    let system = ActorSystem::new(dispatchers);

    let host = TcpHost::bind(
        format!("0.0.0.0:{port}").parse().unwrap(),
        HostOptions::default()
            .with_worker_count(workers)
            .with_max_pending_send(MAX_PENDING_SEND),
        {
            let system = Arc::clone(&system);
            move || {
                SessionActorHost::new(
                    Arc::clone(&system),
                    |sender| EchoActor::new(sender),
                    RawFrameDecoder,
                    RawEncoder,
                )
            }
        },
    )
    .unwrap();

    println!(
        "rust dignus echo server listening on :{port} (dispatchers={dispatchers}, io-workers={workers})"
    );
    host.run().unwrap();
}
