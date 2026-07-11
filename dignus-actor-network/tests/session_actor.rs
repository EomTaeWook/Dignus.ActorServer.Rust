use dignus_actor_core::actor_base::{ActorBase, ActorContext, ActorReceiveResult};
use dignus_actor_core::actor_ref_trait::ActorRefTrait;
use dignus_actor_core::actor_system::ActorSystem;
use dignus_actor_core::messages::actor_message_trait::ActorMessageTrait;
use dignus_actor_network::{
    HostOptions, RawEncoder, RawFrameDecoder, SessionActorHost, SessionReceived, SessionSender,
    TcpHost,
};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;

struct EchoSessionActor {
    context: ActorContext,
    sender: SessionSender,
}

impl EchoSessionActor {
    fn new(sender: SessionSender) -> Self {
        Self {
            context: ActorContext::new(),
            sender,
        }
    }
}

impl ActorBase for EchoSessionActor {
    fn actor_context(&self) -> &ActorContext {
        &self.context
    }

    fn on_receive<'actor>(
        &'actor mut self,
        message: Box<dyn ActorMessageTrait>,
        _sender: Option<Arc<dyn ActorRefTrait>>,
    ) -> ActorReceiveResult<'actor> {
        if let Ok(received) = message.into_any().downcast::<SessionReceived>() {
            let _ = self.sender.send(&received.0);
        }
        ActorReceiveResult::Done
    }

    fn on_kill(&mut self) {
        self.sender.close();
    }
}

#[test]
fn session_actor_echo_roundtrip() {
    let system = ActorSystem::new(2);

    let host = TcpHost::bind(
        "127.0.0.1:0".parse().unwrap(),
        HostOptions::default().with_worker_count(2),
        {
            let system = Arc::clone(&system);
            move || {
                SessionActorHost::new(
                    Arc::clone(&system),
                    |sender| EchoSessionActor::new(sender),
                    RawFrameDecoder,
                    RawEncoder,
                )
            }
        },
    )
    .unwrap();
    let address = host.local_address();

    std::thread::spawn(move || {
        let _ = host.run();
    });

    let mut client = TcpStream::connect(address).unwrap();
    client.write_all(b"actor echo").unwrap();

    let mut received = [0u8; 10];
    client.read_exact(&mut received).unwrap();

    assert_eq!(&received, b"actor echo");

    system.dispose();
}
