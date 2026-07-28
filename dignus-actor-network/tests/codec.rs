use dignus_actor_core::actor_base::{ActorBase, ActorContext, ActorReceiveResult};
use dignus_actor_core::actor_ref_trait::ActorRefTrait;
use dignus_actor_core::actor_system::ActorSystem;
use dignus_actor_core::messages::actor_message_trait::ActorMessageTrait;
use dignus_actor_server::{
    length_prefixed_frame, HostOptions, LengthPrefixedDecoder, LengthPrefixedEncoder,
    SessionActorHost, SessionReceived, SessionSender, TcpHost,
};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;

struct FramedEchoActor {
    context: ActorContext,
    sender: SessionSender,
}

impl FramedEchoActor {
    fn new(sender: SessionSender) -> Self {
        Self {
            context: ActorContext::new(),
            sender,
        }
    }
}

impl ActorBase for FramedEchoActor {
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
}

fn read_frame(client: &mut TcpStream) -> Vec<u8> {
    let mut length_prefix = [0u8; 4];
    client.read_exact(&mut length_prefix).unwrap();
    let length = u32::from_be_bytes(length_prefix) as usize;
    let mut payload = vec![0u8; length];
    client.read_exact(&mut payload).unwrap();
    payload
}

#[test]
fn length_prefixed_frames_roundtrip() {
    let system = ActorSystem::new(2);

    let host = TcpHost::bind(
        "127.0.0.1:0".parse().unwrap(),
        HostOptions::default().with_worker_count(2),
        {
            let system = Arc::clone(&system);
            move || {
                let decoder = LengthPrefixedDecoder::new(|payload: &[u8]| {
                    Some(Box::new(SessionReceived(payload.to_vec())) as Box<dyn ActorMessageTrait>)
                });
                SessionActorHost::new(
                    Arc::clone(&system),
                    FramedEchoActor::new,
                    decoder,
                    LengthPrefixedEncoder,
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

    let mut batch = length_prefixed_frame(b"first");
    batch.extend_from_slice(&length_prefixed_frame(b"second"));
    client.write_all(&batch).unwrap();

    assert_eq!(read_frame(&mut client), b"first");
    assert_eq!(read_frame(&mut client), b"second");

    let third = length_prefixed_frame(b"third");
    let (head, tail) = third.split_at(3);
    client.write_all(head).unwrap();
    client.flush().unwrap();
    std::thread::sleep(std::time::Duration::from_millis(50));
    client.write_all(tail).unwrap();

    assert_eq!(read_frame(&mut client), b"third");

    system.dispose();
}
