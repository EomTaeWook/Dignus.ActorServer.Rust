use actix::io::{FramedWrite, WriteHandler};
use actix::prelude::*;
use bytes::BytesMut;
use std::io;
use tokio::io::{split, WriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::{BytesCodec, FramedRead};

// actix TCP echo: one actor per connection. Each connection's bytes arrive as a
// StreamHandler event on the actor (actix's mailbox/execution), and the actor echoes
// them via its FramedWrite. Connections are round-robined across N arbiters (threads)
// for multi-core. A real tokio-based actor framework doing echo — actor vs actor.

struct EchoActor {
    writer: FramedWrite<BytesMut, WriteHalf<TcpStream>, BytesCodec>,
}

impl Actor for EchoActor {
    type Context = Context<Self>;
}

impl WriteHandler<io::Error> for EchoActor {}

impl StreamHandler<Result<BytesMut, io::Error>> for EchoActor {
    fn handle(&mut self, item: Result<BytesMut, io::Error>, _ctx: &mut Self::Context) {
        if let Ok(bytes) = item {
            self.writer.write(bytes);
        }
    }

    fn finished(&mut self, ctx: &mut Self::Context) {
        ctx.stop();
    }
}

#[actix::main]
async fn main() {
    let mut args = std::env::args().skip(1);
    let port: u16 = args.next().and_then(|v| v.parse().ok()).unwrap_or(5000);
    let workers: usize = args
        .next()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8));

    let arbiters: Vec<Arbiter> = (0..workers).map(|_| Arbiter::new()).collect();

    let listener = TcpListener::bind(("0.0.0.0", port)).await.unwrap();
    println!("actix echo server listening on :{port} (arbiters={workers})");

    let mut next = 0usize;
    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let _ = stream.set_nodelay(true);

        let handle = arbiters[next].handle();
        next = (next + 1) % arbiters.len();

        EchoActor::start_in_arbiter(&handle, move |ctx| {
            let (read_half, write_half) = split(stream);
            EchoActor::add_stream(FramedRead::new(read_half, BytesCodec::new()), ctx);
            EchoActor {
                writer: FramedWrite::new(write_half, BytesCodec::new(), ctx),
            }
        });
    }
}
