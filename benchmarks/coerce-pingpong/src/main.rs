use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use coerce::actor::context::ActorContext;
use coerce::actor::message::{Handler, Message};
use coerce::actor::system::ActorSystem;
use coerce::actor::{Actor, LocalActorRef};

const PAIRS: usize = 348;
const INITIAL_MSGS: usize = 1000;
const RUN_SECS: u64 = 10;

struct PingPong {
    peer: Option<LocalActorRef<PingPong>>,
    running: Arc<AtomicBool>,
    count: Arc<AtomicU64>,
}

impl Actor for PingPong {}

struct SetPeer(LocalActorRef<PingPong>);

impl Message for SetPeer {
    type Result = ();
}

struct Ball;

impl Message for Ball {
    type Result = ();
}

#[async_trait]
impl Handler<SetPeer> for PingPong {
    async fn handle(&mut self, message: SetPeer, _ctx: &mut ActorContext) {
        self.peer = Some(message.0);
    }
}

#[async_trait]
impl Handler<Ball> for PingPong {
    async fn handle(&mut self, _message: Ball, _ctx: &mut ActorContext) {
        if self.running.load(Ordering::Relaxed) {
            self.count.fetch_add(1, Ordering::Relaxed);
            if let Some(peer) = &self.peer {
                let _ = peer.notify(Ball);
            }
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let system = ActorSystem::new();
    let running = Arc::new(AtomicBool::new(false));

    let mut counters: Vec<Arc<AtomicU64>> = Vec::with_capacity(PAIRS * 2);
    let mut pings: Vec<LocalActorRef<PingPong>> = Vec::with_capacity(PAIRS);

    for _ in 0..PAIRS {
        let c_ping = Arc::new(AtomicU64::new(0));
        let c_pong = Arc::new(AtomicU64::new(0));
        counters.push(c_ping.clone());
        counters.push(c_pong.clone());

        let ping = system
            .new_anon_actor(PingPong {
                peer: None,
                running: running.clone(),
                count: c_ping,
            })
            .await
            .expect("spawn ping");
        let pong = system
            .new_anon_actor(PingPong {
                peer: None,
                running: running.clone(),
                count: c_pong,
            })
            .await
            .expect("spawn pong");

        ping.notify(SetPeer(pong.clone())).expect("set peer ping");
        pong.notify(SetPeer(ping.clone())).expect("set peer pong");

        pings.push(ping);
    }

    running.store(true, Ordering::SeqCst);
    let start = Instant::now();

    for ping in &pings {
        for _ in 0..INITIAL_MSGS {
            ping.notify(Ball).expect("seed");
        }
    }

    tokio::time::sleep(std::time::Duration::from_secs(RUN_SECS)).await;
    running.store(false, Ordering::SeqCst);
    let elapsed = start.elapsed();

    let total: u64 = counters.iter().map(|c| c.load(Ordering::Relaxed)).sum();
    let secs = elapsed.as_secs_f64();
    let throughput = (total as f64 / secs) as u64;

    println!("Processed: {}", total);
    println!("Elapsed: {:.3} s", secs);
    println!("Throughput: {} msg/s", throughput);
}
