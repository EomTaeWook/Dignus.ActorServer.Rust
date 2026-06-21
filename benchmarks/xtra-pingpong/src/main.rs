use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use xtra::prelude::*;

const PAIRS: usize = 348;
const INITIAL_MSGS: usize = 1000;
const RUN_SECS: u64 = 10;

// Fire-and-forget message bounced between peers.
struct Ball;

// Message used to wire the peer address into an actor.
struct SetPeer(Address<PingPong>);

struct PingPong {
    peer: Option<Address<PingPong>>,
    running: Arc<AtomicBool>,
    count: Arc<AtomicU64>,
}

impl Actor for PingPong {
    type Stop = ();
    async fn stopped(self) -> Self::Stop {}
}

impl Handler<SetPeer> for PingPong {
    type Return = ();
    async fn handle(&mut self, msg: SetPeer, _ctx: &mut Context<Self>) {
        self.peer = Some(msg.0);
    }
}

impl Handler<Ball> for PingPong {
    type Return = ();
    async fn handle(&mut self, _msg: Ball, _ctx: &mut Context<Self>) {
        if self.running.load(Ordering::Relaxed) {
            self.count.fetch_add(1, Ordering::Relaxed);
            if let Some(peer) = &self.peer {
                let _ = peer.send(Ball).detach().await;
            }
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let running = Arc::new(AtomicBool::new(false));
    let mut counters: Vec<Arc<AtomicU64>> = Vec::new();
    let mut ping_addrs: Vec<Address<PingPong>> = Vec::new();

    for _ in 0..PAIRS {
        let ping_count = Arc::new(AtomicU64::new(0));
        let pong_count = Arc::new(AtomicU64::new(0));
        counters.push(ping_count.clone());
        counters.push(pong_count.clone());

        let ping = PingPong {
            peer: None,
            running: running.clone(),
            count: ping_count,
        };
        let pong = PingPong {
            peer: None,
            running: running.clone(),
            count: pong_count,
        };

        let ping_addr = xtra::spawn_tokio(ping, Mailbox::unbounded());
        let pong_addr = xtra::spawn_tokio(pong, Mailbox::unbounded());

        // Wire peers (FIFO mailbox: SetPeer arrives before any Ball).
        let _ = ping_addr.send(SetPeer(pong_addr.clone())).detach().await;
        let _ = pong_addr.send(SetPeer(ping_addr.clone())).detach().await;

        ping_addrs.push(ping_addr);
    }

    running.store(true, Ordering::Relaxed);
    let start = Instant::now();

    // Seed: 1000 initial messages to each Ping actor.
    for ping_addr in &ping_addrs {
        for _ in 0..INITIAL_MSGS {
            let _ = ping_addr.send(Ball).detach().await;
        }
    }

    tokio::time::sleep(std::time::Duration::from_secs(RUN_SECS)).await;
    running.store(false, Ordering::Relaxed);
    let elapsed = start.elapsed();

    let processed: u64 = counters.iter().map(|c| c.load(Ordering::Relaxed)).sum();
    let secs = elapsed.as_secs_f64();
    let throughput = (processed as f64 / secs) as u64;

    println!("Processed: {} messages", processed);
    println!("Elapsed: {:.3} s", secs);
    println!("Throughput: {} msg/s", throughput);
}
