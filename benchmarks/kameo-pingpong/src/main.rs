use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use kameo::actor::{ActorRef, Spawn};
use kameo::message::{Context, Message};
use kameo::Actor;

const PAIRS: usize = 348;
const PIPELINE: usize = 1000;
const RUN_SECS: u64 = 10;

// Fire-and-forget bounce message.
struct Bounce;

// Wire the peer address into an actor (processed before any Bounce due to FIFO mailbox).
struct SetPeer(ActorRef<Node>);

#[derive(Actor)]
struct Node {
    peer: Option<ActorRef<Node>>,
    running: Arc<AtomicBool>,
    count: Arc<AtomicU64>,
}

impl Node {
    fn new(running: Arc<AtomicBool>, count: Arc<AtomicU64>) -> Self {
        Node {
            peer: None,
            running,
            count,
        }
    }
}

impl Message<SetPeer> for Node {
    type Reply = ();

    async fn handle(&mut self, msg: SetPeer, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.peer = Some(msg.0);
    }
}

impl Message<Bounce> for Node {
    type Reply = ();

    async fn handle(&mut self, _msg: Bounce, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        if self.running.load(Ordering::Relaxed) {
            self.count.fetch_add(1, Ordering::Relaxed);
            if let Some(peer) = &self.peer {
                // fire-and-forget; ignore mailbox-closed errors at shutdown
                let _ = peer.tell(Bounce).try_send();
            }
        }
    }
}

#[tokio::main]
async fn main() {
    let running = Arc::new(AtomicBool::new(false));

    let mut pings: Vec<ActorRef<Node>> = Vec::with_capacity(PAIRS);
    let mut counters: Vec<Arc<AtomicU64>> = Vec::with_capacity(PAIRS * 2);

    for _ in 0..PAIRS {
        let ping_count = Arc::new(AtomicU64::new(0));
        let pong_count = Arc::new(AtomicU64::new(0));
        counters.push(ping_count.clone());
        counters.push(pong_count.clone());

        let ping = Node::spawn(Node::new(running.clone(), ping_count));
        let pong = Node::spawn(Node::new(running.clone(), pong_count));

        // wire peers (FIFO mailbox guarantees these land before seeded Bounces)
        ping.tell(SetPeer(pong.clone())).await.unwrap();
        pong.tell(SetPeer(ping.clone())).await.unwrap();

        pings.push(ping);
    }

    // start
    running.store(true, Ordering::Relaxed);
    let start = Instant::now();

    // seed pipeline: 1000 initial messages to each Ping
    for ping in &pings {
        for _ in 0..PIPELINE {
            ping.tell(Bounce).await.unwrap();
        }
    }

    tokio::time::sleep(Duration::from_secs(RUN_SECS)).await;
    running.store(false, Ordering::Relaxed);
    let elapsed = start.elapsed();

    // let in-flight messages settle so they observe running=false and stop
    tokio::time::sleep(Duration::from_millis(200)).await;

    let total: u64 = counters.iter().map(|c| c.load(Ordering::Relaxed)).sum();
    let secs = elapsed.as_secs_f64();
    let throughput = (total as f64 / secs) as u64;

    println!("Processed: {} messages", total);
    println!("Elapsed: {:.3} s", secs);
    println!("Throughput: {} msg/s", throughput);
}
