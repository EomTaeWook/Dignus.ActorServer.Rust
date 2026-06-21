//! ractor ping-pong throughput benchmark, matching the C# / Dignus setup:
//! 348 ping/pong pairs (696 actors), 1000 in-flight per pair, 10s, per-actor
//! counter summed afterwards.
//!
//! ractor actors are tokio tasks; parallelism comes from the multi-threaded
//! tokio runtime (one worker per CPU). Messages use `send_message` (the
//! fire-and-forget Post equivalent). Both ends of a pair are the same `Node`
//! actor that simply bounces the message back to its peer.

use ractor::{Actor, ActorProcessingErr, ActorRef};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const ACTOR_PAIR_COUNT: usize = 348;
const PIPELINE_SIZE_PER_PAIR: usize = 1_000;
const BENCHMARK_SECONDS: u64 = 10;

enum NodeMsg {
    Bounce,
    SetPeer(ActorRef<NodeMsg>),
}

struct Node;

struct NodeState {
    peer: Option<ActorRef<NodeMsg>>,
    running: Arc<AtomicBool>,
    counter: Arc<AtomicU64>,
}

impl Actor for Node {
    type Msg = NodeMsg;
    type State = NodeState;
    type Arguments = (Arc<AtomicBool>, Arc<AtomicU64>);

    async fn pre_start(
        &self,
        _myself: ActorRef<Self::Msg>,
        args: Self::Arguments,
    ) -> Result<Self::State, ActorProcessingErr> {
        Ok(NodeState {
            peer: None,
            running: args.0,
            counter: args.1,
        })
    }

    async fn handle(
        &self,
        _myself: ActorRef<Self::Msg>,
        message: Self::Msg,
        state: &mut Self::State,
    ) -> Result<(), ActorProcessingErr> {
        match message {
            NodeMsg::SetPeer(peer) => {
                state.peer = Some(peer);
            }
            NodeMsg::Bounce => {
                if state.running.load(Ordering::Relaxed) {
                    state.counter.fetch_add(1, Ordering::Relaxed);
                    if let Some(peer) = &state.peer {
                        let _ = peer.send_message(NodeMsg::Bounce);
                    }
                }
            }
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let running = Arc::new(AtomicBool::new(false));

    let mut counters: Vec<Arc<AtomicU64>> = Vec::with_capacity(ACTOR_PAIR_COUNT * 2);
    let mut a_refs: Vec<ActorRef<NodeMsg>> = Vec::with_capacity(ACTOR_PAIR_COUNT);
    let mut b_refs: Vec<ActorRef<NodeMsg>> = Vec::with_capacity(ACTOR_PAIR_COUNT);

    for _ in 0..ACTOR_PAIR_COUNT {
        let a_counter = Arc::new(AtomicU64::new(0));
        let b_counter = Arc::new(AtomicU64::new(0));

        let (a_ref, _a_handle) = Actor::spawn(None, Node, (Arc::clone(&running), Arc::clone(&a_counter)))
            .await
            .expect("spawn a");
        let (b_ref, _b_handle) = Actor::spawn(None, Node, (Arc::clone(&running), Arc::clone(&b_counter)))
            .await
            .expect("spawn b");

        // FIFO mailbox: SetPeer arrives before the seeded Bounces below.
        a_ref.send_message(NodeMsg::SetPeer(b_ref.clone())).unwrap();
        b_ref.send_message(NodeMsg::SetPeer(a_ref.clone())).unwrap();

        a_refs.push(a_ref);
        b_refs.push(b_ref);
        counters.push(a_counter);
        counters.push(b_counter);
    }

    running.store(true, Ordering::SeqCst);

    let stopwatch = Instant::now();

    for a_ref in &a_refs {
        for _ in 0..PIPELINE_SIZE_PER_PAIR {
            a_ref.send_message(NodeMsg::Bounce).unwrap();
        }
    }

    tokio::time::sleep(Duration::from_secs(BENCHMARK_SECONDS)).await;

    running.store(false, Ordering::SeqCst);
    let elapsed = stopwatch.elapsed();

    let processed_message_count: u64 =
        counters.iter().map(|counter| counter.load(Ordering::Relaxed)).sum();

    for a_ref in &a_refs {
        a_ref.stop(None);
    }
    for b_ref in &b_refs {
        b_ref.stop(None);
    }

    let elapsed_seconds = elapsed.as_secs_f64();
    let messages_per_second = processed_message_count as f64 / elapsed_seconds;

    let worker_threads = std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(0);

    println!("[ractor]");
    println!("Tokio Worker Threads: {}", worker_threads);
    println!("Actor Pair Count: {}", ACTOR_PAIR_COUNT);
    println!("Actual Actor Count: {}", ACTOR_PAIR_COUNT * 2);
    println!("Pipeline Size Per Pair: {}", PIPELINE_SIZE_PER_PAIR);
    println!("Processed Messages: {}", processed_message_count);
    println!("Elapsed: {:.3} sec", elapsed_seconds);
    println!("Throughput: {:.0} msg/s", messages_per_second);
}
