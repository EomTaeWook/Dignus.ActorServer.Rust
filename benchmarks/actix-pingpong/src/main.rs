//! actix ping-pong throughput benchmark, matching the C# / Dignus setup:
//! 348 ping/pong pairs (696 actors), 1000 in-flight per pair, 10s, per-actor
//! counter summed afterwards. Actors are spread across one Arbiter (thread) per
//! logical CPU; messages use `do_send` (fire-and-forget, the Post equivalent).

use actix::prelude::*;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const ACTOR_PAIR_COUNT: usize = 348;
const PIPELINE_SIZE_PER_PAIR: usize = 1_000;
const BENCHMARK_SECONDS: u64 = 10;

#[derive(Message)]
#[rtype(result = "()")]
struct Ping;

#[derive(Message)]
#[rtype(result = "()")]
struct Pong;

#[derive(Message)]
#[rtype(result = "()")]
struct SetPong(Addr<PongActor>);

#[derive(Message)]
#[rtype(result = "()")]
struct SetPing(Addr<PingActor>);

struct PingActor {
    peer: Option<Addr<PongActor>>,
    running: Arc<AtomicBool>,
    counter: Arc<AtomicU64>,
}

struct PongActor {
    peer: Option<Addr<PingActor>>,
    running: Arc<AtomicBool>,
    counter: Arc<AtomicU64>,
}

impl Actor for PingActor {
    type Context = Context<Self>;
}

impl Actor for PongActor {
    type Context = Context<Self>;
}

impl Handler<SetPong> for PingActor {
    type Result = ();
    fn handle(&mut self, message: SetPong, _ctx: &mut Context<Self>) {
        self.peer = Some(message.0);
    }
}

impl Handler<SetPing> for PongActor {
    type Result = ();
    fn handle(&mut self, message: SetPing, _ctx: &mut Context<Self>) {
        self.peer = Some(message.0);
    }
}

impl Handler<Ping> for PingActor {
    type Result = ();
    fn handle(&mut self, _message: Ping, _ctx: &mut Context<Self>) {
        if self.running.load(Ordering::Relaxed) {
            self.counter.fetch_add(1, Ordering::Relaxed);
            if let Some(peer) = &self.peer {
                peer.do_send(Pong);
            }
        }
    }
}

impl Handler<Pong> for PongActor {
    type Result = ();
    fn handle(&mut self, _message: Pong, _ctx: &mut Context<Self>) {
        if self.running.load(Ordering::Relaxed) {
            self.counter.fetch_add(1, Ordering::Relaxed);
            if let Some(peer) = &self.peer {
                peer.do_send(Ping);
            }
        }
    }
}

fn main() {
    let thread_count = std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(4);

    let system = System::new();

    let (processed_message_count, elapsed) = system.block_on(async move {
        let arbiters: Vec<Arbiter> = (0..thread_count).map(|_| Arbiter::new()).collect();

        let running = Arc::new(AtomicBool::new(false));
        let mut counters: Vec<Arc<AtomicU64>> = Vec::with_capacity(ACTOR_PAIR_COUNT * 2);
        let mut ping_addrs: Vec<Addr<PingActor>> = Vec::with_capacity(ACTOR_PAIR_COUNT);

        for pair_index in 0..ACTOR_PAIR_COUNT {
            let ping_counter = Arc::new(AtomicU64::new(0));
            let pong_counter = Arc::new(AtomicU64::new(0));

            let ping = {
                let running = Arc::clone(&running);
                let counter = Arc::clone(&ping_counter);
                PingActor::start_in_arbiter(&arbiters[(2 * pair_index) % thread_count].handle(), move |_ctx| {
                    PingActor { peer: None, running, counter }
                })
            };

            let pong = {
                let running = Arc::clone(&running);
                let counter = Arc::clone(&pong_counter);
                PongActor::start_in_arbiter(&arbiters[(2 * pair_index + 1) % thread_count].handle(), move |_ctx| {
                    PongActor { peer: None, running, counter }
                })
            };

            // FIFO mailbox: SetPong/SetPing arrive before the seeded Pings below.
            ping.do_send(SetPong(pong.clone()));
            pong.do_send(SetPing(ping.clone()));

            ping_addrs.push(ping);
            counters.push(ping_counter);
            counters.push(pong_counter);
        }

        running.store(true, Ordering::SeqCst);

        let stopwatch = Instant::now();

        for ping in &ping_addrs {
            for _ in 0..PIPELINE_SIZE_PER_PAIR {
                ping.do_send(Ping);
            }
        }

        tokio::time::sleep(Duration::from_secs(BENCHMARK_SECONDS)).await;

        running.store(false, Ordering::SeqCst);
        let elapsed = stopwatch.elapsed();

        let processed_message_count: u64 =
            counters.iter().map(|counter| counter.load(Ordering::Relaxed)).sum();

        (processed_message_count, elapsed)
    });

    let elapsed_seconds = elapsed.as_secs_f64();
    let messages_per_second = processed_message_count as f64 / elapsed_seconds;

    println!("[actix]");
    println!("Arbiter (thread) Count: {}", thread_count);
    println!("Actor Pair Count: {}", ACTOR_PAIR_COUNT);
    println!("Actual Actor Count: {}", ACTOR_PAIR_COUNT * 2);
    println!("Pipeline Size Per Pair: {}", PIPELINE_SIZE_PER_PAIR);
    println!("Processed Messages: {}", processed_message_count);
    println!("Elapsed: {:.3} sec", elapsed_seconds);
    println!("Throughput: {:.0} msg/s", messages_per_second);
}
