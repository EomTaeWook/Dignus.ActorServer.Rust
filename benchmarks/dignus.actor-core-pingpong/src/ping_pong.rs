
use dignus_actor_core::actor_base::{ActorBase, ActorContext, ActorReceiveResult};
use dignus_actor_core::actor_ref_trait::ActorRefTrait;
use dignus_actor_core::actor_system::ActorSystem;
use dignus_actor_core::messages::actor_message_trait::ActorMessageTrait;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};


const ACTOR_PAIR_COUNT: usize = 348;
const PIPELINE_SIZE_PER_PAIR: usize = 1_000;
const MAILBOX_CAPACITY: usize = 2_048;
const BENCHMARK_SECONDS: u64 = 10;


struct PingMessage;
struct PongMessage;

impl ActorMessageTrait for PingMessage {}
impl ActorMessageTrait for PongMessage {}

type PeerSlot = Arc<OnceLock<Arc<dyn ActorRefTrait>>>;


struct PingActor {
    context: ActorContext,
    running: Arc<AtomicBool>,
    processed: u64,
    report: Arc<AtomicU64>,
    pong_ref: PeerSlot,
}

impl PingActor {
    fn new(running: Arc<AtomicBool>, report: Arc<AtomicU64>, pong_ref: PeerSlot) -> Self {
        Self {
            context: ActorContext::new(),
            running,
            processed: 0,
            report,
            pong_ref,
        }
    }
}

impl ActorBase for PingActor {
    fn actor_context(&self) -> &ActorContext {
        &self.context
    }

    fn on_receive<'actor>(
        &'actor mut self,
        _message: Box<dyn ActorMessageTrait>,
        _sender: Option<Arc<dyn ActorRefTrait>>,
    ) -> ActorReceiveResult<'actor> {
        if self.running.load(Ordering::Relaxed) {
            self.processed += 1;

            if let Some(pong_ref) = self.pong_ref.get() {
                self.post(&**pong_ref, Box::new(PongMessage));
            }
        }

        ActorReceiveResult::Done
    }

    fn on_kill(&mut self) {
        self.report.store(self.processed, Ordering::Relaxed);
    }
}


struct PongActor {
    context: ActorContext,
    running: Arc<AtomicBool>,
    processed: u64,
    report: Arc<AtomicU64>,
    ping_ref: PeerSlot,
}

impl PongActor {
    fn new(running: Arc<AtomicBool>, report: Arc<AtomicU64>, ping_ref: PeerSlot) -> Self {
        Self {
            context: ActorContext::new(),
            running,
            processed: 0,
            report,
            ping_ref,
        }
    }
}

impl ActorBase for PongActor {
    fn actor_context(&self) -> &ActorContext {
        &self.context
    }

    fn on_receive<'actor>(
        &'actor mut self,
        _message: Box<dyn ActorMessageTrait>,
        _sender: Option<Arc<dyn ActorRefTrait>>,
    ) -> ActorReceiveResult<'actor> {
        if self.running.load(Ordering::Relaxed) {
            self.processed += 1;

            if let Some(ping_ref) = self.ping_ref.get() {
                self.post(&**ping_ref, Box::new(PingMessage));
            }
        }

        ActorReceiveResult::Done
    }

    fn on_kill(&mut self) {
        self.report.store(self.processed, Ordering::Relaxed);
    }
}


fn main() {
    let dispatcher_count = std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(4);

    let actor_system = ActorSystem::new(dispatcher_count);
    let running = Arc::new(AtomicBool::new(false));

    let mut ping_refs: Vec<Arc<dyn ActorRefTrait>> = Vec::with_capacity(ACTOR_PAIR_COUNT);
    let mut counters: Vec<Arc<AtomicU64>> = Vec::with_capacity(ACTOR_PAIR_COUNT * 2);

    for _ in 0..ACTOR_PAIR_COUNT {
        let ping_counter = Arc::new(AtomicU64::new(0));
        let pong_counter = Arc::new(AtomicU64::new(0));

        let pong_slot: PeerSlot = Arc::new(OnceLock::new());
        let ping_slot: PeerSlot = Arc::new(OnceLock::new());

        let ping_ref = actor_system.spawn_with_factory_and_capacity::<PingActor, _>(
            {
                let running = Arc::clone(&running);
                let counter = Arc::clone(&ping_counter);
                let pong_slot = Arc::clone(&pong_slot);
                move || PingActor::new(running, counter, pong_slot)
            },
            MAILBOX_CAPACITY,
        );

        let pong_ref = actor_system.spawn_with_factory_and_capacity::<PongActor, _>(
            {
                let running = Arc::clone(&running);
                let counter = Arc::clone(&pong_counter);
                let ping_slot = Arc::clone(&ping_slot);
                move || PongActor::new(running, counter, ping_slot)
            },
            MAILBOX_CAPACITY,
        );

        let _ = pong_slot.set(Arc::clone(&pong_ref));
        let _ = ping_slot.set(Arc::clone(&ping_ref));

        ping_refs.push(ping_ref);
        counters.push(ping_counter);
        counters.push(pong_counter);
    }

    running.store(true, Ordering::SeqCst);

    let stopwatch = Instant::now();

    for ping_ref in &ping_refs {
        for _ in 0..PIPELINE_SIZE_PER_PAIR {
            ping_ref.post(Box::new(PingMessage), None);
        }
    }

    std::thread::sleep(Duration::from_secs(BENCHMARK_SECONDS));

    running.store(false, Ordering::SeqCst);
    let elapsed = stopwatch.elapsed();

    actor_system.dispose();

    let processed_message_count: u64 = counters
        .iter()
        .map(|counter| counter.load(Ordering::Relaxed))
        .sum();

    let elapsed_seconds = elapsed.as_secs_f64();
    let messages_per_second = processed_message_count as f64 / elapsed_seconds;

    println!("Dispatcher Count: {}", dispatcher_count);
    println!("Actor Pair Count: {}", fmt(ACTOR_PAIR_COUNT as u64));
    println!("Actual Actor Count: {}", fmt((ACTOR_PAIR_COUNT * 2) as u64));
    println!("Pipeline Size Per Pair: {}", fmt(PIPELINE_SIZE_PER_PAIR as u64));
    println!("Processed Messages: {}", fmt(processed_message_count));
    println!("Elapsed: {:.3} sec", elapsed_seconds);
    println!("Throughput: {} msg/s", fmt(messages_per_second as u64));
}

fn fmt(value: u64) -> String {
    let digits = value.to_string();
    let bytes = digits.as_bytes();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);

    for (index, byte) in bytes.iter().enumerate() {
        if index > 0 && (bytes.len() - index) % 3 == 0 {
            out.push(',');
        }
        out.push(*byte as char);
    }

    out
}
