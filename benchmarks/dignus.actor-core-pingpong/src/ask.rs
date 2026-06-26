use dignus_actor_core::actor_base::{ActorBase, ActorContext, ActorReceiveResult};
use dignus_actor_core::actor_ref_trait::ActorRefTrait;
use dignus_actor_core::actor_system::ActorSystem;
use dignus_actor_core::messages::actor_message_trait::ActorMessageTrait;
use dignus_actor_core::ActorRef;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

const ACTOR_COUNT: usize = 32;
const ASKERS_PER_ACTOR: usize = 1024;
const BENCHMARK_SECONDS: u64 = 10;
const ASK_TIMEOUT_MS: u64 = 3000;

struct AskPing;
struct AskPong;
struct Start;
impl ActorMessageTrait for AskPing {
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }
}
impl ActorMessageTrait for AskPong {
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }
}
impl ActorMessageTrait for Start {
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }
}

struct AskBenchmarkActor {
    context: ActorContext,
}

impl ActorBase for AskBenchmarkActor {
    fn actor_context(&self) -> &ActorContext {
        &self.context
    }

    fn on_receive<'actor>(
        &'actor mut self,
        _message: Box<dyn ActorMessageTrait>,
        sender: Option<Arc<dyn ActorRefTrait>>,
    ) -> ActorReceiveResult<'actor> {
        if let Some(sender) = sender {
            sender.post(Box::new(AskPong), None);
        }
        ActorReceiveResult::Done
    }
}

struct AskerActor {
    context: ActorContext,
    target: Arc<ActorRef>,
    running: Arc<AtomicBool>,
    completed: Arc<AtomicU64>,
}

impl ActorBase for AskerActor {
    fn actor_context(&self) -> &ActorContext {
        &self.context
    }

    fn on_receive<'actor>(
        &'actor mut self,
        _message: Box<dyn ActorMessageTrait>,
        _sender: Option<Arc<dyn ActorRefTrait>>,
    ) -> ActorReceiveResult<'actor> {
        let target = Arc::clone(&self.target);
        let running = Arc::clone(&self.running);
        let completed = Arc::clone(&self.completed);

        ActorReceiveResult::Pending(Box::pin(async move {
            while running.load(Ordering::Relaxed) {
                let reply =
                    target.ask::<AskPong>(Box::new(AskPing), Duration::from_millis(ASK_TIMEOUT_MS));
                if reply.await.is_ok() {
                    completed.fetch_add(1, Ordering::Relaxed);
                }
            }
        }))
    }
}

fn main() {
    let dispatcher_count = std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(4);

    let actor_system = ActorSystem::new(dispatcher_count);
    let running = Arc::new(AtomicBool::new(true));
    let mut counters: Vec<Arc<AtomicU64>> = Vec::with_capacity(ACTOR_COUNT * ASKERS_PER_ACTOR);

    let mut targets: Vec<Arc<ActorRef>> = Vec::with_capacity(ACTOR_COUNT);
    for _ in 0..ACTOR_COUNT {
        targets.push(actor_system.spawn_with_factory_and_capacity::<AskBenchmarkActor, _>(
            || AskBenchmarkActor { context: ActorContext::new() },
            4096,
        ));
    }

    let mut askers: Vec<Arc<dyn ActorRefTrait>> = Vec::with_capacity(ACTOR_COUNT * ASKERS_PER_ACTOR);
    for target in &targets {
        for _ in 0..ASKERS_PER_ACTOR {
            let target_clone = Arc::clone(target);
            let running_clone = Arc::clone(&running);
            let counter = Arc::new(AtomicU64::new(0));
            counters.push(Arc::clone(&counter));

            let asker = actor_system.spawn_with_factory_and_capacity::<AskerActor, _>(
                move || AskerActor {
                    context: ActorContext::new(),
                    target: target_clone,
                    running: running_clone,
                    completed: counter,
                },
                16,
            );
            askers.push(asker);
        }
    }

    let stopwatch = Instant::now();

    for asker in &askers {
        asker.post(Box::new(Start), None);
    }

    std::thread::sleep(Duration::from_secs(BENCHMARK_SECONDS));

    running.store(false, Ordering::SeqCst);
    let elapsed = stopwatch.elapsed();

    std::thread::sleep(Duration::from_millis(300));

    let completed_count: u64 = counters.iter().map(|c| c.load(Ordering::Relaxed)).sum();
    let elapsed_seconds = elapsed.as_secs_f64();

    println!("[ask]");
    println!("Dispatcher Count: {}", dispatcher_count);
    println!("Actor Count: {}", ACTOR_COUNT);
    println!("Askers Per Actor: {}", ASKERS_PER_ACTOR);
    println!("In-flight Asks: {}", ACTOR_COUNT * ASKERS_PER_ACTOR);
    println!("Completed Ask Count: {}", completed_count);
    println!("Elapsed: {:.3} sec", elapsed_seconds);
    println!("Throughput: {:.0} ask/s", completed_count as f64 / elapsed_seconds);
}
