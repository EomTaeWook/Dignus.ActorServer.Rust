use dignus_actor_core::actor_base::{ActorBase, ActorContext, ActorReceiveResult};
use dignus_actor_core::actor_ref_trait::ActorRefTrait;
use dignus_actor_core::actor_system::ActorSystem;
use dignus_actor_core::messages::actor_message_trait::ActorMessageTrait;
use std::any::Any;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::sync::{mpsc, Arc};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

struct Sequence(usize);

impl ActorMessageTrait for Sequence {
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

struct RecordingActor {
    context: ActorContext,
    expected: usize,
    done: mpsc::Sender<usize>,
}

impl ActorBase for RecordingActor {
    fn actor_context(&self) -> &ActorContext {
        &self.context
    }

    fn on_receive<'actor>(
        &'actor mut self,
        message: Box<dyn ActorMessageTrait>,
        _sender: Option<Arc<dyn ActorRefTrait>>,
    ) -> ActorReceiveResult<'actor> {
        let sequence = message.into_any().downcast::<Sequence>().unwrap().0;
        assert_eq!(sequence, self.expected);
        self.expected += 1;
        if self.expected == 10_000 {
            self.done.send(self.expected).unwrap();
        }
        ActorReceiveResult::Done
    }
}

#[test]
fn preserves_message_order_from_one_producer() {
    let (done_tx, done_rx) = mpsc::channel();
    let system = ActorSystem::new(2);
    let actor = system.spawn_with_factory_options(
        || RecordingActor {
            context: ActorContext::new(),
            expected: 0,
            done: done_tx,
        },
        None,
        16_384,
    );

    for value in 0..10_000 {
        actor.post(Box::new(Sequence(value)), None);
    }

    assert_eq!(
        done_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
        10_000
    );
    system.shutdown_timeout(Duration::from_secs(5)).unwrap();
}

struct Count;

impl ActorMessageTrait for Count {
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

struct CountingActor {
    context: ActorContext,
    count: usize,
    target: usize,
    done: mpsc::Sender<usize>,
}

impl ActorBase for CountingActor {
    fn actor_context(&self) -> &ActorContext {
        &self.context
    }

    fn on_receive<'actor>(
        &'actor mut self,
        _message: Box<dyn ActorMessageTrait>,
        _sender: Option<Arc<dyn ActorRefTrait>>,
    ) -> ActorReceiveResult<'actor> {
        self.count += 1;
        if self.count == self.target {
            self.done.send(self.count).unwrap();
        }
        ActorReceiveResult::Done
    }
}

#[test]
fn serializes_messages_from_multiple_producers() {
    const PRODUCERS: usize = 8;
    const PER_PRODUCER: usize = 2_000;

    let (done_tx, done_rx) = mpsc::channel();
    let system = ActorSystem::new(4);
    let actor = system.spawn_with_factory_options(
        || CountingActor {
            context: ActorContext::new(),
            count: 0,
            target: PRODUCERS * PER_PRODUCER,
            done: done_tx,
        },
        None,
        32_768,
    );

    let mut producers = Vec::new();
    for _ in 0..PRODUCERS {
        let actor = Arc::clone(&actor);
        producers.push(std::thread::spawn(move || {
            for _ in 0..PER_PRODUCER {
                actor.post(Box::new(Count), None);
            }
        }));
    }
    for producer in producers {
        producer.join().unwrap();
    }

    assert_eq!(
        done_rx.recv_timeout(Duration::from_secs(5)).unwrap(),
        PRODUCERS * PER_PRODUCER
    );
    system.shutdown_timeout(Duration::from_secs(5)).unwrap();
}

struct PendingActor {
    context: ActorContext,
    killed: Arc<AtomicBool>,
    gate: Arc<Gate>,
}

#[derive(Default)]
struct Gate {
    open: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

impl Gate {
    fn open(&self) {
        self.open.store(true, Ordering::Release);
        if let Some(waker) = self.waker.lock().unwrap().take() {
            waker.wake();
        }
    }
}

struct GateFuture(Arc<Gate>);

impl std::future::Future for GateFuture {
    type Output = ();

    fn poll(self: std::pin::Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        if self.0.open.load(Ordering::Acquire) {
            Poll::Ready(())
        } else {
            *self.0.waker.lock().unwrap() = Some(context.waker().clone());
            Poll::Pending
        }
    }
}

impl ActorBase for PendingActor {
    fn actor_context(&self) -> &ActorContext {
        &self.context
    }

    fn on_receive<'actor>(
        &'actor mut self,
        _message: Box<dyn ActorMessageTrait>,
        _sender: Option<Arc<dyn ActorRefTrait>>,
    ) -> ActorReceiveResult<'actor> {
        ActorReceiveResult::Pending(Box::pin(GateFuture(Arc::clone(&self.gate))))
    }

    fn on_kill(&mut self) {
        self.killed.store(true, Ordering::Release);
    }
}

#[test]
fn shutdown_timeout_does_not_hang_on_pending_receive() {
    let killed = Arc::new(AtomicBool::new(false));
    let gate = Arc::new(Gate::default());
    let system = ActorSystem::new(1);
    let actor = system.spawn_with_factory({
        let killed = Arc::clone(&killed);
        let gate = Arc::clone(&gate);
        move || PendingActor {
            context: ActorContext::new(),
            killed,
            gate,
        }
    });
    actor.post(Box::new(Count), None);

    std::thread::sleep(Duration::from_millis(20));
    let error = system
        .shutdown_timeout(Duration::from_millis(20))
        .unwrap_err();

    assert_eq!(error.remaining_actors(), 1);
    assert!(!killed.load(Ordering::Acquire));
    gate.open();
    system.shutdown_timeout(Duration::from_secs(5)).unwrap();
    assert!(killed.load(Ordering::Acquire));
}

struct KillActor {
    context: ActorContext,
    killed: Arc<AtomicUsize>,
}

impl ActorBase for KillActor {
    fn actor_context(&self) -> &ActorContext {
        &self.context
    }

    fn on_receive<'actor>(
        &'actor mut self,
        _message: Box<dyn ActorMessageTrait>,
        _sender: Option<Arc<dyn ActorRefTrait>>,
    ) -> ActorReceiveResult<'actor> {
        ActorReceiveResult::Done
    }

    fn on_kill(&mut self) {
        self.killed.fetch_add(1, Ordering::AcqRel);
    }
}

#[test]
fn shutdown_kills_each_actor_once() {
    let killed = Arc::new(AtomicUsize::new(0));
    let system = ActorSystem::new(2);
    for _ in 0..100 {
        system.spawn_with_factory({
            let killed = Arc::clone(&killed);
            move || KillActor {
                context: ActorContext::new(),
                killed,
            }
        });
    }

    system.shutdown_timeout(Duration::from_secs(5)).unwrap();
    assert_eq!(killed.load(Ordering::Acquire), 100);
}
