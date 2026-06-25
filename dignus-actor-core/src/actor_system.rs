use crate::{
    actor_base::ActorBase,
    dead_letter::{
        dead_letter_message::DeadLetterMessage,
        dead_letter_publisher_trait::DeadLetterPublisherTrait,
    },
    dispatcher::actor_dispatcher::ActorDispatcher,
    internals::{
        actor_ref::ActorRef, actor_runner::ActorRunner, ask_system::AskSystem,
        registry::ActorRegistry,
    },
};

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, RwLock,
    },
    thread,
};

const DEFAULT_MAILBOX_CAPACITY: usize = 1024;
const DEFAULT_ACTOR_CAPACITY: usize = 1 << 16;

type DeadLetterCallback = Arc<dyn Fn(&DeadLetterMessage) + Send + Sync + 'static>;

pub struct ActorSystem {
    registry: Arc<ActorRegistry>,
    alias_to_handle: RwLock<HashMap<String, (u32, u32)>>,
    dispatchers: Vec<Arc<ActorDispatcher>>,
    is_disposed: AtomicBool,
    dead_letter_callback: Mutex<Option<DeadLetterCallback>>,
    ask_system: Arc<AskSystem>,
}

impl ActorSystem {
    pub fn new(dispatcher_thread_count: usize) -> Arc<Self> {
        Self::with_capacity(dispatcher_thread_count, DEFAULT_ACTOR_CAPACITY)
    }

    pub fn with_capacity(dispatcher_thread_count: usize, actor_capacity: usize) -> Arc<Self> {
        if dispatcher_thread_count == 0 {
            panic!("dispatcher_thread_count must be greater than 0.");
        }

        let mut dispatchers = Vec::with_capacity(dispatcher_thread_count);

        for dispatcher_index in 0..dispatcher_thread_count {
            let actor_dispatcher = ActorDispatcher::new(dispatcher_index as i32);
            actor_dispatcher.start();
            dispatchers.push(actor_dispatcher);
        }

        Arc::new(Self {
            registry: Arc::new(ActorRegistry::with_capacity(actor_capacity)),
            alias_to_handle: RwLock::new(HashMap::new()),
            dispatchers,
            is_disposed: AtomicBool::new(false),
            dead_letter_callback: Mutex::new(None),
            ask_system: AskSystem::new(dispatcher_thread_count),
        })
    }

    pub fn set_dead_letter_callback<TDeadLetterCallback>(
        &self,
        dead_letter_callback: TDeadLetterCallback,
    ) where
        TDeadLetterCallback: Fn(&DeadLetterMessage) + Send + Sync + 'static,
    {
        let mut current_dead_letter_callback = self.dead_letter_callback.lock().unwrap();
        *current_dead_letter_callback = Some(Arc::new(dead_letter_callback));
    }

    pub fn clear_dead_letter_callback(&self) {
        let mut current_dead_letter_callback = self.dead_letter_callback.lock().unwrap();
        *current_dead_letter_callback = None;
    }

    pub fn dispatcher_count(&self) -> usize {
        self.dispatchers.len()
    }

    pub fn spawn<TActor>(self: &Arc<Self>) -> Arc<ActorRef>
    where
        TActor: ActorBase + Default + 'static,
    {
        self.spawn_with_options::<TActor>(None, DEFAULT_MAILBOX_CAPACITY)
    }

    pub fn spawn_with_alias<TActor>(self: &Arc<Self>, alias: String) -> Arc<ActorRef>
    where
        TActor: ActorBase + Default + 'static,
    {
        self.spawn_with_options::<TActor>(Some(alias), DEFAULT_MAILBOX_CAPACITY)
    }

    pub fn spawn_with_capacity<TActor>(self: &Arc<Self>, mailbox_capacity: usize) -> Arc<ActorRef>
    where
        TActor: ActorBase + Default + 'static,
    {
        self.spawn_with_options::<TActor>(None, mailbox_capacity)
    }

    pub fn spawn_with_options<TActor>(
        self: &Arc<Self>,
        alias: Option<String>,
        mailbox_capacity: usize,
    ) -> Arc<ActorRef>
    where
        TActor: ActorBase + Default + 'static,
    {
        self.spawn_with_auto_dispatcher(TActor::default(), alias, mailbox_capacity)
    }

    pub fn spawn_with_factory<TActor, TFactory>(
        self: &Arc<Self>,
        factory: TFactory,
    ) -> Arc<ActorRef>
    where
        TActor: ActorBase + 'static,
        TFactory: FnOnce() -> TActor,
    {
        self.spawn_with_factory_options(factory, None, DEFAULT_MAILBOX_CAPACITY)
    }

    pub fn spawn_with_factory_and_alias<TActor, TFactory>(
        self: &Arc<Self>,
        factory: TFactory,
        alias: String,
    ) -> Arc<ActorRef>
    where
        TActor: ActorBase + 'static,
        TFactory: FnOnce() -> TActor,
    {
        self.spawn_with_factory_options(factory, Some(alias), DEFAULT_MAILBOX_CAPACITY)
    }

    pub fn spawn_with_factory_and_capacity<TActor, TFactory>(
        self: &Arc<Self>,
        factory: TFactory,
        mailbox_capacity: usize,
    ) -> Arc<ActorRef>
    where
        TActor: ActorBase + 'static,
        TFactory: FnOnce() -> TActor,
    {
        self.spawn_with_factory_options(factory, None, mailbox_capacity)
    }

    pub fn spawn_with_factory_options<TActor, TFactory>(
        self: &Arc<Self>,
        factory: TFactory,
        alias: Option<String>,
        mailbox_capacity: usize,
    ) -> Arc<ActorRef>
    where
        TActor: ActorBase + 'static,
        TFactory: FnOnce() -> TActor,
    {
        self.spawn_with_auto_dispatcher(factory(), alias, mailbox_capacity)
    }

    pub fn spawn_on_dispatcher<TActor>(self: &Arc<Self>, dispatcher_index: usize) -> Arc<ActorRef>
    where
        TActor: ActorBase + Default + 'static,
    {
        self.spawn_on_dispatcher_with_options::<TActor>(
            dispatcher_index,
            None,
            DEFAULT_MAILBOX_CAPACITY,
        )
    }

    pub fn spawn_on_dispatcher_with_alias<TActor>(
        self: &Arc<Self>,
        dispatcher_index: usize,
        alias: String,
    ) -> Arc<ActorRef>
    where
        TActor: ActorBase + Default + 'static,
    {
        self.spawn_on_dispatcher_with_options::<TActor>(
            dispatcher_index,
            Some(alias),
            DEFAULT_MAILBOX_CAPACITY,
        )
    }

    pub fn spawn_on_dispatcher_with_capacity<TActor>(
        self: &Arc<Self>,
        dispatcher_index: usize,
        mailbox_capacity: usize,
    ) -> Arc<ActorRef>
    where
        TActor: ActorBase + Default + 'static,
    {
        self.spawn_on_dispatcher_with_options::<TActor>(dispatcher_index, None, mailbox_capacity)
    }

    pub fn spawn_on_dispatcher_with_options<TActor>(
        self: &Arc<Self>,
        dispatcher_index: usize,
        alias: Option<String>,
        mailbox_capacity: usize,
    ) -> Arc<ActorRef>
    where
        TActor: ActorBase + Default + 'static,
    {
        self.spawn_with_dispatcher(TActor::default(), dispatcher_index, alias, mailbox_capacity)
    }

    pub fn spawn_on_dispatcher_with_factory<TActor, TFactory>(
        self: &Arc<Self>,
        factory: TFactory,
        dispatcher_index: usize,
    ) -> Arc<ActorRef>
    where
        TActor: ActorBase + 'static,
        TFactory: FnOnce() -> TActor,
    {
        self.spawn_on_dispatcher_with_factory_options(
            factory,
            dispatcher_index,
            None,
            DEFAULT_MAILBOX_CAPACITY,
        )
    }

    pub fn spawn_on_dispatcher_with_factory_and_alias<TActor, TFactory>(
        self: &Arc<Self>,
        factory: TFactory,
        dispatcher_index: usize,
        alias: String,
    ) -> Arc<ActorRef>
    where
        TActor: ActorBase + 'static,
        TFactory: FnOnce() -> TActor,
    {
        self.spawn_on_dispatcher_with_factory_options(
            factory,
            dispatcher_index,
            Some(alias),
            DEFAULT_MAILBOX_CAPACITY,
        )
    }

    pub fn spawn_on_dispatcher_with_factory_and_capacity<TActor, TFactory>(
        self: &Arc<Self>,
        factory: TFactory,
        dispatcher_index: usize,
        mailbox_capacity: usize,
    ) -> Arc<ActorRef>
    where
        TActor: ActorBase + 'static,
        TFactory: FnOnce() -> TActor,
    {
        self.spawn_on_dispatcher_with_factory_options(
            factory,
            dispatcher_index,
            None,
            mailbox_capacity,
        )
    }

    pub fn spawn_on_dispatcher_with_factory_options<TActor, TFactory>(
        self: &Arc<Self>,
        factory: TFactory,
        dispatcher_index: usize,
        alias: Option<String>,
        mailbox_capacity: usize,
    ) -> Arc<ActorRef>
    where
        TActor: ActorBase + 'static,
        TFactory: FnOnce() -> TActor,
    {
        self.spawn_with_dispatcher(factory(), dispatcher_index, alias, mailbox_capacity)
    }

    pub fn dispose(&self) {
        if self.is_disposed.swap(true, Ordering::AcqRel) {
            return;
        }

        self.ask_system.stop();

        for actor_runner in self.registry.snapshot_live() {
            actor_runner.kill();
        }

        loop {
            if self.registry.live_count() == 0 {
                break;
            }

            thread::yield_now();
        }

        for actor_dispatcher in &self.dispatchers {
            actor_dispatcher.dispose();
        }
    }

    pub(crate) fn finalize_kill(&self, index: u32, generation: u32) {
        let Some(actor_runner) = self.registry.remove(index, generation) else {
            return;
        };

        let Some(actor_alias) = actor_runner.actor_alias() else {
            return;
        };

        let mut alias_to_handle = self.alias_to_handle.write().unwrap();
        alias_to_handle.remove(&actor_alias);
    }

    pub fn try_get_actor_ref_by_alias(self: &Arc<Self>, alias: &str) -> Option<Arc<ActorRef>> {
        let handle = {
            let alias_to_handle = self.alias_to_handle.read().unwrap();
            alias_to_handle.get(alias).copied()
        };

        let (index, generation) = handle?;

        let actor_ref = ActorRef::new(
            index,
            generation,
            Some(alias.to_string()),
            Arc::clone(&self.registry),
            Arc::clone(&self.ask_system),
        );

        Some(Arc::new(actor_ref))
    }

    pub(crate) fn spawn_with_dispatcher<TActor>(
        self: &Arc<Self>,
        actor: TActor,
        dispatcher_index: usize,
        alias: Option<String>,
        mailbox_capacity: usize,
    ) -> Arc<ActorRef>
    where
        TActor: ActorBase + 'static,
    {
        self.throw_if_disposed();

        if dispatcher_index >= self.dispatchers.len() {
            panic!(
                "dispatcher_index is out of range. dispatcher_index:{}",
                dispatcher_index
            );
        }

        self.register_actor(actor, Some(dispatcher_index), alias, mailbox_capacity)
    }

    pub(crate) fn spawn_with_auto_dispatcher<TActor>(
        self: &Arc<Self>,
        actor: TActor,
        alias: Option<String>,
        mailbox_capacity: usize,
    ) -> Arc<ActorRef>
    where
        TActor: ActorBase + 'static,
    {
        self.throw_if_disposed();

        self.register_actor(actor, None, alias, mailbox_capacity)
    }

    fn register_actor<TActor>(
        self: &Arc<Self>,
        actor: TActor,
        dispatcher_index: Option<usize>,
        alias: Option<String>,
        mailbox_capacity: usize,
    ) -> Arc<ActorRef>
    where
        TActor: ActorBase + 'static,
    {
        if let Some(actor_alias) = alias.as_ref() {
            let alias_to_handle = self.alias_to_handle.read().unwrap();

            if alias_to_handle.contains_key(actor_alias) {
                panic!("Duplicate actor alias.{}", actor_alias);
            }
        }

        let (index, generation) = self.registry.reserve();
        let dispatcher_index =
            dispatcher_index.unwrap_or((index as usize) % self.dispatchers.len());
        let actor_dispatcher = Arc::clone(&self.dispatchers[dispatcher_index]);

        let actor_ref = ActorRef::new(
            index,
            generation,
            alias.clone(),
            Arc::clone(&self.registry),
            Arc::clone(&self.ask_system),
        );
        let actor_ref_handle = Arc::new(actor_ref.clone());

        let weak_actor_system = Arc::downgrade(self);

        let dead_letter_publisher: Arc<dyn DeadLetterPublisherTrait + Send + Sync> = self.clone();

        let actor_runner = Arc::new(ActorRunner::new(
            Box::new(actor),
            actor_dispatcher,
            actor_ref,
            mailbox_capacity,
            dead_letter_publisher,
            Box::new(move |finalized_index, finalized_generation| {
                let Some(actor_system) = weak_actor_system.upgrade() else {
                    return;
                };

                actor_system.finalize_kill(finalized_index, finalized_generation);
            }),
        ));

        if let Some(actor_alias) = alias.as_ref() {
            let mut alias_to_handle = self.alias_to_handle.write().unwrap();

            if alias_to_handle.contains_key(actor_alias) {
                panic!("Duplicate actor alias.{}", actor_alias);
            }

            alias_to_handle.insert(actor_alias.clone(), (index, generation));
        }

        self.registry.commit(index, actor_runner);

        actor_ref_handle
    }

    fn throw_if_disposed(&self) {
        if self.is_disposed.load(Ordering::Acquire) {
            panic!("ActorSystem is disposed.");
        }
    }
}

impl DeadLetterPublisherTrait for ActorSystem {
    fn publish(&self, dead_letter_message: DeadLetterMessage) {
        let dead_letter_callback = {
            let current_dead_letter_callback = self.dead_letter_callback.lock().unwrap();
            current_dead_letter_callback.clone()
        };

        if let Some(dead_letter_callback) = dead_letter_callback {
            dead_letter_callback(&dead_letter_message);
        }
    }
}
