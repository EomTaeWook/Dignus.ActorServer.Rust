use crate::internals::ask_awaiter_trait::AskAwaiterTrait;
use crate::messages::actor_message_trait::ActorMessageTrait;

use std::error::Error;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AskError {
    Timeout,
    ResponseTypeMismatch,
}

impl std::fmt::Display for AskError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AskError::Timeout => formatter.write_str("ask timed out"),
            AskError::ResponseTypeMismatch => formatter.write_str("ask response type mismatch"),
        }
    }
}

impl Error for AskError {}

struct AskAwaiterState {
    response: Option<Box<dyn ActorMessageTrait>>,
    completed: bool,
    timed_out: bool,
    waker: Option<Waker>,
}

struct AskAwaiterStateHolder {
    state: Mutex<AskAwaiterState>,
}

impl AskAwaiterStateHolder {
    fn new() -> Self {
        Self {
            state: Mutex::new(AskAwaiterState {
                response: None,
                completed: false,
                timed_out: false,
                waker: None,
            }),
        }
    }

    fn complete(&self, response: Option<Box<dyn ActorMessageTrait>>, timed_out: bool) {
        let waker = {
            let mut state = self.state.lock().unwrap();

            if state.completed {
                return;
            }

            state.response = response;
            state.timed_out = timed_out;
            state.completed = true;
            state.waker.take()
        };

        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl AskAwaiterTrait for AskAwaiterStateHolder {
    fn set_response(&self, message: Box<dyn ActorMessageTrait>) {
        self.complete(Some(message), false);
    }

    fn set_timeout(&self) {
        self.complete(None, true);
    }
}

pub struct AskAwaiter<TResponse>
where
    TResponse: ActorMessageTrait,
{
    state_holder: Arc<AskAwaiterStateHolder>,
    response_marker: PhantomData<fn() -> TResponse>,
}

impl<TResponse> AskAwaiter<TResponse>
where
    TResponse: ActorMessageTrait,
{
    pub(crate) fn new() -> Self {
        Self {
            state_holder: Arc::new(AskAwaiterStateHolder::new()),
            response_marker: PhantomData,
        }
    }

    pub(crate) fn ask_awaiter(&self) -> Arc<dyn AskAwaiterTrait> {
        (Arc::clone(&self.state_holder)) as _
    }
}

impl<TResponse> Future for AskAwaiter<TResponse>
where
    TResponse: ActorMessageTrait,
{
    type Output = Result<TResponse, AskError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let ask_awaiter = self.as_ref().get_ref();
        let mut state = ask_awaiter.state_holder.state.lock().unwrap();

        if !state.completed {
            let should_replace_waker = match state.waker.as_ref() {
                Some(current_waker) => !current_waker.will_wake(context.waker()),
                None => true,
            };

            if should_replace_waker {
                state.waker = Some(context.waker().clone());
            }

            return Poll::Pending;
        }

        if state.timed_out {
            return Poll::Ready(Err(AskError::Timeout));
        }

        let response = state
            .response
            .take()
            .expect("ask response is already consumed");

        drop(state);

        match response.into_any().downcast::<TResponse>() {
            Ok(response) => Poll::Ready(Ok(*response)),
            Err(_) => Poll::Ready(Err(AskError::ResponseTypeMismatch)),
        }
    }
}
