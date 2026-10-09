use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use super::types::{AssistantMessage, AssistantMessageEvent};

struct State<T, R> {
    queue: VecDeque<T>,
    done: bool,
    result: Option<R>,
}

pub struct EventStream<T, R> {
    shared: Arc<(Mutex<State<T, R>>, Condvar)>,
    is_complete: fn(&T) -> bool,
    extract: fn(&T) -> R,
}

impl<T, R> Clone for EventStream<T, R> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
            is_complete: self.is_complete,
            extract: self.extract,
        }
    }
}

impl<T, R: Clone> EventStream<T, R> {
    pub fn new(is_complete: fn(&T) -> bool, extract: fn(&T) -> R) -> Self {
        Self {
            shared: Arc::new((
                Mutex::new(State {
                    queue: VecDeque::new(),
                    done: false,
                    result: None,
                }),
                Condvar::new(),
            )),
            is_complete,
            extract,
        }
    }

    fn lock(&self) -> MutexGuard<'_, State<T, R>> {
        self.shared
            .0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn push(&self, event: T) {
        let mut state = self.lock();
        if state.done {
            return;
        }
        if (self.is_complete)(&event) {
            state.done = true;
            state.result = Some((self.extract)(&event));
        }
        state.queue.push_back(event);
        self.shared.1.notify_all();
    }

    pub fn end(&self, result: Option<R>) {
        let mut state = self.lock();
        state.done = true;
        if state.result.is_none() {
            state.result = result;
        }
        self.shared.1.notify_all();
    }

    pub fn is_done(&self) -> bool {
        self.lock().done
    }

    pub fn next_event(&self) -> Option<T> {
        let mut state = self.lock();
        loop {
            if let Some(event) = state.queue.pop_front() {
                return Some(event);
            }
            if state.done {
                return None;
            }
            state = self
                .shared
                .1
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    pub fn result(&self) -> Option<R> {
        let mut state = self.lock();
        loop {
            if let Some(result) = &state.result {
                return Some(result.clone());
            }
            if state.done {
                return None;
            }
            state = self
                .shared
                .1
                .wait(state)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }
}

#[derive(Clone)]
pub struct AssistantMessageEventStream {
    events: EventStream<AssistantMessageEvent, AssistantMessage>,
    started_at_ms: u64,
    started: Instant,
}

impl Default for AssistantMessageEventStream {
    fn default() -> Self {
        Self::new()
    }
}

impl AssistantMessageEventStream {
    pub fn new() -> Self {
        let started_at_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|elapsed| elapsed.as_millis() as u64)
            .unwrap_or(0);
        Self {
            events: EventStream::new(
                |event| {
                    matches!(
                        event,
                        AssistantMessageEvent::Done { .. } | AssistantMessageEvent::Error { .. }
                    )
                },
                |event| match event {
                    AssistantMessageEvent::Done { message, .. } => message.clone(),
                    AssistantMessageEvent::Error { error, .. } => error.clone(),
                    _ => unreachable!("only terminal events complete the stream"),
                },
            ),
            started_at_ms,
            started: Instant::now(),
        }
    }

    fn time(&self, message: &mut AssistantMessage) {
        if self.events.is_done()
            || message.duration_ms.is_some()
            || message.timestamp < self.started_at_ms
        {
            return;
        }
        let elapsed_ms = self.started.elapsed().as_secs_f64() * 1000.0;
        message.duration_ms = Some((elapsed_ms + 0.5).floor() as u64);
    }

    pub fn push(&self, mut event: AssistantMessageEvent) {
        match &mut event {
            AssistantMessageEvent::Done { message, .. } => self.time(message),
            AssistantMessageEvent::Error { error, .. } => self.time(error),
            _ => {}
        }
        self.events.push(event);
    }

    pub fn end(&self, mut result: Option<AssistantMessage>) {
        if let Some(message) = result.as_mut() {
            self.time(message);
        }
        self.events.end(result);
    }

    pub fn is_done(&self) -> bool {
        self.events.is_done()
    }

    pub fn next_event(&self) -> Option<AssistantMessageEvent> {
        self.events.next_event()
    }

    pub fn result(&self) -> Option<AssistantMessage> {
        self.events.result()
    }
}
