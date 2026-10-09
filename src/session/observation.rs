use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::cancel::CancelToken;
use crate::chord::delta::Op;
use crate::chord::state::{Attached, Handle, Listener};
use crate::types::{JsonObject, JsonValue};

use super::types::{DocumentWatch, WatchEnd, WatchHandle, WatchListener};
use crate::errors::{Error, Result};

const MAX_PENDING_WATCH_FRAMES: usize = 100;
const LOCK: &str = "session observer lock poisoned";

pub type Detach = Box<dyn FnOnce() + Send>;

type Job = Box<dyn FnOnce() + Send>;

pub fn retirement_operations() -> Vec<Op> {
    vec![Op::Replace(JsonValue::Null)]
}

#[derive(Default)]
pub struct Deliveries {
    queue: Mutex<Queue>,
}

#[derive(Default)]
struct Queue {
    jobs: VecDeque<Job>,
    draining: bool,
}

impl Deliveries {
    pub fn push(&self, job: impl FnOnce() + Send + 'static) {
        self.queue.lock().expect(LOCK).jobs.push_back(Box::new(job));
    }

    pub fn drain(&self) {
        {
            let mut queue = self.queue.lock().expect(LOCK);
            if queue.draining {
                return;
            }
            queue.draining = true;
        }
        loop {
            let job = {
                let mut queue = self.queue.lock().expect(LOCK);
                match queue.jobs.pop_front() {
                    Some(job) => job,
                    None => {
                        queue.draining = false;
                        return;
                    }
                }
            };
            job();
        }
    }
}

fn value_json(value: &Option<JsonObject>) -> JsonValue {
    match value {
        Some(object) => JsonValue::Object(object.clone()),
        None => JsonValue::Null,
    }
}

#[derive(Clone)]
pub struct DocumentState(Arc<Mutex<Attached>>);

impl DocumentState {
    pub fn value(&self) -> JsonValue {
        self.0.lock().expect(LOCK).value().clone()
    }

    pub fn subscribe(&self, cancel: &CancelToken, listener: Listener) -> Handle {
        self.0.lock().expect(LOCK).subscribe(cancel, listener)
    }

    pub fn unsubscribe(&self, handle: Handle) {
        self.0.lock().expect(LOCK).unsubscribe(handle);
    }
}

pub struct StateSource {
    attached: Arc<Mutex<Attached>>,
    deliveries: Arc<Deliveries>,
    cursor: u64,
    retired: bool,
    closed: bool,
    detach: Option<Detach>,
}

impl StateSource {
    pub fn new(
        value: Option<JsonObject>,
        attached: Attached,
        deliveries: Arc<Deliveries>,
    ) -> (DocumentState, Arc<Mutex<StateSource>>) {
        let retired = value.is_none();
        let attached = Arc::new(Mutex::new(attached));
        let source = Arc::new(Mutex::new(StateSource {
            attached: Arc::clone(&attached),
            deliveries,
            cursor: 0,
            retired,
            closed: false,
            detach: None,
        }));
        (DocumentState(attached), source)
    }

    pub fn set_detach(&mut self, detach: Detach) {
        self.detach = Some(detach);
    }

    fn advance(&mut self, value: Option<JsonObject>, ops: Vec<Op>) {
        if self.closed || self.retired {
            return;
        }
        self.cursor += 1;
        self.retired = value.is_none();
        let attached = Arc::clone(&self.attached);
        let cursor = self.cursor;
        let value = value_json(&value);
        self.deliveries.push(move || {
            attached
                .lock()
                .expect(LOCK)
                .receive(cursor, value, &ops, &CancelToken::new())
                .expect("state cursors are contiguous by construction");
        });
    }

    fn close_session(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        if let Some(detach) = self.detach.take() {
            detach();
        }
    }
}

struct WatchCore {
    value: Option<JsonObject>,
    pending: VecDeque<(Option<JsonObject>, Vec<Op>)>,
    started: bool,
    retired: bool,
    listener: Option<WatchListener>,
    end: Option<WatchEnd>,
    detach: Option<Detach>,
}

pub struct WatchShared {
    core: Mutex<WatchCore>,
    deliveries: Arc<Deliveries>,
}

impl WatchShared {
    pub fn new(value: Option<JsonObject>, deliveries: Arc<Deliveries>) -> Arc<Self> {
        Arc::new(Self {
            core: Mutex::new(WatchCore {
                value,
                pending: VecDeque::new(),
                started: false,
                retired: false,
                listener: None,
                end: None,
                detach: None,
            }),
            deliveries,
        })
    }

    pub fn set_detach(&self, detach: Detach) {
        self.core.lock().expect(LOCK).detach = Some(detach);
    }

    fn advance(self: &Arc<Self>, value: Option<JsonObject>, ops: Vec<Op>) {
        let mut core = self.core.lock().expect(LOCK);
        if core.end.is_some() || core.retired {
            return;
        }
        if value.is_none() {
            core.retired = true;
        }
        if core.pending.len() >= MAX_PENDING_WATCH_FRAMES {
            core.pending.clear();
            let replacement = value_json(&value);
            core.pending
                .push_back((value, vec![Op::Replace(replacement)]));
        } else {
            core.pending.push_back((value, ops));
        }
        let started = core.started;
        drop(core);
        if started {
            let shared = Arc::clone(self);
            self.deliveries.push(move || shared.drain());
        }
    }

    fn start(self: &Arc<Self>, listener: WatchListener) -> Result<()> {
        let mut core = self.core.lock().expect(LOCK);
        if core.started {
            return Err(Error::Invalid("Watch is already started".into()));
        }
        if core.end.is_some() {
            return Err(Error::Invalid("Watch is stopped".into()));
        }
        core.started = true;
        core.listener = Some(listener);
        let pending = !core.pending.is_empty();
        drop(core);
        if pending {
            let shared = Arc::clone(self);
            self.deliveries.push(move || shared.drain());
            self.deliveries.drain();
        }
        Ok(())
    }

    fn drain(&self) {
        loop {
            let (value, ops, mut listener) = {
                let mut core = self.core.lock().expect(LOCK);
                if core.end.is_some() {
                    return;
                }
                let Some(listener) = core.listener.take() else {
                    return;
                };
                let Some((value, ops)) = core.pending.pop_front() else {
                    core.listener = Some(listener);
                    return;
                };
                core.value = value.clone();
                (value, ops, listener)
            };
            let retired = value.is_none();
            if let Err(error) = listener(value.as_ref(), &ops, &CancelToken::new()) {
                self.terminate(WatchEnd::ListenerError(Arc::new(error)));
                return;
            }
            if retired {
                self.terminate(WatchEnd::Retired);
                return;
            }
            let mut core = self.core.lock().expect(LOCK);
            if core.end.is_some() {
                return;
            }
            core.listener = Some(listener);
        }
    }

    pub fn terminate(&self, end: WatchEnd) -> WatchEnd {
        let (end, detach) = {
            let mut core = self.core.lock().expect(LOCK);
            if let Some(existing) = &core.end {
                return existing.clone();
            }
            core.end = Some(end.clone());
            core.pending.clear();
            (end, core.detach.take())
        };
        if let Some(detach) = detach {
            detach();
        }
        end
    }

    pub fn observe_cancellation(self: &Arc<Self>, cancel: &CancelToken) {
        if self.core.lock().expect(LOCK).end.is_some() {
            return;
        }
        let shared = Arc::clone(self);
        cancel.on_cancel(move || {
            shared.terminate(WatchEnd::Cancelled);
        });
    }

    fn closed(&self) -> Option<WatchEnd> {
        self.core.lock().expect(LOCK).end.clone()
    }
}

pub struct Watch(Arc<WatchShared>);

impl Watch {
    pub fn boxed(shared: Arc<WatchShared>) -> DocumentWatch {
        Box::new(Watch(shared))
    }
}

impl WatchHandle for Watch {
    fn value(&self) -> Option<JsonObject> {
        self.0.core.lock().expect(LOCK).value.clone()
    }

    fn start(&mut self, listener: WatchListener) -> Result<()> {
        self.0.start(listener)
    }

    fn stop(&mut self) -> WatchEnd {
        self.0.terminate(WatchEnd::Stopped)
    }

    fn closed(&self) -> Option<WatchEnd> {
        self.0.closed()
    }
}

#[derive(Clone)]
pub enum Sink {
    State(Arc<Mutex<StateSource>>),
    Watch(Arc<WatchShared>),
}

impl Sink {
    pub fn advance(&self, value: Option<JsonObject>, ops: Vec<Op>) {
        match self {
            Sink::State(source) => source.lock().expect(LOCK).advance(value, ops),
            Sink::Watch(shared) => shared.advance(value, ops),
        }
    }

    pub fn close_session(&self) {
        match self {
            Sink::State(source) => source.lock().expect(LOCK).close_session(),
            Sink::Watch(shared) => {
                shared.terminate(WatchEnd::SessionClosed);
            }
        }
    }
}

pub fn observed_operations(
    observed: &mut u64,
    value: &Option<JsonObject>,
    version: Option<u64>,
    ops: &[Op],
) -> Vec<Op> {
    match (value, version) {
        (None, _) => retirement_operations(),
        (Some(_), Some(version)) if version == *observed => ops.to_vec(),
        (Some(object), Some(version)) => {
            *observed = version;
            vec![Op::Replace(JsonValue::Object(object.clone()))]
        }
        (Some(_), None) => ops.to_vec(),
    }
}
