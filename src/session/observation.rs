use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use crate::cancel::CancelToken;
use crate::chord::delta::Op;
use crate::chord::state::{Attached, Handle, Listener};
use crate::errors::{Error, Result};
use crate::ids::DocumentId;
use crate::types::{JsonObject, JsonValue};

use super::types::{DocumentWatch, WatchEnd, WatchHandle, WatchListener};

const MAX_PENDING_WATCH_FRAMES: usize = 100;
const LOCK: &str = "session observer lock poisoned";

type Job = Box<dyn FnOnce() + Send>;

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
    pub(super) fn push(&self, job: impl FnOnce() + Send + 'static) {
        self.queue.lock().expect(LOCK).jobs.push_back(Box::new(job));
    }

    pub(super) fn drain(&self) {
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
    pub(super) fn new(attached: Arc<Mutex<Attached>>) -> Self {
        Self(attached)
    }

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

struct Frame {
    value: Option<JsonObject>,
    ops: Vec<Op>,
}

struct WatchCore {
    value: Option<JsonObject>,
    pending: VecDeque<Frame>,
    started: bool,
    retired: bool,
    listener: Option<WatchListener>,
    end: Option<WatchEnd>,
    detach: Option<Box<dyn FnOnce() + Send>>,
}

pub struct WatchShared {
    core: Mutex<WatchCore>,
    deliveries: Arc<Deliveries>,
    never: CancelToken,
}

pub struct Watch(Arc<WatchShared>);

impl WatchShared {
    pub(super) fn new(
        value: Option<JsonObject>,
        deliveries: Arc<Deliveries>,
        detach: Box<dyn FnOnce() + Send>,
    ) -> Arc<Self> {
        Arc::new(Self {
            core: Mutex::new(WatchCore {
                value,
                pending: VecDeque::new(),
                started: false,
                retired: false,
                listener: None,
                end: None,
                detach: Some(detach),
            }),
            deliveries,
            never: CancelToken::new(),
        })
    }

    pub(super) fn advance(self: &Arc<Self>, value: Option<JsonObject>, ops: Vec<Op>) {
        let mut core = self.core.lock().expect(LOCK);
        if core.end.is_some() || core.retired {
            return;
        }
        if value.is_none() {
            core.retired = true;
        }
        let frame = if core.pending.len() >= MAX_PENDING_WATCH_FRAMES {
            core.pending.clear();
            let replacement = value_json(&value);
            Frame {
                value,
                ops: vec![Op::Replace(replacement)],
            }
        } else {
            Frame { value, ops }
        };
        core.pending.push_back(frame);
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
            let (frame, mut listener) = {
                let mut core = self.core.lock().expect(LOCK);
                if core.end.is_some() {
                    return;
                }
                let Some(listener) = core.listener.take() else {
                    return;
                };
                let Some(frame) = core.pending.pop_front() else {
                    core.listener = Some(listener);
                    return;
                };
                core.value = frame.value.clone();
                (frame, listener)
            };
            if let Err(error) = listener(frame.value.as_ref(), &frame.ops, &self.never) {
                self.terminate(WatchEnd::ListenerError(Arc::new(error)));
                return;
            }
            if frame.value.is_none() {
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

    pub(super) fn terminate(&self, end: WatchEnd) -> WatchEnd {
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

    fn closed(&self) -> Option<WatchEnd> {
        self.core.lock().expect(LOCK).end.clone()
    }
}

impl Watch {
    pub(super) fn boxed(shared: Arc<WatchShared>) -> DocumentWatch {
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

pub enum Target {
    State {
        attached: Arc<Mutex<Attached>>,
        cursor: u64,
        retired: bool,
    },
    Watch(Arc<WatchShared>),
}

pub struct Observer {
    pub record: DocumentId,
    pub version: u64,
    pub target: Target,
}

impl Observer {
    pub(super) fn observe(
        &mut self,
        value: &Option<JsonObject>,
        version: Option<u64>,
        ops: &[Op],
        deliveries: &Deliveries,
    ) {
        let ops = match value {
            None => vec![Op::Replace(JsonValue::Null)],
            Some(_) if version == Some(self.version) => ops.to_vec(),
            Some(object) => {
                self.version = version.expect("a committed value carries its version");
                vec![Op::Replace(JsonValue::Object(object.clone()))]
            }
        };
        if ops.is_empty() {
            return;
        }
        match &mut self.target {
            Target::State {
                attached,
                cursor,
                retired,
            } => {
                if *retired {
                    return;
                }
                *cursor += 1;
                if value.is_none() {
                    *retired = true;
                }
                let attached = Arc::clone(attached);
                let cursor = *cursor;
                let value = value_json(value);
                let never = CancelToken::new();
                deliveries.push(move || {
                    attached
                        .lock()
                        .expect(LOCK)
                        .receive(cursor, value, &ops, &never)
                        .expect("state cursors are contiguous by construction");
                });
            }
            Target::Watch(shared) => shared.advance(value.clone(), ops),
        }
    }

    pub(super) fn close(self) {
        if let Target::Watch(shared) = self.target {
            shared.terminate(WatchEnd::SessionClosed);
        }
    }
}

pub(super) fn state_target(attached: Arc<Mutex<Attached>>) -> Target {
    Target::State {
        attached,
        cursor: 0,
        retired: false,
    }
}
