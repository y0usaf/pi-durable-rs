use super::delta::{Op, Prepared, Tracker, apply, track};
use super::error::Error;
use super::json::JsonValue;
use crate::cancel::CancelToken;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Hydrate,
    Update,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Delivery {
    pub kind: Kind,
    pub sequence: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Handle(u64);

pub type Listener = Box<dyn FnMut(&JsonValue, &CancelToken, Delivery) -> Result<(), Error> + Send>;
pub type SourceListener = Box<dyn FnMut(&[Op], u64, &CancelToken) -> Result<(), Error> + Send>;
pub type ErrorSink = Box<dyn FnMut(Error) + Send>;

struct Subscriber {
    id: u64,
    hydrated: u64,
    listener: Listener,
}

struct Publisher {
    sequence: u64,
    next_id: u64,
    listeners: Vec<Subscriber>,
    sources: Vec<(u64, SourceListener)>,
    on_error: ErrorSink,
}

fn collect(mut errors: Vec<Error>) -> Result<(), Error> {
    match errors.len() {
        0 => Ok(()),
        1 => Err(errors.remove(0)),
        _ => Err(Error::Listeners(errors)),
    }
}

impl Publisher {
    fn new(sequence: u64, on_error: ErrorSink) -> Publisher {
        Publisher {
            sequence,
            next_id: 0,
            listeners: Vec::new(),
            sources: Vec::new(),
            on_error,
        }
    }

    fn add(
        &mut self,
        mut listener: Listener,
        current: Option<&JsonValue>,
        cancel: &CancelToken,
    ) -> Handle {
        let id = self.next_id;
        self.next_id += 1;
        if let Some(value) = current {
            let delivery = Delivery {
                kind: Kind::Hydrate,
                sequence: self.sequence,
            };
            if let Err(error) = listener(value, cancel, delivery) {
                (self.on_error)(error);
            }
        }
        self.listeners.push(Subscriber {
            id,
            hydrated: self.sequence,
            listener,
        });
        Handle(id)
    }

    fn remove(&mut self, handle: Handle) {
        self.listeners.retain(|sub| sub.id != handle.0);
    }

    fn add_source(&mut self, listener: SourceListener) -> Handle {
        let id = self.next_id;
        self.next_id += 1;
        self.sources.push((id, listener));
        Handle(id)
    }

    fn remove_source(&mut self, handle: Handle) {
        self.sources.retain(|(id, _)| *id != handle.0);
    }

    fn hydrate(&mut self, value: &JsonValue, sequence: u64, cancel: &CancelToken) {
        self.sequence = sequence;
        let delivery = Delivery {
            kind: Kind::Hydrate,
            sequence,
        };
        for sub in &mut self.listeners {
            if let Err(error) = (sub.listener)(value, cancel, delivery) {
                (self.on_error)(error);
            }
        }
    }

    fn publish(
        &mut self,
        value: &JsonValue,
        ops: &[Op],
        sequence: u64,
        cancel: &CancelToken,
    ) -> Result<(), Error> {
        self.sequence = sequence;
        let mut errors = Vec::new();
        for (_, source) in &mut self.sources {
            if let Err(error) = source(ops, sequence, cancel) {
                errors.push(error);
            }
        }
        let delivery = Delivery {
            kind: Kind::Update,
            sequence,
        };
        for sub in &mut self.listeners {
            if sequence <= sub.hydrated {
                continue;
            }
            if let Err(error) = (sub.listener)(value, cancel, delivery) {
                (self.on_error)(error);
            }
        }
        collect(errors)
    }
}

pub struct Mutable {
    tracker: Tracker,
    publisher: Publisher,
}

impl Mutable {
    pub fn new(initial: JsonValue, on_error: ErrorSink) -> Mutable {
        Mutable {
            tracker: track(initial),
            publisher: Publisher::new(0, on_error),
        }
    }

    pub fn value(&self) -> &JsonValue {
        self.tracker.value()
    }

    pub fn change(
        &mut self,
        cancel: &CancelToken,
        mutate: impl FnOnce(&mut JsonValue) -> Result<(), Error>,
    ) -> Result<(), Error> {
        let mut change = self.tracker.begin_change();
        mutate(change.state())?;
        let prepared = change.prepare(&self.tracker)?;
        self.commit(prepared, cancel)
    }

    pub fn replace(&mut self, cancel: &CancelToken, value: JsonValue) -> Result<(), Error> {
        let prepared = self.tracker.prepare_replace(value)?;
        self.commit(prepared, cancel)
    }

    fn commit(&mut self, prepared: Prepared, cancel: &CancelToken) -> Result<(), Error> {
        let ops = prepared.ops().to_vec();
        self.tracker.adopt(prepared)?;
        if ops.is_empty() {
            return Ok(());
        }
        let sequence = self.publisher.sequence + 1;
        self.publisher
            .publish(self.tracker.value(), &ops, sequence, cancel)
    }

    pub fn subscribe(&mut self, cancel: &CancelToken, listener: Listener) -> Handle {
        self.publisher
            .add(listener, Some(self.tracker.value()), cancel)
    }

    pub fn unsubscribe(&mut self, handle: Handle) {
        self.publisher.remove(handle);
    }

    pub fn subscribe_source(&mut self, listener: SourceListener) -> Handle {
        self.publisher.add_source(listener)
    }

    pub fn unsubscribe_source(&mut self, handle: Handle) {
        self.publisher.remove_source(handle);
    }
}

pub struct Replica {
    value: Option<JsonValue>,
    publisher: Publisher,
}

impl Replica {
    pub fn new(on_error: ErrorSink) -> Replica {
        Replica {
            value: None,
            publisher: Publisher::new(0, on_error),
        }
    }

    pub fn value(&self) -> Option<&JsonValue> {
        self.value.as_ref()
    }

    pub fn subscribe(&mut self, cancel: &CancelToken, listener: Listener) -> Handle {
        self.publisher.add(listener, self.value.as_ref(), cancel)
    }

    pub fn unsubscribe(&mut self, handle: Handle) {
        self.publisher.remove(handle);
    }

    pub fn hydrate(
        &mut self,
        sequence: u64,
        ops: &[Op],
        cancel: &CancelToken,
    ) -> Result<(), Error> {
        if !matches!(ops.first(), Some(Op::Replace(_))) {
            self.clear();
            return Err(Error::State(
                "Replicated state snapshot is not a base operation batch".to_string(),
            ));
        }
        let next = apply(None, ops).inspect_err(|_| self.clear())?;
        let value = self.value.insert(next);
        self.publisher.hydrate(value, sequence, cancel);
        Ok(())
    }

    pub fn update(&mut self, sequence: u64, ops: &[Op], cancel: &CancelToken) -> Result<(), Error> {
        let Some(current) = self.value.take() else {
            return Err(Error::State(
                "Replicated state received an update before hydration".to_string(),
            ));
        };
        if sequence != self.publisher.sequence + 1 {
            self.clear();
            return Err(Error::State(
                "Replicated state update sequence has a gap".to_string(),
            ));
        }
        let next = apply(Some(current), ops).inspect_err(|_| self.clear())?;
        let value = self.value.insert(next);
        self.publisher.publish(value, ops, sequence, cancel)
    }

    pub fn clear(&mut self) {
        self.value = None;
    }
}

pub struct Attached {
    cursor: u64,
    value: JsonValue,
    publisher: Publisher,
}

impl Attached {
    pub fn new(cursor: u64, value: JsonValue, on_error: ErrorSink) -> Attached {
        Attached {
            cursor,
            value,
            publisher: Publisher::new(cursor, on_error),
        }
    }

    pub fn value(&self) -> &JsonValue {
        &self.value
    }

    pub fn subscribe(&mut self, cancel: &CancelToken, listener: Listener) -> Handle {
        self.publisher.add(listener, Some(&self.value), cancel)
    }

    pub fn unsubscribe(&mut self, handle: Handle) {
        self.publisher.remove(handle);
    }

    pub fn receive(
        &mut self,
        cursor: u64,
        value: JsonValue,
        ops: &[Op],
        cancel: &CancelToken,
    ) -> Result<(), Error> {
        let expected = self.cursor + 1;
        if cursor != expected {
            return Err(Error::Gap {
                expected,
                received: cursor,
            });
        }
        self.cursor = cursor;
        self.value = value;
        self.publisher.publish(&self.value, ops, cursor, cancel)
    }
}
