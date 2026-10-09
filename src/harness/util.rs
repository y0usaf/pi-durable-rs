use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::{Arc, Condvar, Mutex};

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::cancel::CancelToken;
use crate::documents::{CheckpointInfo, CheckpointWhen, DocDefinition, DocSemantics, DocToken};
use crate::errors::{Error, Result};
use crate::harness::json::assign_value;
use crate::ids::ConversationId;
use crate::session::types::{DocRef, Tx};
use crate::types::{Cursor, JsonObject, JsonValue, Op, Page};

pub fn closed_error() -> Error {
    Error::Invalid("Harness is closed".to_owned())
}

pub fn scan_all<T>(mut scan: impl FnMut(Option<Cursor>) -> Result<Page<T>>) -> Result<Vec<T>> {
    let mut items = Vec::new();
    let mut cursor = None;
    loop {
        let page = scan(cursor)?;
        items.extend(page.items);
        cursor = page.next;
        if cursor.is_none() {
            return Ok(items);
        }
    }
}

struct Slot<T> {
    outcome: Mutex<Option<Result<T>>>,
    settled: Condvar,
}

impl<T> Slot<T> {
    fn settle(&self, outcome: Result<T>) -> bool {
        let Ok(mut slot) = self.outcome.lock() else {
            return false;
        };
        if slot.is_some() {
            return false;
        }
        *slot = Some(outcome);
        self.settled.notify_all();
        true
    }
}

type Waiting<T> = BTreeMap<u64, Arc<Slot<T>>>;

struct WaitersInner<K, T> {
    next_id: u64,
    sets: BTreeMap<K, Waiting<T>>,
}

pub struct Waiters<K, T>(Arc<Mutex<WaitersInner<K, T>>>);

pub struct Ticket<K, T> {
    key: K,
    id: u64,
    slot: Arc<Slot<T>>,
}

impl<K, T> Clone for Waiters<K, T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<K: Ord, T> Default for Waiters<K, T> {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(WaitersInner {
            next_id: 0,
            sets: BTreeMap::new(),
        })))
    }
}

impl<K: Ord + Clone + Debug + Send + 'static, T: Send + 'static> Waiters<K, T> {
    pub fn add(&self, key: K) -> Result<Ticket<K, T>> {
        let slot = Arc::new(Slot {
            outcome: Mutex::new(None),
            settled: Condvar::new(),
        });
        let mut inner = self.0.lock().map_err(|_| closed_error())?;
        let id = inner.next_id;
        inner.next_id += 1;
        inner
            .sets
            .entry(key.clone())
            .or_default()
            .insert(id, Arc::clone(&slot));
        Ok(Ticket { key, id, slot })
    }

    pub fn wait(&self, ticket: Ticket<K, T>, cancel: &CancelToken) -> Result<T> {
        let Ticket { key, id, slot } = ticket;
        let waiters = self.clone();
        let abort_key = key.clone();
        cancel.on_cancel(move || waiters.abort(&abort_key, id));
        let mut outcome = slot.outcome.lock().map_err(|_| closed_error())?;
        loop {
            if let Some(settled) = outcome.take() {
                return settled;
            }
            outcome = slot.settled.wait(outcome).map_err(|_| closed_error())?;
        }
    }

    fn abort(&self, key: &K, id: u64) {
        let Ok(mut inner) = self.0.lock() else {
            return;
        };
        let Some(waiting) = inner.sets.get_mut(key) else {
            return;
        };
        let Some(slot) = waiting.remove(&id) else {
            return;
        };
        if waiting.is_empty() {
            inner.sets.remove(key);
        }
        drop(inner);
        slot.settle(Err(Error::Aborted));
    }

    pub fn keys(&self) -> Vec<K> {
        self.0
            .lock()
            .map(|inner| inner.sets.keys().cloned().collect())
            .unwrap_or_default()
    }

    pub fn resolve(&self, key: &K, value: &T)
    where
        T: Clone,
    {
        let waiting = self
            .0
            .lock()
            .ok()
            .and_then(|mut inner| inner.sets.remove(key));
        for slot in waiting
            .into_iter()
            .flat_map(|waiting| waiting.into_values())
        {
            slot.settle(Ok(value.clone()));
        }
    }

    pub fn reject_all(&self, error: impl Fn() -> Error) {
        let sets = match self.0.lock() {
            Ok(mut inner) => std::mem::take(&mut inner.sets),
            Err(_) => return,
        };
        for slot in sets.into_values().flat_map(|waiting| waiting.into_values()) {
            slot.settle(Err(error()));
        }
    }
}

pub fn doc_definition<T: Serialize + DeserializeOwned + 'static>(
    kind: &str,
    version: u64,
    semantics: DocSemantics,
    initial: fn() -> T,
    checkpoint_when: Option<fn(&T) -> bool>,
) -> Result<DocToken<T>> {
    let initial = move |_: Option<&JsonValue>| to_object(&initial());
    let checkpoint = checkpoint_when.map(|predicate| {
        Arc::new(move |value: &JsonObject, _: &[Op], _: CheckpointInfo| {
            serde_json::from_value::<T>(JsonValue::Object(value.clone()))
                .is_ok_and(|typed| predicate(&typed))
        }) as Arc<CheckpointWhen>
    });
    DocToken::new(DocDefinition {
        kind: kind.to_owned(),
        version,
        semantics,
        family: false,
        initial: Arc::new(initial),
        migrate: None,
        checkpoint_when: checkpoint,
    })
}

fn to_object<T: Serialize>(value: &T) -> JsonObject {
    match serde_json::to_value(value) {
        Ok(JsonValue::Object(map)) => map,
        _ => JsonObject::new(),
    }
}

pub fn conversation_doc(definition: &DocDefinition, owner: ConversationId) -> DocRef<'_> {
    DocRef {
        definition,
        owner: Some(owner.0),
        key: None,
    }
}

pub fn read_draft<T: DeserializeOwned>(
    tx: &mut dyn Tx,
    token: &DocToken<T>,
    owner: ConversationId,
) -> Result<T> {
    let draft = tx.doc(conversation_doc(token.definition(), owner), None)?;
    Ok(serde_json::from_value(tx.draft(draft)?.clone())?)
}

pub fn write_draft<T: Serialize + DeserializeOwned>(
    tx: &mut dyn Tx,
    token: &DocToken<T>,
    owner: ConversationId,
    value: &T,
) -> Result<()> {
    let draft = tx.doc(conversation_doc(token.definition(), owner), None)?;
    assign_value(tx.draft(draft)?, serde_json::to_value(value)?);
    Ok(())
}

pub fn edit_doc<T: Serialize + DeserializeOwned, R>(
    tx: &mut dyn Tx,
    token: &DocToken<T>,
    owner: ConversationId,
    edit: impl FnOnce(&mut T) -> R,
) -> Result<R> {
    let draft = tx.doc(conversation_doc(token.definition(), owner), None)?;
    let slot = tx.draft(draft)?;
    let mut typed: T = serde_json::from_value(slot.clone())?;
    let outcome = edit(&mut typed);
    assign_value(slot, serde_json::to_value(&typed)?);
    Ok(outcome)
}
