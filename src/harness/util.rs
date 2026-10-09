use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::cancel::CancelToken;
use crate::documents::{CheckpointInfo, CheckpointWhen, DocDefinition, DocSemantics, DocToken};
use crate::errors::{Error, Result};
use crate::harness::json::assign_value;
use crate::ids::ConversationId;
use crate::session::types::{DocRef, Tx};
use crate::types::{Cursor, JsonObject, JsonValue, Op, Page};

const WAIT_SLICE: Duration = Duration::from_millis(25);

pub fn closed_error() -> Error {
    Error::Invalid("Harness is closed".to_owned())
}

pub fn cancelled_error() -> Error {
    Error::Invalid("Wait cancelled".to_owned())
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

type Sets<K, T> = BTreeMap<K, BTreeMap<u64, Sender<Result<T>>>>;

struct WaitersInner<K, T> {
    next_id: u64,
    sets: Sets<K, T>,
}

pub struct Waiters<K, T>(Arc<Mutex<WaitersInner<K, T>>>);

pub struct Ticket<K, T> {
    key: K,
    id: u64,
    receiver: Receiver<Result<T>>,
}

impl<K, T> Clone for Waiters<K, T> {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl<K: Ord + Clone, T> Default for Waiters<K, T> {
    fn default() -> Self {
        Self(Arc::new(Mutex::new(WaitersInner {
            next_id: 0,
            sets: BTreeMap::new(),
        })))
    }
}

impl<K: Ord + Clone + Debug, T> Waiters<K, T> {
    pub fn add(&self, key: K) -> Result<Ticket<K, T>> {
        let (sender, receiver) = mpsc::channel();
        let mut inner = self.0.lock().map_err(|_| closed_error())?;
        let id = inner.next_id;
        inner.next_id += 1;
        inner
            .sets
            .entry(key.clone())
            .or_default()
            .insert(id, sender);
        Ok(Ticket { key, id, receiver })
    }

    pub fn wait(&self, ticket: Ticket<K, T>, cancel: &CancelToken) -> Result<T> {
        let Ticket { key, id, receiver } = ticket;
        loop {
            match receiver.recv_timeout(WAIT_SLICE) {
                Ok(outcome) => return outcome,
                Err(RecvTimeoutError::Disconnected) => return Err(closed_error()),
                Err(RecvTimeoutError::Timeout) if cancel.is_cancelled() => {
                    self.forget(&key, id);
                    return Err(cancelled_error());
                }
                Err(RecvTimeoutError::Timeout) => {}
            }
        }
    }

    fn forget(&self, key: &K, id: u64) {
        if let Ok(mut inner) = self.0.lock()
            && let Some(set) = inner.sets.get_mut(key)
        {
            set.remove(&id);
            if set.is_empty() {
                inner.sets.remove(key);
            }
        }
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
        let set = self
            .0
            .lock()
            .ok()
            .and_then(|mut inner| inner.sets.remove(key));
        for sender in set.into_iter().flat_map(|set| set.into_values()) {
            let _ = sender.send(Ok(value.clone()));
        }
    }

    pub fn reject_all(&self, error: impl Fn() -> Error) {
        let sets = match self.0.lock() {
            Ok(mut inner) => std::mem::take(&mut inner.sets),
            Err(_) => return,
        };
        for sender in sets.into_values().flat_map(|set| set.into_values()) {
            let _ = sender.send(Err(error()));
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
