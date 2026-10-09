use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::de::DeserializeOwned;

use crate::cancel::CancelToken;
use crate::chord::delta::track;
use crate::chord::error::Error as ChordError;
use crate::chord::state::Attached;
use crate::documents::{
    DocDefinition, DocToken, check_record_scope, check_record_version, materialize_document,
    resolve_address,
};
use crate::errors::{Error, Result};
use crate::ids::{ConversationId, DocumentId, EntryId, Seq};
use crate::session::observation::{
    Deliveries, DocumentState, Observer, Target, Watch, WatchShared, state_target,
};
use crate::session::transaction::{LoadedDocument, Transaction, TransactionHost, TransactionScope};
use crate::session::types::{
    CommitListener, DocRef, DocumentObserver, DocumentReader, DocumentWatch, Tx, Unsubscribe,
    WatchEnd,
};
use crate::types::{
    CommitChange, CommitPublication, ConversationRecord, DocumentAddress, DocumentCommitChange,
    DocumentPoint, DocumentRecord, JsonObject, JsonValue, ScopeRef, Storage, StorageWrite,
};

const LOCK: &str = "session lock poisoned";

type CloseListener = Box<dyn FnOnce() + Send>;
const CLOSED: &str = "Session is closed";
const POISONED: &str = "Session is poisoned by a failed commit after storage admission; reopen it";

#[derive(Default)]
pub struct SessionOptions {
    pub now: Option<Box<dyn Fn() -> u64 + Send + Sync>>,
}

pub fn create_session(storage: impl Storage + 'static, options: SessionOptions) -> Session {
    let now: Box<dyn Fn() -> u64 + Send + Sync> =
        options.now.unwrap_or_else(|| Box::new(system_now));
    Session {
        storage: Box::new(storage),
        now,
        line: Mutex::new(Line::default()),
        documents: Mutex::new(HashMap::new()),
        observers: Arc::new(Mutex::new(BTreeMap::new())),
        commit_listeners: Arc::new(Mutex::new(BTreeMap::new())),
        close_listeners: Arc::new(Mutex::new(BTreeMap::new())),
        deliveries: Arc::new(Deliveries::default()),
        ids: AtomicU64::new(0),
    }
}

fn system_now() -> u64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before 1970");
    u64::try_from(elapsed.as_millis()).expect("system clock is out of range")
}

fn error_sink() -> Box<dyn FnMut(ChordError) + Send> {
    Box::new(|error| eprintln!("{error}"))
}

fn aborted_commit() -> Error {
    Error::Invalid("This operation was aborted".into())
}

fn aborted_watch() -> Error {
    Error::Invalid("The operation was aborted".into())
}

#[derive(Default)]
struct Line {
    poisoned: bool,
    closed: bool,
    close_error: Option<String>,
}

impl Line {
    fn check_healthy(&self) -> Result<()> {
        if self.poisoned {
            return Err(Error::Invalid(POISONED.into()));
        }
        Ok(())
    }

    fn check_usable(&self) -> Result<()> {
        if self.closed {
            return Err(Error::Invalid(CLOSED.into()));
        }
        self.check_healthy()
    }
}

pub struct ConversationDocument {
    pub record: DocumentRecord,
    pub version: u64,
    pub value: JsonObject,
}

pub struct SessionLine<'a> {
    session: &'a Session,
}

impl SessionLine<'_> {
    pub fn conversation_document(
        &self,
        definition: &DocDefinition,
        conversation_id: ConversationId,
        cancel: &CancelToken,
    ) -> Result<Option<ConversationDocument>> {
        let resolved = resolve_address(definition, Some(conversation_id.0), None)?;
        let Some(loaded) =
            self.session
                .load_document(definition, &resolved.id, &resolved.address, cancel)?
        else {
            return Ok(None);
        };
        check_record_scope(definition, &loaded.record.create)?;
        check_record_version(definition, &loaded.record.create, loaded.stored_version)?;
        Ok(Some(ConversationDocument {
            value: object_of(&loaded)?,
            version: loaded.value_version,
            record: loaded.record,
        }))
    }
}

pub struct Session {
    storage: Box<dyn Storage>,
    now: Box<dyn Fn() -> u64 + Send + Sync>,
    line: Mutex<Line>,
    documents: Mutex<HashMap<String, LoadedDocument>>,
    observers: Arc<Mutex<BTreeMap<u64, Observer>>>,
    commit_listeners: Arc<Mutex<BTreeMap<u64, CommitListener>>>,
    close_listeners: Arc<Mutex<BTreeMap<u64, CloseListener>>>,
    deliveries: Arc<Deliveries>,
    ids: AtomicU64,
}

fn object_of(loaded: &LoadedDocument) -> Result<JsonObject> {
    let tracker = loaded.tracker.lock().expect(LOCK);
    match tracker.value() {
        JsonValue::Object(object) => Ok(object.clone()),
        _ => Err(Error::Invalid(format!(
            "Document {} is not an object",
            loaded.record.create.id
        ))),
    }
}

pub fn doc_ref<'a, T>(
    token: &'a DocToken<T>,
    owner: Option<u64>,
    key: Option<&'a str>,
) -> DocRef<'a> {
    DocRef {
        definition: token.definition(),
        owner,
        key,
    }
}

impl Session {
    fn lock_line(&self) -> MutexGuard<'_, Line> {
        self.line.lock().expect(LOCK)
    }

    fn next_id(&self) -> u64 {
        self.ids.fetch_add(1, Ordering::SeqCst)
    }

    fn assert_usable(&self) -> Result<()> {
        self.lock_line().check_usable()
    }

    fn enqueue<R>(&self, job: impl FnOnce(&mut Line) -> Result<R>) -> Result<R> {
        let outcome = job(&mut self.lock_line());
        self.deliveries.drain();
        outcome
    }

    pub fn commit<R>(
        &self,
        change: impl FnOnce(&mut dyn Tx) -> Result<R>,
        cancel: &CancelToken,
    ) -> Result<R> {
        self.commit_with(|tx| change(tx), cancel, TransactionScope::default())
    }

    pub fn commit_with<R>(
        &self,
        change: impl FnOnce(&mut Transaction<'_>) -> Result<R>,
        cancel: &CancelToken,
        scope: TransactionScope,
    ) -> Result<R> {
        self.assert_usable()?;
        self.enqueue(|line| self.run_commit(line, change, cancel, scope))
    }

    fn run_commit<R>(
        &self,
        line: &mut Line,
        change: impl FnOnce(&mut Transaction<'_>) -> Result<R>,
        cancel: &CancelToken,
        scope: TransactionScope,
    ) -> Result<R> {
        line.check_usable()?;
        if cancel.is_cancelled() {
            return Err(aborted_commit());
        }
        let mut tx = Transaction::new(self, cancel, scope);
        let result = match change(&mut tx) {
            Ok(result) => result,
            Err(error) => {
                tx.settle_failure();
                return Err(error);
            }
        };
        let writes = tx.settle_success()?;
        if writes.is_empty() {
            tx.discard();
            return Ok(result);
        }
        let seq = match self.storage.commit(&writes, &CancelToken::new()) {
            Ok(seq) => seq,
            Err(error) => {
                tx.discard();
                if !matches!(error, Error::StorageRejected { .. }) {
                    line.poisoned = true;
                }
                return Err(error);
            }
        };
        let documents = match tx.adopt(seq) {
            Ok(documents) => documents,
            Err(error) => {
                line.poisoned = true;
                return Err(error);
            }
        };
        self.publish(seq, &writes, &documents);
        Ok(result)
    }

    pub fn read_on_line<R>(&self, job: impl FnOnce(&SessionLine<'_>) -> Result<R>) -> Result<R> {
        self.assert_usable()?;
        self.enqueue(|line| {
            line.check_usable()?;
            job(&SessionLine { session: self })
        })
    }

    fn publish(&self, seq: Seq, writes: &[StorageWrite], documents: &[DocumentCommitChange]) {
        let listeners = self.commit_listeners.lock().expect(LOCK);
        if !listeners.is_empty() {
            let mut changes: Vec<CommitChange> = writes
                .iter()
                .filter_map(|write| match write {
                    StorageWrite::Table(change) => Some(CommitChange::Table((**change).clone())),
                    _ => None,
                })
                .collect();
            changes.extend(
                documents
                    .iter()
                    .map(|document| CommitChange::Document(document.clone())),
            );
            let publication = CommitPublication { seq, changes };
            for listener in listeners.values() {
                listener(&publication, &CancelToken::new());
            }
        }
        drop(listeners);
        let mut observers = self.observers.lock().expect(LOCK);
        for change in documents {
            if let DocumentCommitChange::Document {
                record,
                version,
                value,
                ops,
                ..
            } = change
            {
                for observer in observers.values_mut() {
                    if observer.record == record.create.id {
                        observer.observe(value, *version, ops, &self.deliveries);
                    }
                }
            }
        }
    }

    fn load_document(
        &self,
        definition: &DocDefinition,
        address_id: &str,
        address: &DocumentAddress,
        cancel: &CancelToken,
    ) -> Result<Option<LoadedDocument>> {
        let cached = self.documents.lock().expect(LOCK).get(address_id).cloned();
        match cached {
            Some(cached) if cached.value_version == definition.version => return Ok(Some(cached)),
            Some(_) => {
                self.documents.lock().expect(LOCK).remove(address_id);
            }
            None => {}
        }
        let Some(record) = self
            .storage
            .find_document(address, DocumentPoint::Current, cancel)?
        else {
            return Ok(None);
        };
        let Some(stored) =
            self.storage
                .document(record.create.id, DocumentPoint::Current, cancel)?
        else {
            return Err(Error::Invalid(format!(
                "Current document {} ({}) cannot be read",
                record.create.id, record.create.kind
            )));
        };
        let stored_record = stored.record.clone();
        let (stored_version, deltas_since_base) = (stored.version, stored.deltas_since_base);
        let value = materialize_document(definition, stored)?;
        let loaded = LoadedDocument {
            address_id: address_id.to_owned(),
            record: stored_record,
            stored_version,
            value_version: definition.version,
            deltas_since_base,
            tracker: Arc::new(Mutex::new(track(JsonValue::Object(value)))),
        };
        self.documents
            .lock()
            .expect(LOCK)
            .insert(address_id.to_owned(), loaded.clone());
        Ok(Some(loaded))
    }

    fn attach_state(
        &self,
        definition: &DocDefinition,
        loaded: &LoadedDocument,
    ) -> Result<DocumentState> {
        check_record_scope(definition, &loaded.record.create)?;
        check_record_version(definition, &loaded.record.create, loaded.stored_version)?;
        let attached = Arc::new(Mutex::new(Attached::new(
            0,
            JsonValue::Object(object_of(loaded)?),
            error_sink(),
        )));
        let id = self.next_id();
        self.observers.lock().expect(LOCK).insert(
            id,
            Observer {
                record: loaded.record.create.id,
                version: loaded.value_version,
                target: state_target(Arc::clone(&attached)),
            },
        );
        Ok(DocumentState::new(attached))
    }

    fn attach_watch(
        &self,
        definition: &DocDefinition,
        loaded: &LoadedDocument,
    ) -> Result<Arc<WatchShared>> {
        check_record_scope(definition, &loaded.record.create)?;
        check_record_version(definition, &loaded.record.create, loaded.stored_version)?;
        let id = self.next_id();
        let observers = Arc::clone(&self.observers);
        let shared = WatchShared::new(
            Some(object_of(loaded)?),
            Arc::clone(&self.deliveries),
            Box::new(move || {
                observers.lock().expect(LOCK).remove(&id);
            }),
        );
        self.observers.lock().expect(LOCK).insert(
            id,
            Observer {
                record: loaded.record.create.id,
                version: loaded.value_version,
                target: Target::Watch(Arc::clone(&shared)),
            },
        );
        Ok(shared)
    }

    pub fn document_state(
        &self,
        doc: DocRef,
        cancel: &CancelToken,
    ) -> Result<Option<DocumentState>> {
        self.assert_usable()?;
        let resolved = resolve_address(doc.definition, doc.owner, doc.key)?;
        self.enqueue(|line| {
            line.check_healthy()?;
            let Some(loaded) =
                self.load_document(doc.definition, &resolved.id, &resolved.address, cancel)?
            else {
                return Ok(None);
            };
            self.attach_state(doc.definition, &loaded).map(Some)
        })
    }

    pub fn subscribe_commits(&self, listener: CommitListener) -> Result<Unsubscribe> {
        self.assert_usable()?;
        let id = self.next_id();
        self.commit_listeners
            .lock()
            .expect(LOCK)
            .insert(id, listener);
        let listeners = Arc::clone(&self.commit_listeners);
        Ok(Box::new(move || {
            listeners.lock().expect(LOCK).remove(&id);
        }))
    }

    pub fn subscribe_close(&self, listener: Box<dyn FnOnce() + Send>) -> Result<Unsubscribe> {
        self.assert_usable()?;
        let id = self.next_id();
        self.close_listeners
            .lock()
            .expect(LOCK)
            .insert(id, listener);
        let listeners = Arc::clone(&self.close_listeners);
        Ok(Box::new(move || {
            listeners.lock().expect(LOCK).remove(&id);
        }))
    }

    pub fn unload_documents(&self) -> Result<()> {
        self.enqueue(|_| {
            self.documents.lock().expect(LOCK).clear();
            Ok(())
        })
    }

    pub fn close(&self) -> Result<()> {
        let mut line = self.lock_line();
        if line.closed {
            return match &line.close_error {
                None => Ok(()),
                Some(message) => Err(Error::Invalid(message.clone())),
            };
        }
        line.closed = true;
        let close_listeners = std::mem::take(&mut *self.close_listeners.lock().expect(LOCK));
        for listener in close_listeners.into_values() {
            listener();
        }
        let observers = std::mem::take(&mut *self.observers.lock().expect(LOCK));
        for observer in observers.into_values() {
            observer.close();
        }
        self.commit_listeners.lock().expect(LOCK).clear();
        self.documents.lock().expect(LOCK).clear();
        match self.storage.close(&CancelToken::new()) {
            Ok(()) => Ok(()),
            Err(error) => {
                line.close_error = Some(error.to_string());
                Err(error)
            }
        }
    }

    pub fn snapshot_as<T: DeserializeOwned>(
        &self,
        token: &DocToken<T>,
        owner: Option<u64>,
        key: Option<&str>,
        cancel: &CancelToken,
    ) -> Result<Option<T>> {
        match self.snapshot(doc_ref(token, owner, key), cancel)? {
            Some(object) => Ok(Some(serde_json::from_value(JsonValue::Object(object))?)),
            None => Ok(None),
        }
    }
}

impl DocumentReader for Session {
    fn snapshot(&self, doc: DocRef, cancel: &CancelToken) -> Result<Option<JsonObject>> {
        self.assert_usable()?;
        let resolved = resolve_address(doc.definition, doc.owner, doc.key)?;
        let cached = self
            .documents
            .lock()
            .expect(LOCK)
            .get(&resolved.id)
            .filter(|loaded| loaded.value_version == doc.definition.version)
            .cloned();
        let loaded = match cached {
            Some(loaded) => Some(loaded),
            None => self.enqueue(|line| {
                line.check_healthy()?;
                self.load_document(doc.definition, &resolved.id, &resolved.address, cancel)
            })?,
        };
        let Some(loaded) = loaded else {
            return Ok(None);
        };
        check_record_scope(doc.definition, &loaded.record.create)?;
        check_record_version(doc.definition, &loaded.record.create, loaded.stored_version)?;
        object_of(&loaded).map(Some)
    }

    fn snapshot_as_of(
        &self,
        doc: DocRef,
        at: EntryId,
        cancel: &CancelToken,
    ) -> Result<Option<JsonObject>> {
        self.assert_usable()?;
        let resolved = resolve_address(doc.definition, doc.owner, doc.key)?;
        let conversation_id = match &resolved.address.scope {
            ScopeRef::Conversation { conversation_id } => *conversation_id,
            _ => {
                return Err(Error::Invalid(
                    "Session.snapshotAsOf() requires a conversation document".into(),
                ));
            }
        };
        self.enqueue(|line| {
            line.check_healthy()?;
            let stored = self
                .storage
                .visible_entry(conversation_id, at, cancel)?
                .ok_or_else(|| {
                    Error::Invalid(format!(
                        "Entry {at} is not visible from conversation {conversation_id}"
                    ))
                })?;
            let mut address = resolved.address.clone();
            address.scope = ScopeRef::Conversation {
                conversation_id: stored.entry.conversation_id,
            };
            let point = DocumentPoint::At(stored.commit_seq);
            let Some(record) = self.storage.find_document(&address, point, cancel)? else {
                return Ok(None);
            };
            let Some(historical) = self.storage.document(record.create.id, point, cancel)? else {
                return Err(Error::Invalid(format!(
                    "Historical document {} ({}) cannot be read",
                    record.create.id, record.create.kind
                )));
            };
            materialize_document(doc.definition, historical).map(Some)
        })
    }
}

impl DocumentObserver for Session {
    fn watch_doc(&self, doc: DocRef, cancel: &CancelToken) -> Result<Option<DocumentWatch>> {
        self.assert_usable()?;
        let resolved = resolve_address(doc.definition, doc.owner, doc.key)?;
        let shared = self.enqueue(|line| {
            line.check_healthy()?;
            if cancel.is_cancelled() {
                return Err(aborted_watch());
            }
            let Some(loaded) =
                self.load_document(doc.definition, &resolved.id, &resolved.address, cancel)?
            else {
                return Ok(None);
            };
            if cancel.is_cancelled() {
                return Err(aborted_watch());
            }
            self.attach_watch(doc.definition, &loaded).map(Some)
        })?;
        let Some(shared) = shared else {
            return Ok(None);
        };
        let on_cancel = Arc::clone(&shared);
        cancel.on_cancel(move || {
            on_cancel.terminate(WatchEnd::Cancelled);
        });
        if cancel.is_cancelled() {
            return Err(aborted_watch());
        }
        Ok(Some(Watch::boxed(shared)))
    }
}

impl TransactionHost for Session {
    fn storage(&self) -> &dyn Storage {
        self.storage.as_ref()
    }

    fn now(&self) -> u64 {
        (self.now)()
    }

    fn cached(&self, address_id: &str) -> Option<LoadedDocument> {
        self.documents.lock().expect(LOCK).get(address_id).cloned()
    }

    fn load(
        &self,
        definition: &DocDefinition,
        address_id: &str,
        address: &DocumentAddress,
        cancel: &CancelToken,
    ) -> Result<Option<LoadedDocument>> {
        self.load_document(definition, address_id, address, cancel)
    }

    fn install(&self, document: LoadedDocument) {
        self.documents
            .lock()
            .expect(LOCK)
            .insert(document.address_id.clone(), document);
    }

    fn evict(&self, address_id: &str, record_id: DocumentId) {
        let mut documents = self.documents.lock().expect(LOCK);
        if documents
            .get(address_id)
            .is_some_and(|loaded| loaded.record.create.id == record_id)
        {
            documents.remove(address_id);
        }
    }

    fn conversation_created(
        &self,
        _tx: &mut Transaction<'_>,
        _record: &ConversationRecord,
    ) -> Result<()> {
        Ok(())
    }
}
