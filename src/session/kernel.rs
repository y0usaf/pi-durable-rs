use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
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
    Deliveries, Detach, DocumentState, Sink, StateSource, Watch, WatchShared, observed_operations,
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
const CLOSED: &str = "Session is closed";
const POISONED: &str = "Session is poisoned by a failed commit after storage admission; reopen it";
const ABORTED_COMMIT: &str = "This operation was aborted";
const ABORTED_WAIT: &str = "The operation was aborted";

pub type Clock = Box<dyn Fn() -> u64 + Send + Sync>;

type CloseListener = Box<dyn FnOnce() + Send>;

pub fn create_session(storage: impl Storage + 'static, now: Option<Clock>) -> Session {
    Session {
        storage: Box::new(storage),
        now: now.unwrap_or_else(|| Box::new(system_now)),
        line: Mutex::new(Line::default()),
        closing: AtomicBool::new(false),
        documents: Mutex::new(HashMap::new()),
        listeners: Arc::new(Mutex::new(Listeners::default())),
        deliveries: Arc::new(Deliveries::default()),
    }
}

fn system_now() -> u64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock is before 1970");
    u64::try_from(elapsed.as_millis()).expect("system clock is out of range")
}

fn report_error() -> Box<dyn FnMut(ChordError) + Send> {
    Box::new(|error| eprintln!("{error}"))
}

#[derive(Default)]
struct Line {
    poisoned: bool,
    close_error: Option<String>,
}

fn healthy(line: &Line) -> Result<()> {
    if line.poisoned {
        return Err(Error::Invalid(POISONED.into()));
    }
    Ok(())
}

#[derive(Default)]
struct Listeners {
    next: u64,
    commits: BTreeMap<u64, CommitListener>,
    closes: BTreeMap<u64, CloseListener>,
}

pub struct ConversationDocument {
    pub record: DocumentRecord,
    pub version: u64,
    pub value: JsonObject,
}

pub struct Session {
    storage: Box<dyn Storage>,
    now: Clock,
    line: Mutex<Line>,
    closing: AtomicBool,
    documents: Mutex<HashMap<String, LoadedDocument>>,
    listeners: Arc<Mutex<Listeners>>,
    deliveries: Arc<Deliveries>,
}

fn tracked_object(loaded: &LoadedDocument) -> Result<JsonObject> {
    match loaded.tracker.lock().expect(LOCK).value() {
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

    fn assert_usable(&self) -> Result<()> {
        if self.closing.load(Ordering::SeqCst) {
            return Err(Error::Invalid(CLOSED.into()));
        }
        healthy(&self.lock_line())
    }

    fn enqueue<R>(&self, job: impl FnOnce(&mut Line) -> Result<R>) -> Result<R> {
        let outcome = {
            let mut line = self.lock_line();
            job(&mut line)
        };
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
        healthy(line)?;
        if cancel.is_cancelled() {
            return Err(Error::Invalid(ABORTED_COMMIT.into()));
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
        self.publish(seq, &writes, &documents, cancel);
        Ok(result)
    }

    pub fn read_on_line<R>(&self, job: impl FnOnce() -> Result<R>) -> Result<R> {
        self.assert_usable()?;
        self.enqueue(|line| {
            healthy(line)?;
            job()
        })
    }

    pub fn conversation_document_on_line(
        &self,
        definition: &DocDefinition,
        conversation_id: ConversationId,
        cancel: &CancelToken,
    ) -> Result<Option<ConversationDocument>> {
        let resolved = resolve_address(definition, Some(conversation_id.0), None)?;
        let Some(loaded) =
            self.load_document(definition, &resolved.id, &resolved.address, cancel)?
        else {
            return Ok(None);
        };
        check_record_scope(definition, &loaded.record.create)?;
        check_record_version(definition, &loaded.record.create, loaded.stored_version)?;
        Ok(Some(ConversationDocument {
            value: tracked_object(&loaded)?,
            version: loaded.value_version,
            record: loaded.record,
        }))
    }

    fn publish(
        &self,
        seq: Seq,
        writes: &[StorageWrite],
        documents: &[DocumentCommitChange],
        cancel: &CancelToken,
    ) {
        let listeners = self.listeners.lock().expect(LOCK);
        if listeners.commits.is_empty() {
            return;
        }
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
        for listener in listeners.commits.values() {
            listener(&publication, cancel);
        }
    }

    fn register(&self, sink: Sink, record: DocumentId, observed: u64) -> Detach {
        let id = {
            let mut listeners = self.listeners.lock().expect(LOCK);
            listeners.next += 1;
            listeners.next
        };
        let observed = Mutex::new(observed);
        let commit_sink = sink.clone();
        let commit: CommitListener = Box::new(move |publication, _cancel| {
            for change in &publication.changes {
                let CommitChange::Document(DocumentCommitChange::Document {
                    record: changed,
                    value,
                    version,
                    ops,
                    ..
                }) = change
                else {
                    continue;
                };
                if changed.create.id != record {
                    continue;
                }
                let ops =
                    observed_operations(&mut observed.lock().expect(LOCK), value, *version, ops);
                if ops.is_empty() {
                    continue;
                }
                commit_sink.advance(value.clone(), ops);
            }
        });
        let close: CloseListener = Box::new(move || sink.close_session());
        {
            let mut listeners = self.listeners.lock().expect(LOCK);
            listeners.commits.insert(id, commit);
            listeners.closes.insert(id, close);
        }
        let listeners = Arc::clone(&self.listeners);
        Box::new(move || {
            let mut listeners = listeners.lock().expect(LOCK);
            listeners.commits.remove(&id);
            listeners.closes.remove(&id);
        })
    }

    fn attach_state(
        &self,
        definition: &DocDefinition,
        loaded: &LoadedDocument,
    ) -> Result<DocumentState> {
        check_record_scope(definition, &loaded.record.create)?;
        check_record_version(definition, &loaded.record.create, loaded.stored_version)?;
        let object = tracked_object(loaded)?;
        let attached = Attached::new(0, JsonValue::Object(object.clone()), report_error());
        let (state, source) =
            StateSource::new(Some(object), attached, Arc::clone(&self.deliveries));
        let detach = self.register(
            Sink::State(Arc::clone(&source)),
            loaded.record.create.id,
            loaded.value_version,
        );
        source.lock().expect(LOCK).set_detach(detach);
        Ok(state)
    }

    fn attach_watch(
        &self,
        definition: &DocDefinition,
        loaded: &LoadedDocument,
    ) -> Result<Arc<WatchShared>> {
        check_record_scope(definition, &loaded.record.create)?;
        check_record_version(definition, &loaded.record.create, loaded.stored_version)?;
        let shared = WatchShared::new(Some(tracked_object(loaded)?), Arc::clone(&self.deliveries));
        let detach = self.register(
            Sink::Watch(Arc::clone(&shared)),
            loaded.record.create.id,
            loaded.value_version,
        );
        shared.set_detach(detach);
        Ok(shared)
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
        let (stored_version, deltas_since_base) = (stored.version, stored.deltas_since_base);
        let record = stored.record.clone();
        let value = materialize_document(definition, stored)?;
        let loaded = LoadedDocument {
            address_id: address_id.to_owned(),
            record,
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

    pub fn unload_documents(&self) -> Result<()> {
        self.enqueue(|_| {
            self.documents.lock().expect(LOCK).clear();
            Ok(())
        })
    }

    pub fn subscribe_commits(&self, listener: CommitListener) -> Result<Unsubscribe> {
        self.assert_usable()?;
        let id = {
            let mut listeners = self.listeners.lock().expect(LOCK);
            listeners.next += 1;
            let id = listeners.next;
            listeners.commits.insert(id, listener);
            id
        };
        let listeners = Arc::clone(&self.listeners);
        Ok(Box::new(move || {
            listeners.lock().expect(LOCK).commits.remove(&id);
        }))
    }

    pub fn subscribe_close(&self, listener: CloseListener) -> Result<Unsubscribe> {
        self.assert_usable()?;
        let id = {
            let mut listeners = self.listeners.lock().expect(LOCK);
            listeners.next += 1;
            let id = listeners.next;
            listeners.closes.insert(id, listener);
            id
        };
        let listeners = Arc::clone(&self.listeners);
        Ok(Box::new(move || {
            listeners.lock().expect(LOCK).closes.remove(&id);
        }))
    }

    pub fn close(&self, cancel: &CancelToken) -> Result<()> {
        if self.closing.swap(true, Ordering::SeqCst) {
            let line = self.lock_line();
            if cancel.is_cancelled() {
                return Err(Error::Invalid(ABORTED_WAIT.into()));
            }
            return match &line.close_error {
                None => Ok(()),
                Some(message) => Err(Error::Invalid(message.clone())),
            };
        }
        let closes = std::mem::take(&mut self.listeners.lock().expect(LOCK).closes);
        for listener in closes.into_values() {
            listener();
        }
        let mut line = self.lock_line();
        self.listeners.lock().expect(LOCK).commits.clear();
        self.documents.lock().expect(LOCK).clear();
        let outcome = self.storage.close(&CancelToken::new());
        line.close_error = outcome.as_ref().err().map(ToString::to_string);
        if cancel.is_cancelled() {
            return Err(Error::Invalid(ABORTED_WAIT.into()));
        }
        outcome
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

    pub fn document_state(
        &self,
        doc: DocRef,
        cancel: &CancelToken,
    ) -> Result<Option<DocumentState>> {
        self.assert_usable()?;
        let resolved = resolve_address(doc.definition, doc.owner, doc.key)?;
        self.enqueue(|line| {
            healthy(line)?;
            let Some(loaded) =
                self.load_document(doc.definition, &resolved.id, &resolved.address, cancel)?
            else {
                return Ok(None);
            };
            self.attach_state(doc.definition, &loaded).map(Some)
        })
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
                healthy(line)?;
                self.load_document(doc.definition, &resolved.id, &resolved.address, cancel)
            })?,
        };
        let Some(loaded) = loaded else {
            return Ok(None);
        };
        check_record_scope(doc.definition, &loaded.record.create)?;
        check_record_version(doc.definition, &loaded.record.create, loaded.stored_version)?;
        tracked_object(&loaded).map(Some)
    }

    fn snapshot_as_of(
        &self,
        doc: DocRef,
        at: EntryId,
        cancel: &CancelToken,
    ) -> Result<Option<JsonObject>> {
        self.assert_usable()?;
        let resolved = resolve_address(doc.definition, doc.owner, doc.key)?;
        let ScopeRef::Conversation { conversation_id } = resolved.address.scope else {
            return Err(Error::Invalid(
                "Session.snapshotAsOf() requires a conversation document".into(),
            ));
        };
        self.enqueue(|line| {
            healthy(line)?;
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
            healthy(line)?;
            if cancel.is_cancelled() {
                return Err(Error::Invalid(ABORTED_WAIT.into()));
            }
            let Some(loaded) =
                self.load_document(doc.definition, &resolved.id, &resolved.address, cancel)?
            else {
                return Ok(None);
            };
            if cancel.is_cancelled() {
                return Err(Error::Invalid(ABORTED_WAIT.into()));
            }
            self.attach_watch(doc.definition, &loaded).map(Some)
        })?;
        let Some(shared) = shared else {
            return Ok(None);
        };
        if cancel.is_cancelled() {
            shared.terminate(WatchEnd::Cancelled);
            return Err(Error::Invalid(ABORTED_WAIT.into()));
        }
        shared.observe_cancellation(cancel);
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
