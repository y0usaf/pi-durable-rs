use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::cancel::CancelToken;
use crate::chord::delta::{Change, Prepared, Tracker, track};
use crate::documents::{
    CheckpointInfo, DocDefinition, ResolvedAddress, address_id, check_record_scope,
    check_record_version, document_create, materialize_document_value, resolve_address,
};
use crate::entries::Entry;
use crate::errors::{Error, Result};
use crate::ids::{ConversationId, DocumentId, EntryId, Seq, SubmissionId, TaskId};
use crate::session::forks::prepare_fork_document_copies;
use crate::session::types::{DocRef, DraftId, TaskCreate, TaskOptions, Tx};
use crate::tasks::{TaskOwnership, TaskState, TaskStatus};
use crate::types::{
    AnyTaskRecord, ConversationOwner, ConversationOwnership, ConversationParent, ConversationQuery,
    ConversationRecord, Cursor, DocumentAddress, DocumentBase, DocumentCommitChange,
    DocumentContent, DocumentCopySource, DocumentCreate, DocumentDelta, DocumentPoint,
    DocumentQuery, DocumentRecord, EntryDraft, EntryHead, EntryQuery, EntryRecord, HeadMarker,
    InputState, JsonObject, JsonValue, Page, ScopeRef, Storage, StorageWrite, SubmissionBody,
    SubmissionCreate, SubmissionRecord, SubmissionSettlement, TableChange, TaskQuery, TypedEntry,
    WriteState,
};

#[derive(Debug, Clone)]
pub struct TypedEntryDraft<D> {
    pub model: Option<Vec<crate::types::Message>>,
    pub data: D,
    pub head: Option<EntryHead>,
    pub edits: Option<Vec<crate::types::ContextEdit>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskWriteKind {
    Create,
    Replace,
}

#[derive(Debug, Clone)]
pub struct TaskWrite {
    pub kind: TaskWriteKind,
    pub record: AnyTaskRecord,
}

#[derive(Debug, Clone, Default)]
pub struct TransactionTask {
    pub committed: Option<Option<AnyTaskRecord>>,
    pub write: Option<TaskWrite>,
    pub publication_conversation_id: Option<ConversationId>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SubmissionChange {
    Settle(SubmissionSettlement),
    Place(EntryId),
}

#[derive(Debug, Clone, Copy, Default)]
pub struct TransactionScope {
    pub conversation_id: Option<ConversationId>,
    pub task_id: Option<TaskId>,
}

#[derive(Clone)]
pub struct LoadedDocument {
    pub address_id: String,
    pub record: DocumentRecord,
    pub stored_version: u64,
    pub value_version: u64,
    pub deltas_since_base: u64,
    pub tracker: Arc<Mutex<Tracker>>,
}

pub trait TransactionHost {
    fn storage(&self) -> &dyn Storage;
    fn now(&self) -> u64;
    fn cached(&self, address_id: &str) -> Option<LoadedDocument>;
    fn load(
        &self,
        definition: &DocDefinition,
        address_id: &str,
        address: &DocumentAddress,
        cancel: &CancelToken,
    ) -> Result<Option<LoadedDocument>>;
    fn install(&self, document: LoadedDocument);
    fn evict(&self, address_id: &str, record_id: DocumentId);
}

#[derive(Clone)]
pub enum DocumentTarget {
    Loaded(LoadedDocument),
    Created {
        record: DocumentCreate,
        version: u64,
        tracker: Arc<Mutex<Tracker>>,
    },
    ForkCopy {
        record: DocumentCreate,
        source: DocumentCopySource,
    },
    RetireOnly {
        record: DocumentRecord,
    },
}

pub struct DocumentEntry {
    pub address_id: String,
    pub address: DocumentAddress,
    pub definition: Option<DocDefinition>,
    pub drafted: bool,
    pub target: Option<DocumentTarget>,
    pub change: Option<Change>,
    pub prepared: Option<Prepared>,
    pub retire_on_commit: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlanRecord {
    New(DocumentCreate),
    Stored(DocumentRecord),
}

impl PlanRecord {
    fn create(&self) -> &DocumentCreate {
        match self {
            PlanRecord::New(create) => create,
            PlanRecord::Stored(record) => &record.create,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum PlanContent {
    Create {
        record: DocumentCreate,
        content: DocumentBase,
    },
    Copy {
        record: DocumentCreate,
        source: DocumentCopySource,
    },
    Change {
        id: DocumentId,
        content: DocumentContent,
    },
}

pub struct PlanChange {
    tracker: Arc<Mutex<Tracker>>,
    prepared: Option<Prepared>,
    version: u64,
    loaded: Option<LoadedDocument>,
    definition: Option<DocDefinition>,
}

pub struct DocumentPlan {
    address_id: String,
    record: PlanRecord,
    retire: bool,
    content: Option<PlanContent>,
    change: Option<PlanChange>,
    conversation_id: Option<ConversationId>,
}

pub struct Transaction<'a> {
    host: &'a dyn TransactionHost,
    cancel: &'a CancelToken,
    scope: TransactionScope,
    sealed: bool,
    has_table_write: bool,
    writes: Vec<StorageWrite>,
    created_conversations: Vec<ConversationId>,
    fork_source_conversations: Vec<ConversationId>,
    fork_source_documents: Vec<DocumentId>,
    tasks: Vec<(TaskId, TransactionTask)>,
    submissions: Vec<(SubmissionId, SubmissionRecord)>,
    submission_changes: Vec<(SubmissionId, SubmissionChange)>,
    documents: Vec<DocumentEntry>,
    latest_by_address: HashMap<String, usize>,
    plans: Vec<DocumentPlan>,
}

const INTERNAL_SCAN_PAGE_SIZE: usize = 256;

fn lock_tracker(tracker: &Mutex<Tracker>) -> Result<MutexGuard<'_, Tracker>> {
    tracker
        .lock()
        .map_err(|_| Error::Invalid("Document tracker lock is poisoned".to_owned()))
}

fn object_value(value: &JsonValue) -> Result<JsonObject> {
    match value {
        Value::Object(object) => Ok(object.clone()),
        _ => Err(Error::Invalid(
            "Document value must be an object".to_owned(),
        )),
    }
}

fn fork_is_current(record: &DocumentCreate) -> bool {
    use crate::types::{ConversationSemantics, LatestFork, RewindableFork};
    matches!(
        record.semantics,
        Some(ConversationSemantics::Latest {
            fork: LatestFork::Current
        }) | Some(ConversationSemantics::Rewindable {
            fork: RewindableFork::Current
        })
    )
}

fn apply_submission_change(
    id: SubmissionId,
    current: &SubmissionRecord,
    change: &SubmissionChange,
) -> Result<Option<SubmissionRecord>> {
    let body = match (&current.body, change) {
        (SubmissionBody::Input(InputState::Done { .. } | InputState::Unanswered { .. }), _)
        | (SubmissionBody::Write(WriteState::Done { .. } | WriteState::Unanswered { .. }), _) => {
            return Ok(None);
        }
        (SubmissionBody::Input(InputState::Queued), SubmissionChange::Place(entry)) => {
            SubmissionBody::Input(InputState::Placed { entry: *entry })
        }
        (SubmissionBody::Write(WriteState::Queued), SubmissionChange::Place(entry)) => {
            SubmissionBody::Write(WriteState::Done { entry: *entry })
        }
        (_, SubmissionChange::Place(_)) => {
            return Err(Error::Invalid(format!("Submission {id} is not queued")));
        }
        (
            SubmissionBody::Input(InputState::Placed { entry }),
            SubmissionChange::Settle(SubmissionSettlement::Done { answer }),
        ) => SubmissionBody::Input(InputState::Done {
            entry: *entry,
            answer: *answer,
        }),
        (_, SubmissionChange::Settle(SubmissionSettlement::Done { .. })) => {
            return Err(Error::Invalid(format!(
                "Submission {id} is not a placed input"
            )));
        }
        (
            SubmissionBody::Input(InputState::Queued),
            SubmissionChange::Settle(SubmissionSettlement::Unanswered { reason, detail }),
        ) => SubmissionBody::Input(InputState::Unanswered {
            entry: None,
            reason: reason.clone(),
            detail: detail.clone(),
        }),
        (
            SubmissionBody::Input(InputState::Placed { entry }),
            SubmissionChange::Settle(SubmissionSettlement::Unanswered { reason, detail }),
        ) => SubmissionBody::Input(InputState::Unanswered {
            entry: Some(*entry),
            reason: reason.clone(),
            detail: detail.clone(),
        }),
        (
            SubmissionBody::Write(WriteState::Queued),
            SubmissionChange::Settle(SubmissionSettlement::Unanswered { reason, detail }),
        ) => SubmissionBody::Write(WriteState::Unanswered {
            reason: reason.clone(),
            detail: detail.clone(),
        }),
    };
    Ok(Some(SubmissionRecord {
        body,
        ..current.clone()
    }))
}

fn target_tracker(target: &DocumentTarget) -> Option<Arc<Mutex<Tracker>>> {
    match target {
        DocumentTarget::Loaded(loaded) => Some(loaded.tracker.clone()),
        DocumentTarget::Created { tracker, .. } => Some(tracker.clone()),
        DocumentTarget::ForkCopy { .. } | DocumentTarget::RetireOnly { .. } => None,
    }
}

fn plan_document(document: &mut DocumentEntry) -> Result<Option<DocumentPlan>> {
    let Some(target) = document.target.clone() else {
        return Ok(None);
    };
    let retire = document.retire_on_commit;
    let address_id = document.address_id.clone();
    let plan = match target {
        DocumentTarget::Created {
            record,
            version,
            tracker,
        } => {
            let prepared = document
                .prepared
                .take()
                .ok_or_else(|| Error::Invalid("Document change was not prepared".to_owned()))?;
            let value = object_value(prepared.value())?;
            DocumentPlan {
                address_id,
                record: PlanRecord::New(record.clone()),
                retire,
                content: Some(PlanContent::Create {
                    record,
                    content: DocumentBase { version, value },
                }),
                change: Some(PlanChange {
                    tracker,
                    prepared: Some(prepared),
                    version,
                    loaded: None,
                    definition: None,
                }),
                conversation_id: None,
            }
        }
        DocumentTarget::ForkCopy { record, source } => DocumentPlan {
            address_id,
            record: PlanRecord::New(record.clone()),
            retire,
            content: Some(PlanContent::Copy { record, source }),
            change: None,
            conversation_id: None,
        },
        DocumentTarget::RetireOnly { record } => DocumentPlan {
            address_id,
            record: PlanRecord::Stored(record),
            retire,
            content: None,
            change: None,
            conversation_id: None,
        },
        DocumentTarget::Loaded(loaded) => {
            let definition = document
                .definition
                .clone()
                .ok_or_else(|| Error::Invalid("Document definition is missing".to_owned()))?;
            let prepared = document
                .prepared
                .take()
                .ok_or_else(|| Error::Invalid("Document change was not prepared".to_owned()))?;
            let version = definition.version;
            let id = loaded.record.create.id;
            let content = if loaded.stored_version < version {
                let value = object_value(prepared.value())?;
                Some(PlanContent::Change {
                    id,
                    content: DocumentContent::Base(DocumentBase { version, value }),
                })
            } else if !prepared.ops().is_empty() {
                Some(PlanContent::Change {
                    id,
                    content: DocumentContent::Delta(DocumentDelta {
                        version,
                        ops: prepared.ops().to_vec(),
                    }),
                })
            } else {
                None
            };
            DocumentPlan {
                address_id,
                record: PlanRecord::Stored(loaded.record.clone()),
                retire,
                content,
                change: Some(PlanChange {
                    tracker: loaded.tracker.clone(),
                    prepared: Some(prepared),
                    version,
                    loaded: Some(loaded),
                    definition: Some(definition),
                }),
                conversation_id: None,
            }
        }
    };
    Ok(Some(plan))
}

fn publishes(plan: &DocumentPlan) -> bool {
    plan.retire
        || plan
            .change
            .as_ref()
            .is_none_or(|change| change.loaded.is_none())
        || plan.content.is_some()
}

impl<'a> Transaction<'a> {
    pub fn new(
        host: &'a dyn TransactionHost,
        cancel: &'a CancelToken,
        scope: TransactionScope,
    ) -> Self {
        Self {
            host,
            cancel,
            scope,
            sealed: false,
            has_table_write: false,
            writes: Vec::new(),
            created_conversations: Vec::new(),
            tasks: Vec::new(),
            submissions: Vec::new(),
            submission_changes: Vec::new(),
            documents: Vec::new(),
            latest_by_address: HashMap::new(),
            plans: Vec::new(),
            fork_source_conversations: Vec::new(),
            fork_source_documents: Vec::new(),
        }
    }

    fn assert_open(&self) -> Result<()> {
        if self.sealed {
            return Err(Error::Invalid("Transaction has settled".to_owned()));
        }
        Ok(())
    }

    fn read<T>(&mut self, method: &str, read: impl FnOnce(&mut Self) -> Result<T>) -> Result<T> {
        self.assert_open()?;
        if self.has_table_write {
            return Err(Error::ReadAfterWrite {
                method: method.to_owned(),
            });
        }
        read(self)
    }

    fn begin_write(&mut self) -> Result<()> {
        self.assert_open()?;
        self.has_table_write = true;
        Ok(())
    }

    fn task_slot(&mut self, id: TaskId) -> &mut TransactionTask {
        let index = match self.tasks.iter().position(|(task, _)| *task == id) {
            Some(index) => index,
            None => {
                self.tasks.push((id, TransactionTask::default()));
                self.tasks.len() - 1
            }
        };
        &mut self.tasks[index].1
    }

    fn committed_task(&mut self, id: TaskId) -> Result<Option<AnyTaskRecord>> {
        if let Some(committed) = self.task_slot(id).committed.clone() {
            return Ok(committed);
        }
        let host = self.host;
        let record = host.storage().task(id, self.cancel)?;
        self.task_slot(id).committed = Some(record.clone());
        Ok(record)
    }

    fn current_task(&mut self, id: TaskId) -> Result<Option<AnyTaskRecord>> {
        if let Some(write) = self
            .tasks
            .iter()
            .find(|(task, _)| *task == id)
            .and_then(|(_, task)| task.write.as_ref())
        {
            return Ok(Some(write.record.clone()));
        }
        self.committed_task(id)
    }

    fn require_conversation(&mut self, id: ConversationId) -> Result<()> {
        if self.created_conversations.contains(&id) {
            return Ok(());
        }
        let host = self.host;
        if host.storage().conversation(id, self.cancel)?.is_none() {
            return Err(Error::Invalid(format!("Conversation {id} does not exist")));
        }
        Ok(())
    }

    fn stamp_times(
        &self,
        value: AnyTaskRecord,
        candidate: Option<&AnyTaskRecord>,
    ) -> AnyTaskRecord {
        let status = value.status();
        let started_at = candidate
            .and_then(|record| record.started_at)
            .or(value.started_at)
            .or_else(|| matches!(status, TaskStatus::Running).then(|| self.host.now()));
        let ended_at = candidate
            .and_then(|record| record.ended_at)
            .or(value.ended_at)
            .or_else(|| matches!(status, TaskStatus::Terminal).then(|| self.host.now()));
        if started_at == value.started_at && ended_at == value.ended_at {
            return value;
        }
        AnyTaskRecord {
            started_at,
            ended_at,
            ..value
        }
    }

    fn submission_set(&mut self, id: SubmissionId, record: SubmissionRecord) {
        match self.submissions.iter_mut().find(|(known, _)| *known == id) {
            Some((_, slot)) => *slot = record,
            None => self.submissions.push((id, record)),
        }
    }

    pub fn conversation(&mut self, id: ConversationId) -> Result<Option<ConversationRecord>> {
        self.read("conversation", |tx| {
            let host = tx.host;
            host.storage().conversation(id, tx.cancel)
        })
    }

    pub fn entry(&mut self, id: EntryId) -> Result<Option<EntryRecord>> {
        self.read("entry", |tx| {
            let host = tx.host;
            Ok(host
                .storage()
                .entry(id, tx.cancel)?
                .map(|committed| committed.entry))
        })
    }

    pub fn entry_as<D: DeserializeOwned>(
        &mut self,
        token: &Entry<D>,
        id: EntryId,
    ) -> Result<Option<TypedEntry<D>>> {
        match self.entry(id)? {
            Some(record) => token.narrow(record),
            None => Ok(None),
        }
    }

    pub fn task(&mut self, id: TaskId) -> Result<Option<AnyTaskRecord>> {
        self.read("task", |tx| tx.committed_task(id))
    }

    pub fn scan_conversations(
        &mut self,
        query: &ConversationQuery,
        limit: usize,
        cursor: Option<&Cursor>,
    ) -> Result<Page<ConversationRecord>> {
        self.read("scanConversations", |tx| {
            let host = tx.host;
            host.storage()
                .scan_conversations(query, limit, cursor, tx.cancel)
        })
    }

    pub fn scan_entries(
        &mut self,
        query: &EntryQuery,
        limit: usize,
        cursor: Option<&Cursor>,
    ) -> Result<Page<EntryRecord>> {
        self.read("scanEntries", |tx| {
            let host = tx.host;
            host.storage().scan_entries(query, limit, cursor, tx.cancel)
        })
    }

    pub fn latest_head_marker(
        &mut self,
        conversation_id: ConversationId,
    ) -> Result<Option<HeadMarker>> {
        self.read("latestHeadMarker", |tx| {
            let host = tx.host;
            host.storage()
                .find_latest_head_marker(conversation_id, None, tx.cancel)
        })
    }

    pub fn scan_tasks(
        &mut self,
        query: &TaskQuery,
        limit: usize,
        cursor: Option<&Cursor>,
    ) -> Result<Page<AnyTaskRecord>> {
        self.read("scanTasks", |tx| {
            let host = tx.host;
            host.storage().scan_tasks(query, limit, cursor, tx.cancel)
        })
    }

    pub fn submission(&mut self, id: SubmissionId) -> Result<Option<SubmissionRecord>> {
        self.read("submission", |tx| {
            let host = tx.host;
            host.storage().submission(id, tx.cancel)
        })
    }

    pub fn submission_by_request(
        &mut self,
        conversation_id: ConversationId,
        request_id: &str,
    ) -> Result<Option<SubmissionRecord>> {
        self.read("submissionByRequest", |tx| {
            let host = tx.host;
            host.storage()
                .submission_by_request(conversation_id, request_id, tx.cancel)
        })
    }

    pub fn append_entry(
        &mut self,
        conversation_id: ConversationId,
        draft: EntryDraft,
    ) -> Result<EntryRecord> {
        self.begin_write()?;
        self.require_conversation(conversation_id)?;
        self.assert_open()?;
        let host = self.host;
        let id = EntryId(host.storage().mint_id()?);
        self.assert_open()?;
        let head = match draft.head {
            None => None,
            Some(EntryHead::SelfHead) => Some(id),
            Some(EntryHead::Entry(head)) => Some(head),
        };
        let record = EntryRecord {
            id,
            conversation_id,
            kind: draft.kind,
            model: draft.model,
            data: draft.data,
            head,
            edits: draft.edits,
            by_task_id: self.scope.task_id,
        };
        self.writes
            .push(StorageWrite::Table(Box::new(TableChange::Entry(
                record.clone(),
            ))));
        Ok(record)
    }

    pub fn append_entry_as<D: Serialize>(
        &mut self,
        token: &Entry<D>,
        conversation_id: ConversationId,
        draft: TypedEntryDraft<D>,
    ) -> Result<TypedEntry<D>> {
        let data = serde_json::to_value(&draft.data)?;
        let entry = self.append_entry(
            conversation_id,
            EntryDraft {
                kind: token.kind().to_owned(),
                model: draft.model,
                data: Some(data),
                head: draft.head,
                edits: draft.edits,
            },
        )?;
        Ok(TypedEntry {
            entry,
            data: draft.data,
        })
    }

    pub fn create_task(&mut self, task: TaskCreate, options: TaskOptions) -> Result<TaskId> {
        self.begin_write()?;
        let mut owner_id = None;
        let mut owner_conversation = None;
        if let TaskOwnership::Task { task_id } = options.ownership {
            let owner = self.current_task(task_id)?;
            self.assert_open()?;
            let owner = owner
                .ok_or_else(|| Error::Invalid(format!("Task owner {task_id} does not exist")))?;
            if options.background == Some(true) {
                return Err(Error::Invalid(
                    "A child task cannot be background".to_owned(),
                ));
            }
            if options
                .conversation_id
                .is_some_and(|id| id != owner.conversation_id)
            {
                return Err(Error::Invalid(format!(
                    "A child task lives in its owner's conversation {}",
                    owner.conversation_id
                )));
            }
            owner_id = Some(task_id);
            owner_conversation = Some(owner.conversation_id);
        }
        let conversation_id = owner_conversation
            .or(options.conversation_id)
            .or(self.scope.conversation_id)
            .ok_or_else(|| {
                Error::Invalid("Tx.createTask() requires options.conversationId".to_owned())
            })?;
        self.require_conversation(conversation_id)?;
        self.assert_open()?;
        let host = self.host;
        let id = TaskId::new(host.storage().mint_id()?);
        self.assert_open()?;
        let record = AnyTaskRecord {
            id,
            conversation_id,
            kind: task.kind,
            version: task.version,
            input: task.input,
            owner: owner_id,
            background: options.background.unwrap_or(false),
            abort_requested: false,
            started_at: None,
            ended_at: None,
            state: TaskState::Pending {
                checkpoint: task.checkpoint,
            },
            memos: None,
        };
        self.task_slot(id).write = Some(TaskWrite {
            kind: TaskWriteKind::Create,
            record,
        });
        Ok(id)
    }

    pub fn create_submission(&mut self, create: SubmissionCreate) -> Result<SubmissionRecord> {
        self.begin_write()?;
        self.require_conversation(create.conversation_id)?;
        self.assert_open()?;
        let host = self.host;
        let id = SubmissionId(host.storage().mint_id()?);
        self.assert_open()?;
        let record = SubmissionRecord {
            id,
            conversation_id: create.conversation_id,
            request_id: create.request_id,
            body: create.body,
        };
        self.submission_set(id, record.clone());
        Ok(record)
    }

    pub fn settle_submission(
        &mut self,
        id: SubmissionId,
        settlement: SubmissionSettlement,
    ) -> Result<()> {
        self.assert_open()?;
        self.has_table_write = true;
        self.submission_changes
            .push((id, SubmissionChange::Settle(settlement)));
        Ok(())
    }

    pub fn place_submission(&mut self, id: SubmissionId, entry: EntryId) -> Result<()> {
        self.assert_open()?;
        self.has_table_write = true;
        self.submission_changes
            .push((id, SubmissionChange::Place(entry)));
        Ok(())
    }

    pub fn set_task(&mut self, value: AnyTaskRecord) -> Result<()> {
        self.assert_open()?;
        self.has_table_write = true;
        let id = value.id;
        let existing = self.task_slot(id).write.clone();
        let candidate = existing.as_ref().map(|write| &write.record);
        if let Some(candidate) = candidate {
            if matches!(candidate.state, TaskState::Terminal { .. }) {
                return Err(Error::Invalid(format!(
                    "Task {id} already has a terminal candidate"
                )));
            }
            if candidate.conversation_id != value.conversation_id {
                return Err(Error::Invalid(format!(
                    "Task {id} cannot change conversations"
                )));
            }
        }
        let kind = match existing.as_ref().map(|write| write.kind) {
            Some(TaskWriteKind::Create) => TaskWriteKind::Create,
            _ => TaskWriteKind::Replace,
        };
        let record = self.stamp_times(value, candidate);
        self.task_slot(id).write = Some(TaskWrite { kind, record });
        Ok(())
    }

    pub fn staged_tasks(&self) -> Vec<AnyTaskRecord> {
        self.tasks
            .iter()
            .filter_map(|(_, task)| task.write.as_ref().map(|write| write.record.clone()))
            .collect()
    }

    pub fn staged_conversations(&self) -> Vec<ConversationRecord> {
        self.writes
            .iter()
            .filter_map(|write| match write {
                StorageWrite::Table(table) => match table.as_ref() {
                    TableChange::Conversation(record) => Some(record.clone()),
                    _ => None,
                },
                _ => None,
            })
            .collect()
    }
}
impl Transaction<'_> {
    fn latest_index(&self, address_id: &str) -> Option<usize> {
        self.latest_by_address.get(address_id).copied()
    }

    fn push_document(&mut self, entry: DocumentEntry) -> usize {
        let index = self.documents.len();
        self.latest_by_address
            .insert(entry.address_id.clone(), index);
        self.documents.push(entry);
        index
    }

    fn assert_task_documents_open(&self, address: &DocumentAddress) -> Result<()> {
        if let ScopeRef::Task { task_id } = address.scope {
            let terminal = self
                .tasks
                .iter()
                .find(|(task, _)| *task == task_id)
                .and_then(|(_, task)| task.write.as_ref())
                .is_some_and(|write| matches!(write.record.state, TaskState::Terminal { .. }));
            if terminal {
                return Err(Error::Invalid(format!("Task {task_id} is terminal")));
            }
        }
        Ok(())
    }

    fn open_doc(&mut self, doc: DocRef, seed: Option<JsonValue>) -> Result<DraftId> {
        self.assert_open()?;
        let resolved: ResolvedAddress = resolve_address(doc.definition, doc.owner, doc.key)?;
        self.assert_task_documents_open(&resolved.address)?;
        let latest = self.latest_index(&resolved.id);
        if let Some(index) = latest {
            let entry = &self.documents[index];
            if !entry.retire_on_commit {
                if entry.drafted {
                    return Ok(DraftId(index));
                }
                if let Some(DocumentTarget::ForkCopy { record, source }) = entry.target.clone() {
                    let definition = doc.definition.clone();
                    self.acquire_fork_copy(index, &definition, record, source)?;
                    self.documents[index].drafted = true;
                    return Ok(DraftId(index));
                }
            }
        }
        let skip_load = latest.is_some_and(|index| self.documents[index].retire_on_commit);
        let seed = if doc.definition.family { seed } else { None };
        let index = self.push_document(DocumentEntry {
            address_id: resolved.id,
            address: resolved.address,
            definition: Some(doc.definition.clone()),
            drafted: false,
            target: None,
            change: None,
            prepared: None,
            retire_on_commit: false,
        });
        self.acquire(index, seed, skip_load)?;
        self.documents[index].drafted = true;
        Ok(DraftId(index))
    }

    fn acquire(&mut self, index: usize, seed: Option<JsonValue>, skip_load: bool) -> Result<()> {
        let host = self.host;
        let entry = &self.documents[index];
        let definition = entry
            .definition
            .clone()
            .ok_or_else(|| Error::Invalid("Document definition is missing".to_owned()))?;
        let address_id = entry.address_id.clone();
        let address = entry.address.clone();
        let loaded = if skip_load {
            None
        } else {
            host.load(&definition, &address_id, &address, self.cancel)?
        };
        self.assert_open()?;
        if let Some(loaded) = loaded {
            check_record_scope(&definition, &loaded.record.create)?;
            check_record_version(&definition, &loaded.record.create, loaded.stored_version)?;
            let change = lock_tracker(&loaded.tracker)?.begin_change();
            let entry = &mut self.documents[index];
            entry.target = Some(DocumentTarget::Loaded(loaded));
            entry.change = Some(change);
            return Ok(());
        }
        match address.scope {
            ScopeRef::Conversation { conversation_id } => {
                self.require_conversation(conversation_id)?
            }
            ScopeRef::Task { task_id } => {
                let task = self
                    .current_task(task_id)?
                    .ok_or_else(|| Error::Invalid(format!("Task {task_id} does not exist")))?;
                if matches!(task.state, TaskState::Terminal { .. }) {
                    return Err(Error::Invalid(format!("Task {task_id} is terminal")));
                }
            }
            ScopeRef::Session => {}
        }
        self.assert_open()?;
        let value = (definition.initial)(seed.as_ref());
        let id = DocumentId(host.storage().mint_id()?);
        self.assert_open()?;
        let tracker = Arc::new(Mutex::new(track(Value::Object(value))));
        let change = lock_tracker(&tracker)?.begin_change();
        let entry = &mut self.documents[index];
        entry.target = Some(DocumentTarget::Created {
            record: document_create(&definition, &address, id),
            version: definition.version,
            tracker,
        });
        entry.change = Some(change);
        Ok(())
    }

    fn acquire_fork_copy(
        &mut self,
        index: usize,
        definition: &DocDefinition,
        record: DocumentCreate,
        source: DocumentCopySource,
    ) -> Result<()> {
        let host = self.host;
        let stored = host.storage().document(source.id, source.at, self.cancel)?;
        self.assert_open()?;
        let stored = stored.ok_or_else(|| {
            Error::Invalid(format!("Fork source document {} cannot be read", source.id))
        })?;
        let create = &stored.record.create;
        if !matches!(create.scope, ScopeRef::Conversation { .. })
            || create.kind != record.kind
            || create.key != record.key
            || create.semantics != record.semantics
        {
            return Err(Error::Invalid(format!(
                "Fork source document {} does not match the copied record",
                source.id
            )));
        }
        let value = materialize_document_value(definition, &record, stored.version, stored.value)?;
        let tracker = Arc::new(Mutex::new(track(Value::Object(value))));
        let change = lock_tracker(&tracker)?.begin_change();
        let entry = &mut self.documents[index];
        entry.definition = Some(definition.clone());
        entry.target = Some(DocumentTarget::Created {
            record,
            version: definition.version,
            tracker,
        });
        entry.change = Some(change);
        Ok(())
    }

    fn find_retirement(&mut self, index: usize) -> Result<()> {
        let host = self.host;
        let entry = &self.documents[index];
        let definition = entry
            .definition
            .clone()
            .ok_or_else(|| Error::Invalid("Document definition is missing".to_owned()))?;
        let address = entry.address.clone();
        let cached = host.cached(&entry.address_id).map(|loaded| loaded.record);
        let record = match cached {
            Some(record) => Some(record),
            None => host
                .storage()
                .find_document(&address, DocumentPoint::Current, self.cancel)?,
        };
        self.assert_open()?;
        let Some(record) = record else {
            return Ok(());
        };
        check_record_scope(&definition, &record.create)?;
        self.documents[index].target = Some(DocumentTarget::RetireOnly { record });
        Ok(())
    }

    fn retire_doc(&mut self, doc: DocRef) -> Result<()> {
        self.assert_open()?;
        let resolved = resolve_address(doc.definition, doc.owner, doc.key)?;
        if let Some(index) = self.latest_index(&resolved.id) {
            let entry = &self.documents[index];
            if entry.retire_on_commit {
                return Ok(());
            }
            if let Some(DocumentTarget::ForkCopy { record, .. }) = &entry.target {
                check_record_scope(doc.definition, record)?;
                self.documents[index].retire_on_commit = true;
                return Ok(());
            }
            if entry.drafted {
                self.documents[index].retire_on_commit = true;
                return Ok(());
            }
        }
        let index = self.push_document(DocumentEntry {
            address_id: resolved.id,
            address: resolved.address,
            definition: Some(doc.definition.clone()),
            drafted: false,
            target: None,
            change: None,
            prepared: None,
            retire_on_commit: true,
        });
        self.find_retirement(index)
    }

    fn draft_mut(&mut self, draft: DraftId) -> Result<&mut JsonValue> {
        self.assert_open()?;
        let entry = self
            .documents
            .get_mut(draft.0)
            .ok_or_else(|| Error::Invalid("Document draft does not exist".to_owned()))?;
        let change = entry
            .change
            .as_mut()
            .ok_or_else(|| Error::Invalid("Document draft is not open".to_owned()))?;
        Ok(change.state())
    }

    fn stage_conversation(
        &mut self,
        parent: Option<ConversationParent>,
        ownership: ConversationOwnership,
    ) -> Result<ConversationRecord> {
        let host = self.host;
        let id = ConversationId(host.storage().mint_id()?);
        self.assert_open()?;
        let owner = match ownership {
            ConversationOwnership::Ownerless => None,
            ConversationOwnership::Task(task_id) => {
                let task = self.current_task(task_id)?;
                self.assert_open()?;
                let task = task.ok_or_else(|| {
                    Error::Invalid(format!("Conversation owner task {task_id} does not exist"))
                })?;
                Some(ConversationOwner {
                    conversation_id: task.conversation_id,
                    task_id,
                })
            }
        };
        let record = ConversationRecord {
            id,
            parent: parent.clone(),
            owner,
        };
        let copies = match &parent {
            None => Vec::new(),
            Some(parent) => prepare_fork_document_copies(
                host.storage(),
                parent.conversation_id,
                parent.at,
                id,
                self.cancel,
            )?,
        };
        self.assert_open()?;
        for copy in copies {
            self.fork_source_documents.push(copy.source.id);
            let address = DocumentAddress {
                kind: copy.record.kind.clone(),
                scope: copy.record.scope.clone(),
                key: copy.record.key.clone(),
            };
            let key = address_id(&address);
            self.push_document(DocumentEntry {
                address_id: key,
                address,
                definition: None,
                drafted: false,
                target: Some(DocumentTarget::ForkCopy {
                    record: copy.record,
                    source: copy.source,
                }),
                change: None,
                prepared: None,
                retire_on_commit: false,
            });
        }
        if let Some(parent) = &parent {
            self.fork_source_conversations.push(parent.conversation_id);
        }
        self.created_conversations.push(id);
        self.writes
            .push(StorageWrite::Table(Box::new(TableChange::Conversation(
                record.clone(),
            ))));
        self.assert_open()?;
        Ok(record)
    }
}

impl Tx for Transaction<'_> {
    fn conversation(&mut self, id: ConversationId) -> Result<Option<ConversationRecord>> {
        Transaction::conversation(self, id)
    }

    fn entry(&mut self, id: EntryId) -> Result<Option<EntryRecord>> {
        Transaction::entry(self, id)
    }

    fn task(&mut self, id: TaskId) -> Result<Option<AnyTaskRecord>> {
        Transaction::task(self, id)
    }

    fn submission(&mut self, id: SubmissionId) -> Result<Option<SubmissionRecord>> {
        Transaction::submission(self, id)
    }

    fn scan_conversations(
        &mut self,
        query: &ConversationQuery,
        limit: usize,
        cursor: Option<&Cursor>,
    ) -> Result<Page<ConversationRecord>> {
        Transaction::scan_conversations(self, query, limit, cursor)
    }

    fn scan_entries(
        &mut self,
        query: &EntryQuery,
        limit: usize,
        cursor: Option<&Cursor>,
    ) -> Result<Page<EntryRecord>> {
        Transaction::scan_entries(self, query, limit, cursor)
    }

    fn scan_tasks(
        &mut self,
        query: &TaskQuery,
        limit: usize,
        cursor: Option<&Cursor>,
    ) -> Result<Page<AnyTaskRecord>> {
        Transaction::scan_tasks(self, query, limit, cursor)
    }

    fn latest_head_marker(
        &mut self,
        conversation_id: ConversationId,
    ) -> Result<Option<HeadMarker>> {
        Transaction::latest_head_marker(self, conversation_id)
    }

    fn submission_by_request(
        &mut self,
        conversation_id: ConversationId,
        request_id: &str,
    ) -> Result<Option<SubmissionRecord>> {
        Transaction::submission_by_request(self, conversation_id, request_id)
    }

    fn create_conversation(
        &mut self,
        ownership: ConversationOwnership,
    ) -> Result<ConversationRecord> {
        self.begin_write()?;
        self.stage_conversation(None, ownership)
    }

    fn fork_conversation(
        &mut self,
        parent: ConversationId,
        at: EntryId,
        ownership: ConversationOwnership,
    ) -> Result<ConversationRecord> {
        self.begin_write()?;
        self.stage_conversation(
            Some(ConversationParent {
                conversation_id: parent,
                at,
            }),
            ownership,
        )
    }

    fn append_entry(
        &mut self,
        conversation_id: ConversationId,
        draft: EntryDraft,
    ) -> Result<EntryRecord> {
        Transaction::append_entry(self, conversation_id, draft)
    }

    fn create_task(&mut self, task: TaskCreate, options: TaskOptions) -> Result<TaskId> {
        Transaction::create_task(self, task, options)
    }

    fn create_submission(&mut self, create: SubmissionCreate) -> Result<SubmissionRecord> {
        Transaction::create_submission(self, create)
    }

    fn settle_submission(
        &mut self,
        id: SubmissionId,
        settlement: SubmissionSettlement,
    ) -> Result<()> {
        Transaction::settle_submission(self, id, settlement)
    }

    fn place_submission(&mut self, id: SubmissionId, entry: EntryId) -> Result<()> {
        Transaction::place_submission(self, id, entry)
    }

    fn doc(&mut self, doc: DocRef, seed: Option<JsonValue>) -> Result<DraftId> {
        self.open_doc(doc, seed)
    }

    fn draft(&mut self, draft: DraftId) -> Result<&mut JsonValue> {
        self.draft_mut(draft)
    }

    fn retire_doc(&mut self, doc: DocRef) -> Result<()> {
        Transaction::retire_doc(self, doc)
    }
}

impl Transaction<'_> {
    pub fn settle_failure(&mut self) {
        self.sealed = true;
        self.abort_changes();
    }

    pub fn settle_success(&mut self) -> Result<Vec<StorageWrite>> {
        self.sealed = true;
        let result = self.prepare_and_assemble();
        if result.is_err() {
            self.abort_changes();
        }
        result
    }

    fn prepare_and_assemble(&mut self) -> Result<Vec<StorageWrite>> {
        for document in &mut self.documents {
            let Some(change) = document.change.take() else {
                continue;
            };
            let tracker = document
                .target
                .as_ref()
                .and_then(target_tracker)
                .ok_or_else(|| Error::Invalid("Document change has no tracker".to_owned()))?;
            let guard = lock_tracker(&tracker)?;
            let prepared = change.prepare(&guard)?;
            document.prepared = Some(prepared);
        }
        self.assemble()
    }

    pub fn discard(&mut self) {
        self.abort_changes();
    }

    fn abort_changes(&mut self) {
        for document in &mut self.documents {
            document.change = None;
            document.prepared = None;
        }
    }

    fn task_write(&self, id: TaskId) -> Option<&TaskWrite> {
        self.tasks
            .iter()
            .find(|(task, _)| *task == id)
            .and_then(|(_, task)| task.write.as_ref())
    }

    pub fn adopt(&mut self, seq: Seq) -> Result<Vec<DocumentCommitChange>> {
        let host = self.host;
        let mut publications = Vec::new();
        for mut plan in std::mem::take(&mut self.plans) {
            let committed = matches!(plan.record, PlanRecord::Stored(_));
            let mut record = match &plan.record {
                PlanRecord::Stored(record) => record.clone(),
                PlanRecord::New(create) => DocumentRecord {
                    create: create.clone(),
                    created_at: seq,
                    retired_at: None,
                },
            };
            if plan.retire {
                record.retired_at = Some(seq);
            }
            let publish = publishes(&plan);
            let conversation_id = plan.conversation_id;
            let copy_source = match &plan.content {
                Some(PlanContent::Copy { source, .. }) => Some(source.clone()),
                _ => None,
            };
            let mut publication = None;
            if let Some(PlanChange {
                tracker,
                prepared,
                version,
                loaded,
                ..
            }) = plan.change.take()
            {
                let is_new = loaded.is_none();
                let prepared = prepared
                    .ok_or_else(|| Error::Invalid("Document change was not prepared".to_owned()))?;
                let value = object_value(prepared.value())?;
                let ops = prepared.ops().to_vec();
                let adopt = match &loaded {
                    None => !plan.retire,
                    Some(_) => !ops.is_empty(),
                };
                if adopt {
                    lock_tracker(&tracker)?.adopt(prepared)?;
                }
                match loaded {
                    Some(mut loaded) => {
                        if loaded.stored_version < version {
                            loaded.stored_version = version;
                        }
                        if let Some(PlanContent::Change { content, .. }) = &plan.content {
                            match content {
                                DocumentContent::Base(_) => loaded.deltas_since_base = 0,
                                DocumentContent::Delta(_) => loaded.deltas_since_base += 1,
                            }
                        }
                        host.install(loaded);
                    }
                    None if !plan.retire => host.install(LoadedDocument {
                        address_id: plan.address_id.clone(),
                        record: record.clone(),
                        stored_version: version,
                        value_version: version,
                        deltas_since_base: 0,
                        tracker,
                    }),
                    None => {}
                }
                publication = Some((version, value, ops, is_new));
            }
            if plan.retire {
                if committed {
                    host.evict(&plan.address_id, record.create.id);
                }
                publications.push(DocumentCommitChange::Document {
                    record,
                    conversation_id,
                    version: None,
                    value: None,
                    ops: Vec::new(),
                });
            } else if let Some(source) = copy_source {
                let conversation_id = conversation_id.ok_or_else(|| {
                    Error::Invalid("Document copy has no conversation".to_owned())
                })?;
                publications.push(DocumentCommitChange::Copy {
                    record,
                    conversation_id,
                    source,
                });
            } else if let (Some((version, value, ops, is_new)), true) = (publication, publish) {
                publications.push(DocumentCommitChange::Document {
                    record,
                    conversation_id,
                    version: Some(version),
                    value: Some(value),
                    ops: if is_new { Vec::new() } else { ops },
                });
            }
        }
        Ok(publications)
    }

    fn assemble(&mut self) -> Result<Vec<StorageWrite>> {
        let host = self.host;
        let mut plans = Vec::new();
        for document in &mut self.documents {
            if let Some(plan) = plan_document(document)? {
                plans.push(plan);
            }
        }
        self.reject_fork_source_writes(&plans)?;
        self.validate_owners()?;
        let task_ids: Vec<TaskId> = self.tasks.iter().map(|(id, _)| *id).collect();
        for id in task_ids {
            let Some(write) = self.task_write(id).cloned() else {
                continue;
            };
            if write.kind != TaskWriteKind::Replace {
                continue;
            }
            let committed = self
                .committed_task(id)?
                .ok_or_else(|| Error::Invalid(format!("Task {id} does not exist")))?;
            if matches!(committed.state, TaskState::Terminal { .. }) {
                return Err(Error::Invalid(format!("Task {id} is already terminal")));
            }
            if committed.conversation_id != write.record.conversation_id {
                return Err(Error::Invalid(format!(
                    "Task {id} cannot change conversations"
                )));
            }
        }

        let terminal_ids: Vec<TaskId> = self
            .tasks
            .iter()
            .filter_map(|(_, task)| task.write.as_ref())
            .filter(|write| matches!(write.record.state, TaskState::Terminal { .. }))
            .map(|write| write.record.id)
            .collect();
        if !terminal_ids.is_empty() {
            let mut retiring: Vec<DocumentId> = Vec::new();
            for plan in &mut plans {
                let ScopeRef::Task { task_id } = plan.record.create().scope else {
                    continue;
                };
                if !terminal_ids.contains(&task_id) {
                    continue;
                }
                plan.retire = true;
                retiring.push(plan.record.create().id);
            }
            for task_id in &terminal_ids {
                if self
                    .task_write(*task_id)
                    .is_some_and(|write| write.kind == TaskWriteKind::Create)
                {
                    continue;
                }
                let mut cursor: Option<Cursor> = None;
                loop {
                    let query = DocumentQuery {
                        scope: ScopeRef::Task { task_id: *task_id },
                        at: DocumentPoint::Current,
                        kind: None,
                    };
                    let page = host.storage().scan_documents(
                        &query,
                        INTERNAL_SCAN_PAGE_SIZE,
                        cursor.as_ref(),
                        self.cancel,
                    )?;
                    for record in page.items {
                        if retiring.contains(&record.create.id) {
                            continue;
                        }
                        retiring.push(record.create.id);
                        let address = DocumentAddress {
                            kind: record.create.kind.clone(),
                            scope: record.create.scope.clone(),
                            key: record.create.key.clone(),
                        };
                        plans.push(DocumentPlan {
                            address_id: address_id(&address),
                            record: PlanRecord::Stored(record),
                            retire: true,
                            content: None,
                            change: None,
                            conversation_id: None,
                        });
                    }
                    match page.next {
                        Some(next) => cursor = Some(next),
                        None => break,
                    }
                }
            }
        }

        for plan in &mut plans {
            if !publishes(plan) {
                continue;
            }
            match plan.record.create().scope {
                ScopeRef::Conversation { conversation_id } => {
                    plan.conversation_id = Some(conversation_id)
                }
                ScopeRef::Task { task_id } => {
                    if self
                        .task_slot(task_id)
                        .publication_conversation_id
                        .is_none()
                        && let Some(current) = self.current_task(task_id)?
                    {
                        self.task_slot(task_id).publication_conversation_id =
                            Some(current.conversation_id);
                    }
                    plan.conversation_id = self.task_slot(task_id).publication_conversation_id;
                }
                ScopeRef::Session => {}
            }
        }

        for (id, change) in self.submission_changes.clone() {
            let current = match self.submissions.iter().find(|(known, _)| *known == id) {
                Some((_, record)) => record.clone(),
                None => host
                    .storage()
                    .submission(id, self.cancel)?
                    .ok_or_else(|| Error::Invalid(format!("Submission {id} does not exist")))?,
            };
            if let Some(next) = apply_submission_change(id, &current, &change)? {
                self.submission_set(id, next);
            }
        }

        let mut writes = std::mem::take(&mut self.writes);
        for (_, record) in &self.submissions {
            writes.push(StorageWrite::Table(Box::new(TableChange::Submission(
                record.clone(),
            ))));
        }
        for (_, task) in &self.tasks {
            if let Some(write) = &task.write {
                writes.push(StorageWrite::Table(Box::new(TableChange::Task(Box::new(
                    write.record.clone(),
                )))));
            }
        }
        for plan in &mut plans {
            let checkpoint = match (&plan.content, &plan.change) {
                (
                    Some(PlanContent::Change {
                        id,
                        content: DocumentContent::Delta(_),
                    }),
                    Some(PlanChange {
                        loaded: Some(loaded),
                        definition: Some(definition),
                        prepared: Some(prepared),
                        version,
                        ..
                    }),
                ) => match &definition.checkpoint_when {
                    Some(checkpoint_when) => {
                        let info = CheckpointInfo {
                            deltas_since_base: loaded.deltas_since_base,
                        };
                        let value = object_value(prepared.value())?;
                        if checkpoint_when(&value, prepared.ops(), info) {
                            Some((*id, *version, value))
                        } else {
                            None
                        }
                    }
                    None => None,
                },
                _ => None,
            };
            if let Some((id, version, value)) = checkpoint {
                plan.content = Some(PlanContent::Change {
                    id,
                    content: DocumentContent::Base(DocumentBase { version, value }),
                });
            }
            match plan.content.clone() {
                None => {}
                Some(PlanContent::Create { record, content }) => {
                    writes.push(StorageWrite::DocumentCreate { record, content });
                }
                Some(PlanContent::Copy { record, source }) => {
                    writes.push(StorageWrite::DocumentCopy { record, source });
                }
                Some(PlanContent::Change { id, content }) => {
                    writes.push(StorageWrite::DocumentChange { id, content });
                }
            }
            if plan.retire {
                writes.push(StorageWrite::DocumentRetire {
                    id: plan.record.create().id,
                });
            }
        }
        self.plans = plans;
        Ok(writes)
    }

    fn validate_owners(&mut self) -> Result<()> {
        let mut owners: Vec<(&'static str, TaskId)> = Vec::new();
        for write in &self.writes {
            if let StorageWrite::Table(table) = write
                && let TableChange::Conversation(record) = table.as_ref()
                && let Some(owner) = &record.owner
            {
                owners.push(("Conversation owner task", owner.task_id));
            }
        }
        for (_, task) in &self.tasks {
            if let Some(TaskWrite {
                kind: TaskWriteKind::Create,
                record,
            }) = &task.write
                && let Some(owner) = record.owner
            {
                owners.push(("Task owner", owner));
            }
        }
        for (what, task_id) in owners {
            let task = self
                .current_task(task_id)?
                .ok_or_else(|| Error::Invalid(format!("{what} {task_id} does not exist")))?;
            let status = match task.state {
                TaskState::Terminal { .. } => Some("terminal"),
                TaskState::Completing { .. } => Some("completing"),
                _ => None,
            };
            if let Some(status) = status {
                return Err(Error::Invalid(format!("{what} {task_id} is {status}")));
            }
            if task.abort_requested {
                return Err(Error::Invalid(format!("{what} {task_id} is abort-marked")));
            }
        }
        Ok(())
    }

    fn reject_fork_source_writes(&self, plans: &[DocumentPlan]) -> Result<()> {
        for plan in plans {
            if plan.content.is_none() && !plan.retire {
                continue;
            }
            let record = plan.record.create();
            if self.fork_source_documents.contains(&record.id) {
                return Err(Error::Invalid(format!(
                    "Cannot change fork source document {} in the fork transaction",
                    record.id
                )));
            }
            if let ScopeRef::Conversation { conversation_id } = record.scope
                && self.fork_source_conversations.contains(&conversation_id)
                && fork_is_current(record)
            {
                return Err(Error::Invalid(format!(
                    "Cannot fork conversation {conversation_id} while changing its current-policy documents"
                )));
            }
        }
        Ok(())
    }
}
