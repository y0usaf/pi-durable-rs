use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::cancel::CancelToken;
use crate::entries::Entry;
use crate::errors::{Error, Result};
use crate::ids::{ConversationId, EntryId, SubmissionId, TaskId};
use crate::session::types::{TaskCreate, TaskOptions};
use crate::tasks::{TaskOwnership, TaskState, TaskStatus};
use crate::types::{
    AnyTaskRecord, ConversationQuery, ConversationRecord, Cursor, EntryDraft, EntryHead,
    EntryQuery, EntryRecord, HeadMarker, Page, Storage, StorageWrite, SubmissionCreate,
    SubmissionRecord, SubmissionSettlement, TableChange, TaskQuery, TypedEntry,
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

pub trait TransactionHost {
    fn storage(&self) -> &dyn Storage;
    fn now(&self) -> u64;
}

pub struct Transaction<'a> {
    host: &'a dyn TransactionHost,
    cancel: &'a CancelToken,
    scope: TransactionScope,
    sealed: bool,
    has_table_write: bool,
    writes: Vec<StorageWrite>,
    created_conversations: Vec<ConversationId>,
    tasks: Vec<(TaskId, TransactionTask)>,
    submissions: Vec<(SubmissionId, SubmissionRecord)>,
    submission_changes: Vec<(SubmissionId, SubmissionChange)>,
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
