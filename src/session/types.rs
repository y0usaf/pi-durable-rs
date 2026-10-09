use crate::cancel::CancelToken;
use crate::documents::DocDefinition;
use crate::errors::{Error, Result};
use crate::ids::{ConversationId, EntryId, SubmissionId, TaskId};
use crate::tasks::TaskOwnership;
use crate::types::{
    AnyTaskRecord, CommitPublication, ConversationOwnership, ConversationQuery, ConversationRecord,
    Cursor, EntryDraft, EntryQuery, EntryRecord, HeadMarker, JsonObject, JsonValue, Op, Page,
    SubmissionCreate, SubmissionRecord, SubmissionSettlement, TaskQuery,
};

#[derive(Clone, Copy)]
pub struct DocRef<'a> {
    pub definition: &'a DocDefinition,
    pub owner: Option<u64>,
    pub key: Option<&'a str>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DraftId(pub usize);

#[derive(Debug, Clone)]
pub enum WatchEnd {
    Stopped,
    Cancelled,
    SessionClosed,
    Retired,
    ListenerError(std::sync::Arc<Error>),
}

pub type WatchListener =
    Box<dyn FnMut(Option<&JsonObject>, &[Op], &CancelToken) -> Result<()> + Send>;

pub trait WatchHandle {
    fn value(&self) -> Option<JsonObject>;
    fn start(&mut self, listener: WatchListener) -> Result<()>;
    fn stop(&mut self) -> WatchEnd;
    fn closed(&self) -> Option<WatchEnd>;
}

pub type DocumentWatch = Box<dyn WatchHandle + Send>;

pub trait DocumentReader {
    fn snapshot(&self, doc: DocRef, cancel: &CancelToken) -> Result<Option<JsonObject>>;
    fn snapshot_as_of(
        &self,
        doc: DocRef,
        at: EntryId,
        cancel: &CancelToken,
    ) -> Result<Option<JsonObject>>;
}

pub trait DocumentObserver {
    fn watch_doc(&self, doc: DocRef, cancel: &CancelToken) -> Result<Option<DocumentWatch>>;
}

pub type CommitListener = Box<dyn Fn(&CommitPublication, &CancelToken) + Send + Sync>;
pub type Unsubscribe = Box<dyn FnOnce() + Send>;

#[derive(Debug, Clone, PartialEq)]
pub struct TaskOptions {
    pub ownership: TaskOwnership,
    pub conversation_id: Option<ConversationId>,
    pub background: Option<bool>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TaskCreate {
    pub kind: String,
    pub version: u64,
    pub input: JsonValue,
    pub checkpoint: JsonValue,
}

pub trait Tx {
    fn conversation(&mut self, id: ConversationId) -> Result<Option<ConversationRecord>>;
    fn entry(&mut self, id: EntryId) -> Result<Option<EntryRecord>>;
    fn task(&mut self, id: TaskId) -> Result<Option<AnyTaskRecord>>;
    fn submission(&mut self, id: SubmissionId) -> Result<Option<SubmissionRecord>>;
    fn scan_conversations(
        &mut self,
        query: &ConversationQuery,
        limit: usize,
        cursor: Option<&Cursor>,
    ) -> Result<Page<ConversationRecord>>;
    fn scan_entries(
        &mut self,
        query: &EntryQuery,
        limit: usize,
        cursor: Option<&Cursor>,
    ) -> Result<Page<EntryRecord>>;
    fn scan_tasks(
        &mut self,
        query: &TaskQuery,
        limit: usize,
        cursor: Option<&Cursor>,
    ) -> Result<Page<AnyTaskRecord>>;
    fn latest_head_marker(&mut self, conversation_id: ConversationId)
    -> Result<Option<HeadMarker>>;
    fn submission_by_request(
        &mut self,
        conversation_id: ConversationId,
        request_id: &str,
    ) -> Result<Option<SubmissionRecord>>;
    fn create_conversation(
        &mut self,
        ownership: ConversationOwnership,
    ) -> Result<ConversationRecord>;
    fn fork_conversation(
        &mut self,
        parent: ConversationId,
        at: EntryId,
        ownership: ConversationOwnership,
    ) -> Result<ConversationRecord>;
    fn append_entry(
        &mut self,
        conversation_id: ConversationId,
        draft: EntryDraft,
    ) -> Result<EntryRecord>;
    fn create_task(&mut self, task: TaskCreate, options: TaskOptions) -> Result<TaskId>;
    fn create_submission(&mut self, create: SubmissionCreate) -> Result<SubmissionRecord>;
    fn settle_submission(
        &mut self,
        id: SubmissionId,
        settlement: SubmissionSettlement,
    ) -> Result<()>;
    fn place_submission(&mut self, id: SubmissionId, entry: EntryId) -> Result<()>;
    fn doc(&mut self, doc: DocRef, seed: Option<JsonValue>) -> Result<DraftId>;
    fn draft(&mut self, draft: DraftId) -> Result<&mut JsonValue>;
    fn retire_doc(&mut self, doc: DocRef) -> Result<()>;
}
