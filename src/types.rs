use serde::{Deserialize, Deserializer, Serialize};

use crate::cancel::CancelToken;
use crate::errors::Result;
use crate::ids::{ConversationId, DocumentId, EntryId, Seq, SubmissionId, TaskId};
use crate::tasks::TaskRecord;

pub use crate::ai::types::Message;
pub use crate::chord::delta::Op;
pub use crate::chord::json::{JsonObject, JsonValue};
pub type Cursor = JsonObject;
pub type AnyTaskRecord = TaskRecord<JsonValue, JsonValue, JsonValue>;

pub(crate) fn absent_default<T>() -> Option<T> {
    None
}

pub(crate) fn present<'de, D: Deserializer<'de>, T: Deserialize<'de>>(
    deserializer: D,
) -> std::result::Result<Option<T>, D::Error> {
    T::deserialize(deserializer).map(Some)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LatestFork {
    Current,
    Initial,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RewindableFork {
    AsOf,
    Current,
    Initial,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "history", rename_all = "lowercase")]
pub enum ConversationSemantics {
    Latest { fork: LatestFork },
    Rewindable { fork: RewindableFork },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    rename_all = "lowercase",
    rename_all_fields = "camelCase"
)]
pub enum ScopeRef {
    Session,
    Conversation { conversation_id: ConversationId },
    Task { task_id: TaskId },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocumentPoint {
    Current,
    At(Seq),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocumentAddress {
    pub kind: String,
    pub scope: ScopeRef,
    pub key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentCreate {
    pub id: DocumentId,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    pub scope: ScopeRef,
    #[serde(flatten, skip_serializing_if = "Option::is_none")]
    pub semantics: Option<ConversationSemantics>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DocumentRecord {
    #[serde(flatten)]
    pub create: DocumentCreate,
    pub created_at: Seq,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retired_at: Option<Seq>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DocumentBase {
    pub version: u64,
    pub value: JsonObject,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocumentDelta {
    pub version: u64,
    pub ops: Vec<Op>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum DocumentContent {
    Base(DocumentBase),
    Delta(DocumentDelta),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocumentCopySource {
    pub id: DocumentId,
    pub at: DocumentPoint,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StoredDocument {
    pub record: DocumentRecord,
    pub version: u64,
    pub value: JsonObject,
    pub deltas_since_base: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConversationParent {
    pub conversation_id: ConversationId,
    pub at: EntryId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConversationOwner {
    pub conversation_id: ConversationId,
    pub task_id: TaskId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConversationRecord {
    pub id: ConversationId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<ConversationParent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<ConversationOwner>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConversationOwnership {
    Ownerless,
    Task(TaskId),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "lowercase")]
pub enum ContextAction {
    Omit,
    Replace { messages: Vec<Message> },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ContextEdit {
    pub target: EntryId,
    #[serde(flatten)]
    pub action: ContextAction,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EntryRecord {
    pub id: EntryId,
    pub conversation_id: ConversationId,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<Vec<Message>>,
    #[serde(
        default = "absent_default",
        deserialize_with = "present",
        skip_serializing_if = "Option::is_none"
    )]
    pub data: Option<JsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub head: Option<EntryId>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edits: Option<Vec<ContextEdit>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub by_task_id: Option<TaskId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryHead {
    SelfHead,
    Entry(EntryId),
}

#[derive(Debug, Clone, PartialEq)]
pub struct EntryDraft {
    pub kind: String,
    pub model: Option<Vec<Message>>,
    pub data: Option<JsonValue>,
    pub head: Option<EntryHead>,
    pub edits: Option<Vec<ContextEdit>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TypedEntry<D> {
    pub entry: EntryRecord,
    pub data: D,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CommittedEntry {
    pub entry: EntryRecord,
    pub commit_seq: Seq,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HeadMarker {
    pub entry: EntryRecord,
    pub head: EntryId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ToolDiagnosticSeverity {
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolDiagnostic {
    pub severity: ToolDiagnosticSeverity,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompactionReason {
    Manual,
    Threshold,
    Overflow,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "status",
    rename_all = "lowercase",
    rename_all_fields = "camelCase"
)]
pub enum InputState {
    Queued,
    Placed {
        entry: EntryId,
    },
    Done {
        entry: EntryId,
        answer: EntryId,
    },
    Unanswered {
        #[serde(skip_serializing_if = "Option::is_none")]
        entry: Option<EntryId>,
        reason: String,
        #[serde(
            default = "absent_default",
            deserialize_with = "present",
            skip_serializing_if = "Option::is_none"
        )]
        detail: Option<JsonValue>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "status",
    rename_all = "lowercase",
    rename_all_fields = "camelCase"
)]
pub enum WriteState {
    Queued,
    Done {
        entry: EntryId,
    },
    Unanswered {
        reason: String,
        #[serde(
            default = "absent_default",
            deserialize_with = "present",
            skip_serializing_if = "Option::is_none"
        )]
        detail: Option<JsonValue>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum SubmissionBody {
    Input(InputState),
    Write(WriteState),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SubmissionRecord {
    pub id: SubmissionId,
    pub conversation_id: ConversationId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    #[serde(flatten)]
    pub body: SubmissionBody,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmissionCreate {
    pub conversation_id: ConversationId,
    pub request_id: Option<String>,
    pub body: SubmissionBody,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubmissionSettlement {
    Done {
        answer: EntryId,
    },
    Unanswered {
        reason: String,
        detail: Option<JsonValue>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmissionStatus {
    Queued,
    Placed,
    Done,
    Unanswered,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanOrder {
    Ascending,
    Descending,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConversationQuery {
    pub owner_conversation_id: Option<ConversationId>,
    pub owner_task_id: Option<TaskId>,
    pub order: Option<ScanOrder>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryQuery {
    pub conversation_id: ConversationId,
    pub min_entry_id: Option<EntryId>,
    pub max_entry_id: Option<EntryId>,
    pub order: Option<ScanOrder>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TaskQuery {
    pub conversation_id: Option<ConversationId>,
    pub kind: Option<String>,
    pub status: Option<crate::tasks::TaskStatus>,
    pub abort_requested: Option<bool>,
    pub background: Option<bool>,
    pub order: Option<ScanOrder>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SubmissionQuery {
    pub conversation_id: Option<ConversationId>,
    pub status: Option<SubmissionStatus>,
    pub order: Option<ScanOrder>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DocumentQuery {
    pub scope: ScopeRef,
    pub at: DocumentPoint,
    pub kind: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next: Option<Cursor>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableChange {
    Conversation(ConversationRecord),
    Entry(EntryRecord),
    Task(Box<AnyTaskRecord>),
    Submission(SubmissionRecord),
}

#[derive(Debug, Clone, PartialEq)]
pub enum StorageWrite {
    Table(Box<TableChange>),
    DocumentCreate {
        record: DocumentCreate,
        content: DocumentBase,
    },
    DocumentCopy {
        record: DocumentCreate,
        source: DocumentCopySource,
    },
    DocumentChange {
        id: DocumentId,
        content: DocumentContent,
    },
    DocumentRetire {
        id: DocumentId,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum DocumentCommitChange {
    Document {
        record: DocumentRecord,
        conversation_id: Option<ConversationId>,
        version: Option<u64>,
        value: Option<JsonObject>,
        ops: Vec<Op>,
    },
    Copy {
        record: DocumentRecord,
        conversation_id: ConversationId,
        source: DocumentCopySource,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum CommitChange {
    Table(TableChange),
    Document(DocumentCommitChange),
}

#[derive(Debug, Clone, PartialEq)]
pub struct CommitPublication {
    pub seq: Seq,
    pub changes: Vec<CommitChange>,
}

pub trait Storage: Send + Sync {
    fn commit(&self, writes: &[StorageWrite], cancel: &CancelToken) -> Result<Seq>;
    fn mint_id(&self) -> Result<u64>;
    fn conversation(
        &self,
        id: ConversationId,
        cancel: &CancelToken,
    ) -> Result<Option<ConversationRecord>>;
    fn scan_conversations(
        &self,
        query: &ConversationQuery,
        limit: usize,
        cursor: Option<&Cursor>,
        cancel: &CancelToken,
    ) -> Result<Page<ConversationRecord>>;
    fn entry(&self, id: EntryId, cancel: &CancelToken) -> Result<Option<CommittedEntry>>;
    fn visible_entry(
        &self,
        conversation_id: ConversationId,
        id: EntryId,
        cancel: &CancelToken,
    ) -> Result<Option<CommittedEntry>>;
    fn find_latest_head_marker(
        &self,
        conversation_id: ConversationId,
        at_or_before: Option<EntryId>,
        cancel: &CancelToken,
    ) -> Result<Option<HeadMarker>>;
    fn scan_entries(
        &self,
        query: &EntryQuery,
        limit: usize,
        cursor: Option<&Cursor>,
        cancel: &CancelToken,
    ) -> Result<Page<EntryRecord>>;
    fn task(&self, id: TaskId, cancel: &CancelToken) -> Result<Option<AnyTaskRecord>>;
    fn scan_tasks(
        &self,
        query: &TaskQuery,
        limit: usize,
        cursor: Option<&Cursor>,
        cancel: &CancelToken,
    ) -> Result<Page<AnyTaskRecord>>;
    fn submission(
        &self,
        id: SubmissionId,
        cancel: &CancelToken,
    ) -> Result<Option<SubmissionRecord>>;
    fn scan_submissions(
        &self,
        query: &SubmissionQuery,
        limit: usize,
        cursor: Option<&Cursor>,
        cancel: &CancelToken,
    ) -> Result<Page<SubmissionRecord>>;
    fn submission_by_request(
        &self,
        conversation_id: ConversationId,
        request_id: &str,
        cancel: &CancelToken,
    ) -> Result<Option<SubmissionRecord>>;
    fn find_document(
        &self,
        address: &DocumentAddress,
        at: DocumentPoint,
        cancel: &CancelToken,
    ) -> Result<Option<DocumentRecord>>;
    fn document(
        &self,
        id: DocumentId,
        at: DocumentPoint,
        cancel: &CancelToken,
    ) -> Result<Option<StoredDocument>>;
    fn scan_documents(
        &self,
        query: &DocumentQuery,
        limit: usize,
        cursor: Option<&Cursor>,
        cancel: &CancelToken,
    ) -> Result<Page<DocumentRecord>>;
    fn close(&self, cancel: &CancelToken) -> Result<()>;
}
