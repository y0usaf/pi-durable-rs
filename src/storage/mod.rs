pub mod jsonl;
pub mod memory;
pub mod scan;
pub mod sqlite;

use crate::chord::delta::{Op, apply};
use crate::errors::{Error, Result};
use crate::types::{
    ConversationSemantics, DocumentAddress, DocumentCreate, DocumentPoint, DocumentRecord,
    InputState, JsonObject, JsonValue, ScopeRef, SubmissionBody, SubmissionRecord,
    SubmissionStatus, WriteState,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Table {
    Conversation,
    Entry,
    Task,
    Submission,
    Document,
}

impl Table {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Table::Conversation => "conversation",
            Table::Entry => "entry",
            Table::Task => "task",
            Table::Submission => "submission",
            Table::Document => "document",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum ScopeKey {
    Session,
    Conversation(u64),
    Task(u64),
}

impl From<&ScopeRef> for ScopeKey {
    fn from(scope: &ScopeRef) -> Self {
        match scope {
            ScopeRef::Session => ScopeKey::Session,
            ScopeRef::Conversation { conversation_id } => ScopeKey::Conversation(conversation_id.0),
            ScopeRef::Task { task_id } => ScopeKey::Task(task_id.get()),
        }
    }
}

pub(crate) type AddressKey = (String, ScopeKey, Option<String>);

pub(crate) fn address_key(create: &DocumentCreate) -> AddressKey {
    (
        create.kind.clone(),
        ScopeKey::from(&create.scope),
        create.key.clone(),
    )
}

pub(crate) fn lookup_key(address: &DocumentAddress) -> AddressKey {
    (
        address.kind.clone(),
        ScopeKey::from(&address.scope),
        address.key.clone(),
    )
}

pub(crate) fn is_current_only(create: &DocumentCreate) -> bool {
    match create.scope {
        ScopeRef::Conversation { .. } => {
            matches!(create.semantics, Some(ConversationSemantics::Latest { .. }))
        }
        ScopeRef::Session | ScopeRef::Task { .. } => true,
    }
}

pub(crate) fn is_alive_at(record: &DocumentRecord, at: DocumentPoint) -> bool {
    match at {
        DocumentPoint::Current => record.retired_at.is_none(),
        DocumentPoint::At(seq) => {
            record.created_at.0 <= seq.0
                && record.retired_at.is_none_or(|retired| seq.0 < retired.0)
        }
    }
}

pub(crate) fn submission_status(record: &SubmissionRecord) -> SubmissionStatus {
    match &record.body {
        SubmissionBody::Input(InputState::Queued) | SubmissionBody::Write(WriteState::Queued) => {
            SubmissionStatus::Queued
        }
        SubmissionBody::Input(InputState::Placed { .. }) => SubmissionStatus::Placed,
        SubmissionBody::Input(InputState::Done { .. })
        | SubmissionBody::Write(WriteState::Done { .. }) => SubmissionStatus::Done,
        SubmissionBody::Input(InputState::Unanswered { .. })
        | SubmissionBody::Write(WriteState::Unanswered { .. }) => SubmissionStatus::Unanswered,
    }
}

pub(crate) fn apply_ops(value: JsonObject, ops: &[Op]) -> Result<JsonObject> {
    match apply(Some(JsonValue::Object(value)), ops) {
        Ok(JsonValue::Object(object)) => Ok(object),
        Ok(_) => Err(Error::Invalid("Document value is not an object".to_owned())),
        Err(error) => Err(Error::Invalid(error.to_string())),
    }
}
