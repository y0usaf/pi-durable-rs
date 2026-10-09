mod database;
pub mod migrations;

use std::collections::HashMap;
use std::fmt::Display;
use std::path::Path;
use std::sync::{Mutex, MutexGuard};

use rusqlite::types::Value;
use rusqlite::{Connection, Row, params, params_from_iter};
use serde::Serialize;
use serde::de::DeserializeOwned;

use self::database::{all, get, invalid, run, transaction};
use self::migrations::{SQLITE_MIGRATIONS, apply_migrations};
use crate::cancel::CancelToken;
use crate::chord::delta::Op;
use crate::documents::MAX_SAFE_INTEGER;
use crate::errors::{Error, Result};
use crate::ids::{ConversationId, DocumentId, EntryId, Seq, SubmissionId, TaskId};
use crate::storage::scan::{ScanStart, cursor_after, page, scan_start};
use crate::storage::{
    AddressKey, Table, address_key, apply_ops, is_alive_at, is_current_only, submission_status,
};
use crate::tasks::TaskStatus;
use crate::types::{
    AnyTaskRecord, CommittedEntry, ConversationQuery, ConversationRecord, Cursor, DocumentAddress,
    DocumentBase, DocumentContent, DocumentCopySource, DocumentCreate, DocumentPoint,
    DocumentQuery, DocumentRecord, EntryQuery, EntryRecord, HeadMarker, JsonObject, Page,
    ScanOrder, ScopeRef, Storage, StorageWrite, StoredDocument, SubmissionQuery, SubmissionRecord,
    SubmissionStatus, TableChange, TaskQuery,
};

pub use database::OpenOptions;

#[derive(Default)]
struct DocumentAction {
    create: Option<DocumentCreate>,
    content: Option<DocumentContent>,
    copy: Option<DocumentCopySource>,
    retire: bool,
}

impl DocumentAction {
    fn has_content(&self) -> bool {
        self.create.is_some() || self.content.is_some() || self.copy.is_some()
    }
}

struct Revision {
    seq: u64,
    kind: String,
    version: u64,
    content: String,
}

struct Parts {
    kind: String,
    scope_kind: &'static str,
    owner_id: u64,
    family: i64,
    key_value: String,
}

struct Filter {
    clauses: Vec<String>,
    params: Vec<Value>,
}

impl Filter {
    fn new(clause: &str, value: Value) -> Self {
        Self {
            clauses: vec![clause.to_owned()],
            params: vec![value],
        }
    }

    fn with(mut self, clause: &str, value: Value) -> Self {
        self.clauses.push(clause.to_owned());
        self.params.push(value);
        self
    }

    fn bare(mut self, clause: &str) -> Self {
        self.clauses.push(clause.to_owned());
        self
    }
}

#[derive(Debug)]
struct State {
    connection: Option<Connection>,
    next_id: u64,
}

#[derive(Debug)]
pub struct SqliteStorage {
    state: Mutex<State>,
}

fn closed() -> Error {
    invalid("SqliteStorage is closed")
}

fn unknown_conversation(id: ConversationId) -> Error {
    invalid(format!("Unknown conversation: {id}"))
}

fn bound<T: TryInto<i64>>(value: T) -> Result<i64>
where
    T::Error: Display,
{
    value.try_into().map_err(invalid)
}

fn int<T: TryInto<i64>>(value: T) -> Result<Value>
where
    T::Error: Display,
{
    bound(value).map(Value::Integer)
}

fn encode<T: Serialize + ?Sized>(value: &T) -> Result<String> {
    serde_json::to_string(value).map_err(Error::from)
}

fn decode<T: DeserializeOwned>(text: &str) -> Result<T> {
    serde_json::from_str(text).map_err(Error::from)
}

fn indexed(value: &str) -> Result<String> {
    encode(value)
}

fn scope_columns(scope: &ScopeRef) -> (&'static str, u64) {
    match scope {
        ScopeRef::Session => ("session", 0),
        ScopeRef::Conversation { conversation_id } => ("conversation", conversation_id.0),
        ScopeRef::Task { task_id } => ("task", task_id.get()),
    }
}

fn parts(kind: &str, scope: &ScopeRef, key: Option<&str>) -> Result<Parts> {
    let (scope_kind, owner_id) = scope_columns(scope);
    Ok(Parts {
        kind: indexed(kind)?,
        scope_kind,
        owner_id,
        family: i64::from(key.is_some()),
        key_value: indexed(key.unwrap_or(""))?,
    })
}

fn address_values(parts: Parts) -> Result<Vec<Value>> {
    Ok(vec![
        Value::Text(parts.kind),
        Value::Text(parts.scope_kind.to_owned()),
        int(parts.owner_id)?,
        Value::Integer(parts.family),
        Value::Text(parts.key_value),
    ])
}

fn task_status_name(status: TaskStatus) -> &'static str {
    match status {
        TaskStatus::Pending => "pending",
        TaskStatus::Running => "running",
        TaskStatus::Waiting => "waiting",
        TaskStatus::Completing => "completing",
        TaskStatus::Terminal => "terminal",
    }
}

fn submission_status_name(status: SubmissionStatus) -> &'static str {
    match status {
        SubmissionStatus::Queued => "queued",
        SubmissionStatus::Placed => "placed",
        SubmissionStatus::Done => "done",
        SubmissionStatus::Unanswered => "unanswered",
    }
}

fn revision(row: &Row<'_>) -> rusqlite::Result<Revision> {
    Ok(Revision {
        seq: row.get(0)?,
        kind: row.get(1)?,
        version: row.get(2)?,
        content: row.get(3)?,
    })
}

fn record_at<T: DeserializeOwned>(
    connection: &Connection,
    sql: &str,
    params: impl rusqlite::Params,
) -> Result<Option<T>> {
    match get(connection, sql, params, |row| row.get::<_, String>(0))? {
        Some(text) => decode(&text).map(Some),
        None => Ok(None),
    }
}

fn records<T: DeserializeOwned>(
    connection: &Connection,
    sql: &str,
    params: impl rusqlite::Params,
) -> Result<Vec<T>> {
    all(connection, sql, params, |row| row.get::<_, String>(0))?
        .iter()
        .map(|text| decode(text))
        .collect()
}

fn scan<T: DeserializeOwned>(
    connection: &Connection,
    table: &str,
    filter: Filter,
    direction: &str,
    fetch: usize,
) -> Result<Vec<T>> {
    let Filter {
        clauses,
        mut params,
    } = filter;
    params.push(int(fetch)?);
    let sql = format!(
        "SELECT record FROM {table} WHERE {} ORDER BY id {direction} LIMIT ?",
        clauses.join(" AND ")
    );
    records(connection, &sql, params_from_iter(params))
}

fn ordered(start: ScanStart) -> Result<(Filter, &'static str)> {
    let (clause, bound, direction) = match (start.order, start.after) {
        (ScanOrder::Ascending, Some(after)) => ("id > ?", int(after)?, "ASC"),
        (ScanOrder::Ascending, None) => ("id > ?", Value::Integer(-1), "ASC"),
        (ScanOrder::Descending, Some(after)) => ("id < ?", int(after)?, "DESC"),
        (ScanOrder::Descending, None) => ("id < ?", int(MAX_SAFE_INTEGER)?, "DESC"),
    };
    Ok((Filter::new(clause, bound), direction))
}

fn metadata(connection: &Connection) -> Result<(u64, u64)> {
    let (next_id, next_seq) = get(
        connection,
        "SELECT next_id, next_seq FROM durable_metadata WHERE singleton = 1",
        params![],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?)),
    )?
    .ok_or_else(|| invalid("Durable SQLite metadata is missing"))?;
    Ok((next_id.parse::<u64>().map_err(invalid)?, next_seq))
}

fn conversation_at(
    connection: &Connection,
    id: ConversationId,
) -> Result<Option<ConversationRecord>> {
    record_at(
        connection,
        "SELECT record FROM conversations WHERE id = ?",
        params![id.0],
    )
}

fn required_conversation(
    connection: &Connection,
    id: ConversationId,
) -> Result<ConversationRecord> {
    conversation_at(connection, id)?.ok_or_else(|| unknown_conversation(id))
}

fn entry_row(connection: &Connection, id: EntryId) -> Result<Option<CommittedEntry>> {
    let row = get(
        connection,
        "SELECT record, commit_seq FROM entries WHERE id = ?",
        params![id.0],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, u64>(1)?)),
    )?;
    match row {
        Some((text, commit_seq)) => Ok(Some(CommittedEntry {
            entry: decode(&text)?,
            commit_seq: Seq(commit_seq),
        })),
        None => Ok(None),
    }
}

fn visible_entry_at(
    connection: &Connection,
    conversation_id: ConversationId,
    id: EntryId,
) -> Result<Option<CommittedEntry>> {
    let mut conversation = required_conversation(connection, conversation_id)?;
    let Some(committed) = entry_row(connection, id)? else {
        return Ok(None);
    };
    let mut upper = u64::MAX;
    while conversation.id != committed.entry.conversation_id {
        let Some(parent) = conversation.parent.clone() else {
            return Ok(None);
        };
        upper = upper.min(parent.at.0);
        conversation = required_conversation(connection, parent.conversation_id)?;
    }
    if committed.entry.id.0 > upper {
        return Ok(None);
    }
    Ok(Some(committed))
}

fn latest_head_marker(
    connection: &Connection,
    conversation_id: ConversationId,
    at_or_before: Option<EntryId>,
) -> Result<Option<HeadMarker>> {
    let mut conversation = required_conversation(connection, conversation_id)?;
    let mut upper = at_or_before.map(|id| bound(id.0)).transpose()?;
    loop {
        let entry: Option<EntryRecord> = match upper {
            None => record_at(
                connection,
                "SELECT record FROM entries WHERE conversation_id = ? AND head IS NOT NULL ORDER BY id DESC LIMIT 1",
                params![conversation.id.0],
            )?,
            Some(upper) => record_at(
                connection,
                "SELECT record FROM entries WHERE conversation_id = ? AND head IS NOT NULL AND id <= ? ORDER BY id DESC LIMIT 1",
                params![conversation.id.0, upper],
            )?,
        };
        if let Some(entry) = entry {
            let head = entry
                .head
                .ok_or_else(|| invalid("Head marker entry has no head"))?;
            return Ok(Some(HeadMarker { entry, head }));
        }
        let Some(parent) = conversation.parent.clone() else {
            return Ok(None);
        };
        let parent_at = bound(parent.at.0)?;
        upper = Some(upper.map_or(parent_at, |upper| upper.min(parent_at)));
        conversation = required_conversation(connection, parent.conversation_id)?;
    }
}

fn entries_descending(
    connection: &Connection,
    query: &EntryQuery,
    limit: usize,
    after: Option<u64>,
) -> Result<Page<EntryRecord>> {
    let min = query.min_entry_id.map(|id| bound(id.0)).transpose()?;
    let mut conversation = required_conversation(connection, query.conversation_id)?;
    let mut upper = query.max_entry_id.map(|id| bound(id.0)).transpose()?;
    if let Some(after) = after {
        let before = bound(after)? - 1;
        upper = Some(upper.unwrap_or(bound(MAX_SAFE_INTEGER)?).min(before));
    }
    let mut values: Vec<EntryRecord> = Vec::new();
    loop {
        let mut filter = Filter::new("conversation_id = ?", int(conversation.id.0)?);
        if let Some(min) = min {
            filter = filter.with("id >= ?", Value::Integer(min));
        }
        if let Some(upper) = upper {
            filter = filter.with("id <= ?", Value::Integer(upper));
        }
        let fetch = (limit + 1).saturating_sub(values.len());
        values.extend(scan::<EntryRecord>(
            connection, "entries", filter, "DESC", fetch,
        )?);
        let parent = match conversation.parent.clone() {
            Some(parent) if values.len() <= limit => parent,
            _ => break,
        };
        let parent_at = bound(parent.at.0)?;
        let next = upper.map_or(parent_at, |upper| upper.min(parent_at));
        upper = Some(next);
        if min.is_some_and(|min| next < min) {
            break;
        }
        conversation = required_conversation(connection, parent.conversation_id)?;
    }
    page(values, limit, ScanOrder::Descending, |entry| entry.id.0)
}

fn entries_ascending(
    connection: &Connection,
    query: &EntryQuery,
    limit: usize,
    after: Option<u64>,
) -> Result<Page<EntryRecord>> {
    let min = query.min_entry_id.map(|id| bound(id.0)).transpose()?;
    let mut segments: Vec<(ConversationId, Option<i64>)> = Vec::new();
    let mut conversation = required_conversation(connection, query.conversation_id)?;
    let mut upper = query.max_entry_id.map(|id| bound(id.0)).transpose()?;
    loop {
        segments.push((conversation.id, upper));
        let Some(parent) = conversation.parent.clone() else {
            break;
        };
        let parent_at = bound(parent.at.0)?;
        let next = upper.map_or(parent_at, |upper| upper.min(parent_at));
        upper = Some(next);
        if min.is_some_and(|min| next < min) {
            break;
        }
        conversation = required_conversation(connection, parent.conversation_id)?;
    }
    let mut lower = min;
    if let Some(after) = after {
        let after_next = bound(after)? + 1;
        lower = Some(lower.map_or(after_next, |lower| lower.max(after_next)));
    }
    let mut values: Vec<EntryRecord> = Vec::new();
    for (segment_conversation, segment_upper) in segments.into_iter().rev() {
        let mut filter = Filter::new("conversation_id = ?", int(segment_conversation.0)?);
        if let Some(lower) = lower {
            filter = filter.with("id >= ?", Value::Integer(lower));
        }
        if let Some(segment_upper) = segment_upper {
            filter = filter.with("id <= ?", Value::Integer(segment_upper));
        }
        let fetch = (limit + 1).saturating_sub(values.len());
        values.extend(scan::<EntryRecord>(
            connection, "entries", filter, "ASC", fetch,
        )?);
        if values.len() > limit {
            break;
        }
    }
    page(values, limit, ScanOrder::Ascending, |entry| entry.id.0)
}

fn materialize(
    connection: &Connection,
    id: DocumentId,
    at: DocumentPoint,
) -> Result<Option<StoredDocument>> {
    let Some(record) = record_at::<DocumentRecord>(
        connection,
        "SELECT record FROM documents WHERE id = ?",
        params![id.0],
    )?
    else {
        return Ok(None);
    };
    if at != DocumentPoint::Current && is_current_only(&record.create) {
        return Err(invalid(format!(
            "Document {id} does not retain historical content"
        )));
    }
    if !is_alive_at(&record, at) {
        return Ok(None);
    }
    let upper = match at {
        DocumentPoint::Current => MAX_SAFE_INTEGER,
        DocumentPoint::At(seq) => seq.0,
    };
    let base = get(
        connection,
        "SELECT seq, kind, version, content FROM document_revisions WHERE document_id = ? AND kind = 'base' AND seq <= ? ORDER BY seq DESC LIMIT 1",
        params![id.0, upper],
        revision,
    )?
    .ok_or_else(|| invalid(format!("Document {id} is missing a required base")))?;
    let mut value: JsonObject = decode(&base.content)?;
    let tail = all(
        connection,
        "SELECT seq, kind, version, content FROM document_revisions WHERE document_id = ? AND seq > ? AND seq <= ? ORDER BY seq",
        params![id.0, base.seq, upper],
        revision,
    )?;
    for change in &tail {
        if change.kind != "delta" || change.version != base.version {
            return Err(invalid(format!(
                "Document {id} crosses a stored version boundary without a base"
            )));
        }
        let ops: Vec<Op> = decode(&change.content)?;
        value = apply_ops(value, &ops)?;
    }
    Ok(Some(StoredDocument {
        record,
        version: base.version,
        value,
        deltas_since_base: u64::try_from(tail.len()).map_err(invalid)?,
    }))
}

fn current_document_id(connection: &Connection, address: &DocumentCreate) -> Result<Option<u64>> {
    let values = address_values(parts(
        &address.kind,
        &address.scope,
        address.key.as_deref(),
    )?)?;
    get(
        connection,
        "SELECT id FROM documents WHERE kind = ? AND scope_kind = ? AND owner_id = ? AND family = ? AND key_value = ? AND retired_at IS NULL LIMIT 1",
        params_from_iter(values),
        |row| row.get::<_, u64>(0),
    )
}

fn claim_id(connection: &Connection, id: u64, table: Table) -> Result<()> {
    run(
        connection,
        "INSERT OR IGNORE INTO record_ids (id, record_type) VALUES (?, ?)",
        params![id, table.name()],
    )
}

fn copy_matches(source: &DocumentCreate, target: &DocumentCreate) -> bool {
    matches!(source.scope, ScopeRef::Conversation { .. })
        && matches!(target.scope, ScopeRef::Conversation { .. })
        && source.kind == target.kind
        && source.key == target.key
        && source.semantics == target.semantics
}

fn copied_base(
    connection: &Connection,
    source: &DocumentCopySource,
    create: &DocumentCreate,
) -> Result<DocumentContent> {
    let stored = materialize(connection, source.id, source.at)?
        .ok_or_else(|| invalid(format!("Fork source document {} cannot be read", source.id)))?;
    if !copy_matches(&stored.record.create, create) {
        return Err(invalid(format!(
            "Fork source document {} does not match the copied record",
            source.id
        )));
    }
    Ok(DocumentContent::Base(DocumentBase {
        version: stored.version,
        value: stored.value,
    }))
}

fn copy_content(
    connection: &Connection,
    id: DocumentId,
    source: &DocumentCopySource,
    create: &DocumentCreate,
) -> Result<DocumentContent> {
    copied_base(connection, source, create).map_err(|error| match error {
        rejected @ Error::StorageRejected { .. } => rejected,
        other => Error::StorageRejected {
            message: format!("Document copy {id} was rejected"),
            cause: Some(Box::new(other)),
        },
    })
}

fn prepare_document_actions(writes: &[StorageWrite]) -> Result<Vec<(DocumentId, DocumentAction)>> {
    let mut actions: Vec<(DocumentId, DocumentAction)> = Vec::new();
    for write in writes {
        let id = match write {
            StorageWrite::DocumentCreate { record, .. }
            | StorageWrite::DocumentCopy { record, .. } => record.id,
            StorageWrite::DocumentChange { id, .. } | StorageWrite::DocumentRetire { id } => *id,
            StorageWrite::Table(_) => continue,
        };
        let index = match actions.iter().position(|(existing, _)| *existing == id) {
            Some(index) => index,
            None => {
                actions.push((id, DocumentAction::default()));
                actions.len() - 1
            }
        };
        let action = &mut actions[index].1;
        match write {
            StorageWrite::DocumentCreate { record, content } => {
                if action.has_content() {
                    return Err(invalid(format!(
                        "Document {id} has more than one content command"
                    )));
                }
                action.create = Some(record.clone());
                action.content = Some(DocumentContent::Base(content.clone()));
            }
            StorageWrite::DocumentCopy { record, source } => {
                if action.has_content() {
                    return Err(invalid(format!(
                        "Document {id} has more than one content command"
                    )));
                }
                action.create = Some(record.clone());
                action.copy = Some(source.clone());
            }
            StorageWrite::DocumentChange { content, .. } => {
                if action.content.is_some() || action.copy.is_some() {
                    return Err(invalid(format!(
                        "Document {id} has more than one content command"
                    )));
                }
                action.content = Some(content.clone());
            }
            StorageWrite::DocumentRetire { .. } => {
                if action.retire {
                    return Err(invalid(format!("Document {id} is retired more than once")));
                }
                action.retire = true;
            }
            StorageWrite::Table(_) => {}
        }
    }
    Ok(actions)
}

fn write_id(write: &StorageWrite) -> Option<u64> {
    match write {
        StorageWrite::Table(change) => Some(match change.as_ref() {
            TableChange::Conversation(record) => record.id.0,
            TableChange::Entry(record) => record.id.0,
            TableChange::Task(record) => record.id.get(),
            TableChange::Submission(record) => record.id.0,
        }),
        StorageWrite::DocumentCreate { record, .. } | StorageWrite::DocumentCopy { record, .. } => {
            Some(record.id.0)
        }
        StorageWrite::DocumentChange { .. } | StorageWrite::DocumentRetire { .. } => None,
    }
}

fn candidate_next_id(next_id: u64, writes: &[StorageWrite]) -> u64 {
    writes
        .iter()
        .filter_map(write_id)
        .fold(next_id, |next_id, id| next_id.max(id.saturating_add(1)))
}

fn check_global_ids(connection: &Connection, writes: &[StorageWrite]) -> Result<()> {
    let mut claimed: HashMap<u64, Table> = HashMap::new();
    for write in writes {
        let (table, id) = match write {
            StorageWrite::DocumentChange { .. } | StorageWrite::DocumentRetire { .. } => continue,
            StorageWrite::DocumentCreate { record, .. }
            | StorageWrite::DocumentCopy { record, .. } => (Table::Document, record.id.0),
            StorageWrite::Table(change) => match change.as_ref() {
                TableChange::Conversation(record) => (Table::Conversation, record.id.0),
                TableChange::Entry(record) => (Table::Entry, record.id.0),
                TableChange::Task(record) => (Table::Task, record.id.get()),
                TableChange::Submission(record) => (Table::Submission, record.id.0),
            },
        };
        let existing: Option<String> = get(
            connection,
            "SELECT record_type FROM record_ids WHERE id = ?",
            params![id],
            |row| row.get(0),
        )?;
        let earlier = claimed.get(&id).copied();
        match table {
            Table::Conversation | Table::Entry | Table::Document => {
                if let Some(existing) = existing {
                    return Err(invalid(format!("ID {id} already belongs to {existing}")));
                }
                if earlier.is_some() {
                    return Err(invalid(format!("ID {id} is written more than once")));
                }
            }
            Table::Task | Table::Submission => {
                if let Some(existing) = existing.filter(|name| name.as_str() != table.name()) {
                    return Err(invalid(format!("ID {id} already belongs to {existing}")));
                }
                if earlier.is_some_and(|earlier| earlier != table) {
                    return Err(invalid(format!("ID {id} is written as two record types")));
                }
            }
        }
        claimed.insert(id, table);
    }
    Ok(())
}

fn check_document_actions(
    connection: &Connection,
    actions: &[(DocumentId, DocumentAction)],
) -> Result<()> {
    let mut live_counts: HashMap<AddressKey, i64> = HashMap::new();
    for (id, action) in actions {
        if let Some(source) = &action.copy
            && actions.iter().any(|(other, _)| *other == source.id)
        {
            return Err(Error::StorageRejected {
                message: format!("Document copy {id} source is changed in the copy batch"),
                cause: None,
            });
        }
        let existing: Option<DocumentRecord> = record_at(
            connection,
            "SELECT record FROM documents WHERE id = ?",
            params![id.0],
        )?;
        let address = match (&action.create, &existing) {
            (Some(create), _) => create,
            (None, Some(record)) => &record.create,
            (None, None) => return Err(invalid(format!("Unknown document: {id}"))),
        };
        if action.create.is_some() && existing.is_some() {
            return Err(invalid(format!("Document {id} already exists")));
        }
        if existing
            .as_ref()
            .is_some_and(|record| record.retired_at.is_some())
        {
            return Err(invalid(format!("Document {id} is retired")));
        }
        if let Some(DocumentContent::Delta(delta)) = &action.content {
            let previous: Option<u64> = get(
                connection,
                "SELECT version FROM document_revisions WHERE document_id = ? ORDER BY seq DESC LIMIT 1",
                params![id.0],
                |row| row.get(0),
            )?;
            let previous =
                previous.ok_or_else(|| invalid(format!("Document {id} delta has no base")))?;
            if previous != delta.version {
                return Err(invalid(format!(
                    "Document {id} version transition requires a base"
                )));
            }
        }
        let key = address_key(address);
        let mut live = match live_counts.get(&key) {
            Some(live) => *live,
            None => i64::from(current_document_id(connection, address)?.is_some()),
        };
        if action.retire && existing.is_some() {
            live -= 1;
        }
        if action.create.is_some() && !action.retire {
            live += 1;
        }
        live_counts.insert(key, live);
    }
    if live_counts.values().any(|live| *live > 1) {
        return Err(invalid(
            "Document address already has a current incarnation",
        ));
    }
    Ok(())
}

fn apply_table_write(connection: &Connection, change: &TableChange, seq: Seq) -> Result<()> {
    match change {
        TableChange::Conversation(record) => {
            claim_id(connection, record.id.0, Table::Conversation)?;
            run(
                connection,
                "INSERT INTO conversations (id, owner_conversation_id, owner_task_id, record) VALUES (?, ?, ?, ?)",
                params![
                    record.id.0,
                    record.owner.as_ref().map(|owner| owner.conversation_id.0),
                    record.owner.as_ref().map(|owner| owner.task_id.get()),
                    encode(record)?,
                ],
            )
        }
        TableChange::Entry(record) => {
            claim_id(connection, record.id.0, Table::Entry)?;
            run(
                connection,
                "INSERT INTO entries (id, conversation_id, head, commit_seq, record) VALUES (?, ?, ?, ?, ?)",
                params![
                    record.id.0,
                    record.conversation_id.0,
                    record.head.map(|head| head.0),
                    seq.0,
                    encode(record)?,
                ],
            )
        }
        TableChange::Task(record) => {
            claim_id(connection, record.id.get(), Table::Task)?;
            run(
                connection,
                "INSERT INTO tasks (id, conversation_id, kind, status, abort_requested, background, record)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET conversation_id = excluded.conversation_id, kind = excluded.kind,
                    status = excluded.status, abort_requested = excluded.abort_requested,
                    background = excluded.background, record = excluded.record",
                params![
                    record.id.get(),
                    record.conversation_id.0,
                    indexed(&record.kind)?,
                    task_status_name(record.status()),
                    i64::from(record.abort_requested),
                    i64::from(record.background),
                    encode(&**record)?,
                ],
            )
        }
        TableChange::Submission(record) => {
            claim_id(connection, record.id.0, Table::Submission)?;
            run(
                connection,
                "INSERT INTO submissions (id, conversation_id, request_id, status, record) VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(id) DO UPDATE SET conversation_id = excluded.conversation_id,
                    request_id = excluded.request_id, status = excluded.status, record = excluded.record",
                params![
                    record.id.0,
                    record.conversation_id.0,
                    record.request_id.as_deref().map(indexed).transpose()?,
                    submission_status_name(submission_status(record)),
                    encode(record)?,
                ],
            )
        }
    }
}

fn apply_document_actions(
    connection: &Connection,
    actions: &[(DocumentId, DocumentAction)],
    seq: Seq,
) -> Result<()> {
    for (id, action) in actions {
        let mut content = action.content.clone();
        if let (Some(source), Some(create)) = (&action.copy, &action.create) {
            content = Some(copy_content(connection, *id, source, create)?);
        }
        let mut record = match &action.create {
            Some(create) => {
                let record = DocumentRecord {
                    create: create.clone(),
                    created_at: seq,
                    retired_at: action.retire.then_some(seq),
                };
                claim_id(connection, id.0, Table::Document)?;
                let parts = parts(&create.kind, &create.scope, create.key.as_deref())?;
                let retired_at = if action.retire {
                    Value::Integer(bound(seq.0)?)
                } else {
                    Value::Null
                };
                run(
                    connection,
                    "INSERT INTO documents
                        (id, kind, family, key_value, scope_kind, owner_id, created_at, retired_at, record)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    params_from_iter(vec![
                        int(id.0)?,
                        Value::Text(parts.kind),
                        Value::Integer(parts.family),
                        Value::Text(parts.key_value),
                        Value::Text(parts.scope_kind.to_owned()),
                        int(parts.owner_id)?,
                        int(seq.0)?,
                        retired_at,
                        Value::Text(encode(&record)?),
                    ]),
                )?;
                record
            }
            None => record_at(
                connection,
                "SELECT record FROM documents WHERE id = ?",
                params![id.0],
            )?
            .ok_or_else(|| invalid(format!("Unknown document: {id}")))?,
        };
        if let Some(content) = content {
            if matches!(content, DocumentContent::Base(_)) && is_current_only(&record.create) {
                run(
                    connection,
                    "DELETE FROM document_revisions WHERE document_id = ?",
                    params![id.0],
                )?;
            }
            let (kind, version, text) = match &content {
                DocumentContent::Base(base) => ("base", base.version, encode(&base.value)?),
                DocumentContent::Delta(delta) => ("delta", delta.version, encode(&delta.ops)?),
            };
            run(
                connection,
                "INSERT INTO document_revisions (document_id, seq, kind, version, content) VALUES (?, ?, ?, ?, ?)",
                params![id.0, seq.0, kind, version, text],
            )?;
        }
        if action.retire {
            if action.create.is_none() {
                record.retired_at = Some(seq);
                run(
                    connection,
                    "UPDATE documents SET retired_at = ?, record = ? WHERE id = ?",
                    params![seq.0, encode(&record)?, id.0],
                )?;
            }
            if is_current_only(&record.create) {
                run(
                    connection,
                    "DELETE FROM document_revisions WHERE document_id = ?",
                    params![id.0],
                )?;
            }
        }
    }
    Ok(())
}

fn initialize(connection: &mut Connection) -> Result<u64> {
    apply_migrations(connection, SQLITE_MIGRATIONS)?;
    let (next_id, _) = metadata(connection)?;
    Ok(next_id)
}

impl SqliteStorage {
    pub fn open(path: impl AsRef<Path>, options: &OpenOptions) -> Result<Self> {
        let mut connection = database::open(path.as_ref(), options)?;
        match initialize(&mut connection) {
            Ok(next_id) => Ok(Self {
                state: Mutex::new(State {
                    connection: Some(connection),
                    next_id,
                }),
            }),
            Err(error) => {
                let _ = database::close(connection);
                Err(error)
            }
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().expect("sqlite storage lock poisoned")
    }

    fn read<T>(&self, operation: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        let state = self.lock();
        operation(state.connection.as_ref().ok_or_else(closed)?)
    }
}

impl Storage for SqliteStorage {
    fn commit(&self, writes: &[StorageWrite], _cancel: &CancelToken) -> Result<Seq> {
        let mut guard = self.lock();
        let state = &mut *guard;
        let connection = state.connection.as_mut().ok_or_else(closed)?;
        let actions = prepare_document_actions(writes)?;
        let candidate = candidate_next_id(state.next_id, writes);
        let seq = transaction(connection, |tx| {
            let (next_id, next_seq) = metadata(tx)?;
            let seq = Seq(next_seq);
            check_global_ids(tx, writes)?;
            check_document_actions(tx, &actions)?;
            for write in writes {
                if let StorageWrite::Table(change) = write {
                    apply_table_write(tx, change, seq)?;
                }
            }
            apply_document_actions(tx, &actions, seq)?;
            run(
                tx,
                "UPDATE durable_metadata SET next_id = ?, next_seq = ? WHERE singleton = 1",
                params![next_id.max(candidate).to_string(), next_seq + 1],
            )?;
            Ok(seq)
        })?;
        state.next_id = state.next_id.max(candidate);
        Ok(seq)
    }

    fn mint_id(&self) -> Result<u64> {
        let mut state = self.lock();
        state.connection.as_ref().ok_or_else(closed)?;
        if state.next_id > MAX_SAFE_INTEGER {
            return Err(invalid("ID space is exhausted"));
        }
        let id = state.next_id;
        state.next_id += 1;
        Ok(id)
    }

    fn conversation(
        &self,
        id: ConversationId,
        _cancel: &CancelToken,
    ) -> Result<Option<ConversationRecord>> {
        self.read(|connection| conversation_at(connection, id))
    }

    fn scan_conversations(
        &self,
        query: &ConversationQuery,
        limit: usize,
        cursor: Option<&Cursor>,
        _cancel: &CancelToken,
    ) -> Result<Page<ConversationRecord>> {
        self.read(|connection| {
            let start = scan_start(query.order, cursor, ScanOrder::Ascending)?;
            let (mut filter, direction) = ordered(start)?;
            if let Some(owner) = query.owner_conversation_id {
                filter = filter.with("owner_conversation_id = ?", int(owner.0)?);
            }
            if let Some(owner) = query.owner_task_id {
                filter = filter.with("owner_task_id = ?", int(owner.get())?);
            }
            let items = scan::<ConversationRecord>(
                connection,
                "conversations",
                filter,
                direction,
                limit.saturating_add(1),
            )?;
            page(items, limit, start.order, |record| record.id.0)
        })
    }

    fn entry(&self, id: EntryId, _cancel: &CancelToken) -> Result<Option<CommittedEntry>> {
        self.read(|connection| entry_row(connection, id))
    }

    fn visible_entry(
        &self,
        conversation_id: ConversationId,
        id: EntryId,
        _cancel: &CancelToken,
    ) -> Result<Option<CommittedEntry>> {
        self.read(|connection| visible_entry_at(connection, conversation_id, id))
    }

    fn find_latest_head_marker(
        &self,
        conversation_id: ConversationId,
        at_or_before: Option<EntryId>,
        _cancel: &CancelToken,
    ) -> Result<Option<HeadMarker>> {
        self.read(|connection| latest_head_marker(connection, conversation_id, at_or_before))
    }

    fn scan_entries(
        &self,
        query: &EntryQuery,
        limit: usize,
        cursor: Option<&Cursor>,
        _cancel: &CancelToken,
    ) -> Result<Page<EntryRecord>> {
        self.read(|connection| {
            let start = scan_start(query.order, cursor, ScanOrder::Descending)?;
            match start.order {
                ScanOrder::Ascending => entries_ascending(connection, query, limit, start.after),
                ScanOrder::Descending => entries_descending(connection, query, limit, start.after),
            }
        })
    }

    fn task(&self, id: TaskId, _cancel: &CancelToken) -> Result<Option<AnyTaskRecord>> {
        self.read(|connection| {
            record_at(
                connection,
                "SELECT record FROM tasks WHERE id = ?",
                params![id.get()],
            )
        })
    }

    fn scan_tasks(
        &self,
        query: &TaskQuery,
        limit: usize,
        cursor: Option<&Cursor>,
        _cancel: &CancelToken,
    ) -> Result<Page<AnyTaskRecord>> {
        self.read(|connection| {
            let start = scan_start(query.order, cursor, ScanOrder::Ascending)?;
            let (mut filter, direction) = ordered(start)?;
            if let Some(conversation_id) = query.conversation_id {
                filter = filter.with("conversation_id = ?", int(conversation_id.0)?);
            }
            if let Some(kind) = &query.kind {
                filter = filter.with("kind = ?", Value::Text(indexed(kind)?));
            }
            if let Some(status) = query.status {
                filter = filter.with(
                    "status = ?",
                    Value::Text(task_status_name(status).to_owned()),
                );
            }
            if let Some(abort_requested) = query.abort_requested {
                filter = filter.with(
                    "abort_requested = ?",
                    Value::Integer(i64::from(abort_requested)),
                );
            }
            if let Some(background) = query.background {
                filter = filter.with("background = ?", Value::Integer(i64::from(background)));
            }
            let items = scan::<AnyTaskRecord>(
                connection,
                "tasks",
                filter,
                direction,
                limit.saturating_add(1),
            )?;
            page(items, limit, start.order, |record| record.id.get())
        })
    }

    fn submission(
        &self,
        id: SubmissionId,
        _cancel: &CancelToken,
    ) -> Result<Option<SubmissionRecord>> {
        self.read(|connection| {
            record_at(
                connection,
                "SELECT record FROM submissions WHERE id = ?",
                params![id.0],
            )
        })
    }

    fn scan_submissions(
        &self,
        query: &SubmissionQuery,
        limit: usize,
        cursor: Option<&Cursor>,
        _cancel: &CancelToken,
    ) -> Result<Page<SubmissionRecord>> {
        self.read(|connection| {
            let start = scan_start(query.order, cursor, ScanOrder::Ascending)?;
            let (mut filter, direction) = ordered(start)?;
            if let Some(conversation_id) = query.conversation_id {
                filter = filter.with("conversation_id = ?", int(conversation_id.0)?);
            }
            if let Some(status) = query.status {
                filter = filter.with(
                    "status = ?",
                    Value::Text(submission_status_name(status).to_owned()),
                );
            }
            let items = scan::<SubmissionRecord>(
                connection,
                "submissions",
                filter,
                direction,
                limit.saturating_add(1),
            )?;
            page(items, limit, start.order, |record| record.id.0)
        })
    }

    fn submission_by_request(
        &self,
        conversation_id: ConversationId,
        request_id: &str,
        _cancel: &CancelToken,
    ) -> Result<Option<SubmissionRecord>> {
        self.read(|connection| {
            record_at(
                connection,
                "SELECT record FROM submissions WHERE conversation_id = ? AND request_id = ?",
                params![conversation_id.0, indexed(request_id)?],
            )
        })
    }

    fn find_document(
        &self,
        address: &DocumentAddress,
        at: DocumentPoint,
        _cancel: &CancelToken,
    ) -> Result<Option<DocumentRecord>> {
        self.read(|connection| {
            let mut values = address_values(parts(&address.kind, &address.scope, address.key.as_deref())?)?;
            let sql = match at {
                DocumentPoint::Current => {
                    "SELECT record FROM documents
                        WHERE kind = ? AND scope_kind = ? AND owner_id = ? AND family = ? AND key_value = ?
                        AND retired_at IS NULL ORDER BY created_at DESC LIMIT 1"
                }
                DocumentPoint::At(seq) => {
                    values.push(int(seq.0)?);
                    values.push(int(seq.0)?);
                    "SELECT record FROM documents
                        WHERE kind = ? AND scope_kind = ? AND owner_id = ? AND family = ? AND key_value = ?
                        AND created_at <= ? AND (retired_at IS NULL OR retired_at > ?)
                        ORDER BY created_at DESC LIMIT 1"
                }
            };
            record_at(connection, sql, params_from_iter(values))
        })
    }

    fn document(
        &self,
        id: DocumentId,
        at: DocumentPoint,
        _cancel: &CancelToken,
    ) -> Result<Option<StoredDocument>> {
        self.read(|connection| materialize(connection, id, at))
    }

    fn scan_documents(
        &self,
        query: &DocumentQuery,
        limit: usize,
        cursor: Option<&Cursor>,
        _cancel: &CancelToken,
    ) -> Result<Page<DocumentRecord>> {
        self.read(|connection| {
            let (scope_kind, owner_id) = scope_columns(&query.scope);
            let after = match cursor_after(cursor)? {
                Some(after) => int(after)?,
                None => Value::Integer(-1),
            };
            let mut filter = Filter::new("id > ?", after)
                .with("scope_kind = ?", Value::Text(scope_kind.to_owned()))
                .with("owner_id = ?", int(owner_id)?);
            if let Some(kind) = &query.kind {
                filter = filter.with("kind = ?", Value::Text(indexed(kind)?));
            }
            filter = match query.at {
                DocumentPoint::Current => filter.bare("retired_at IS NULL"),
                DocumentPoint::At(seq) => filter
                    .with("created_at <= ?", int(seq.0)?)
                    .with("(retired_at IS NULL OR retired_at > ?)", int(seq.0)?),
            };
            let items = scan::<DocumentRecord>(
                connection,
                "documents",
                filter,
                "ASC",
                limit.saturating_add(1),
            )?;
            page(items, limit, ScanOrder::Ascending, |record| {
                record.create.id.0
            })
        })
    }

    fn close(&self, _cancel: &CancelToken) -> Result<()> {
        let mut state = self.lock();
        match state.connection.take() {
            Some(connection) => database::close(connection),
            None => Ok(()),
        }
    }
}
