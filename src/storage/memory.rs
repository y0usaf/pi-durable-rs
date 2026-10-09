use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::Bound::{self, Excluded, Unbounded};
use std::sync::{Mutex, MutexGuard};

use crate::cancel::CancelToken;
use crate::documents::MAX_SAFE_INTEGER;
use crate::errors::{Error, Result};
use crate::ids::{ConversationId, DocumentId, EntryId, Seq, SubmissionId};
use crate::storage::scan::{ScanStart, cursor_after, page, scan_start};
use crate::storage::{
    AddressKey, ScopeKey, Table, address_key, apply_ops, is_alive_at, is_current_only, lookup_key,
    submission_status,
};
use crate::types::{
    AnyTaskRecord, CommittedEntry, ConversationParent, ConversationQuery, ConversationRecord,
    Cursor, DocumentAddress, DocumentBase, DocumentContent, DocumentCopySource, DocumentCreate,
    DocumentPoint, DocumentQuery, DocumentRecord, EntryQuery, EntryRecord, HeadMarker, Page,
    ScanOrder, ScopeRef, Storage, StorageWrite, StoredDocument, SubmissionQuery, SubmissionRecord,
    TableChange, TaskQuery,
};

#[derive(Debug, Clone)]
struct Revision {
    seq: u64,
    content: DocumentContent,
}

#[derive(Debug, Clone)]
struct StoredDocumentState {
    record: DocumentRecord,
    revisions: Vec<Revision>,
}

#[derive(Debug, Default)]
struct DocumentAction {
    create: Option<DocumentCreate>,
    content: Option<DocumentContent>,
    retire: bool,
}

#[derive(Debug)]
pub(crate) struct Prepared {
    pub(crate) seq: Seq,
    pub(crate) writes: Vec<StorageWrite>,
    actions: Vec<(u64, DocumentAction)>,
}

#[derive(Debug, Default)]
pub(crate) struct Core {
    record_types: HashMap<u64, Table>,
    conversations: BTreeMap<u64, ConversationRecord>,
    entries: BTreeMap<u64, EntryRecord>,
    entry_commit_seqs: HashMap<u64, u64>,
    entry_ids: HashMap<u64, BTreeSet<u64>>,
    head_entry_ids: HashMap<u64, BTreeSet<u64>>,
    tasks: BTreeMap<u64, AnyTaskRecord>,
    submissions: BTreeMap<u64, SubmissionRecord>,
    submission_ids_by_request: HashMap<(u64, String), u64>,
    documents: BTreeMap<u64, StoredDocumentState>,
    document_ids_by_address: BTreeMap<AddressKey, BTreeSet<u64>>,
    document_ids_by_scope: BTreeMap<ScopeKey, BTreeSet<u64>>,
    next_id: u64,
    next_seq: u64,
    closed: bool,
}

fn invalid(message: String) -> Error {
    Error::Invalid(message)
}

fn unknown_conversation(id: ConversationId) -> Error {
    invalid(format!("Unknown conversation: {id}"))
}

fn revision_version(content: &DocumentContent) -> u64 {
    match content {
        DocumentContent::Base(base) => base.version,
        DocumentContent::Delta(delta) => delta.version,
    }
}

fn same_copy_target(source: &DocumentCreate, target: &DocumentCreate) -> bool {
    matches!(source.scope, ScopeRef::Conversation { .. })
        && matches!(target.scope, ScopeRef::Conversation { .. })
        && source.kind == target.kind
        && source.key == target.key
        && source.semantics == target.semantics
}

fn scan_map<'a, V: Clone + 'a>(
    map: &'a BTreeMap<u64, V>,
    start: ScanStart,
    limit: usize,
    keep: impl Fn(&V) -> bool,
) -> Vec<V> {
    let values: Box<dyn Iterator<Item = (&u64, &V)> + 'a> = match (start.order, start.after) {
        (ScanOrder::Ascending, None) => Box::new(map.iter()),
        (ScanOrder::Ascending, Some(after)) => Box::new(map.range((Excluded(after), Unbounded))),
        (ScanOrder::Descending, None) => Box::new(map.iter().rev()),
        (ScanOrder::Descending, Some(after)) => Box::new(map.range(..after).rev()),
    };
    values
        .map(|(_, value)| value)
        .filter(|value| keep(value))
        .take(limit + 1)
        .cloned()
        .collect()
}

fn push_revision(stored: &mut StoredDocumentState, content: &DocumentContent, seq: Seq) {
    let revision = Revision {
        seq: seq.0,
        content: content.clone(),
    };
    if matches!(content, DocumentContent::Base(_)) && is_current_only(&stored.record.create) {
        stored.revisions = vec![revision];
    } else {
        stored.revisions.push(revision);
    }
}

fn check_global_ids(record_types: &HashMap<u64, Table>, writes: &[StorageWrite]) -> Result<()> {
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
        let existing = record_types.get(&id).copied();
        let earlier = claimed.get(&id).copied();
        match table {
            Table::Conversation | Table::Entry | Table::Document => {
                if let Some(existing) = existing {
                    return Err(invalid(format!(
                        "ID {id} already belongs to {}",
                        existing.name()
                    )));
                }
                if earlier.is_some() {
                    return Err(invalid(format!("ID {id} is written more than once")));
                }
            }
            Table::Task | Table::Submission => {
                if let Some(existing) = existing.filter(|existing| *existing != table) {
                    return Err(invalid(format!(
                        "ID {id} already belongs to {}",
                        existing.name()
                    )));
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

fn prepare_actions(writes: &[StorageWrite]) -> Result<Vec<(u64, DocumentAction)>> {
    let mut actions: Vec<(u64, DocumentAction)> = Vec::new();
    for write in writes {
        let id = match write {
            StorageWrite::DocumentCreate { record, .. } => record.id.0,
            StorageWrite::DocumentChange { id, .. } | StorageWrite::DocumentRetire { id } => id.0,
            StorageWrite::Table(_) | StorageWrite::DocumentCopy { .. } => continue,
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
                if action.create.is_some() || action.content.is_some() {
                    return Err(invalid(format!(
                        "Document {id} has more than one content command"
                    )));
                }
                action.create = Some(record.clone());
                action.content = Some(DocumentContent::Base(content.clone()));
            }
            StorageWrite::DocumentChange { content, .. } => {
                if action.content.is_some() {
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
            StorageWrite::Table(_) | StorageWrite::DocumentCopy { .. } => {}
        }
    }
    Ok(actions)
}

impl Core {
    pub(crate) fn new() -> Self {
        Self {
            next_id: 2,
            next_seq: 1,
            ..Self::default()
        }
    }

    fn open(&self) -> Result<()> {
        if self.closed {
            Err(invalid("MemoryStorage is closed".to_owned()))
        } else {
            Ok(())
        }
    }

    pub(crate) fn prepare(&self, writes: &[StorageWrite], seq: Seq) -> Result<Prepared> {
        self.open()?;
        if seq.0 > MAX_SAFE_INTEGER || seq.0 < self.next_seq {
            return Err(invalid(format!(
                "Commit sequence {} does not strictly increase",
                seq.0
            )));
        }
        let writes = self.resolve_copies(writes)?;
        check_global_ids(&self.record_types, &writes)?;
        let actions = prepare_actions(&writes)?;
        self.check_actions(&actions)?;
        Ok(Prepared {
            seq,
            writes,
            actions,
        })
    }

    pub(crate) fn apply(&mut self, prepared: Prepared) -> Seq {
        let Prepared {
            seq,
            writes,
            actions,
        } = prepared;
        for write in &writes {
            if let StorageWrite::Table(change) = write {
                self.apply_table(change, seq);
            }
        }
        self.apply_actions(&actions, seq);
        self.next_seq = seq.0 + 1;
        seq
    }

    fn resolve_copies(&self, writes: &[StorageWrite]) -> Result<Vec<StorageWrite>> {
        if !writes
            .iter()
            .any(|write| matches!(write, StorageWrite::DocumentCopy { .. }))
        {
            return Ok(writes.to_vec());
        }
        let changed: HashSet<u64> = writes
            .iter()
            .filter_map(|write| match write {
                StorageWrite::DocumentCreate { record, .. }
                | StorageWrite::DocumentCopy { record, .. } => Some(record.id.0),
                StorageWrite::DocumentChange { id, .. } | StorageWrite::DocumentRetire { id } => {
                    Some(id.0)
                }
                StorageWrite::Table(_) => None,
            })
            .collect();
        writes
            .iter()
            .map(|write| match write {
                StorageWrite::DocumentCopy { record, source } => self
                    .copy_as_create(record, source, &changed)
                    .map_err(|cause| Error::StorageRejected {
                        message: format!("Document copy {} was rejected", record.id),
                        cause: Some(Box::new(cause)),
                    }),
                other => Ok(other.clone()),
            })
            .collect()
    }

    fn copy_as_create(
        &self,
        record: &DocumentCreate,
        source: &DocumentCopySource,
        changed: &HashSet<u64>,
    ) -> Result<StorageWrite> {
        if changed.contains(&source.id.0) {
            return Err(invalid(format!(
                "Fork source document {} is changed in the copy batch",
                source.id
            )));
        }
        let stored = self
            .materialize(source.id, source.at)?
            .ok_or_else(|| invalid(format!("Fork source document {} cannot be read", source.id)))?;
        let source_matches = matches!(stored.record.create.scope, ScopeRef::Conversation { .. })
            && same_copy_target(&stored.record.create, record);
        if !source_matches {
            return Err(invalid(format!(
                "Fork source document {} does not match the copied record",
                source.id
            )));
        }
        Ok(StorageWrite::DocumentCreate {
            record: record.clone(),
            content: DocumentBase {
                version: stored.version,
                value: stored.value,
            },
        })
    }

    fn apply_table(&mut self, change: &TableChange, seq: Seq) {
        match change {
            TableChange::Conversation(record) => {
                let id = record.id.0;
                self.record_types.insert(id, Table::Conversation);
                self.conversations.insert(id, record.clone());
                self.next_id = self.next_id.max(id + 1);
            }
            TableChange::Entry(record) => {
                let id = record.id.0;
                let conversation = record.conversation_id.0;
                self.record_types.insert(id, Table::Entry);
                self.entries.insert(id, record.clone());
                self.entry_commit_seqs.insert(id, seq.0);
                self.entry_ids.entry(conversation).or_default().insert(id);
                if record.head.is_some() {
                    self.head_entry_ids
                        .entry(conversation)
                        .or_default()
                        .insert(id);
                }
                self.next_id = self.next_id.max(id + 1);
            }
            TableChange::Task(record) => {
                let id = record.id.get();
                self.record_types.insert(id, Table::Task);
                self.tasks.insert(id, record.as_ref().clone());
                self.next_id = self.next_id.max(id + 1);
            }
            TableChange::Submission(record) => {
                let id = record.id.0;
                self.record_types.insert(id, Table::Submission);
                if let Some(previous) = self.submissions.get(&id)
                    && let Some(request) = &previous.request_id
                {
                    let key = (previous.conversation_id.0, request.clone());
                    if self.submission_ids_by_request.get(&key) == Some(&id) {
                        self.submission_ids_by_request.remove(&key);
                    }
                }
                if let Some(request) = &record.request_id {
                    self.submission_ids_by_request
                        .insert((record.conversation_id.0, request.clone()), id);
                }
                self.submissions.insert(id, record.clone());
                self.next_id = self.next_id.max(id + 1);
            }
        }
    }

    pub(crate) fn mint_id(&mut self) -> Result<u64> {
        self.open()?;
        if self.next_id > MAX_SAFE_INTEGER {
            return Err(invalid("ID space is exhausted".to_owned()));
        }
        let id = self.next_id;
        self.next_id += 1;
        Ok(id)
    }

    pub(crate) fn conversation(&self, id: ConversationId) -> Result<Option<ConversationRecord>> {
        self.open()?;
        Ok(self.conversations.get(&id.0).cloned())
    }

    pub(crate) fn scan_conversations(
        &self,
        query: &ConversationQuery,
        limit: usize,
        cursor: Option<&Cursor>,
    ) -> Result<Page<ConversationRecord>> {
        self.open()?;
        let start = scan_start(query.order, cursor, ScanOrder::Ascending)?;
        let values = scan_map(&self.conversations, start, limit, |record| {
            let owner = record.owner.as_ref();
            query.owner_conversation_id.is_none_or(|conversation| {
                owner.is_some_and(|owner| owner.conversation_id == conversation)
            }) && query
                .owner_task_id
                .is_none_or(|task| owner.is_some_and(|owner| owner.task_id.get() == task.get()))
        });
        Ok(page(values, limit, start.order, |record| record.id.0))
    }

    fn committed(&self, entry: EntryRecord) -> CommittedEntry {
        let commit_seq = self
            .entry_commit_seqs
            .get(&entry.id.0)
            .copied()
            .unwrap_or_default();
        CommittedEntry {
            entry,
            commit_seq: Seq(commit_seq),
        }
    }

    pub(crate) fn entry(&self, id: EntryId) -> Result<Option<CommittedEntry>> {
        self.open()?;
        Ok(self
            .entries
            .get(&id.0)
            .cloned()
            .map(|entry| self.committed(entry)))
    }

    pub(crate) fn visible_entry(
        &self,
        conversation: ConversationId,
        id: EntryId,
    ) -> Result<Option<CommittedEntry>> {
        self.open()?;
        let entry = self
            .visible(conversation, id.0, id.0, ScanOrder::Descending, 1)?
            .into_iter()
            .next();
        Ok(entry.map(|entry| self.committed(entry)))
    }

    pub(crate) fn find_latest_head_marker(
        &self,
        conversation: ConversationId,
        at_or_before: Option<EntryId>,
    ) -> Result<Option<HeadMarker>> {
        self.open()?;
        if !self.conversations.contains_key(&conversation.0) {
            return Err(unknown_conversation(conversation));
        }
        let mut current = conversation.0;
        let mut upper = at_or_before.map(|id| id.0);
        loop {
            let found = self
                .head_entry_ids
                .get(&current)
                .and_then(|ids| match upper {
                    Some(upper) => ids.range(..=upper).next_back(),
                    None => ids.iter().next_back(),
                });
            if let Some(entry) = found.and_then(|id| self.entries.get(id))
                && let Some(head) = entry.head
            {
                return Ok(Some(HeadMarker {
                    entry: entry.clone(),
                    head,
                }));
            }
            let Some(parent) = self.parent_of(current) else {
                return Ok(None);
            };
            upper = Some(upper.map_or(parent.at.0, |upper| upper.min(parent.at.0)));
            current = parent.conversation_id.0;
        }
    }

    fn parent_of(&self, conversation: u64) -> Option<ConversationParent> {
        self.conversations
            .get(&conversation)
            .and_then(|record| record.parent.clone())
    }

    pub(crate) fn scan_entries(
        &self,
        query: &EntryQuery,
        limit: usize,
        cursor: Option<&Cursor>,
    ) -> Result<Page<EntryRecord>> {
        self.open()?;
        let ScanStart { order, after } = scan_start(query.order, cursor, ScanOrder::Descending)?;
        let mut min = query.min_entry_id.map(|id| id.0);
        let mut max = query.max_entry_id.map(|id| id.0);
        if let Some(after) = after {
            match order {
                ScanOrder::Descending => {
                    let bound = after.saturating_sub(1);
                    max = Some(max.map_or(bound, |max| max.min(bound)));
                }
                ScanOrder::Ascending => {
                    min = Some(min.map_or(after + 1, |min| min.max(after + 1)));
                }
            }
        }
        let values = self.visible(
            query.conversation_id,
            min.unwrap_or(0),
            max.unwrap_or(u64::MAX),
            order,
            limit + 1,
        )?;
        Ok(page(values, limit, order, |entry| entry.id.0))
    }

    fn visible(
        &self,
        conversation: ConversationId,
        min: u64,
        max: u64,
        order: ScanOrder,
        take: usize,
    ) -> Result<Vec<EntryRecord>> {
        if !self.conversations.contains_key(&conversation.0) {
            return Err(unknown_conversation(conversation));
        }
        Ok(match order {
            ScanOrder::Descending => self.visible_descending(conversation.0, min, max, take),
            ScanOrder::Ascending => self.visible_ascending(conversation.0, min, max, take),
        })
    }

    fn visible_descending(
        &self,
        conversation: u64,
        min: u64,
        max: u64,
        take: usize,
    ) -> Vec<EntryRecord> {
        let mut out = Vec::new();
        let mut current = conversation;
        let mut upper = max;
        let none = BTreeSet::new();
        loop {
            let ids = self.entry_ids.get(&current).unwrap_or(&none);
            for id in ids.range(..=upper).rev().take_while(|id| **id >= min) {
                if let Some(entry) = self.entries.get(id) {
                    out.push(entry.clone());
                }
                if out.len() >= take {
                    return out;
                }
            }
            let Some(parent) = self.parent_of(current) else {
                break;
            };
            upper = upper.min(parent.at.0);
            if upper < min {
                break;
            }
            current = parent.conversation_id.0;
        }
        out
    }

    fn visible_ascending(
        &self,
        conversation: u64,
        min: u64,
        max: u64,
        take: usize,
    ) -> Vec<EntryRecord> {
        let mut segments = Vec::new();
        let mut current = conversation;
        let mut upper = max;
        loop {
            segments.push((current, upper));
            let Some(parent) = self.parent_of(current) else {
                break;
            };
            upper = upper.min(parent.at.0);
            if upper < min {
                break;
            }
            current = parent.conversation_id.0;
        }
        let mut out = Vec::new();
        let none = BTreeSet::new();
        for (segment, segment_upper) in segments.into_iter().rev() {
            let ids = self.entry_ids.get(&segment).unwrap_or(&none);
            for id in ids.range(min..).take_while(|id| **id <= segment_upper) {
                if let Some(entry) = self.entries.get(id) {
                    out.push(entry.clone());
                }
                if out.len() >= take {
                    return out;
                }
            }
        }
        out
    }

    pub(crate) fn task(&self, id: u64) -> Result<Option<AnyTaskRecord>> {
        self.open()?;
        Ok(self.tasks.get(&id).cloned())
    }

    pub(crate) fn scan_tasks(
        &self,
        query: &TaskQuery,
        limit: usize,
        cursor: Option<&Cursor>,
    ) -> Result<Page<AnyTaskRecord>> {
        self.open()?;
        let start = scan_start(query.order, cursor, ScanOrder::Ascending)?;
        let values = scan_map(&self.tasks, start, limit, |task| {
            query
                .conversation_id
                .is_none_or(|conversation| task.conversation_id == conversation)
                && query.kind.as_ref().is_none_or(|kind| *kind == task.kind)
                && query.status.is_none_or(|status| status == task.status())
                && query
                    .abort_requested
                    .is_none_or(|flag| flag == task.abort_requested)
                && query.background.is_none_or(|flag| flag == task.background)
        });
        Ok(page(values, limit, start.order, |task| task.id.get()))
    }

    pub(crate) fn submission(&self, id: SubmissionId) -> Result<Option<SubmissionRecord>> {
        self.open()?;
        Ok(self.submissions.get(&id.0).cloned())
    }

    pub(crate) fn scan_submissions(
        &self,
        query: &SubmissionQuery,
        limit: usize,
        cursor: Option<&Cursor>,
    ) -> Result<Page<SubmissionRecord>> {
        self.open()?;
        let start = scan_start(query.order, cursor, ScanOrder::Ascending)?;
        let values = scan_map(&self.submissions, start, limit, |submission| {
            query
                .conversation_id
                .is_none_or(|conversation| submission.conversation_id == conversation)
                && query
                    .status
                    .is_none_or(|status| status == submission_status(submission))
        });
        Ok(page(values, limit, start.order, |submission| {
            submission.id.0
        }))
    }

    pub(crate) fn submission_by_request(
        &self,
        conversation: ConversationId,
        request_id: &str,
    ) -> Result<Option<SubmissionRecord>> {
        self.open()?;
        Ok(self
            .submission_ids_by_request
            .get(&(conversation.0, request_id.to_owned()))
            .and_then(|id| self.submissions.get(id))
            .cloned())
    }

    pub(crate) fn find_document(
        &self,
        address: &DocumentAddress,
        at: DocumentPoint,
    ) -> Result<Option<DocumentRecord>> {
        self.open()?;
        let Some(ids) = self.document_ids_by_address.get(&lookup_key(address)) else {
            return Ok(None);
        };
        Ok(ids
            .iter()
            .filter_map(|id| self.documents.get(id))
            .find(|stored| is_alive_at(&stored.record, at))
            .map(|stored| stored.record.clone()))
    }

    pub(crate) fn document(
        &self,
        id: DocumentId,
        at: DocumentPoint,
    ) -> Result<Option<StoredDocument>> {
        self.open()?;
        self.materialize(id, at)
    }

    pub(crate) fn scan_documents(
        &self,
        query: &DocumentQuery,
        limit: usize,
        cursor: Option<&Cursor>,
    ) -> Result<Page<DocumentRecord>> {
        self.open()?;
        let lower = cursor_after(cursor)?.map_or(Unbounded, Excluded);
        let none = BTreeSet::new();
        let ids = self
            .document_ids_by_scope
            .get(&ScopeKey::from(&query.scope))
            .unwrap_or(&none);
        let values: Vec<DocumentRecord> = ids
            .range((lower, Bound::Unbounded))
            .filter_map(|id| self.documents.get(id))
            .filter(|stored| {
                query
                    .kind
                    .as_ref()
                    .is_none_or(|kind| *kind == stored.record.create.kind)
            })
            .filter(|stored| is_alive_at(&stored.record, query.at))
            .take(limit + 1)
            .map(|stored| stored.record.clone())
            .collect();
        Ok(page(values, limit, ScanOrder::Ascending, |record| {
            record.create.id.0
        }))
    }

    fn materialize(&self, id: DocumentId, at: DocumentPoint) -> Result<Option<StoredDocument>> {
        let Some(stored) = self.documents.get(&id.0) else {
            return Ok(None);
        };
        if at != DocumentPoint::Current && is_current_only(&stored.record.create) {
            return Err(invalid(format!(
                "Document {id} does not retain historical content"
            )));
        }
        if !is_alive_at(&stored.record, at) {
            return Ok(None);
        }
        let revisions: Vec<&Revision> = stored
            .revisions
            .iter()
            .filter(|revision| match at {
                DocumentPoint::Current => true,
                DocumentPoint::At(seq) => revision.seq <= seq.0,
            })
            .collect();
        let Some((base_index, base)) =
            revisions
                .iter()
                .enumerate()
                .rev()
                .find_map(|(index, revision)| match &revision.content {
                    DocumentContent::Base(base) => Some((index, base)),
                    DocumentContent::Delta(_) => None,
                })
        else {
            return Err(invalid(format!("Document {id} is missing a required base")));
        };
        let mut value = base.value.clone();
        let tail = &revisions[base_index + 1..];
        for revision in tail {
            match &revision.content {
                DocumentContent::Delta(delta) if delta.version == base.version => {
                    value = apply_ops(value, &delta.ops)?;
                }
                _ => {
                    return Err(invalid(format!(
                        "Document {id} crosses a stored version boundary without a base"
                    )));
                }
            }
        }
        Ok(Some(StoredDocument {
            record: stored.record.clone(),
            version: base.version,
            value,
            deltas_since_base: tail.len() as u64,
        }))
    }

    fn live_document(&self, key: &AddressKey) -> Option<u64> {
        self.document_ids_by_address
            .get(key)?
            .iter()
            .find(|id| {
                self.documents
                    .get(id)
                    .is_some_and(|stored| stored.record.retired_at.is_none())
            })
            .copied()
    }

    fn check_actions(&self, actions: &[(u64, DocumentAction)]) -> Result<()> {
        let mut live_counts: HashMap<AddressKey, i64> = HashMap::new();
        for (id, action) in actions {
            let existing = self.documents.get(id);
            if action.create.is_none() && existing.is_none() {
                return Err(invalid(format!("Unknown document: {id}")));
            }
            if action.create.is_some() && existing.is_some() {
                return Err(invalid(format!("Document {id} already exists")));
            }
            if existing.is_some_and(|stored| stored.record.retired_at.is_some()) {
                return Err(invalid(format!("Document {id} is retired")));
            }
            if let Some(DocumentContent::Delta(delta)) = &action.content {
                let Some(previous) = existing.and_then(|stored| stored.revisions.last()) else {
                    return Err(invalid(format!("Document {id} delta has no base")));
                };
                if revision_version(&previous.content) != delta.version {
                    return Err(invalid(format!(
                        "Document {id} version transition requires a base"
                    )));
                }
            }
            let Some(create) = action
                .create
                .as_ref()
                .or(existing.map(|stored| &stored.record.create))
            else {
                return Err(invalid(format!("Unknown document: {id}")));
            };
            let key = address_key(create);
            let current = self.live_document(&key);
            let live = live_counts
                .entry(key)
                .or_insert_with(|| i64::from(current.is_some()));
            if action.retire && current == Some(*id) {
                *live -= 1;
            }
            if action.create.is_some() && !action.retire {
                *live += 1;
            }
        }
        if live_counts.values().any(|live| *live > 1) {
            return Err(invalid(
                "Document address already has a current incarnation".to_owned(),
            ));
        }
        Ok(())
    }

    fn apply_actions(&mut self, actions: &[(u64, DocumentAction)], seq: Seq) {
        for (id, action) in actions {
            let id = *id;
            match &action.create {
                Some(create) => {
                    let record = DocumentRecord {
                        create: create.clone(),
                        created_at: seq,
                        retired_at: action.retire.then_some(seq),
                    };
                    let revisions = action
                        .content
                        .iter()
                        .map(|content| Revision {
                            seq: seq.0,
                            content: content.clone(),
                        })
                        .collect();
                    self.record_types.insert(id, Table::Document);
                    self.document_ids_by_address
                        .entry(address_key(create))
                        .or_default()
                        .insert(id);
                    self.document_ids_by_scope
                        .entry(ScopeKey::from(&create.scope))
                        .or_default()
                        .insert(id);
                    self.documents
                        .insert(id, StoredDocumentState { record, revisions });
                    self.next_id = self.next_id.max(id + 1);
                }
                None => {
                    if let Some(stored) = self.documents.get_mut(&id) {
                        if let Some(content) = &action.content {
                            push_revision(stored, content, seq);
                        }
                        if action.retire {
                            stored.record.retired_at = Some(seq);
                        }
                    }
                }
            }
            if action.retire
                && let Some(stored) = self.documents.get_mut(&id)
                && is_current_only(&stored.record.create)
            {
                stored.revisions.clear();
            }
        }
    }
}

#[derive(Debug)]
pub struct MemoryStorage {
    core: Mutex<Core>,
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self {
            core: Mutex::new(Core::new()),
        }
    }

    pub(crate) fn lock(&self) -> MutexGuard<'_, Core> {
        self.core.lock().expect("memory storage lock poisoned")
    }
}

impl Storage for MemoryStorage {
    fn commit(&self, writes: &[StorageWrite], _cancel: &CancelToken) -> Result<Seq> {
        let mut core = self.lock();
        let seq = Seq(core.next_seq);
        let prepared = core.prepare(writes, seq)?;
        Ok(core.apply(prepared))
    }

    fn mint_id(&self) -> Result<u64> {
        self.lock().mint_id()
    }

    fn conversation(
        &self,
        id: ConversationId,
        _cancel: &CancelToken,
    ) -> Result<Option<ConversationRecord>> {
        self.lock().conversation(id)
    }

    fn scan_conversations(
        &self,
        query: &ConversationQuery,
        limit: usize,
        cursor: Option<&Cursor>,
        _cancel: &CancelToken,
    ) -> Result<Page<ConversationRecord>> {
        self.lock().scan_conversations(query, limit, cursor)
    }

    fn entry(&self, id: EntryId, _cancel: &CancelToken) -> Result<Option<CommittedEntry>> {
        self.lock().entry(id)
    }

    fn visible_entry(
        &self,
        conversation_id: ConversationId,
        id: EntryId,
        _cancel: &CancelToken,
    ) -> Result<Option<CommittedEntry>> {
        self.lock().visible_entry(conversation_id, id)
    }

    fn find_latest_head_marker(
        &self,
        conversation_id: ConversationId,
        at_or_before: Option<EntryId>,
        _cancel: &CancelToken,
    ) -> Result<Option<HeadMarker>> {
        self.lock()
            .find_latest_head_marker(conversation_id, at_or_before)
    }

    fn scan_entries(
        &self,
        query: &EntryQuery,
        limit: usize,
        cursor: Option<&Cursor>,
        _cancel: &CancelToken,
    ) -> Result<Page<EntryRecord>> {
        self.lock().scan_entries(query, limit, cursor)
    }

    fn task(&self, id: crate::ids::TaskId, _cancel: &CancelToken) -> Result<Option<AnyTaskRecord>> {
        self.lock().task(id.get())
    }

    fn scan_tasks(
        &self,
        query: &TaskQuery,
        limit: usize,
        cursor: Option<&Cursor>,
        _cancel: &CancelToken,
    ) -> Result<Page<AnyTaskRecord>> {
        self.lock().scan_tasks(query, limit, cursor)
    }

    fn submission(
        &self,
        id: SubmissionId,
        _cancel: &CancelToken,
    ) -> Result<Option<SubmissionRecord>> {
        self.lock().submission(id)
    }

    fn scan_submissions(
        &self,
        query: &SubmissionQuery,
        limit: usize,
        cursor: Option<&Cursor>,
        _cancel: &CancelToken,
    ) -> Result<Page<SubmissionRecord>> {
        self.lock().scan_submissions(query, limit, cursor)
    }

    fn submission_by_request(
        &self,
        conversation_id: ConversationId,
        request_id: &str,
        _cancel: &CancelToken,
    ) -> Result<Option<SubmissionRecord>> {
        self.lock()
            .submission_by_request(conversation_id, request_id)
    }

    fn find_document(
        &self,
        address: &DocumentAddress,
        at: DocumentPoint,
        _cancel: &CancelToken,
    ) -> Result<Option<DocumentRecord>> {
        self.lock().find_document(address, at)
    }

    fn document(
        &self,
        id: DocumentId,
        at: DocumentPoint,
        _cancel: &CancelToken,
    ) -> Result<Option<StoredDocument>> {
        self.lock().document(id, at)
    }

    fn scan_documents(
        &self,
        query: &DocumentQuery,
        limit: usize,
        cursor: Option<&Cursor>,
        _cancel: &CancelToken,
    ) -> Result<Page<DocumentRecord>> {
        self.lock().scan_documents(query, limit, cursor)
    }

    fn close(&self, _cancel: &CancelToken) -> Result<()> {
        self.lock().closed = true;
        Ok(())
    }
}
