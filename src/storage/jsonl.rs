use std::collections::{BTreeMap, HashMap, HashSet};
use std::io;
use std::sync::{Mutex, MutexGuard};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Map;

use crate::cancel::CancelToken;
use crate::documents::MAX_SAFE_INTEGER;
use crate::env::{
    ExecutionEnv, FileError, FileErrorCode, FileKind, NodeExecutionEnv, NodeOptions, WatchOptions,
};
use crate::errors::{Error, Result};
use crate::ids::{ConversationId, DocumentId, EntryId, Seq, SubmissionId, TaskId};
use crate::storage::is_current_only;
use crate::storage::memory::{MemoryStorage, Prepared};
use crate::tasks::TaskStatus;
use crate::types::{
    AnyTaskRecord, CommittedEntry, ConversationQuery, ConversationRecord, Cursor, DocumentAddress,
    DocumentBase, DocumentContent, DocumentCreate, DocumentPoint, DocumentQuery, DocumentRecord,
    EntryQuery, EntryRecord, HeadMarker, Page, Storage, StorageWrite, StoredDocument,
    SubmissionQuery, SubmissionRecord, TableChange, TaskQuery,
};

const FORMAT_VERSION: u64 = 1;
const MAIN_FILE: &str = "main.jsonl";
const RECLAIM_SUFFIX: &str = ".reclaim";
const POISONED: &str = "JSONL storage is poisoned and must be reopened";

#[derive(Debug, Clone, Copy, Default)]
pub struct JsonlStorageOptions {
    pub fsync: bool,
}

type RecordKey = (String, u64, u64);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
enum MainOperation {
    #[serde(rename = "conversation")]
    Conversation { value: ConversationRecord },
    #[serde(rename = "entry")]
    Entry { value: EntryRecord },
    #[serde(rename = "submission")]
    Submission { value: SubmissionRecord },
    #[serde(rename = "task")]
    Task { value: AnyTaskRecord },
    #[serde(rename = "task.sidecar")]
    TaskSidecar { id: TaskId, ordinal: u64 },
    #[serde(rename = "document.create")]
    DocumentCreate {
        record: DocumentCreate,
        ordinal: u64,
    },
    #[serde(rename = "document.change")]
    DocumentChange { id: DocumentId, ordinal: u64 },
    #[serde(rename = "document.retire")]
    DocumentRetire { id: DocumentId },
}

#[derive(Debug, Serialize, Deserialize)]
struct MainMarker {
    format: u64,
    #[serde(rename = "type")]
    kind: String,
    seq: Seq,
    writes: Vec<MainOperation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
enum SidecarPayload {
    #[serde(rename = "task")]
    Task { value: AnyTaskRecord },
    #[serde(rename = "document")]
    Document {
        id: DocumentId,
        content: DocumentContent,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SidecarRecord {
    format: u64,
    #[serde(rename = "type")]
    kind: String,
    seq: Seq,
    ordinal: u64,
    payload: SidecarPayload,
}

struct ParsedLine<T> {
    value: T,
    start: u64,
}

struct ParsedFile<T> {
    path: String,
    lines: Vec<ParsedLine<T>>,
}

struct SidecarFile {
    name: String,
    task: bool,
    id: u64,
    parsed: ParsedFile<SidecarRecord>,
}

struct Encoded {
    marker: String,
    sidecars: BTreeMap<String, String>,
}

#[derive(Default)]
struct Sidecars {
    seq: u64,
    next_ordinal: u64,
    files: BTreeMap<String, Vec<SidecarRecord>>,
}

impl Sidecars {
    fn push(&mut self, file: String, payload: SidecarPayload) -> u64 {
        let ordinal = self.next_ordinal;
        self.next_ordinal += 1;
        self.files.entry(file).or_default().push(SidecarRecord {
            format: FORMAT_VERSION,
            kind: "record".to_owned(),
            seq: Seq(self.seq),
            ordinal,
            payload,
        });
        ordinal
    }
}

#[derive(Default)]
struct State {
    next_seq: u64,
    current_only_documents: HashSet<DocumentId>,
    live_task_sidecars: HashSet<u64>,
    closed: bool,
    poisoned: Option<String>,
}

pub struct JsonlStorage<E> {
    env: E,
    directory: String,
    main_path: String,
    fsync: bool,
    memory: MemoryStorage,
    state: Mutex<State>,
}

fn corruption(message: impl Into<String>) -> Error {
    Error::StorageRejected {
        message: message.into(),
        cause: None,
    }
}

fn file_error(action: &str, error: FileError) -> Error {
    Error::StorageRejected {
        message: format!("JSONL {action} failed: {error}"),
        cause: Some(Box::new(error)),
    }
}

fn poisoned_error(cause: &str) -> Error {
    Error::StorageRejected {
        message: POISONED.to_owned(),
        cause: Some(Box::new(io::Error::other(cause.to_owned()))),
    }
}

fn poison(state: &mut State, cause: Error) -> Error {
    let message = state
        .poisoned
        .get_or_insert_with(|| cause.to_string())
        .clone();
    poisoned_error(&message)
}

fn valid_seq(seq: Seq) -> bool {
    (1..=MAX_SAFE_INTEGER).contains(&seq.0)
}

fn decode<T: DeserializeOwned>(text: &str, description: &str, invalid: &str) -> Result<T> {
    serde_json::from_str(text).map_err(|error| {
        if error.is_syntax() {
            corruption(format!("Malformed complete {description}"))
        } else {
            corruption(format!("{invalid} in {description}"))
        }
    })
}

fn parse_marker(text: &str, line: u64) -> Result<MainMarker> {
    let description = format!("{MAIN_FILE} line {line}");
    let marker: MainMarker = decode(text, &description, "Invalid commit marker")?;
    if marker.format != FORMAT_VERSION || marker.kind != "commit" || !valid_seq(marker.seq) {
        return Err(corruption(format!(
            "Invalid commit marker in {description}"
        )));
    }
    Ok(marker)
}

fn parse_record(text: &str, file: &str, line: u64) -> Result<SidecarRecord> {
    let description = format!("{file} line {line}");
    let record: SidecarRecord = decode(text, &description, "Invalid sidecar record")?;
    if record.format != FORMAT_VERSION || record.kind != "record" || !valid_seq(record.seq) {
        return Err(corruption(format!(
            "Invalid sidecar record in {description}"
        )));
    }
    if let SidecarPayload::Task { value } = &record.payload {
        if matches!(value.status(), TaskStatus::Terminal) {
            return Err(corruption(format!(
                "Invalid live task record in {description}"
            )));
        }
    }
    Ok(record)
}

fn jsonl_line<T: Serialize>(value: &T) -> Result<String> {
    let mut line = serde_json::to_string(value)?;
    line.push('\n');
    Ok(line)
}

fn doc_file(id: DocumentId) -> String {
    format!("doc-{}.jsonl", id.0)
}

fn task_file(id: u64) -> String {
    format!("task-{id}.jsonl")
}

fn sidecar_id(name: &str) -> Option<(bool, u64)> {
    let stem = name.strip_suffix(".jsonl")?;
    let (kind, digits) = stem.split_once('-')?;
    let task = match kind {
        "doc" => false,
        "task" => true,
        _ => return None,
    };
    let canonical = digits == "0" || (!digits.starts_with('0') && !digits.is_empty());
    if !canonical || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    Some((task, digits.parse().ok()?))
}

fn is_reclaim_name(name: &str) -> bool {
    name.strip_suffix(RECLAIM_SUFFIX)
        .is_some_and(|stem| sidecar_id(stem).is_some())
}

fn key(file: &str, seq: u64, ordinal: u64) -> RecordKey {
    (file.to_owned(), seq, ordinal)
}

fn encode(seq: Seq, writes: &[StorageWrite]) -> Result<Encoded> {
    let mut main = Vec::new();
    let mut sidecars = Sidecars {
        seq: seq.0,
        ..Sidecars::default()
    };
    for write in writes {
        match write {
            StorageWrite::Table(change) => match change.as_ref() {
                TableChange::Conversation(value) => main.push(MainOperation::Conversation {
                    value: value.clone(),
                }),
                TableChange::Entry(value) => main.push(MainOperation::Entry {
                    value: value.clone(),
                }),
                TableChange::Submission(value) => main.push(MainOperation::Submission {
                    value: value.clone(),
                }),
                TableChange::Task(value) => {
                    if matches!(value.status(), TaskStatus::Terminal) {
                        main.push(MainOperation::Task {
                            value: value.as_ref().clone(),
                        });
                    } else {
                        let ordinal = sidecars.push(
                            task_file(value.id.get()),
                            SidecarPayload::Task {
                                value: value.as_ref().clone(),
                            },
                        );
                        main.push(MainOperation::TaskSidecar {
                            id: value.id,
                            ordinal,
                        });
                    }
                }
            },
            StorageWrite::DocumentCreate { record, content } => {
                let ordinal = sidecars.push(
                    doc_file(record.id),
                    SidecarPayload::Document {
                        id: record.id,
                        content: DocumentContent::Base(content.clone()),
                    },
                );
                main.push(MainOperation::DocumentCreate {
                    record: record.clone(),
                    ordinal,
                });
            }
            StorageWrite::DocumentChange { id, content } => {
                let ordinal = sidecars.push(
                    doc_file(*id),
                    SidecarPayload::Document {
                        id: *id,
                        content: content.clone(),
                    },
                );
                main.push(MainOperation::DocumentChange { id: *id, ordinal });
            }
            StorageWrite::DocumentRetire { id } => {
                main.push(MainOperation::DocumentRetire { id: *id })
            }
            StorageWrite::DocumentCopy { .. } => {
                return Err(corruption("Prepared document copy was not resolved"));
            }
        }
    }
    let marker = jsonl_line(&MainMarker {
        format: FORMAT_VERSION,
        kind: "commit".to_owned(),
        seq,
        writes: main,
    })?;
    let sidecars = sidecars
        .files
        .iter()
        .map(|(file, records)| {
            let content = records.iter().map(jsonl_line).collect::<Result<String>>()?;
            Ok((file.clone(), content))
        })
        .collect::<Result<_>>()?;
    Ok(Encoded { marker, sidecars })
}

fn plan_reclamations(
    state: &State,
    writes: &[StorageWrite],
    encoded: &Encoded,
) -> BTreeMap<String, String> {
    let mut created = HashSet::new();
    let mut retired = HashSet::new();
    let mut bases = HashSet::new();
    let mut final_tasks = BTreeMap::new();
    for write in writes {
        match write {
            StorageWrite::DocumentCreate { record, .. } if is_current_only(record) => {
                created.insert(record.id);
            }
            StorageWrite::DocumentChange {
                id,
                content: DocumentContent::Base(_),
            } => {
                bases.insert(*id);
            }
            StorageWrite::DocumentRetire { id } => {
                retired.insert(*id);
            }
            StorageWrite::Table(change) => {
                if let TableChange::Task(task) = change.as_ref() {
                    final_tasks
                        .insert(task.id.get(), matches!(task.status(), TaskStatus::Terminal));
                }
            }
            _ => {}
        }
    }
    let current_only =
        |id: &DocumentId| state.current_only_documents.contains(id) || created.contains(id);
    let mut replacements = BTreeMap::new();
    for id in &retired {
        if current_only(id) {
            replacements.insert(doc_file(*id), String::new());
        }
    }
    for id in &bases {
        if !current_only(id) || retired.contains(id) {
            continue;
        }
        let file = doc_file(*id);
        if let Some(content) = encoded.sidecars.get(&file) {
            replacements.insert(file, content.clone());
        }
    }
    for (id, terminal) in final_tasks {
        let file = task_file(id);
        if terminal
            && (state.live_task_sidecars.contains(&id) || encoded.sidecars.contains_key(&file))
        {
            replacements.insert(file, String::new());
        }
    }
    replacements
}

fn adopt(state: &mut State, writes: &[StorageWrite]) {
    for write in writes {
        match write {
            StorageWrite::DocumentCreate { record, .. } if is_current_only(record) => {
                state.current_only_documents.insert(record.id);
            }
            StorageWrite::Table(change) => {
                if let TableChange::Task(task) = change.as_ref() {
                    if matches!(task.status(), TaskStatus::Terminal) {
                        state.live_task_sidecars.remove(&task.id.get());
                    } else {
                        state.live_task_sidecars.insert(task.id.get());
                    }
                }
            }
            _ => {}
        }
    }
}

fn confirm<'a>(
    records: &HashMap<RecordKey, &'a SidecarRecord>,
    confirmed: &mut HashSet<RecordKey>,
    file: &str,
    seq: u64,
    ordinal: u64,
    optional: bool,
) -> Result<Option<&'a SidecarRecord>> {
    let record_key = key(file, seq, ordinal);
    if confirmed.contains(&record_key) {
        return Err(corruption("Sidecar record is confirmed more than once"));
    }
    match records.get(&record_key) {
        None if optional => Ok(None),
        None => Err(corruption(format!(
            "Missing confirmed sidecar record {file} at sequence {seq}"
        ))),
        Some(record) => {
            confirmed.insert(record_key);
            Ok(Some(*record))
        }
    }
}

fn confirm_document(
    records: &HashMap<RecordKey, &SidecarRecord>,
    confirmed: &mut HashSet<RecordKey>,
    id: DocumentId,
    seq: u64,
    ordinal: u64,
    optional: bool,
) -> Result<Option<DocumentContent>> {
    let Some(record) = confirm(records, confirmed, &doc_file(id), seq, ordinal, optional)? else {
        return Ok(None);
    };
    match &record.payload {
        SidecarPayload::Document {
            id: payload_id,
            content,
        } if *payload_id == id => Ok(Some(content.clone())),
        _ => Err(corruption(format!(
            "Confirmed document sidecar data does not match commit {seq}"
        ))),
    }
}

impl<E: ExecutionEnv + Send + Sync> JsonlStorage<E> {
    pub fn open(
        env: E,
        directory: &str,
        options: JsonlStorageOptions,
        cancel: &CancelToken,
    ) -> Result<Self> {
        let directory = env
            .absolute_path(directory, cancel)
            .map_err(|error| file_error("path resolution", error))?;
        env.create_dir(&directory, true, cancel)
            .map_err(|error| file_error("directory creation", error))?;
        let main_path = env
            .join_path(&[&directory, MAIN_FILE], cancel)
            .map_err(|error| file_error("path join", error))?;
        let storage = Self {
            env,
            directory,
            main_path,
            fsync: options.fsync,
            memory: MemoryStorage::new(),
            state: Mutex::new(State {
                next_seq: 1,
                ..State::default()
            }),
        };
        storage.recover(cancel)?;
        Ok(storage)
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().expect("JSONL state lock poisoned")
    }

    fn usable(&self) -> Result<MutexGuard<'_, State>> {
        let state = self.state();
        if state.closed {
            return Err(Error::Invalid("JsonlStorage is closed".to_owned()));
        }
        if let Some(cause) = &state.poisoned {
            return Err(poisoned_error(cause));
        }
        Ok(state)
    }

    fn store(&self) -> Result<&MemoryStorage> {
        drop(self.usable()?);
        Ok(&self.memory)
    }

    fn resolve(&self, file: &str, cancel: &CancelToken) -> Result<String> {
        self.env
            .join_path(&[&self.directory, file], cancel)
            .map_err(|error| file_error("path join", error))
    }

    fn reclaim(&self, replacements: &BTreeMap<String, String>, cancel: &CancelToken) {
        if replacements.is_empty() {
            return;
        }
        if self.fsync && self.env.flush_file(&self.main_path, cancel).is_err() {
            return;
        }
        for (file, content) in replacements {
            self.replace_sidecar(file, content, cancel);
        }
    }

    fn replace_sidecar(&self, file: &str, content: &str, cancel: &CancelToken) {
        let Ok(path) = self.resolve(file, cancel) else {
            return;
        };
        if content.is_empty() {
            let _ = self.env.remove(&path, false, true, cancel);
            return;
        }
        let temporary = format!("{file}{RECLAIM_SUFFIX}");
        let Ok(temporary) = self.resolve(&temporary, cancel) else {
            return;
        };
        if self
            .env
            .write_file(&temporary, content.as_bytes(), cancel)
            .is_err()
        {
            return;
        }
        if self.fsync && self.env.flush_file(&temporary, cancel).is_err() {
            return;
        }
        let _ = self.env.rename_file(&temporary, &path, cancel);
    }

    fn read_lines<T>(
        &self,
        path: &str,
        name: &str,
        cancel: &CancelToken,
        parse: impl Fn(&str, u64) -> Result<T>,
    ) -> Result<ParsedFile<T>> {
        let bytes = match self.env.read_binary_file(path, cancel) {
            Ok(bytes) => bytes,
            Err(error) if error.code == FileErrorCode::NotFound => {
                return Ok(ParsedFile {
                    path: path.to_owned(),
                    lines: Vec::new(),
                });
            }
            Err(error) => return Err(file_error(&format!("read of {name}"), error)),
        };
        let mut complete = bytes.len();
        if complete > 0 && bytes[complete - 1] != b'\n' {
            complete = bytes
                .iter()
                .rposition(|byte| *byte == b'\n')
                .map_or(0, |index| index + 1);
            self.env
                .truncate_file(path, complete as u64, cancel)
                .map_err(|error| file_error(&format!("torn-line truncation of {name}"), error))?;
        }
        let complete = &bytes[..complete];
        let mut lines = Vec::new();
        let mut start = 0;
        let newlines = complete
            .iter()
            .enumerate()
            .filter(|(_, byte)| **byte == b'\n')
            .map(|(index, _)| index);
        for (number, end) in (1..).zip(newlines) {
            let text = std::str::from_utf8(&complete[start..end]).map_err(|error| {
                Error::StorageRejected {
                    message: format!("Invalid UTF-8 in complete {name} line {number}"),
                    cause: Some(Box::new(error)),
                }
            })?;
            lines.push(ParsedLine {
                value: parse(text, number)?,
                start: start as u64,
            });
            start = end + 1;
        }
        Ok(ParsedFile {
            path: path.to_owned(),
            lines,
        })
    }

    fn recover(&self, cancel: &CancelToken) -> Result<()> {
        let main = self.read_lines(&self.main_path, MAIN_FILE, cancel, parse_marker)?;
        let mut previous = 0;
        for line in &main.lines {
            if line.value.seq.0 <= previous {
                return Err(corruption(format!(
                    "Commit sequence does not strictly increase in {MAIN_FILE}"
                )));
            }
            previous = line.value.seq.0;
        }

        let listed = self
            .env
            .list_dir(&self.directory, cancel)
            .map_err(|error| file_error("directory listing", error))?;
        for info in &listed {
            if matches!(info.kind, FileKind::File) && is_reclaim_name(&info.name) {
                let _ = self.env.remove(&info.path, false, true, cancel);
            }
        }
        let mut names: Vec<&str> = listed
            .iter()
            .filter(|info| matches!(info.kind, FileKind::File) && sidecar_id(&info.name).is_some())
            .map(|info| info.name.as_str())
            .collect();
        names.sort_unstable();
        let mut files = Vec::new();
        for name in names {
            let Some((task, id)) = sidecar_id(name) else {
                continue;
            };
            let path = self.resolve(name, cancel)?;
            let parsed = self.read_lines(&path, name, cancel, |text, line| {
                parse_record(text, name, line)
            })?;
            let mut previous: Option<(u64, u64)> = None;
            for line in &parsed.lines {
                let current = (line.value.seq.0, line.value.ordinal);
                if previous.is_some_and(|last| current <= last) {
                    return Err(corruption(format!(
                        "Sidecar records are out of order in {name}"
                    )));
                }
                previous = Some(current);
            }
            files.push(SidecarFile {
                name: name.to_owned(),
                task,
                id,
                parsed,
            });
        }
        let mut records: HashMap<RecordKey, &SidecarRecord> = HashMap::new();
        for file in &files {
            for line in &file.parsed.lines {
                records.insert(
                    key(&file.name, line.value.seq.0, line.value.ordinal),
                    &line.value,
                );
            }
        }

        let mut current_only: HashSet<DocumentId> = HashSet::new();
        let mut retired: HashSet<DocumentId> = HashSet::new();
        let mut live_tasks: BTreeMap<u64, bool> = BTreeMap::new();
        for line in &main.lines {
            for operation in &line.value.writes {
                match operation {
                    MainOperation::DocumentCreate { record, .. } => {
                        if is_current_only(record) {
                            current_only.insert(record.id);
                        }
                    }
                    MainOperation::DocumentRetire { id } => {
                        retired.insert(*id);
                    }
                    MainOperation::Task { value } => {
                        live_tasks.insert(value.id.get(), false);
                    }
                    MainOperation::TaskSidecar { id, .. } => {
                        live_tasks.insert(id.get(), true);
                    }
                    _ => {}
                }
            }
        }
        let retired_current_only: HashSet<DocumentId> = retired
            .iter()
            .filter(|id| current_only.contains(id))
            .copied()
            .collect();

        let mut latest_bases: HashMap<DocumentId, (u64, u64)> = HashMap::new();
        for line in &main.lines {
            let seq = line.value.seq.0;
            for operation in &line.value.writes {
                let (id, ordinal) = match operation {
                    MainOperation::DocumentCreate { record, ordinal } => (record.id, *ordinal),
                    MainOperation::DocumentChange { id, ordinal } => (*id, *ordinal),
                    _ => continue,
                };
                if !current_only.contains(&id) {
                    continue;
                }
                let Some(record) = records.get(&key(&doc_file(id), seq, ordinal)) else {
                    continue;
                };
                let SidecarPayload::Document {
                    id: payload_id,
                    content: DocumentContent::Base(_),
                } = &record.payload
                else {
                    continue;
                };
                if *payload_id != id {
                    continue;
                }
                let position = (record.seq.0, record.ordinal);
                if latest_bases.get(&id).is_none_or(|old| position > *old) {
                    latest_bases.insert(id, position);
                }
            }
        }
        let before_latest = |id: DocumentId, seq: u64, ordinal: u64| {
            latest_bases
                .get(&id)
                .is_some_and(|&(base_seq, base_ordinal)| {
                    seq < base_seq || (seq == base_seq && ordinal < base_ordinal)
                })
        };
        let terminal: HashSet<u64> = live_tasks
            .iter()
            .filter(|(_, live)| !**live)
            .map(|(id, _)| *id)
            .collect();

        let mut confirmed: HashSet<RecordKey> = HashSet::new();
        for line in &main.lines {
            let marker = &line.value;
            let seq = marker.seq.0;
            let mut writes = Vec::new();
            for operation in &marker.writes {
                match operation {
                    MainOperation::Conversation { value } => writes.push(StorageWrite::Table(
                        Box::new(TableChange::Conversation(value.clone())),
                    )),
                    MainOperation::Entry { value } => writes.push(StorageWrite::Table(Box::new(
                        TableChange::Entry(value.clone()),
                    ))),
                    MainOperation::Submission { value } => writes.push(StorageWrite::Table(
                        Box::new(TableChange::Submission(value.clone())),
                    )),
                    MainOperation::Task { value } => writes.push(StorageWrite::Table(Box::new(
                        TableChange::Task(Box::new(value.clone())),
                    ))),
                    MainOperation::DocumentRetire { id } => {
                        writes.push(StorageWrite::DocumentRetire { id: *id });
                    }
                    MainOperation::TaskSidecar { id, ordinal } => {
                        let optional = terminal.contains(&id.get());
                        let record = confirm(
                            &records,
                            &mut confirmed,
                            &task_file(id.get()),
                            seq,
                            *ordinal,
                            optional,
                        )?;
                        if let Some(record) = record {
                            let SidecarPayload::Task { value } = &record.payload else {
                                return Err(corruption(format!(
                                    "Confirmed task sidecar data does not match commit {seq}"
                                )));
                            };
                            if value.id.get() != id.get() {
                                return Err(corruption(format!(
                                    "Confirmed task sidecar data does not match commit {seq}"
                                )));
                            }
                            if !optional {
                                writes.push(StorageWrite::Table(Box::new(TableChange::Task(
                                    Box::new(value.clone()),
                                ))));
                            }
                        }
                    }
                    MainOperation::DocumentCreate { record, ordinal } => {
                        let id = record.id;
                        let reclaimed =
                            retired_current_only.contains(&id) || before_latest(id, seq, *ordinal);
                        let content = confirm_document(
                            &records,
                            &mut confirmed,
                            id,
                            seq,
                            *ordinal,
                            reclaimed,
                        )?;
                        let content = match content {
                            Some(DocumentContent::Delta(_)) => {
                                return Err(corruption(format!(
                                    "Document creation lacks a confirmed base in commit {seq}"
                                )));
                            }
                            Some(DocumentContent::Base(base)) if !reclaimed => base,
                            _ => DocumentBase {
                                version: 1,
                                value: Map::new(),
                            },
                        };
                        writes.push(StorageWrite::DocumentCreate {
                            record: record.clone(),
                            content,
                        });
                    }
                    MainOperation::DocumentChange { id, ordinal } => {
                        let reclaimed =
                            retired_current_only.contains(id) || before_latest(*id, seq, *ordinal);
                        let content = confirm_document(
                            &records,
                            &mut confirmed,
                            *id,
                            seq,
                            *ordinal,
                            reclaimed,
                        )?;
                        if let (false, Some(content)) = (reclaimed, content) {
                            writes.push(StorageWrite::DocumentChange { id: *id, content });
                        }
                    }
                }
            }
            let mut core = self.memory.lock();
            let prepared: Prepared =
                core.prepare(&writes, marker.seq)
                    .map_err(|cause| Error::StorageRejected {
                        message: format!("Invalid committed state at sequence {seq}"),
                        cause: Some(Box::new(cause)),
                    })?;
            core.apply(prepared);
        }

        let mut reclamations = BTreeMap::new();
        for file in &files {
            let mut unconfirmed_at: Option<u64> = None;
            for line in &file.parsed.lines {
                let record_key = key(&file.name, line.value.seq.0, line.value.ordinal);
                if confirmed.contains(&record_key) {
                    if unconfirmed_at.is_some() {
                        return Err(corruption(format!(
                            "Confirmed record follows an unconfirmed tail in {}",
                            file.name
                        )));
                    }
                } else if unconfirmed_at.is_none() {
                    unconfirmed_at = Some(line.start);
                }
            }
            if let Some(size) = unconfirmed_at {
                self.env
                    .truncate_file(&file.parsed.path, size, cancel)
                    .map_err(|error| {
                        file_error(&format!("tail truncation of {}", file.name), error)
                    })?;
            }

            let confirmed_lines: Vec<&SidecarRecord> = file
                .parsed
                .lines
                .iter()
                .filter(|line| {
                    confirmed.contains(&key(&file.name, line.value.seq.0, line.value.ordinal))
                })
                .map(|line| &line.value)
                .collect();
            let retained: Option<Vec<&SidecarRecord>> = if file.task {
                terminal.contains(&file.id).then(Vec::new)
            } else if retired_current_only.contains(&DocumentId(file.id)) {
                Some(Vec::new())
            } else if latest_bases.contains_key(&DocumentId(file.id)) {
                Some(
                    confirmed_lines
                        .iter()
                        .copied()
                        .filter(|record| {
                            !before_latest(DocumentId(file.id), record.seq.0, record.ordinal)
                        })
                        .collect(),
                )
            } else {
                None
            };
            if let Some(retained) = retained {
                if retained.len() < confirmed_lines.len() || retained.is_empty() {
                    let content = retained
                        .iter()
                        .map(|record| jsonl_line(record))
                        .collect::<Result<String>>()?;
                    reclamations.insert(file.name.clone(), content);
                }
            }
        }
        self.reclaim(&reclamations, cancel);

        let mut state = self.state();
        state.current_only_documents.extend(current_only);
        state.live_task_sidecars.extend(
            live_tasks
                .iter()
                .filter(|(_, live)| **live)
                .map(|(id, _)| *id),
        );
        state.next_seq = main.lines.last().map_or(1, |line| line.value.seq.0 + 1);
        Ok(())
    }
}

pub fn open_node(
    directory: &str,
    options: JsonlStorageOptions,
    cancel: &CancelToken,
) -> Result<JsonlStorage<NodeExecutionEnv>> {
    let cwd = std::env::current_dir().map_err(|error| Error::StorageRejected {
        message: format!("JSONL current directory failed: {error}"),
        cause: Some(Box::new(error)),
    })?;
    let env = NodeExecutionEnv::new(NodeOptions {
        cwd: cwd.to_string_lossy().into_owned(),
        shell_path: None,
        shell_env: BTreeMap::new(),
        watch: WatchOptions::default(),
    });
    JsonlStorage::open(env, directory, options, cancel)
}

impl<E: ExecutionEnv + Send + Sync> JsonlStorage<E> {
    fn append(
        &self,
        state: &mut State,
        path: &str,
        content: &str,
        action: &str,
        cancel: &CancelToken,
    ) -> Result<()> {
        self.env
            .append_file(path, content.as_bytes(), cancel)
            .map_err(|error| poison(state, file_error(action, error)))
    }

    fn flush(
        &self,
        state: &mut State,
        path: &str,
        action: &str,
        cancel: &CancelToken,
    ) -> Result<()> {
        self.env
            .flush_file(path, cancel)
            .map_err(|error| poison(state, file_error(action, error)))
    }
}

impl<E: ExecutionEnv + Send + Sync> Storage for JsonlStorage<E> {
    fn commit(&self, writes: &[StorageWrite], cancel: &CancelToken) -> Result<Seq> {
        let mut state = self.usable()?;
        let seq = Seq(state.next_seq);
        let mut core = self.memory.lock();
        let prepared = core.prepare(writes, seq)?;
        let encoded = encode(seq, &prepared.writes)?;
        let reclamations = plan_reclamations(&state, &prepared.writes, &encoded);
        let mut sidecars = Vec::with_capacity(encoded.sidecars.len());
        for (file, content) in &encoded.sidecars {
            sidecars.push((self.resolve(file, cancel)?, file, content));
        }
        for (path, file, content) in &sidecars {
            self.append(
                &mut state,
                path,
                content,
                &format!("append to {file}"),
                cancel,
            )?;
        }
        if self.fsync {
            for (path, file, _) in &sidecars {
                self.flush(&mut state, path, &format!("flush of {file}"), cancel)?;
            }
        }
        self.append(
            &mut state,
            &self.main_path,
            &encoded.marker,
            &format!("append to {MAIN_FILE}"),
            cancel,
        )?;
        adopt(&mut state, &prepared.writes);
        state.next_seq = seq.0 + 1;
        let seq = core.apply(prepared);
        drop(core);
        drop(state);
        self.reclaim(&reclamations, cancel);
        Ok(seq)
    }

    fn mint_id(&self) -> Result<u64> {
        self.store()?.mint_id()
    }

    fn conversation(
        &self,
        id: ConversationId,
        cancel: &CancelToken,
    ) -> Result<Option<ConversationRecord>> {
        self.store()?.conversation(id, cancel)
    }

    fn scan_conversations(
        &self,
        query: &ConversationQuery,
        limit: usize,
        cursor: Option<&Cursor>,
        cancel: &CancelToken,
    ) -> Result<Page<ConversationRecord>> {
        self.store()?
            .scan_conversations(query, limit, cursor, cancel)
    }

    fn entry(&self, id: EntryId, cancel: &CancelToken) -> Result<Option<CommittedEntry>> {
        self.store()?.entry(id, cancel)
    }

    fn visible_entry(
        &self,
        conversation_id: ConversationId,
        id: EntryId,
        cancel: &CancelToken,
    ) -> Result<Option<CommittedEntry>> {
        self.store()?.visible_entry(conversation_id, id, cancel)
    }

    fn find_latest_head_marker(
        &self,
        conversation_id: ConversationId,
        at_or_before: Option<EntryId>,
        cancel: &CancelToken,
    ) -> Result<Option<HeadMarker>> {
        self.store()?
            .find_latest_head_marker(conversation_id, at_or_before, cancel)
    }

    fn scan_entries(
        &self,
        query: &EntryQuery,
        limit: usize,
        cursor: Option<&Cursor>,
        cancel: &CancelToken,
    ) -> Result<Page<EntryRecord>> {
        self.store()?.scan_entries(query, limit, cursor, cancel)
    }

    fn task(&self, id: TaskId, cancel: &CancelToken) -> Result<Option<AnyTaskRecord>> {
        self.store()?.task(id, cancel)
    }

    fn scan_tasks(
        &self,
        query: &TaskQuery,
        limit: usize,
        cursor: Option<&Cursor>,
        cancel: &CancelToken,
    ) -> Result<Page<AnyTaskRecord>> {
        self.store()?.scan_tasks(query, limit, cursor, cancel)
    }

    fn submission(
        &self,
        id: SubmissionId,
        cancel: &CancelToken,
    ) -> Result<Option<SubmissionRecord>> {
        self.store()?.submission(id, cancel)
    }

    fn scan_submissions(
        &self,
        query: &SubmissionQuery,
        limit: usize,
        cursor: Option<&Cursor>,
        cancel: &CancelToken,
    ) -> Result<Page<SubmissionRecord>> {
        self.store()?.scan_submissions(query, limit, cursor, cancel)
    }

    fn submission_by_request(
        &self,
        conversation_id: ConversationId,
        request_id: &str,
        cancel: &CancelToken,
    ) -> Result<Option<SubmissionRecord>> {
        self.store()?
            .submission_by_request(conversation_id, request_id, cancel)
    }

    fn find_document(
        &self,
        address: &DocumentAddress,
        at: DocumentPoint,
        cancel: &CancelToken,
    ) -> Result<Option<DocumentRecord>> {
        self.store()?.find_document(address, at, cancel)
    }

    fn document(
        &self,
        id: DocumentId,
        at: DocumentPoint,
        cancel: &CancelToken,
    ) -> Result<Option<StoredDocument>> {
        self.store()?.document(id, at, cancel)
    }

    fn scan_documents(
        &self,
        query: &DocumentQuery,
        limit: usize,
        cursor: Option<&Cursor>,
        cancel: &CancelToken,
    ) -> Result<Page<DocumentRecord>> {
        self.store()?.scan_documents(query, limit, cursor, cancel)
    }

    fn close(&self, cancel: &CancelToken) -> Result<()> {
        let mut state = self.state();
        if state.closed {
            return Ok(());
        }
        state.closed = true;
        drop(state);
        self.memory.close(cancel)
    }
}
