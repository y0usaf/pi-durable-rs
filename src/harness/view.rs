use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use serde_json::{Map, Value, json};

use crate::cancel::CancelToken;
use crate::chord::delta::{Op, Seg, apply};
use crate::documents::DocDefinition;
use crate::errors::{Error, Result};
use crate::harness::agent::agent_doc;
use crate::harness::context::{active_entries, capture_context_bounds};
use crate::harness::inbox::inbox_doc;
use crate::harness::live::live_doc;
use crate::harness::provider::provider_doc;
use crate::harness::usage::usage_doc;
use crate::ids::ConversationId;
use crate::session::kernel::Session;
use crate::types::{
    CommitChange, CommitPublication, ConversationRecord, DocumentCommitChange, EntryRecord,
    TableChange,
};

const LOCK: &str = "conversation view lock poisoned";

pub struct Change<'a> {
    pub before: &'a Value,
    pub after: &'a Value,
    pub ops: &'a [Op],
    pub publication: &'a CommitPublication,
}

pub type Observer = Box<dyn Fn(&Change<'_>) + Send + Sync>;

struct Mount {
    entries: Vec<EntryRecord>,
    value: Value,
    incarnations: BTreeMap<String, (crate::ids::DocumentId, u64)>,
    observers: Vec<(u64, Observer)>,
}

#[derive(Default)]
struct State {
    next: u64,
    mounts: BTreeMap<u64, Mount>,
}

pub struct ConversationViews {
    state: Arc<Mutex<State>>,
    definitions: Arc<Vec<DocDefinition>>,
}

pub struct ViewGuard {
    state: Arc<Mutex<State>>,
    conversation: u64,
    observer: u64,
}

impl Drop for ViewGuard {
    fn drop(&mut self) {
        let mut state = self.state.lock().expect(LOCK);
        let Some(mount) = state.mounts.get_mut(&self.conversation) else {
            return;
        };
        mount.observers.retain(|(id, _)| *id != self.observer);
        if mount.observers.is_empty() {
            state.mounts.remove(&self.conversation);
        }
    }
}

pub fn mounted_definitions() -> Result<Vec<DocDefinition>> {
    Ok(vec![
        agent_doc()?.definition().clone(),
        live_doc()?.definition().clone(),
        inbox_doc()?.definition().clone(),
        provider_doc()?.definition().clone(),
        usage_doc()?.definition().clone(),
    ])
}

impl ConversationViews {
    pub fn new(session: &Session) -> Result<Self> {
        let state = Arc::new(Mutex::new(State::default()));
        let listener_state = Arc::clone(&state);
        let _unsubscribe = session.subscribe_commits(Box::new(move |publication, _cancel| {
            let mut state = listener_state.lock().expect(LOCK);
            for (&conversation, mount) in state.mounts.iter_mut() {
                let id = ConversationId(conversation);
                let before = mount.value.clone();
                let ops = advance(id, mount, publication);
                for (_, observer) in &mount.observers {
                    observer(&Change {
                        before: &before,
                        after: &mount.value,
                        ops: &ops,
                        publication,
                    });
                }
            }
        }))?;
        Ok(Self {
            state,
            definitions: Arc::new(mounted_definitions()?),
        })
    }

    /// Must be called on the Session line: the build and the registration are atomic with commits.
    pub fn attach(
        &self,
        session: &Session,
        id: ConversationId,
        observer: Observer,
        cancel: &CancelToken,
    ) -> Result<(Value, ViewGuard)> {
        session.read_on_line(|| {
            let known = self.state.lock().expect(LOCK).mounts.contains_key(&id.0);
            if !known {
                let mount = build(session, id, &self.definitions, cancel)?;
                self.state.lock().expect(LOCK).mounts.insert(id.0, mount);
            }
            let mut state = self.state.lock().expect(LOCK);
            state.next += 1;
            let token = state.next;
            let mount = state.mounts.get_mut(&id.0).expect("mount inserted above");
            mount.observers.push((token, observer));
            let guard = ViewGuard {
                state: Arc::clone(&self.state),
                conversation: id.0,
                observer: token,
            };
            Ok((mount.value.clone(), guard))
        })
    }
}

fn build(
    session: &Session,
    id: ConversationId,
    definitions: &[DocDefinition],
    cancel: &CancelToken,
) -> Result<Mount> {
    let storage = session.storage();
    let conversation = storage
        .conversation(id, cancel)?
        .ok_or_else(|| Error::Invalid(format!("Conversation {} does not exist", id.0)))?;
    let bounds = capture_context_bounds(storage, id, None, cancel)?;
    let entries = active_entries(storage, id, bounds, cancel)?;
    let mut docs = Map::new();
    let mut incarnations = BTreeMap::new();
    for definition in definitions {
        let Some(loaded) = session.conversation_document_on_line(definition, id, cancel)? else {
            continue;
        };
        docs.insert(definition.kind.clone(), Value::Object(loaded.value));
        incarnations.insert(
            definition.kind.clone(),
            (loaded.record.create.id, loaded.version),
        );
    }
    let value = view_value(&conversation, &entries, docs);
    Ok(Mount {
        entries,
        value,
        incarnations,
        observers: Vec::new(),
    })
}

fn view_value(
    conversation: &ConversationRecord,
    entries: &[EntryRecord],
    docs: Map<String, Value>,
) -> Value {
    json!({
        "conversation": conversation,
        "entries": entries,
        "docs": docs,
    })
}

fn advance(id: ConversationId, mount: &mut Mount, publication: &CommitPublication) -> Vec<Op> {
    let mut doc_ops = Vec::new();
    let mut entry_ops = Vec::new();
    for change in &publication.changes {
        match change {
            CommitChange::Table(TableChange::Entry(entry)) if entry.conversation_id == id => {
                let value = serde_json::to_value(entry).expect("entry records serialize");
                match entry.head {
                    None => {
                        entry_ops.push(Op::Splice(
                            vec![Seg::Key("entries".into())],
                            mount.entries.len(),
                            0,
                            vec![value],
                        ));
                        mount.entries.push(entry.clone());
                    }
                    Some(target) => {
                        let kept = mount
                            .entries
                            .iter()
                            .position(|candidate| {
                                candidate.head.is_none() && candidate.id >= target
                            })
                            .unwrap_or(mount.entries.len());
                        entry_ops.push(Op::Splice(
                            vec![Seg::Key("entries".into())],
                            0,
                            kept,
                            vec![value],
                        ));
                        let rest = mount.entries.split_off(kept);
                        mount.entries = std::iter::once(entry.clone()).chain(rest).collect();
                    }
                }
            }
            CommitChange::Document(DocumentCommitChange::Document {
                record,
                conversation_id,
                version,
                value,
                ops,
            }) => {
                if *conversation_id != Some(id) || record.create.key.is_some() {
                    continue;
                }
                let kind = record.create.kind.clone();
                if !MOUNTED.contains(&kind.as_str()) {
                    continue;
                }
                let path = vec![Seg::Key("docs".into()), Seg::Key(kind.clone())];
                let mounted = mount.incarnations.get(&kind).copied();
                match value {
                    None => {
                        if mounted.map(|(document, _)| document) != Some(record.create.id) {
                            continue;
                        }
                        mount.incarnations.remove(&kind);
                        doc_ops.push(Op::Delete(path));
                    }
                    Some(object) => {
                        let version = version.expect("a document value carries its version");
                        if mounted == Some((record.create.id, version)) {
                            doc_ops.extend(ops.iter().map(|op| prefixed(op, &path)));
                        } else {
                            mount.incarnations.insert(kind, (record.create.id, version));
                            doc_ops.push(Op::Set(path, Value::Object(object.clone())));
                        }
                    }
                }
            }
            _ => {}
        }
    }
    if !doc_ops.is_empty() {
        mount.value =
            apply(Some(mount.value.clone()), &doc_ops).expect("view document operations apply");
    }
    if !entry_ops.is_empty() {
        mount.value["entries"] =
            serde_json::to_value(&mount.entries).expect("entry records serialize");
    }
    doc_ops.extend(entry_ops);
    doc_ops
}

const MOUNTED: [&str; 5] = ["pi.agent", "pi.live", "pi.inbox", "pi.provider", "pi.usage"];

fn prefixed(op: &Op, prefix: &[Seg]) -> Op {
    let at = |path: &[Seg]| prefix.iter().chain(path).cloned().collect::<Vec<Seg>>();
    match op {
        Op::Replace(value) => Op::Set(prefix.to_vec(), value.clone()),
        Op::Set(path, value) => Op::Set(at(path), value.clone()),
        Op::Delete(path) => Op::Delete(at(path)),
        Op::Append(path, text) => Op::Append(at(path), text.clone()),
        Op::Truncate(path, count) => Op::Truncate(at(path), *count),
        Op::Splice(path, start, remove, items) => {
            Op::Splice(at(path), *start, *remove, items.clone())
        }
        Op::Permute(path, order) => Op::Permute(at(path), order.clone()),
    }
}
