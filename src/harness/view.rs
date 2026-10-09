use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use serde_json::{Map, Value, json};

use crate::cancel::CancelToken;
use crate::chord::delta::{Op, Seg, apply};
use crate::chord::state::Attached;
use crate::documents::DocDefinition;
use crate::errors::{Error, Result};
use crate::harness::agent::agent_doc;
use crate::harness::context::{active_entries, capture_context_bounds};
use crate::harness::inbox::inbox_doc;
use crate::harness::live::live_doc;
use crate::harness::provider::provider_doc;
use crate::harness::usage::usage_doc;
use crate::ids::ConversationId;
use crate::session::kernel::{Session, report_error};
use crate::session::observation::{AttachedState, Deliveries, Sink, Watch, WatchShared, feeder};
use crate::session::types::{DocumentWatch, WatchEnd};
use crate::types::{
    CommitChange, CommitPublication, ConversationRecord, DocumentCommitChange, EntryRecord,
    TableChange,
};
use crate::types::{JsonObject, Storage};

const LOCK: &str = "conversation view lock poisoned";

pub struct Change<'a> {
    pub before: &'a Value,
    pub after: &'a Value,
    pub ops: &'a [Op],
    pub publication: &'a CommitPublication,
}

pub type Observer = Arc<dyn Fn(&Change<'_>) + Send + Sync>;

pub fn notify(
    deliveries: &Deliveries,
    observers: &[(u64, Observer)],
    before: &Value,
    after: &Value,
    ops: &[Op],
    publication: &CommitPublication,
) {
    for (_, observer) in observers {
        let observer = Arc::clone(observer);
        let before = before.clone();
        let after = after.clone();
        let ops = ops.to_vec();
        let publication = publication.clone();
        deliveries.push(move || {
            observer(&Change {
                before: &before,
                after: &after,
                ops: &ops,
                publication: &publication,
            });
        });
    }
}

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
        let deliveries = session.deliveries();
        let listener_deliveries = Arc::clone(&deliveries);
        let _unsubscribe = session.subscribe_commits(Box::new(move |publication, _cancel| {
            let mut state = listener_state.lock().expect(LOCK);
            for (&conversation, mount) in state.mounts.iter_mut() {
                let id = ConversationId(conversation);
                let before = mount.value.clone();
                let ops = advance(id, mount, publication);
                notify(
                    &listener_deliveries,
                    &mount.observers,
                    &before,
                    &mount.value,
                    &ops,
                    publication,
                );
            }
        }))?;
        Ok(Self {
            state,
            definitions: Arc::new(mounted_definitions()?),
        })
    }

    pub fn attach(
        &self,
        session: &Session,
        id: ConversationId,
        observer: Observer,
        cancel: &CancelToken,
    ) -> Result<(Value, ViewGuard)> {
        let (value, guard, ()) =
            self.attach_with(session, id, cancel, |_, _| Ok((observer, ())))?;
        Ok((value, guard))
    }

    pub fn attach_with<R>(
        &self,
        session: &Session,
        id: ConversationId,
        cancel: &CancelToken,
        create: impl FnOnce(&Value, &dyn Storage) -> Result<(Observer, R)>,
    ) -> Result<(Value, ViewGuard, R)> {
        session.read_on_line(|| {
            let known = self.state.lock().expect(LOCK).mounts.contains_key(&id.0);
            let built = match known {
                true => None,
                false => Some(build(session, id, &self.definitions, cancel)?),
            };
            let value = match &built {
                Some(mount) => mount.value.clone(),
                None => self.state.lock().expect(LOCK).mounts[&id.0].value.clone(),
            };
            let (observer, extra) = create(&value, session.storage())?;
            let mut state = self.state.lock().expect(LOCK);
            if let Some(mount) = built {
                state.mounts.insert(id.0, mount);
            }
            state.next += 1;
            let token = state.next;
            let mount = state.mounts.get_mut(&id.0).expect("mount inserted above");
            mount.observers.push((token, observer));
            let guard = ViewGuard {
                state: Arc::clone(&self.state),
                conversation: id.0,
                observer: token,
            };
            Ok((value, guard, extra))
        })
    }

    pub fn state(
        &self,
        session: &Session,
        id: ConversationId,
        cancel: &CancelToken,
    ) -> Result<AttachedState<ViewGuard>> {
        let (_, guard, attached) =
            self.attach_with(session, id, cancel, |value, _| Ok(replicate(value)))?;
        Ok(AttachedState::new(attached, guard))
    }

    pub fn watch(
        &self,
        session: &Session,
        id: ConversationId,
        cancel: &CancelToken,
    ) -> Result<DocumentWatch> {
        let deliveries = session.deliveries();
        let (_, guard, shared) = self.attach_with(session, id, cancel, |value, _| {
            let shared = WatchShared::new(value.as_object().cloned(), deliveries);
            Ok((watch_observer(&shared), shared))
        })?;
        attached_watch(shared, guard, cancel)
    }
}

pub(crate) fn watch_observer(shared: &Arc<WatchShared<JsonObject>>) -> Observer {
    let sink = Sink::Watch(Arc::clone(shared));
    Arc::new(move |change| {
        if !change.ops.is_empty() {
            sink.advance(change.after.as_object().cloned(), change.ops.to_vec());
        }
    })
}

pub(crate) fn attached_watch<G: Send + 'static>(
    shared: Arc<WatchShared<JsonObject>>,
    guard: G,
    cancel: &CancelToken,
) -> Result<DocumentWatch> {
    shared.set_detach(Box::new(move || drop(guard)));
    if cancel.is_cancelled() {
        shared.terminate(WatchEnd::Cancelled);
        return Err(Error::Aborted);
    }
    shared.observe_cancellation(cancel);
    Ok(Watch::boxed(shared))
}

pub(crate) fn replicate(value: &Value) -> (Observer, Arc<Mutex<Attached>>) {
    let attached = Arc::new(Mutex::new(Attached::new(0, value.clone(), report_error())));
    let feed = feeder(Arc::clone(&attached));
    (
        Arc::new(move |change| {
            if !change.ops.is_empty() {
                feed(change.after, change.ops);
            }
        }),
        attached,
    )
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
