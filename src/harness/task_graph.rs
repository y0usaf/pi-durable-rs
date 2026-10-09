use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use serde::Serialize;
use serde_json::{Map, Value, json};

use crate::cancel::CancelToken;
use crate::chord::delta::{Op, Seg, apply};
use crate::errors::Result;
use crate::harness::util::scan_all;
use crate::harness::view::{Observer, notify, replicate};
use crate::ids::{ConversationId, TaskId};
use crate::session::kernel::Session;
use crate::session::observation::AttachedState;
use crate::tasks::{JoinPolicy, TaskState, TaskStatus};
use crate::types::Storage;
use crate::types::{
    AnyTaskRecord, CommitChange, CommitPublication, ConversationQuery, TableChange, TaskQuery,
};

const LOCK: &str = "task graph lock poisoned";
const SCAN_PAGE_SIZE: usize = 256;
const LIVE: [TaskStatus; 4] = [
    TaskStatus::Pending,
    TaskStatus::Running,
    TaskStatus::Waiting,
    TaskStatus::Completing,
];

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(tag = "status", rename_all = "lowercase")]
pub enum TaskGraphState {
    Pending {
        phase: String,
    },
    Running {
        phase: String,
    },
    Waiting {
        phase: String,
        on: Vec<TaskId>,
        policy: JoinPolicy,
    },
    Completing {
        outcome: String,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TaskGraphNode {
    pub id: TaskId,
    pub kind: String,
    pub conversation_id: ConversationId,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<TaskId>,
    pub background: bool,
    pub abort_requested: bool,
    pub state: TaskGraphState,
    pub conversations: Vec<ConversationId>,
}

struct Mount {
    nodes: BTreeMap<u64, TaskGraphNode>,
    value: Value,
    observers: Vec<(u64, Observer)>,
}

#[derive(Default)]
struct State {
    next: u64,
    mount: Option<Mount>,
}

pub struct TaskGraphView {
    state: Arc<Mutex<State>>,
}

pub struct GraphGuard {
    state: Arc<Mutex<State>>,
    observer: u64,
}

impl Drop for GraphGuard {
    fn drop(&mut self) {
        let mut state = self.state.lock().expect(LOCK);
        let Some(mount) = state.mount.as_mut() else {
            return;
        };
        mount.observers.retain(|(id, _)| *id != self.observer);
        if mount.observers.is_empty() {
            state.mount = None;
        }
    }
}

impl TaskGraphView {
    pub fn new(session: &Session) -> Result<Self> {
        let state = Arc::new(Mutex::new(State::default()));
        let listener_state = Arc::clone(&state);
        let deliveries = session.deliveries();
        let listener_deliveries = Arc::clone(&deliveries);
        let _unsubscribe = session.subscribe_commits(Box::new(move |publication, _cancel| {
            let mut state = listener_state.lock().expect(LOCK);
            let Some(mount) = state.mount.as_mut() else {
                return;
            };
            let before = mount.value.clone();
            let ops = advance(mount, publication);
            if ops.is_empty() {
                return;
            }
            notify(
                &listener_deliveries,
                &mount.observers,
                &before,
                &mount.value,
                &ops,
                publication,
            );
        }))?;
        Ok(Self { state })
    }

    pub fn attach(
        &self,
        session: &Session,
        observer: Observer,
        cancel: &CancelToken,
    ) -> Result<(Value, GraphGuard)> {
        let (value, guard, ()) = self.attach_with(session, cancel, |_, _| Ok((observer, ())))?;
        Ok((value, guard))
    }

    pub fn attach_with<R>(
        &self,
        session: &Session,
        cancel: &CancelToken,
        create: impl FnOnce(&Value, &dyn Storage) -> Result<(Observer, R)>,
    ) -> Result<(Value, GraphGuard, R)> {
        session.read_on_line(|| {
            let known = self.state.lock().expect(LOCK).mount.is_some();
            let built = match known {
                true => None,
                false => Some(build(session, cancel)?),
            };
            let value = match &built {
                Some(mount) => mount.value.clone(),
                None => self
                    .state
                    .lock()
                    .expect(LOCK)
                    .mount
                    .as_ref()
                    .expect("mount exists")
                    .value
                    .clone(),
            };
            let (observer, extra) = create(&value, session.storage())?;
            let mut state = self.state.lock().expect(LOCK);
            if let Some(mount) = built {
                state.mount = Some(mount);
            }
            state.next += 1;
            let token = state.next;
            let mount = state.mount.as_mut().expect("mount inserted above");
            mount.observers.push((token, observer));
            let guard = GraphGuard {
                state: Arc::clone(&self.state),
                observer: token,
            };
            Ok((value, guard, extra))
        })
    }

    pub fn state(
        &self,
        session: &Session,
        cancel: &CancelToken,
    ) -> Result<AttachedState<GraphGuard>> {
        let (_, guard, attached) =
            self.attach_with(session, cancel, |value, _| Ok(replicate(value)))?;
        Ok(AttachedState::new(attached, guard))
    }
}

fn build(session: &Session, cancel: &CancelToken) -> Result<Mount> {
    let storage = session.storage();
    let mut records: Vec<AnyTaskRecord> = Vec::new();
    for status in LIVE {
        let query = TaskQuery {
            status: Some(status),
            ..TaskQuery::default()
        };
        records.extend(scan_all(|cursor| {
            storage.scan_tasks(&query, SCAN_PAGE_SIZE, cursor.as_ref(), cancel)
        })?);
    }
    records.sort_by_key(|record| record.id.get());
    let mut nodes = BTreeMap::new();
    for record in records {
        let query = ConversationQuery {
            owner_task_id: Some(record.id),
            ..ConversationQuery::default()
        };
        let owned = scan_all(|cursor| {
            storage.scan_conversations(&query, SCAN_PAGE_SIZE, cursor.as_ref(), cancel)
        })?;
        let mut conversations: Vec<ConversationId> = owned.iter().map(|record| record.id).collect();
        conversations.sort();
        let node = node_of(&record, conversations).expect("live tasks have graph nodes");
        nodes.insert(record.id.get(), node);
    }
    let value = graph_value(&nodes);
    Ok(Mount {
        nodes,
        value,
        observers: Vec::new(),
    })
}

fn graph_value(nodes: &BTreeMap<u64, TaskGraphNode>) -> Value {
    let mut tasks = Map::new();
    for (key, node) in nodes {
        tasks.insert(
            key.to_string(),
            serde_json::to_value(node).expect("graph nodes serialize"),
        );
    }
    json!({ "tasks": tasks })
}

fn advance(mount: &mut Mount, publication: &CommitPublication) -> Vec<Op> {
    let mut ops = Vec::new();
    let mut changed: BTreeMap<u64, Option<TaskGraphNode>> = BTreeMap::new();
    for change in &publication.changes {
        let CommitChange::Table(TableChange::Task(record)) = change else {
            continue;
        };
        let key = record.id.get();
        let previous = match changed.get(&key) {
            Some(node) => node.clone(),
            None => mount.nodes.get(&key).cloned(),
        };
        let Some(state) = graph_state(&record.state) else {
            if previous.is_none() {
                continue;
            }
            ops.push(Op::Delete(task_path(key)));
            changed.insert(key, None);
            continue;
        };
        let conversations = previous.map(|node| node.conversations).unwrap_or_default();
        let next = node_of_state(record, state, conversations);
        if mount.nodes.get(&key) == Some(&next) {
            continue;
        }
        ops.push(Op::Set(
            task_path(key),
            serde_json::to_value(&next).expect("graph nodes serialize"),
        ));
        changed.insert(key, Some(next));
    }
    let mut created: BTreeMap<u64, Vec<ConversationId>> = BTreeMap::new();
    for change in &publication.changes {
        let CommitChange::Table(TableChange::Conversation(conversation)) = change else {
            continue;
        };
        let Some(owner) = &conversation.owner else {
            continue;
        };
        let key = owner.task_id.get();
        if current(mount, &changed, key).is_some() {
            created.entry(key).or_default().push(conversation.id);
        }
    }
    for (key, ids) in created {
        let node = current(mount, &changed, key).expect("owner node checked above");
        let mut conversations = node.conversations.clone();
        conversations.extend(ids);
        conversations.sort();
        ops.push(Op::Set(
            [task_path(key), vec![Seg::Key("conversations".into())]].concat(),
            serde_json::to_value(&conversations).expect("conversation ids serialize"),
        ));
        changed.insert(
            key,
            Some(TaskGraphNode {
                conversations,
                ..node
            }),
        );
    }
    if ops.is_empty() {
        return ops;
    }
    mount.value = apply(Some(mount.value.clone()), &ops).expect("graph operations apply");
    for (key, node) in changed {
        match node {
            Some(node) => mount.nodes.insert(key, node),
            None => mount.nodes.remove(&key),
        };
    }
    ops
}

fn current(
    mount: &Mount,
    changed: &BTreeMap<u64, Option<TaskGraphNode>>,
    key: u64,
) -> Option<TaskGraphNode> {
    match changed.get(&key) {
        Some(node) => node.clone(),
        None => mount.nodes.get(&key).cloned(),
    }
}

fn task_path(key: u64) -> Vec<Seg> {
    vec![Seg::Key("tasks".into()), Seg::Key(key.to_string())]
}

fn node_of(record: &AnyTaskRecord, conversations: Vec<ConversationId>) -> Option<TaskGraphNode> {
    let state = graph_state(&record.state)?;
    Some(node_of_state(record, state, conversations))
}

fn node_of_state(
    record: &AnyTaskRecord,
    state: TaskGraphState,
    conversations: Vec<ConversationId>,
) -> TaskGraphNode {
    TaskGraphNode {
        id: record.id,
        kind: record.kind.clone(),
        conversation_id: record.conversation_id,
        owner: record.owner,
        background: record.background,
        abort_requested: record.abort_requested,
        state,
        conversations,
    }
}

fn graph_state(state: &TaskState<Value, Value>) -> Option<TaskGraphState> {
    match state {
        TaskState::Pending { checkpoint } => Some(TaskGraphState::Pending {
            phase: phase_of(checkpoint),
        }),
        TaskState::Running { checkpoint } => Some(TaskGraphState::Running {
            phase: phase_of(checkpoint),
        }),
        TaskState::Waiting {
            checkpoint,
            on,
            policy,
        } => Some(TaskGraphState::Waiting {
            phase: phase_of(checkpoint),
            on: on.clone(),
            policy: *policy,
        }),
        TaskState::Completing { outcome } => Some(TaskGraphState::Completing {
            outcome: outcome_status(outcome),
        }),
        TaskState::Terminal { .. } => None,
    }
}

fn phase_of(checkpoint: &Value) -> String {
    checkpoint
        .get("phase")
        .and_then(Value::as_str)
        .expect("task checkpoints carry a phase")
        .to_owned()
}

fn outcome_status<R: Serialize>(outcome: &crate::tasks::TaskOutcome<R>) -> String {
    let value = serde_json::to_value(outcome).expect("task outcomes serialize");
    value
        .get("status")
        .and_then(Value::as_str)
        .expect("task outcomes carry a status")
        .to_owned()
}
