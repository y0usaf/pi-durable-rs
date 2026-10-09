use std::collections::{BTreeMap, BTreeSet};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;

use crate::cancel::CancelToken;
use crate::errors::{Error, Result};
use crate::harness::registry::Registry;
use crate::harness::types::Settings;
use crate::harness::util::scan_all;
use crate::ids::{ConversationId, TaskId};
use crate::session::kernel::Session;
use crate::session::transaction::{Transaction, TransactionScope};

use crate::tasks::{TaskOutcome, TaskState, TaskStatus};
use crate::types::{
    AnyTaskRecord, CommitChange, CommitPublication, ConversationRecord, JsonValue, Storage, SubmissionBody,
    SubmissionQuery, SubmissionStatus, TableChange, TaskQuery,
};

const SCAN_PAGE_SIZE: usize = 256;
const LIVE_STATUSES: [TaskStatus; 4] = [
    TaskStatus::Pending,
    TaskStatus::Running,
    TaskStatus::Waiting,
    TaskStatus::Completing,
];

pub type SettleOutcome = Arc<
    dyn Fn(&mut Transaction<'_>, &AnyTaskRecord, &TaskOutcome<JsonValue>) -> Result<()>
        + Send
        + Sync,
>;
pub type WithdrawInputs =
    Arc<dyn Fn(&mut Transaction<'_>, ConversationId) -> Result<()> + Send + Sync>;
pub type Report = Arc<dyn Fn(Error) + Send + Sync>;
pub type SettingsSource = Arc<dyn Fn() -> Settings + Send + Sync>;
pub type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;

#[derive(Clone)]
struct TaskNode {
    conversation_id: ConversationId,
    owner: Option<TaskId>,
    background: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Up {
    Task(TaskId),
    Conversation(ConversationId),
}

enum Step {
    Task(TaskId, TaskNode),
    Conversation(ConversationId),
    Unknown,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Scope {
    Conversation(ConversationId),
    Roots,
}

#[derive(Default)]
struct Overlay {
    tasks: BTreeMap<TaskId, AnyTaskRecord>,
    edges: BTreeMap<ConversationId, Option<TaskId>>,
}


struct State {
    live: BTreeMap<TaskId, AnyTaskRecord>,
    edges: BTreeMap<ConversationId, Option<TaskId>>,
    conversation_owners: BTreeSet<TaskId>,
    settled: BTreeMap<TaskId, TaskNode>,
    fail_fast_checks: BTreeSet<TaskId>,
    reconcile_scheduled: bool,
    cascade_pending: bool,
    enabled: bool,
    closing: bool,
    wake: Sender<()>,
}

pub struct SchedulerOptions {
    pub session: Arc<Session>,
    pub storage: Arc<dyn Storage>,
    pub registry: Arc<Registry>,
    pub settings: SettingsSource,
    pub now: Clock,
    pub report: Report,
    pub settle_outcome: SettleOutcome,
    pub withdraw_inputs: WithdrawInputs,
}

struct Shared {
    session: Arc<Session>,
    storage: Arc<dyn Storage>,
    registry: Arc<Registry>,
    settings: SettingsSource,
    now: Clock,
    report: Report,
    settle_outcome: SettleOutcome,
    withdraw_inputs: WithdrawInputs,
    state: Mutex<State>,
    wake_rx: Mutex<Option<Receiver<()>>>,
}

#[derive(Clone)]
pub struct TaskScheduler(Arc<Shared>);

fn node_of(record: &AnyTaskRecord) -> TaskNode {
    TaskNode {
        conversation_id: record.conversation_id,
        owner: record.owner,
        background: record.background,
    }
}

fn parent_of(node: &TaskNode) -> Up {
    match node.owner {
        Some(task) => Up::Task(task),
        None => Up::Conversation(node.conversation_id),
    }
}

fn failed_outcome(record: &AnyTaskRecord) -> bool {
    match &record.state {
        TaskState::Completing { outcome } | TaskState::Terminal { outcome } => {
            !matches!(outcome, TaskOutcome::Completed { .. })
        }
        _ => false,
    }
}

fn cancellation_intent(record: &AnyTaskRecord) -> bool {
    !matches!(record.state, TaskState::Terminal { .. })
        && (record.abort_requested || failed_outcome(record))
}

fn is_terminal(record: &AnyTaskRecord) -> bool {
    matches!(record.state, TaskState::Terminal { .. })
}

fn is_queued_input(submission: &crate::types::SubmissionRecord) -> bool {
    matches!(submission.body, SubmissionBody::Input(_))
        && crate::storage::submission_status(submission) == SubmissionStatus::Queued
}

impl State {
    fn schedule_reconcile(&mut self) {
        if self.reconcile_scheduled || self.closing {
            return;
        }
        self.reconcile_scheduled = true;
        let _ = self.wake.send(());
    }

    fn edge(&self, id: ConversationId, overlay: Option<&Overlay>) -> Option<Option<TaskId>> {
        match overlay.and_then(|o| o.edges.get(&id)) {
            Some(owner) => Some(*owner),
            None => self.edges.get(&id).copied(),
        }
    }

    fn set_edge(&mut self, id: ConversationId, owner: Option<TaskId>) {
        self.edges.insert(id, owner);
        if let Some(task) = owner {
            self.conversation_owners.insert(task);
        }
    }

    fn node(&self, id: TaskId, overlay: Option<&Overlay>) -> Option<TaskNode> {
        if let Some(record) = overlay.and_then(|o| o.tasks.get(&id)) {
            return Some(node_of(record));
        }
        if let Some(record) = self.live.get(&id) {
            return Some(node_of(record));
        }
        self.settled.get(&id).cloned()
    }

    fn above(&self, start: Up, overlay: Option<&Overlay>) -> Vec<Step> {
        let mut steps = Vec::new();
        let mut at = Some(start);
        while let Some(current) = at {
            match current {
                Up::Task(id) => match self.node(id, overlay) {
                    Some(node) => {
                        at = Some(parent_of(&node));
                        steps.push(Step::Task(id, node));
                    }
                    None => {
                        steps.push(Step::Unknown);
                        break;
                    }
                },
                Up::Conversation(id) => {
                    steps.push(Step::Conversation(id));
                    match self.edge(id, overlay) {
                        None => {
                            steps.push(Step::Unknown);
                            break;
                        }
                        Some(None) => at = None,
                        Some(Some(task)) => at = Some(Up::Task(task)),
                    }
                }
            }
        }
        steps
    }

    fn chain_known(&self, start: Up, overlay: Option<&Overlay>) -> bool {
        !self
            .above(start, overlay)
            .iter()
            .any(|step| matches!(step, Step::Unknown))
    }

    fn live_records(&self, overlay: Option<&Overlay>) -> Vec<AnyTaskRecord> {
        let mut records = Vec::new();
        for record in self.live.values() {
            let candidate = overlay
                .and_then(|o| o.tasks.get(&record.id))
                .unwrap_or(record);
            if !is_terminal(candidate) {
                records.push(candidate.clone());
            }
        }
        if let Some(overlay) = overlay {
            for record in overlay.tasks.values() {
                if !self.live.contains_key(&record.id) && !is_terminal(record) {
                    records.push(record.clone());
                }
            }
        }
        records
    }

    fn owned_live(&self, overlay: Option<&Overlay>) -> BTreeMap<TaskId, Vec<TaskId>> {
        let mut owned: BTreeMap<TaskId, Vec<TaskId>> = BTreeMap::new();
        for record in self.live_records(overlay) {
            if record.background {
                continue;
            }
            for step in self.above(parent_of(&node_of(&record)), overlay) {
                match step {
                    Step::Unknown => break,
                    Step::Conversation(_) => {}
                    Step::Task(task, node) => {
                        owned.entry(task).or_default().push(record.id);
                        if node.background {
                            break;
                        }
                    }
                }
            }
        }
        owned
    }

    fn in_scope(&self, start: Up, scope: Scope, cross_background: bool) -> Option<bool> {
        for step in self.above(start, None) {
            match step {
                Step::Unknown => return None,
                Step::Conversation(id) => {
                    if scope == Scope::Conversation(id) {
                        return Some(true);
                    }
                }
                Step::Task(_, node) => {
                    if node.background && !cross_background {
                        return Some(false);
                    }
                }
            }
        }
        Some(scope == Scope::Roots)
    }

    fn below_cancelled(&self, start: Up) -> bool {
        for step in self.above(start, None) {
            match step {
                Step::Unknown => return false,
                Step::Conversation(_) => {}
                Step::Task(task, node) => {
                    if self
                        .live
                        .get(&task)
                        .is_some_and(cancellation_intent)
                    {
                        return true;
                    }
                    if node.background {
                        return false;
                    }
                }
            }
        }
        false
    }
}

fn overlay_of(tx: &Transaction<'_>) -> Overlay {
    Overlay {
        tasks: tx
            .staged_tasks()
            .into_iter()
            .map(|record| (record.id, record))
            .collect(),
        edges: tx
            .staged_conversations()
            .into_iter()
            .map(|record| (record.id, record.owner.map(|owner| owner.task_id)))
            .collect(),
    }
}

impl TaskScheduler {
    pub fn new(options: SchedulerOptions) -> Self {
        let (wake, wake_rx) = channel();
        let state = State {
            live: BTreeMap::new(),
            edges: BTreeMap::new(),
            conversation_owners: BTreeSet::new(),
            settled: BTreeMap::new(),
            fail_fast_checks: BTreeSet::new(),
            reconcile_scheduled: false,
            cascade_pending: false,
            enabled: false,
            closing: false,
            wake,
        };
        Self(Arc::new(Shared {
            session: options.session,
            storage: options.storage,
            registry: options.registry,
            settings: options.settings,
            now: options.now,
            report: options.report,
            settle_outcome: options.settle_outcome,
            withdraw_inputs: options.withdraw_inputs,
            state: Mutex::new(state),
            wake_rx: Mutex::new(Some(wake_rx)),
        }))
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.0.state.lock().expect("scheduler state lock")
    }

    pub fn open(&self) -> Result<()> {
        let inner = self.clone();
        self.0.session.subscribe_commits(Box::new(move |publication, _| {
            inner.observe(publication);
        }))?;
        let closer = self.clone();
        self.0.session.subscribe_close(Box::new(move || closer.seal()))?;
        let reconciler = self.clone();
        let wake = self
            .0
            .wake_rx
            .lock()
            .expect("scheduler wake lock")
            .take()
            .ok_or_else(|| Error::Invalid("scheduler already opened".to_owned()))?;
        thread::spawn(move || {
            while wake.recv().is_ok() {
                reconciler.reconcile();
            }
        });
        self.0.session.commit_with(
            |tx| {
                for status in LIVE_STATUSES {
                    let query = TaskQuery {
                        conversation_id: None,
                        kind: None,
                        status: Some(status),
                        abort_requested: None,
                        background: None,
                        order: None,
                    };
                    let records = scan_all(|cursor| {
                        tx.scan_tasks(&query, SCAN_PAGE_SIZE, cursor.as_ref())
                    })?;
                    let mut st = self.lock();
                    for record in records {
                        if let TaskState::Running { checkpoint } = &record.state {
                            let pending = TaskState::Pending {
                                checkpoint: checkpoint.clone(),
                            };
                            let mut next = record.clone();
                            next.state = pending;
                            tx.set_task(next)?;
                        }
                        if let TaskState::Waiting { policy: crate::tasks::JoinPolicy::FailFast, .. } = record.state {
                            st.fail_fast_checks.insert(record.id);
                        }
                        st.live.insert(record.id, record);
                    }
                }
                self.lock().cascade_pending = true;
                self.lock().schedule_reconcile();
                Ok(())
            },
            &CancelToken::new(),
            TransactionScope::default(),
        )
    }

    pub fn resume(&self) {
        self.lock().enabled = true;
    }

    fn seal(&self) {
        self.lock().closing = true;
    }

    fn observe(&self, publication: &CommitPublication) {
        let mut st = self.lock();
        let mut updated: Vec<AnyTaskRecord> = Vec::new();
        let mut failed: Vec<TaskId> = Vec::new();
        let mut changed = false;
        for change in &publication.changes {
            let CommitChange::Table(table) = change else {
                continue;
            };
            let TableChange::Task(record) = table else {
                continue;
            };
            let record: &AnyTaskRecord = record;
            changed = true;
            let previous = st.live.get(&record.id).cloned();
            if failed_outcome(record) && previous.as_ref().is_none_or(|p| !failed_outcome(p)) {
                failed.push(record.id);
            }
            if is_terminal(record) {
                st.live.remove(&record.id);
                st.fail_fast_checks.remove(&record.id);
                if st.conversation_owners.contains(&record.id) {
                    st.settled.insert(record.id, node_of(record));
                }
                st.schedule_reconcile();
                continue;
            }
            if record.abort_requested && !previous.as_ref().is_some_and(|p| p.abort_requested) {
                st.cascade_pending = true;
            }
            let was_completing = previous
                .as_ref()
                .is_some_and(|p| matches!(p.state, TaskState::Completing { .. }));
            let was_waiting = previous
                .as_ref()
                .is_some_and(|p| matches!(p.state, TaskState::Waiting { .. }));
            if matches!(record.state, TaskState::Completing { .. }) && !was_completing {
                if cancellation_intent(record) {
                    st.cascade_pending = true;
                }
                st.schedule_reconcile();
            }
            if let TaskState::Waiting {
                policy: crate::tasks::JoinPolicy::FailFast,
                ..
            } = record.state
                && !was_waiting
            {
                st.fail_fast_checks.insert(record.id);
                st.schedule_reconcile();
            }
            st.live.insert(record.id, record.clone());
            updated.push(record.clone());
        }
        for id in failed {
            let waiters: Vec<TaskId> = st
                .live
                .values()
                .filter(|record| {
                    matches!(&record.state, TaskState::Waiting { on, policy: crate::tasks::JoinPolicy::FailFast, .. } if on.contains(&id))
                })
                .map(|record| record.id)
                .collect();
            for waiter in waiters {
                st.fail_fast_checks.insert(waiter);
                st.schedule_reconcile();
            }
        }
        for change in &publication.changes {
            if let CommitChange::Table(table) = change
                && let TableChange::Conversation(conversation) = table
                && !st.edges.contains_key(&conversation.id)
            {
                st.set_edge(
                    conversation.id,
                    conversation.owner.as_ref().map(|owner| owner.task_id),
                );
            }
        }
        for change in &publication.changes {
            if let CommitChange::Table(table) = change
                && let TableChange::Submission(submission) = table
                && is_queued_input(submission)
            {
                let up = Up::Conversation(submission.conversation_id);
                if !st.chain_known(up, None) || st.below_cancelled(up) {
                    st.cascade_pending = true;
                }
            }
        }
        for record in &updated {
            let up = parent_of(&node_of(record));
            if !st.chain_known(up, None) {
                st.schedule_reconcile();
            } else if !record.background && !record.abort_requested && st.below_cancelled(up) {
                st.cascade_pending = true;
            }
        }
        if st.cascade_pending {
            st.schedule_reconcile();
        }
        if !changed {
            return;
        }
    }

    fn reconcile(&self) {
        let (cascade, checks) = {
            let mut st = self.lock();
            st.reconcile_scheduled = false;
            let cascade = std::mem::take(&mut st.cascade_pending);
            let checks: Vec<TaskId> = std::mem::take(&mut st.fail_fast_checks)
                .into_iter()
                .collect();
            (cascade, checks)
        };
        let result = self.0.session.commit_with(
            |tx| self.reconcile_in(tx, cascade, &checks),
            &CancelToken::new(),
            TransactionScope::default(),
        );
        if let Err(error) = result {
            let closing = {
                let mut st = self.lock();
                st.cascade_pending = true;
                st.fail_fast_checks.extend(checks);
                st.closing
            };
            if !closing {
                (self.0.report)(error);
            }
        }
    }

    fn reconcile_in(
        &self,
        tx: &mut Transaction<'_>,
        cascade: bool,
        checks: &[TaskId],
    ) -> Result<()> {
        let mut st = self.lock();
        if st.closing {
            return Ok(());
        }
        let queued = self.load_scopes(&mut st, cascade)?;
        let mut marked = BTreeSet::new();
        let live: Vec<AnyTaskRecord> = st.live.values().cloned().collect();
        for record in &live {
            if !record.background && st.below_cancelled(parent_of(&node_of(record))) {
                mark(tx, &mut marked, record)?;
            }
        }
        for id in checks {
            let Some(waiter) = st.live.get(id).cloned() else {
                continue;
            };
            let TaskState::Waiting { on, .. } = &waiter.state else {
                continue;
            };
            if !self.any_failed(&st, on)? {
                continue;
            }
            for member in on {
                if let Some(record) = st.live.get(member).cloned()
                    && !failed_outcome(&record)
                {
                    mark(tx, &mut marked, &record)?;
                }
            }
        }
        for id in queued {
            if st.below_cancelled(Up::Conversation(id)) {
                (self.0.withdraw_inputs)(tx, id)?;
            }
        }
        self.finalize(tx, &mut st)
    }

    fn any_failed(&self, st: &State, ids: &[TaskId]) -> Result<bool> {
        for id in ids {
            let record = match st.live.get(id) {
                Some(record) => Some(record.clone()),
                None => self.0.storage.task(*id, &CancelToken::new())?,
            };
            if record.is_some_and(|record| failed_outcome(&record)) {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn finalize(&self, tx: &mut Transaction<'_>, st: &mut State) -> Result<()> {
        loop {
            let overlay = overlay_of(tx);
            let owned = st.owned_live(Some(&overlay));
            let done: Vec<AnyTaskRecord> = st
                .live_records(Some(&overlay))
                .into_iter()
                .filter(|record| {
                    matches!(record.state, TaskState::Completing { .. })
                        && !owned.contains_key(&record.id)
                })
                .collect();
            if done.is_empty() {
                return Ok(());
            }
            for record in done {
                let outcome = match &record.state {
                    TaskState::Completing { outcome } => outcome.clone(),
                    _ => continue,
                };
                tx.set_task(with_state(
                    &record,
                    TaskState::Terminal {
                        outcome: outcome.clone(),
                    },
                ))?;
                if matches!(
                    outcome,
                    TaskOutcome::Faulted { .. } | TaskOutcome::Orphaned { .. }
                ) {
                    (self.0.settle_outcome)(tx, &record, &outcome)?;
                }
            }
        }
    }

    fn load_scopes(&self, st: &mut State, queued: bool) -> Result<Vec<ConversationId>> {
        let live: Vec<AnyTaskRecord> = st.live.values().cloned().collect();
        for record in &live {
            let up = parent_of(&node_of(record));
            if !st.chain_known(up, None) {
                self.load_chain(st, up, None)?;
            }
        }
        if !queued {
            return Ok(Vec::new());
        }
        let query = SubmissionQuery {
            conversation_id: None,
            status: Some(SubmissionStatus::Queued),
            order: None,
        };
        let submissions = scan_all(|cursor| {
            self.0.storage.scan_submissions(
                &query,
                SCAN_PAGE_SIZE,
                cursor.as_ref(),
                &CancelToken::new(),
            )
        })?;
        let mut conversations: Vec<ConversationId> = Vec::new();
        for submission in submissions {
            if !conversations.contains(&submission.conversation_id) {
                conversations.push(submission.conversation_id);
            }
        }
        for id in &conversations {
            self.load_chain(st, Up::Conversation(*id), None)?;
        }
        Ok(conversations)
    }

    fn load_chain(&self, st: &mut State, start: Up, overlay: Option<&Overlay>) -> Result<()> {
        let mut at = Some(start);
        while let Some(current) = at {
            match current {
                Up::Task(id) => {
                    let node = match st.node(id, overlay) {
                        Some(node) => node,
                        None => {
                            let Some(record) = self.0.storage.task(id, &CancelToken::new())?
                            else {
                                return Ok(());
                            };
                            let node = node_of(&record);
                            if is_terminal(&record) {
                                st.settled.insert(id, node.clone());
                            }
                            node
                        }
                    };
                    at = Some(parent_of(&node));
                }
                Up::Conversation(id) => {
                    let edge = match st.edge(id, overlay) {
                        Some(edge) => edge,
                        None => {
                            let owner = self
                                .0
                                .storage
                                .conversation(id, &CancelToken::new())?
                                .and_then(|record: ConversationRecord| {
                                    record.owner.map(|owner| owner.task_id)
                                });
                            st.set_edge(id, owner);
                            owner
                        }
                    };
                    at = edge.map(Up::Task);
                }
            }
        }
        Ok(())
    }
}

fn mark(
    tx: &mut Transaction<'_>,
    marked: &mut BTreeSet<TaskId>,
    record: &AnyTaskRecord,
) -> Result<()> {
    if record.abort_requested || !marked.insert(record.id) {
        return Ok(());
    }
    let mut next = record.clone();
    next.abort_requested = true;
    tx.set_task(next)
}

fn with_state(record: &AnyTaskRecord, state: TaskState<JsonValue, JsonValue>) -> AnyTaskRecord {
    let mut next = record.clone();
    if matches!(state, TaskState::Terminal { .. } | TaskState::Completing { .. }) {
        next.memos = None;
    }
    next.state = state;
    next
}
