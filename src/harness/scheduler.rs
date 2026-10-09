use std::collections::{BTreeMap, BTreeSet};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;

use crate::ai::models::Models;
use crate::cancel::CancelToken;
use crate::env::ExecutionEnv;
use crate::harness::agent::Agent;
use crate::harness::registry::RegistrySnapshot;
use crate::harness::types::Settings;
use crate::session::types::{DocRef, DocumentObserver, DocumentReader, DocumentWatch};

use crate::harness::util::closed_error;
use crate::tasks::{HookRunner, RunningTask, RuntimeChange, TaskRuntime};
use crate::types::{EntryRecord, JsonObject};

use crate::errors::{Error, Result};
use crate::harness::context::ContextRange;
use crate::harness::registry::Registry;
use crate::harness::runtime::HarnessTaskRuntime;
use crate::harness::util::{Ticket, Waiters, scan_all};
use crate::ids::{ConversationId, TaskId};
use crate::session::kernel::Session;
use crate::session::transaction::{Transaction, TransactionScope};
use crate::turn::{Turn, pause};

use crate::tasks::{
    JoinPolicy, NextTaskState, TaskOutcome, TaskOutcomeError, TaskState, TaskStatus,
};
use crate::types::{
    AnyTaskRecord, CommitChange, CommitPublication, ConversationRecord, JsonValue, Storage,
    SubmissionBody, SubmissionQuery, SubmissionStatus, TableChange, TaskQuery,
};

const SCAN_PAGE_SIZE: usize = 256;
const MAX_TIMER_DELAY: u64 = 2_147_483_647;

mod invocation;
pub use invocation::InvocationRuntime;
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

pub type Runtime<'a> =
    &'a (dyn HarnessTaskRuntime<JsonValue, JsonValue, JsonValue, JsonValue> + Sync);

pub trait ErasedTask: Send + Sync {
    fn name(&self) -> &str;
    fn version(&self) -> u64;
    fn can_migrate(&self) -> bool;
    fn migrate(
        &self,
        input: &JsonValue,
        checkpoint: &JsonValue,
        from: u64,
    ) -> Result<(JsonValue, JsonValue)>;
    fn run_phase(
        &self,
        phase: &str,
        record: &AnyTaskRecord,
        runtime: Runtime<'_>,
        cancel: &CancelToken,
    ) -> Result<()>;
    fn run_abort(
        &self,
        record: &AnyTaskRecord,
        runtime: Runtime<'_>,
        cancel: &CancelToken,
    ) -> Result<()>;
}

#[derive(Clone)]
pub struct Snapshot {
    pub registry: Arc<RegistrySnapshot>,
    pub task: Lookup,
}

pub type Lookup = Arc<dyn Fn(&str) -> Option<Arc<dyn ErasedTask>> + Send + Sync>;

impl Snapshot {
    fn lookup(&self, kind: &str) -> Option<Arc<dyn ErasedTask>> {
        (self.task)(kind)
    }
}

pub type TakeSnapshot = Arc<dyn Fn() -> Snapshot + Send + Sync>;
pub type AgentResolver =
    Arc<dyn Fn(ConversationId, &Snapshot, &CancelToken) -> Result<Agent> + Send + Sync>;
pub type EnvResolver = Arc<
    dyn Fn(ConversationId, &CancelToken) -> Result<Option<Arc<dyn ExecutionEnv>>> + Send + Sync,
>;
pub type HooksOf = Arc<dyn Fn(&Agent, &str) -> Vec<JsonValue> + Send + Sync>;
pub type SettingsSource = Arc<dyn Fn() -> Settings + Send + Sync>;
pub type Clock = Arc<dyn Fn() -> u64 + Send + Sync>;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum BlockedReason {
    MissingTask,
    TaskTooOld,
    MigrationFailed,
}

enum Resolution {
    Ready {
        task: Arc<dyn ErasedTask>,
        record: Box<AnyTaskRecord>,
        migrated: bool,
    },
    Blocked(BlockedReason),
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Run,
    Abort,
}

#[derive(Clone)]
pub struct Latch(Arc<(Mutex<bool>, std::sync::Condvar)>);

impl Latch {
    fn new() -> Self {
        Self(Arc::new((Mutex::new(false), std::sync::Condvar::new())))
    }

    fn open(&self) {
        *self.0.0.lock().expect("latch lock") = true;
        self.0.1.notify_all();
    }

    pub fn wait(&self) {
        pause(|| {
            let mut done = self.0.0.lock().expect("latch lock");
            while !*done {
                done = self.0.1.wait(done).expect("latch wait");
            }
        });
    }

    fn wait_or_cancel(&self, cancel: &CancelToken) -> Result<()> {
        let pair = Arc::clone(&self.0);
        cancel.on_cancel(move || {
            let _guard = pair.0.lock().expect("latch lock");
            pair.1.notify_all();
        });
        pause(|| {
            let mut done = self.0.0.lock().expect("latch lock");
            while !*done {
                if cancel.is_cancelled() {
                    return Err(Error::Aborted);
                }
                done = self.0.1.wait(done).expect("latch wait");
            }
            Ok(())
        })
    }
}

pub struct Invocation {
    pub task_id: TaskId,
    pub conversation_id: ConversationId,
    pub mode: Mode,
    pub cancel: CancelToken,
    pub done: Latch,
    pub ended: std::sync::atomic::AtomicBool,
}

struct Reservation {
    invocation: Arc<Invocation>,
    task: Arc<dyn ErasedTask>,
    snapshot: Snapshot,
}

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

#[derive(Default)]
struct Overlay {
    tasks: BTreeMap<TaskId, AnyTaskRecord>,
    edges: BTreeMap<ConversationId, Option<TaskId>>,
}

struct KeptContext {
    range: ContextRange,
    idle_since: Option<u64>,
}

struct State {
    live: BTreeMap<TaskId, AnyTaskRecord>,
    edges: BTreeMap<ConversationId, Option<TaskId>>,
    conversation_owners: BTreeSet<TaskId>,
    settled: BTreeMap<TaskId, TaskNode>,
    fail_fast_checks: BTreeSet<TaskId>,
    failed_migrations: BTreeMap<TaskId, (Arc<dyn ErasedTask>, String)>,
    invocations: BTreeMap<TaskId, Arc<Invocation>>,
    contexts: BTreeMap<ConversationId, KeptContext>,
    expiry: Option<(u64, CancelToken)>,
    reconcile_scheduled: bool,
    cascade_pending: bool,
    enabled: bool,
    closing: bool,
    dirty: bool,
    draining: bool,
    wake: Sender<()>,
}

pub struct SchedulerOptions {
    pub session: Arc<Session>,
    pub storage: Arc<dyn Storage>,
    pub report: Report,
    pub settle_outcome: SettleOutcome,
    pub withdraw_inputs: WithdrawInputs,
    pub take_snapshot: TakeSnapshot,
    pub agent: AgentResolver,
    pub models: Arc<dyn Models + Send + Sync>,
    pub env: EnvResolver,
    pub hooks_of: HooksOf,
    pub settings: SettingsSource,
    pub now: Clock,
    pub registry: Arc<Registry>,
}

struct Shared {
    session: Arc<Session>,
    storage: Arc<dyn Storage>,
    report: Report,
    settle_outcome: SettleOutcome,
    withdraw_inputs: WithdrawInputs,
    take_snapshot: TakeSnapshot,
    agent: AgentResolver,
    models: Arc<dyn Models + Send + Sync>,
    env: EnvResolver,
    hooks_of: HooksOf,
    settings: SettingsSource,
    now: Clock,
    task_waiters: Waiters<TaskId, AnyTaskRecord>,
    idle_waiters: Waiters<Option<ConversationId>, ()>,
    registry: Arc<Registry>,
    registry_listener: Mutex<Option<u64>>,
    turn: Turn,
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

    fn below_cancelled(&self, start: Up) -> bool {
        for step in self.above(start, None) {
            match step {
                Step::Unknown => return false,
                Step::Conversation(_) => {}
                Step::Task(task, node) => {
                    if self.live.get(&task).is_some_and(cancellation_intent) {
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
            failed_migrations: BTreeMap::new(),
            invocations: BTreeMap::new(),
            contexts: BTreeMap::new(),
            expiry: None,
            reconcile_scheduled: false,
            cascade_pending: false,
            enabled: false,
            closing: false,
            dirty: false,
            draining: false,
            wake,
        };
        Self(Arc::new(Shared {
            session: options.session,
            storage: options.storage,
            report: options.report,
            settle_outcome: options.settle_outcome,
            withdraw_inputs: options.withdraw_inputs,
            take_snapshot: options.take_snapshot,
            agent: options.agent,
            models: options.models,
            env: options.env,
            hooks_of: options.hooks_of,
            settings: options.settings,
            now: options.now,
            task_waiters: Waiters::default(),
            idle_waiters: Waiters::default(),
            registry: options.registry,
            registry_listener: Mutex::new(None),
            turn: Turn::default(),
            state: Mutex::new(state),
            wake_rx: Mutex::new(Some(wake_rx)),
        }))
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.0.state.lock().expect("scheduler state lock")
    }

    pub fn open(&self) -> Result<()> {
        let inner = self.clone();
        let _commits = self
            .0
            .session
            .subscribe_commits(Box::new(move |publication, _| {
                inner.observe(publication);
            }))?;
        let closer = self.clone();
        let _close = self
            .0
            .session
            .subscribe_close(Box::new(move || closer.seal()))?;
        let kicker = self.clone();
        let listener = self.0.registry.subscribe(Arc::new(move || kicker.kick()));
        *self
            .0
            .registry_listener
            .lock()
            .expect("registry listener lock") = Some(listener);
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
                let mut records = Vec::new();
                for status in LIVE_STATUSES {
                    let query = TaskQuery {
                        conversation_id: None,
                        kind: None,
                        status: Some(status),
                        abort_requested: None,
                        background: None,
                        order: None,
                    };
                    records.extend(scan_all(|cursor| {
                        tx.scan_tasks(&query, SCAN_PAGE_SIZE, cursor.as_ref())
                    })?);
                }
                for record in records {
                    self.lock().live.insert(record.id, record.clone());
                    if let TaskState::Running { checkpoint } = &record.state {
                        let mut next = record.clone();
                        next.state = TaskState::Pending {
                            checkpoint: checkpoint.clone(),
                        };
                        tx.set_task(next)?;
                    }
                    if let TaskState::Waiting {
                        policy: crate::tasks::JoinPolicy::FailFast,
                        ..
                    } = record.state
                    {
                        self.lock().fail_fast_checks.insert(record.id);
                    }
                }
                Ok(())
            },
            &CancelToken::new(),
            TransactionScope::default(),
        )?;
        let mut st = self.lock();
        st.cascade_pending = true;
        st.schedule_reconcile();
        Ok(())
    }

    pub fn resume(&self) {
        self.lock().enabled = true;
        self.kick();
    }

    fn seal(&self) {
        if let Some(listener) = self
            .0
            .registry_listener
            .lock()
            .expect("registry listener lock")
            .take()
        {
            self.0.registry.unsubscribe(listener);
        }
        self.0.task_waiters.reject_all(closed_error);
        self.0.idle_waiters.reject_all(closed_error);
        let mut st = self.lock();
        st.closing = true;
        st.contexts.clear();
        if let Some((_, token)) = st.expiry.take() {
            token.cancel();
        }
        for invocation in st.invocations.values() {
            invocation.cancel.cancel();
        }
    }

    fn observe(&self, publication: &CommitPublication) {
        let mut st = self.lock();
        let mut updated: Vec<AnyTaskRecord> = Vec::new();
        let mut failed: Vec<TaskId> = Vec::new();
        for change in &publication.changes {
            let CommitChange::Table(table) = change else {
                continue;
            };
            let TableChange::Task(record) = table else {
                continue;
            };
            let record: &AnyTaskRecord = record;
            let previous = st.live.get(&record.id).cloned();
            if failed_outcome(record) && previous.as_ref().is_none_or(|p| !failed_outcome(p)) {
                failed.push(record.id);
            }
            if is_terminal(record) {
                st.live.remove(&record.id);
                st.fail_fast_checks.remove(&record.id);
                st.failed_migrations.remove(&record.id);
                if st.conversation_owners.contains(&record.id) {
                    st.settled.insert(record.id, node_of(record));
                }
                self.0.task_waiters.resolve(&record.id, record);
                st.schedule_reconcile();
                continue;
            }
            if record.abort_requested && !previous.as_ref().is_some_and(|p| p.abort_requested) {
                st.cascade_pending = true;
                if let Some(invocation) = st.invocations.get(&record.id)
                    && invocation.mode == Mode::Run
                {
                    invocation.cancel.cancel();
                }
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
        self.settle_idle(&mut st);
        drop(st);
        self.kick();
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
                            let Some(record) = self.0.storage.task(id, &CancelToken::new())? else {
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
    if matches!(
        state,
        TaskState::Terminal { .. } | TaskState::Completing { .. }
    ) {
        next.memos = None;
    }
    next.state = state;
    next
}

enum Fit {
    Task {
        task: Arc<dyn ErasedTask>,
        migrates: bool,
    },
    Blocked(BlockedReason),
}

fn checkpoint_of(state: &TaskState<JsonValue, JsonValue>) -> Option<&JsonValue> {
    match state {
        TaskState::Pending { checkpoint }
        | TaskState::Running { checkpoint }
        | TaskState::Waiting { checkpoint, .. } => Some(checkpoint),
        TaskState::Completing { .. } | TaskState::Terminal { .. } => None,
    }
}

fn fit(st: &State, record: &AnyTaskRecord, task: Option<Arc<dyn ErasedTask>>) -> Fit {
    let Some(task) = task else {
        return Fit::Blocked(BlockedReason::MissingTask);
    };
    let version = task.version();
    if version == record.version {
        return Fit::Task {
            task,
            migrates: false,
        };
    }
    if version < record.version {
        return Fit::Blocked(BlockedReason::TaskTooOld);
    }
    if let Some((failed, _)) = st.failed_migrations.get(&record.id)
        && Arc::ptr_eq(failed, &task)
    {
        return Fit::Blocked(BlockedReason::MigrationFailed);
    }
    Fit::Task {
        task,
        migrates: true,
    }
}

fn waiting_on(
    st: &State,
    record: &AnyTaskRecord,
    owned: &BTreeMap<TaskId, Vec<TaskId>>,
) -> Vec<TaskId> {
    if record.abort_requested {
        return owned.get(&record.id).cloned().unwrap_or_default();
    }
    match &record.state {
        TaskState::Waiting { on, .. } => on
            .iter()
            .filter(|id| st.live.contains_key(id))
            .copied()
            .collect(),
        _ => Vec::new(),
    }
}

fn create_invocation(st: &mut State, record: &AnyTaskRecord, mode: Mode) -> Arc<Invocation> {
    let invocation = Arc::new(Invocation {
        task_id: record.id,
        conversation_id: record.conversation_id,
        mode,
        cancel: CancelToken::new(),
        done: Latch::new(),
        ended: std::sync::atomic::AtomicBool::new(false),
    });
    st.invocations.insert(record.id, Arc::clone(&invocation));
    invocation
}

impl TaskScheduler {
    fn resolve(&self, st: &mut State, record: &AnyTaskRecord, snapshot: &Snapshot) -> Resolution {
        match fit(st, record, snapshot.lookup(&record.kind)) {
            Fit::Blocked(reason) => Resolution::Blocked(reason),
            Fit::Task {
                task,
                migrates: false,
            } => Resolution::Ready {
                task,
                record: Box::new(record.clone()),
                migrated: false,
            },
            Fit::Task {
                task,
                migrates: true,
            } => {
                let outcome = if task.can_migrate() {
                    let checkpoint = checkpoint_of(&record.state)
                        .cloned()
                        .unwrap_or(JsonValue::Null);
                    task.migrate(&record.input, &checkpoint, record.version)
                        .map(|(input, checkpoint)| {
                            let mut next = record.clone();
                            next.version = task.version();
                            next.input = input;
                            next.state = with_checkpoint(&record.state, checkpoint);
                            next
                        })
                } else {
                    Err(Error::Invalid(format!(
                        "Task {} version {} has no migration from {}",
                        record.kind,
                        task.version(),
                        record.version
                    )))
                };
                match outcome {
                    Ok(next) => Resolution::Ready {
                        task,
                        record: Box::new(next),
                        migrated: true,
                    },
                    Err(error) => {
                        let message = error.to_string();
                        st.failed_migrations
                            .insert(record.id, (Arc::clone(&task), message));
                        (self.0.report)(error);
                        Resolution::Blocked(BlockedReason::MigrationFailed)
                    }
                }
            }
        }
    }
}

fn with_checkpoint(
    state: &TaskState<JsonValue, JsonValue>,
    checkpoint: JsonValue,
) -> TaskState<JsonValue, JsonValue> {
    match state {
        TaskState::Pending { .. } => TaskState::Pending { checkpoint },
        TaskState::Running { .. } => TaskState::Running { checkpoint },
        TaskState::Waiting { on, policy, .. } => TaskState::Waiting {
            checkpoint,
            on: on.clone(),
            policy: *policy,
        },
        other => other.clone(),
    }
}

impl BlockedReason {
    fn as_str(self) -> &'static str {
        match self {
            Self::MissingTask => "missing_task",
            Self::TaskTooOld => "task_too_old",
            Self::MigrationFailed => "migration_failed",
        }
    }
}

impl TaskScheduler {
    fn terminate(
        &self,
        tx: &mut Transaction<'_>,
        st: &mut State,
        record: &AnyTaskRecord,
        outcome: TaskOutcome<JsonValue>,
    ) -> Result<()> {
        self.load_scopes(st, false)?;
        let overlay = overlay_of(tx);
        if st.owned_live(Some(&overlay)).contains_key(&record.id) {
            return tx.set_task(with_state(record, TaskState::Completing { outcome }));
        }
        tx.set_task(with_state(
            record,
            TaskState::Terminal {
                outcome: outcome.clone(),
            },
        ))?;
        (self.0.settle_outcome)(tx, record, &outcome)
    }

    fn reserve(&self) -> Result<Vec<Reservation>> {
        let mut reservations: Vec<Reservation> = Vec::new();
        let result = self.0.session.commit_with(
            |tx| {
                let mut st = self.lock();
                if !st.enabled || st.closing {
                    return Ok(());
                }
                self.load_scopes(&mut st, false)?;
                let owned = st.owned_live(None);
                let mut snapshot: Option<Snapshot> = None;
                let live: Vec<AnyTaskRecord> = st.live.values().cloned().collect();
                for record in live {
                    if st.invocations.contains_key(&record.id)
                        || !waiting_on(&st, &record, &owned).is_empty()
                        || matches!(record.state, TaskState::Completing { .. })
                    {
                        continue;
                    }
                    let mode = if record.abort_requested {
                        Mode::Abort
                    } else {
                        Mode::Run
                    };
                    let snap = snapshot
                        .get_or_insert_with(|| (self.0.take_snapshot)())
                        .clone();
                    match self.resolve(&mut st, &record, &snap) {
                        Resolution::Blocked(reason) => {
                            if mode == Mode::Abort {
                                let outcome = TaskOutcome::Orphaned {
                                    reason: reason.as_str().to_owned(),
                                };
                                self.terminate(tx, &mut st, &record, outcome)?;
                            }
                        }
                        Resolution::Ready {
                            task,
                            record: next,
                            migrated,
                        } => {
                            if migrated || !matches!(record.state, TaskState::Running { .. }) {
                                let checkpoint = checkpoint_of(&next.state)
                                    .cloned()
                                    .unwrap_or(JsonValue::Null);
                                tx.set_task(with_state(&next, TaskState::Running { checkpoint }))?;
                            }
                            let invocation = create_invocation(&mut st, &record, mode);
                            reservations.push(Reservation {
                                invocation,
                                task,
                                snapshot: snap,
                            });
                        }
                    }
                }
                Ok(())
            },
            &CancelToken::new(),
            TransactionScope::default(),
        );
        if let Err(error) = result {
            let mut st = self.lock();
            for reservation in &reservations {
                st.invocations.remove(&reservation.invocation.task_id);
                reservation.invocation.done.open();
            }
            return Err(error);
        }
        Ok(reservations)
    }
}

enum Decision {
    Continue,
    End,
    Fault(String),
}

struct PhaseResult {
    checkpoint: JsonValue,
    failure: Option<String>,
}

struct RunState {
    task: Arc<dyn ErasedTask>,
    snapshot: Snapshot,
    reported: Option<Option<Arc<dyn ErasedTask>>>,
}

fn phase_of(checkpoint: &JsonValue) -> String {
    checkpoint
        .get("phase")
        .and_then(JsonValue::as_str)
        .unwrap_or_default()
        .to_owned()
}

impl TaskScheduler {
    fn end(&self, invocation: &Invocation) {
        invocation.cancel.cancel();
        let mut st = self.lock();
        st.invocations.remove(&invocation.task_id);
        drop(st);
        invocation.done.open();
    }

    fn decide(
        &self,
        tx: &mut Transaction<'_>,
        current: &AnyTaskRecord,
        previous: Option<&PhaseResult>,
        state: &mut RunState,
    ) -> Decision {
        if current.abort_requested {
            return Decision::End;
        }
        let Some(previous) = previous else {
            return Decision::Continue;
        };
        if let Some(failure) = &previous.failure {
            return Decision::Fault(failure.clone());
        }
        if checkpoint_of(&current.state) == Some(&previous.checkpoint) {
            return Decision::Fault(format!(
                "Task {} phase {} returned without durable progress",
                current.kind,
                phase_of(&previous.checkpoint)
            ));
        }
        state.snapshot = (self.0.take_snapshot)();
        let next = state.snapshot.lookup(&current.kind);
        let differs = match &next {
            Some(task) => !Arc::ptr_eq(task, &state.task),
            None => true,
        };
        if differs {
            if let Some(task) = &next
                && can_reserve(task.as_ref(), current)
            {
                let checkpoint = checkpoint_of(&current.state)
                    .cloned()
                    .unwrap_or(JsonValue::Null);
                if let Err(error) =
                    tx.set_task(with_state(current, TaskState::Pending { checkpoint }))
                {
                    return Decision::Fault(error.to_string());
                }
                return Decision::End;
            }
            if state
                .reported
                .as_ref()
                .is_none_or(|reported| !same_option(reported, &next))
            {
                state.reported = Some(next.clone());
                (self.0.report)(Error::Invalid(format!(
                    "Task {} keeps running under its old {} definition",
                    current.id, current.kind
                )));
            }
        }
        Decision::Continue
    }
}

fn same_option(left: &Option<Arc<dyn ErasedTask>>, right: &Option<Arc<dyn ErasedTask>>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => Arc::ptr_eq(left, right),
        (None, None) => true,
        _ => false,
    }
}

fn can_reserve(task: &dyn ErasedTask, record: &AnyTaskRecord) -> bool {
    task.version() == record.version || (task.version() > record.version && task.can_migrate())
}

impl TaskScheduler {
    fn step(
        &self,
        invocation: &Invocation,
        decide: impl FnOnce(&mut Transaction<'_>, &AnyTaskRecord) -> Decision,
    ) -> Option<AnyTaskRecord> {
        let result = self.0.session.commit_with(
            |tx| {
                let found = self.lock().live.get(&invocation.task_id).cloned();
                let current =
                    found.filter(|record| matches!(record.state, TaskState::Running { .. }));
                let closing = self.lock().closing;
                let decision = match &current {
                    Some(record) if !closing => decide(tx, record),
                    _ => Decision::End,
                };
                match decision {
                    Decision::Continue => Ok(current),
                    Decision::End => Ok(None),
                    Decision::Fault(message) => {
                        if let Some(record) = current {
                            let outcome = TaskOutcome::Faulted {
                                error: TaskOutcomeError {
                                    message,
                                    detail: None,
                                },
                            };
                            self.terminate(tx, &mut self.lock(), &record, outcome)?;
                        }
                        Ok(None)
                    }
                }
            },
            &CancelToken::new(),
            TransactionScope::default(),
        );
        match result {
            Ok(current) => current,
            Err(error) => {
                if !self.lock().closing {
                    (self.0.report)(error);
                }
                None
            }
        }
    }
}

impl TaskScheduler {
    fn run(&self, reservation: Reservation) {
        let invocation = Arc::clone(&reservation.invocation);
        let mut state = RunState {
            task: reservation.task,
            snapshot: reservation.snapshot,
            reported: None,
        };
        let mut previous: Option<PhaseResult> = None;
        loop {
            let Some(current) = self.step(&invocation, |tx, current| {
                self.decide(tx, current, previous.as_ref(), &mut state)
            }) else {
                return;
            };
            if self.lock().closing {
                return;
            }
            let checkpoint = checkpoint_of(&current.state)
                .cloned()
                .unwrap_or(JsonValue::Null);
            let phase = phase_of(&checkpoint);
            let runtime = InvocationRuntime::new(
                self.clone(),
                Arc::clone(&invocation),
                state.snapshot.clone(),
                Arc::clone(&state.task),
                (self.0.settings)(),
            );
            let failure = state
                .task
                .run_phase(&phase, &current, &runtime, &invocation.cancel)
                .err()
                .map(|error| error.to_string());
            previous = Some(PhaseResult {
                checkpoint,
                failure,
            });
        }
    }

    fn run_abort(&self, reservation: Reservation) {
        let invocation = Arc::clone(&reservation.invocation);
        let current = self.lock().live.get(&invocation.task_id).cloned();
        let Some(current) = current else {
            return;
        };
        if self.lock().closing {
            return;
        }
        let runtime = InvocationRuntime::new(
            self.clone(),
            Arc::clone(&invocation),
            reservation.snapshot.clone(),
            Arc::clone(&reservation.task),
            (self.0.settings)(),
        );
        let failure = reservation
            .task
            .run_abort(&current, &runtime, &invocation.cancel)
            .err()
            .map(|error| error.to_string());
        let message = failure.unwrap_or_else(|| {
            format!(
                "Abort handler of task {} returned without a terminal outcome",
                invocation.task_id
            )
        });
        self.step(&invocation, |_, _| Decision::Fault(message));
    }

    fn start(&self, reservation: Reservation) {
        let scheduler = self.clone();
        let ticket = self.0.turn.reserve();
        thread::spawn(move || {
            ticket.run(|| {
                let invocation = Arc::clone(&reservation.invocation);
                match invocation.mode {
                    Mode::Run => scheduler.run(reservation),
                    Mode::Abort => scheduler.run_abort(reservation),
                }
                scheduler.end(&invocation);
                scheduler.kick();
            });
        });
    }

    fn kick(&self) {
        {
            let mut st = self.lock();
            st.dirty = true;
            if st.draining || !st.enabled || st.closing {
                return;
            }
            st.draining = true;
        }
        let scheduler = self.clone();
        let ticket = self.0.turn.reserve();
        thread::spawn(move || ticket.run(|| scheduler.drain()));
    }

    fn drain(&self) {
        loop {
            {
                let mut st = self.lock();
                if !(st.dirty && st.enabled && !st.closing) {
                    st.draining = false;
                    return;
                }
                st.dirty = false;
            }
            match self.reserve() {
                Ok(reservations) => {
                    for reservation in reservations {
                        self.start(reservation);
                    }
                }
                Err(error) => {
                    if !self.lock().closing {
                        (self.0.report)(error);
                    }
                }
            }
        }
    }
}

impl TaskScheduler {
    fn commit_state(
        &self,
        tx: &mut Transaction<'_>,
        st: &mut State,
        invocation: &Invocation,
        current: &AnyTaskRecord,
        next: NextTaskState<JsonValue, JsonValue>,
    ) -> Result<()> {
        if let NextTaskState::Waiting { on, policy, .. } = &next {
            self.validate_wait(tx, st, invocation, current, on, *policy)?;
        }
        if let NextTaskState::Terminal { outcome } = &next {
            let overlay = overlay_of(tx);
            self.load_scopes(st, false)?;
            for record in overlay.tasks.values() {
                self.load_chain(st, parent_of(&node_of(record)), Some(&overlay))?;
            }
            if st.owned_live(Some(&overlay)).contains_key(&current.id) {
                return tx.set_task(with_state(
                    current,
                    TaskState::Completing {
                        outcome: outcome.clone(),
                    },
                ));
            }
        }
        tx.set_task(with_state(current, next.into_state()))
    }

    fn validate_wait(
        &self,
        tx: &mut Transaction<'_>,
        st: &mut State,
        invocation: &Invocation,
        current: &AnyTaskRecord,
        on: &[TaskId],
        policy: JoinPolicy,
    ) -> Result<()> {
        if invocation.mode == Mode::Abort {
            return Err(Error::Invalid(format!(
                "Abort handler of task {} cannot wait",
                current.id
            )));
        }
        let overlay = overlay_of(tx);
        self.load_chain(st, parent_of(&node_of(current)), None)?;
        let mut owners: BTreeSet<TaskId> = BTreeSet::new();
        for step in st.above(parent_of(&node_of(current)), None) {
            if let Step::Task(task, _) = step {
                owners.insert(task);
            }
        }
        for id in on {
            if *id == current.id || owners.contains(id) {
                return Err(Error::Invalid(format!(
                    "Task {} cannot wait on itself or its owner {}",
                    current.id, id
                )));
            }
            let member = match overlay.tasks.get(id).or_else(|| st.live.get(id)) {
                Some(member) => Some(member.clone()),
                None => self.0.storage.task(*id, &CancelToken::new())?,
            };
            let Some(member) = member else {
                return Err(Error::Invalid(format!("Task {id} does not exist")));
            };
            if policy == JoinPolicy::FailFast && member.owner != Some(current.id) {
                return Err(Error::Invalid(format!(
                    "Task {} can wait failFast only on tasks it owns; {} is not one",
                    current.id, id
                )));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum Scope {
    Conversation(ConversationId),
    Roots,
}

fn in_scope(st: &State, start: Up, scope: Scope, cross_background: bool) -> Option<bool> {
    for step in st.above(start, None) {
        match step {
            Step::Unknown => return None,
            Step::Conversation(id) => {
                if let Scope::Conversation(target) = scope
                    && target == id
                {
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
    Some(matches!(scope, Scope::Roots))
}

fn idle(st: &State, conversation: Option<ConversationId>) -> bool {
    let scope = match conversation {
        Some(id) => Scope::Conversation(id),
        None => Scope::Roots,
    };
    st.live.values().all(|record| {
        record.background || in_scope(st, parent_of(&node_of(record)), scope, false) == Some(false)
    })
}

pub enum Inspected {
    Running,
    Completing,
    Waiting {
        on: Vec<TaskId>,
    },
    Blocked {
        reason: BlockedReason,
        error: Option<String>,
    },
    Ready {
        migrates: bool,
    },
}

pub struct Inspection {
    pub scheduling: &'static str,
    pub tasks: Vec<(AnyTaskRecord, Inspected)>,
}

impl TaskScheduler {
    pub fn join(&self) {
        let latches: Vec<Latch> = self
            .lock()
            .invocations
            .values()
            .map(|invocation| invocation.done.clone())
            .collect();
        for latch in latches {
            latch.wait();
        }
    }

    pub fn abort(&self, id: TaskId, cancel: &CancelToken) -> Result<&'static str> {
        let (result, run) = self.0.session.commit_with(
            |tx| {
                let current = tx
                    .task(id)?
                    .ok_or_else(|| Error::Invalid(format!("Task {id} does not exist")))?;
                if matches!(current.state, TaskState::Terminal { .. }) {
                    return Ok(("terminal", None));
                }
                let mut st = self.lock();
                let invocation = st.invocations.get(&id).cloned();
                if invocation.is_none() && !matches!(current.state, TaskState::Completing { .. }) {
                    self.load_scopes(&mut st, false)?;
                    if !st.owned_live(None).contains_key(&id)
                        && let Resolution::Blocked(reason) =
                            self.resolve(&mut st, &current, &(self.0.take_snapshot)())
                    {
                        self.terminate(
                            tx,
                            &mut st,
                            &current,
                            TaskOutcome::Orphaned {
                                reason: reason.as_str().to_owned(),
                            },
                        )?;
                        return Ok(("marked", None));
                    }
                }
                if !current.abort_requested {
                    let mut next = current.clone();
                    next.abort_requested = true;
                    tx.set_task(next)?;
                }
                let run = invocation.filter(|inv| inv.mode == Mode::Run);
                Ok(("marked", run))
            },
            cancel,
            TransactionScope::default(),
        )?;
        if let Some(run) = run {
            run.done.wait_or_cancel(cancel)?;
        }
        Ok(result)
    }

    pub fn wait_for_task(&self, id: TaskId, cancel: &CancelToken) -> Result<AnyTaskRecord> {
        let found: std::result::Result<Ticket<TaskId, AnyTaskRecord>, AnyTaskRecord> =
            self.0.session.read_on_line(|| {
                if self.lock().closing {
                    return Err(closed_error());
                }
                if self.lock().live.contains_key(&id) {
                    return Ok(Ok(self.0.task_waiters.add(id)?));
                }
                let record = self
                    .0
                    .storage
                    .task(id, cancel)?
                    .ok_or_else(|| Error::Invalid(format!("Task {id} does not exist")))?;
                Ok(Err(record))
            })?;
        match found {
            Err(record) => Ok(record),
            Ok(ticket) => self.0.task_waiters.wait(ticket, cancel),
        }
    }

    pub fn wait_for_idle(
        &self,
        conversation: Option<ConversationId>,
        cancel: &CancelToken,
    ) -> Result<()> {
        let ticket = {
            let mut st = self.lock();
            if st.closing {
                return Err(closed_error());
            }
            if idle(&st, conversation) {
                return Ok(());
            }
            st.schedule_reconcile();
            self.0.idle_waiters.add(conversation)?
        };
        self.0.idle_waiters.wait(ticket, cancel)
    }

    pub fn abort_conversation(
        &self,
        conversation: ConversationId,
        background: bool,
        cancel: &CancelToken,
    ) -> Result<()> {
        let scope = Scope::Conversation(conversation);
        let reached = self.0.session.commit_with(
            |tx| {
                let mut st = self.lock();
                let queued = self.load_scopes(&mut st, true)?;
                let live: Vec<AnyTaskRecord> = st.live.values().cloned().collect();
                let mut reached = Vec::new();
                for record in live {
                    if record.background && !background {
                        continue;
                    }
                    if in_scope(&st, parent_of(&node_of(&record)), scope, background) != Some(true)
                    {
                        continue;
                    }
                    reached.push(record.id);
                    if !record.abort_requested {
                        let mut next = record.clone();
                        next.abort_requested = true;
                        tx.set_task(next)?;
                    }
                }
                for id in queued {
                    if in_scope(&st, Up::Conversation(id), scope, background) == Some(true) {
                        (self.0.withdraw_inputs)(tx, id)?;
                    }
                }
                Ok(reached)
            },
            cancel,
            TransactionScope::default(),
        )?;
        if background {
            for id in reached {
                self.wait_for_task(id, cancel)?;
            }
        }
        self.wait_for_idle(Some(conversation), cancel)
    }

    pub fn inspect(&self, snapshot: &Snapshot) -> Result<Inspection> {
        let mut st = self.lock();
        self.load_scopes(&mut st, false)?;
        let owned = st.owned_live(None);
        let mut tasks = Vec::new();
        for record in st.live.values() {
            let state = if st.invocations.contains_key(&record.id) {
                Inspected::Running
            } else if matches!(record.state, TaskState::Completing { .. }) {
                Inspected::Completing
            } else {
                let on = waiting_on(&st, record, &owned);
                if !on.is_empty() {
                    Inspected::Waiting { on }
                } else {
                    match fit(&st, record, snapshot.lookup(&record.kind)) {
                        Fit::Blocked(reason) => {
                            let error = match reason {
                                BlockedReason::MigrationFailed => st
                                    .failed_migrations
                                    .get(&record.id)
                                    .map(|(_, error)| error.clone()),
                                _ => None,
                            };
                            Inspected::Blocked { reason, error }
                        }
                        Fit::Task { task, migrates } => {
                            if migrates && !task.can_migrate() {
                                Inspected::Blocked {
                                    reason: BlockedReason::MigrationFailed,
                                    error: Some(format!(
                                        "Task {} version {} has no migration from {}",
                                        record.kind,
                                        task.version(),
                                        record.version
                                    )),
                                }
                            } else {
                                Inspected::Ready { migrates }
                            }
                        }
                    }
                }
            };
            tasks.push((record.clone(), state));
        }
        let scheduling = if st.closing {
            "closing"
        } else if st.enabled {
            "running"
        } else {
            "paused"
        };
        Ok(Inspection { scheduling, tasks })
    }
}

impl TaskScheduler {
    fn retention(&self) -> u64 {
        (self.0.settings)().context_retention_ms
    }

    fn settle_idle(&self, st: &mut State) {
        for key in self.0.idle_waiters.keys() {
            if idle(st, key) {
                self.0.idle_waiters.resolve(&key, &());
            }
        }
        let now = (self.0.now)();
        let retention = self.retention();
        let ids: Vec<ConversationId> = st.contexts.keys().copied().collect();
        for id in ids {
            let expired = matches!(
                st.contexts[&id].idle_since,
                Some(since) if now.saturating_sub(since) >= retention
            );
            if expired {
                st.contexts.remove(&id);
            } else if !idle(st, Some(id)) {
                if let Some(kept) = st.contexts.get_mut(&id) {
                    kept.idle_since = None;
                }
            } else if retention > 0 {
                if let Some(kept) = st.contexts.get_mut(&id) {
                    kept.idle_since.get_or_insert(now);
                }
            } else {
                st.contexts.remove(&id);
            }
        }
        self.schedule_expiry(st);
    }

    fn schedule_expiry(&self, st: &mut State) {
        let retention = self.retention();
        let at = st
            .contexts
            .values()
            .filter_map(|kept| kept.idle_since.map(|since| since + retention))
            .min();
        if let Some((current, _)) = &st.expiry
            && Some(*current) == at
        {
            return;
        }
        if let Some((_, token)) = st.expiry.take() {
            token.cancel();
        }
        let Some(at) = at else {
            return;
        };
        if st.closing {
            return;
        }
        let token = CancelToken::new();
        st.expiry = Some((at, token.clone()));
        let delay = at.saturating_sub((self.0.now)()).min(MAX_TIMER_DELAY);
        let scheduler = self.clone();
        thread::spawn(move || {
            if !token.wait_timeout(std::time::Duration::from_millis(delay)) {
                scheduler.expire();
            }
        });
    }

    fn expire(&self) {
        let mut st = self.lock();
        st.expiry = None;
        self.settle_idle(&mut st);
    }
}
