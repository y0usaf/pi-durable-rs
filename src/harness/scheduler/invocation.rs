use super::*;
use crate::harness::context::{ContextView, capture_context_bounds, read_context_from};
use crate::harness::runtime::HarnessTaskRuntime;
use crate::ids::EntryId;
use crate::tasks::TaskOutcome;

pub struct InvocationRuntime {
    scheduler: TaskScheduler,
    invocation: Arc<Invocation>,
    snapshot: Snapshot,
    task: Arc<dyn ErasedTask>,
    settings: Settings,
}

impl InvocationRuntime {
    pub fn new(
        scheduler: TaskScheduler,
        invocation: Arc<Invocation>,
        snapshot: Snapshot,
        task: Arc<dyn ErasedTask>,
        settings: Settings,
    ) -> Self {
        Self {
            scheduler,
            invocation,
            snapshot,
            task,
            settings,
        }
    }

    fn ensure_live(&self) -> Result<()> {
        if self
            .invocation
            .ended
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            return Err(Error::Invalid(format!(
                "Task {} invocation has ended",
                self.invocation.task_id
            )));
        }
        Ok(())
    }

    fn gated<R>(
        &self,
        cancel: &CancelToken,
        change: impl FnOnce(&mut Transaction<'_>, &AnyTaskRecord, &mut State) -> Result<R>,
    ) -> Result<R> {
        self.ensure_live()?;
        let scope = TransactionScope {
            conversation_id: Some(self.invocation.conversation_id),
            task_id: Some(self.invocation.task_id),
        };
        self.scheduler.0.session.commit_with(
            |tx| {
                self.ensure_live()?;
                let mut st = self.scheduler.lock();
                if st.closing {
                    return Err(closed_error());
                }
                let Some(found) = st.live.get(&self.invocation.task_id).cloned() else {
                    return Err(Error::Invalid(format!(
                        "Task {} is terminal",
                        self.invocation.task_id
                    )));
                };
                if !matches!(found.state, TaskState::Running { .. }) {
                    return Err(Error::Invalid(format!(
                        "Task {} is {}",
                        self.invocation.task_id,
                        status_name(&found.state)
                    )));
                }
                if self.invocation.mode == Mode::Run && found.abort_requested {
                    return Err(Error::Invalid(format!(
                        "Task {} has a durable abort mark",
                        self.invocation.task_id
                    )));
                }
                change(tx, &found, &mut st)
            },
            cancel,
            scope,
        )
    }
}

fn status_name(state: &TaskState<JsonValue, JsonValue>) -> &'static str {
    match state {
        TaskState::Pending { .. } => "pending",
        TaskState::Running { .. } => "running",
        TaskState::Waiting { .. } => "waiting",
        TaskState::Completing { .. } => "completing",
        TaskState::Terminal { .. } => "terminal",
    }
}

fn memo_of(record: &AnyTaskRecord, name: &str) -> Option<JsonValue> {
    record
        .memos
        .as_ref()
        .and_then(|memos| memos.get(name).cloned())
}

impl HarnessTaskRuntime<JsonValue, JsonValue, JsonValue, JsonValue> for InvocationRuntime {
    fn context(&self, at: Option<EntryId>, cancel: &CancelToken) -> Result<ContextView> {
        self.ensure_live()?;
        let conversation = self.invocation.conversation_id;
        let storage = self.scheduler.0.storage.as_ref();
        let previous = self
            .scheduler
            .lock()
            .contexts
            .get(&conversation)
            .map(|kept| kept.range.clone());
        let bounds = self
            .scheduler
            .0
            .session
            .read_on_line(|| capture_context_bounds(storage, conversation, at, cancel))?;
        let (view, range) =
            read_context_from(storage, conversation, bounds, previous.as_ref(), cancel)?;
        let mut st = self.scheduler.lock();
        let kept_tail = st
            .contexts
            .get(&conversation)
            .map(|kept| kept.range.bounds.tail);
        let kept_since = st
            .contexts
            .get(&conversation)
            .and_then(|kept| kept.idle_since);
        if let Some(range) = range
            && !st.closing
            && !self
                .invocation
                .ended
                .load(std::sync::atomic::Ordering::SeqCst)
            && kept_tail.is_none_or(|tail| tail <= range.bounds.tail)
        {
            if !idle(&st, Some(conversation)) {
                st.contexts.insert(
                    conversation,
                    KeptContext {
                        range,
                        idle_since: None,
                    },
                );
            } else if self.scheduler.retention() > 0 {
                let now = (self.scheduler.0.now)();
                st.contexts.insert(
                    conversation,
                    KeptContext {
                        range,
                        idle_since: Some(kept_since.unwrap_or(now)),
                    },
                );
                self.scheduler.schedule_expiry(&mut st);
            }
        }
        Ok(view)
    }

    fn wait_for_task(&self, id: TaskId, cancel: &CancelToken) -> Result<TaskOutcome<JsonValue>> {
        self.ensure_live()?;
        match self.scheduler.wait_for_task(id, cancel)?.state {
            TaskState::Terminal { outcome } => Ok(outcome),
            _ => Err(Error::Invalid(format!("Task {id} is not terminal"))),
        }
    }
}

impl TaskRuntime<JsonValue, JsonValue, JsonValue, JsonValue> for InvocationRuntime {
    fn task_id(&self) -> TaskId {
        self.invocation.task_id
    }

    fn conversation_id(&self) -> ConversationId {
        self.invocation.conversation_id
    }

    fn agent(&self, cancel: &CancelToken) -> Result<Agent> {
        self.ensure_live()?;
        (self.scheduler.0.agent)(self.invocation.conversation_id, &self.snapshot, cancel)
    }

    fn models(&self) -> &dyn Models {
        self.scheduler.0.models.as_ref()
    }

    fn env(&self, cancel: &CancelToken) -> Result<Option<Arc<dyn ExecutionEnv>>> {
        self.ensure_live()?;
        (self.scheduler.0.env)(self.invocation.conversation_id, cancel)
    }

    fn registry(&self) -> Arc<RegistrySnapshot> {
        Arc::clone(&self.snapshot.registry)
    }

    fn settings(&self) -> &Settings {
        &self.settings
    }

    fn signal(&self) -> &CancelToken {
        &self.invocation.cancel
    }

    fn hooks(&self) -> &dyn HookRunner<JsonValue> {
        self
    }

    fn commit(
        &self,
        change: RuntimeChange<'_, JsonValue, JsonValue, JsonValue>,
        cancel: &CancelToken,
    ) -> Result<()> {
        self.gated(cancel, |tx, current, st| {
            let running = RunningTask::new(current.clone())?;
            if let Some(next) = change(tx, running)? {
                self.scheduler
                    .commit_state(tx, st, &self.invocation, current, next)?;
            }
            Ok(())
        })
    }

    fn memo(&self, name: &str, _cancel: &CancelToken) -> Result<Option<JsonValue>> {
        self.ensure_live()?;
        let st = self.scheduler.lock();
        Ok(st
            .live
            .get(&self.invocation.task_id)
            .and_then(|record| memo_of(record, name)))
    }

    fn memo_or_set(
        &self,
        name: &str,
        candidate: JsonValue,
        cancel: &CancelToken,
    ) -> Result<JsonValue> {
        self.gated(cancel, |tx, current, _| {
            if let Some(winner) = memo_of(current, name) {
                return Ok(winner);
            }
            let mut next = current.clone();
            let mut memos = next.memos.take().unwrap_or_default();
            memos.insert(name.to_owned(), candidate.clone());
            next.memos = Some(memos);
            tx.set_task(next)?;
            Ok(candidate)
        })
    }

    fn get_task(&self, id: TaskId, cancel: &CancelToken) -> Result<Option<AnyTaskRecord>> {
        self.ensure_live()?;
        self.scheduler
            .0
            .session
            .read_on_line(|| self.scheduler.0.storage.task(id, cancel))
    }

    fn outcomes(
        &self,
        ids: &[TaskId],
        cancel: &CancelToken,
    ) -> Result<Vec<TaskOutcome<JsonValue>>> {
        self.ensure_live()?;
        self.scheduler.0.session.read_on_line(|| {
            ids.iter()
                .map(|id| match self.scheduler.0.storage.task(*id, cancel)? {
                    Some(AnyTaskRecord {
                        state: TaskState::Terminal { outcome },
                        ..
                    }) => Ok(outcome),
                    _ => Err(Error::Invalid(format!("Task {id} is not terminal"))),
                })
                .collect()
        })
    }

    fn entry(&self, id: EntryId, cancel: &CancelToken) -> Result<Option<EntryRecord>> {
        self.ensure_live()?;
        self.scheduler.0.session.read_on_line(|| {
            Ok(self
                .scheduler
                .0
                .storage
                .entry(id, cancel)?
                .map(|found| found.entry))
        })
    }

    fn now(&self) -> u64 {
        (self.scheduler.0.now)()
    }

    fn report(&self, error: Error) {
        (self.scheduler.0.report)(error);
    }

    fn sleep(&self, until: u64, cancel: &CancelToken) -> Result<()> {
        self.ensure_live()?;
        let joined = CancelToken::new();
        let inner = joined.clone();
        self.invocation.cancel.on_cancel(move || inner.cancel());
        let outer = joined.clone();
        cancel.on_cancel(move || outer.cancel());
        loop {
            if joined.is_cancelled() {
                return Err(Error::Aborted);
            }
            let remaining = until.saturating_sub(self.now());
            if remaining == 0 {
                return Ok(());
            }
            pause(|| {
                joined.wait_timeout(std::time::Duration::from_millis(
                    remaining.min(MAX_TIMER_DELAY),
                ))
            });
        }
    }
}

impl HookRunner<JsonValue> for InvocationRuntime {
    fn each(&self, invoke: &mut dyn FnMut(&JsonValue) -> Result<()>) -> Result<()> {
        let agent = TaskRuntime::agent(self, &self.invocation.cancel)?;
        for handler in (self.scheduler.0.hooks_of)(&agent, self.task.name()) {
            if let Err(error) = invoke(&handler) {
                if self.invocation.cancel.is_cancelled() {
                    return Err(error);
                }
                (self.scheduler.0.report)(error);
            }
        }
        Ok(())
    }
}

impl DocumentReader for InvocationRuntime {
    fn snapshot(&self, doc: DocRef, cancel: &CancelToken) -> Result<Option<JsonObject>> {
        self.ensure_live()?;
        self.scheduler.0.session.snapshot(doc, cancel)
    }

    fn snapshot_as_of(
        &self,
        doc: DocRef,
        at: EntryId,
        cancel: &CancelToken,
    ) -> Result<Option<JsonObject>> {
        self.ensure_live()?;
        self.scheduler.0.session.snapshot_as_of(doc, at, cancel)
    }
}

impl DocumentObserver for InvocationRuntime {
    fn watch_doc(&self, doc: DocRef, cancel: &CancelToken) -> Result<Option<DocumentWatch>> {
        self.ensure_live()?;
        self.scheduler.0.session.watch_doc(doc, cancel)
    }
}
