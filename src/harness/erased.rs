use std::marker::PhantomData;
use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::ai::models::Models;
use crate::cancel::CancelToken;
use crate::env::ExecutionEnv;
use crate::errors::{Error, Result};
use crate::harness::agent::Agent;
use crate::harness::context::ContextView;
use crate::harness::registry::RegistrySnapshot;
use crate::harness::runtime::{HarnessTaskDefinition, HarnessTaskRuntime};
use crate::harness::scheduler::{ErasedTask, Runtime};
use crate::harness::types::Settings;
use crate::ids::{ConversationId, EntryId, TaskId};
use crate::session::types::{DocRef, DocumentObserver, DocumentReader, DocumentWatch};
use crate::tasks::{
    HookRunner, NextTaskState, RunningTask, RuntimeChange, TaskOutcome, TaskRuntime,
};
use crate::types::{AnyTaskRecord, JsonObject, JsonValue};

fn convert<A: Serialize + ?Sized, B: DeserializeOwned>(value: &A) -> Result<B> {
    Ok(serde_json::from_value(serde_json::to_value(value)?)?)
}

fn running<I, S, R>(
    record: RunningTask<JsonValue, JsonValue, JsonValue>,
) -> Result<RunningTask<I, S, R>>
where
    I: Serialize + DeserializeOwned,
    S: Serialize + DeserializeOwned,
    R: Serialize + DeserializeOwned,
{
    RunningTask::new(convert(&record.into_record())?)
}

fn next_json<S: Serialize, R: Serialize>(
    next: NextTaskState<S, R>,
) -> Result<NextTaskState<JsonValue, JsonValue>> {
    Ok(match next {
        NextTaskState::Running { checkpoint } => NextTaskState::Running {
            checkpoint: convert(&checkpoint)?,
        },
        NextTaskState::Waiting {
            checkpoint,
            on,
            policy,
        } => NextTaskState::Waiting {
            checkpoint: convert(&checkpoint)?,
            on,
            policy,
        },
        NextTaskState::Terminal { outcome } => NextTaskState::Terminal {
            outcome: convert(&outcome)?,
        },
    })
}

type Shape<I, S, R, H> = fn() -> (I, S, R, H);

pub struct Typed<'a, I, S, R, H> {
    inner: Runtime<'a>,
    task: &'a str,
    types: PhantomData<Shape<I, S, R, H>>,
}

impl<'a, I, S, R, H> Typed<'a, I, S, R, H> {
    fn new(inner: Runtime<'a>, task: &'a str) -> Self {
        Self {
            inner,
            task,
            types: PhantomData,
        }
    }
}

impl<I, S, R, H> HookRunner<H> for Typed<'_, I, S, R, H>
where
    H: Send + Sync + 'static,
{
    fn each(&self, invoke: &mut dyn FnMut(&H) -> Result<()>) -> Result<()> {
        let signal = self.inner.signal();
        let agent: Agent = self.inner.agent(signal)?;
        for extension in &agent.extensions {
            for registration in extension.hooks.iter().filter(|hook| hook.task == self.task) {
                let Some(handlers) = registration.handlers.downcast_ref::<H>() else {
                    self.inner.report(Error::Invalid(format!(
                        "Hook handlers registered for {} do not match the task's hook type",
                        self.task
                    )));
                    continue;
                };
                if let Err(error) = invoke(handlers) {
                    if signal.is_cancelled() {
                        return Err(error);
                    }
                    self.inner.report(error);
                }
            }
        }
        Ok(())
    }
}

impl<I, S, R, H> TaskRuntime<I, S, R, H> for Typed<'_, I, S, R, H>
where
    I: Serialize + DeserializeOwned,
    S: Serialize + DeserializeOwned,
    R: Serialize + DeserializeOwned,
    H: Send + Sync + 'static,
{
    fn task_id(&self) -> TaskId<R> {
        TaskId::new(self.inner.task_id().get())
    }

    fn conversation_id(&self) -> ConversationId {
        self.inner.conversation_id()
    }

    fn agent(&self, cancel: &CancelToken) -> Result<Agent> {
        self.inner.agent(cancel)
    }

    fn models(&self) -> &dyn Models {
        self.inner.models()
    }

    fn env(&self, cancel: &CancelToken) -> Result<Option<Arc<dyn ExecutionEnv>>> {
        self.inner.env(cancel)
    }

    fn registry(&self) -> Arc<RegistrySnapshot> {
        self.inner.registry()
    }

    fn settings(&self) -> &Settings {
        self.inner.settings()
    }

    fn signal(&self) -> &CancelToken {
        self.inner.signal()
    }

    fn hooks(&self) -> &dyn HookRunner<H> {
        self
    }

    fn commit(&self, change: RuntimeChange<'_, I, S, R>, cancel: &CancelToken) -> Result<()> {
        self.inner.commit(
            Box::new(move |tx, record| {
                let typed = running::<I, S, R>(record)?;
                match change(tx, typed)? {
                    None => Ok(None),
                    Some(next) => Ok(Some(next_json(next)?)),
                }
            }),
            cancel,
        )
    }

    fn memo(&self, name: &str, cancel: &CancelToken) -> Result<Option<JsonValue>> {
        self.inner.memo(name, cancel)
    }

    fn memo_or_set(
        &self,
        name: &str,
        candidate: JsonValue,
        cancel: &CancelToken,
    ) -> Result<JsonValue> {
        self.inner.memo_or_set(name, candidate, cancel)
    }

    fn get_task(&self, id: TaskId, cancel: &CancelToken) -> Result<Option<AnyTaskRecord>> {
        self.inner.get_task(id, cancel)
    }

    fn outcomes(
        &self,
        ids: &[TaskId],
        cancel: &CancelToken,
    ) -> Result<Vec<TaskOutcome<JsonValue>>> {
        self.inner.outcomes(ids, cancel)
    }

    fn entry(
        &self,
        id: EntryId,
        cancel: &CancelToken,
    ) -> Result<Option<crate::types::EntryRecord>> {
        self.inner.entry(id, cancel)
    }

    fn now(&self) -> u64 {
        self.inner.now()
    }

    fn report(&self, error: Error) {
        self.inner.report(error)
    }

    fn sleep(&self, until: u64, cancel: &CancelToken) -> Result<()> {
        self.inner.sleep(until, cancel)
    }
}

impl<I, S, R, H> HarnessTaskRuntime<I, S, R, H> for Typed<'_, I, S, R, H>
where
    I: Serialize + DeserializeOwned,
    S: Serialize + DeserializeOwned,
    R: Serialize + DeserializeOwned,
    H: Send + Sync + 'static,
{
    fn context(&self, at: Option<EntryId>, cancel: &CancelToken) -> Result<ContextView> {
        self.inner.context(at, cancel)
    }

    fn wait_for_task(&self, id: TaskId, cancel: &CancelToken) -> Result<TaskOutcome<JsonValue>> {
        self.inner.wait_for_task(id, cancel)
    }
}

impl<I, S, R, H> DocumentReader for Typed<'_, I, S, R, H> {
    fn snapshot(&self, doc: DocRef, cancel: &CancelToken) -> Result<Option<JsonObject>> {
        self.inner.snapshot(doc, cancel)
    }

    fn snapshot_as_of(
        &self,
        doc: DocRef,
        at: EntryId,
        cancel: &CancelToken,
    ) -> Result<Option<JsonObject>> {
        self.inner.snapshot_as_of(doc, at, cancel)
    }
}

impl<I, S, R, H> DocumentObserver for Typed<'_, I, S, R, H> {
    fn watch_doc(&self, doc: DocRef, cancel: &CancelToken) -> Result<Option<DocumentWatch>> {
        self.inner.watch_doc(doc, cancel)
    }
}

pub struct Erased<I, S, R, H> {
    definition: HarnessTaskDefinition<I, S, R, H>,
}

pub fn erase<I, S, R, H>(definition: HarnessTaskDefinition<I, S, R, H>) -> Arc<dyn ErasedTask>
where
    I: Serialize + DeserializeOwned + Send + Sync + 'static,
    S: Serialize + DeserializeOwned + Send + Sync + 'static,
    R: Serialize + DeserializeOwned + Send + Sync + 'static,
    H: Send + Sync + 'static,
{
    Arc::new(Erased { definition })
}

impl<I, S, R, H> ErasedTask for Erased<I, S, R, H>
where
    I: Serialize + DeserializeOwned + Send + Sync + 'static,
    S: Serialize + DeserializeOwned + Send + Sync + 'static,
    R: Serialize + DeserializeOwned + Send + Sync + 'static,
    H: Send + Sync + 'static,
{
    fn name(&self) -> &str {
        &self.definition.name
    }

    fn version(&self) -> u64 {
        self.definition.version
    }

    fn can_migrate(&self) -> bool {
        false
    }

    fn migrate(
        &self,
        _input: &JsonValue,
        _checkpoint: &JsonValue,
        from: u64,
    ) -> Result<(JsonValue, JsonValue)> {
        Err(Error::Invalid(format!(
            "Task {} version {from} cannot migrate",
            self.definition.name
        )))
    }

    fn run_phase(
        &self,
        phase: &str,
        record: &AnyTaskRecord,
        runtime: Runtime<'_>,
        cancel: &CancelToken,
    ) -> Result<()> {
        let handler = self.definition.phases.get(phase).ok_or_else(|| {
            Error::Invalid(format!(
                "Task {} has no phase {phase}",
                self.definition.name
            ))
        })?;
        let task = running::<I, S, R>(RunningTask::new(record.clone())?)?;
        let typed = Typed::<I, S, R, H>::new(runtime, &self.definition.name);
        handler(task, &typed, cancel)
    }

    fn run_abort(
        &self,
        record: &AnyTaskRecord,
        runtime: Runtime<'_>,
        cancel: &CancelToken,
    ) -> Result<()> {
        let task = running::<I, S, R>(RunningTask::new(record.clone())?)?;
        let typed = Typed::<I, S, R, H>::new(runtime, &self.definition.name);
        (self.definition.abort)(task, &typed, cancel)
    }
}
