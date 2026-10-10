use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock, Weak};

use crate::ai::models::Models;
use crate::ai::types::{Message, UserContent, UserMessage, UserRole};
use crate::cancel::CancelToken;
use crate::entries::reset_entry;
use crate::env::ExecutionEnv;
use crate::errors::{Error, Result};
use crate::harness::agent::{self, Agent, AgentChange, agent_doc, resolve_agent, resolve_settings};
use crate::harness::compaction::{CompactionInput, create_compaction_with};
use crate::harness::context::{ContextView, capture_context_bounds, read_context_from};
use crate::harness::generation::queue_modes;
use crate::harness::inbox::withdraw_queued_inputs;
use crate::harness::inbox::{SelfMarker, WriteDraft, WriteHead};
use crate::harness::live::settle_scheduler_outcome;
use crate::harness::registry::Registry;
use crate::harness::scheduler::{Inspected, SchedulerOptions, Snapshot, TaskScheduler};
use crate::harness::submissions::{AbortResult, SubmissionDraft, SubmissionHandle, Submissions};
use crate::harness::task_graph::{GraphGuard, TaskGraphView};
use crate::harness::types::{ConversationBinder, ConversationHandle, InvocationBinding};
use crate::harness::types::{
    ConversationCreateOptions, ConversationCreated, ConversationInit, EnvBuilder, EnvTarget,
    ErrorReport, HarnessOptions, HarnessSettings, RootOptions,
};
use crate::harness::usage::{UsageState, add_usage_state, usage_doc};
use crate::harness::util::{conversation_doc, scan_all};
use crate::harness::view::{ConversationViews, ViewGuard};
use crate::ids::{ConversationId, EntryId, ROOT_CONVERSATION_ID, TaskId};
use crate::session::kernel::{CreatedHook, Session, open_session, system_now};
use crate::session::observation::AttachedState;
use crate::session::transaction::TransactionScope;
use crate::session::types::{DocumentWatch, Tx};
use crate::types::CompactionReason;
use crate::types::{
    AnyTaskRecord, ConversationOwnership, ConversationQuery, ConversationRecord, Cursor,
    EntryQuery, EntryRecord, Page, Storage, SubmissionQuery, SubmissionRecord, SubmissionStatus,
};

const SCAN_PAGE_SIZE: usize = 256;

enum Target {
    Root,
    Independent(ConversationOwnership),
    Fork {
        parent: ConversationId,
        at: EntryId,
        ownership: ConversationOwnership,
    },
}

struct Inner {
    session: Arc<Session>,
    storage: Arc<dyn Storage>,
    registry: Arc<crate::harness::registry::Registry>,
    settings: Option<HarnessSettings>,
    report: ErrorReport,
    now: Arc<dyn Fn() -> u64 + Send + Sync>,
    submissions: Arc<Submissions>,
    views: ConversationViews,
    task_graph: TaskGraphView,
    tasks: TaskScheduler,
    env: Option<EnvBuilder>,
    closed: AtomicBool,
}

pub struct Harness(Arc<Inner>);

pub struct Conversation {
    id: ConversationId,
    inner: Arc<Inner>,
}

fn run_created_hook(
    tx: &mut dyn Tx,
    record: &ConversationRecord,
    user: Option<&ConversationCreated>,
) -> Result<()> {
    tx.doc(
        conversation_doc(crate::harness::live::live_doc()?.definition(), record.id),
        None,
    )?;
    tx.doc(
        conversation_doc(crate::harness::inbox::inbox_doc()?.definition(), record.id),
        None,
    )?;
    tx.doc(conversation_doc(usage_doc()?.definition(), record.id), None)?;
    tx.doc(
        conversation_doc(
            crate::harness::provider::provider_doc()?.definition(),
            record.id,
        ),
        None,
    )?;
    agent::create_agent(tx, record)?;
    match user {
        Some(user) => user(tx, record),
        None => Ok(()),
    }
}

impl Harness {
    pub fn open(storage: Arc<dyn Storage>, options: HarnessOptions) -> Result<Self> {
        let now: Arc<dyn Fn() -> u64 + Send + Sync> = match options.now {
            Some(now) => now,
            None => Arc::new(system_now),
        };
        let clock = Arc::clone(&now);
        let user_created = options.conversation_created;
        let created: CreatedHook =
            Arc::new(move |tx, record| run_created_hook(tx, record, user_created.as_ref()));
        let session = Arc::new(open_session(
            Arc::clone(&storage),
            Some(Box::new(move || clock())),
            Some(created),
        ));
        let settings = options.settings;
        let queue_settings = settings.clone();
        let report: ErrorReport = options.on_report.unwrap_or_else(|| Arc::new(|_| {}));
        let env = options.env;
        let conversation_slot: Arc<OnceLock<Weak<Inner>>> = Arc::new(OnceLock::new());
        let tasks = scheduler(
            conversation_binder(Arc::clone(&conversation_slot)),
            &session,
            &storage,
            &options.registry,
            &options.models,
            &settings,
            &report,
            &now,
            env.clone(),
        );
        let resume_tasks = tasks.clone();
        let submissions = Submissions::new(
            Arc::clone(&session),
            Arc::clone(&storage),
            Arc::clone(&now),
            Arc::new(move || queue_modes(&resolve_settings(queue_settings.as_ref()))),
            Arc::new(move || resume_tasks.resume()),
        )?;
        let task_graph = TaskGraphView::new(&session)?;
        let views = ConversationViews::new(&session)?;
        if let Err(error) = tasks.open() {
            tasks.join();
            if let Err(close_error) = session.close(&CancelToken::new()) {
                report(close_error);
            }
            return Err(error);
        }
        let inner = Arc::new(Inner {
            session,
            storage,
            registry: options.registry,
            settings,
            report,
            now,
            submissions,
            views,
            task_graph,
            tasks,
            env,
            closed: AtomicBool::new(false),
        });
        let _ = conversation_slot.set(Arc::downgrade(&inner));
        Ok(Self(inner))
    }

    pub fn session(&self) -> &Session {
        &self.0.session
    }

    pub fn root(&self, options: RootOptions, cancel: &CancelToken) -> Result<Conversation> {
        create(&self.0, Target::Root, options.agent, options.init, cancel)
    }

    pub fn conversation(
        &self,
        id: ConversationId,
        cancel: &CancelToken,
    ) -> Result<Option<Conversation>> {
        ensure_open(&self.0)?;
        let storage = &*self.0.storage;
        let record = self
            .0
            .session
            .read_on_line(|| storage.conversation(id, cancel))?;
        Ok(record.map(|record| Conversation {
            id: record.id,
            inner: Arc::clone(&self.0),
        }))
    }

    pub fn create_conversation(
        &self,
        options: ConversationCreateOptions,
        cancel: &CancelToken,
    ) -> Result<Conversation> {
        create(
            &self.0,
            Target::Independent(options.ownership),
            options.agent,
            options.init,
            cancel,
        )
    }

    pub fn get_task(&self, id: TaskId, cancel: &CancelToken) -> Result<Option<AnyTaskRecord>> {
        let storage = &*self.0.storage;
        self.0.session.read_on_line(|| storage.task(id, cancel))
    }

    pub fn submission(
        &self,
        id: crate::ids::SubmissionId,
        cancel: &CancelToken,
    ) -> Result<Option<SubmissionHandle>> {
        self.0.submissions.get(id, cancel)
    }

    pub fn abort_submission(
        &self,
        id: crate::ids::SubmissionId,
        cancel: &CancelToken,
        conversation_id: Option<ConversationId>,
    ) -> Result<AbortResult> {
        self.0.submissions.abort(id, cancel, conversation_id)
    }

    pub fn usage(&self, cancel: &CancelToken) -> Result<UsageState> {
        let storage = &*self.0.storage;
        let query = ConversationQuery {
            owner_conversation_id: None,
            owner_task_id: None,
            order: None,
        };
        let conversations = self.0.session.read_on_line(|| {
            scan_all(|cursor| {
                storage.scan_conversations(&query, SCAN_PAGE_SIZE, cursor.as_ref(), cancel)
            })
        })?;
        let token = usage_doc()?;
        let mut total = UsageState::default();
        for record in conversations {
            if let Some(state) =
                self.0
                    .session
                    .snapshot_as(&token, Some(record.id.0), None, cancel)?
            {
                add_usage_state(&mut total, &state);
            }
        }
        Ok(total)
    }

    pub fn task_graph(&self, cancel: &CancelToken) -> Result<AttachedState<GraphGuard>> {
        self.0.task_graph.state(&self.0.session, cancel)
    }

    pub fn watch_task_graph(&self, cancel: &CancelToken) -> Result<DocumentWatch> {
        self.0.task_graph.watch(&self.0.session, cancel)
    }

    pub fn build_env(
        &self,
        id: ConversationId,
        cancel: &CancelToken,
    ) -> Result<Option<Arc<dyn ExecutionEnv>>> {
        build_env(&self.0.session, self.0.env.as_ref(), id, cancel)
    }

    pub fn resume(&self) -> Result<()> {
        ensure_open(&self.0)?;
        self.0.tasks.resume();
        Ok(())
    }

    pub fn wait_for_task(&self, id: TaskId, cancel: &CancelToken) -> Result<AnyTaskRecord> {
        self.0.tasks.resume();
        self.0.tasks.wait_for_task(id, cancel)
    }

    pub fn wait_for_idle(&self, cancel: &CancelToken) -> Result<()> {
        self.0.tasks.resume();
        self.0.tasks.wait_for_idle(None, cancel)?;
        self.0.session.deliveries().drain();
        Ok(())
    }

    pub fn abort_task(&self, id: TaskId, cancel: &CancelToken) -> Result<&'static str> {
        self.0.tasks.abort(id, cancel)
    }

    pub fn inspect(&self, cancel: &CancelToken) -> Result<HarnessInspection> {
        let storage = &*self.0.storage;
        self.0.session.read_on_line(|| {
            let snapshot = snapshot_of(&self.0.registry);
            let inspection = self.0.tasks.inspect(&snapshot)?;
            let mut submissions = Vec::new();
            for status in [SubmissionStatus::Queued, SubmissionStatus::Placed] {
                let query = SubmissionQuery {
                    conversation_id: None,
                    status: Some(status),
                    order: None,
                };
                submissions.extend(scan_all(|cursor| {
                    storage.scan_submissions(&query, SCAN_PAGE_SIZE, cursor.as_ref(), cancel)
                })?);
            }
            submissions.sort_by_key(|record| record.id);
            Ok(HarnessInspection {
                scheduling: inspection.scheduling,
                tasks: inspection.tasks,
                submissions,
            })
        })
    }

    pub fn close(&self, cancel: &CancelToken) -> Result<()> {
        self.0.closed.store(true, Ordering::SeqCst);
        self.0.tasks.join();
        self.0.session.close(cancel)
    }
}

pub struct HarnessInspection {
    pub scheduling: &'static str,
    pub tasks: Vec<(AnyTaskRecord, Inspected)>,
    pub submissions: Vec<SubmissionRecord>,
}

fn snapshot_of(registry: &Registry) -> Snapshot {
    let current = registry.snapshot();
    let lookup = Arc::clone(&current);
    Snapshot {
        registry: current,
        task: Arc::new(move |name| lookup.task(name).cloned()),
    }
}

#[allow(clippy::too_many_arguments)]
fn conversation_binder(slot: Arc<OnceLock<Weak<Inner>>>) -> ConversationBinder {
    Arc::new(move |id, binding, cancel| {
        let Some(inner) = slot.get().and_then(Weak::upgrade) else {
            return Ok(None);
        };
        let storage = &*inner.storage;
        let record = inner
            .session
            .read_on_line(|| storage.conversation(id, cancel))?;
        Ok(record.map(|record| {
            Box::new(BoundConversation {
                conversation: Conversation {
                    id: record.id,
                    inner,
                },
                binding,
            }) as Box<dyn ConversationHandle>
        }))
    })
}

struct BoundConversation {
    conversation: Conversation,
    binding: InvocationBinding,
}

impl BoundConversation {
    fn bound(&self, cancel: &CancelToken) -> Result<CancelToken> {
        (self.binding.check)()?;
        let token = self.binding.signal.child();
        let link = token.clone();
        cancel.on_cancel(move || link.cancel());
        Ok(token)
    }
}

impl ConversationHandle for BoundConversation {
    fn id(&self) -> ConversationId {
        self.conversation.id()
    }

    fn submit(&self, draft: SubmissionDraft, cancel: &CancelToken) -> Result<SubmissionHandle> {
        let token = self.bound(cancel)?;
        self.conversation.submit(draft, &token)
    }

    fn abort(&self, background: bool, cancel: &CancelToken) -> Result<()> {
        let token = self.bound(cancel)?;
        self.conversation.abort(background, &token)
    }

    fn wait_for_idle(&self, cancel: &CancelToken) -> Result<()> {
        let token = self.bound(cancel)?;
        self.conversation.wait_for_idle(&token)
    }
}

#[allow(clippy::too_many_arguments)]
fn scheduler(
    conversation: ConversationBinder,
    session: &Arc<Session>,
    storage: &Arc<dyn Storage>,
    registry: &Arc<Registry>,
    models: &Arc<dyn Models + Send + Sync>,
    settings: &Option<HarnessSettings>,
    report: &ErrorReport,
    now: &Arc<dyn Fn() -> u64 + Send + Sync>,
    env: Option<EnvBuilder>,
) -> TaskScheduler {
    let resolve_session = Arc::clone(session);
    let resolve_settings_of = settings.clone();
    let resolve_report = Arc::clone(report);
    let env_session = Arc::clone(session);
    let env_build = env.clone();
    let settings_of = settings.clone();
    let clock = Arc::clone(now);
    TaskScheduler::new(SchedulerOptions {
        session: Arc::clone(session),
        storage: Arc::clone(storage),
        report: Arc::clone(report),
        settle_outcome: Arc::new(|tx, record, outcome| {
            settle_scheduler_outcome(tx, record, outcome)
        }),
        withdraw_inputs: Arc::new(|tx, id| withdraw_queued_inputs(tx, id)),
        take_snapshot: {
            let registry = Arc::clone(registry);
            Arc::new(move || snapshot_of(&registry))
        },
        agent: Arc::new(move |id, snapshot, cancel| {
            let token = agent_doc()?;
            let state = resolve_session.snapshot_as(&token, Some(id.0), None, cancel)?;
            Ok(resolve_agent(
                state.as_ref(),
                &snapshot.registry,
                &resolve_settings(resolve_settings_of.as_ref()),
                &*resolve_report,
            ))
        }),
        models: Arc::clone(models),
        env: Arc::new(move |id, cancel| build_env(&env_session, env_build.as_ref(), id, cancel)),
        conversation,
        settings: Arc::new(move || resolve_settings(settings_of.as_ref())),
        now: Arc::new(move || clock()),
        registry: Arc::clone(registry),
    })
}

fn build_env(
    session: &Session,
    build: Option<&EnvBuilder>,
    id: ConversationId,
    cancel: &CancelToken,
) -> Result<Option<Arc<dyn ExecutionEnv>>> {
    let Some(build) = build else {
        return Ok(None);
    };
    let token = agent_doc()?;
    let cwd = session
        .snapshot_as(&token, Some(id.0), None, cancel)?
        .and_then(|state| state.cwd);
    build(
        &EnvTarget {
            conversation_id: id,
            cwd,
            read: session,
        },
        cancel,
    )
}

fn ensure_open(inner: &Inner) -> Result<()> {
    if inner.closed.load(Ordering::SeqCst) {
        return Err(Error::Invalid("Harness is closed".into()));
    }
    Ok(())
}

fn create(
    inner: &Arc<Inner>,
    target: Target,
    agent: Option<AgentChange>,
    init: Option<ConversationInit>,
    cancel: &CancelToken,
) -> Result<Conversation> {
    ensure_open(inner)?;
    let id = inner.session.commit(
        move |tx| {
            let record = match target {
                Target::Root => {
                    if let Some(existing) = tx.conversation(ROOT_CONVERSATION_ID)? {
                        return Ok(existing.id);
                    }
                    tx.create_root_conversation()?
                }
                Target::Independent(ownership) => tx.create_conversation(ownership)?,
                Target::Fork {
                    parent,
                    at,
                    ownership,
                } => tx.fork_conversation(parent, at, ownership)?,
            };
            if let Some(change) = agent {
                agent::configure(tx, record.id, &change)?;
            }
            if let Some(init) = init {
                init(tx, record.id)?;
            }
            Ok(record.id)
        },
        cancel,
    )?;
    Ok(Conversation {
        id,
        inner: Arc::clone(inner),
    })
}

impl Conversation {
    pub fn id(&self) -> ConversationId {
        self.id
    }

    pub fn agent(&self, cancel: &CancelToken) -> Result<Agent> {
        let registry = self.inner.registry.snapshot();
        let token = agent_doc()?;
        let state = self
            .inner
            .session
            .snapshot_as(&token, Some(self.id.0), None, cancel)?;
        let settings = resolve_settings(self.inner.settings.as_ref());
        Ok(resolve_agent(
            state.as_ref(),
            &registry,
            &settings,
            &*self.inner.report,
        ))
    }

    pub fn compact(&self, instructions: Option<&str>, cancel: &CancelToken) -> Result<TaskId> {
        self.inner.tasks.resume();
        let id = self.id;
        let input = CompactionInput {
            reason: CompactionReason::Manual,
            instructions: instructions.map(str::to_owned),
        };
        self.inner
            .session
            .commit(|tx| create_compaction_with(tx, id, input, None), cancel)
    }

    pub fn abort(&self, background: bool, cancel: &CancelToken) -> Result<()> {
        self.inner.tasks.resume();
        self.inner
            .tasks
            .abort_conversation(self.id, background, cancel)
    }

    pub fn wait_for_idle(&self, cancel: &CancelToken) -> Result<()> {
        self.inner.tasks.resume();
        self.inner.tasks.wait_for_idle(Some(self.id), cancel)?;
        self.inner.session.deliveries().drain();
        Ok(())
    }

    pub fn configure(&self, change: &AgentChange, cancel: &CancelToken) -> Result<()> {
        let id = self.id;
        self.inner
            .session
            .commit(|tx| agent::configure(tx, id, change), cancel)
    }

    pub fn submit(&self, draft: SubmissionDraft, cancel: &CancelToken) -> Result<SubmissionHandle> {
        self.inner.submissions.submit(self.id, draft, cancel)
    }

    pub fn reset(&self, handoff: Option<&str>, cancel: &CancelToken) -> Result<()> {
        let model = handoff.map(|text| {
            vec![Message::User(Arc::new(UserMessage {
                role: UserRole,
                content: UserContent::Text(text.to_owned()),
                timestamp: (self.inner.now)(),
            }))]
        });
        let entry = WriteDraft {
            kind: reset_entry().kind().to_owned(),
            model,
            data: None,
            head: Some(WriteHead::SelfHead(SelfMarker::Myself)),
            edits: None,
        };
        self.submit(
            SubmissionDraft::Write {
                request_id: None,
                entry,
            },
            cancel,
        )?;
        Ok(())
    }

    pub fn commit<R>(
        &self,
        change: impl FnOnce(&mut dyn Tx) -> Result<R>,
        cancel: &CancelToken,
    ) -> Result<R> {
        let scope = TransactionScope {
            conversation_id: Some(self.id),
            task_id: None,
        };
        self.inner
            .session
            .commit_with(|tx| change(tx), cancel, scope)
    }

    pub fn context(&self, at: Option<EntryId>, cancel: &CancelToken) -> Result<ContextView> {
        let id = self.id;
        let storage = &*self.inner.storage;
        self.inner.session.read_on_line(|| {
            let bounds = capture_context_bounds(storage, id, at, cancel)?;
            read_context_from(storage, id, bounds, None, cancel)
                .map(|(view, _)| Arc::unwrap_or_clone(view))
        })
    }

    pub fn entries(
        &self,
        query: EntryQuery,
        limit: usize,
        cursor: Option<Cursor>,
        cancel: &CancelToken,
    ) -> Result<Page<EntryRecord>> {
        let query = EntryQuery {
            conversation_id: self.id,
            ..query
        };
        let storage = &*self.inner.storage;
        self.inner
            .session
            .read_on_line(|| storage.scan_entries(&query, limit, cursor.as_ref(), cancel))
    }

    pub fn fork(
        &self,
        at: EntryId,
        options: ConversationCreateOptions,
        cancel: &CancelToken,
    ) -> Result<Conversation> {
        create(
            &self.inner,
            Target::Fork {
                parent: self.id,
                at,
                ownership: options.ownership,
            },
            options.agent,
            options.init,
            cancel,
        )
    }

    pub fn view_state(&self, cancel: &CancelToken) -> Result<AttachedState<ViewGuard>> {
        self.inner.views.state(&self.inner.session, self.id, cancel)
    }

    pub fn watch(&self, cancel: &CancelToken) -> Result<DocumentWatch> {
        self.inner.views.watch(&self.inner.session, self.id, cancel)
    }
}
