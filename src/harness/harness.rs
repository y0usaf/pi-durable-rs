use std::sync::Arc;

use crate::ai::types::{Message, UserContent, UserMessage, UserRole};
use crate::cancel::CancelToken;
use crate::entries::reset_entry;
use crate::errors::Result;
use crate::harness::agent::{self, Agent, AgentChange, agent_doc, resolve_agent, resolve_settings};
use crate::harness::context::{ContextView, capture_context_bounds, read_context_from};
use crate::harness::generation::queue_modes;
use crate::harness::inbox::{SelfMarker, WriteDraft, WriteHead};
use crate::harness::submissions::{AbortResult, SubmissionDraft, SubmissionHandle, Submissions};
use crate::harness::task_graph::{GraphGuard, TaskGraphView};
use crate::harness::types::{
    ConversationCreateOptions, ConversationCreated, ConversationInit, ErrorReport, HarnessOptions,
    HarnessSettings, RootOptions,
};
use crate::harness::usage::{UsageState, add_usage_state, usage_doc};
use crate::harness::util::{conversation_doc, scan_all};
use crate::harness::view::{ConversationViews, ViewGuard};
use crate::ids::{ConversationId, EntryId, ROOT_CONVERSATION_ID, TaskId};
use crate::session::kernel::{CreatedHook, Session, open_session, system_now};
use crate::session::observation::AttachedState;
use crate::session::transaction::TransactionScope;
use crate::session::types::Tx;
use crate::types::{
    AnyTaskRecord, ConversationOwnership, ConversationQuery, ConversationRecord, Cursor,
    EntryQuery, EntryRecord, Page, Storage,
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
        let submissions = Submissions::new(
            Arc::clone(&session),
            Arc::clone(&storage),
            Arc::clone(&now),
            Arc::new(move || queue_modes(&resolve_settings(queue_settings.as_ref()))),
            Arc::new(|| {}),
        )?;
        let views = ConversationViews::new(&session)?;
        let task_graph = TaskGraphView::new(&session)?;
        let report: ErrorReport = options.on_report.unwrap_or_else(|| Arc::new(|_| {}));
        Ok(Self(Arc::new(Inner {
            session,
            storage,
            registry: options.registry,
            settings,
            report,
            now,
            submissions,
            views,
            task_graph,
        })))
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

    pub fn close(&self, cancel: &CancelToken) -> Result<()> {
        self.0.session.close(cancel)
    }
}

fn create(
    inner: &Arc<Inner>,
    target: Target,
    agent: Option<AgentChange>,
    init: Option<ConversationInit>,
    cancel: &CancelToken,
) -> Result<Conversation> {
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
            vec![Message::User(UserMessage {
                role: UserRole,
                content: UserContent::Text(text.to_owned()),
                timestamp: (self.inner.now)(),
            })]
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
            read_context_from(storage, id, bounds, None, cancel).map(|(view, _)| view)
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
}
