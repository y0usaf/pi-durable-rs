use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::ai::types::{Message, UserMessage, UserRole};
use crate::cancel::CancelToken;
use crate::entries::user_entry;
use crate::errors::{Error, Result};
use crate::harness::generation::start_run;
use crate::harness::inbox::{
    InboxItem, InboxState, Phase, QueueModes, WriteDraft, apply_boundary, inbox_doc, is_stale,
    prepare_boundary, remove_inbox_item,
};
use crate::harness::live::{LiveState, live_doc};
use crate::harness::types::UserInput;
use crate::harness::util::{Waiters, closed_error};
use crate::harness::util::{read_draft, write_draft};
use crate::ids::{ConversationId, SubmissionId};
use crate::session::kernel::Session;
use crate::session::types::Tx;
use crate::session::types::Unsubscribe;
use crate::types::{CommitChange, Storage, TableChange};
use crate::types::{
    EntryDraft, InputState, SubmissionBody, SubmissionCreate, SubmissionRecord,
    SubmissionSettlement, WriteState,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WhenBusy {
    Steer,
    #[default]
    FollowUp,
    Reject,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SubmissionDraft {
    Input {
        request_id: Option<String>,
        content: UserInput,
        when_busy: WhenBusy,
    },
    Write {
        request_id: Option<String>,
        entry: WriteDraft,
    },
}

impl SubmissionDraft {
    fn request_id(&self) -> Option<&str> {
        match self {
            Self::Input { request_id, .. } | Self::Write { request_id, .. } => {
                request_id.as_deref()
            }
        }
    }

    fn is_input(&self) -> bool {
        matches!(self, Self::Input { .. })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AbortResult {
    Aborted,
    AlreadyPlaced,
    Settled,
    NotFound,
}

pub fn admit_submission(
    tx: &mut dyn Tx,
    conversation_id: ConversationId,
    draft: SubmissionDraft,
    now: u64,
    queue_modes: QueueModes,
) -> Result<SubmissionId> {
    if let Some(request_id) = draft.request_id()
        && let Some(existing) = tx.submission_by_request(conversation_id, request_id)?
    {
        if matches!(existing.body, SubmissionBody::Input(_)) != draft.is_input() {
            return Err(Error::Invalid(format!(
                "Request {request_id} already identifies a submission of another type"
            )));
        }
        return Ok(existing.id);
    }
    let live_token = live_doc()?;
    let mut live: LiveState = read_draft(tx, &live_token, conversation_id)?;
    let busy = live.run.is_some();
    if busy
        && matches!(
            draft,
            SubmissionDraft::Input {
                when_busy: WhenBusy::Reject,
                ..
            }
        )
    {
        return Err(Error::ConversationBusy(conversation_id));
    }
    let request_id = draft.request_id().map(str::to_owned);
    let mut boundary = if busy {
        None
    } else {
        Some(prepare_boundary(tx, conversation_id, queue_modes)?)
    };
    let queued = boundary
        .as_ref()
        .is_none_or(|boundary| !boundary.inbox.items.is_empty());
    if queued {
        let id = tx
            .create_submission(SubmissionCreate {
                conversation_id,
                request_id,
                body: SubmissionBody::Input(InputState::Queued),
            })
            .map(|record| record.id)?;
        let item = match &draft {
            SubmissionDraft::Write { entry, .. } => InboxItem::Write {
                id,
                entry: entry.clone(),
            },
            SubmissionDraft::Input {
                content, when_busy, ..
            } => match when_busy {
                WhenBusy::Steer => InboxItem::Steer {
                    id,
                    content: content.clone(),
                },
                WhenBusy::FollowUp => InboxItem::FollowUp {
                    id,
                    content: content.clone(),
                },
                WhenBusy::Reject => unreachable!("reject returns before queueing"),
            },
        };
        let Some(prepared) = boundary.as_mut() else {
            let token = inbox_doc()?;
            let mut inbox: InboxState = read_draft(tx, &token, conversation_id)?;
            inbox.items.push(item);
            write_draft(tx, &token, conversation_id, &inbox)?;
            return Ok(id);
        };
        prepared.inbox.items.push(item);
        let result = apply_boundary(tx, prepared, Phase::Final, now)?;
        if !result.users.is_empty() {
            start_run(tx, conversation_id, &mut live, result.users)?;
            write_draft(tx, &live_token, conversation_id, &live)?;
        }
        return Ok(id);
    }
    let boundary = boundary.expect("idle admission prepared a boundary");
    match draft {
        SubmissionDraft::Write { entry, .. } => {
            if is_stale(&boundary, &entry) {
                let record: SubmissionRecord = tx.create_submission(SubmissionCreate {
                    conversation_id,
                    request_id,
                    body: SubmissionBody::Write(WriteState::Unanswered {
                        reason: "stale".to_owned(),
                        detail: None,
                    }),
                })?;
                return Ok(record.id);
            }
            let appended = tx.append_entry(conversation_id, entry.entry_draft())?;
            let record = tx.create_submission(SubmissionCreate {
                conversation_id,
                request_id,
                body: SubmissionBody::Write(WriteState::Done { entry: appended.id }),
            })?;
            Ok(record.id)
        }
        SubmissionDraft::Input { content, .. } => {
            let message = Message::User(Arc::new(UserMessage {
                role: UserRole,
                content,
                timestamp: now,
            }));
            let entry = tx.append_entry(
                conversation_id,
                EntryDraft {
                    kind: user_entry().kind().to_owned(),
                    model: Some(vec![message]),
                    data: None,
                    head: None,
                    edits: None,
                },
            )?;
            let record = tx.create_submission(SubmissionCreate {
                conversation_id,
                request_id,
                body: SubmissionBody::Input(InputState::Placed { entry: entry.id }),
            })?;
            start_run(tx, conversation_id, &mut live, vec![record.id])?;
            write_draft(tx, &live_token, conversation_id, &live)?;
            Ok(record.id)
        }
    }
}

pub fn abort_submission(
    tx: &mut dyn Tx,
    id: SubmissionId,
    conversation_id: Option<ConversationId>,
) -> Result<AbortResult> {
    let Some(record) = tx.submission(id)? else {
        return Ok(AbortResult::NotFound);
    };
    if conversation_id.is_some_and(|expected| expected != record.conversation_id) {
        return Ok(AbortResult::NotFound);
    }
    match record.body {
        SubmissionBody::Input(InputState::Queued) | SubmissionBody::Write(WriteState::Queued) => {
            tx.settle_submission(
                id,
                SubmissionSettlement::Unanswered {
                    reason: "aborted".to_owned(),
                    detail: None,
                },
            )?;
            remove_inbox_item(tx, record.conversation_id, id)?;
            Ok(AbortResult::Aborted)
        }
        SubmissionBody::Input(InputState::Placed { .. }) => Ok(AbortResult::AlreadyPlaced),
        _ => Ok(AbortResult::Settled),
    }
}

pub type QueueModesFn = Arc<dyn Fn() -> QueueModes + Send + Sync>;
pub type ResumeFn = Arc<dyn Fn() + Send + Sync>;
pub type NowFn = Arc<dyn Fn() -> u64 + Send + Sync>;

pub fn is_settled(record: &SubmissionRecord) -> bool {
    matches!(
        record.body,
        SubmissionBody::Input(InputState::Done { .. } | InputState::Unanswered { .. })
            | SubmissionBody::Write(WriteState::Done { .. } | WriteState::Unanswered { .. })
    )
}

pub struct Submissions {
    session: Arc<Session>,
    storage: Arc<dyn Storage>,
    now: NowFn,
    queue_modes: QueueModesFn,
    resume: ResumeFn,
    waiters: Waiters<SubmissionId, SubmissionRecord>,
    closed: Arc<AtomicBool>,
    _subscriptions: Mutex<Vec<Unsubscribe>>,
}

impl Submissions {
    pub fn new(
        session: Arc<Session>,
        storage: Arc<dyn Storage>,
        now: NowFn,
        queue_modes: QueueModesFn,
        resume: ResumeFn,
    ) -> Result<Arc<Self>> {
        let waiters = Waiters::default();
        let closed = Arc::new(AtomicBool::new(false));
        let observed = waiters.clone();
        let commits = session.subscribe_commits(Box::new(move |publication, _cancel| {
            for change in &publication.changes {
                if let CommitChange::Table(TableChange::Submission(record)) = change
                    && is_settled(record)
                {
                    observed.resolve(&record.id, record);
                }
            }
        }))?;
        let closing = waiters.clone();
        let flag = Arc::clone(&closed);
        let close = session.subscribe_close(Box::new(move || {
            flag.store(true, Ordering::SeqCst);
            closing.reject_all(closed_error);
        }))?;
        Ok(Arc::new(Self {
            session,
            storage,
            now,
            queue_modes,
            resume,
            waiters,
            closed,
            _subscriptions: Mutex::new(vec![commits, close]),
        }))
    }

    pub fn submit(
        self: &Arc<Self>,
        conversation_id: ConversationId,
        draft: SubmissionDraft,
        cancel: &CancelToken,
    ) -> Result<SubmissionHandle> {
        (self.resume)();
        let now = (self.now)();
        let queue_modes = (self.queue_modes)();
        let id = self.session.commit(
            move |tx| admit_submission(tx, conversation_id, draft, now, queue_modes),
            cancel,
        )?;
        Ok(SubmissionHandle {
            id,
            submissions: Arc::clone(self),
        })
    }

    pub fn get(
        self: &Arc<Self>,
        id: SubmissionId,
        cancel: &CancelToken,
    ) -> Result<Option<SubmissionHandle>> {
        let record = self
            .session
            .read_on_line(|| self.storage.submission(id, cancel))?;
        Ok(record.map(|record| SubmissionHandle {
            id: record.id,
            submissions: Arc::clone(self),
        }))
    }

    pub fn status(&self, id: SubmissionId, cancel: &CancelToken) -> Result<SubmissionRecord> {
        self.session
            .read_on_line(|| self.storage.submission(id, cancel))?
            .ok_or_else(|| Error::Invalid(format!("Submission {id} does not exist")))
    }

    pub fn wait(&self, id: SubmissionId, cancel: &CancelToken) -> Result<SubmissionRecord> {
        (self.resume)();
        let found = self.session.read_on_line(|| {
            let record = self
                .storage
                .submission(id, cancel)?
                .ok_or_else(|| Error::Invalid(format!("Submission {id} does not exist")))?;
            if is_settled(&record) {
                return Ok(Err(record));
            }
            if self.closed.load(Ordering::SeqCst) {
                return Err(closed_error());
            }
            Ok(Ok(self.waiters.add(id)?))
        })?;
        match found {
            Err(record) => Ok(record),
            Ok(ticket) => self.waiters.wait(ticket, cancel),
        }
    }

    pub fn abort(
        &self,
        id: SubmissionId,
        cancel: &CancelToken,
        conversation_id: Option<ConversationId>,
    ) -> Result<AbortResult> {
        self.session
            .commit(|tx| abort_submission(tx, id, conversation_id), cancel)
    }
}

pub struct SubmissionHandle {
    pub id: SubmissionId,
    submissions: Arc<Submissions>,
}

impl SubmissionHandle {
    pub fn status(&self, cancel: &CancelToken) -> Result<SubmissionRecord> {
        self.submissions.status(self.id, cancel)
    }

    pub fn wait(&self, cancel: &CancelToken) -> Result<SubmissionRecord> {
        self.submissions.wait(self.id, cancel)
    }

    pub fn abort(&self, cancel: &CancelToken) -> Result<AbortResult> {
        match self.submissions.abort(self.id, cancel, None)? {
            AbortResult::NotFound => Err(Error::Invalid(format!(
                "Submission {} does not exist",
                self.id
            ))),
            result => Ok(result),
        }
    }
}
