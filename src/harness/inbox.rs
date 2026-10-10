use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::ai::types::{Message, UserMessage, UserRole};
use crate::documents::DocSemantics;
use crate::documents::DocToken;
use crate::entries::user_entry;
use crate::errors::Result;
use crate::harness::types::{QueueMode, UserInput};
use crate::harness::util::{doc_definition, edit_doc};
use crate::ids::{ConversationId, EntryId, SubmissionId};
use crate::session::types::Tx;
use crate::types::{
    ContextEdit, ConversationSemantics, EntryDraft, EntryHead, JsonValue, LatestFork,
    SubmissionSettlement,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum WriteHead {
    SelfHead(SelfMarker),
    Entry(EntryId),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SelfMarker {
    #[serde(rename = "self")]
    Myself,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WriteDraft {
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<Vec<Message>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<JsonValue>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head: Option<WriteHead>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub edits: Option<Vec<ContextEdit>>,
}

impl WriteDraft {
    pub fn entry_draft(&self) -> EntryDraft {
        EntryDraft {
            kind: self.kind.clone(),
            model: self.model.clone(),
            data: self.data.clone(),
            head: self.head.map(|head| match head {
                WriteHead::SelfHead(_) => EntryHead::SelfHead,
                WriteHead::Entry(id) => EntryHead::Entry(id),
            }),
            edits: self.edits.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "mode")]
pub enum InboxItem {
    #[serde(rename = "steer")]
    Steer {
        id: SubmissionId,
        content: UserInput,
    },
    #[serde(rename = "followUp")]
    FollowUp {
        id: SubmissionId,
        content: UserInput,
    },
    #[serde(rename = "write")]
    Write { id: SubmissionId, entry: WriteDraft },
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct InboxState {
    pub items: Vec<InboxItem>,
}

pub fn inbox_doc() -> Result<DocToken<InboxState>> {
    doc_definition(
        "pi.inbox",
        1,
        DocSemantics::Conversation(ConversationSemantics::Latest {
            fork: LatestFork::Initial,
        }),
        InboxState::default,
        Some(|state| state.items.is_empty()),
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueModes {
    pub steering_mode: QueueMode,
    pub follow_up_mode: QueueMode,
}

#[derive(Debug, Clone)]
pub struct Boundary {
    pub conversation_id: ConversationId,
    pub inbox: InboxState,
    pub steering_mode: QueueMode,
    pub follow_up_mode: QueueMode,
    pub head: Option<EntryId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoundaryResult {
    pub users: Vec<SubmissionId>,
    pub reset: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    PostTools,
    Final,
}

pub fn prepare_boundary(
    tx: &mut dyn Tx,
    conversation_id: ConversationId,
    modes: QueueModes,
) -> Result<Boundary> {
    let head = tx
        .latest_head_marker(conversation_id)?
        .map(|marker| marker.head);
    let token = inbox_doc()?;
    let inbox = crate::harness::util::read_draft(tx, &token, conversation_id)?;
    Ok(Boundary {
        conversation_id,
        inbox,
        steering_mode: modes.steering_mode,
        follow_up_mode: modes.follow_up_mode,
        head,
    })
}

pub fn apply_boundary(
    tx: &mut dyn Tx,
    boundary: &mut Boundary,
    at: Phase,
    now: u64,
) -> Result<BoundaryResult> {
    let items = boundary.inbox.items.clone();
    let reset = items.iter().any(|item| {
        matches!(item, InboxItem::Write { entry, .. } if entry.head == Some(WriteHead::SelfHead(SelfMarker::Myself)))
    });
    let final_phase = at == Phase::Final || reset;
    let steer = pick(&items, "steer", boundary.steering_mode);
    let follow = pick(&items, "followUp", boundary.follow_up_mode);
    let writes: Vec<usize> = items
        .iter()
        .enumerate()
        .filter(|(_, item)| matches!(item, InboxItem::Write { .. }))
        .map(|(index, _)| index)
        .collect();
    let mut users = steer;
    if final_phase {
        users.extend(follow);
    }
    users.sort_unstable();
    for &index in &writes {
        let InboxItem::Write { id, entry } = &items[index] else {
            continue;
        };
        if is_stale(boundary, entry) {
            tx.settle_submission(
                *id,
                SubmissionSettlement::Unanswered {
                    reason: "stale".to_owned(),
                    detail: None,
                },
            )?;
            continue;
        }
        let appended = tx.append_entry(boundary.conversation_id, entry.entry_draft())?;
        if let Some(head) = entry.head {
            boundary.head = Some(match head {
                WriteHead::SelfHead(_) => appended.id,
                WriteHead::Entry(id) => id,
            });
        }
        tx.place_submission(*id, appended.id)?;
    }
    let mut placed = Vec::new();
    for &index in &users {
        let (InboxItem::Steer { id, content } | InboxItem::FollowUp { id, content }) =
            &items[index]
        else {
            continue;
        };
        let message = Message::User(Arc::new(UserMessage {
            role: UserRole,
            content: content.clone(),
            timestamp: now,
        }));
        let appended = tx.append_entry(
            boundary.conversation_id,
            EntryDraft {
                kind: user_entry().kind().to_owned(),
                model: Some(vec![message]),
                data: None,
                head: None,
                edits: None,
            },
        )?;
        tx.place_submission(*id, appended.id)?;
        placed.push(*id);
    }
    let mut removed: Vec<usize> = writes.into_iter().chain(users).collect();
    removed.sort_unstable();
    boundary.inbox.items = items
        .into_iter()
        .enumerate()
        .filter(|(index, _)| removed.binary_search(index).is_err())
        .map(|(_, item)| item)
        .collect();
    let token = inbox_doc()?;
    crate::harness::util::write_draft(tx, &token, boundary.conversation_id, &boundary.inbox)?;
    Ok(BoundaryResult {
        users: placed,
        reset,
    })
}

fn pick(items: &[InboxItem], mode: &str, queue_mode: QueueMode) -> Vec<usize> {
    let indexes = items.iter().enumerate().filter_map(|(index, item)| {
        let matches = match item {
            InboxItem::Steer { .. } => mode == "steer",
            InboxItem::FollowUp { .. } => mode == "followUp",
            InboxItem::Write { .. } => false,
        };
        matches.then_some(index)
    });
    match queue_mode {
        QueueMode::All => indexes.collect(),
        QueueMode::OneAtATime => indexes.take(1).collect(),
    }
}

pub fn is_stale(boundary: &Boundary, entry: &WriteDraft) -> bool {
    match (entry.head, boundary.head) {
        (Some(WriteHead::Entry(head)), Some(current)) => head < current,
        _ => false,
    }
}

pub fn remove_inbox_item(
    tx: &mut dyn Tx,
    conversation_id: ConversationId,
    id: SubmissionId,
) -> Result<()> {
    let token = inbox_doc()?;
    edit_doc(tx, &token, conversation_id, |state| {
        if let Some(index) = state.items.iter().position(|item| item_id(item) == id) {
            state.items.remove(index);
        }
    })
}

pub fn withdraw_queued_inputs(tx: &mut dyn Tx, conversation_id: ConversationId) -> Result<()> {
    let token = inbox_doc()?;
    let mut state = crate::harness::util::read_draft(tx, &token, conversation_id)?;
    for index in (0..state.items.len()).rev() {
        if matches!(state.items[index], InboxItem::Write { .. }) {
            continue;
        }
        let queued = state.items.remove(index);
        tx.settle_submission(
            item_id(&queued),
            SubmissionSettlement::Unanswered {
                reason: "aborted".to_owned(),
                detail: None,
            },
        )?;
    }
    crate::harness::util::write_draft(tx, &token, conversation_id, &state)
}

fn item_id(item: &InboxItem) -> SubmissionId {
    match item {
        InboxItem::Steer { id, .. }
        | InboxItem::FollowUp { id, .. }
        | InboxItem::Write { id, .. } => *id,
    }
}
