use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

use crate::ai::utils::uuid::uuidv7;
use crate::cancel::CancelToken;
use crate::documents::{DocSemantics, DocToken};
use crate::errors::{Error, Result};
use crate::harness::util::{conversation_doc, doc_definition};
use crate::session::types::Tx;
use crate::tasks::{NextTaskState, TaskRuntime};
use crate::types::{ConversationSemantics, JsonValue, LatestFork};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProviderState {
    pub session_id: String,
}

fn new_provider_state() -> ProviderState {
    ProviderState {
        session_id: uuidv7(None).expect("the OS random source is available"),
    }
}

pub fn provider_doc() -> Result<DocToken<ProviderState>> {
    doc_definition(
        "pi.provider",
        1,
        DocSemantics::Conversation(ConversationSemantics::Latest {
            fork: LatestFork::Initial,
        }),
        new_provider_state,
        Some(|_| true),
    )
}

pub fn ensure_provider_session_id<I, S, R, H>(
    runtime: &dyn TaskRuntime<I, S, R, H>,
    cancel: &CancelToken,
) -> Result<String> {
    let token = provider_doc()?;
    let doc = conversation_doc(token.definition(), runtime.conversation_id());
    if let Some(existing) = runtime.snapshot(doc, cancel)? {
        let state: ProviderState = serde_json::from_value(JsonValue::Object(existing))?;
        return Ok(state.session_id);
    }
    let created: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let sink = Arc::clone(&created);
    let conversation_id = runtime.conversation_id();
    runtime.commit(
        Box::new(
            move |tx: &mut dyn Tx, _task| -> Result<Option<NextTaskState<S, R>>> {
                let draft = tx.doc(conversation_doc(token.definition(), conversation_id), None)?;
                let state: ProviderState = serde_json::from_value(tx.draft(draft)?.clone())?;
                if let Ok(mut slot) = sink.lock() {
                    *slot = Some(state.session_id);
                }
                Ok(None)
            },
        ),
        cancel,
    )?;
    let session = created
        .lock()
        .ok()
        .and_then(|slot| slot.clone())
        .ok_or_else(|| {
            Error::Invalid(format!(
                "Conversation {} has no provider session ID",
                conversation_id
            ))
        })?;
    Ok(session)
}
