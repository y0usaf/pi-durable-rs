use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::ai::types::Usage;
use crate::documents::{DocSemantics, DocToken};
use crate::errors::Result;
use crate::harness::util::{doc_definition, edit_doc};
use crate::ids::ConversationId;
use crate::session::types::Tx;
use crate::types::{ConversationSemantics, LatestFork};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct UsageState {
    pub models: BTreeMap<String, Usage>,
    pub tools: BTreeMap<String, Usage>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UsageBucket {
    Models,
    Tools,
}

pub fn usage_doc() -> Result<DocToken<UsageState>> {
    doc_definition(
        "pi.usage",
        1,
        DocSemantics::Conversation(ConversationSemantics::Latest {
            fork: LatestFork::Initial,
        }),
        UsageState::default,
        Some(|_| true),
    )
}

pub fn record_usage(
    tx: &mut dyn Tx,
    conversation_id: ConversationId,
    bucket: UsageBucket,
    key: &str,
    usage: &Usage,
) -> Result<()> {
    let token = usage_doc()?;
    edit_doc(tx, &token, conversation_id, |state| {
        let totals = match bucket {
            UsageBucket::Models => &mut state.models,
            UsageBucket::Tools => &mut state.tools,
        };
        match totals.get_mut(key) {
            Some(total) => add_usage(total, usage),
            None => {
                totals.insert(key.to_owned(), usage.clone());
            }
        }
    })
}

pub fn add_usage(total: &mut Usage, usage: &Usage) {
    total.input += usage.input;
    total.output += usage.output;
    total.cache_read += usage.cache_read;
    total.cache_write += usage.cache_write;
    total.total_tokens += usage.total_tokens;
    if let Some(cache_write_1h) = usage.cache_write_1h {
        total.cache_write_1h = Some(total.cache_write_1h.unwrap_or(0) + cache_write_1h);
    }
    if let Some(reasoning) = usage.reasoning {
        total.reasoning = Some(total.reasoning.unwrap_or(0) + reasoning);
    }
    total.cost.input += usage.cost.input;
    total.cost.output += usage.cost.output;
    total.cost.cache_read += usage.cost.cache_read;
    total.cost.cache_write += usage.cost.cache_write;
    total.cost.total += usage.cost.total;
}

pub fn add_usage_state(sum: &mut UsageState, state: &UsageState) {
    for (sum_bucket, state_bucket) in [
        (&mut sum.models, &state.models),
        (&mut sum.tools, &state.tools),
    ] {
        for (key, usage) in state_bucket {
            match sum_bucket.get_mut(key) {
                Some(total) => add_usage(total, usage),
                None => {
                    sum_bucket.insert(key.clone(), usage.clone());
                }
            }
        }
    }
}
