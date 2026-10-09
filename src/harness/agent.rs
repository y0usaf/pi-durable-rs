use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::ai::types::ModelThinkingLevel;
use crate::cancel::CancelToken;
use crate::documents::{DocSemantics, DocToken};
use crate::errors::{Error, Result};
use crate::harness::registry::RegistrySnapshot;
use crate::harness::types::{
    CompactionOverrides, CompactionPolicy, ConversationRetryPolicy, Extension, HarnessSettings,
    ModelRef, ProgressPolicy, PromptInput, PromptSection, Render, RetryOverrides, Settings,
    ToolRegistration,
};
use crate::harness::util::{doc_definition, edit_doc};
use crate::ids::ConversationId;
use crate::session::types::Tx;
use crate::types::{ConversationRecord, ConversationSemantics, RewindableFork};

pub const INSTRUCTIONS_KEY: &str = "instructions";

pub const DEFAULT_RETRY_POLICY: ConversationRetryPolicy = ConversationRetryPolicy {
    enabled: true,
    max_retries: 3,
    base_delay_ms: 2000,
    max_agent_delay_ms: Some(60000),
};

pub const DEFAULT_COMPACTION_POLICY: CompactionPolicy = CompactionPolicy {
    enabled: true,
    reserve_tokens: 16384,
    keep_recent_tokens: 20000,
    background_tokens: 32768,
};

pub const DEFAULT_PROGRESS_POLICY: ProgressPolicy = ProgressPolicy {
    partial_interval_ms: 100,
    output_interval_ms: 100,
};

const DEFAULT_CONTEXT_RETENTION_MS: u64 = 600_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ExtensionSelection {
    List(Vec<String>),
    Edit {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        add: Option<Vec<String>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        remove: Option<Vec<String>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ToolSelection {
    List(Vec<String>),
    Remove { remove: Vec<String> },
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentState {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<ModelRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub thinking_level: Option<ModelThinkingLevel>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extensions: Option<ExtensionSelection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tools: Option<ToolSelection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
}

pub fn agent_doc() -> Result<DocToken<AgentState>> {
    doc_definition(
        "pi.agent",
        1,
        DocSemantics::Conversation(ConversationSemantics::Rewindable {
            fork: RewindableFork::AsOf,
        }),
        AgentState::default,
        Some(|_| true),
    )
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum Change<T> {
    #[default]
    Keep,
    Clear,
    Set(T),
}

#[derive(Clone)]
pub enum ExtensionsChange {
    List(Vec<Arc<Extension>>),
    Edit {
        add: Option<Vec<Arc<Extension>>>,
        remove: Option<Vec<Arc<Extension>>>,
    },
}

#[derive(Clone)]
pub enum ToolsChange {
    List(Vec<ToolRegistration>),
    Remove(Vec<ToolRegistration>),
}

#[derive(Clone, Default)]
pub struct AgentChange {
    pub model: Change<ModelRef>,
    pub thinking_level: Change<ModelThinkingLevel>,
    pub extensions: Change<ExtensionsChange>,
    pub tools: Change<ToolsChange>,
    pub instructions: Change<String>,
    pub cwd: Change<String>,
}

#[derive(Clone)]
pub struct Agent {
    pub model: Option<ModelRef>,
    pub thinking_level: ModelThinkingLevel,
    pub extensions: Vec<Arc<Extension>>,
    pub tools: Vec<ToolRegistration>,
    pub sections: Vec<PromptSection>,
    pub instructions: Option<String>,
    pub cwd: Option<String>,
}

pub fn resolve_settings(settings: Option<&HarnessSettings>) -> Settings {
    let retry = settings.and_then(|settings| settings.retry.as_ref());
    let compaction = settings.and_then(|settings| settings.compaction.as_ref());
    let progress = settings.and_then(|settings| settings.progress.as_ref());
    Settings {
        extensions: settings.and_then(|settings| settings.extensions.clone()),
        stream: settings
            .and_then(|settings| settings.stream.clone())
            .unwrap_or_default(),
        retry: resolve_retry(retry),
        compaction: resolve_compaction(compaction),
        progress: ProgressPolicy {
            partial_interval_ms: progress
                .and_then(|progress| progress.partial_interval_ms)
                .unwrap_or(DEFAULT_PROGRESS_POLICY.partial_interval_ms),
            output_interval_ms: progress
                .and_then(|progress| progress.output_interval_ms)
                .unwrap_or(DEFAULT_PROGRESS_POLICY.output_interval_ms),
        },
        tool_execution: settings
            .and_then(|settings| settings.tool_execution)
            .unwrap_or(crate::harness::types::ToolExecutionMode::Parallel),
        steering_mode: settings
            .and_then(|settings| settings.steering_mode)
            .unwrap_or(crate::harness::types::QueueMode::OneAtATime),
        follow_up_mode: settings
            .and_then(|settings| settings.follow_up_mode)
            .unwrap_or(crate::harness::types::QueueMode::OneAtATime),
        context_retention_ms: settings
            .and_then(|settings| settings.context_retention_ms)
            .unwrap_or(DEFAULT_CONTEXT_RETENTION_MS),
    }
}

fn resolve_retry(overrides: Option<&RetryOverrides>) -> ConversationRetryPolicy {
    let base = DEFAULT_RETRY_POLICY;
    match overrides {
        None => base,
        Some(overrides) => ConversationRetryPolicy {
            enabled: overrides.enabled.unwrap_or(base.enabled),
            max_retries: overrides.max_retries.unwrap_or(base.max_retries),
            base_delay_ms: overrides.base_delay_ms.unwrap_or(base.base_delay_ms),
            max_agent_delay_ms: overrides.max_agent_delay_ms.or(base.max_agent_delay_ms),
        },
    }
}

fn resolve_compaction(overrides: Option<&CompactionOverrides>) -> CompactionPolicy {
    let base = DEFAULT_COMPACTION_POLICY;
    match overrides {
        None => base,
        Some(overrides) => CompactionPolicy {
            enabled: overrides.enabled.unwrap_or(base.enabled),
            reserve_tokens: overrides.reserve_tokens.unwrap_or(base.reserve_tokens),
            keep_recent_tokens: overrides
                .keep_recent_tokens
                .unwrap_or(base.keep_recent_tokens),
            background_tokens: overrides
                .background_tokens
                .unwrap_or(base.background_tokens),
        },
    }
}

pub fn configure(
    tx: &mut dyn Tx,
    conversation_id: ConversationId,
    change: &AgentChange,
) -> Result<()> {
    let token = agent_doc()?;
    edit_doc(tx, &token, conversation_id, |state| {
        apply_change(state, change)
    })
}

fn apply_change(state: &mut AgentState, change: &AgentChange) {
    match &change.model {
        Change::Keep => {}
        Change::Clear => state.model = None,
        Change::Set(model) => state.model = Some(model.clone()),
    }
    match change.thinking_level {
        Change::Keep => {}
        Change::Clear => state.thinking_level = None,
        Change::Set(level) => state.thinking_level = Some(level),
    }
    match &change.extensions {
        Change::Keep => {}
        Change::Clear => state.extensions = None,
        Change::Set(ExtensionsChange::List(extensions)) => {
            state.extensions = Some(ExtensionSelection::List(extension_names(extensions)));
        }
        Change::Set(ExtensionsChange::Edit { add, remove }) => {
            state.extensions = Some(ExtensionSelection::Edit {
                add: add.as_deref().map(extension_names),
                remove: remove.as_deref().map(extension_names),
            });
        }
    }
    match &change.tools {
        Change::Keep => {}
        Change::Clear => state.tools = None,
        Change::Set(ToolsChange::List(tools)) => {
            state.tools = Some(ToolSelection::List(tool_names(tools)));
        }
        Change::Set(ToolsChange::Remove(tools)) => {
            state.tools = Some(ToolSelection::Remove {
                remove: tool_names(tools),
            });
        }
    }
    match &change.instructions {
        Change::Keep => {}
        Change::Clear => state.instructions = None,
        Change::Set(text) => state.instructions = Some(text.clone()),
    }
    match &change.cwd {
        Change::Keep => {}
        Change::Clear => state.cwd = None,
        Change::Set(cwd) => state.cwd = Some(cwd.clone()),
    }
}

fn extension_names(extensions: &[Arc<Extension>]) -> Vec<String> {
    extensions
        .iter()
        .map(|extension| extension.name.clone())
        .collect()
}

fn tool_names(tools: &[ToolRegistration]) -> Vec<String> {
    tools.iter().map(|tool| tool.tool.name.clone()).collect()
}

pub fn add_tools(tx: &mut dyn Tx, conversation_id: ConversationId, added: &[String]) -> Result<()> {
    let token = agent_doc()?;
    edit_doc(tx, &token, conversation_id, |state| {
        match state.tools.clone() {
            None => {}
            Some(ToolSelection::List(mut tools)) => {
                for name in added {
                    if !tools.contains(name) {
                        tools.push(name.clone());
                    }
                }
                state.tools = Some(ToolSelection::List(tools));
            }
            Some(ToolSelection::Remove { remove })
                if remove.iter().any(|name| added.contains(name)) =>
            {
                let kept = remove
                    .into_iter()
                    .filter(|name| !added.contains(name))
                    .collect();
                state.tools = Some(ToolSelection::Remove { remove: kept });
            }
            Some(ToolSelection::Remove { .. }) => {}
        }
    })
}

pub fn create_agent(tx: &mut dyn Tx, conversation: &ConversationRecord) -> Result<()> {
    if conversation.parent.is_some() {
        return Ok(());
    }
    let token = agent_doc()?;
    let agent = tx.doc(
        crate::harness::util::conversation_doc(token.definition(), conversation.id),
        None,
    )?;
    let Some(owner) = &conversation.owner else {
        return Ok(());
    };
    let owner_draft = tx.doc(
        crate::harness::util::conversation_doc(token.definition(), owner.conversation_id),
        None,
    )?;
    let source = tx.draft(owner_draft)?.clone();
    if let (serde_json::Value::Object(source), serde_json::Value::Object(target)) =
        (source, tx.draft(agent)?)
    {
        for (key, value) in source {
            target.insert(key, value);
        }
    }
    Ok(())
}

pub fn resolve_agent(
    state: Option<&AgentState>,
    snapshot: &RegistrySnapshot,
    settings: &Settings,
    report: &dyn Fn(Error),
) -> Agent {
    let extensions = select_extensions(
        state.and_then(|state| state.extensions.as_ref()),
        snapshot,
        settings,
    );
    let mut tools: Vec<(String, ToolRegistration)> = Vec::new();
    for extension in &extensions {
        for tool in &extension.tools {
            set_entry(&mut tools, tool.tool.name.clone(), tool.clone());
        }
    }
    let mut sections: Vec<(String, PromptSection)> = Vec::new();
    for extension in &extensions {
        for section in &extension.sections {
            set_entry(&mut sections, section.key.clone(), section.clone());
        }
    }
    for extension in &extensions {
        for wrap in &extension.wraps {
            match wrap {
                crate::harness::types::Wrap::Tool { tool, wrap } => apply_wrap(
                    &mut tools,
                    tool,
                    |item| wrap(item),
                    |item| &item.tool.name,
                    report,
                ),
                crate::harness::types::Wrap::Section { section, wrap } => apply_wrap(
                    &mut sections,
                    section,
                    |item| wrap(item),
                    |item| &item.key,
                    report,
                ),
            }
        }
    }
    let tools: Vec<ToolRegistration> = match state.and_then(|state| state.tools.as_ref()) {
        None => tools.into_iter().map(|(_, tool)| tool).collect(),
        Some(ToolSelection::List(names)) => {
            let mut chosen: Vec<&str> = Vec::new();
            for name in names {
                if !chosen.contains(&name.as_str()) {
                    chosen.push(name);
                }
            }
            chosen
                .into_iter()
                .filter_map(|name| {
                    tools
                        .iter()
                        .find(|(key, _)| key == name)
                        .map(|(_, tool)| tool.clone())
                })
                .collect()
        }
        Some(ToolSelection::Remove { remove }) => tools
            .into_iter()
            .filter(|(name, _)| !remove.contains(name))
            .map(|(_, tool)| tool)
            .collect(),
    };
    let mut sections: Vec<PromptSection> =
        sections.into_iter().map(|(_, section)| section).collect();
    let instructions = state.and_then(|state| state.instructions.clone());
    if let Some(text) = &instructions {
        sections.push(instructions_section(text.clone()));
    }
    Agent {
        model: state.and_then(|state| state.model.clone()),
        thinking_level: state
            .and_then(|state| state.thinking_level)
            .unwrap_or(ModelThinkingLevel::Off),
        extensions,
        tools,
        sections,
        instructions,
        cwd: state.and_then(|state| state.cwd.clone()),
    }
}

fn instructions_section(text: String) -> PromptSection {
    let render: Render =
        Arc::new(move |_: &PromptInput<'_>, _: &CancelToken| Ok(Some(text.clone())));
    PromptSection {
        key: INSTRUCTIONS_KEY.to_owned(),
        render,
        tag: None,
    }
}

fn select_extensions(
    stored: Option<&ExtensionSelection>,
    snapshot: &RegistrySnapshot,
    settings: &Settings,
) -> Vec<Arc<Extension>> {
    let selected: Vec<String> = match stored {
        Some(ExtensionSelection::List(names)) => names.clone(),
        other => {
            let base: Vec<String> = match &settings.extensions {
                Some(extensions) => extension_names(extensions),
                None => snapshot
                    .installed()
                    .iter()
                    .map(|extension| extension.name.clone())
                    .collect(),
            };
            let (add, remove) = match other {
                Some(ExtensionSelection::Edit { add, remove }) => (
                    add.clone().unwrap_or_default(),
                    remove.clone().unwrap_or_default(),
                ),
                _ => (Vec::new(), Vec::new()),
            };
            base.into_iter()
                .chain(add)
                .filter(|name| !remove.contains(name))
                .collect()
        }
    };
    let mut unique: Vec<String> = Vec::new();
    for name in selected {
        if !unique.contains(&name) {
            unique.push(name);
        }
    }
    unique
        .iter()
        .filter_map(|name| snapshot.extension(name).cloned())
        .collect()
}

fn set_entry<T>(entries: &mut Vec<(String, T)>, key: String, value: T) {
    match entries.iter_mut().find(|(existing, _)| *existing == key) {
        Some(entry) => entry.1 = value,
        None => entries.push((key, value)),
    }
}

fn apply_wrap<T: Clone>(
    entries: &mut Vec<(String, T)>,
    target: &str,
    wrap: impl FnOnce(T) -> Result<T>,
    name_of: impl Fn(&T) -> &str,
    report: &dyn Fn(Error),
) {
    let Some(index) = entries.iter().position(|(key, _)| key == target) else {
        return;
    };
    let item = entries[index].1.clone();
    let outcome = wrap(item).and_then(|wrapped| {
        if name_of(&wrapped) == target {
            Ok(wrapped)
        } else {
            Err(Error::Invalid(format!(
                "Wrapper renamed {target} to {}",
                name_of(&wrapped)
            )))
        }
    });
    match outcome {
        Ok(wrapped) => entries[index].1 = wrapped,
        Err(error) => {
            entries.remove(index);
            report(error);
        }
    }
}
