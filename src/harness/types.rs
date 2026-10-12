use std::any::Any;
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::ai::models::Models;
use crate::ai::types::{CacheRetention, Tool, ToolCall, Transport, Usage, UserBlock, UserContent};
use crate::cancel::CancelToken;
use crate::env::ExecutionEnv;
use crate::errors::{Error, Result};
use crate::harness::agent::{Agent, AgentChange};
use crate::harness::output::{OutputChunk, Retain, ShellOutputSkip};
use crate::harness::registry::{Registry, RegistrySnapshot};
use crate::harness::scheduler::ErasedTask;
use crate::harness::submissions::{SubmissionDraft, SubmissionHandle};
use crate::ids::{ConversationId, EntryId, TaskId};
use crate::session::types::{DocumentObserver, DocumentReader, Tx};
use crate::types::{
    AnyTaskRecord, ConversationOwnership, ConversationRecord, JsonObject, JsonValue, ToolDiagnostic,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ModelRef {
    pub provider: String,
    pub model_id: String,
}

pub type UserInput = UserContent;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueueMode {
    #[serde(rename = "all")]
    All,
    #[serde(rename = "one-at-a-time")]
    OneAtATime,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ToolExecutionMode {
    Parallel,
    Sequential,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Replay {
    Safe,
    Unsafe,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ToolControl {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub add_tools: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub terminate: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handoff: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ToolExecutionResult {
    pub content: Option<Vec<UserBlock>>,
    pub is_error: Option<bool>,
    pub details: Option<JsonValue>,
    pub diagnostics: Option<Vec<ToolDiagnostic>>,
    pub usage: Option<Usage>,
    pub control: Option<ToolControl>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OutputLimitsSpec {
    pub max_bytes: Option<usize>,
    pub max_lines: Option<usize>,
    pub retain: Option<Retain>,
}

pub struct InvocationBinding {
    pub signal: CancelToken,
    pub check: Arc<dyn Fn() -> Result<()> + Send + Sync>,
}

pub type ConversationBinder = Arc<
    dyn Fn(
            ConversationId,
            InvocationBinding,
            &CancelToken,
        ) -> Result<Option<Box<dyn ConversationHandle>>>
        + Send
        + Sync,
>;

pub trait ConversationHandle: Send + Sync {
    fn id(&self) -> ConversationId;
    fn submit(&self, draft: SubmissionDraft, cancel: &CancelToken) -> Result<SubmissionHandle>;
    fn abort(&self, background: bool, cancel: &CancelToken) -> Result<()>;
    fn wait_for_idle(&self, cancel: &CancelToken) -> Result<()>;
}

pub trait ToolExecutionApi: DocumentObserver + DocumentReader + Sync {
    fn task_id(&self) -> TaskId;
    fn conversation_id(&self) -> ConversationId;
    fn call_id(&self) -> &str;
    fn assistant_entry(&self) -> Option<EntryId> {
        None
    }
    fn registry(&self) -> Arc<RegistrySnapshot>;
    fn agent(&self, cancel: &CancelToken) -> Result<Agent>;
    fn models(&self) -> &dyn Models;
    fn env(&self) -> Option<&dyn ExecutionEnv>;
    fn conversation(
        &self,
        id: ConversationId,
        cancel: &CancelToken,
    ) -> Result<Option<Box<dyn ConversationHandle>>>;
    fn output(&self, chunk: OutputChunk<'_>, skipped: Option<ShellOutputSkip>) -> Result<()>;
    fn diagnostic(&self, diagnostic: ToolDiagnostic) -> Result<()>;
    fn details(&self, value: JsonValue, cancel: &CancelToken) -> Result<()>;
    fn commit(
        &self,
        change: &mut dyn FnMut(&mut dyn Tx) -> Result<()>,
        cancel: &CancelToken,
    ) -> Result<()>;
    fn memo(&self, name: &str, cancel: &CancelToken) -> Result<Option<JsonValue>>;
    fn memo_or_set(
        &self,
        name: &str,
        candidate: JsonValue,
        cancel: &CancelToken,
    ) -> Result<JsonValue>;
    fn get_task(&self, id: TaskId, cancel: &CancelToken) -> Result<Option<AnyTaskRecord>>;
    fn before_tool(
        &self,
        _tool: &ToolRegistration,
        call: &ToolCall,
        _cancel: &CancelToken,
    ) -> Result<ToolGate> {
        Ok(ToolGate::Run(call.arguments.clone()))
    }
    fn after_tool(
        &self,
        _call: &ToolCall,
        result: ToolExecutionResult,
        _cancel: &CancelToken,
    ) -> Result<ToolExecutionResult> {
        Ok(result)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ToolGate {
    Run(JsonObject),
    Blocked(Box<ToolExecutionResult>),
}

pub type ToolExecute = Arc<
    dyn Fn(JsonValue, &dyn ToolExecutionApi, &CancelToken) -> Result<ToolExecutionResult>
        + Send
        + Sync,
>;

pub type PrepareArguments = Arc<dyn Fn(JsonValue) -> Result<JsonValue> + Send + Sync>;

#[derive(Clone)]
pub struct ToolRegistration {
    pub tool: Tool,
    pub replay: Option<Replay>,
    pub execution_mode: Option<ToolExecutionMode>,
    pub output_limits: Option<OutputLimitsSpec>,
    pub prepare_arguments: Option<PrepareArguments>,
    pub execute: ToolExecute,
}

pub struct PromptInput<'a> {
    pub conversation_id: ConversationId,
    pub agent: &'a Agent,
    pub env: Option<&'a dyn ExecutionEnv>,
    pub shown: &'a BTreeMap<String, String>,
    pub read: &'a dyn DocumentReader,
}

pub type Render =
    Arc<dyn for<'a> Fn(&PromptInput<'a>, &CancelToken) -> Result<Option<String>> + Send + Sync>;

#[derive(Clone)]
pub struct PromptSection {
    pub key: String,
    pub render: Render,
    pub tag: Option<bool>,
}

#[derive(Clone)]
pub enum Wrap {
    Tool {
        tool: String,
        wrap: Arc<dyn Fn(ToolRegistration) -> Result<ToolRegistration> + Send + Sync>,
    },
    Section {
        section: String,
        wrap: Arc<dyn Fn(PromptSection) -> Result<PromptSection> + Send + Sync>,
    },
}

#[derive(Clone)]
pub struct Extension {
    pub name: String,
    pub tools: Vec<ToolRegistration>,
    pub sections: Vec<PromptSection>,
    pub wraps: Vec<Wrap>,
    pub hooks: Vec<HookRegistration>,
    pub tasks: Vec<Arc<dyn ErasedTask>>,
}

/// Names a task together with the handler type its hooks take, so a registration
/// cannot pair a task with handlers of another task's type.
pub struct HookTask<H> {
    name: &'static str,
    handlers: PhantomData<fn() -> H>,
}

impl<H> HookTask<H> {
    /// Names the task `name`, whose hooks are `H`. Built-in tasks export one, such as
    /// `tool::TOOL_HOOK`.
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            handlers: PhantomData,
        }
    }
}

pub struct HookApi {
    pub conversation_id: ConversationId,
}

#[derive(Clone)]
pub struct HookRegistration {
    pub task: String,
    pub handlers: Arc<dyn Any + Send + Sync>,
}

impl HookRegistration {
    /// Registers `handlers` for the task `task`, as an extension's `hooks` entry.
    pub fn new<H: Any + Send + Sync>(task: &HookTask<H>, handlers: H) -> Self {
        Self {
            task: task.name.to_owned(),
            handlers: Arc::new(handlers),
        }
    }
}

pub struct EnvTarget<'a> {
    pub conversation_id: ConversationId,
    pub cwd: Option<String>,
    pub read: &'a dyn DocumentReader,
}

pub type EnvBuilder = Arc<
    dyn Fn(&EnvTarget<'_>, &CancelToken) -> Result<Option<Arc<dyn ExecutionEnv>>> + Send + Sync,
>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConversationRetryPolicy {
    pub enabled: bool,
    pub max_retries: u64,
    pub base_delay_ms: u64,
    pub max_agent_delay_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionPolicy {
    pub enabled: bool,
    pub reserve_tokens: u64,
    pub keep_recent_tokens: u64,
    pub background_tokens: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProgressPolicy {
    pub partial_interval_ms: u64,
    pub output_interval_ms: u64,
}

pub use crate::types::CompactionReason;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ConversationStreamOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transport: Option<Transport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_retries: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_retry_delay_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub headers: Option<BTreeMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<crate::types::JsonObject>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cache_retention: Option<CacheRetention>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deferred: Option<Deferred>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeferredWindow {
    #[serde(rename = "15m")]
    FifteenMinutes,
    #[serde(rename = "1h")]
    OneHour,
    #[serde(rename = "24h")]
    TwentyFourHours,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Deferred {
    Enabled(bool),
    Window {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        window: Option<DeferredWindow>,
    },
}

#[derive(Debug, Clone, Default)]
pub struct RetryOverrides {
    pub enabled: Option<bool>,
    pub max_retries: Option<u64>,
    pub base_delay_ms: Option<u64>,
    pub max_agent_delay_ms: Option<u64>,
}

#[derive(Debug, Clone, Default)]
pub struct CompactionOverrides {
    pub enabled: Option<bool>,
    pub reserve_tokens: Option<u64>,
    pub keep_recent_tokens: Option<u64>,
    pub background_tokens: Option<u64>,
}

#[derive(Debug, Clone, Default)]
pub struct ProgressOverrides {
    pub partial_interval_ms: Option<u64>,
    pub output_interval_ms: Option<u64>,
}

#[derive(Clone, Default)]
pub struct HarnessSettings {
    pub extensions: Option<Vec<Arc<Extension>>>,
    pub stream: Option<ConversationStreamOptions>,
    pub retry: Option<RetryOverrides>,
    pub compaction: Option<CompactionOverrides>,
    pub progress: Option<ProgressOverrides>,
    pub tool_execution: Option<ToolExecutionMode>,
    pub steering_mode: Option<QueueMode>,
    pub follow_up_mode: Option<QueueMode>,
    pub context_retention_ms: Option<u64>,
}

#[derive(Clone)]
pub struct Settings {
    pub extensions: Option<Vec<Arc<Extension>>>,
    pub stream: ConversationStreamOptions,
    pub retry: ConversationRetryPolicy,
    pub compaction: CompactionPolicy,
    pub progress: ProgressPolicy,
    pub tool_execution: ToolExecutionMode,
    pub steering_mode: QueueMode,
    pub follow_up_mode: QueueMode,
    pub context_retention_ms: u64,
}

pub type ConversationCreated =
    Arc<dyn Fn(&mut dyn Tx, &ConversationRecord) -> Result<()> + Send + Sync>;
pub type ErrorReport = Arc<dyn Fn(Error) + Send + Sync>;
pub type ConversationInit = Box<dyn FnOnce(&mut dyn Tx, ConversationId) -> Result<()> + Send>;

pub struct HarnessOptions {
    pub models: Arc<dyn Models + Send + Sync>,
    pub registry: Arc<Registry>,
    pub settings: Option<HarnessSettings>,
    pub conversation_created: Option<ConversationCreated>,
    pub now: Option<Arc<dyn Fn() -> u64 + Send + Sync>>,
    pub env: Option<EnvBuilder>,
    pub on_report: Option<ErrorReport>,
}

#[derive(Default)]
pub struct RootOptions {
    pub agent: Option<AgentChange>,
    pub init: Option<ConversationInit>,
}

pub struct ConversationCreateOptions {
    pub ownership: ConversationOwnership,
    pub agent: Option<AgentChange>,
    pub init: Option<ConversationInit>,
}
