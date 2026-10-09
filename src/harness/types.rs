use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::ai::models::Models;
use crate::ai::types::{CacheRetention, Tool, Transport, Usage, UserBlock, UserContent};
use crate::cancel::CancelToken;
use crate::env::ExecutionEnv;
use crate::errors::Result;
use crate::harness::agent::Agent;
use crate::harness::output::{OutputChunk, Retain, ShellOutputSkip};
use crate::harness::registry::RegistrySnapshot;
use crate::ids::{ConversationId, TaskId};
use crate::session::types::{DocumentObserver, DocumentReader, Tx};
use crate::types::{AnyTaskRecord, JsonValue, ToolDiagnostic};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Replay {
    Safe,
    Unsafe,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ToolControl {
    pub add_tools: Option<Vec<String>>,
    pub terminate: bool,
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

pub trait ToolExecutionApi: DocumentObserver + DocumentReader {
    fn task_id(&self) -> TaskId;
    fn conversation_id(&self) -> ConversationId;
    fn call_id(&self) -> &str;
    fn registry(&self) -> Arc<RegistrySnapshot>;
    fn agent(&self, cancel: &CancelToken) -> Result<Agent>;
    fn models(&self) -> &dyn Models;
    fn env(&self) -> Option<&dyn ExecutionEnv>;
    fn output(&self, chunk: OutputChunk<'_>, skipped: Option<ShellOutputSkip>) -> Result<()>;
    fn diagnostic(&self, diagnostic: ToolDiagnostic);
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
}

pub type ToolExecute = Arc<
    dyn Fn(JsonValue, &dyn ToolExecutionApi, &CancelToken) -> Result<ToolExecutionResult>
        + Send
        + Sync,
>;

pub type PrepareArguments = Arc<dyn Fn(JsonValue) -> JsonValue + Send + Sync>;

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
}

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompactionReason {
    Manual,
    Threshold,
    Overflow,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct ConversationStreamOptions {
    pub transport: Option<Transport>,
    pub timeout_ms: Option<u64>,
    pub max_retries: Option<u64>,
    pub max_retry_delay_ms: Option<u64>,
    pub headers: Option<BTreeMap<String, String>>,
    pub metadata: Option<crate::types::JsonObject>,
    pub cache_retention: Option<CacheRetention>,
    pub deferred: Option<Deferred>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeferredWindow {
    FifteenMinutes,
    OneHour,
    TwentyFourHours,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Deferred {
    Enabled(bool),
    Window(DeferredWindow),
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
