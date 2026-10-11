# boar-rs coverage of pi-durable

What Boar (Go, on PiG) calls, and whether pi-durable (Rust) covers it. Source of the call list: `internal/daemon`, `optchat`, `codemode` in the Boar repo, grepped for `harness.`, `durable.`, `ai.`, `coding.`, `mcpext.`, `sandbox.`, `nodeenv.`, `sqlitenode.`. Rust paths are under `src/`.

Status: **covered** = the Rust API exists and is used by the same shape; **divergent** = exists, different shape or semantics (see README); **missing** = no Rust counterpart.

## Harness and conversations

| Boar call | Rust | Status |
|---|---|---|
| `harness.OpenHarness`, `Resume`, `Close`, `Inspect` | `Harness::open`, `resume`, `close`, `inspect` (`harness/harness.rs`) | covered |
| `harness.Root`, `CreateConversation`, `Conversation` | `root`, `create_conversation`, `conversation` | covered |
| `conv.Submit`, `submissionHandle.Wait/Abort/Status` | `Conversation::submit`, `SubmissionHandle` (`harness/submissions.rs`) | covered |
| `conv.Abort`, `WaitForIdle`, `harness.AbortTask`, `WaitForTask` | `abort`, `wait_for_idle`, `abort_task`, `wait_for_task` | covered |
| `conv.Configure` with `harness.SetTo`/`AgentChange` | `Conversation::configure`, `AgentChange` (`harness/agent.rs`) | covered |
| `conv.Compact`, `conv.Reset`, `conv.Fork` | `compact`, `reset`, `fork` | covered |
| `conv.Context`, `conv.Entries`, `conv.ViewState`, `conv.Watch` | `context`, `entries`, `view_state`, `watch` | covered |
| `harness.Commit`, `durable.Commit` (tx over docs) | `Harness::commit`, `session::types::Tx` | covered |
| `harness.SubscribeCommits`, `SnapshotErased` | `watch_task_graph`, `harness/events.rs` `watch_events`, `Snapshot` | divergent: no commit-subscribe callback; use `DocumentWatch` |
| `harness.TaskGraph` / `WatchTaskGraph` | `task_graph`, `watch_task_graph` | covered |
| `harness.Usage`, `RecordUsage`, `NewUsageState` | `usage`, `usage.rs` `record_usage`, `UsageState` | covered |
| `harness.PrepareBoundary`, `ApplyBoundary`, `StartRun`, `AdmitSubmission`, `QueueModesOf`, `ResolveSettings` | `inbox::prepare_boundary`, `inbox::apply_boundary`, `generation::start_run`, `submissions::admit_submission`, `Settings` | covered (Boar's `engine.go:582-590` interrupt-and-run maps to these; not exercised end to end) |
| `harness.CreateRegistry`, `registry.Install`, `Uninstall`, `Snapshot` | `Registry`, `RegistrySnapshot` (`harness/registry.rs`) | covered (`install`, `uninstall`, `installed`) |
| `harness.Hook(GenerationTask/ToolTask/CompactionTask, …)` | `HookRegistration::new(&GENERATION_HOOK/TOOL_HOOK/COMPACTION_HOOK, …)` | covered by commit 110e427 (hooks reach tasks via `erased::each_hook`) |
| `harness.GenerationHooks{BeforeRequest}`, `ToolHooks{AfterTool}`, `CompactionHooks{BeforeCompact}` | same field names, closures take `&HookApi` (conversation id only) | divergent: `HookApi` has the conversation id only, no memo or models; `optchat/turn.go` reads `api.ConversationId()` and can use `HookApi.conversation_id` |
| `harness.Section`, `SectionOptions{Tag}`, `PromptSection` | `PromptSection`, `render_sections` (`harness/prompt.rs`) | covered |
| `harness.Extension`, `ToolRegistration`, `durable.Extension` | `Extension`, `ToolRegistration` (`harness/types.rs`) | covered |
| `durable.DefineTask`, `CreateTask`, `TaskDefinition` (optchat compactor, spawn) | `session::types::TaskCreate`, `HarnessTaskDefinition` (`harness/runtime.rs`) | divergent: typed via `HarnessTaskDefinition`, no `DefineTask` builder |
| `durable.DefineDoc`, `DocDefinition`, `TxDoc` | `documents.rs`, `DocToken` | covered |
| `durable.ToolExecutionApi` (memo, output, diagnostic, details, commit, conversation, getTask, waitForTask, watchDoc) | `ToolExecutionApi` (`harness/types.rs`) | covered |
| `durable.ToolExecutionResult`, `ToolReplay`, `ToolControl` | same names (`harness/types.rs`) | covered |
| `harness.BoundedOutput`, `OutputBuffer`, `OutputLimits` | `harness/output.rs` | covered |

## Models, providers, auth

| Boar call | Rust | Status |
|---|---|---|
| `ai.Model`, `ai.Message`, `ai.UserText`, `ai.AssistantMessage`, `ai.TextContent`, `ai.ToolCall` etc. | `ai/types.rs` | covered |
| `ai.StreamOptions`, `ai.SimpleStreamOptions` | `ai/simple_options.rs`, `ai/types.rs` | covered |
| `ai.NewFauxProvider`, `FauxConfig`, `FauxResponse`… | `ai/providers/faux.rs` | covered |
| `ai.BuiltinProviderAuth`, `ai.APIKeyProviders`, `ai.GetOAuthProvider(s)`, `ai.RegisterOAuthProvider` | `ai/registry.rs` `Providers::builtin`, `set_auth` resolver, `ai/auth.rs` `env_api_key` | divergent: one resolver callback per request replaces the registries; multi-account selection is the resolver's job |
| `ai.CredentialFromOAuth`, `ai.Credential*`, `ai.AuthStorage` | none | **missing** |
| Login flow events (`ai.Auth*Prompt`, `AuthURLEvent`, `AuthDeviceCodeEvent`…) (`internal/daemon/login.go`) | `ai/oauth/anthropic.rs` (authorize link, callback listener, manual-code parsing, exchange, refresh) | divergent: plain functions, no event types; other providers' OAuth not ported |
| Anthropic messages API (`ai.APIAnthropicMessages`) | `ai/api/anthropic/` (feature `anthropic`; also serves the Vercel AI Gateway) | covered |
| `ai.ConfigureHTTPDispatcher` | `ai/http.rs` (hand-written HTTP/1.1 over rustls, feature `http`) | divergent: no global dispatcher setting |
| `ai.GetCurrentTools`, `ai.GrammarFormat*`, `ai.ConstrainedSamplingConfig` | none | **missing** (only needed by codemode) |
| `ai.MergeProviderHeaders`, `ai.ProviderEnv` | none | **missing** (minor) |
| `harness` model/agent settings for `Models` | `ai/models.rs` `Models` trait | covered |

## Coding services, skills, prompts

| Boar call | Rust | Status |
|---|---|---|
| `coding.NewServices`, `ModelRuntime`, `SettingsManager` | none (settings file loading absent) | **missing** |
| `coding.NewDefaultResourceLoader` (skills, prompt templates, project context from AGENTS.md) | none | **missing** |
| `coding.Skill`, `coding.PromptTemplate` expansion (`prompts.go`) | none | **missing** |
| `coding.Services` sessions dir / agent dir layout | none | **missing** |
| `tools.CodingTools` (read, write, edit, bash) | `harness/coding_tools.rs`, `tools/{read,write,edit,bash}.rs` | covered |
| `extension.AgentTool`, `extension.Tools`, `extension.NewOrderedStrings` | none (plain `ToolRegistration`) | divergent, not needed if Boar uses `ToolRegistration` |

## Environment, storage, tools

| Boar call | Rust | Status |
|---|---|---|
| `sqlitenode.OpenNodeSqliteStorage` | `storage/sqlite.rs`, `storage/sqlite/` | covered (Rust sqlite via rusqlite; no Node) |
| `nodeenv.NewNodeExecutionEnv`, `env.ExecutionEnv`, `env.Get/Keys` | `env/node.rs`, `env/mod.rs` `ExecutionEnv` | covered |
| `harness.HarnessOptions.Env` (env builder) | `HarnessOptions.env`, `Harness::build_env` | covered |
| `durable.Storage`, `MemoryStorage` | `storage/memory.rs`, `storage/mod.rs` | covered |
| `durable.RetainHead/RetainTail`, `DEFAULT_MAX_LINES/BYTES` | `harness/output.rs` `Retain` (Head/Tail), `truncate.rs` `TruncationOptions` | covered |
| `source.Parse` (git source for furrows) | none | **missing** |

## MCP and codemode

| Boar call | Rust | Status |
|---|---|---|
| `mcpext.LoadMcpConfig`, `NewConnection`, `CreateDefaultTransport`, `ConvertMcpResult`, `McpOAuthCredentialStore` | none (no `mcp` module) | **missing** |
| `codemode.New`, `builtin.CreateDescription`, `sandbox.*` (JS sandbox) | none | **missing** |
| `toolsearch.NewBm25Ranker` | none | **missing** |

## Optchat (furrow memory)

| Boar call | Rust | Status |
|---|---|---|
| `optchat` built on `durable.Commit`, docs, tasks, `harness.Section`, `Hook` | covered by the rows above | depends on the hook and HookApi gaps |

## Ranked gaps

1. **Landed since the audit:** Anthropic Messages, OpenAI Chat Completions and Responses, the Vercel AI Gateway, Claude OAuth functions, the `set_auth` resolver. Boar-rs supplies its own credential store (three accounts), refresh and login events.
2. **Account selection** is the resolver's job; nothing in the crate rotates or stores tokens.
3. **HookApi memo and models.** Hooks now get the conversation id; memo, models and conversation access inside hooks remain missing.
4. **Coding services and resource loader** (settings file, skills, prompt templates, AGENTS.md). Needed for Boar's prompts (`prompts.go`). Likely >100 lines; list, not fix.
5. **MCP client, codemode sandbox, toolsearch**. Large; separate projects in Boar. Can be left out of the first boar-rs cut.
6. **Commit subscription callback** (`SubscribeCommits`) for the daemon's hub. Could use `DocumentWatch`; decide in boar-rs.
7. **Git source parsing** (`source.Parse`) and grammar format for codemode: small, only with codemode.

## Fixed in this task

- Hook wiring: `HookTask<H>`, `TOOL_HOOK`, `GENERATION_HOOK`, `COMPACTION_HOOK`; `HooksOf` and `HarnessTaskDefinition.hooks` removed (commit 110e427).
