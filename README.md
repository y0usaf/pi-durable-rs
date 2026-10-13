# pi-durable

A Rust library that keeps an LLM agent's conversations, tool calls and background tasks in a transactional store, so a process that dies mid-turn picks up where it stopped.

It won't: ship a CLI or a UI; bring an async runtime; follow redirects or speak through proxies; talk to every provider Pi does (three wire formats are real, the rest are yours to add behind the `Api` trait); store your credentials (you hand it keys or a resolver, and it never reads or writes a token file); run on Windows.

## Why it exists

Pi's TypeScript package [`@earendil-works/pi-durable`](https://github.com/earendil-works/pi) 1.1.0 already does this job well. This crate is the same design for programs that are written in Rust and don't want a Node process next to them. It is a port, not a redesign: behaviour is translated from the TypeScript at one pinned commit (`f1b2e77f5b13b2a199b1052cb79c235451afe7d7`, a flake input in `flake.nix`), together with the parts of `pi-ai` its harness needs. A flake check runs both implementations on the same scripted conversations and compares what they wrote down (see [What the parity check covers](#what-the-parity-check-covers)).

The two can share a SQLite file: the check kills a TypeScript or Rust writer mid-tool-call and lets the other implementation finish the turn (`parity/run.sh`, the `cross` cells).

Do not mistake it for finished. Read [Where it differs](#where-it-differs-from-typescript-pi-durable) before you build on it.

## Try it

It is not on crates.io yet (`repository` is unset in `Cargo.toml`), so depend on it by path or git:

```toml
pi-durable = { path = "../pi-durable-rs", features = ["providers"] }
serde_json = "1"
```

The example below uses the scripted `faux` model, so it runs offline. This example builds and runs offline; a second run continues the same conversation from `agent.db`, so the reply prints twice:

```rust
use std::sync::Arc;

use pi_durable::ai::providers::faux::{Faux, FauxOptions, FauxResponse};
use pi_durable::ai::types::{AssistantBlock, Message};
use pi_durable::cancel::CancelToken;
use pi_durable::harness::agent::{AgentChange, Change};
use pi_durable::harness::harness::Harness;
use pi_durable::harness::registry::Registry;
use pi_durable::harness::submissions::{SubmissionDraft, WhenBusy};
use pi_durable::harness::types::{Extension, HarnessOptions, ModelRef, RootOptions};
use pi_durable::storage::sqlite::SqliteStorage;
use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cancel = CancelToken::new();

    let faux = Arc::new(Faux::new(FauxOptions::default()));
    faux.set_responses(vec![FauxResponse::Message(Box::new(serde_json::from_value(json!({
        "role": "assistant", "content": [{"type": "text", "text": "Hello from the faux model."}],
        "api": "faux", "provider": "faux", "model": "faux-1", "stopReason": "stop", "timestamp": 0,
        "usage": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0, "totalTokens": 0,
            "cost": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0, "total": 0}}
    }))?))]);

    let registry = Arc::new(Registry::default());
    registry.install(Extension {
        name: "app".into(),
        tools: vec![],
        sections: vec![],
        wraps: vec![],
        hooks: vec![],
        tasks: vec![],
    })?;

    let harness = Harness::open(
        Arc::new(SqliteStorage::open("agent.db", &Default::default())?),
        HarnessOptions {
            models: faux,
            registry,
            settings: None,
            conversation_created: None,
            now: None,
            env: None,
            on_report: None,
        },
    )?;

    let root = harness.root(
        RootOptions {
            agent: Some(AgentChange {
                model: Change::Set(ModelRef { provider: "faux".into(), model_id: "faux-1".into() }),
                ..Default::default()
            }),
            init: None,
        },
        &cancel,
    )?;

    root.submit(
        SubmissionDraft::Input {
            request_id: None,
            content: serde_json::from_value(json!("hello"))?,
            when_busy: WhenBusy::default(),
        },
        &cancel,
    )?;
    harness.wait_for_idle(&cancel)?;

    for message in root.context(None, &cancel)?.messages {
        if let Message::Assistant(reply) = message {
            for block in &reply.content {
                if let AssistantBlock::Text(text) = block {
                    println!("{}", text.text);
                }
            }
        }
    }
    harness.close(&cancel)?;
    Ok(())
}
```

Four things to know from reading it:

- A **harness** owns a storage, a model source and a registry of extensions. A **conversation** (here the root one) takes submissions and writes every step of the agent loop as an entry in the store.
- Every call takes a `CancelToken`. Pass one you never cancel if you don't need it.
- `registry.install(...)` is required, even with an empty extension. Known bug: `Registry::default()` has no built-in tasks and `Harness::open` does not check, so without an install `wait_for_idle` never returns. TypeScript's `createRegistry()` includes them and `Harness` throws if they are missing (`registry.ts:10,22`, `harness.ts:423`).
- For a real model, swap `models: faux` for `Arc::new(Providers::builtin())` and the `ModelRef` for, say, `anthropic` / `claude-sonnet-5-5`. `examples/agent.rs` is that, with a tool and `--db FILE --resume` to finish a turn a killed run left behind.

Other runnable examples: `examples/faux_agent.rs` (scripted provider plus one tool, no network).

### Features

| Feature | Adds |
| --- | --- |
| *(none)* | storage (memory, JSONL, SQLite), session, documents, tasks, env, tools, the harness, the faux provider. No HTTP or TLS code is compiled. |
| `http` | the HTTP/1.1 client (`ai::http`), SSE reader, OAuth helpers (`ai::oauth`) |
| `anthropic` | Anthropic Messages API (API keys and Claude OAuth bearer tokens) and the Anthropic catalog |
| `vercel-gateway` | the Vercel AI Gateway catalog, served through Anthropic Messages |
| `openai` | OpenAI Chat Completions and Responses, and the OpenAI catalog |
| `providers` | all of the above |

`Providers::builtin()` only exists when at least one of `anthropic`, `openai`, `vercel-gateway` is on (`src/ai/registry.rs:42`). SQLite is `rusqlite` with its bundled C source, so a C compiler is needed.

### Providers and credentials

`Providers::add_models` adds your own `Model` values (any base URL, which is how a local OpenAI-compatible server is used) and `Providers::add_api` adds your own `Api`.

Keys come from, in order: `SimpleStreamOptions::api_key`, the resolver you set with `Providers::set_auth` (called per request with the model; it returns the key or OAuth token, extra headers, a base URL), then the provider's environment variable (`ANTHROPIC_OAUTH_TOKEN`, `ANTHROPIC_API_KEY`, `AI_GATEWAY_API_KEY`, `OPENAI_API_KEY`). A token starting with `sk-ant-oat` is sent as a Claude OAuth bearer token.

`ai::oauth::anthropic` has the Claude login as plain functions: build the authorize link, wait for the callback or parse a pasted code, exchange it, refresh it. They return `Credentials { refresh, access, expires_ms }` and write nothing. Never call `refresh` on a token another program also holds: refresh rotates the refresh token. Choosing between several accounts is the resolver's job.

### Cancellation

Cancelling a `CancelToken` shuts down the socket of any in-flight provider request (`src/ai/http.rs:402`), and the stream ends with stop reason `aborted` ("Request was aborted"). `provider_e2e` asserts an abort in the middle of a stream finishes in under a second against a loopback server (it reports `cancel ok in 490µs`). The compaction summary call passes the harness signal (`src/harness/compaction.rs:349`).

If you write your own `Api`, honouring `options.signal` is your job. Nothing stops a provider from ignoring it, and then `Models::complete_simple` blocks until the provider returns.

## How fast, how big

This is the one place the numbers live. Update it here and nowhere else.

Fair bench, TypeScript `pi-durable` 1.1.0 on Node versus this crate at `97d82f3`. Scripted fake model, one machine, 5 samples, storage on tmpfs. The machine was busy while this ran, so absolute milliseconds are inflated; the ratios are the part to trust. Median, Rust / TypeScript:

| Thread length | 50 turns | 250 | 1,000 | 3,500 |
| --- | ---: | ---: | ---: | ---: |
| Warm turn, ms | 7.9 / 19 | 8.3 / 20 | 8.5 / 20 | 10 / 29 |
| Cold: open + first turn, ms | 11 / 50 | 14 / 58 | 23 / 75 | 56 / 154 |

Peak memory is 10 to 38 MB for Rust against 87 to 169 MB for TypeScript across those lengths.

Before the recent performance commits (`38e15ae`) the same bench had warm turns roughly tied with TypeScript 1.1.0 and cold starts 2 to 3 times faster. A real model call takes seconds, so none of this decides much unless you run many conversations in one process or open them often. The bench harness is not in this repo yet, so these numbers cannot be rerun from here.

## Where it differs from TypeScript pi-durable

Everything below was read from the code and docs in this repository, from the pinned TypeScript at `f1b2e77`, or from running the `nix` checks. Where a claim could not be checked, it says so.

### API shape

| Topic | TypeScript | Rust | Where |
| --- | --- | --- | --- |
| Concurrency | `async` functions, one event loop | blocking calls; the scheduler runs tasks on pooled worker threads; handlers take turns under a FIFO lock so they interleave like JS tasks do between `await`s | `src/workers.rs`, `src/turn.rs` |
| Cancellation | `Context` / `AbortSignal` | `&CancelToken` on every call; cancelling gives `Error::Aborted` ("aborted") | `src/cancel.rs`, `src/errors.rs` |
| Errors | classes `ReadAfterWrite`, `StorageRejected`, `ConversationBusy` plus plain `Error` | one `enum Error` (those three, `Aborted`, `Invalid(String)`, `Json`); it is not `Clone`, so a failure replayed to a later caller keeps its text but becomes `Invalid` | `errors.ts`, `src/errors.rs` |
| Messages | plain objects | typed enums; `Message::User / Assistant / ToolResult` hold an `Arc`, so cloning a transcript copies pointers; faux responses hold a `Box`. JSON shape is the same (camelCase) | `src/ai/types.rs:406`, `src/ai/providers/faux.rs:81` |
| Absent values | `undefined` | `Option::None`, skipped when serialised; an explicit JSON `null` in `details` / `data` stays `Some(null)` | `src/ai/types.rs` (`present`) |
| Registry | `createRegistry()` has built-ins; `Harness` throws without them | `Registry::default()` has none until an install; no check (see Try it) | `registry.ts`, `src/harness/registry.rs` |
| Extensions and tools | `defineExtension`, `defineTool`, typebox schemas | struct literals (`Extension`, `ToolRegistration`); schemas are hand-built JSON; argument validation follows typebox 1.3.27 for the subset tools use and errors on other keywords | `src/harness/define.rs`, `src/ai/utils/validation.rs` |
| Hooks | handlers get a `HookApi` (memo, models, conversation) | generation and compaction hooks get a `HookApi` with `conversation_id` only; no memo or models | `src/harness/types.rs` |
| Documents | positional `args`, checked in overloads | explicit owner and key, ids and versions are `u64`, scope checked at run time on `DocToken<T>` | `src/documents.rs` |
| Transaction | async callback; unawaited `Tx` calls are an error | methods return values directly, so that error cannot happen | `session/transaction.ts:679` |
| Storage | memory, JSONL, SQLite (Node and Cloudflare) | memory, JSONL, SQLite (`rusqlite`); no Cloudflare backend | `packages/durable/src/storage/sqlite/cloudflare.ts`, `src/storage/` |
| Names | camelCase | snake_case in Rust; stored JSON keeps the TypeScript camelCase | |
| Context reads | per-conversation cache (`settings.contextRetentionMs`, default ten minutes) | none; callers read through `capture_context_bounds` | `harness/agent.ts:52`, `src/harness/context.rs` |

The crate is 0.1.0 and its API is still moving.

### Behaviour gaps

- **Tool-call order.** Parallel tool calls run on separate threads under the turn lock, so their start and settle order matches TypeScript's in the parity scenario `f2-tool-parallel`. Storage commits do not yield the turn (`src/turn.rs`), so interleaving at commit level can differ; no check looks for it.
- **File watching** polls every 2000 ms (`WatchOptions::poll_interval`), never TypeScript's native mode, with no debounce and no `overflow` change.
- **Tracker op sequences** differ (Rust diffs base against draft; arrays diff element by element). Applying either sequence gives the same value, but tool-output events may carry a different, equivalent op.
- **Bash output windows.** `ToolExecutionApi::outputWindow` does not exist; `skipped` is always `None` and chunk boundaries differ on large output. The retained tail matches.
- **Unicode.** Strings cannot hold lone surrogates, so a cut through a surrogate pair yields U+FFFD; `t` counts stay UTF-16 units.
- **Not ported from chord:** replicated state for views (`viewState` observers run synchronously on the commit path), `copyJson`, `isJsonValue`, the revision validator.
- **Not ported** (from `docs/COVERAGE.md`): `HookApi` memo and models, a commit-subscribe callback (use `DocumentWatch`), coding services and resource loader, MCP, the codemode sandbox.
- **Process groups** use `process_group(0)` plus a two-line `extern "C"` call to `kill(2)`; temp-file names come from `RandomState` instead of a UUID. Unix only.
- **Context budget (Rust only).** `AgentChange::context_budget_tokens` stores `contextBudgetTokens` in the conversation's agent document. Compaction thresholds then use the smaller of the model's context window and the budget (`budgeted_window`, `src/harness/generation.rs`), so a 1M-window model compacts near the budget; with no budget nothing changes, and a model with an unknown window (0) uses the budget alone. It is per conversation, because `CompactionPolicy` is harness-wide.
- **Close.** `Harness::close` joins running invocations before closing the session; TypeScript seals admission first, so a task admitted during close can start in Rust and be refused in TypeScript.

### Providers

TypeScript's `packages/ai/src/providers` has about 40 provider modules and `auth/oauth` has nine logins. Rust has three wire formats (Anthropic Messages, OpenAI Chat Completions, OpenAI Responses), one catalog (every chat model in the published `@earendil-works/pi-ai`, `src/ai/catalog/models.json`; to update, change `VERSION` and `INTEGRITY` in `scripts/gen-catalog.py` and run `scripts/gen-catalog.py > src/ai/catalog/models.json`), and one login (Anthropic).

Not ported: `onPayload` / `onResponse` hooks, custom fetch, proxies, redirects, WebSocket and the Codex, Azure, Bedrock, Google, Mistral transports, Copilot headers, constrained sampling and grammar tools, Anthropic workload-identity federation and `ANTHROPIC_AUTH_TOKEN` (set it as a header from `set_auth`), OpenRouter `reasoning_details` replay, `PI_CACHE_RETENTION`. Chat Completions rejects unknown `compat` keys with an error instead of ignoring them.

The HTTP client is hand-written HTTP/1.1 over `rustls` (`src/ai/http.rs`). One connection per request, no redirects, no proxy. It exists because cancelling a streaming response has to close the socket, and `ureq` and `reqwest` do not expose it on a stable API.

### What is proven and what is not

| Path | Status |
| --- | --- |
| Anthropic Messages, OpenAI Chat Completions and Responses through the **Vercel AI Gateway** | Run live on 2026-10-10 with a tool round trip, and a `kill -9` then `--resume` finished the turn. Reported, not repeatable from this repository without a gateway key; unverified here. |
| **api.anthropic.com** direct, API key | Not proven live. |
| **Anthropic OAuth** (login and bearer tokens) | Not proven live. `oauth_flow` runs it against a loopback fake server only. |
| **api.openai.com** direct | Not proven live. |
| Retry on 429, cancel mid-stream, tool turns | Proven against loopback fake servers in `provider_e2e`, `openai_completions_e2e`, `openai_responses_e2e` (`nix flake check`). |
| Anthropic request bodies | Reported: 770 generated cases compared with TypeScript, 0 field differences. The probe is not in the repository, so this is unverified. |
| Other reported probe counts (700 reducer cases per seed, 40,000 validation cases, 7 serialisation fixtures) | Probes not in the repository; unverified. |

### What the parity check covers

`nix build .#checks.x86_64-linux.parity` passed with `total probes 48 diffs 0`. It drives the same scenarios through the TypeScript package at the pinned commit and through `examples/parity.rs`, then compares the stored entries, reports and task records after normalising timestamps, usage, response ids, and the text of an aborted partial (`parity/compare.mjs`).

- **Scenarios:** 23 in seven files (`parity/scenarios/`): plain turns (4), tool calls (5: single, parallel, unknown tool, bad arguments, two rounds), cancel (3), crash and resume (2), compaction (2), retry (3), queueing (4: steer, follow-up, reject while busy, de-duplicated request id).
- **Probes:** 21 non-crash scenarios on memory and on SQLite storage (42), plus the two crash scenarios run as a writer-then-reader matrix with three TypeScript/Rust pairings (6).
- **Steps the driver can take:** submit, wait idle, sleep, wait for a run or tool, abort, compact, crash point.
- **Model and tools:** the faux provider with two toy tools (`echo`, `sleep`).

What 48 green probes do **not** say: anything about a real provider or the HTTP code, the built-in `read` / `write` / `edit` / `bash` tools, the execution environment, JSONL storage, hooks, prompt sections, extension-defined tasks, fork / reset / configure, images or thinking blocks, event streams and watches, or timing. Some of those have older, uncommitted probes (see above), and some have none.

## Build and checks

```
nix build
nix flake check
```

`nix flake check` builds with and without features, runs `clippy` and `rustfmt`, the loopback end-to-end examples, and the parity check described above. The crate uses edition 2024, so it needs a recent Rust (`Cargo.toml` sets no `rust-version`, so the exact floor is unknown).

## Layout

| Module | TypeScript source |
| --- | --- |
| `chord` | `packages/chord/src/{delta,json.ts,services/state.ts}` (only the delta tracker, ops and state) |
| `ids`, `errors`, `entries`, `documents`, `types`, `truncate`, `tasks` | `packages/durable/src/*.ts` |
| `storage`, `session`, `env`, `tools`, `harness` | `packages/durable/src/{storage,session,env,tools,harness}` |
| `ai` | `packages/ai/src/{types,api,utils,providers,auth/oauth}` |

`docs/COVERAGE.md` lists which PiG calls a downstream agent (Boar) makes and whether this crate covers them.

## Licence

MIT, as Pi.
