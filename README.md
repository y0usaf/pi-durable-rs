# Pi-Rust-Durable

A Rust reproduction of Pi's `@earendil-works/pi-durable` 1.1.0 (durable conversations, tasks and documents over a transactional store), with no runtime or build dependency on Pi or Node. It does not ship a CLI, an LLM client or an async runtime.

Oracle: [earendil-works/pi](https://github.com/earendil-works/pi) at `f1b2e77f5b13b2a199b1052cb79c235451afe7d7` (`packages/durable` 1.1.0 and the `packages/chord` parts it consumes). Behaviour is translated from the TypeScript; [PiG](https://github.com/MichaelKinsy/PiG)'s Go port is used only as a map of where to draw the lines.

## Scope

- `chord`: only `delta/` (ops, change tracker, immutable apply, strict JSON clone), replicated state and `copyJson`. Chord's `Context` is not ported; a cancellation token stands in where TypeScript passes a `Context` or `AbortSignal`.
- `durable`: ids, errors, entries, documents, types, truncate, codecs, storage (memory, JSONL, SQLite), session, env and tools.
- Not yet: harness (needs the `pi-ai` types).

## Layout

| Module | Source |
| --- | --- |
| `chord` | `packages/chord/src/{delta,json.ts,services/state.ts}` |
| `ids`, `errors`, `entries`, `documents`, `types`, `truncate`, `codecs`, `tasks` | `packages/durable/src/*.ts` |
| `storage` | `packages/durable/src/storage` |
| `session` | `packages/durable/src/session` |
| `env`, `tools` | `packages/durable/src/{env,tools}` |

## Build

```
nix build
nix flake check
```

## Divergences

Every place behaviour differs from the TypeScript, and why.

- Synchronous API, no async runtime. TypeScript returns promises; this port blocks and uses threads where TypeScript uses concurrency.
- Replicated-state listeners run synchronously on the thread that publishes the source frame, as in PiG, where TypeScript delivers from the microtask queue.
- JavaScript `undefined` has no JSON form. Absent values are `Option::None` and are skipped when serialized, following PiG, which drops `omitUndefinedProperties`.
- `Context` and `AbortSignal` are replaced by a cancellation token.
- The tracker copies the value when a change begins and emits ops by diffing base against draft. Op sequences therefore differ from TypeScript's piece-table emission; applying either sequence to the base gives the same value. Replacing with an equal value emits no ops, as in TypeScript.
- Rust strings cannot hold lone surrogates, so `t` counts stay UTF-16 units as in TypeScript, but a cut through a surrogate pair yields U+FFFD.
- `copyJson`, `isJsonValue` and the revision validator are not ported: `serde_json::Value` cannot hold cycles, non-finite numbers, symbols or sparse arrays, and it keeps `1` and `1.0` apart where JavaScript does not.
- A `Prepared` change is consumed by `adopt` and the draft is a plain `&mut` value, so the "already used", "aborted" and settled-proxy errors cannot occur; stale and foreign adoptions still fail.
- Subscriber errors go to an error sink given at construction instead of an asynchronous rethrow. Source-listener errors return from `change` and `replace`, several together as `Error::Listeners`. Delivery is synchronous, so there is no per-subscriber queue or 100-frame cap.
- Document addresses take explicit owner and key arguments, not TypeScript's positional `args`, and owner IDs and versions are `u64`, so negative or non-integer values cannot be expressed. Scope and version checks run at runtime on `DocToken<T>`, where TypeScript checks them in overloads.
- Rust strings are well-formed UTF-8, so lone surrogates cannot occur, and `truncate_head_of` panics on an empty prefix that TypeScript rejects with a `TypeError`.
- A scan with `limit: 0` that finds a row fails with `Invalid("Storage scan limit must be at least 1")`. TypeScript's page helper crashes on the same input with an internal `TypeError` (`Cannot read properties of undefined`). With no matching row both return an empty page.
- Memory storage takes typed records, so a malformed chord op or a submission missing its required field cannot be built in Rust. TypeScript's memory storage stores such values at commit without checking them, and fails later on read or never. The Rust driver therefore cannot submit them to the Rust side.
- Windows and PowerShell are not supported. Process groups and the shell are Unix only, so `Command::Line` runs `bash` (or `sh`) and nothing else.
- File watching polls directory snapshots every 2000 ms (`WatchOptions::poll_interval`) instead of native `fs.watch`/inotify, so `FileWatcher::mode` is always `polling`, never TypeScript's `native`. Polling needs no crate and no inotify FFI, at the cost of latency. There is no 50 ms debounce; changes found in one poll are reported together. Polling never reports TypeScript's `overflow` change, which only native watching can need; Rust `WatchChange` has no such variant.
- Output callbacks return `Result<(), String>`. An `Err` stops the command and becomes `callback_error` with the message, as in TypeScript.
- Output is queued through a bounded channel of 16 chunks, so a slow spill or caller stalls the child's pipe, as TypeScript pauses its streams. Chunk sizes and pause points differ from Node's stream buffering.
- Temp names have no UUID: std has no UUID generator, so `uuid()` in `src/env/node.rs` builds a v4-shaped string from `RandomState` hashes of time, a counter, and the pid. It is unique in practice and not cryptographic.
- Process groups use `CommandExt::process_group(0)` and a two-line `extern "C"` to `kill(2)`, instead of a libc crate.
- `openNodeJsonlStorage` reads `process.cwd()`. `jsonl::open_node` reads `std::env::current_dir()` at open and fails with `JSONL current directory failed` if that fails, instead of taking a cwd argument. This follows Go's `os.Getwd` at open time; PiG has no JSONL port, so this is the nearest choice rather than a copied one.
- Transaction methods return values directly, so TypeScript's rule that a commit callback must not settle with unawaited `Tx` operations (`Session commit callback settled before its pending Tx operations`) has no case to fire and is not ported.
- `Tx::create_task` takes the initial checkpoint already computed (`TaskCreate::checkpoint`). TypeScript calls `definition.initial(input)` inside the write, so an exception from `initial` surfaces there instead of before the call.
- A failed draft acquisition is replayed to later `doc` and `retire_doc` calls on the same address with the same message, as in TypeScript, but as `Error::Invalid(message)`, since `Error` is not `Clone`. The original variant is lost; the text is identical.
- Session watches and document state subscribers receive frames from a queue shared by the session. Frames are delivered after the committing call leaves the session line, by one drainer at a time, so commit listeners run before the frames of the same commit. TypeScript delivers these from microtasks, so the order matches and the timing differs. The per-subscriber queue belongs to the session path only; chord subscribers still deliver synchronously.
- An unstarted watch keeps at most 100 pending frames. On overflow the queue is replaced by one `Replace` op carrying the newest value, as in TypeScript's `MAX_PENDING_WATCH_FRAMES`.
- pi-ai `Message` is typed (`ai::types::Message`) and round-trips to the TypeScript JSON shape. Role and `type` tags are fixed-value unit types, so a wrong tag fails to deserialize. Untagged enums pick the first variant that fits, so a block is matched by field shape, not by tag.
- pi-ai `details` and `data` keep an explicit JSON `null` as `Some(null)`, so they round-trip; a missing key is `None`.
- pi-ai `uuidv7` takes its random seed from `/dev/urandom` (Unix only) and uses `u64` where TypeScript uses `bigint`; the bit layout and monotonic sequence follow TypeScript.
- pi-ai `validateToolArguments` (`src/ai/utils/validation.rs`) follows typebox 1.3.27 for the JSON Schema subset the tools use. It rejects other keywords with an error instead of ignoring them, and `$ref`, `patternProperties`, `format` and `const` are unsupported. Coercion and the error text match TypeScript. Typebox's compiled `Check` takes a fast path for `additionalProperties: false` when `required` names every property, so an unrelated key can pass `Check` while `Errors` still reports it; the port reproduces that. Boolean subschemas never count as validators in the coercion and null-stripping steps, as in TypeScript. Probes: 40,000 random cases, 0 mismatches.
- pi-ai faux provider (`src/ai/providers/faux.rs`) runs each stream on a spawned thread, and `tokensPerSecond` delays use `thread::sleep` in whole milliseconds. Chunk sizes come from `RandomState`, not `Math.random`, and ids are `prefix:ms:hex`. Chunks are reproducible only when `tokenSize` has equal min and max.
- pi-ai faux event snapshots are fixed when pushed. TypeScript shares content blocks between `partial` objects, so an event read after later deltas shows the later text; Rust does not.
- pi-ai faux drops the `onResponse` hooks, the `fauxText`/`fauxAssistantMessage`/`fauxToolCall` builders and `pendingResponseCount`. `fetch_deferred` returns only the final message; `Faux::fetch_stream` returns the event stream.
- pi-ai regex patterns (retry, overflow) are compiled with the `regex` crate, not JavaScript RegExp: `.` excludes the same line terminators, patterns match on ASCII-lowercased text, and case folding is ASCII only.
- Session-created state sources and watches report chord errors to stderr through a sink. TypeScript reports them with `reportErrorAsync`.
- A cancelled session operation fails with the fixed text `The operation was aborted` (watch) or `This operation was aborted` (commit). TypeScript uses the signal's reason when that reason is an `Error`.
- `create_session(storage, now)` takes `Option<Clock>` in place of TypeScript's `{ now }` options object. An absent clock reads the system time in milliseconds.

- The tools port stops before `defineTool`, `defineExtension` and `CodingTools`: `src/tools` gives each tool a plain function over the `ExecutionEnv` and `CancelToken`, returning `ToolOutput` or `ToolError` (text, diagnostics, details). Milestone 6 wraps these in the harness registration. Schemas are JSON values built by hand, not typebox.
- npm `diff` is not a dependency: `src/tools/jsdiff.rs` reproduces only `diffLines` and the `createTwoFilesPatch` hunk output that edit-diff.ts uses (Myers diff over lines, four lines of context). It has no change objects for word or character diffs, and it does not reproduce jsdiff's `oneChangePerToken` or option handling. NFKC normalization for fuzzy edit matching takes the `unicode-normalization` crate, since its tables would otherwise be rediscovered.
