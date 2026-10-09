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

