#!/usr/bin/env bash
set -eu
here="$(cd "$(dirname "$0")" && pwd)"
out="$(mktemp -d)"
server=""
cleanup() {
	[ -n "$server" ] && kill "$server" 2>/dev/null || true
	rm -rf "$out"
}
trap cleanup EXIT
ts() { node --import "$PARITY_AI_TS/parity-ai/register.mjs" "$@"; }
ts "$here/gen.mjs" "$PARITY_AI_TS/packages/ai/src" "$out/scenarios"
if ! diff -r "$here/scenarios" "$out/scenarios" >"$out/gen.diff"; then
	head -n 40 "$out/gen.diff" >&2
	echo "parity-ai: gen.mjs output differs from parity-ai/scenarios; regenerate and review the diff" >&2
	exit 1
fi
node "$here/server.mjs" "$here/scenarios" "$out" ts rs &
server=$!
for _ in $(seq 1 300); do [ -s "$out/ports.json" ] && break; sleep 0.1; done
[ -s "$out/ports.json" ] || { echo "parity-ai: server never wrote ports.json" >&2; exit 1; }
ts "$PARITY_AI_TS/parity-ai/driver-ai.ts" e1 "$here/scenarios" "$out/ports.json" >"$out/ts.jsonl" &
tspid=$!
"$PARITY_AI_RS" e1 "$here/scenarios" "$out/ports.json" >"$out/rs.jsonl" &
rspid=$!
wait "$tspid"
wait "$rspid"
kill "$server"
wait "$server" 2>/dev/null || true
server=""
ts "$PARITY_AI_TS/parity-ai/driver-ai.ts" e2 "$here/fns.json" >"$out/ts-fns.jsonl"
"$PARITY_AI_RS" e2 "$here/fns.json" >"$out/rs-fns.jsonl"
node "$here/compare-ai.mjs" "$here" "$out"
