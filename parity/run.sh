#!/usr/bin/env bash
set -eu
here="$(cd "$(dirname "$0")" && pwd)"
out="$(mktemp -d)"
trap 'rm -rf "$out"' EXIT
drive() {
	local impl="$1"
	shift
	if [ "$impl" = ts ]; then
		node --import "$PARITY_TS/register.mjs" "$PARITY_TS/driver.ts" "$@"
	else
		"$PARITY_RS" "$@"
	fi
}
for storage in memory sqlite; do
	mkdir -p "$out/db/$storage"
	for file in "$here"/scenarios/*.json; do
		for impl in ts rs; do
			drive "$impl" "$file" "$out/$storage/$impl" "$storage" "$out/db/$storage" >/dev/null
		done
	done
done
crash_cases=$(node -e 'for (const f of process.argv.slice(1)) for (const c of JSON.parse(require("fs").readFileSync(f, "utf8"))) if (c.crash) console.log(c.name + "\t" + f)' "$here"/scenarios/*.json)
for writer in ts rs; do
	for reader in ts rs; do
		cell="$writer-$reader"
		db="$out/cross-db/$cell"
		mkdir -p "$db" "$out/cross/$cell"
		while IFS=$'\t' read -r name file; do
			log="$db/$name.writer.log"
			drive "$writer" "$file" "$db" sqlite "$db" "$name" run >"$log" 2>&1 &
			pid=$!
			for _ in $(seq 1 600); do grep -q READY "$log" && break; sleep 0.1; done
			grep -q READY "$log" || { echo "writer $cell $name never reached its crash point" >&2; kill -9 "$pid"; exit 1; }
			kill -9 "$pid"
			wait "$pid" 2>/dev/null || true
			drive "$reader" "$file" "$out/cross/$cell" sqlite "$db" "$name" resume >/dev/null
		done <<<"$crash_cases"
	done
done
node "$here/compare.mjs" "$out/memory/ts=$out/memory/rs" "$out/sqlite/ts=$out/sqlite/rs" "$out/cross/ts-ts=$out/cross/ts-rs" "$out/cross/ts-ts=$out/cross/rs-ts" "$out/cross/ts-ts=$out/cross/rs-rs"
