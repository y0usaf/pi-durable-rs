import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";

const [here, out] = process.argv.slice(2);
if (!here || !out) throw new Error("usage: compare-ai.mjs <parity-ai-dir> <run-dir>");
const json = (file) => JSON.parse(readFileSync(file, "utf8"));
const lines = (file) =>
	readFileSync(file, "utf8")
		.split("\n")
		.filter(Boolean)
		.map((line) => JSON.parse(line));
const rules = json(join(here, "normalize.json"));
const known = json(join(here, "known.json"));
const ABSENT = "<absent>";

function canon(value) {
	if (Array.isArray(value)) return value.map(canon);
	if (value === null || typeof value !== "object") return value;
	return Object.fromEntries(
		Object.keys(value)
			.sort()
			.map((key) => [key, canon(value[key])]),
	);
}

function apply(node, segments, rule) {
	if (node === null || typeof node !== "object") return;
	const [head, ...rest] = segments;
	const many = head.endsWith("[]");
	const key = many ? head.slice(0, -2) : head;
	if (!(key in node)) return;
	if (many) {
		if (Array.isArray(node[key])) for (const item of node[key]) apply(item, rest, rule);
		return;
	}
	if (rest.length > 0) return apply(node[key], rest, rule);
	if (rule.action === "drop") delete node[key];
	else if (rule.action === "blank") node[key] = "<norm>";
	else if (rule.action === "replace" && typeof node[key] === "string") node[key] = node[key].replace(new RegExp(rule.pattern), rule.with);
}

function normalize(record, scope) {
	const copy = structuredClone(record);
	let result = copy;
	for (const rule of rules) {
		if (rule.scope !== scope) continue;
		if (rule.action === "sort-keys") result = canon(result);
		else apply(result, rule.path.split("."), rule);
	}
	return result;
}

function equal(a, b) {
	return JSON.stringify(a) === JSON.stringify(b);
}

function diff(a, b, sig, loc, found) {
	if (equal(a, b)) return;
	if (Array.isArray(a) && Array.isArray(b)) {
		if (a.length !== b.length) found.push({ sig: sig + ".length", loc: loc + ".length", ts: a.length, rs: b.length });
		for (let i = 0; i < Math.min(a.length, b.length); i++) diff(a[i], b[i], sig + "[]", loc + "[" + i + "]", found);
		return;
	}
	const object = (v) => v !== null && typeof v === "object" && !Array.isArray(v);
	if (object(a) && object(b)) {
		for (const key of [...new Set([...Object.keys(a), ...Object.keys(b)])].sort()) {
			const at = (sig ? sig + "." : "") + key;
			const where = (loc ? loc + "." : "") + key;
			if (!(key in a)) found.push({ sig: at, loc: where, ts: ABSENT, rs: b[key] });
			else if (!(key in b)) found.push({ sig: at, loc: where, ts: a[key], rs: ABSENT });
			else diff(a[key], b[key], at, where, found);
		}
		return;
	}
	found.push({ sig, loc, ts: a, rs: b });
}

function parseBody(body) {
	try {
		return JSON.parse(body);
	} catch {
		return body;
	}
}

function project(message) {
	if (!message) return null;
	const content = (message.content ?? []).map((b) =>
		b.type === "text" ? { type: "text", text: b.text } : b.type === "thinking" ? { type: "thinking", thinking: b.thinking } : { type: b.type, id: b.id, name: b.name, arguments: b.arguments },
	);
	const u = message.usage ?? {};
	return { stopReason: message.stopReason, content, usage: { input: u.input, output: u.output, cacheRead: u.cacheRead, cacheWrite: u.cacheWrite }, responseId: message.responseId };
}

function expectDiff(expect, message) {
	const projected = project(message) ?? {};
	const found = [];
	for (const key of Object.keys(expect).sort()) diff(expect[key], projected[key] ?? ABSENT, key, key, found);
	return found;
}

function layered(ts, rs) {
	const found = [];
	if (ts.requests.length !== rs.requests.length) {
		const why = (side) => side.message?.errorMessage ?? side.error ?? null;
		found.push({ sig: "requests.length", loc: "requests.length", ts: [ts.requests.length, why(ts)], rs: [rs.requests.length, why(rs)] });
		return found;
	}
	diff(ts.requests, rs.requests, "requests", "requests", found);
	const head = ["stopReason", "errorMessage"].find((key) => !equal(ts.message?.[key], rs.message?.[key]));
	if (head) {
		found.push({ sig: "message." + head, loc: "message." + head, ts: ts.message?.[head] ?? ABSENT, rs: rs.message?.[head] ?? ABSENT });
		return found;
	}
	diff(ts.events, rs.events, "events", "events", found);
	diff(ts.message, rs.message, "message", "message", found);
	diff(ts.error, rs.error, "error", "error", found);
	return found;
}

const scenarios = new Map();
for (const file of readdirSync(join(here, "scenarios")).filter((f) => f.endsWith(".json")).sort()) {
	const s = json(join(here, "scenarios", file));
	scenarios.set(s.name, s);
}
const outputs = { ts: new Map(), rs: new Map() };
for (const side of ["ts", "rs"]) for (const line of lines(join(out, side + ".jsonl"))) outputs[side].set(line.name, line);
const requests = { ts: new Map(), rs: new Map() };
for (const r of lines(join(out, "requests.jsonl"))) {
	const list = requests[r.side].get(r.name) ?? [];
	list.push({ method: r.method, url: r.url, headers: r.headers, body: parseBody(r.body) });
	requests[r.side].set(r.name, list);
}

const diffs = [];
const failures = [];
const perApi = new Map();
const tally = (api) => {
	if (!perApi.has(api)) perApi.set(api, { cases: 0, pass: 0, known: 0, unexpected: 0, rsExpect: 0 });
	return perApi.get(api);
};
const covering = (scopes, sig) =>
	Object.keys(known).filter((key) => {
		const [head, ...rest] = key.split(":");
		const path = rest.join(":");
		return scopes.includes(head) && (path === "" || sig === path || sig.startsWith(path + ".") || sig.startsWith(path + "["));
	});
const hits = new Map(Object.keys(known).map((key) => [key, 0]));
const unexpected = new Map();

function record(scope, name, found, t) {
	let open = 0;
	for (const d of found) {
		const keys = covering([scope, name], d.sig);
		for (const key of keys) hits.set(key, hits.get(key) + 1);
		if (keys.length > 0) continue;
		open++;
		const id = scope + ":" + d.sig;
		const list = unexpected.get(id) ?? [];
		list.push({ name, ...d });
		unexpected.set(id, list);
	}
	if (found.length === 0) t.pass++;
	else if (open === 0) t.known++;
	else t.unexpected++;
}

for (const [name, scenario] of scenarios) {
	const t = tally(scenario.api);
	t.cases++;
	const sides = {};
	for (const side of ["ts", "rs"]) {
		const line = outputs[side].get(name);
		if (!line) {
			failures.push("MISSING " + side + " output for " + name);
			continue;
		}
		sides[side] = normalize({ requests: requests[side].get(name) ?? [], events: line.events, message: line.message, error: line.error }, "e1");
	}
	if (!sides.ts || !sides.rs) continue;
	const tsExpect = expectDiff(scenario.expect, outputs.ts.get(name).message);
	for (const d of tsExpect) failures.push("EXPECT ts " + name + " " + d.loc + ": expect=" + JSON.stringify(d.ts) + " ts=" + JSON.stringify(d.rs));
	if (expectDiff(scenario.expect, outputs.rs.get(name).message).length > 0) t.rsExpect++;
	record(scenario.api, name, layered(sides.ts, sides.rs), t);
}

const rows = json(join(here, "fns.json"));
const fnOut = { ts: new Map(), rs: new Map() };
for (const side of ["ts", "rs"]) for (const line of lines(join(out, side + "-fns.jsonl"))) fnOut[side].set(line.fn + "#" + line.case, line.out);
const fnTally = new Map();
for (const row of rows) {
	const t = { cases: 0, pass: 0, known: 0, unexpected: 0 };
	fnTally.set(row.fn, t);
	row.cases.forEach((args, index) => {
		t.cases++;
		const id = row.fn + "#" + index;
		if (!fnOut.ts.has(id) || !fnOut.rs.has(id)) {
			failures.push("MISSING fn output for " + id);
			return;
		}
		const found = [];
		diff(normalize(fnOut.ts.get(id), "e2"), normalize(fnOut.rs.get(id), "e2"), "", "", found);
		record("fn", row.fn + "#" + index, found.map((d) => ({ ...d, sig: row.fn + (d.sig ? "." + d.sig : "") })), t);
	});
}

const stale = [...hits].filter(([, n]) => n === 0).map(([key]) => key);
const show = (v) => {
	const s = JSON.stringify(v);
	return s === undefined ? "undefined" : s.length > 160 ? s.slice(0, 160) + "..." : s;
};
for (const [id, list] of [...unexpected].sort()) {
	const names = [...new Set(list.map((d) => d.name))];
	console.log("UNEXPECTED " + id + " in " + names.length + " case(s): " + names.slice(0, 4).join(" ") + (names.length > 4 ? " ..." : ""));
	for (const d of list.slice(0, 2)) console.log("  " + d.name + " " + d.loc + ": ts=" + show(d.ts) + " rs=" + show(d.rs));
}
for (const key of stale) console.log("NOW MATCHES " + key + ": " + known[key]);
for (const line of failures) console.log(line);
let total = 0;
for (const [api, t] of [...perApi].sort()) {
	total += t.cases;
	console.log("api " + api + ": cases " + t.cases + " pass " + t.pass + " known " + t.known + " unexpected " + t.unexpected + " rs-vs-expect " + t.rsExpect);
}
const fnSum = { cases: 0, pass: 0, known: 0, unexpected: 0 };
for (const [fn, t] of fnTally) {
	for (const k of Object.keys(fnSum)) fnSum[k] += t[k];
	console.log("fn " + fn + ": cases " + t.cases + " pass " + t.pass + " known " + t.known + " unexpected " + t.unexpected);
}
console.log("fns: rows " + fnTally.size + " cases " + fnSum.cases + " pass " + fnSum.pass + " known " + fnSum.known + " unexpected " + fnSum.unexpected);
if (total === 0 || fnSum.cases === 0) failures.push("no cases compared");
const failed = unexpected.size + stale.length + failures.length;
console.log("parity-ai: " + (failed === 0 ? "ok" : "FAIL") + " (" + unexpected.size + " unexpected, " + stale.length + " now matching, " + failures.length + " other)");
process.exitCode = failed === 0 ? 0 : 1;
