import { mkdirSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

const [srcDir, outDir, countArg] = process.argv.slice(2);
if (!srcDir || !outDir) throw new Error("usage: gen.mjs <pi-ai-src-dir> <out-dir> [count-per-api]");
const COUNT = Number(countArg ?? 24);
const schemas = await import(pathToFileURL(join(srcDir, "providers/compat-schema.ts")).href);

const MASK = (1n << 64n) - 1n;
function fnv1a64(text) {
	let hash = 0xcbf29ce484222325n;
	for (const byte of Buffer.from(text, "utf8")) hash = ((hash ^ BigInt(byte)) * 0x100000001b3n) & MASK;
	return hash;
}
function splitmix64(seed) {
	let state = seed;
	return () => {
		state = (state + 0x9e3779b97f4a7c15n) & MASK;
		let z = state;
		z = ((z ^ (z >> 30n)) * 0xbf58476d1ce4e5b9n) & MASK;
		z = ((z ^ (z >> 27n)) * 0x94d049bb133111ebn) & MASK;
		return z ^ (z >> 31n);
	};
}
function rng(seed) {
	const next = splitmix64(seed);
	const r = {
		int: (n) => Number(next() % BigInt(n)),
		range: (lo, hi) => lo + r.int(hi - lo + 1),
		chance: (p) => r.int(1000000) < p * 1000000,
		pick: (list) => list[r.int(list.length)],
		hex: (n) => Array.from({ length: n }, () => "0123456789abcdef"[r.int(16)]).join(""),
		maybe: (p, make) => (r.chance(p) ? make() : undefined),
		sample: (list, n) => {
			const pool = [...list];
			return Array.from({ length: Math.min(n, pool.length) }, () => pool.splice(r.int(pool.length), 1)[0]);
		},
	};
	return r;
}

const WORDS = {
	ascii: ["alpha", "river", "stone", "cloud", "seven", "quiet", "amber", "north", "lamp", "field"],
	bmp: ["héllo", "naïve", "Grüße", "漢字", "Ωmega", "ñandú", "Привет", "café"],
	astral: ["😀", "𝄞", "🦀", "𐐷", "👩‍💻", "🇯🇵"],
};
const UNICODE = ["ascii", "ascii", "bmp", "astral", "split-surrogate"];
const PNG = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwAEhQGAhKmMIQAAAABJRU5ErkJggg==";
const TOOLS = [
	{ name: "lookup", description: "Look up a term", parameters: { type: "object", properties: { q: { type: "string" } }, required: ["q"] } },
	{ name: "count_items", description: "Count items", parameters: { type: "object", properties: { n: { type: "integer", minimum: 0 }, flag: { type: "boolean" } } } },
	{ name: "tag_list", description: "Tag a list", parameters: { type: "object", properties: { items: { type: "array", items: { type: "string" } } }, additionalProperties: false } },
	{ name: "noop", description: "Does nothing", parameters: { type: "object" } },
	{ name: "nested_shape", description: "Nested object", parameters: { type: "object", properties: { box: { type: "object", properties: { w: { type: "number" }, h: { type: "number" } }, required: ["w"] } } } },
];
const MODELS = {
	"anthropic-messages": { providers: ["anthropic"], ids: ["claude-sonnet-4-5", "claude-opus-4-6", "claude-haiku-4-5", "claude-3-5-haiku-20241022"], path: "" },
	"openai-completions": { providers: ["openai", "openrouter", "xai", "groq", "deepseek", "cerebras", "zai"], ids: ["gpt-4o-mini", "gpt-5-mini", "o3-mini", "deepseek-chat", "grok-4", "llama-3.3-70b"], path: "/v1" },
	"openai-responses": { providers: ["openai", "openrouter", "xai"], ids: ["gpt-5", "gpt-5-mini", "o4-mini", "gpt-4.1", "grok-4"], path: "/v1" },
	"openai-codex-responses": { providers: ["openai-codex"], ids: ["gpt-5.1-codex", "gpt-5-codex", "gpt-5.2", "gpt-5.1-codex-mini"], path: "" },
	"azure-openai-responses": { providers: ["azure"], ids: ["gpt-5", "gpt-5-mini", "gpt-4.1", "o4-mini"], path: "/openai/v1" },
	"mistral-conversations": { providers: ["mistral"], ids: ["mistral-large-latest", "magistral-medium-latest", "devstral-medium-latest", "codestral-latest"], path: "" },
	"pi-messages": { providers: ["radius"], ids: ["claude-sonnet-4-5", "gpt-5", "gemini-2.5-pro"], path: "/v1" },
};
const COMPAT = {
	"anthropic-messages": schemas.AnthropicMessagesCompatSchema,
	"openai-completions": schemas.OpenAICompletionsCompatSchema,
	"openai-responses": schemas.OpenAIResponsesCompatSchema,
	"openai-codex-responses": schemas.OpenAIResponsesCompatSchema,
	"azure-openai-responses": schemas.OpenAIResponsesCompatSchema,
	"mistral-conversations": schemas.MistralConversationsCompatSchema,
};
const RESPONSES = new Set(["openai-responses", "openai-codex-responses", "azure-openai-responses"]);
const FOREIGN = [
	{ api: "openai-responses", provider: "openai", model: "gpt-5" },
	{ api: "google-generative-ai", provider: "google", model: "gemini-2.5-pro" },
	{ api: "anthropic-messages", provider: "anthropic", model: "claude-sonnet-4-5" },
];

function domain(schema) {
	if (schema.type === "boolean") return [true, false];
	if (schema.const !== undefined) return [schema.const];
	if (Array.isArray(schema.enum)) return schema.enum;
	if (Array.isArray(schema.anyOf)) {
		const parts = schema.anyOf.map(domain);
		return parts.every(Boolean) ? parts.flat() : undefined;
	}
	return undefined;
}
function compat(r, api) {
	const schema = COMPAT[api];
	if (!schema) return undefined;
	const keys = Object.keys(schema.properties)
		.sort()
		.filter((key) => domain(schema.properties[key]));
	const out = {};
	for (const key of r.sample(keys, r.pick([0, 0, 0, 1, 1, 2, 3]))) out[key] = r.pick(domain(schema.properties[key]));
	return Object.keys(out).length > 0 ? Object.fromEntries(Object.entries(out).sort()) : undefined;
}

function text(r, kind, words) {
	const pool = kind === "split-surrogate" ? [...WORDS.ascii, ...WORDS.astral] : [...WORDS.ascii, ...(WORDS[kind] ?? [])];
	return Array.from({ length: words }, () => r.pick(pool)).join(" ");
}
function pieces(r, value, kind) {
	const units = kind === "split-surrogate" ? [...value.split("")] : Array.from(value);
	const n = Math.min(units.length, r.range(1, 4));
	const cuts = new Set();
	while (cuts.size < n - 1) cuts.add(r.range(1, units.length - 1));
	const bounds = [0, ...[...cuts].sort((a, b) => a - b), units.length];
	return bounds.slice(1).map((end, i) => units.slice(bounds[i], end).join(""));
}
function args(r, tool, kind) {
	const s = () => text(r, kind === "split-surrogate" ? "astral" : kind, r.range(1, 3));
	switch (tool.name) {
		case "lookup":
			return { q: s() };
		case "count_items":
			return { n: r.range(0, 99), flag: r.chance(0.5) };
		case "tag_list":
			return { items: Array.from({ length: r.range(0, 3) }, s) };
		case "nested_shape":
			return { box: { w: r.range(1, 9), h: r.range(1, 9) / 2 } };
		default:
			return {};
	}
}

const usage = (r) => {
	const u = { input: r.range(1, 900), output: r.range(1, 300), cacheRead: r.chance(0.4) ? r.range(1, 500) : 0, cacheWrite: r.chance(0.3) ? r.range(1, 200) : 0 };
	return { ...u, totalTokens: u.input + u.output + u.cacheRead + u.cacheWrite, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } };
};

function assistantHistory(r, model, tools, at) {
	const origin = r.chance(0.6) ? { api: model.api, provider: model.provider, model: model.id } : r.pick(FOREIGN);
	const kind = r.pick(UNICODE.slice(0, 4));
	const content = [];
	if (r.chance(0.35)) {
		const block = r.chance(0.15)
			? { type: "thinking", thinking: "[Reasoning redacted]", thinkingSignature: r.hex(24), redacted: true }
			: { type: "thinking", thinking: text(r, kind, r.range(2, 6)), ...(r.chance(0.7) ? { thinkingSignature: r.chance(0.2) ? "" : r.hex(32) } : {}) };
		if (RESPONSES.has(origin.api) && block.thinkingSignature) {
			const summary = block.redacted ? [] : [{ type: "summary_text", text: block.thinking }];
			block.thinkingSignature = JSON.stringify({ type: "reasoning", id: "rs_" + block.thinkingSignature.slice(0, 16), summary, encrypted_content: block.thinkingSignature });
		}
		content.push(block);
	}
	if (r.chance(0.8)) content.push({ type: "text", text: text(r, kind, r.range(1, 8)) });
	const calls = tools.length > 0 && r.chance(0.5) ? r.sample(tools, r.range(1, 2)) : [];
	for (const tool of calls) content.push({ type: "toolCall", id: "call_" + r.hex(10), name: tool.name, arguments: args(r, tool, kind) });
	if (content.length === 0) content.push({ type: "text", text: text(r, kind, 2) });
	return {
		role: "assistant",
		content,
		...origin,
		usage: usage(r),
		stopReason: calls.length > 0 ? "toolUse" : "stop",
		timestamp: at,
	};
}

function context(r, model) {
	const tools = r.chance(0.65) ? r.sample(TOOLS, r.range(1, 3)) : [];
	const messages = [];
	let at = 1700000000000;
	const user = () => {
		const kind = r.pick(UNICODE.slice(0, 4));
		at += 1000;
		if (r.chance(0.6)) return { role: "user", content: text(r, kind, r.range(1, 10)), timestamp: at };
		const content = [{ type: "text", text: text(r, kind, r.range(1, 6)) }];
		if (r.chance(0.4)) content.push({ type: "image", data: PNG, mimeType: "image/png" });
		return { role: "user", content, timestamp: at };
	};
	messages.push(user());
	const turns = r.range(0, 3);
	for (let t = 0; t < turns; t++) {
		at += 1000;
		const assistant = assistantHistory(r, model, tools, at);
		messages.push(assistant);
		const calls = assistant.content.filter((b) => b.type === "toolCall");
		for (const call of calls) {
			at += 1000;
			const content = [{ type: "text", text: text(r, r.pick(UNICODE.slice(0, 4)), r.range(1, 6)) }];
			if (r.chance(0.2)) content.push({ type: "image", data: PNG, mimeType: "image/png" });
			messages.push({ role: "toolResult", toolCallId: call.id, toolName: call.name, content, isError: r.chance(0.15), timestamp: at });
		}
		if (calls.length === 0 || r.chance(0.5)) messages.push(user());
	}
	return {
		...(r.chance(0.7) ? { systemPrompt: text(r, r.pick(UNICODE.slice(0, 3)), r.range(2, 12)) } : {}),
		messages,
		...(tools.length > 0 ? { tools } : {}),
	};
}

function options(r, api) {
	const oauth = api === "anthropic-messages" && r.chance(0.1);
	const o = {
		apiKey: oauth ? "sk-ant-oat01-" + r.hex(24) : "test-key-" + r.hex(12),
		reasoning: r.maybe(0.5, () => r.pick(["minimal", "low", "medium", "high", "xhigh"])),
		cacheRetention: r.maybe(0.5, () => r.pick(["none", "short", "long"])),
		maxTokens: r.maybe(0.4, () => r.pick([256, 1024, 4096, 32000])),
		temperature: r.maybe(0.3, () => r.range(0, 10) / 10),
		toolChoice: r.maybe(0.2, () => r.pick(["auto", "none"])),
		sessionId: r.maybe(0.4, () => "session-" + r.hex(8)),
		headers: r.maybe(0.3, () => ({ "x-parity-test": r.hex(6) })),
		maxRetries: 0,
		timeoutMs: 10000,
	};
	if (api === "openai-codex-responses") {
		const claims = { "https://api.openai.com/auth": { chatgpt_account_id: "acct_" + o.apiKey.slice(-12) } };
		o.apiKey = "eyJhbGciOiJub25lIn0." + Buffer.from(JSON.stringify(claims)).toString("base64url") + ".sig";
		o.transport = "sse";
	}
	return Object.fromEntries(Object.entries(o).filter(([, v]) => v !== undefined));
}

function canonical(r, tools) {
	const kind = r.pick(UNICODE);
	const blocks = [];
	if (r.chance(0.35)) blocks.push({ type: "thinking", thinking: text(r, kind, r.range(1, 6)), signature: r.hex(40) });
	if (r.chance(0.8)) blocks.push({ type: "text", text: text(r, kind, r.range(1, 10)) });
	const pool = tools.length > 0 ? tools : [TOOLS[0]];
	const calls = r.chance(0.35) ? r.range(1, 2) : 0;
	for (let i = 0; i < calls; i++) {
		const tool = r.pick(pool);
		blocks.push({ type: "toolCall", id: "call_" + r.hex(12), name: tool.name, arguments: args(r, tool, kind) });
	}
	for (const block of blocks) {
		if (block.type === "text") block.pieces = pieces(r, block.text, kind);
		if (block.type === "thinking") block.pieces = pieces(r, block.thinking, kind);
		if (block.type === "toolCall") block.pieces = pieces(r, JSON.stringify(block.arguments), "ascii");
	}
	const stop = calls > 0 ? "toolUse" : r.chance(0.15) ? "length" : "stop";
	const u = usage(r);
	return { id: r.hex(16), blocks, stop, usage: { input: u.input, output: u.output, cacheRead: u.cacheRead, cacheWrite: u.cacheWrite }, tail: r.pick(["ok", "ok", "ok", "ok", "ok", "ok", "ok", "ok", "truncated", "error"]) };
}

const sse = (event, data) => (event ? "event: " + event + "\n" : "") + "data: " + JSON.stringify(data) + "\n\n";
const ANTHROPIC_STOP = { stop: "end_turn", length: "max_tokens", toolUse: "tool_use" };
const OPENAI_STOP = { stop: "stop", length: "length", toolUse: "tool_calls" };

const ENCODE = {
	"anthropic-messages": (c, model) => {
		const out = [
			sse("message_start", {
				type: "message_start",
				message: {
					id: "msg_" + c.id,
					type: "message",
					role: "assistant",
					model: model.id,
					content: [],
					stop_reason: null,
					stop_sequence: null,
					usage: { input_tokens: c.usage.input, output_tokens: 1, cache_read_input_tokens: c.usage.cacheRead, cache_creation_input_tokens: c.usage.cacheWrite },
				},
			}),
		];
		c.blocks.forEach((block, index) => {
			const start = {
				text: { type: "text", text: "" },
				thinking: { type: "thinking", thinking: "", signature: "" },
				toolCall: { type: "tool_use", id: block.id, name: block.name, input: {} },
			}[block.type];
			out.push(sse("content_block_start", { type: "content_block_start", index, content_block: start }));
			for (const piece of block.pieces) {
				const delta = {
					text: { type: "text_delta", text: piece },
					thinking: { type: "thinking_delta", thinking: piece },
					toolCall: { type: "input_json_delta", partial_json: piece },
				}[block.type];
				out.push(sse("content_block_delta", { type: "content_block_delta", index, delta }));
			}
			if (block.type === "thinking") out.push(sse("content_block_delta", { type: "content_block_delta", index, delta: { type: "signature_delta", signature: block.signature } }));
			out.push(sse("content_block_stop", { type: "content_block_stop", index }));
		});
		if (c.tail === "error") {
			out.push(sse("error", { type: "error", error: { type: "overloaded_error", message: "Overloaded" } }));
			return out;
		}
		out.push(sse("message_delta", { type: "message_delta", delta: { stop_reason: ANTHROPIC_STOP[c.stop], stop_sequence: null }, usage: { output_tokens: c.usage.output } }));
		if (c.tail !== "truncated") out.push(sse("message_stop", { type: "message_stop" }));
		return out;
	},
	"openai-completions": (c, model) => {
		const id = "chatcmpl-" + c.id;
		const chunk = (delta, finish = null) => sse(null, { id, object: "chat.completion.chunk", created: 0, model: model.id, choices: [{ index: 0, delta, finish_reason: finish }] });
		const out = [chunk({ role: "assistant", content: "" })];
		let toolIndex = 0;
		const field = c.id.charCodeAt(0) % 2 === 0 ? "reasoning_content" : "reasoning";
		for (const block of c.blocks) {
			if (block.type === "thinking") for (const piece of block.pieces) out.push(chunk({ [field]: piece }));
			if (block.type === "text") for (const piece of block.pieces) out.push(chunk({ content: piece }));
			if (block.type === "toolCall") {
				const index = toolIndex++;
				out.push(chunk({ tool_calls: [{ index, id: block.id, type: "function", function: { name: block.name, arguments: "" } }] }));
				for (const piece of block.pieces) out.push(chunk({ tool_calls: [{ index, function: { arguments: piece } }] }));
			}
		}
		if (c.tail === "error") {
			out.push(sse(null, { error: { message: "Upstream overloaded", type: "server_error", code: null } }));
			return out;
		}
		if (c.tail !== "truncated") out.push(chunk({}, OPENAI_STOP[c.stop]));
		out.push(
			sse(null, {
				id,
				object: "chat.completion.chunk",
				created: 0,
				model: model.id,
				choices: [],
				usage: {
					prompt_tokens: c.usage.input + c.usage.cacheRead + c.usage.cacheWrite,
					completion_tokens: c.usage.output,
					total_tokens: c.usage.input + c.usage.cacheRead + c.usage.cacheWrite + c.usage.output,
					prompt_tokens_details: { cached_tokens: c.usage.cacheRead, cache_write_tokens: c.usage.cacheWrite },
				},
			}),
		);
		out.push("data: [DONE]\n\n");
		return out;
	},
};

const responsesUsage = (c) => ({
	input_tokens: c.usage.input + c.usage.cacheRead + c.usage.cacheWrite,
	output_tokens: c.usage.output,
	total_tokens: c.usage.input + c.usage.cacheRead + c.usage.cacheWrite + c.usage.output,
	input_tokens_details: { cached_tokens: c.usage.cacheRead, ...(c.usage.cacheWrite > 0 ? { cache_write_tokens: c.usage.cacheWrite } : {}) },
	output_tokens_details: { reasoning_tokens: 0 },
});

function responsesEvents(c, model, terminal) {
	const out = [];
	let seq = 0;
	const ev = (type, data) => sse(type, { type, sequence_number: seq++, ...data });
	const base = { id: "resp_" + c.id, object: "response", created_at: 0, model: model.id, output: [] };
	out.push(ev("response.created", { response: { ...base, status: "in_progress" } }));
	const done = [];
	c.blocks.forEach((block, output_index) => {
		const suffix = block.type === "toolCall" ? block.id.slice(5) : c.id + output_index;
		if (block.type === "thinking") {
			const id = "rs_" + suffix;
			out.push(ev("response.output_item.added", { output_index, item: { type: "reasoning", id, summary: [] } }));
			out.push(ev("response.reasoning_summary_part.added", { item_id: id, output_index, summary_index: 0, part: { type: "summary_text", text: "" } }));
			for (const delta of block.pieces) out.push(ev("response.reasoning_summary_text.delta", { item_id: id, output_index, summary_index: 0, delta }));
			out.push(ev("response.reasoning_summary_text.done", { item_id: id, output_index, summary_index: 0, text: block.thinking }));
			out.push(ev("response.reasoning_summary_part.done", { item_id: id, output_index, summary_index: 0, part: { type: "summary_text", text: block.thinking } }));
			const item = { type: "reasoning", id, summary: [{ type: "summary_text", text: block.thinking }], encrypted_content: block.signature };
			out.push(ev("response.output_item.done", { output_index, item }));
			done.push(item);
		}
		if (block.type === "text") {
			const id = "msg_" + suffix;
			out.push(ev("response.output_item.added", { output_index, item: { type: "message", id, role: "assistant", status: "in_progress", content: [] } }));
			out.push(ev("response.content_part.added", { item_id: id, output_index, content_index: 0, part: { type: "output_text", text: "", annotations: [] } }));
			for (const delta of block.pieces) out.push(ev("response.output_text.delta", { item_id: id, output_index, content_index: 0, delta }));
			out.push(ev("response.output_text.done", { item_id: id, output_index, content_index: 0, text: block.text }));
			const item = { type: "message", id, role: "assistant", status: "completed", content: [{ type: "output_text", text: block.text, annotations: [] }] };
			out.push(ev("response.output_item.done", { output_index, item }));
			done.push(item);
		}
		if (block.type === "toolCall") {
			const id = "fc_" + suffix;
			const args = JSON.stringify(block.arguments);
			out.push(ev("response.output_item.added", { output_index, item: { type: "function_call", id, call_id: block.id, name: block.name, arguments: "", status: "in_progress" } }));
			for (const delta of block.pieces) out.push(ev("response.function_call_arguments.delta", { item_id: id, output_index, delta }));
			out.push(ev("response.function_call_arguments.done", { item_id: id, output_index, arguments: args }));
			const item = { type: "function_call", id, call_id: block.id, name: block.name, arguments: args, status: "completed" };
			out.push(ev("response.output_item.done", { output_index, item }));
			done.push(item);
		}
	});
	if (c.tail === "error") {
		out.push(ev("error", { code: "server_error", message: "Upstream overloaded", param: null }));
		return out;
	}
	if (c.tail === "truncated") return out;
	const incomplete = c.stop === "length";
	out.push(
		ev(incomplete ? "response.incomplete" : "response.completed", {
			response: { ...base, status: incomplete ? "incomplete" : "completed", ...(incomplete ? { incomplete_details: { reason: "max_output_tokens" } } : {}), output: done, usage: responsesUsage(c) },
		}),
	);
	return terminal ? terminal(out) : out;
}
ENCODE["openai-responses"] = (c, model) => responsesEvents(c, model);
ENCODE["openai-codex-responses"] = (c, model) => responsesEvents(c, model);
ENCODE["azure-openai-responses"] = (c, model) => responsesEvents(c, model);
ENCODE["mistral-conversations"] = (c, model) => {
	const chunk = (delta, finish = null, extra = {}) => sse(null, { id: c.id, object: "chat.completion.chunk", created: 0, model: model.id, choices: [{ index: 0, delta, finish_reason: finish }], ...extra });
	const out = [chunk({ role: "assistant", content: "" })];
	let toolIndex = 0;
	for (const block of c.blocks) {
		if (block.type === "thinking") for (const piece of block.pieces) out.push(chunk({ content: [{ type: "thinking", thinking: [{ type: "text", text: piece }] }] }));
		if (block.type === "text") for (const piece of block.pieces) out.push(chunk({ content: piece }));
		if (block.type === "toolCall") {
			const index = toolIndex++;
			for (const piece of block.pieces) out.push(chunk({ tool_calls: [{ id: block.id, type: "function", index, function: { name: block.name, arguments: piece } }] }));
		}
	}
	if (c.tail === "error") {
		out.push(sse(null, { object: "error", message: "Upstream overloaded", type: "server_error", code: 3000 }));
		return out;
	}
	const usage = { prompt_tokens: c.usage.input + c.usage.cacheRead, completion_tokens: c.usage.output, total_tokens: c.usage.input + c.usage.cacheRead + c.usage.output, prompt_tokens_details: { cached_tokens: c.usage.cacheRead } };
	if (c.tail !== "truncated") out.push(chunk({ content: "" }, OPENAI_STOP[c.stop], { usage }));
	out.push("data: [DONE]\n\n");
	return out;
};
ENCODE["pi-messages"] = (c) => {
	const ev = (data) => sse(null, data);
	const out = [ev({ type: "start" })];
	c.blocks.forEach((block, contentIndex) => {
		if (block.type === "thinking") {
			out.push(ev({ type: "thinking_start", contentIndex }));
			for (const delta of block.pieces) out.push(ev({ type: "thinking_delta", contentIndex, delta }));
			out.push(ev({ type: "thinking_end", contentIndex, content: block.thinking, contentSignature: block.signature }));
		}
		if (block.type === "text") {
			out.push(ev({ type: "text_start", contentIndex }));
			for (const delta of block.pieces) out.push(ev({ type: "text_delta", contentIndex, delta }));
			out.push(ev({ type: "text_end", contentIndex, content: block.text }));
		}
		if (block.type === "toolCall") {
			out.push(ev({ type: "toolcall_start", contentIndex, id: block.id, toolName: block.name }));
			for (const delta of block.pieces) out.push(ev({ type: "toolcall_delta", contentIndex, delta }));
			out.push(ev({ type: "toolcall_end", contentIndex, toolCall: { type: "toolCall", id: block.id, name: block.name, arguments: block.arguments } }));
		}
	});
	if (c.tail === "truncated") return out;
	const u = c.usage;
	const usage = { ...u, totalTokens: u.input + u.output + u.cacheRead + u.cacheWrite, cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } };
	if (c.tail === "error") out.push(ev({ type: "error", reason: "error", usage, errorMessage: "Upstream overloaded", responseId: c.id }));
	else out.push(ev({ type: "done", reason: c.stop, usage, responseId: c.id }));
	return out;
};
const SANITIZE = /[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/g;
const EXPECT_TEXT = { "mistral-conversations": (block) => block.pieces.map((piece) => piece.replace(SANITIZE, "")).join("") };
const EXPECT_USAGE = { "mistral-conversations": (u) => ({ ...u, cacheWrite: 0 }) };

const ORDER = { "anthropic-messages": ["thinking", "text", "toolCall"], "openai-completions": ["thinking", "text", "toolCall"], "openai-responses": ["thinking", "text", "toolCall"], "openai-codex-responses": ["thinking", "text", "toolCall"], "azure-openai-responses": ["thinking", "text", "toolCall"], "mistral-conversations": ["thinking", "text", "toolCall"], "pi-messages": ["thinking", "text", "toolCall"] };
const RESPONSE_ID = { "anthropic-messages": "msg_", "openai-completions": "chatcmpl-", "openai-responses": "resp_", "openai-codex-responses": "resp_", "azure-openai-responses": "resp_", "mistral-conversations": "", "pi-messages": "" };
const TOOL_ID = { "openai-responses": (id) => id + "|fc_" + id.slice(5), "openai-codex-responses": (id) => id + "|fc_" + id.slice(5), "azure-openai-responses": (id) => id + "|fc_" + id.slice(5) };

function expectOf(c, api, model) {
	const lenient = api === "openai-completions" && c.tail === "truncated" && model.compat?.supportsFinishReason === false;
	if (c.tail !== "ok" && !lenient) return { stopReason: "error" };
	const order = ORDER[api];
	const content = [...c.blocks]
		.sort((a, b) => order.indexOf(a.type) - order.indexOf(b.type))
		.map((b) => (b.type === "text" ? { type: "text", text: EXPECT_TEXT[api]?.(b) ?? b.text } : b.type === "thinking" ? { type: "thinking", thinking: EXPECT_TEXT[api]?.(b) ?? b.thinking } : { type: "toolCall", id: (TOOL_ID[api] ?? ((id) => id))(b.id), name: b.name, arguments: b.arguments }));
	return {
		stopReason: lenient ? (c.blocks.some((b) => b.type === "toolCall") ? "toolUse" : "stop") : c.stop,
		content,
		usage: (EXPECT_USAGE[api] ?? ((u) => u))(c.usage),
		responseId: RESPONSE_ID[api] + c.id,
	};
}

function transport(r, events) {
	const bytes = Buffer.from(events.join(""), "utf8");
	if (!r.chance(0.3)) return events;
	const cuts = new Set();
	const n = r.range(2, 8);
	while (cuts.size < n - 1) cuts.add(r.range(1, bytes.length - 1));
	const bounds = [0, ...[...cuts].sort((a, b) => a - b), bytes.length];
	return bounds.slice(1).map((end, i) => {
		const part = bytes.subarray(bounds[i], end);
		const text = part.toString("utf8");
		return Buffer.from(text, "utf8").equals(part) ? text : { base64: part.toString("base64") };
	});
}

function errorScript(r, api) {
	const status = r.pick([400, 401, 404, 429, 500, 529]);
	const message = text(r, "ascii", r.range(2, 6));
	if (r.chance(0.25)) return { status, headers: { "content-type": "text/plain" }, chunks: [message] };
	const body = api === "anthropic-messages" ? { type: "error", error: { type: "invalid_request_error", message } } : { error: { message, type: "invalid_request_error", code: null } };
	return { status, headers: { "content-type": "application/json" }, chunks: [JSON.stringify(body)] };
}

function scenario(api, index) {
	const name = api + "-" + String(index).padStart(4, "0");
	const seed = splitmix64(fnv1a64(name))();
	const r = rng(seed);
	const spec = MODELS[api];
	const id = r.pick(spec.ids);
	const reasoning = r.chance(0.6);
	const model = {
		id,
		name: id,
		api,
		provider: r.pick(spec.providers),
		baseUrl: "http://127.0.0.1:PORT" + spec.path,
		reasoning,
		input: r.chance(0.7) ? ["text", "image"] : ["text"],
		cost: { input: r.range(0, 30) / 4, output: r.range(0, 60) / 4, cacheRead: r.range(0, 8) / 8, cacheWrite: r.range(0, 16) / 4 },
		contextWindow: r.pick([32000, 128000, 200000, 1000000]),
		maxTokens: r.pick([4096, 16384, 64000]),
	};
	const flags = compat(r, api);
	if (flags) model.compat = flags;
	const ctx = context(r, model);
	const opts = options(r, api);
	let script;
	let expect;
	if (r.chance(0.12)) {
		script = [errorScript(r, api)];
		expect = { stopReason: "error" };
	} else {
		const c = canonical(r, ctx.tools ?? []);
		script = [{ status: 200, headers: { "content-type": "text/event-stream" }, chunks: transport(r, ENCODE[api](c, model)) }];
		expect = expectOf(c, api, model);
	}
	return { name, seed: seed.toString(16).padStart(16, "0"), api, provider: model.provider, model, context: ctx, options: opts, script, expect };
}

mkdirSync(outDir, { recursive: true });
for (const file of readdirSync(outDir).filter((f) => f.endsWith(".json"))) rmSync(join(outDir, file));
for (const api of Object.keys(MODELS)) {
	for (let i = 0; i < COUNT; i++) {
		const s = scenario(api, i);
		writeFileSync(join(outDir, s.name + ".json"), JSON.stringify(s, null, "\t") + "\n");
	}
}
