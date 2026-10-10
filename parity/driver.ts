import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { createModels, fauxAssistantMessage, fauxText, fauxToolCall, Type } from "@earendil-works/pi-ai";
import { fauxProvider } from "@earendil-works/pi-ai/compat";
import { createRegistry, defineExtension, defineTool, Harness, LiveDoc, MemoryStorage } from "@earendil-works/pi-durable";
import { openNodeSqliteStorage } from "@earendil-works/pi-durable/storage/sqlite/node";

const [file, outDir, storageKind, dbDir, onlyCase, phaseArg] = process.argv.slice(2);
if (!file || !outDir || !storageKind || !dbDir) throw new Error("usage: driver <scenarios> <out-dir> <memory|sqlite> <db-dir> [case] [run|resume]");
const cases: any[] = JSON.parse(readFileSync(file, "utf8")).filter((c: any) => !onlyCase || c.name === onlyCase);
mkdirSync(outDir, { recursive: true });
const sleepMs = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

const echo = defineTool({
	name: "echo",
	description: "echo tool",
	parameters: Type.Object({ text: Type.String() }),
	execute: async (args: { text: string }) => ({ content: [{ type: "text", text: "echoed:" + args.text }] }),
});
const sleep = defineTool({
	name: "sleep",
	description: "sleep tool",
	parameters: Type.Object({ ms: Type.Number() }),
	execute: async (args: { ms: number }) => {
		await sleepMs(args.ms);
		return { content: [{ type: "text", text: "slept:" + args.ms }] };
	},
});
const TOOLS: Record<string, any> = { echo, sleep };

function block(b: any) {
	return b.type === "toolCall" ? fauxToolCall(b.name, b.arguments, { id: b.id }) : fauxText(b.text);
}
const responses = (list: any[]) =>
	list.map((r: any) => fauxAssistantMessage(r.content.map(block), { stopReason: r.stopReason, ...(r.errorMessage ? { errorMessage: r.errorMessage } : {}) }));

async function until(read: () => Promise<boolean>) {
	const deadline = Date.now() + 30000;
	while (!(await read())) {
		if (Date.now() > deadline) throw new Error("driver: wait timed out");
		await sleepMs(5);
	}
}

async function openStorage(c: any, phase: string) {
	if (storageKind === "memory") return new MemoryStorage();
	const path = join(dbDir, c.name + ".db");
	if (phase !== "resume") rmSync(path, { force: true });
	return openNodeSqliteStorage(path);
}

async function runCase(c: any, phase: string) {
	const ctx = {};
	const crashAt = c.steps.findIndex((s: any) => s.do === "crashPoint");
	const resuming = phase === "resume";
	const faux = fauxProvider(c.settings?.tokensPerSecond ? { tokensPerSecond: c.settings.tokensPerSecond } : {});
	faux.setResponses(responses(resuming ? c.resumeResponses ?? [] : c.responses));
	const models = createModels();
	models.setProvider(faux.provider);
	const registry = createRegistry();
	registry.install(defineExtension({ name: "tools", tools: (c.tools ?? []).map((t: string) => TOOLS[t]) }));
	const settings: any = {};
	if (c.settings?.retry) settings.retry = c.settings.retry;
	if (c.settings?.compaction) settings.compaction = c.settings.compaction;
	const reports: string[] = [];
	const storage = await openStorage(c, phase);
	const harness = await Harness.open(storage, { models, registry, settings, now: () => 1000, onReport: (e: unknown) => reports.push(String(e)) }, ctx);
	const root = await harness.root(ctx, { agent: { model: { provider: "faux", modelId: "faux-1" } } });
	const stepResults: any[] = [];
	const steps = resuming ? c.steps.slice(crashAt + 1) : c.steps;
	for (const step of steps) {
		switch (step.do) {
			case "submit": {
				const draft: any = { type: "input", content: step.text };
				if (step.requestId !== undefined) draft.requestId = step.requestId;
				if (step.whenBusy !== undefined) draft.whenBusy = step.whenBusy;
				try {
					const sub = await root.submit(draft, ctx);
					stepResults.push({ submit: sub.id });
				} catch (error: any) {
					stepResults.push({ err: error.message });
				}
				break;
			}
			case "idle":
				await harness.waitForIdle(ctx);
				break;
			case "sleepMs":
				await sleepMs(step.ms);
				break;
			case "waitRun":
				await until(async () => !!(await harness.snapshot(LiveDoc, root.id, ctx))?.run);
				break;
			case "waitTool":
				await until(async () => ((await harness.snapshot(LiveDoc, root.id, ctx))?.tools ?? []).some((t: any) => t.status === "running"));
				break;
			case "abortRun":
				await root.abort(ctx);
				break;
			case "compact":
				stepResults.push({ compact: await root.compact(step.instructions, ctx) });
				break;
			case "crashPoint":
				if (phase === "run") {
					process.stdout.write("READY\n");
					setInterval(() => {}, 1 << 30);
					await new Promise(() => {});
				}
				break;
			default:
				throw new Error("driver: unknown step " + step.do);
		}
	}
	await harness.waitForIdle(ctx);
	const page = await root.entries({}, 1000, undefined, ctx);
	const entries = [...page.items].sort((a: any, b: any) => a.id - b.id);
	const ids = [...new Set([...entries.map((e: any) => e.byTaskId), ...stepResults.map((s: any) => s.compact)].filter((x: any) => x !== undefined))].sort((a: any, b: any) => a - b);
	const tasks = [];
	for (const id of ids) tasks.push(await harness.getTask(id as any, ctx));
	await harness.close(ctx);
	return { steps: stepResults, entries, reports, tasks };
}

for (const c of cases) {
	const phase = phaseArg ?? "full";
	if (c.crash && phase === "full") continue;
	if (!c.crash && phase !== "full") continue;
	const out = await runCase(c, phase);
	writeFileSync(join(outDir, c.name + ".json"), JSON.stringify(out, null, 1));
	console.log("ts", c.name, phase, "entries", out.entries.length, "tasks", out.tasks.length, "reports", out.reports.length);
}
process.exit(0);
