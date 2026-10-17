import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { streamSimple } from "@earendil-works/pi-ai/compat";

const [mode, input, portsFile] = process.argv.slice(2);
const src = new URL("../packages/ai/src/", import.meta.url);
const snapshot = (value: unknown) => JSON.parse(JSON.stringify(value));
const print = (value: unknown) => process.stdout.write(JSON.stringify(value) + "\n");

async function e1(dir: string, ports: Record<string, number>) {
	for (const file of readdirSync(dir).filter((f) => f.endsWith(".json")).sort()) {
		const scenario = JSON.parse(readFileSync(join(dir, file), "utf8"));
		const model = { ...scenario.model, baseUrl: scenario.model.baseUrl.replace("PORT", String(ports[scenario.name])) };
		const events: unknown[] = [];
		let message: unknown = null;
		let error: string | null = null;
		try {
			const stream = streamSimple(model, scenario.context, scenario.options);
			for await (const event of stream) events.push(snapshot(event));
			message = snapshot(await stream.result());
		} catch (thrown) {
			error = thrown instanceof Error ? thrown.message : String(thrown);
		}
		print({ name: scenario.name, events, message, error });
	}
}

async function e2(rows: any[]) {
	for (const row of rows) {
		const fn = (await import(new URL(row.ts, src).href))[row.fn];
		for (const [index, args] of row.cases.entries()) {
			let out;
			try {
				out = { ok: true, value: snapshot((await fn(...structuredClone(args))) ?? null) };
			} catch (thrown) {
				out = { ok: false, kind: thrown instanceof Error ? thrown.name : typeof thrown, error: String(thrown instanceof Error ? thrown.message : thrown) };
			}
			print({ fn: row.fn, case: index, out });
		}
	}
}

if (mode === "e1" && input && portsFile) await e1(input, JSON.parse(readFileSync(portsFile, "utf8")).ts);
else if (mode === "e2" && input) await e2(JSON.parse(readFileSync(input, "utf8")));
else throw new Error("usage: driver-ai.ts e1 <scenarios-dir> <ports.json> | e2 <fns.json>");
