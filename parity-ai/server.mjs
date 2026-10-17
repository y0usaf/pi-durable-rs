import { appendFileSync, readdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { createServer } from "node:http";
import { join } from "node:path";

const [scenarioDir, outDir, ...sides] = process.argv.slice(2);
if (!scenarioDir || !outDir || sides.length === 0) throw new Error("usage: server.mjs <scenarios-dir> <out-dir> <side>...");
const log = join(outDir, "requests.jsonl");
writeFileSync(log, "");
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function listen(side, scenario) {
	const queue = [...scenario.script];
	const server = createServer(async (req, res) => {
		const chunks = [];
		for await (const chunk of req) chunks.push(chunk);
		const reply = queue.shift() ?? { status: 599, headers: { "content-type": "text/plain" }, chunks: ["parity-ai: script exhausted"] };
		appendFileSync(
			log,
			JSON.stringify({ side, name: scenario.name, method: req.method, url: req.url, headers: req.headers, body: Buffer.concat(chunks).toString("utf8") }) + "\n",
		);
		res.writeHead(reply.status, reply.headers);
		for (const chunk of reply.chunks) {
			res.write(typeof chunk === "string" ? chunk : Buffer.from(chunk.base64, "base64"));
			if (reply.chunkDelayMs) await sleep(reply.chunkDelayMs);
		}
		res.end();
	});
	return new Promise((resolve) => server.listen(0, "127.0.0.1", () => resolve(server.address().port)));
}

const ports = Object.fromEntries(sides.map((side) => [side, {}]));
for (const file of readdirSync(scenarioDir).filter((f) => f.endsWith(".json")).sort()) {
	const scenario = JSON.parse(readFileSync(join(scenarioDir, file), "utf8"));
	for (const side of sides) ports[side][scenario.name] = await listen(side, scenario);
}
writeFileSync(join(outDir, "ports.json.tmp"), JSON.stringify(ports));
renameSync(join(outDir, "ports.json.tmp"), join(outDir, "ports.json"));
process.on("SIGTERM", () => process.exit(0));
