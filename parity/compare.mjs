import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
const pairArgs = process.argv.slice(2);
const NORMALISED = new Set(["timestamp", "usage", "responseId", "api", "startedAt", "endedAt"]);
const DROPPED = new Set(["durationMs"]);
function canon(value) {
	if (Array.isArray(value)) return value.map(canon);
	if (value === null || typeof value !== "object") return value;
	const partial = value.stopReason === "aborted";
	return Object.fromEntries(
		Object.keys(value)
			.filter((key) => !DROPPED.has(key))
			.sort()
			.map((key) => {
				if (NORMALISED.has(key) && value[key] !== undefined) return [key, "<norm>"];
				if (partial && key === "content" && Array.isArray(value[key])) {
					return [key, value[key].map((b) => (b && b.type === "text" ? { ...b, text: "<partial>" } : canon(b)))];
				}
				return [key, canon(value[key])];
			}),
	);
}
function load(file) {
	return JSON.stringify(canon(JSON.parse(readFileSync(file, "utf8"))), null, 1);
}
const families = new Map();
let total = 0;
let failed = 0;
for (const pair of pairArgs) {
	const [a, b] = pair.split("=");
	const label = a + " vs " + b;
	const names = readdirSync(a).filter((n) => n.endsWith(".json")).sort();
	for (const name of names) {
		const family = name.split("-")[0];
		const tally = families.get(family) ?? { probes: 0, diffs: 0 };
		tally.probes += 1;
		total += 1;
		let same = false;
		try {
			same = load(join(a, name)) === load(join(b, name));
		} catch (error) {
			console.log("MISSING " + label + " " + name + ": " + error.code);
		}
		if (!same) {
			tally.diffs += 1;
			failed += 1;
			console.log("DIFF " + label + " " + name);
		}
		families.set(family, tally);
	}
}
for (const [family, tally] of [...families].sort()) console.log(family + " probes " + tally.probes + " diffs " + tally.diffs);
console.log("total probes " + total + " diffs " + failed);
process.exitCode = failed === 0 ? 0 : 1;
