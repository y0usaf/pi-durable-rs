#!/usr/bin/env python3
"""Generate the embedded model catalogs in src/ai/catalog/ from PiG's models_generated.go.

Usage: scripts/gen-catalog.py PIG_AI_DIR [OUT_DIR]

The Go file is a list of struct literals; they are parsed, not executed. Only the
chat providers the crate ships are kept. Output is the serde shape of
crate::ai::types::Model (camelCase), costs per million tokens.
"""
import json
import pathlib
import re
import sys

PROVIDERS = {
    "anthropic": "anthropic.json",
    "vercel-ai-gateway": "vercel-ai-gateway.json",
    "openai": "openai.json",
}
KNOWN_FIELDS = {
    "ID", "Provider", "DisplayName", "API", "BaseURL", "Headers", "Compat",
    "ThinkingLevelMap", "SamplingParams", "PromptCache", "InputLimits",
    "ContextWindow", "MaxOutputTokens", "InputCostPerMTokens",
    "OutputCostPerMTokens", "CacheReadCost", "CacheWriteCost", "Tiers",
    "Reasoning", "Capabilities", "Enabled", "Lab", "Providers",
}
DROPPED_FIELDS = {"Enabled", "Lab", "Providers", "InputLimits", "Capabilities"}
INPUT_MODALITIES = {"text", "image"}

TOKEN = re.compile(
    r"""(?P<ws>\s+|//[^\n]*)
    |(?P<str>"(?:[^"\\\n]|\\.)*")
    |(?P<num>-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)
    |(?P<id>[A-Za-z_]\w*)
    |(?P<punct>[\[\]{}(),:&*=])""",
    re.X,
)
STRUCT = re.compile(r"^type (\w+) struct \{(.*?)^\}", re.M | re.S)
FIELD = re.compile(r"^\s+(\w+)\s+\S.*?`json:\"([^\",`]+)", re.M)
ALIAS = re.compile(r"^type (\w+) = (\w+)$", re.M)


def die(message):
    sys.exit(f"gen-catalog: {message}")


def tokenize(src):
    tokens = []
    pos = 0
    while pos < len(src):
        match = TOKEN.match(src, pos)
        if not match:
            die(f"cannot tokenize at offset {pos}: {src[pos:pos + 40]!r}")
        pos = match.end()
        kind = match.lastgroup
        if kind != "ws":
            tokens.append((kind, match.group(kind)))
    return tokens


class Ident:
    def __init__(self, name):
        self.name = name


class Parser:
    def __init__(self, tokens, tags, aliases):
        self.toks = tokens
        self.i = 0
        self.tags = tags
        self.aliases = aliases

    def peek(self, ahead=0):
        j = self.i + ahead
        return self.toks[j] if j < len(self.toks) else ("eof", "")

    def take(self, expect=None):
        kind, value = self.peek()
        if expect is not None and value != expect:
            die(f"expected {expect!r}, found {value!r} (token {self.i})")
        self.i += 1
        return value

    def type_(self):
        if self.peek()[1] == "*":
            self.take()
            return "*" + self.type_()
        if self.peek()[1] == "[":
            self.take("[")
            self.take("]")
            return "[]" + self.type_()
        name = self.take()
        if name == "map":
            self.take("[")
            key = self.type_()
            self.take("]")
            return f"map[{key}]{self.type_()}"
        return name

    def value(self, elem=None):
        kind, v = self.peek()
        if v == "&":
            self.take()
            return self.value(elem)
        if v == "{":
            return self.composite(elem)
        if v == "[":
            return self.composite(self.type_())
        if kind == "str":
            self.take()
            return json.loads(v)
        if kind == "num":
            self.take()
            return float(v) if any(c in v for c in ".eE") else int(v)
        if kind != "id":
            die(f"unexpected token {v!r} at {self.i}")
        self.take()
        nxt = self.peek()[1]
        if v == "map" and nxt == "[":
            self.i -= 1
            return self.composite(self.type_())
        if v == "nil":
            return None
        if v in ("true", "false"):
            return v == "true"
        if nxt == "{":
            return self.composite(v)
        if nxt == "(":
            self.take("(")
            args = []
            while self.peek()[1] != ")":
                args.append(self.value())
                if self.peek()[1] == ",":
                    self.take()
            self.take(")")
            if len(args) != 1:
                die(f"call {v} with {len(args)} arguments at {self.i}")
            return args[0]
        return Ident(v)

    def composite(self, typ):
        self.take("{")
        elem = typ[2:] if typ and typ.startswith("[]") else None
        items, pairs = [], []
        while self.peek()[1] != "}":
            key = self.value(elem)
            if self.peek()[1] == ":":
                self.take(":")
                pairs.append((key, self.value(elem)))
            else:
                items.append(key)
            if self.peek()[1] == ",":
                self.take()
        self.take("}")
        if pairs:
            return self.keyed(typ, pairs)
        if items or (typ and typ.startswith("[]")):
            return items
        return {}

    def keyed(self, typ, pairs):
        name = self.aliases.get(typ, typ)
        fields = self.tags.get(name, {})
        out = {}
        for key, value in pairs:
            if isinstance(key, Ident):
                key = key.name
            if not isinstance(key, str):
                die(f"non-string key {key!r} in {typ}")
            if key in out:
                die(f"duplicate key {key} in {typ}")
            out[fields.get(key, key)] = value
        return out


def load_types(ai_dir):
    tags, aliases = {}, {}
    for path in sorted(ai_dir.glob("*.go")):
        if path.name.endswith("_test.go"):
            continue
        src = path.read_text()
        for alias, target in ALIAS.findall(src):
            aliases[alias] = target
        for name, body in STRUCT.findall(src):
            tags[name] = {field: tag for field, tag in FIELD.findall(body)}
    return tags, aliases


def to_model(entry):
    unknown = set(entry) - KNOWN_FIELDS
    if unknown:
        die(f"{entry['ID']}: unknown fields {sorted(unknown)}")
    capabilities = entry.get("Capabilities", [])
    if "text" not in capabilities or not set(capabilities) <= INPUT_MODALITIES:
        die(f"{entry['ID']}: unsupported capabilities {capabilities}")
    model = {
        "id": entry["ID"],
        "name": entry.get("DisplayName") or entry["ID"],
        "api": entry["API"],
        "provider": entry["Provider"],
        "reasoning": entry.get("Reasoning", False),
        "contextWindow": entry["ContextWindow"],
        "maxTokens": entry["MaxOutputTokens"],
        "baseUrl": entry["BaseURL"],
        "input": [c for c in capabilities if c in INPUT_MODALITIES],
        "cost": {
            "input": float(entry.get("InputCostPerMTokens", 0)),
            "output": float(entry.get("OutputCostPerMTokens", 0)),
            "cacheRead": float(entry.get("CacheReadCost", 0)),
            "cacheWrite": float(entry.get("CacheWriteCost", 0)),
        },
    }
    tiers = entry.get("Tiers")
    if tiers:
        model["cost"]["tiers"] = [{k: float(v) if k != "inputTokensAbove" else v for k, v in t.items()} for t in tiers]
    if "Headers" in entry:
        model["headers"] = entry["Headers"]
    if "ThinkingLevelMap" in entry:
        model["thinkingLevelMap"] = entry["ThinkingLevelMap"]
    if "PromptCache" in entry:
        model["promptCache"] = entry["PromptCache"]
    if "Compat" in entry:
        model["compat"] = entry["Compat"]
    if "SamplingParams" in entry:
        model["samplingParams"] = entry["SamplingParams"]
    return model


def main(argv):
    if len(argv) not in (2, 3):
        die("usage: gen-catalog.py PIG_AI_DIR [OUT_DIR]")
    ai_dir = pathlib.Path(argv[1])
    out_dir = pathlib.Path(argv[2]) if len(argv) == 3 else pathlib.Path(__file__).resolve().parent.parent / "src/ai/catalog"
    src = (ai_dir / "models_generated.go").read_text()
    marker = "var GeneratedModels = "
    start = src.find(marker)
    if start < 0:
        die("GeneratedModels not found")
    tags, aliases = load_types(ai_dir)
    parser = Parser(tokenize(src[start + len(marker):]), tags, aliases)
    entries = parser.value()
    if not isinstance(entries, list) or not entries:
        die("GeneratedModels is not a non-empty list")

    grouped = {provider: [] for provider in PROVIDERS}
    for entry in entries:
        if not isinstance(entry, dict):
            die(f"entry is not a struct literal: {entry!r}")
        if entry.get("Provider") in grouped:
            grouped[entry["Provider"]].append(to_model(entry))

    out_dir.mkdir(parents=True, exist_ok=True)
    for provider, filename in PROVIDERS.items():
        models = grouped[provider]
        if not models:
            die(f"no models for {provider}")
        ids = [m["id"] for m in models]
        if len(set(ids)) != len(ids):
            die(f"duplicate model ids for {provider}")
        (out_dir / filename).write_text(json.dumps(models, indent=2, ensure_ascii=False) + "\n")
        print(f"{provider}: {len(models)} models -> {filename}", file=sys.stderr)


if __name__ == "__main__":
    main(sys.argv)
