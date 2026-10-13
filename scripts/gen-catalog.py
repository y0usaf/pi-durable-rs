#!/usr/bin/env python3
import base64
import hashlib
import io
import json
import pathlib
import sys
import tarfile
import urllib.request

VERSION = "1.2.0"
INTEGRITY = "sha512-U5/1zXqfV1on7/9sbez03tT4Ru49Lvfb4+zXSDK3DBKF5cRk6KxvAOTOY3HJ4jimlZth+rs7vbJqN6iBQAoNuQ=="

URL = f"https://registry.npmjs.org/@earendil-works/pi-ai/-/pi-ai-{VERSION}.tgz"
SCHEMA = 6
DATA = "package/dist/providers/data/"
FIELDS = (
    "id",
    "name",
    "api",
    "provider",
    "reasoning",
    "contextWindow",
    "maxTokens",
    "baseUrl",
    "input",
    "cost",
    "headers",
    "thinkingLevelMap",
    "promptCache",
    "compat",
    "samplingParams",
    "samplingParamsByThinkingLevel",
)


def fetch():
    with urllib.request.urlopen(URL, timeout=60) as response:
        data = response.read()
    digest = "sha512-" + base64.b64encode(hashlib.sha512(data).digest()).decode()
    if digest != INTEGRITY:
        sys.exit(f"pi-ai {VERSION}: integrity {digest} does not match pin {INTEGRITY}")
    return tarfile.open(fileobj=io.BytesIO(data))


def model(raw):
    out = {key: raw[key] for key in FIELDS if key in raw}
    if not out.get("baseUrl"):
        out.pop("baseUrl", None)
    out["cost"] = {**out["cost"], "tiers": out["cost"].get("tiers", [])}
    return out


def read(archive, name):
    return json.load(archive.extractfile(DATA + name))


def catalog(archive):
    manifest = read(archive, ".manifest.json")
    if manifest["schemaVersion"] != SCHEMA:
        sys.exit(f"pi-ai {VERSION}: data schema {manifest['schemaVersion']}, generator reads {SCHEMA}")
    models = []
    for name in sorted(manifest["files"], key=lambda name: pathlib.PurePosixPath(name).stem):
        for group in read(archive, name).values():
            models.extend(model(raw) for raw in group.values() if raw["type"] == "chat")
    return models


sys.stdout.write(json.dumps(catalog(fetch()), indent=2, sort_keys=True, ensure_ascii=False) + "\n")
