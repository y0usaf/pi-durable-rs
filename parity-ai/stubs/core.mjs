export function buildHeaders(sources) {
	const out = new Map();
	for (const source of sources) {
		if (!source) continue;
		const entries = source instanceof Headers ? [...source.entries()] : Object.entries(source);
		for (const [name, value] of entries) {
			if (value === undefined) continue;
			const key = name.toLowerCase();
			if (value === null) out.delete(key);
			else out.set(key, String(value));
		}
	}
	return Object.fromEntries(out);
}

export function makeMessage(status, error, message) {
	const msg = error?.message
		? typeof error.message === "string"
			? error.message
			: JSON.stringify(error.message)
		: error
			? JSON.stringify(error)
			: message;
	if (status && msg) return `${status} ${msg}`;
	if (status) return `${status} status code (no body)`;
	if (msg) return msg;
	return "(no status code or body)";
}

export function safeJSON(text) {
	try {
		return JSON.parse(text);
	} catch {
		return undefined;
	}
}

export function buildURL(baseURL, path, query) {
	const url = new URL(baseURL + (baseURL.endsWith("/") && path.startsWith("/") ? path.slice(1) : path));
	const merged = { ...Object.fromEntries(url.searchParams), ...(query ?? {}) };
	if (Object.keys(merged).length > 0) url.search = new URLSearchParams(merged).toString();
	return url.toString();
}

export function errorClasses(base) {
	class APIError extends base {
		constructor(status, error, message, headers) {
			super(makeMessage(status, error, message));
			this.status = status;
			this.headers = headers;
			this.error = error;
		}
	}
	class APIUserAbortError extends APIError {
		constructor({ message } = {}) {
			super(undefined, undefined, message || "Request was aborted.", undefined);
		}
	}
	class APIConnectionError extends APIError {
		constructor({ message, cause } = {}) {
			super(undefined, undefined, message || "Connection error.", undefined);
			if (cause) this.cause = cause;
		}
	}
	class APIConnectionTimeoutError extends APIConnectionError {
		constructor({ message } = {}) {
			super({ message: message ?? "Request timed out." });
		}
	}
	return { APIError, APIUserAbortError, APIConnectionError, APIConnectionTimeoutError };
}

export async function send({ fetch, url, headers, body, timeout, signal, errors, statusError }) {
	const controller = new AbortController();
	const abort = () => controller.abort();
	if (signal?.aborted) throw new errors.APIUserAbortError();
	signal?.addEventListener("abort", abort, { once: true });
	let timedOut = false;
	const timer = setTimeout(() => {
		timedOut = true;
		controller.abort();
	}, timeout);
	let response;
	try {
		response = await (fetch ?? globalThis.fetch)(url, {
			method: "POST",
			headers,
			body: JSON.stringify(body),
			signal: controller.signal,
		});
	} catch (cause) {
		if (signal?.aborted) throw new errors.APIUserAbortError();
		if (timedOut) throw new errors.APIConnectionTimeoutError();
		throw new errors.APIConnectionError({ cause });
	} finally {
		clearTimeout(timer);
	}
	if (!response.ok) {
		const errText = await response.text().catch((err) => String(err?.message ?? err));
		const errJSON = safeJSON(errText);
		throw statusError(response.status, errJSON, errJSON ? undefined : errText, response.headers);
	}
	return { response, controller };
}

export function apiPromise(run, parse) {
	let pending;
	const raw = () => (pending ??= run());
	return {
		asResponse: async () => (await raw()).response,
		withResponse: async () => {
			const result = await raw();
			return { data: await parse(result), response: result.response, request_id: result.response.headers.get("x-request-id") };
		},
		then: (ok, fail) => raw().then(parse).then(ok, fail),
		catch: (fail) => raw().then(parse).catch(fail),
	};
}

export async function* sseMessages(body) {
	const decoder = new TextDecoder();
	let buffer = "";
	let event = null;
	let data = [];
	const decode = function* (line) {
		if (line.endsWith("\r")) line = line.slice(0, -1);
		if (!line) {
			if (!event && data.length === 0) return;
			yield { event, data: data.join("\n") };
			event = null;
			data = [];
			return;
		}
		if (line.startsWith(":")) return;
		const colon = line.indexOf(":");
		const field = colon === -1 ? line : line.slice(0, colon);
		let value = colon === -1 ? "" : line.slice(colon + 1);
		if (value.startsWith(" ")) value = value.slice(1);
		if (field === "event") event = value;
		else if (field === "data") data.push(value);
	};
	for await (const chunk of body) {
		buffer += decoder.decode(chunk, { stream: true });
		let index;
		while ((index = buffer.search(/\r\n|\n|\r(?!$)/)) !== -1) {
			const width = buffer.startsWith("\r\n", index) ? 2 : 1;
			const line = buffer.slice(0, index);
			buffer = buffer.slice(index + width);
			yield* decode(line);
		}
	}
	buffer += decoder.decode();
	if (buffer) yield* decode(buffer);
}

export function unimplemented(name) {
	return class {
		constructor() {
			throw new Error(`parity-ai stub: ${name} is not implemented`);
		}
	};
}
