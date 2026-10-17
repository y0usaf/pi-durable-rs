import { apiPromise, buildHeaders, buildURL, errorClasses, send, sseMessages } from "./core.mjs";

const VERSION = "7.19.0";

export class OpenAIError extends Error {}
const errors = errorClasses(OpenAIError);
export const { APIError, APIUserAbortError, APIConnectionError, APIConnectionTimeoutError } = errors;

function statusError(status, body, message, headers) {
	return new APIError(status, body?.error, message, headers);
}

async function* stream(response) {
	for await (const sse of sseMessages(response.body)) {
		if (sse.data === "[DONE]") return;
		let data;
		try {
			data = JSON.parse(sse.data);
		} catch {
			throw new SyntaxError("Error reading response: malformed server-sent event JSON.");
		}
		if (sse.event === null || !sse.event.startsWith("thread.")) {
			if (sse.event === "error") throw new APIError(undefined, data?.error ?? data, undefined, response.headers);
			if (data && data.error) throw new APIError(undefined, data.error, undefined, response.headers);
			yield data;
		} else {
			yield { event: sse.event, data };
		}
	}
}

export class OpenAI {
	constructor({ baseURL, apiKey, organization, project, ...opts } = {}) {
		this.apiKey = apiKey ?? null;
		this.organization = organization ?? null;
		this.project = project ?? null;
		this.baseURL = baseURL || "https://api.openai.com/v1";
		this.timeout = opts.timeout ?? 600000;
		this._options = { ...opts, baseURL: this.baseURL, apiKey: this.apiKey };
		this.chat = { completions: { create: (body, options) => this._post("/chat/completions", body, options) } };
		this.responses = { create: (body, options) => this._post("/responses", body, options) };
	}

	authHeaders() {
		return this.apiKey == null ? undefined : { Authorization: `Bearer ${this.apiKey}` };
	}

	defaultQuery() {
		return this._options.defaultQuery;
	}

	_post(path, body, options) {
		const headers = buildHeaders([
			{
				Accept: "application/json",
				"User-Agent": `${this.constructor.name}/JS ${VERSION}`,
				"OpenAI-Organization": this.organization,
				"OpenAI-Project": this.project,
			},
			this.authHeaders(),
			this._options.defaultHeaders,
			{ "content-type": "application/json" },
			options?.headers,
		]);
		if (!headers.authorization && !headers["api-key"]) {
			throw new Error(
				'Could not resolve authentication method. Expected either apiKey or adminAPIKey to be set. Or for one of the "Authorization" or "api-key" headers to be explicitly omitted',
			);
		}
		return apiPromise(
			() =>
				send({
					fetch: this._options.fetch,
					url: buildURL(this.baseURL, path, { ...this.defaultQuery(), ...options?.query }),
					headers,
					body,
					timeout: options?.timeout ?? this.timeout,
					signal: options?.signal,
					errors,
					statusError,
				}),
			async ({ response }) => (body?.stream ? stream(response) : response.json()),
		);
	}
}

export class AzureOpenAI extends OpenAI {
	constructor({ baseURL, apiKey, apiVersion, endpoint, deployment, azureADTokenProvider, ...opts } = {}) {
		if (!apiVersion) {
			throw new OpenAIError(
				"The OPENAI_API_VERSION environment variable is missing or empty; either provide it, or instantiate the AzureOpenAI client with an apiVersion option, like new AzureOpenAI({ apiVersion: 'My API Version' }).",
			);
		}
		if (!azureADTokenProvider && !apiKey) {
			throw new OpenAIError(
				"Missing credentials. Please pass one of `apiKey` and `azureADTokenProvider`, or set the `AZURE_OPENAI_API_KEY` environment variable.",
			);
		}
		opts.defaultQuery = { ...opts.defaultQuery, "api-version": apiVersion };
		if (!baseURL) {
			if (!endpoint) throw new OpenAIError("Must provide one of the `baseURL` or `endpoint` arguments, or the `AZURE_OPENAI_ENDPOINT` environment variable");
			baseURL = `${endpoint.replace(/\/+$/, "")}/openai`;
		}
		super({ apiKey: azureADTokenProvider ?? apiKey, baseURL, ...opts });
		this.apiVersion = apiVersion;
		this.deploymentName = deployment;
	}

	authHeaders() {
		return typeof this.apiKey === "string" ? { "api-key": this.apiKey } : undefined;
	}
}

export default OpenAI;
