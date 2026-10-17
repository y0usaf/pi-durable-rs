import { apiPromise, buildHeaders, buildURL, errorClasses, send } from "./core.mjs";

const VERSION = "0.129.0";

export class AnthropicError extends Error {}
const errors = errorClasses(AnthropicError);
export const { APIError, APIUserAbortError, APIConnectionError, APIConnectionTimeoutError } = errors;

export class Anthropic {
	constructor({ baseURL, apiKey, authToken, ...opts } = {}) {
		this.apiKey = apiKey ?? null;
		this.authToken = authToken ?? null;
		this.baseURL = baseURL || "https://api.anthropic.com";
		this.timeout = opts.timeout ?? 600000;
		this._options = { ...opts, baseURL: this.baseURL, apiKey: this.apiKey, authToken: this.authToken };
		this._shouldResolveDefaultCredentials();
		const client = this;
		this.beta = {
			messages: {
				create(params, options) {
					return client._createMessage("/v1/messages?beta=true", params, options, true);
				},
			},
		};
		this.messages = {
			create(params, options) {
				return client._createMessage("/v1/messages", params, options, false);
			},
		};
	}

	_shouldResolveDefaultCredentials() {
		return true;
	}

	withOptions(options) {
		return new this.constructor({ ...this._options, ...options });
	}

	_createMessage(path, params, options, beta) {
		const { betas, user_profile_id, workspace_id, ...body } = params;
		const methodHeaders = beta
			? {
					...(betas?.toString() != null ? { "anthropic-beta": betas.toString() } : undefined),
					...(user_profile_id != null ? { "anthropic-user-profile-id": user_profile_id } : undefined),
					...(workspace_id != null ? { "anthropic-workspace-id": workspace_id } : undefined),
				}
			: undefined;
		const timeout = options?.timeout ?? this.timeout;
		const headers = buildHeaders([
			{
				Accept: "application/json",
				"User-Agent": `Anthropic/JS ${VERSION}`,
				...(this._options.dangerouslyAllowBrowser ? { "anthropic-dangerous-direct-browser-access": "true" } : undefined),
				"anthropic-version": "2023-06-01",
			},
			this.apiKey == null ? undefined : { "X-Api-Key": this.apiKey },
			this.authToken == null ? undefined : { Authorization: `Bearer ${this.authToken}` },
			this._options.defaultHeaders,
			{ "content-type": "application/json" },
			methodHeaders,
			options?.headers,
		]);
		if (!headers["x-api-key"] && !headers.authorization) {
			throw new AnthropicError(
				'Could not resolve authentication method. Expected either apiKey or authToken to be set. Or for one of the "X-Api-Key" or "Authorization" headers to be explicitly omitted',
			);
		}
		return apiPromise(
			() =>
				send({
					fetch: this._options.fetch,
					url: buildURL(this.baseURL, path),
					headers,
					body,
					timeout,
					signal: options?.signal,
					errors,
					statusError: (status, error, message, responseHeaders) => new APIError(status, error, message, responseHeaders),
				}),
			async ({ response }) => response.json(),
		);
	}
}

export default Anthropic;
