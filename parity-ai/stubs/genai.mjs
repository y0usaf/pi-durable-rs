const LABEL = "google-genai-sdk/2.21.0 gl-node/" + process.version;

const enumOf = (names) => Object.freeze(Object.fromEntries(names.map((name) => [name, name])));
export const FinishReason = enumOf(["FINISH_REASON_UNSPECIFIED", "STOP", "MAX_TOKENS", "SAFETY", "RECITATION", "LANGUAGE", "OTHER", "BLOCKLIST", "PROHIBITED_CONTENT", "SPII", "MALFORMED_FUNCTION_CALL", "IMAGE_SAFETY", "UNEXPECTED_TOOL_CALL", "TOO_MANY_TOOL_CALLS", "IMAGE_PROHIBITED_CONTENT", "NO_IMAGE", "IMAGE_RECITATION", "IMAGE_OTHER"]);
export const FunctionCallingConfigMode = enumOf(["MODE_UNSPECIFIED", "AUTO", "ANY", "NONE", "VALIDATED"]);
export const ThinkingLevel = enumOf(["THINKING_LEVEL_UNSPECIFIED", "MINIMAL", "LOW", "MEDIUM", "HIGH"]);
export const ResourceScope = enumOf(["COLLECTION"]);

export class ApiError extends Error {
	constructor({ message, status }) {
		super(message);
		this.name = "ApiError";
		this.status = status;
	}
}

const unmodelled = (what) => {
	throw new Error("@google/genai stub: " + what + " is not modelled");
};
const env = (name) => process.env[name]?.trim() || undefined;

function copy(from, keys, to = {}) {
	for (const key of keys) if (from[key] != null) to[key] = from[key];
	return to;
}
function forbid(from, keys, [only, not]) {
	for (const key of keys) if (from[key] !== undefined) throw new Error(key + " parameter is only supported in " + only + ", not in " + not + ".");
}
function guard(from, keys) {
	for (const key of keys) if (from[key] != null) unmodelled(key);
}

const ENTERPRISE = ["Gemini Enterprise Agent Platform mode", "Gemini Developer API mode"];
const DEVELOPER = [ENTERPRISE[1], ENTERPRISE[0]];

function tModel(model, vertex) {
	if (!model || typeof model !== "string") throw new Error("model is required and must be a string");
	if (model.includes("..") || model.includes("?") || model.includes("&")) throw new Error("invalid model parameter");
	if (!vertex) return model.startsWith("models/") || model.startsWith("tunedModels/") ? model : "models/" + model;
	if (["publishers/", "projects/", "models/"].some((prefix) => model.startsWith(prefix))) return model;
	if (model.includes("/")) {
		const [publisher, id] = model.split("/", 2);
		return "publishers/" + publisher + "/models/" + id;
	}
	return "publishers/google/models/" + model;
}
function tPart(origin) {
	if (origin === null || origin === undefined) throw new Error("PartUnion is required");
	if (typeof origin === "object") return origin;
	if (typeof origin === "string") return { text: origin };
	throw new Error("Unsupported part type: " + typeof origin);
}
function tParts(origin) {
	if (origin === null || origin === undefined || (Array.isArray(origin) && origin.length === 0)) throw new Error("PartListUnion is required");
	return Array.isArray(origin) ? origin.map(tPart) : [tPart(origin)];
}
const isContent = (origin) => origin !== null && typeof origin === "object" && "parts" in origin && Array.isArray(origin.parts);
const isCallPart = (origin) => origin !== null && typeof origin === "object" && ("functionCall" in origin || "functionResponse" in origin);
function tContent(origin) {
	if (origin === null || origin === undefined) throw new Error("ContentUnion is required");
	return isContent(origin) ? origin : { role: "user", parts: tParts(origin) };
}
function tContents(origin) {
	if (origin === null || origin === undefined || (Array.isArray(origin) && origin.length === 0)) throw new Error("contents are required");
	if (!Array.isArray(origin)) {
		if (isCallPart(origin)) throw new Error("To specify functionCall or functionResponse parts, please wrap them in a Content object, specifying the role for them");
		return [tContent(origin)];
	}
	const result = [];
	const parts = [];
	const contentArray = isContent(origin[0]);
	for (const item of origin) {
		if (isContent(item) !== contentArray) throw new Error("Mixing Content and Parts is not supported, please group the parts into a the appropriate Content objects and specify the roles for them");
		if (contentArray) result.push(item);
		else if (isCallPart(item)) throw new Error("To specify functionCall or functionResponse parts, please wrap them, and any other parts, in Content objects as appropriate, specifying the role for them");
		else parts.push(item);
	}
	if (!contentArray) result.push({ role: "user", parts: tParts(parts) });
	return result;
}

function partToMldev(from) {
	guard(from, ["fileData"]);
	const to = copy(from, ["mediaResolution", "toolCall", "toolResponse", "audioTranscription", "codeExecutionResult", "executableCode"]);
	if (from.functionCall != null) {
		const call = from.functionCall;
		to.functionCall = copy(call, ["args", "id", "name"]);
		forbid(call, ["partialArgs", "willContinue"], ENTERPRISE);
	}
	copy(from, ["functionResponse"], to);
	if (from.inlineData != null) {
		const blob = from.inlineData;
		forbid(blob, ["displayName"], ENTERPRISE);
		to.inlineData = copy(blob, ["data", "mimeType"]);
	}
	return copy(from, ["text", "thought", "thoughtSignature", "videoMetadata", "partMetadata", "mediaProcessing"], to);
}
function contentToMldev(from) {
	const to = {};
	if (from.parts != null) to.parts = Array.isArray(from.parts) ? from.parts.map(partToMldev) : from.parts;
	return copy(from, ["role"], to);
}
function tTool(tool) {
	for (const declaration of tool.functionDeclarations ?? []) {
		for (const [field, jsonField] of [["parameters", "parametersJsonSchema"], ["response", "responseJsonSchema"]]) {
			if (!declaration[field]) continue;
			if (!Object.keys(declaration[field]).includes("$schema")) unmodelled("functionDeclarations[]." + field + " without $schema (processJsonSchema)");
			if (!declaration[jsonField]) {
				declaration[jsonField] = declaration[field];
				delete declaration[field];
			}
		}
	}
	return tool;
}
function toolToMldev(from) {
	forbid(from, ["retrieval"], ENTERPRISE);
	guard(from, ["googleMaps", "googleSearch"]);
	const to = copy(from, ["mcpServers", "codeExecution", "computerUse"]);
	forbid(from, ["enterpriseWebSearch", "exaAiSearch"], ENTERPRISE);
	copy(from, ["functionDeclarations", "googleSearchRetrieval"], to);
	forbid(from, ["parallelAiSearch"], ENTERPRISE);
	return copy(from, ["urlContext", "fileSearch"], to);
}
function toolConfigToMldev(from) {
	const to = {};
	if (from.functionCallingConfig != null) {
		const config = from.functionCallingConfig;
		to.functionCallingConfig = copy(config, ["allowedFunctionNames", "mode"]);
		forbid(config, ["streamFunctionCallArguments"], ENTERPRISE);
	}
	return copy(from, ["retrievalConfig", "includeServerSideToolInvocations"], to);
}
function configToMldev(from, parent) {
	guard(from, ["responseSchema", "responseJsonSchema", "safetySettings", "cachedContent", "speechConfig", "imageConfig"]);
	copy(from, ["serviceTier"], parent);
	if (from.systemInstruction != null) parent.systemInstruction = contentToMldev(tContent(from.systemInstruction));
	const to = copy(from, ["temperature", "topP", "topK", "candidateCount", "maxOutputTokens", "stopSequences", "responseLogprobs", "logprobs", "presencePenalty", "frequencyPenalty", "seed", "responseMimeType"]);
	forbid(from, ["routingConfig", "modelSelectionConfig"], ENTERPRISE);
	if (from.tools != null) {
		if (!Array.isArray(from.tools)) throw new Error("tools is required and must be an array of Tools");
		parent.tools = [...from.tools].map((tool) => toolToMldev(tTool(tool)));
	}
	if (from.toolConfig != null) parent.toolConfig = toolConfigToMldev(from.toolConfig);
	forbid(from, ["labels"], ENTERPRISE);
	copy(from, ["responseModalities", "mediaResolution"], to);
	forbid(from, ["audioTimestamp"], ENTERPRISE);
	copy(from, ["thinkingConfig", "audioTranscriptionConfig", "enableEnhancedCivicAnswers"], to);
	forbid(from, ["modelArmorConfig"], ENTERPRISE);
	return to;
}
function partToVertex(from) {
	forbid(from, ["toolCall", "toolResponse"], DEVELOPER);
	const to = copy(from, ["mediaResolution", "audioTranscription", "codeExecutionResult", "executableCode", "fileData", "functionCall", "functionResponse", "inlineData", "text", "thought", "thoughtSignature", "videoMetadata"]);
	forbid(from, ["partMetadata"], DEVELOPER);
	return copy(from, ["mediaProcessing"], to);
}
function contentToVertex(from) {
	const to = {};
	if (from.parts != null) to.parts = Array.isArray(from.parts) ? from.parts.map(partToVertex) : from.parts;
	return copy(from, ["role"], to);
}
function toolToVertex(from) {
	guard(from, ["mcpServers", "computerUse"]);
	const to = copy(from, ["retrieval", "googleMaps", "codeExecution", "enterpriseWebSearch", "exaAiSearch", "functionDeclarations", "googleSearch", "googleSearchRetrieval", "parallelAiSearch", "urlContext"]);
	forbid(from, ["fileSearch"], DEVELOPER);
	return to;
}
function toolConfigToVertex(from) {
	const to = copy(from, ["functionCallingConfig", "retrievalConfig"]);
	forbid(from, ["includeServerSideToolInvocations"], DEVELOPER);
	return to;
}
function configToVertex(from, parent) {
	guard(from, ["responseSchema", "responseJsonSchema", "cachedContent", "speechConfig", "imageConfig"]);
	copy(from, ["serviceTier"], parent);
	if (from.systemInstruction != null) parent.systemInstruction = contentToVertex(tContent(from.systemInstruction));
	const to = copy(from, ["temperature", "topP", "topK", "candidateCount", "maxOutputTokens", "stopSequences", "responseLogprobs", "logprobs", "presencePenalty", "frequencyPenalty", "seed", "responseMimeType", "routingConfig"]);
	if (from.modelSelectionConfig != null) to.modelConfig = from.modelSelectionConfig;
	copy(from, ["safetySettings"], parent);
	if (from.tools != null) {
		if (!Array.isArray(from.tools)) throw new Error("tools is required and must be an array of Tools");
		parent.tools = [...from.tools].map((tool) => toolToVertex(tTool(tool)));
	}
	if (from.toolConfig != null) parent.toolConfig = toolConfigToVertex(from.toolConfig);
	copy(from, ["labels"], parent);
	copy(from, ["responseModalities", "mediaResolution", "audioTimestamp", "thinkingConfig", "audioTranscriptionConfig"], to);
	forbid(from, ["enableEnhancedCivicAnswers"], DEVELOPER);
	copy(from, ["modelArmorConfig"], parent);
	return to;
}
function responseFromVertex(from) {
	return copy(from, ["candidates", "createTime", "modelVersion", "promptFeedback", "responseId", "usageMetadata"]);
}
function toWire(params, vertex) {
	const body = {};
	const model = params.model != null ? tModel(params.model, vertex) : undefined;
	if (params.contents != null) body.contents = tContents(params.contents).map(vertex ? contentToVertex : contentToMldev);
	if (params.config != null) body.generationConfig = (vertex ? configToVertex : configToMldev)(params.config, body);
	return { model, body };
}
function candidateFromMldev(from) {
	guard(from, ["citationMetadata"]);
	return copy(from, ["content", "tokenCount", "finishReason", "avgLogprobs", "groundingMetadata", "index", "logprobsResult", "safetyRatings", "urlContextMetadata", "finishMessage"]);
}
function responseFromMldev(from) {
	const to = {};
	if (from.candidates != null) to.candidates = Array.isArray(from.candidates) ? from.candidates.map(candidateFromMldev) : from.candidates;
	return copy(from, ["modelVersion", "promptFeedback", "responseId", "usageMetadata", "modelStatus"], to);
}

async function throwErrorIfNotOK(response) {
	if (response.ok) return;
	const errorBody = response.headers.get("content-type")?.includes("application/json")
		? await response.json()
		: { error: { message: await response.text(), code: response.status, status: response.statusText } };
	const message = JSON.stringify(errorBody);
	if (response.status >= 400 && response.status < 600) throw new ApiError({ message, status: response.status });
	throw new Error(message);
}

async function* readStream(response) {
	const reader = response.body?.getReader();
	if (!reader) throw new Error("Response body is empty");
	const decoder = new TextDecoder("utf-8");
	try {
		let buffer = "";
		while (true) {
			const { done, value } = await reader.read();
			if (done) {
				if (buffer.trim().length > 0) throw new Error("Incomplete JSON segment at the end");
				break;
			}
			const chunkString = decoder.decode(value, { stream: true });
			try {
				const chunkJson = JSON.parse(chunkString);
				if ("error" in chunkJson) {
					const { status, code } = chunkJson.error;
					if (code >= 400 && code < 600) throw new ApiError({ message: "got status: " + status + ". " + JSON.stringify(chunkJson), status: code });
				}
			} catch (error) {
				if (error.name === "ApiError") throw error;
			}
			buffer += chunkString;
			while (true) {
				let index = -1;
				let length = 0;
				for (const delimiter of ["\n\n", "\r\r", "\r\n\r\n"]) {
					const at = buffer.indexOf(delimiter);
					if (at !== -1 && (index === -1 || at < index)) {
						index = at;
						length = delimiter.length;
					}
				}
				if (index === -1) break;
				const event = buffer.substring(0, index).trim();
				buffer = buffer.substring(index + length);
				if (event.startsWith("data:")) yield event.substring(5).trim();
			}
		}
	} finally {
		reader.releaseLock();
	}
}

function patchHttpOptions(base, patch) {
	const out = JSON.parse(JSON.stringify(base));
	for (const [key, value] of Object.entries(patch)) {
		if (typeof value === "object") out[key] = { ...out[key], ...value };
		else if (value !== undefined) out[key] = value;
	}
	return out;
}

function vertexFlag(options) {
	if (options.enterprise !== undefined || options.vertexai !== undefined) {
		if (options.enterprise !== undefined && options.vertexai !== undefined && options.enterprise !== options.vertexai) throw new Error("enterprise and vertexAI flags have conflicting values, please set enterprise value only.");
		return options.enterprise ?? options.vertexai;
	}
	if (env("GOOGLE_GENAI_USE_ENTERPRISE") !== undefined || env("GOOGLE_GENAI_USE_VERTEXAI") !== undefined) unmodelled("GOOGLE_GENAI_USE_ENTERPRISE/GOOGLE_GENAI_USE_VERTEXAI");
	return false;
}
function vertexCredentials(options, envApiKey) {
	const envProject = env("GOOGLE_CLOUD_PROJECT");
	const envLocation = env("GOOGLE_CLOUD_LOCATION");
	let apiKey = options.apiKey ?? envApiKey;
	let project = options.project ?? envProject;
	let location = options.location ?? envLocation;
	if (options.googleAuthOptions?.credentials) apiKey = undefined;
	if (!options.project && !options.location && (envProject || envLocation) && options.apiKey) {
		project = undefined;
		location = undefined;
	} else if ((options.project || options.location) && !options.apiKey && envApiKey) apiKey = undefined;
	else if (!options.project && !options.location && !options.apiKey && (envProject || envLocation) && envApiKey) apiKey = undefined;
	if (!location && !apiKey) location = "global";
	if (apiKey === undefined) unmodelled("GoogleAuth credentials");
	if (project !== undefined) unmodelled("Vertex project/location with an API key");
	return apiKey;
}

export class GoogleGenAI {
	constructor(options = {}) {
		const vertex = vertexFlag(options);
		if ((options.project || options.location) && !vertex) throw new Error("Project and location are not supported for Gemini API backend.");
		const envApiKey = env("GOOGLE_API_KEY") || env("GEMINI_API_KEY");
		const apiKey = vertex ? vertexCredentials(options, envApiKey) : (options.apiKey ?? envApiKey);
		if (apiKey === undefined) unmodelled("GoogleAuth credentials");
		if (apiKey.startsWith("auth_tokens/")) throw new Error("Ephemeral tokens are only supported by the live API.");
		const baseUrl = options.httpOptions?.baseUrl || env(vertex ? "GOOGLE_VERTEX_BASE_URL" : "GOOGLE_GEMINI_BASE_URL");
		const httpOptions = baseUrl ? { ...options.httpOptions, baseUrl } : options.httpOptions;
		const init = vertex
			? { baseUrl: "https://aiplatform.googleapis.com/", apiVersion: options.apiVersion ?? "v1beta1" }
			: { apiVersion: options.apiVersion ?? "v1beta", baseUrl: "https://generativelanguage.googleapis.com/" };
		init.headers = { "User-Agent": LABEL, "x-goog-api-client": LABEL, "Content-Type": "application/json" };
		const clientOptions = httpOptions ? patchHttpOptions(init, httpOptions) : init;
		this.models = {
			generateContentStream: async (params) => {
				if (params.config?.automaticFunctionCalling) unmodelled("automaticFunctionCalling");
				if ((params.config?.tools ?? []).some((tool) => typeof tool.callTool === "function")) unmodelled("CallableTool");
				const { model, body } = toWire(params, vertex);
				const http = params.config?.httpOptions ? patchHttpOptions(clientOptions, params.config.httpOptions) : clientOptions;
				if (http.timeout || http.retryOptions || http.extraBody) unmodelled("httpOptions.timeout/retryOptions/extraBody");
				const root = [http.baseUrl.endsWith("/") ? http.baseUrl.slice(0, -1) : http.baseUrl, ...(http.apiVersion ? [http.apiVersion] : [])].join("/");
				const url = new URL(root + "/" + model + ":streamGenerateContent?alt=sse");
				const headers = new Headers();
				for (const [key, value] of Object.entries(http.headers ?? {})) headers.append(key, value);
				if (headers.get("x-goog-api-key") === null) headers.append("x-goog-api-key", apiKey);
				const response = await fetch(url.toString(), { body: JSON.stringify(body), headers, method: "POST", signal: params.config?.abortSignal });
				await throwErrorIfNotOK(response);
				return (async function* () {
					for await (const data of readStream(response)) yield { ...(vertex ? responseFromVertex : responseFromMldev)(JSON.parse(data)), sdkHttpResponse: { headers: response.headers } };
				})();
			},
		};
	}
}
