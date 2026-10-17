const STUBS = {
	"@anthropic-ai/sdk": "anthropic.mjs",
	openai: "openai.mjs",
	"@google/genai": "genai.mjs",
	"@aws-sdk/client-bedrock-runtime": "bedrock.mjs",
	"@smithy/node-http-handler": "absent.mjs",
	"http-proxy-agent": "absent.mjs",
	"https-proxy-agent": "absent.mjs",
};
const stubDir = new URL("./stubs/", import.meta.url);

export async function resolve(specifier, context, next) {
	const stub = STUBS[specifier];
	if (stub) return { url: new URL(stub, stubDir).href, shortCircuit: true };
	try {
		return await next(specifier, context);
	} catch (error) {
		if (error?.code === "ERR_MODULE_NOT_FOUND" && specifier.startsWith(".") && specifier.endsWith(".js")) {
			return next(specifier.slice(0, -3) + ".ts", context);
		}
		throw error;
	}
}
