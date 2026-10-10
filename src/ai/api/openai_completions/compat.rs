use crate::ai::types::Model;
use crate::chord::json::{JsonObject, JsonValue};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThinkingFormat {
    OpenAi,
    OpenRouter,
    DeepSeek,
    Together,
    Zai,
    Qwen,
    QwenChatTemplate,
    StringThinking,
    AntLing,
}

impl ThinkingFormat {
    fn parse(name: &str) -> Result<Self, String> {
        Ok(match name {
            "openai" => Self::OpenAi,
            "openrouter" => Self::OpenRouter,
            "deepseek" => Self::DeepSeek,
            "together" => Self::Together,
            "zai" => Self::Zai,
            "qwen" => Self::Qwen,
            "qwen-chat-template" => Self::QwenChatTemplate,
            "string-thinking" => Self::StringThinking,
            "ant-ling" => Self::AntLing,
            other => {
                return Err(format!(
                    "compat.thinkingFormat `{other}` is not supported by openai-completions"
                ));
            }
        })
    }
}

#[derive(Debug, Clone)]
pub struct Compat {
    pub supports_store: bool,
    pub supports_developer_role: bool,
    pub supports_reasoning_effort: bool,
    pub supports_usage_in_streaming: bool,
    pub supports_finish_reason: bool,
    pub supports_strict_mode: bool,
    pub legacy_max_tokens: bool,
    pub requires_tool_result_name: bool,
    pub requires_assistant_after_tool_result: bool,
    pub requires_thinking_as_text: bool,
    pub requires_reasoning_content: bool,
    pub thinking_format: ThinkingFormat,
    pub open_router_routing: Option<JsonValue>,
    pub vercel_gateway_routing: Option<JsonObject>,
    pub zai_tool_stream: bool,
    pub thinking_token_budget_field: Option<String>,
    pub mid_convo_system_messages: bool,
    pub mid_convo_tool_additions: bool,
    pub long_cache_retention: bool,
    pub vllm_priority: Option<JsonValue>,
}

impl Compat {
    pub fn resolve(model: &Model, base_url: &str) -> Result<Self, String> {
        let mut compat = Self::detect(&model.provider, base_url, &model.id);
        match &model.compat {
            None | Some(JsonValue::Null) => {}
            Some(JsonValue::Object(explicit)) => {
                for (key, value) in explicit {
                    compat.apply(key, value)?;
                }
            }
            Some(_) => return Err("model compat must be an object".to_owned()),
        }
        Ok(compat)
    }

    fn detect(provider: &str, url: &str, id: &str) -> Self {
        let is_zai = matches!(provider, "zai" | "zai-coding-cn")
            || url.contains("api.z.ai")
            || url.contains("open.bigmodel.cn");
        let is_together = provider == "together"
            || url.contains("api.together.ai")
            || url.contains("api.together.xyz");
        let is_moonshot =
            matches!(provider, "moonshotai" | "moonshotai-cn") || url.contains("api.moonshot.");
        let is_openrouter = provider == "openrouter" || url.contains("openrouter.ai");
        let is_cf_workers =
            provider == "cloudflare-workers-ai" || url.contains("api.cloudflare.com");
        let is_cf_gateway =
            provider == "cloudflare-ai-gateway" || url.contains("gateway.ai.cloudflare.com");
        let is_nvidia = provider == "nvidia" || url.contains("integrate.api.nvidia.com");
        let is_ant_ling = provider == "ant-ling" || url.contains("api.ant-ling.com");
        let is_cerebras = provider == "cerebras" || url.contains("cerebras.ai");
        let is_deepseek = provider == "deepseek" || url.to_lowercase().contains("deepseek.com");
        let is_grok = provider == "xai" || url.contains("api.x.ai");
        let non_standard = is_nvidia
            || is_cerebras
            || is_grok
            || is_together
            || url.contains("chutes.ai")
            || is_deepseek
            || is_zai
            || is_moonshot
            || provider == "opencode"
            || url.contains("opencode.ai")
            || is_cf_workers
            || is_cf_gateway
            || is_ant_ling;
        let legacy_max_tokens = url.contains("chutes.ai")
            || is_deepseek
            || is_moonshot
            || is_cf_gateway
            || is_together
            || is_nvidia
            || is_ant_ling
            || is_zai;
        let openrouter_developer_role =
            is_openrouter && (id.starts_with("anthropic/") || id.starts_with("openai/"));
        let thinking_format = if is_deepseek {
            ThinkingFormat::DeepSeek
        } else if is_zai {
            ThinkingFormat::Zai
        } else if is_together {
            ThinkingFormat::Together
        } else if is_ant_ling {
            ThinkingFormat::AntLing
        } else if is_openrouter {
            ThinkingFormat::OpenRouter
        } else {
            ThinkingFormat::OpenAi
        };
        Self {
            supports_store: !non_standard,
            supports_developer_role: openrouter_developer_role || (!non_standard && !is_openrouter),
            supports_reasoning_effort: !(is_grok
                || is_zai
                || is_moonshot
                || is_together
                || is_cf_gateway
                || is_nvidia
                || is_ant_ling),
            supports_usage_in_streaming: true,
            supports_finish_reason: true,
            supports_strict_mode: true,
            legacy_max_tokens,
            requires_tool_result_name: false,
            requires_assistant_after_tool_result: false,
            requires_thinking_as_text: false,
            requires_reasoning_content: is_deepseek,
            thinking_format,
            open_router_routing: None,
            vercel_gateway_routing: None,
            zai_tool_stream: false,
            thinking_token_budget_field: None,
            mid_convo_system_messages: false,
            mid_convo_tool_additions: false,
            long_cache_retention: !(is_together
                || is_cf_workers
                || is_cf_gateway
                || is_nvidia
                || is_ant_ling),
            vllm_priority: None,
        }
    }

    fn apply(&mut self, key: &str, value: &JsonValue) -> Result<(), String> {
        let expected = |kind: &str| format!("compat.{key} must be {kind}");
        let flag = || value.as_bool().ok_or_else(|| expected("a boolean"));
        let text = || value.as_str().ok_or_else(|| expected("a string"));
        match key {
            "supportsStore" => self.supports_store = flag()?,
            "supportsDeveloperRole" => self.supports_developer_role = flag()?,
            "supportsReasoningEffort" => self.supports_reasoning_effort = flag()?,
            "supportsUsageInStreaming" => self.supports_usage_in_streaming = flag()?,
            "supportsFinishReason" => self.supports_finish_reason = flag()?,
            "supportsStrictMode" => self.supports_strict_mode = flag()?,
            "requiresToolResultName" => self.requires_tool_result_name = flag()?,
            "requiresAssistantAfterToolResult" => {
                self.requires_assistant_after_tool_result = flag()?;
            }
            "requiresThinkingAsText" => self.requires_thinking_as_text = flag()?,
            "requiresReasoningContentOnAssistantMessages" => {
                self.requires_reasoning_content = flag()?;
            }
            "zaiToolStream" => self.zai_tool_stream = flag()?,
            "supportsMidConvoSystemMessages" => self.mid_convo_system_messages = flag()?,
            "supportsMidConvoToolAdditions" => self.mid_convo_tool_additions = flag()?,
            "supportsLongCacheRetention" => self.long_cache_retention = flag()?,
            "maxTokensField" => {
                self.legacy_max_tokens = match text()? {
                    "max_tokens" => true,
                    "max_completion_tokens" => false,
                    _ => return Err(expected("max_tokens or max_completion_tokens")),
                };
            }
            "thinkingFormat" => self.thinking_format = ThinkingFormat::parse(text()?)?,
            "thinkingTokenBudgetField" => {
                self.thinking_token_budget_field = Some(match text()? {
                    name @ ("thinking_token_budget"
                    | "thinking_budget"
                    | "thinking_budget_tokens") => name.to_owned(),
                    _ => {
                        return Err(expected(
                            "thinking_token_budget, thinking_budget or thinking_budget_tokens",
                        ));
                    }
                });
            }
            "openRouterRouting" => {
                self.open_router_routing = Some(JsonValue::Object(
                    value
                        .as_object()
                        .cloned()
                        .ok_or_else(|| expected("an object"))?,
                ));
            }
            "vercelGatewayRouting" => {
                self.vercel_gateway_routing = Some(
                    value
                        .as_object()
                        .cloned()
                        .ok_or_else(|| expected("an object"))?,
                );
            }
            "vllmPriority" => {
                if !value.is_number() {
                    return Err(expected("a number"));
                }
                self.vllm_priority = Some(value.clone());
            }
            other => {
                return Err(format!(
                    "compat.{other} is not supported by openai-completions"
                ));
            }
        }
        Ok(())
    }
}
