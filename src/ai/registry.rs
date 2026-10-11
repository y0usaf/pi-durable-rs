use std::collections::BTreeMap;
use std::sync::Arc;

use super::event_stream::AssistantMessageEventStream;
use super::models::{Models, ModelsError};
use super::stream::error_stream;
use super::types::{
    AssistantMessage, DeferredHandle, Model, SimpleStreamOptions, TranscriptContext,
};
use crate::cancel::CancelToken;

pub trait Api: Send + Sync {
    fn stream_simple(
        &self,
        model: &Model,
        context: TranscriptContext,
        options: SimpleStreamOptions,
    ) -> AssistantMessageEventStream;
}

#[derive(Debug, Clone, Default)]
pub struct Auth {
    pub api_key: Option<String>,
    pub headers: BTreeMap<String, String>,
    pub base_url: Option<String>,
    pub env: BTreeMap<String, String>,
}

pub type AuthResolver = Arc<dyn Fn(&Model) -> Result<Auth, String> + Send + Sync>;

#[derive(Default)]
pub struct Providers {
    models: Vec<Model>,
    apis: BTreeMap<String, Arc<dyn Api>>,
    auth: Option<AuthResolver>,
}

impl Providers {
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(any(feature = "anthropic", feature = "openai", feature = "vercel-gateway"))]
    pub fn builtin() -> Self {
        let mut providers = Self::new();
        #[cfg(feature = "anthropic")]
        providers.add_api("anthropic-messages", super::api::anthropic::api());
        #[cfg(feature = "openai")]
        {
            providers.add_api("openai-completions", super::api::openai_completions::api());
            providers.add_api("openai-responses", super::api::openai_responses::api());
            providers.add_api(
                "openai-codex-responses",
                super::api::openai_responses::api(),
            );
            providers.add_api("mistral-conversations", super::api::mistral::api());
            providers.add_api("azure-openai-responses", super::api::azure::api());
            providers.add_api("pi-messages", super::api::pi_messages::api());
        }
        #[cfg(feature = "http")]
        providers.add_api("bedrock-converse-stream", super::api::bedrock::api());
        #[cfg(feature = "http")]
        providers.add_api("google-generative-ai", super::api::google::api());
        #[cfg(feature = "http")]
        providers.add_api("google-vertex", super::api::google::vertex_api());
        providers.add_models(super::catalog::all());
        providers
    }

    pub fn add_api(&mut self, name: &str, api: Arc<dyn Api>) -> &mut Self {
        self.apis.insert(name.to_owned(), api);
        self
    }

    pub fn add_models(&mut self, models: impl IntoIterator<Item = Model>) -> &mut Self {
        for model in models {
            match self
                .models
                .iter_mut()
                .find(|known| known.provider == model.provider && known.id == model.id)
            {
                Some(slot) => *slot = model,
                None => self.models.push(model),
            }
        }
        self
    }

    pub fn set_auth(&mut self, resolver: AuthResolver) -> &mut Self {
        self.auth = Some(resolver);
        self
    }

    pub fn models(&self) -> &[Model] {
        &self.models
    }

    fn resolve(
        &self,
        model: &Model,
        mut options: SimpleStreamOptions,
    ) -> Result<SimpleStreamOptions, String> {
        let mut env = BTreeMap::new();
        if let Some(resolver) = &self.auth {
            let auth = resolver(model)?;
            env = auth.env;
            if options.api_key.is_none() {
                options.api_key = auth.api_key;
            }
            if options.base_url.is_none() {
                options.base_url = auth.base_url;
            }
            if !auth.headers.is_empty() {
                let mut headers = auth.headers;
                headers.extend(options.headers.take().unwrap_or_default());
                options.headers = Some(headers);
            }
        }
        if options.api_key.is_none() {
            options.api_key = super::auth::env_api_key(&model.provider, &env);
        }
        Ok(options)
    }
}

impl Models for Providers {
    fn get_model(&self, provider: &str, id: &str) -> Option<Model> {
        self.models
            .iter()
            .find(|model| model.provider == provider && model.id == id)
            .cloned()
    }

    fn stream_simple(
        &self,
        model: &Model,
        context: TranscriptContext,
        options: SimpleStreamOptions,
    ) -> AssistantMessageEventStream {
        let Some(api) = self.apis.get(&model.api) else {
            return error_stream(
                model,
                format!("No API provider registered for api: {}", model.api),
            );
        };
        match self.resolve(model, options) {
            Ok(options) => api.stream_simple(model, context, options),
            Err(message) => error_stream(model, message),
        }
    }

    fn fetch_deferred(
        &self,
        model: &Model,
        _handle: &DeferredHandle,
        _signal: &CancelToken,
    ) -> Result<AssistantMessage, ModelsError> {
        Err(ModelsError(format!(
            "{}/{} does not support deferred responses",
            model.provider, model.id
        )))
    }

    fn cancel_deferred(
        &self,
        model: &Model,
        _handle: &DeferredHandle,
        _signal: &CancelToken,
    ) -> Result<(), ModelsError> {
        Err(ModelsError(format!(
            "{}/{} does not support deferred responses",
            model.provider, model.id
        )))
    }
}
