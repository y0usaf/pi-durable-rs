use std::fmt;

use super::event_stream::AssistantMessageEventStream;
use super::types::{
    AssistantMessage, DeferredHandle, Model, SimpleStreamOptions, TranscriptContext,
};
use crate::cancel::CancelToken;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelsError(pub String);

impl fmt::Display for ModelsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ModelsError {}

pub trait Models: Sync {
    fn get_model(&self, provider: &str, id: &str) -> Option<Model>;

    fn stream_simple(
        &self,
        model: &Model,
        context: TranscriptContext,
        options: SimpleStreamOptions,
    ) -> AssistantMessageEventStream;

    fn complete_simple(
        &self,
        model: &Model,
        context: TranscriptContext,
        options: SimpleStreamOptions,
    ) -> Result<AssistantMessage, ModelsError> {
        self.stream_simple(model, context, options)
            .result()
            .ok_or_else(|| ModelsError("stream ended without a final message".to_owned()))
    }

    fn fetch_deferred(
        &self,
        model: &Model,
        handle: &DeferredHandle,
        signal: &CancelToken,
    ) -> Result<AssistantMessage, ModelsError>;

    fn cancel_deferred(
        &self,
        model: &Model,
        handle: &DeferredHandle,
        signal: &CancelToken,
    ) -> Result<(), ModelsError>;
}
