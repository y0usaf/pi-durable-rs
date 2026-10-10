use std::sync::Arc;

use crate::ai::event_stream::AssistantMessageEventStream;
use crate::ai::registry::Api;
use crate::ai::stream::error_stream;
use crate::ai::types::{Model, SimpleStreamOptions, TranscriptContext};

struct Unimplemented;

impl Api for Unimplemented {
    fn stream_simple(
        &self,
        model: &Model,
        _context: TranscriptContext,
        _options: SimpleStreamOptions,
    ) -> AssistantMessageEventStream {
        error_stream(model, format!("{} is not implemented yet", model.api))
    }
}

pub fn api() -> Arc<dyn Api> {
    Arc::new(Unimplemented)
}
