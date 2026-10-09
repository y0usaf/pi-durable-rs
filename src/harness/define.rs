use std::sync::Arc;

use crate::errors::Result;
use crate::harness::types::{PromptInput, PromptSection, Render, ToolRegistration, Wrap};

pub fn section<F>(key: &str, render: F, tag: Option<bool>) -> PromptSection
where
    F: for<'a> Fn(&PromptInput<'a>, &crate::cancel::CancelToken) -> Result<Option<String>>
        + Send
        + Sync
        + 'static,
{
    let render: Render = Arc::new(render);
    PromptSection {
        key: key.to_owned(),
        render,
        tag,
    }
}

pub fn wrap_tool<F>(tool: &str, wrap: F) -> Wrap
where
    F: Fn(ToolRegistration) -> Result<ToolRegistration> + Send + Sync + 'static,
{
    Wrap::Tool {
        tool: tool.to_owned(),
        wrap: Arc::new(wrap),
    }
}

pub fn wrap_section<F>(key: &str, wrap: F) -> Wrap
where
    F: Fn(PromptSection) -> Result<PromptSection> + Send + Sync + 'static,
{
    Wrap::Section {
        section: key.to_owned(),
        wrap: Arc::new(wrap),
    }
}
