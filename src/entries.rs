use std::marker::PhantomData;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::errors::{Error, Result};
use crate::types::{CompactionReason, EntryRecord, JsonValue, ToolDiagnostic, TypedEntry};

#[derive(Debug, Clone)]
pub struct Entry<D = ()> {
    kind: String,
    data: PhantomData<fn() -> D>,
}

impl<D> Entry<D> {
    pub fn define(kind: &str) -> Result<Self> {
        if kind.is_empty() {
            return Err(Error::Invalid(
                "Entry kind must be a non-empty string".to_owned(),
            ));
        }
        Ok(Self::named(kind))
    }

    fn named(kind: &str) -> Self {
        Self {
            kind: kind.to_owned(),
            data: PhantomData,
        }
    }

    pub fn kind(&self) -> &str {
        &self.kind
    }

    pub fn is(&self, entry: Option<&EntryRecord>) -> bool {
        entry.is_some_and(|entry| entry.kind == self.kind)
    }
}

impl<D: DeserializeOwned> Entry<D> {
    pub fn narrow(&self, mut entry: EntryRecord) -> Result<Option<TypedEntry<D>>> {
        if !self.is(Some(&entry)) {
            return Ok(None);
        }
        let data = serde_json::from_value(entry.data.take().unwrap_or(JsonValue::Null))?;
        Ok(Some(TypedEntry { entry, data }))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolResultData {
    pub diagnostics: Vec<ToolDiagnostic>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompactionData {
    pub reason: CompactionReason,
}

pub fn user_entry() -> Entry {
    Entry::named("pi.user")
}

pub fn assistant_entry() -> Entry {
    Entry::named("pi.assistant")
}

pub fn system_entry() -> Entry {
    Entry::named("pi.system")
}

pub fn tool_result_entry() -> Entry<ToolResultData> {
    Entry::named("pi.tool-result")
}

pub fn reset_entry() -> Entry {
    Entry::named("pi.reset")
}

pub fn compaction_entry() -> Entry<CompactionData> {
    Entry::named("pi.compaction")
}
