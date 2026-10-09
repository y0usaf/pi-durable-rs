use std::marker::PhantomData;
use std::sync::Arc;

use serde_json::json;

use crate::errors::{Error, Result};
use crate::ids::{ConversationId, DocumentId, TaskId};
use crate::types::{
    ConversationSemantics, DocumentAddress, DocumentCreate, JsonObject, JsonValue, Op, ScopeRef,
    StoredDocument,
};

pub const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocSemantics {
    Session,
    Conversation(ConversationSemantics),
    Task,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CheckpointInfo {
    pub deltas_since_base: u64,
}

pub type Initial = dyn Fn(Option<&JsonValue>) -> JsonObject + Send + Sync;
pub type Migrate = dyn Fn(JsonObject, u64) -> JsonObject + Send + Sync;
pub type CheckpointWhen = dyn Fn(&JsonObject, &[Op], CheckpointInfo) -> bool + Send + Sync;

#[derive(Clone)]
pub struct DocDefinition {
    pub kind: String,
    pub version: u64,
    pub semantics: DocSemantics,
    pub family: bool,
    pub initial: Arc<Initial>,
    pub migrate: Option<Arc<Migrate>>,
    pub checkpoint_when: Option<Arc<CheckpointWhen>>,
}

pub struct DocToken<T> {
    definition: DocDefinition,
    value: PhantomData<fn() -> T>,
}

impl<T> DocToken<T> {
    pub fn new(definition: DocDefinition) -> Result<Self> {
        validate_definition(&definition)?;
        Ok(Self {
            definition,
            value: PhantomData,
        })
    }

    pub fn definition(&self) -> &DocDefinition {
        &self.definition
    }
}

fn validate_definition(definition: &DocDefinition) -> Result<()> {
    if (1..=MAX_SAFE_INTEGER).contains(&definition.version) {
        Ok(())
    } else {
        Err(Error::Invalid(format!(
            "Document {} version must be a positive integer",
            definition.kind
        )))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedAddress {
    pub address: DocumentAddress,
    pub id: String,
}

fn owner_id(definition: &DocDefinition, owner: Option<u64>) -> Result<u64> {
    match owner {
        Some(value) if value <= MAX_SAFE_INTEGER => Ok(value),
        _ => {
            let scope = match definition.semantics {
                DocSemantics::Session => "session",
                DocSemantics::Conversation(_) => "conversation",
                DocSemantics::Task => "task",
            };
            Err(Error::Invalid(format!(
                "Document {} requires a {scope} ID",
                definition.kind
            )))
        }
    }
}

pub fn resolve_address(
    definition: &DocDefinition,
    owner: Option<u64>,
    key: Option<&str>,
) -> Result<ResolvedAddress> {
    let scope = match definition.semantics {
        DocSemantics::Session => ScopeRef::Session,
        DocSemantics::Conversation(_) => ScopeRef::Conversation {
            conversation_id: ConversationId(owner_id(definition, owner)?),
        },
        DocSemantics::Task => ScopeRef::Task {
            task_id: TaskId::new(owner_id(definition, owner)?),
        },
    };
    let key = if definition.family {
        key.map(str::to_owned)
    } else {
        None
    };
    let address = DocumentAddress {
        kind: definition.kind.clone(),
        scope,
        key,
    };
    let id = address_id(&address);
    Ok(ResolvedAddress { address, id })
}

pub fn address_id(address: &DocumentAddress) -> String {
    let (scope, owner) = match &address.scope {
        ScopeRef::Session => ("session", JsonValue::Null),
        ScopeRef::Conversation { conversation_id } => ("conversation", json!(conversation_id.0)),
        ScopeRef::Task { task_id } => ("task", json!(task_id.get())),
    };
    json!([address.kind, scope, owner, address.key]).to_string()
}

pub fn document_create(
    definition: &DocDefinition,
    address: &DocumentAddress,
    id: DocumentId,
) -> DocumentCreate {
    let semantics = match definition.semantics {
        DocSemantics::Conversation(semantics) => Some(semantics),
        DocSemantics::Session | DocSemantics::Task => None,
    };
    DocumentCreate {
        id,
        kind: address.kind.clone(),
        key: address.key.clone(),
        scope: address.scope.clone(),
        semantics,
    }
}

pub fn check_record_scope(definition: &DocDefinition, record: &DocumentCreate) -> Result<()> {
    let scope_matches = matches!(
        (definition.semantics, &record.scope),
        (DocSemantics::Session, ScopeRef::Session)
            | (DocSemantics::Conversation(_), ScopeRef::Conversation { .. })
            | (DocSemantics::Task, ScopeRef::Task { .. })
    );
    let semantics_match = match definition.semantics {
        DocSemantics::Conversation(semantics) => record.semantics == Some(semantics),
        DocSemantics::Session | DocSemantics::Task => true,
    };
    if scope_matches && semantics_match {
        Ok(())
    } else {
        Err(Error::Invalid(format!(
            "Document {} ({}) does not match the supplied definition semantics",
            record.id, record.kind
        )))
    }
}

pub fn check_record_version(
    definition: &DocDefinition,
    record: &DocumentCreate,
    version: u64,
) -> Result<()> {
    if version > definition.version {
        return Err(Error::Invalid(format!(
            "Document {} ({}) has newer version {version} than {}",
            record.id, record.kind, definition.version
        )));
    }
    if version < definition.version && definition.migrate.is_none() {
        return Err(migration_required(record, version));
    }
    Ok(())
}

fn migration_required(record: &DocumentCreate, version: u64) -> Error {
    Error::Invalid(format!(
        "Document {} ({}) requires migration from version {version}",
        record.id, record.kind
    ))
}

pub fn materialize_document(
    definition: &DocDefinition,
    stored: StoredDocument,
) -> Result<JsonObject> {
    materialize_document_value(
        definition,
        &stored.record.create,
        stored.version,
        stored.value,
    )
}

pub fn materialize_document_value(
    definition: &DocDefinition,
    record: &DocumentCreate,
    version: u64,
    value: JsonObject,
) -> Result<JsonObject> {
    check_record_scope(definition, record)?;
    check_record_version(definition, record, version)?;
    if version == definition.version {
        return Ok(value);
    }
    match &definition.migrate {
        Some(migrate) => Ok(migrate(value, version)),
        None => Err(migration_required(record, version)),
    }
}
