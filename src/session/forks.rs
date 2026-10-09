use std::collections::HashSet;

use crate::cancel::CancelToken;
use crate::documents::address_id;
use crate::errors::{Error, Result};
use crate::ids::{ConversationId, DocumentId, EntryId};
use crate::types::{
    ConversationSemantics, Cursor, DocumentAddress, DocumentCopySource, DocumentCreate,
    DocumentPoint, DocumentQuery, DocumentRecord, LatestFork, RewindableFork, ScopeRef, Storage,
};

const SCAN_PAGE_SIZE: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForkDocumentCopy {
    pub record: DocumentCreate,
    pub source: DocumentCopySource,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ForkPolicy {
    AsOf,
    Current,
}

pub fn prepare_fork_document_copies(
    storage: &dyn Storage,
    parent: ConversationId,
    at: EntryId,
    child: ConversationId,
    cancel: &CancelToken,
) -> Result<Vec<ForkDocumentCopy>> {
    let entry = storage.visible_entry(parent, at, cancel)?.ok_or_else(|| {
        Error::Invalid(format!(
            "Entry {at} is not visible from conversation {parent}"
        ))
    })?;
    let mut collector = Collector {
        storage,
        child,
        cancel,
        copies: Vec::new(),
        copied: HashSet::new(),
    };
    collector.collect(
        entry.entry.conversation_id,
        DocumentPoint::At(entry.commit_seq),
        ForkPolicy::AsOf,
    )?;
    collector.collect(parent, DocumentPoint::Current, ForkPolicy::Current)?;
    Ok(collector.copies)
}

struct Collector<'a> {
    storage: &'a dyn Storage,
    child: ConversationId,
    cancel: &'a CancelToken,
    copies: Vec<ForkDocumentCopy>,
    copied: HashSet<String>,
}

impl Collector<'_> {
    fn collect(
        &mut self,
        scope: ConversationId,
        at: DocumentPoint,
        policy: ForkPolicy,
    ) -> Result<()> {
        let mut cursor: Option<Cursor> = None;
        loop {
            let query = DocumentQuery {
                scope: ScopeRef::Conversation {
                    conversation_id: scope,
                },
                at,
                kind: None,
            };
            let page = self.storage.scan_documents(
                &query,
                SCAN_PAGE_SIZE,
                cursor.as_ref(),
                self.cancel,
            )?;
            for source in page.items {
                let Some(semantics) = copy_policy(&source, policy) else {
                    continue;
                };
                let record = DocumentCreate {
                    id: DocumentId(self.storage.mint_id()?),
                    kind: source.create.kind.clone(),
                    key: source.create.key.clone(),
                    scope: ScopeRef::Conversation {
                        conversation_id: self.child,
                    },
                    semantics: Some(semantics),
                };
                let copy_address = address_id(&DocumentAddress {
                    kind: record.kind.clone(),
                    scope: record.scope.clone(),
                    key: record.key.clone(),
                });
                if !self.copied.insert(copy_address) {
                    let member = match &record.key {
                        None => record.kind.clone(),
                        Some(key) => format!("{}/{key}", record.kind),
                    };
                    return Err(Error::Invalid(format!(
                        "Fork selects multiple source documents for {member}"
                    )));
                }
                self.copies.push(ForkDocumentCopy {
                    record,
                    source: DocumentCopySource {
                        id: source.create.id,
                        at,
                    },
                });
            }
            match page.next {
                Some(next) => cursor = Some(next),
                None => return Ok(()),
            }
        }
    }
}

fn copy_policy(source: &DocumentRecord, policy: ForkPolicy) -> Option<ConversationSemantics> {
    if !matches!(source.create.scope, ScopeRef::Conversation { .. }) {
        return None;
    }
    let semantics = source.create.semantics?;
    let selected = match policy {
        ForkPolicy::AsOf => matches!(
            semantics,
            ConversationSemantics::Rewindable {
                fork: RewindableFork::AsOf
            }
        ),
        ForkPolicy::Current => matches!(
            semantics,
            ConversationSemantics::Latest {
                fork: LatestFork::Current
            } | ConversationSemantics::Rewindable {
                fork: RewindableFork::Current
            }
        ),
    };
    selected.then_some(semantics)
}
