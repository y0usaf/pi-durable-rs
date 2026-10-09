use std::fmt;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

use serde::{Deserialize, Serialize};

use crate::types::JsonValue;

macro_rules! numeric_id {
    ($($name:ident),* $(,)?) => {$(
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(pub u64);

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }
    )*};
}

numeric_id!(ConversationId, EntryId, SubmissionId, DocumentId, Seq);

pub const ROOT_CONVERSATION_ID: ConversationId = ConversationId(1);

#[derive(Serialize, Deserialize)]
#[serde(transparent)]
pub struct TaskId<R = JsonValue> {
    value: u64,
    #[serde(skip)]
    result: PhantomData<fn() -> R>,
}

impl<R> TaskId<R> {
    pub const fn new(value: u64) -> Self {
        Self {
            value,
            result: PhantomData,
        }
    }

    pub const fn get(self) -> u64 {
        self.value
    }
}

impl<R> Clone for TaskId<R> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<R> Copy for TaskId<R> {}

impl<R> PartialEq for TaskId<R> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl<R> Eq for TaskId<R> {}

impl<R> PartialOrd for TaskId<R> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<R> Ord for TaskId<R> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.value.cmp(&other.value)
    }
}

impl<R> Hash for TaskId<R> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}

impl<R> fmt::Debug for TaskId<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TaskId({})", self.value)
    }
}

impl<R> fmt::Display for TaskId<R> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.value.fmt(f)
    }
}
