use std::fmt;

use crate::ids::ConversationId;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    ReadAfterWrite {
        method: String,
    },
    StorageRejected {
        message: String,
        cause: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
    ConversationBusy(ConversationId),
    Invalid(String),
    Json(serde_json::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::ReadAfterWrite { method } => {
                write!(
                    f,
                    "Tx.{method}() cannot read tables after the first table write"
                )
            }
            Error::StorageRejected { message, .. } | Error::Invalid(message) => {
                f.write_str(message)
            }
            Error::ConversationBusy(id) => write!(f, "Conversation {id} is busy"),
            Error::Json(error) => fmt::Display::fmt(error, f),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::StorageRejected {
                cause: Some(cause), ..
            } => Some(cause.as_ref()),
            Error::Json(error) => Some(error),
            _ => None,
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Error::Json(error)
    }
}
