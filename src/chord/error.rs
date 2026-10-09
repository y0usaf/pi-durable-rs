use super::delta::{Path, Seg};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    UnsafePath(Seg),
    Path(Path),
    Op(String),
    Stale,
    ForeignPrepared,
    Gap { expected: u64, received: u64 },
    State(String),
    Listeners(Vec<Error>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::UnsafePath(seg) => write!(f, "unsafe path segment: {seg}"),
            Error::Path(path) => {
                let segs: Vec<String> = path
                    .iter()
                    .map(|seg| match seg {
                        Seg::Key(key) => format!("{key:?}"),
                        Seg::Index(index) => index.to_string(),
                    })
                    .collect();
                write!(f, "unresolvable path: [{}]", segs.join(","))
            }
            Error::Op(message) | Error::State(message) => f.write_str(message),
            Error::Stale => f.write_str("Prepared change is stale"),
            Error::ForeignPrepared => f.write_str("Prepared change belongs to a different tracker"),
            Error::Gap { expected, received } => write!(
                f,
                "Replicated state source cursor has a gap: expected {expected}, received {received}"
            ),
            Error::Listeners(_) => f.write_str("Replicated state listeners failed"),
        }
    }
}

impl std::error::Error for Error {}
