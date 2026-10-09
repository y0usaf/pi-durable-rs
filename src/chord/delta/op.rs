use crate::chord::error::Error;
use crate::chord::json::JsonValue;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Seg {
    Key(String),
    Index(usize),
}

impl fmt::Display for Seg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Seg::Key(key) => f.write_str(key),
            Seg::Index(index) => write!(f, "{index}"),
        }
    }
}

pub type Path = Vec<Seg>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    Replace(JsonValue),
    Set(Path, JsonValue),
    Delete(Path),
    Append(Path, String),
    Truncate(Path, usize),
    Splice(Path, usize, usize, Vec<JsonValue>),
    Permute(Path, Vec<usize>),
}

const RESERVED: [&str; 3] = ["__proto__", "constructor", "prototype"];

pub(super) fn is_reserved(key: &str) -> bool {
    RESERVED.contains(&key)
}

fn invalid(message: &str) -> Error {
    Error::Op(message.to_string())
}

fn index(value: &JsonValue) -> Option<usize> {
    value.as_u64().and_then(|n| usize::try_from(n).ok())
}

fn path_from_json(value: &JsonValue) -> Result<Path, Error> {
    let JsonValue::Array(segs) = value else {
        return Err(invalid("path is not an array"));
    };
    segs.iter()
        .map(|seg| match seg {
            JsonValue::String(key) => Ok(Seg::Key(key.clone())),
            other => index(other)
                .map(Seg::Index)
                .ok_or_else(|| invalid("path segment is not a string or index")),
        })
        .collect()
}

fn check_path(path: &[Seg]) -> Result<(), Error> {
    path.iter().try_for_each(|seg| match seg {
        Seg::Key(key) if is_reserved(key) => Err(Error::UnsafePath(seg.clone())),
        _ => Ok(()),
    })
}

fn check_permutation(permutation: &[usize]) -> Result<(), Error> {
    let mut seen = vec![false; permutation.len()];
    for &from in permutation {
        match seen.get_mut(from) {
            Some(slot) if !*slot => *slot = true,
            _ => return Err(invalid("m permutation is not a bijection")),
        }
    }
    Ok(())
}

pub(super) fn check(op: &Op) -> Result<(), Error> {
    match op {
        Op::Replace(_) => Ok(()),
        Op::Set(path, _) | Op::Delete(path) | Op::Append(path, _) | Op::Truncate(path, _) => {
            if path.is_empty() {
                return Err(invalid("path is empty"));
            }
            check_path(path)
        }
        Op::Splice(path, ..) => check_path(path),
        Op::Permute(path, permutation) => {
            check_path(path)?;
            check_permutation(permutation)
        }
    }
}

fn seg_json(seg: &Seg) -> JsonValue {
    match seg {
        Seg::Key(key) => JsonValue::String(key.clone()),
        Seg::Index(index) => JsonValue::from(*index),
    }
}

fn path_json(path: &[Seg]) -> JsonValue {
    JsonValue::Array(path.iter().map(seg_json).collect())
}

impl Op {
    pub fn from_json(value: &JsonValue) -> Result<Op, Error> {
        let Some((verb, args)) = value.as_array().and_then(|items| items.split_first()) else {
            return Err(invalid("op is not a tuple"));
        };
        let op = match (verb, args) {
            (JsonValue::String(verb), args) => match (verb.as_str(), args) {
                ("r", [value]) => Op::Replace(value.clone()),
                ("r", _) => return Err(invalid("r arity")),
                ("s", [path, value]) => Op::Set(path_from_json(path)?, value.clone()),
                ("s", _) => return Err(invalid("s arity")),
                ("d", [path]) => Op::Delete(path_from_json(path)?),
                ("d", _) => return Err(invalid("d arity")),
                ("a", [path, JsonValue::String(text)]) => {
                    Op::Append(path_from_json(path)?, text.clone())
                }
                ("a", _) => return Err(invalid("a shape")),
                ("t", [path, count]) => {
                    let path = path_from_json(path)?;
                    let count = index(count).ok_or_else(|| invalid("t shape"))?;
                    Op::Truncate(path, count)
                }
                ("t", _) => return Err(invalid("t shape")),
                ("p", [path, at, remove, items]) => {
                    let path = path_from_json(path)?;
                    let at = index(at).ok_or_else(|| invalid("p index"))?;
                    let remove = index(remove).ok_or_else(|| invalid("p remove"))?;
                    let JsonValue::Array(items) = items else {
                        return Err(invalid("p items"));
                    };
                    Op::Splice(path, at, remove, items.clone())
                }
                ("p", _) => return Err(invalid("p arity")),
                ("m", [path, permutation]) => {
                    let path = path_from_json(path)?;
                    let JsonValue::Array(items) = permutation else {
                        return Err(invalid("m permutation is not an array"));
                    };
                    let permutation = items
                        .iter()
                        .map(index)
                        .collect::<Option<Vec<usize>>>()
                        .ok_or_else(|| invalid("m permutation is not a bijection"))?;
                    Op::Permute(path, permutation)
                }
                ("m", _) => return Err(invalid("m arity")),
                _ => return Err(invalid(&format!("unknown op verb: {verb}"))),
            },
            (other, _) => return Err(invalid(&format!("unknown op verb: {other}"))),
        };
        check(&op)?;
        Ok(op)
    }

    pub fn to_json(&self) -> JsonValue {
        let s = |text: &str| JsonValue::String(text.to_string());
        let items = match self {
            Op::Replace(value) => vec![s("r"), value.clone()],
            Op::Set(path, value) => vec![s("s"), path_json(path), value.clone()],
            Op::Delete(path) => vec![s("d"), path_json(path)],
            Op::Append(path, text) => vec![s("a"), path_json(path), s(text)],
            Op::Truncate(path, count) => vec![s("t"), path_json(path), JsonValue::from(*count)],
            Op::Splice(path, at, remove, items) => vec![
                s("p"),
                path_json(path),
                JsonValue::from(*at),
                JsonValue::from(*remove),
                JsonValue::Array(items.clone()),
            ],
            Op::Permute(path, permutation) => vec![
                s("m"),
                path_json(path),
                JsonValue::Array(
                    permutation
                        .iter()
                        .map(|&from| JsonValue::from(from))
                        .collect(),
                ),
            ],
        };
        JsonValue::Array(items)
    }
}

impl Serialize for Op {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.to_json().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Op {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = JsonValue::deserialize(deserializer)?;
        Op::from_json(&value).map_err(serde::de::Error::custom)
    }
}
