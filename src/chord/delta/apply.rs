use super::op::{Op, Path, Seg, check};
use crate::chord::error::Error;
use crate::chord::json::JsonValue;

pub fn apply(target: Option<JsonValue>, ops: &[Op]) -> Result<JsonValue, Error> {
    let mut root = target;
    for op in ops {
        check(op)?;
        match op {
            Op::Replace(value) => root = Some(value.clone()),
            Op::Splice(path, at, remove, items) => {
                let JsonValue::Array(vec) = resolve(&mut root, path)? else {
                    return Err(Error::Path(path.clone()));
                };
                let start = (*at).min(vec.len());
                let end = start.saturating_add(*remove).min(vec.len());
                vec.splice(start..end, items.iter().cloned());
            }
            Op::Permute(path, permutation) => {
                let JsonValue::Array(vec) = resolve(&mut root, path)? else {
                    return Err(Error::Path(path.clone()));
                };
                if vec.len() != permutation.len() {
                    return Err(Error::Path(path.clone()));
                }
                let previous = vec.clone();
                for (slot, &from) in vec.iter_mut().zip(permutation) {
                    *slot = previous[from].clone();
                }
            }
            Op::Set(path, value) => {
                let (parent, key) = split(&mut root, path)?;
                put(parent, key, path, value.clone())?;
            }
            Op::Delete(path) => {
                let (parent, key) = split(&mut root, path)?;
                match parent {
                    JsonValue::Array(vec) => {
                        let Seg::Index(index) = key else {
                            return Err(Error::UnsafePath(key.clone()));
                        };
                        if *index >= vec.len() {
                            return Err(Error::Path(path.clone()));
                        }
                        vec.remove(*index);
                    }
                    JsonValue::Object(map) => {
                        map.shift_remove(&key.to_string());
                    }
                    _ => return Err(Error::Path(path.clone())),
                }
            }
            Op::Append(path, text) => {
                let (parent, key) = split(&mut root, path)?;
                let next = format!("{}{text}", read_text(parent, key, path)?);
                put(parent, key, path, JsonValue::String(next))?;
            }
            Op::Truncate(path, count) => {
                let (parent, key) = split(&mut root, path)?;
                let units: Vec<u16> = read_text(parent, key, path)?
                    .encode_utf16()
                    .skip(*count)
                    .collect();
                put(
                    parent,
                    key,
                    path,
                    JsonValue::String(String::from_utf16_lossy(&units)),
                )?;
            }
        }
    }
    root.ok_or_else(|| Error::Path(Vec::new()))
}

fn resolve<'a>(root: &'a mut Option<JsonValue>, path: &[Seg]) -> Result<&'a mut JsonValue, Error> {
    let mut node = root.as_mut().ok_or_else(|| Error::Path(path.to_vec()))?;
    for seg in path {
        node = child(node, seg, path)?;
    }
    match node {
        JsonValue::Array(_) | JsonValue::Object(_) => Ok(node),
        _ => Err(Error::Path(path.to_vec())),
    }
}

fn child<'a>(node: &'a mut JsonValue, seg: &Seg, path: &[Seg]) -> Result<&'a mut JsonValue, Error> {
    match node {
        JsonValue::Object(map) => map
            .get_mut(&seg.to_string())
            .ok_or_else(|| Error::Path(path.to_vec())),
        JsonValue::Array(vec) => match seg {
            Seg::Index(index) => vec
                .get_mut(*index)
                .ok_or_else(|| Error::Path(path.to_vec())),
            Seg::Key(_) => Err(Error::UnsafePath(seg.clone())),
        },
        _ => Err(Error::Path(path.to_vec())),
    }
}

fn split<'a, 'p>(
    root: &'a mut Option<JsonValue>,
    path: &'p Path,
) -> Result<(&'a mut JsonValue, &'p Seg), Error> {
    let (key, parent_path) = path.split_last().ok_or_else(|| Error::Path(Vec::new()))?;
    Ok((resolve(root, parent_path)?, key))
}

fn read_text(parent: &JsonValue, key: &Seg, path: &Path) -> Result<String, Error> {
    let current = match (parent, key) {
        (JsonValue::Object(map), key) => map.get(&key.to_string()),
        (JsonValue::Array(vec), Seg::Index(index)) => vec.get(*index),
        (JsonValue::Array(_), Seg::Key(_)) => return Err(Error::UnsafePath(key.clone())),
        _ => None,
    };
    match current {
        Some(JsonValue::String(text)) => Ok(text.clone()),
        _ => Err(Error::Path(path.clone())),
    }
}

fn put(parent: &mut JsonValue, key: &Seg, path: &Path, value: JsonValue) -> Result<(), Error> {
    match parent {
        JsonValue::Object(map) => {
            map.insert(key.to_string(), value);
            Ok(())
        }
        JsonValue::Array(vec) => {
            let Seg::Index(index) = key else {
                return Err(Error::UnsafePath(key.clone()));
            };
            match (*index).cmp(&vec.len()) {
                std::cmp::Ordering::Less => vec[*index] = value,
                std::cmp::Ordering::Equal => vec.push(value),
                std::cmp::Ordering::Greater => return Err(Error::UnsafePath(key.clone())),
            }
            Ok(())
        }
        _ => Err(Error::Path(path.clone())),
    }
}
