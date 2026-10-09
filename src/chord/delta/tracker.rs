use super::op::{Op, Path, Seg, check};
use crate::chord::error::Error;
use crate::chord::json::JsonValue;
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_OWNER: AtomicU64 = AtomicU64::new(1);

pub struct Tracker {
    owner: u64,
    value: JsonValue,
    revision: u64,
}

pub struct Change {
    owner: u64,
    base_revision: u64,
    base: JsonValue,
    draft: JsonValue,
}

pub struct Prepared {
    owner: u64,
    base_revision: u64,
    base: JsonValue,
    value: JsonValue,
    ops: Vec<Op>,
}

pub fn track(initial: JsonValue) -> Tracker {
    Tracker {
        owner: NEXT_OWNER.fetch_add(1, Ordering::Relaxed),
        value: initial,
        revision: 0,
    }
}

impl Tracker {
    pub fn value(&self) -> &JsonValue {
        &self.value
    }

    pub fn revision(&self) -> u64 {
        self.revision
    }

    pub fn begin_change(&self) -> Change {
        Change {
            owner: self.owner,
            base_revision: self.revision,
            base: self.value.clone(),
            draft: self.value.clone(),
        }
    }

    pub fn prepare_replace(&self, value: JsonValue) -> Result<Prepared, Error> {
        let ops = if equal_trusted(&self.value, &value) {
            Vec::new()
        } else {
            let op = Op::Replace(value.clone());
            check(&op)?;
            vec![op]
        };
        Ok(Prepared {
            owner: self.owner,
            base_revision: self.revision,
            base: self.value.clone(),
            value,
            ops,
        })
    }

    pub fn adopt(&mut self, prepared: Prepared) -> Result<(), Error> {
        if prepared.owner != self.owner {
            return Err(Error::ForeignPrepared);
        }
        if prepared.base_revision != self.revision {
            return Err(Error::Stale);
        }
        self.value = prepared.value;
        self.revision += 1;
        Ok(())
    }
}

impl Change {
    pub fn state(&mut self) -> &mut JsonValue {
        &mut self.draft
    }

    pub fn prepare(self, tracker: &Tracker) -> Result<Prepared, Error> {
        if self.owner != tracker.owner {
            return Err(Error::ForeignPrepared);
        }
        let mut ops = Vec::new();
        diff(&mut Vec::new(), &self.base, &self.draft, &mut ops);
        ops.iter().try_for_each(check)?;
        Ok(Prepared {
            owner: self.owner,
            base_revision: self.base_revision,
            base: self.base,
            value: self.draft,
            ops,
        })
    }
}

impl Prepared {
    pub fn value(&self) -> &JsonValue {
        &self.value
    }

    pub fn base(&self) -> &JsonValue {
        &self.base
    }

    pub fn ops(&self) -> &[Op] {
        &self.ops
    }

    pub fn base_revision(&self) -> u64 {
        self.base_revision
    }
}

fn equal_trusted(a: &JsonValue, b: &JsonValue) -> bool {
    match (a, b) {
        (JsonValue::Array(x), JsonValue::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(p, q)| equal_trusted(p, q))
        }
        (JsonValue::Object(x), JsonValue::Object(y)) => {
            x.len() == y.len()
                && x.iter()
                    .all(|(k, v)| y.get(k).is_some_and(|w| equal_trusted(v, w)))
        }
        _ => a == b,
    }
}

fn same(a: &JsonValue, b: &JsonValue) -> bool {
    match (a, b) {
        (JsonValue::Array(x), JsonValue::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(p, q)| same(p, q))
        }
        (JsonValue::Object(x), JsonValue::Object(y)) => {
            x.len() == y.len()
                && x.iter()
                    .zip(y.iter())
                    .all(|((k1, v1), (k2, v2))| k1 == k2 && same(v1, v2))
        }
        _ => a == b,
    }
}

fn leaf(path: &Path, value: &JsonValue, ops: &mut Vec<Op>) {
    if path.is_empty() {
        ops.push(Op::Replace(value.clone()));
    } else {
        ops.push(Op::Set(path.clone(), value.clone()));
    }
}

fn diff(path: &mut Path, base: &JsonValue, next: &JsonValue, ops: &mut Vec<Op>) {
    if same(base, next) {
        return;
    }
    match (base, next) {
        (JsonValue::Object(old), JsonValue::Object(new)) => {
            let retained: Vec<&String> = old.keys().filter(|key| new.contains_key(*key)).collect();
            let added: Vec<&String> = new.keys().filter(|key| !old.contains_key(*key)).collect();
            let expected = retained.iter().chain(added.iter()).copied();
            if !expected.eq(new.keys()) {
                leaf(path, next, ops);
                return;
            }
            for key in old.keys().filter(|key| !new.contains_key(*key)) {
                let mut child = path.clone();
                child.push(Seg::Key(key.clone()));
                ops.push(Op::Delete(child));
            }
            for key in retained {
                path.push(Seg::Key(key.clone()));
                diff(path, &old[key], &new[key], ops);
                path.pop();
            }
            for key in added {
                let mut child = path.clone();
                child.push(Seg::Key(key.clone()));
                ops.push(Op::Set(child, new[key].clone()));
            }
        }
        (JsonValue::Array(old), JsonValue::Array(new)) if old.len() == new.len() => {
            for (index, (before, after)) in old.iter().zip(new).enumerate() {
                path.push(Seg::Index(index));
                diff(path, before, after, ops);
                path.pop();
            }
        }
        (JsonValue::Array(old), JsonValue::Array(new)) => {
            let prefix = old.iter().zip(new).take_while(|(a, b)| same(a, b)).count();
            let limit = old.len().min(new.len()) - prefix;
            let suffix = (0..limit)
                .take_while(|&k| same(&old[old.len() - 1 - k], &new[new.len() - 1 - k]))
                .count();
            ops.push(Op::Splice(
                path.clone(),
                prefix,
                old.len() - prefix - suffix,
                new[prefix..new.len() - suffix].to_vec(),
            ));
        }
        (JsonValue::String(old), JsonValue::String(new))
            if !path.is_empty() && new.starts_with(old.as_str()) =>
        {
            ops.push(Op::Append(path.clone(), new[old.len()..].to_string()));
        }
        (JsonValue::String(old), JsonValue::String(new))
            if !path.is_empty() && old.ends_with(new.as_str()) =>
        {
            let removed = old.encode_utf16().count() - new.encode_utf16().count();
            ops.push(Op::Truncate(path.clone(), removed));
        }
        _ => leaf(path, next, ops),
    }
}
