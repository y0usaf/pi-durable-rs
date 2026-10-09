use serde_json::{Map, Value};

pub fn assign_value(current: &mut Value, value: Value) {
    match value {
        Value::Object(next) if current.is_object() => assign_object(current, next),
        Value::Array(next)
            if current
                .as_array()
                .is_some_and(|items| items.len() <= next.len()) =>
        {
            assign_array(current, next)
        }
        other => *current = other,
    }
}

fn assign_object(current: &mut Value, next: Map<String, Value>) {
    let Some(map) = current.as_object_mut() else {
        return;
    };
    let stale: Vec<String> = map
        .keys()
        .filter(|name| !next.contains_key(name.as_str()))
        .cloned()
        .collect();
    for name in stale {
        map.shift_remove(&name);
    }
    for (name, child) in next {
        match map.get_mut(&name) {
            Some(slot) => assign_value(slot, child),
            None => {
                map.insert(name, child);
            }
        }
    }
}

fn assign_array(current: &mut Value, next: Vec<Value>) {
    let Some(items) = current.as_array_mut() else {
        return;
    };
    for (index, child) in next.into_iter().enumerate() {
        match items.get_mut(index) {
            Some(slot) => assign_value(slot, child),
            None => items.push(child),
        }
    }
}
