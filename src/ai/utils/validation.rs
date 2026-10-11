use std::fmt;

use serde_json::{Map, Number, Value};

use super::js;
use crate::ai::types::{Tool, ToolCall};

const MAX_ERRORS: usize = 8;
const ANNOTATIONS: [&str; 5] = ["title", "description", "default", "examples", "$comment"];
const TYPES: [&str; 7] = [
    "object", "array", "string", "number", "integer", "boolean", "null",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationError(pub String);

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ValidationError {}

pub fn validate_tool_arguments(tool: &Tool, call: &ToolCall) -> Result<Value, ValidationError> {
    let schema = &tool.parameters;
    check_schema(schema).map_err(ValidationError)?;
    let mut args = canonical(&Value::Object(call.arguments.clone()));
    normalize_optional_nulls(&mut args, schema);
    let args = coerce(args, schema);
    if check(schema, &args) {
        return Ok(args);
    }
    let mut ctx = Ctx::default();
    errors(schema, &args, "", &mut ctx);
    let listed = ctx
        .issues
        .iter()
        .map(|issue| format!("  - {}: {}", format_path(issue), issue.message))
        .collect::<Vec<_>>()
        .join("\n");
    let listed = if listed.is_empty() {
        "Unknown validation error".to_owned()
    } else {
        listed
    };
    Err(ValidationError(format!(
        "Validation failed for tool \"{}\":\n{listed}\n\nReceived arguments:\n{}",
        call.name,
        js::stringify_pretty(&Value::Object(call.arguments.clone()))
    )))
}

fn canonical(value: &Value) -> Value {
    match value {
        Value::Number(number) => number_value(number.as_f64().unwrap_or(f64::NAN)),
        Value::Array(items) => Value::Array(items.iter().map(canonical).collect()),
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(key, value)| (key.clone(), canonical(value)))
                .collect(),
        ),
        other => other.clone(),
    }
}

fn number_value(value: f64) -> Value {
    if value.fract() == 0.0 && value.abs() < 9_007_199_254_740_992.0 {
        Value::from(value as i64)
    } else {
        Number::from_f64(value).map_or(Value::Null, Value::Number)
    }
}

fn check_schema(schema: &Value) -> Result<(), String> {
    let map = match schema {
        Value::Bool(_) => return Ok(()),
        Value::Object(map) => map,
        _ => return Err("schema must be an object or a boolean".to_owned()),
    };
    for (key, value) in map {
        match key.as_str() {
            name if ANNOTATIONS.contains(&name) => {}
            "type" => check_type_keyword(value)?,
            "required" => {
                if !value
                    .as_array()
                    .is_some_and(|names| names.iter().all(Value::is_string))
                {
                    return Err("\"required\" must be an array of strings".to_owned());
                }
            }
            "properties" => {
                let props = value
                    .as_object()
                    .ok_or("\"properties\" must be an object")?;
                for sub in props.values() {
                    check_schema(sub)?;
                }
            }
            "minItems" => {
                if !value.is_u64() {
                    return Err("\"minItems\" must be a non-negative integer".to_owned());
                }
            }
            "additionalProperties" => check_schema(value)?,
            "items" => match value {
                Value::Array(tuple) => {
                    for sub in tuple {
                        check_schema(sub)?;
                    }
                }
                other => check_schema(other)?,
            },
            "allOf" | "anyOf" | "oneOf" => {
                let subs = value
                    .as_array()
                    .ok_or_else(|| format!("\"{key}\" must be an array"))?;
                for sub in subs {
                    check_schema(sub)?;
                }
            }
            other => return Err(format!("unsupported schema keyword \"{other}\"")),
        }
    }
    Ok(())
}

fn check_type_keyword(value: &Value) -> Result<(), String> {
    let known = |name: &Value| name.as_str().is_some_and(|name| TYPES.contains(&name));
    let valid = match value {
        Value::Array(names) => names.iter().all(known),
        other => known(other),
    };
    if valid {
        Ok(())
    } else {
        Err(format!("unsupported \"type\": {value}"))
    }
}

fn types_of(schema: &Value) -> Vec<&str> {
    match schema.get("type") {
        Some(Value::String(name)) => vec![name.as_str()],
        Some(Value::Array(names)) => names.iter().filter_map(Value::as_str).collect(),
        _ => Vec::new(),
    }
}

fn is_type(value: &Value, name: &str) -> bool {
    match name {
        "object" => value.is_object(),
        "array" => value.is_array(),
        "string" => value.is_string(),
        "boolean" => value.is_boolean(),
        "null" => value.is_null(),
        "number" => value.is_number(),
        "integer" => value.as_f64().is_some_and(|number| number.fract() == 0.0),
        _ => false,
    }
}

fn type_matches(declared: &Value, value: &Value) -> bool {
    match declared {
        Value::String(name) => is_type(value, name),
        Value::Array(names) => names
            .iter()
            .filter_map(Value::as_str)
            .any(|name| is_type(value, name)),
        _ => false,
    }
}

fn validates(schema: &Value, value: &Value) -> bool {
    !matches!(schema, Value::Bool(_)) && check(schema, value)
}

fn check(schema: &Value, value: &Value) -> bool {
    let map = match schema {
        Value::Bool(flag) => return *flag,
        Value::Object(map) => map,
        _ => return false,
    };
    if map
        .get("type")
        .is_some_and(|declared| !type_matches(declared, value))
    {
        return false;
    }
    if let Value::Object(object) = value {
        let required_ok = map
            .get("required")
            .and_then(Value::as_array)
            .is_none_or(|names| {
                names
                    .iter()
                    .filter_map(Value::as_str)
                    .all(|name| object.contains_key(name))
            });
        let props = map.get("properties").and_then(Value::as_object);
        let required = map.get("required").and_then(Value::as_array);
        let fast_closed = matches!(map.get("additionalProperties"), Some(Value::Bool(false)))
            && props.is_some_and(|props| required.is_some_and(|names| names.len() == props.len()));
        let additional_ok = if fast_closed {
            object.len() == required.map_or(0, Vec::len)
        } else {
            map.get("additionalProperties").is_none_or(|extra| {
                object.iter().all(|(key, value)| {
                    props.is_some_and(|props| props.contains_key(key)) || check(extra, value)
                })
            })
        };
        let props_ok = props.is_none_or(|props| {
            props
                .iter()
                .all(|(key, sub)| object.get(key).is_none_or(|value| check(sub, value)))
        });
        if !(required_ok && additional_ok && props_ok) {
            return false;
        }
    }
    if let Value::Array(array) = value {
        let items_ok = map.get("items").is_none_or(|items| match items {
            Value::Array(tuple) => tuple
                .iter()
                .zip(array.iter())
                .all(|(sub, item)| check(sub, item)),
            other => array.iter().all(|item| check(other, item)),
        });
        let min_ok = map
            .get("minItems")
            .and_then(Value::as_u64)
            .is_none_or(|min| array.len() as u64 >= min);
        if !(items_ok && min_ok) {
            return false;
        }
    }
    let all_of = map
        .get("allOf")
        .and_then(Value::as_array)
        .is_none_or(|subs| subs.iter().all(|sub| check(sub, value)));
    let any_of = map
        .get("anyOf")
        .and_then(Value::as_array)
        .is_none_or(|subs| subs.iter().any(|sub| check(sub, value)));
    let one_of = map
        .get("oneOf")
        .and_then(Value::as_array)
        .is_none_or(|subs| subs.iter().filter(|sub| check(sub, value)).count() == 1);
    all_of && any_of && one_of
}

struct Issue {
    keyword: &'static str,
    path: String,
    message: String,
    missing: Vec<String>,
}

impl Issue {
    fn new(keyword: &'static str, path: &str, message: &str) -> Self {
        Self {
            keyword,
            path: path.to_owned(),
            message: message.to_owned(),
            missing: Vec::new(),
        }
    }
}

#[derive(Default)]
struct Ctx {
    issues: Vec<Issue>,
}

impl Ctx {
    fn at_capacity(&self) -> bool {
        self.issues.len() >= MAX_ERRORS
    }

    fn add(&mut self, issue: Issue) -> bool {
        if !self.at_capacity() {
            self.issues.push(issue);
        }
        false
    }
}

fn adopt(branches: Vec<Ctx>, ctx: &mut Ctx) {
    for branch in branches {
        for issue in branch.issues {
            ctx.add(issue);
        }
    }
}

fn errors(schema: &Value, value: &Value, path: &str, ctx: &mut Ctx) -> bool {
    if ctx.at_capacity() {
        return false;
    }
    let map = match schema {
        Value::Bool(true) => return true,
        Value::Bool(false) => return ctx.add(Issue::new("boolean", path, "schema is false")),
        Value::Object(map) => map,
        _ => return false,
    };
    let mut all = true;
    if let Some(declared) = map.get("type") {
        let matched = type_matches(declared, value);
        all &= matched || ctx.add(type_issue(declared, path));
    }
    if let Value::Object(object) = value {
        let mut block = true;
        if let Some(names) = map.get("required").and_then(Value::as_array) {
            block &= error_required(names, object, path, ctx);
        }
        if let Some(extra) = map.get("additionalProperties") {
            let props = map.get("properties").and_then(Value::as_object);
            block &= error_additional(extra, props, object, path, ctx);
        }
        if let Some(props) = map.get("properties").and_then(Value::as_object) {
            block &= error_properties(props, object, path, ctx);
        }
        all &= block;
    }
    if let (Value::Array(array), Some(items)) = (value, map.get("items")) {
        all &= error_items(items, array, path, ctx);
    }
    if let (Value::Array(array), Some(min)) = (value, map.get("minItems").and_then(Value::as_u64)) {
        all &= array.len() as u64 >= min
            || ctx.add(Issue::new(
                "minItems",
                path,
                &format!("must NOT have fewer than {min} items"),
            ));
    }
    if let Some(subs) = map.get("allOf").and_then(Value::as_array) {
        all &= error_all_of(subs, value, path, ctx);
    }
    if let Some(subs) = map.get("anyOf").and_then(Value::as_array) {
        all &= error_any_of(subs, value, path, ctx);
    }
    if let Some(subs) = map.get("oneOf").and_then(Value::as_array) {
        all &= error_one_of(subs, value, path, ctx);
    }
    all
}

fn type_issue(declared: &Value, path: &str) -> Issue {
    let message = match declared {
        Value::Array(names) => format!(
            "must be either {}",
            names
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(" or ")
        ),
        other => format!("must be {}", other.as_str().unwrap_or_default()),
    };
    Issue::new("type", path, &message)
}

fn error_required(names: &[Value], object: &Map<String, Value>, path: &str, ctx: &mut Ctx) -> bool {
    let missing: Vec<String> = names
        .iter()
        .filter_map(Value::as_str)
        .filter(|name| !object.contains_key(*name))
        .map(str::to_owned)
        .collect();
    if missing.is_empty() {
        return true;
    }
    let message = format!("must have required properties {}", missing.join(", "));
    ctx.add(Issue {
        keyword: "required",
        path: path.to_owned(),
        message,
        missing,
    })
}

fn error_additional(
    extra: &Value,
    props: Option<&Map<String, Value>>,
    object: &Map<String, Value>,
    path: &str,
    ctx: &mut Ctx,
) -> bool {
    let mut all = true;
    for key in js::keys(object) {
        if props.is_some_and(|props| props.contains_key(key)) {
            continue;
        }
        let child = format!("{path}/{key}");
        all &= errors(extra, &object[key], &child, ctx);
    }
    all || ctx.add(Issue::new(
        "additionalProperties",
        path,
        "must not have additional properties",
    ))
}

fn error_properties(
    props: &Map<String, Value>,
    object: &Map<String, Value>,
    path: &str,
    ctx: &mut Ctx,
) -> bool {
    let mut all = true;
    for key in js::keys(props) {
        if let Some(value) = object.get(key) {
            let child = format!("{path}/{key}");
            all &= errors(&props[key], value, &child, ctx);
        }
    }
    all
}

fn error_items(items: &Value, array: &[Value], path: &str, ctx: &mut Ctx) -> bool {
    let mut all = true;
    match items {
        Value::Array(tuple) => {
            for (index, sub) in tuple.iter().enumerate() {
                if let Some(value) = array.get(index) {
                    all &= errors(sub, value, &format!("{path}/{index}"), ctx);
                }
            }
        }
        sub => {
            for (index, value) in array.iter().enumerate() {
                all &= errors(sub, value, &format!("{path}/{index}"), ctx);
            }
        }
    }
    all
}

fn error_all_of(subs: &[Value], value: &Value, path: &str, ctx: &mut Ctx) -> bool {
    let mut passed = 0;
    let mut failed = Vec::new();
    for sub in subs {
        let mut branch = Ctx::default();
        if errors(sub, value, path, &mut branch) {
            passed += 1;
        } else {
            failed.push(branch);
        }
    }
    let ok = passed == subs.len();
    if !ok {
        adopt(failed, ctx);
    }
    ok
}

fn error_any_of(subs: &[Value], value: &Value, path: &str, ctx: &mut Ctx) -> bool {
    let mut passed = 0;
    let mut failed = Vec::new();
    for sub in subs {
        let mut branch = Ctx::default();
        if errors(sub, value, path, &mut branch) {
            passed += 1;
        } else {
            failed.push(branch);
        }
    }
    let ok = passed > 0;
    if !ok {
        adopt(failed, ctx);
    }
    ok || ctx.add(Issue::new("anyOf", path, "must match a schema in anyOf"))
}

fn error_one_of(subs: &[Value], value: &Value, path: &str, ctx: &mut Ctx) -> bool {
    let mut passed = 0;
    let mut failed = Vec::new();
    for sub in subs {
        let mut branch = Ctx::default();
        if errors(sub, value, path, &mut branch) {
            passed += 1;
        } else {
            failed.push(branch);
        }
    }
    let ok = passed == 1;
    if !ok && passed == 0 {
        adopt(failed, ctx);
    }
    ok || ctx.add(Issue::new(
        "oneOf",
        path,
        "must match exactly one schema in oneOf",
    ))
}

fn format_path(issue: &Issue) -> String {
    let base = issue
        .path
        .strip_prefix('/')
        .unwrap_or(&issue.path)
        .replace('/', ".");
    if let Some(first) = issue
        .missing
        .first()
        .filter(|name| issue.keyword == "required" && !name.is_empty())
    {
        return if base.is_empty() {
            first.clone()
        } else {
            format!("{base}.{first}")
        };
    }
    if base.is_empty() {
        "root".to_owned()
    } else {
        base
    }
}

fn normalize_optional_nulls(value: &mut Value, schema: &Value) {
    match value {
        Value::Array(array) => match schema.get("items") {
            Some(Value::Array(tuple)) => {
                for (index, sub) in tuple.iter().enumerate() {
                    if let Some(item) = array.get_mut(index) {
                        normalize_optional_nulls(item, sub);
                    }
                }
            }
            Some(sub @ Value::Object(_)) => {
                for item in array.iter_mut() {
                    normalize_optional_nulls(item, sub);
                }
            }
            _ => {}
        },
        Value::Object(object) => {
            let Some(props) = schema.get("properties").and_then(Value::as_object) else {
                return;
            };
            let required: Vec<&str> = schema
                .get("required")
                .and_then(Value::as_array)
                .map(|names| names.iter().filter_map(Value::as_str).collect())
                .unwrap_or_default();
            for key in js::keys(props) {
                let sub = &props[key];
                let Some(item) = object.get(key) else {
                    continue;
                };
                if item.is_null()
                    && !required.contains(&key)
                    && !matches!(sub, Value::Bool(_))
                    && !check(sub, &Value::Null)
                {
                    object.shift_remove(key);
                } else if let Some(item) = object.get_mut(key) {
                    normalize_optional_nulls(item, sub);
                }
            }
        }
        _ => {}
    }
}

fn coerce(value: Value, schema: &Value) -> Value {
    let Value::Object(map) = schema else {
        return value;
    };
    let mut next = value;
    if let Some(subs) = map.get("allOf").and_then(Value::as_array) {
        for sub in subs {
            next = coerce(next, sub);
        }
    }
    if let Some(subs) = map.get("anyOf").and_then(Value::as_array) {
        next = coerce_union(next, subs);
    }
    if let Some(subs) = map.get("oneOf").and_then(Value::as_array) {
        next = coerce_union(next, subs);
    }
    let names = types_of(schema);
    let union_member = names.len() > 1 && names.iter().any(|name| is_type(&next, name));
    if !names.is_empty()
        && !union_member
        && let Some(coerced) = names.iter().find_map(|name| coerce_primitive(&next, name))
    {
        next = coerced;
    }
    match &mut next {
        Value::Object(object) if names.contains(&"object") => apply_object(object, map),
        Value::Array(array) if names.contains(&"array") => apply_array(array, map),
        _ => {}
    }
    next
}

fn coerce_union(value: Value, subs: &[Value]) -> Value {
    if subs.iter().any(|sub| validates(sub, &value)) {
        return value;
    }
    for sub in subs {
        let coerced = coerce(value.clone(), sub);
        if validates(sub, &coerced) {
            return coerced;
        }
    }
    value
}

fn apply_object(object: &mut Map<String, Value>, schema: &Map<String, Value>) {
    let props = schema.get("properties").and_then(Value::as_object);
    if let Some(props) = props {
        for key in js::keys(props) {
            if let Some(slot) = object.get_mut(key) {
                *slot = coerce(std::mem::take(slot), &props[key]);
            }
        }
    }
    if let Some(extra @ Value::Object(_)) = schema.get("additionalProperties") {
        let keys: Vec<String> = js::keys(object).into_iter().map(str::to_owned).collect();
        for key in keys {
            if props.is_some_and(|props| props.contains_key(&key)) {
                continue;
            }
            if let Some(slot) = object.get_mut(&key) {
                *slot = coerce(std::mem::take(slot), extra);
            }
        }
    }
}

fn apply_array(array: &mut [Value], schema: &Map<String, Value>) {
    match schema.get("items") {
        Some(Value::Array(tuple)) => {
            for (index, sub) in tuple.iter().enumerate() {
                if let Some(slot) = array.get_mut(index) {
                    *slot = coerce(std::mem::take(slot), sub);
                }
            }
        }
        Some(items @ Value::Object(_)) => {
            for slot in array.iter_mut() {
                *slot = coerce(std::mem::take(slot), items);
            }
        }
        _ => {}
    }
}

fn coerce_primitive(value: &Value, name: &str) -> Option<Value> {
    match name {
        "number" => match value {
            Value::Null => Some(number_value(0.0)),
            Value::String(text) if !js::trim(text).is_empty() => {
                js::parse_number(text).map(number_value)
            }
            Value::Bool(flag) => Some(number_value(f64::from(u8::from(*flag)))),
            _ => None,
        },
        "integer" => match value {
            Value::Null => Some(number_value(0.0)),
            Value::String(text) if !js::trim(text).is_empty() => js::parse_number(text)
                .filter(|number| number.fract() == 0.0)
                .map(number_value),
            Value::Bool(flag) => Some(number_value(f64::from(u8::from(*flag)))),
            _ => None,
        },
        "boolean" => match value {
            Value::Null => Some(Value::Bool(false)),
            Value::String(text) if text == "true" => Some(Value::Bool(true)),
            Value::String(text) if text == "false" => Some(Value::Bool(false)),
            Value::Number(number) => match number.as_f64() {
                Some(1.0) => Some(Value::Bool(true)),
                Some(0.0) => Some(Value::Bool(false)),
                _ => None,
            },
            _ => None,
        },
        "string" => match value {
            Value::Null => Some(Value::String(String::new())),
            Value::Number(number) => number
                .as_f64()
                .map(|number| Value::String(js::number_to_string(number))),
            Value::Bool(flag) => Some(Value::String(flag.to_string())),
            _ => None,
        },
        "null" => match value {
            Value::String(text) if text.is_empty() => Some(Value::Null),
            Value::Number(number) if number.as_f64() == Some(0.0) => Some(Value::Null),
            Value::Bool(false) => Some(Value::Null),
            _ => None,
        },
        _ => None,
    }
}
