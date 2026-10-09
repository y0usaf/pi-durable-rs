use serde_json::{Map, Value, json};

use crate::documents::MAX_SAFE_INTEGER;
use crate::errors::{Error, Result};
use crate::types::{Cursor, Page, ScanOrder};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanStart {
    pub order: ScanOrder,
    pub after: Option<u64>,
}

pub fn order_name(order: ScanOrder) -> &'static str {
    match order {
        ScanOrder::Ascending => "ascending",
        ScanOrder::Descending => "descending",
    }
}

fn invalid_cursor() -> Error {
    Error::Invalid("Invalid storage cursor".to_owned())
}

fn safe_id(value: &Value) -> Option<u64> {
    value.as_u64().filter(|id| *id <= MAX_SAFE_INTEGER)
}

pub fn cursor_after(cursor: Option<&Cursor>) -> Result<Option<u64>> {
    match cursor.and_then(|cursor| cursor.get("after")) {
        None => Ok(None),
        Some(value) => safe_id(value).map(Some).ok_or_else(invalid_cursor),
    }
}

pub fn scan_start(
    requested: Option<ScanOrder>,
    cursor: Option<&Cursor>,
    fallback: ScanOrder,
) -> Result<ScanStart> {
    let Some(cursor) = cursor else {
        return Ok(ScanStart {
            order: requested.unwrap_or(fallback),
            after: None,
        });
    };
    let after = cursor
        .get("after")
        .and_then(safe_id)
        .ok_or_else(invalid_cursor)?;
    let order = match cursor.get("order") {
        None => fallback,
        Some(Value::String(name)) if name == "ascending" => ScanOrder::Ascending,
        Some(Value::String(name)) if name == "descending" => ScanOrder::Descending,
        Some(_) => return Err(invalid_cursor()),
    };
    if let Some(requested) = requested
        && requested != order
    {
        return Err(Error::Invalid(format!(
            "The cursor continues a {} scan; the query asks for {}",
            order_name(order),
            order_name(requested)
        )));
    }
    Ok(ScanStart {
        order,
        after: Some(after),
    })
}

pub fn next_cursor(id: u64, order: ScanOrder) -> Cursor {
    let mut cursor = Map::new();
    cursor.insert("after".to_owned(), json!(id));
    cursor.insert("order".to_owned(), json!(order_name(order)));
    cursor
}

pub fn page<T>(
    mut items: Vec<T>,
    limit: usize,
    order: ScanOrder,
    id_of: impl Fn(&T) -> u64,
) -> Page<T> {
    let overflow = items.len() > limit;
    items.truncate(limit);
    let next = if overflow {
        items.last().map(|item| next_cursor(id_of(item), order))
    } else {
        None
    };
    Page { items, next }
}
