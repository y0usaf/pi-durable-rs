use crate::chord::json::{JsonObject, JsonValue};

const VALID_JSON_ESCAPES: [char; 9] = ['"', '\\', '/', 'b', 'f', 'n', 'r', 't', 'u'];

fn is_js_whitespace(c: char) -> bool {
    matches!(
        c,
        '\t' | '\n' | '\u{0b}' | '\u{0c}' | '\r' | ' ' | '\u{a0}' | '\u{1680}' | '\u{2000}'
            ..='\u{200a}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202f}'
                | '\u{205f}'
                | '\u{3000}'
                | '\u{feff}'
    )
}

/// Trim with the whitespace set of JavaScript's String.prototype.trim.
pub fn trim_js(text: &str) -> &str {
    text.trim_matches(is_js_whitespace)
}

fn escape_control_character(c: char) -> String {
    match c {
        '\u{08}' => "\\b".to_owned(),
        '\u{0c}' => "\\f".to_owned(),
        '\n' => "\\n".to_owned(),
        '\r' => "\\r".to_owned(),
        '\t' => "\\t".to_owned(),
        _ => format!("\\u{:04x}", c as u32),
    }
}

/// Repair malformed JSON string literals: escape raw control characters inside
/// strings and double backslashes before invalid escape characters.
pub fn repair_json(json: &str) -> String {
    let chars: Vec<char> = json.chars().collect();
    let mut repaired = String::with_capacity(json.len());
    let mut in_string = false;
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        i += 1;
        if !in_string {
            repaired.push(c);
            in_string = c == '"';
            continue;
        }
        match c {
            '"' => {
                repaired.push(c);
                in_string = false;
            }
            '\\' => match chars.get(i).copied() {
                None => repaired.push_str("\\\\"),
                Some('u') if is_hex4(&chars, i + 1) => {
                    repaired.push_str("\\u");
                    repaired.extend(&chars[i + 1..i + 5]);
                    i += 5;
                }
                Some(next) if VALID_JSON_ESCAPES.contains(&next) => {
                    repaired.push('\\');
                    repaired.push(next);
                    i += 1;
                }
                Some(_) => repaired.push_str("\\\\"),
            },
            c if (c as u32) <= 0x1f => repaired.push_str(&escape_control_character(c)),
            c => repaired.push(c),
        }
    }
    repaired
}

fn is_hex4(chars: &[char], start: usize) -> bool {
    chars
        .get(start..start + 4)
        .is_some_and(|digits| digits.iter().all(|d| d.is_ascii_hexdigit()))
}

/// Strict JSON parse; on failure retry once with [repair_json] applied. When repair
/// changes nothing the original parse error is returned.
pub fn parse_json_with_repair(json: &str) -> Result<JsonValue, serde_json::Error> {
    match serde_json::from_str(json) {
        Ok(value) => Ok(value),
        Err(error) => {
            let repaired = repair_json(json);
            if repaired == json {
                Err(error)
            } else {
                serde_json::from_str(&repaired)
            }
        }
    }
}

type Parsed<T> = Result<T, ()>;

struct PartialParser {
    units: Vec<u16>,
    index: usize,
}

impl PartialParser {
    fn is(&self, index: usize, c: char) -> bool {
        self.units.get(index) == Some(&(c as u16))
    }

    fn is_one_of(&self, index: usize, set: &str) -> bool {
        set.chars().any(|c| self.is(index, c))
    }

    fn slice(&self, start: isize, end: isize) -> String {
        let len = self.units.len() as isize;
        let clamp = |value: isize| value.clamp(0, len) as usize;
        let (mut from, mut to) = (clamp(start), clamp(end));
        if from > to {
            std::mem::swap(&mut from, &mut to);
        }
        String::from_utf16_lossy(&self.units[from..to])
    }

    fn remaining(&self) -> isize {
        self.units.len() as isize - self.index as isize
    }

    fn token_at(&self, token: &str) -> bool {
        let end = self.index + token.len();
        self.units
            .get(self.index..end)
            .is_some_and(|units| units.iter().copied().eq(token.encode_utf16()))
    }

    fn partial_token(&self, word: &str, max_len: usize) -> bool {
        let rest = self.slice(self.index as isize, self.units.len() as isize);
        self.remaining() < max_len as isize && word.starts_with(&rest)
    }

    fn take_literal(&mut self, token: &str, value: JsonValue) -> Option<JsonValue> {
        if self.token_at(token) || self.partial_token(token, token.len()) {
            self.index += token.len();
            Some(value)
        } else {
            None
        }
    }

    fn skip_blank(&mut self) {
        while self.is_one_of(self.index, " \n\r\t") {
            self.index += 1;
        }
    }

    fn any(&mut self) -> Parsed<JsonValue> {
        self.skip_blank();
        if self.index >= self.units.len() {
            return Err(());
        }
        if self.is(self.index, '"') {
            return self.string().map(JsonValue::String);
        }
        if self.is(self.index, '{') {
            return Ok(JsonValue::Object(self.object()));
        }
        if self.is(self.index, '[') {
            return Ok(JsonValue::Array(self.array()));
        }
        if let Some(value) = self.take_literal("null", JsonValue::Null) {
            return Ok(value);
        }
        if let Some(value) = self.take_literal("true", JsonValue::Bool(true)) {
            return Ok(value);
        }
        if let Some(value) = self.take_literal("false", JsonValue::Bool(false)) {
            return Ok(value);
        }
        if let Some(value) = self.take_literal("Infinity", JsonValue::Null) {
            return Ok(value);
        }
        if self.token_at("-Infinity")
            || (1 < self.remaining() && self.partial_token("-Infinity", 9))
        {
            self.index += 9;
            return Ok(JsonValue::Null);
        }
        if let Some(value) = self.take_literal("NaN", JsonValue::Null) {
            return Ok(value);
        }
        self.number()
    }

    fn string(&mut self) -> Parsed<String> {
        let start = self.index as isize;
        let mut escape = false;
        self.index += 1;
        while self.index < self.units.len()
            && (!self.is(self.index, '"') || (escape && self.is(self.index - 1, '\\')))
        {
            escape = self.is(self.index, '\\') && !escape;
            self.index += 1;
        }
        if self.is(self.index, '"') {
            self.index += 1;
            let end = self.index as isize - isize::from(escape);
            return json_string(&self.slice(start, end));
        }
        let end = self.index as isize - isize::from(escape);
        json_string(&(self.slice(start, end) + "\"")).or_else(|()| {
            let cut = self.last_index_of('\\');
            json_string(&(self.slice(start, cut) + "\""))
        })
    }

    fn last_index_of(&self, c: char) -> isize {
        self.units
            .iter()
            .rposition(|&unit| unit == c as u16)
            .map_or(-1, |i| i as isize)
    }

    fn object(&mut self) -> JsonObject {
        self.index += 1;
        self.skip_blank();
        let mut object = JsonObject::new();
        while !self.is(self.index, '}') {
            self.skip_blank();
            if self.index >= self.units.len() {
                return object;
            }
            let Ok(key) = self.string() else {
                return object;
            };
            self.skip_blank();
            self.index += 1;
            let Ok(value) = self.any() else {
                return object;
            };
            object.insert(key, value);
            self.skip_blank();
            if self.is(self.index, ',') {
                self.index += 1;
            }
        }
        self.index += 1;
        object
    }

    fn array(&mut self) -> Vec<JsonValue> {
        self.index += 1;
        let mut array = Vec::new();
        while !self.is(self.index, ']') {
            let Ok(value) = self.any() else {
                return array;
            };
            array.push(value);
            self.skip_blank();
            if self.is(self.index, ',') {
                self.index += 1;
            }
        }
        self.index += 1;
        array
    }

    fn number(&mut self) -> Parsed<JsonValue> {
        if self.index == 0 {
            let whole = self.slice(0, self.units.len() as isize);
            if whole == "-" {
                return Err(());
            }
            return json_value(&whole)
                .or_else(|()| json_value(&self.slice(0, self.last_index_of('e'))));
        }
        let start = self.index as isize;
        if self.is(self.index, '-') {
            self.index += 1;
        }
        while self.index < self.units.len() && !self.is_one_of(self.index, ",]}") {
            self.index += 1;
        }
        let token = self.slice(start, self.index as isize);
        json_value(&token).or_else(|()| {
            if token == "-" {
                return Err(());
            }
            json_value(&self.slice(start, self.last_index_of('e')))
        })
    }
}

fn json_string(text: &str) -> Parsed<String> {
    serde_json::from_str(text).map_err(|_| ())
}

fn json_value(text: &str) -> Parsed<JsonValue> {
    serde_json::from_str(text).map_err(|_| ())
}

fn partial_parse(text: &str) -> Parsed<JsonValue> {
    let trimmed = trim_js(text);
    if trimmed.is_empty() {
        return Err(());
    }
    PartialParser {
        units: trimmed.encode_utf16().collect(),
        index: 0,
    }
    .any()
}

fn or_empty_object(value: JsonValue) -> JsonValue {
    match value {
        JsonValue::Null => JsonValue::Object(JsonObject::new()),
        value => value,
    }
}

/// Parse a possibly truncated JSON document from a streaming tool call, like pi-ai's
/// parseStreamingJson. Empty input gives {}. Otherwise strict JSON, repaired JSON and
/// partial parsing are tried in that order; a partial parse keeps the complete prefix
/// of the document. Non-finite numbers come back as null, as JSON.stringify writes them.
pub fn parse_streaming_json(partial: Option<&str>) -> JsonValue {
    let empty = || JsonValue::Object(JsonObject::new());
    let Some(text) = partial.filter(|text| !trim_js(text).is_empty()) else {
        return empty();
    };
    if let Ok(value) = serde_json::from_str(text) {
        return value;
    }
    let repaired = repair_json(text);
    if let Ok(value) = serde_json::from_str(&repaired) {
        return value;
    }
    match partial_parse(text) {
        Ok(value) => or_empty_object(value),
        Err(()) => partial_parse(&repaired).map_or_else(|()| empty(), or_empty_object),
    }
}
