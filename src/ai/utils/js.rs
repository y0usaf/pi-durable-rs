use serde_json::{Map, Value};

pub fn trim(text: &str) -> &str {
    text.trim_matches(is_js_space)
}

fn is_js_space(c: char) -> bool {
    matches!(
        c,
        '\t' | '\n' | '\u{b}' | '\u{c}' | '\r' | ' ' | '\u{a0}' | '\u{1680}' | '\u{2000}'
            ..='\u{200a}'
                | '\u{2028}'
                | '\u{2029}'
                | '\u{202f}'
                | '\u{205f}'
                | '\u{3000}'
                | '\u{feff}'
    )
}

pub fn parse_number(text: &str) -> Option<f64> {
    let text = trim(text);
    if text.is_empty() {
        return Some(0.0);
    }
    for (prefix, radix) in [
        ("0x", 16),
        ("0X", 16),
        ("0o", 8),
        ("0O", 8),
        ("0b", 2),
        ("0B", 2),
    ] {
        if let Some(digits) = text.strip_prefix(prefix) {
            return radix_value(digits, radix);
        }
    }
    decimal(text).filter(|value| value.is_finite())
}

fn radix_value(digits: &str, radix: u32) -> Option<f64> {
    if digits.is_empty() {
        return None;
    }
    let mut value = 0.0;
    for c in digits.chars() {
        value = value * f64::from(radix) + f64::from(c.to_digit(radix)?);
    }
    value.is_finite().then_some(value)
}

fn decimal(text: &str) -> Option<f64> {
    let bytes = text.as_bytes();
    let digits = |at: &mut usize| {
        let start = *at;
        while bytes.get(*at).is_some_and(u8::is_ascii_digit) {
            *at += 1;
        }
        *at - start
    };
    let mut at = 0;
    if matches!(bytes.first(), Some(b'+' | b'-')) {
        at += 1;
    }
    let whole = digits(&mut at);
    let fraction = if bytes.get(at) == Some(&b'.') {
        at += 1;
        digits(&mut at)
    } else {
        0
    };
    if whole + fraction == 0 {
        return None;
    }
    if matches!(bytes.get(at), Some(b'e' | b'E')) {
        at += 1;
        if matches!(bytes.get(at), Some(b'+' | b'-')) {
            at += 1;
        }
        if digits(&mut at) == 0 {
            return None;
        }
    }
    if at != bytes.len() {
        return None;
    }
    text.parse().ok()
}

pub fn number_to_string(value: f64) -> String {
    if value == 0.0 {
        return "0".to_owned();
    }
    if value.is_infinite() {
        return if value > 0.0 { "Infinity" } else { "-Infinity" }.to_owned();
    }
    if value < 0.0 {
        return format!("-{}", number_to_string(-value));
    }
    let shortest = format!("{value:e}");
    let (mantissa, exponent) = shortest
        .split_once('e')
        .expect("LowerExp of a finite f64 always has an exponent");
    let digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
    let exponent: i64 = exponent
        .parse()
        .expect("LowerExp exponent is a decimal integer");
    let k = digits.len() as i64;
    let n = exponent + 1;
    if k <= n && n <= 21 {
        format!("{digits}{}", "0".repeat((n - k) as usize))
    } else if 0 < n && n <= 21 {
        let (whole, fraction) = digits.split_at(n as usize);
        format!("{whole}.{fraction}")
    } else if -6 < n && n <= 0 {
        format!("0.{}{digits}", "0".repeat((-n) as usize))
    } else {
        let sign = if n - 1 < 0 { "-" } else { "+" };
        let tail = if k == 1 {
            String::new()
        } else {
            format!(".{}", &digits[1..])
        };
        format!("{}{tail}e{sign}{}", &digits[..1], (n - 1).abs())
    }
}

pub fn keys(map: &Map<String, Value>) -> Vec<&str> {
    let mut indexed: Vec<(u32, &str)> = Vec::new();
    let mut named: Vec<&str> = Vec::new();
    for key in map.keys() {
        match array_index(key) {
            Some(index) => indexed.push((index, key)),
            None => named.push(key),
        }
    }
    indexed.sort_unstable();
    indexed
        .into_iter()
        .map(|(_, key)| key)
        .chain(named)
        .collect()
}

fn array_index(key: &str) -> Option<u32> {
    let canonical = key == "0" || (!key.is_empty() && !key.starts_with('0'));
    if !canonical || !key.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    key.parse::<u32>().ok().filter(|index| *index < u32::MAX)
}

pub fn stringify(value: &Value) -> String {
    let mut out = String::new();
    write_value(value, None, 0, &mut out);
    out
}

pub fn stringify_object(map: &Map<String, Value>) -> String {
    let mut out = String::new();
    write_object(map, None, 0, &mut out);
    out
}

pub fn stringify_pretty(value: &Value) -> String {
    let mut out = String::new();
    write_value(value, Some(2), 0, &mut out);
    out
}

fn write_value(value: &Value, indent: Option<usize>, depth: usize, out: &mut String) {
    match value {
        Value::Array(items) => {
            if items.is_empty() {
                out.push_str("[]");
                return;
            }
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                newline(indent, depth + 1, out);
                write_value(item, indent, depth + 1, out);
            }
            newline(indent, depth, out);
            out.push(']');
        }
        Value::Object(map) => write_object(map, indent, depth, out),
        scalar => write_scalar(scalar, out),
    }
}

fn write_object(map: &Map<String, Value>, indent: Option<usize>, depth: usize, out: &mut String) {
    if map.is_empty() {
        out.push_str("{}");
        return;
    }
    out.push('{');
    for (index, key) in keys(map).into_iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        newline(indent, depth + 1, out);
        write_string(key, out);
        out.push_str(if indent.is_some() { ": " } else { ":" });
        write_value(&map[key], indent, depth + 1, out);
    }
    newline(indent, depth, out);
    out.push('}');
}

fn newline(indent: Option<usize>, depth: usize, out: &mut String) {
    if let Some(width) = indent {
        out.push('\n');
        for _ in 0..depth * width {
            out.push(' ');
        }
    }
}

fn write_scalar(value: &Value, out: &mut String) {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(flag) => out.push_str(if *flag { "true" } else { "false" }),
        Value::Number(number) => out.push_str(
            &number
                .as_f64()
                .map_or_else(|| "null".to_owned(), number_to_string),
        ),
        Value::String(text) => write_string(text, out),
        Value::Array(_) | Value::Object(_) => write_value(value, None, 0, out),
    }
}

fn write_string(text: &str, out: &mut String) {
    out.push('"');
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}
