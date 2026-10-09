pub fn resolve(cwd: &str, path: &str) -> String {
    let expanded = expand(path);
    if expanded.starts_with('/') {
        return normalize_absolute(&expanded);
    }
    let base = std::path::absolute(cwd)
        .map(|absolute| absolute.to_string_lossy().into_owned())
        .unwrap_or_else(|_| cwd.to_owned());
    normalize_absolute(&format!("{base}/{expanded}"))
}

pub fn join(parts: &[&str]) -> String {
    let joined = parts
        .iter()
        .copied()
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("/");
    if joined.is_empty() {
        return ".".to_owned();
    }
    let absolute = joined.starts_with('/');
    let mut stack: Vec<&str> = Vec::new();
    for segment in joined.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                let top = stack.last().copied();
                match top {
                    Some(name) if name != ".." => {
                        stack.pop();
                    }
                    _ if !absolute => stack.push(".."),
                    _ => {}
                }
            }
            name => stack.push(name),
        }
    }
    let body = stack.join("/");
    let mut result = if absolute {
        format!("/{body}")
    } else if body.is_empty() {
        ".".to_owned()
    } else {
        body
    };
    if joined.ends_with('/') && !result.ends_with('/') {
        result.push('/');
    }
    result
}

pub fn parent(path: &str) -> &str {
    if path == "/" {
        return path;
    }
    match path.rfind('/') {
        Some(0) => "/",
        Some(index) => &path[..index],
        None => ".",
    }
}

pub fn basename(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

pub fn child(directory: &str, name: &str) -> String {
    if directory.ends_with('/') {
        format!("{directory}{name}")
    } else {
        format!("{directory}/{name}")
    }
}

fn normalize_absolute(path: &str) -> String {
    let mut stack = Vec::new();
    for segment in path.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                stack.pop();
            }
            name => stack.push(name),
        }
    }
    format!("/{}", stack.join("/"))
}

fn expand(path: &str) -> String {
    if path == "~" {
        return home();
    }
    if let Some(rest) = path.strip_prefix("~/") {
        return format!("{}/{rest}", home());
    }
    file_url_path(path).unwrap_or_else(|| path.to_owned())
}

fn home() -> String {
    std::env::var("HOME").unwrap_or_else(|_| "/".to_owned())
}

fn file_url_path(url: &str) -> Option<String> {
    let rest = url.strip_prefix("file://")?;
    let (host, path) = match rest.find('/') {
        Some(index) => rest.split_at(index),
        None => (rest, "/"),
    };
    if !host.is_empty() && host != "localhost" {
        return None;
    }
    let path = path.split(['?', '#']).next()?;
    if path.to_ascii_lowercase().contains("%2f") {
        return None;
    }
    Some(String::from_utf8_lossy(&percent_decode(path)).into_owned())
}

fn percent_decode(text: &str) -> Vec<u8> {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        let decoded = if bytes[index] == b'%' && index + 2 < bytes.len() {
            std::str::from_utf8(&bytes[index + 1..index + 3])
                .ok()
                .and_then(|hex| u8::from_str_radix(hex, 16).ok())
        } else {
            None
        };
        match decoded {
            Some(byte) => {
                out.push(byte);
                index += 3;
            }
            None => {
                out.push(bytes[index]);
                index += 1;
            }
        }
    }
    out
}
