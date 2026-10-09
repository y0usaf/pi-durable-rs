#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    pub value: String,
    pub added: bool,
    pub removed: bool,
}

#[derive(Clone, Copy)]
struct Component {
    count: usize,
    added: bool,
    removed: bool,
    previous: Option<usize>,
}

#[derive(Clone, Copy)]
struct Path {
    old_pos: isize,
    last: Option<usize>,
}

struct Myers<'a> {
    old: Vec<&'a str>,
    new: Vec<&'a str>,
    components: Vec<Component>,
}

impl<'a> Myers<'a> {
    fn push(&mut self, count: usize, added: bool, removed: bool, previous: Option<usize>) -> usize {
        self.components.push(Component {
            count,
            added,
            removed,
            previous,
        });
        self.components.len() - 1
    }

    fn add_to_path(
        &mut self,
        path: Path,
        added: bool,
        removed: bool,
        old_pos_increment: isize,
    ) -> Path {
        let old_pos = path.old_pos + old_pos_increment;
        if let Some(last) = path.last {
            let component = self.components[last];
            if component.added == added && component.removed == removed {
                let last = self.push(component.count + 1, added, removed, component.previous);
                return Path {
                    old_pos,
                    last: Some(last),
                };
            }
        }
        let last = self.push(1, added, removed, path.last);
        Path {
            old_pos,
            last: Some(last),
        }
    }

    fn extract_common(&mut self, base: &mut Path, diagonal: isize) -> isize {
        let new_len = self.new.len() as isize;
        let old_len = self.old.len() as isize;
        let mut old_pos = base.old_pos;
        let mut new_pos = old_pos - diagonal;
        let mut common = 0;
        while new_pos + 1 < new_len
            && old_pos + 1 < old_len
            && self.old[(old_pos + 1) as usize] == self.new[(new_pos + 1) as usize]
        {
            new_pos += 1;
            old_pos += 1;
            common += 1;
        }
        if common > 0 {
            base.last = Some(self.push(common, false, false, base.last));
        }
        base.old_pos = old_pos;
        new_pos
    }

    fn build(&self, last: Option<usize>) -> Vec<Change> {
        let mut order = Vec::new();
        let mut cursor = last;
        while let Some(index) = cursor {
            order.push(index);
            cursor = self.components[index].previous;
        }
        order.reverse();
        let mut changes = Vec::with_capacity(order.len());
        let (mut new_pos, mut old_pos) = (0, 0);
        for index in order {
            let component = self.components[index];
            if component.removed {
                changes.push(Change {
                    value: self.old[old_pos..old_pos + component.count].concat(),
                    added: false,
                    removed: true,
                });
                old_pos += component.count;
            } else {
                changes.push(Change {
                    value: self.new[new_pos..new_pos + component.count].concat(),
                    added: component.added,
                    removed: false,
                });
                new_pos += component.count;
                if !component.added {
                    old_pos += component.count;
                }
            }
        }
        changes
    }

    fn run(&mut self) -> Vec<Change> {
        let new_len = self.new.len() as isize;
        let old_len = self.old.len() as isize;
        let max_edit = new_len + old_len;
        let offset = max_edit + 2;
        let index = |diagonal: isize| (diagonal + offset) as usize;
        let mut best: Vec<Option<Path>> = vec![None; (2 * max_edit + 5) as usize];

        let mut origin = Path {
            old_pos: -1,
            last: None,
        };
        let new_pos = self.extract_common(&mut origin, 0);
        if origin.old_pos + 1 >= old_len && new_pos + 1 >= new_len {
            return self.build(origin.last);
        }
        best[index(0)] = Some(origin);

        let mut min_diagonal = isize::MIN;
        let mut max_diagonal = isize::MAX;
        let mut edit_length: isize = 1;
        loop {
            let mut diagonal = min_diagonal.max(-edit_length);
            let upper = max_diagonal.min(edit_length);
            while diagonal <= upper {
                let remove_path = best[index(diagonal - 1)];
                let add_path = best[index(diagonal + 1)];
                if remove_path.is_some() {
                    best[index(diagonal - 1)] = None;
                }
                let can_add = add_path.is_some_and(|path| {
                    let new_pos = path.old_pos - diagonal;
                    0 <= new_pos && new_pos < new_len
                });
                let can_remove = remove_path.is_some_and(|path| path.old_pos + 1 < old_len);
                if !can_add && !can_remove {
                    best[index(diagonal)] = None;
                    diagonal += 2;
                    continue;
                }
                let mut base = match (can_remove, add_path, remove_path) {
                    (false, Some(add), _) => self.add_to_path(add, true, false, 0),
                    (true, Some(add), Some(remove)) if can_add && remove.old_pos < add.old_pos => {
                        self.add_to_path(add, true, false, 0)
                    }
                    (_, _, Some(remove)) => self.add_to_path(remove, false, true, 1),
                    _ => unreachable!("a usable diagonal has a path to extend"),
                };
                let new_pos = self.extract_common(&mut base, diagonal);
                if base.old_pos + 1 >= old_len && new_pos + 1 >= new_len {
                    return self.build(base.last);
                }
                best[index(diagonal)] = Some(base);
                if base.old_pos + 1 >= old_len {
                    max_diagonal = max_diagonal.min(diagonal - 1);
                }
                if new_pos + 1 >= new_len {
                    min_diagonal = min_diagonal.max(diagonal + 1);
                }
                diagonal += 2;
            }
            edit_length += 1;
        }
    }
}

fn tokenize(text: &str) -> Vec<&str> {
    text.split_inclusive('\n').collect()
}

pub fn diff_lines(old: &str, new: &str) -> Vec<Change> {
    let mut engine = Myers {
        old: tokenize(old),
        new: tokenize(new),
        components: Vec::new(),
    };
    engine.run()
}

fn split_lines(text: &str) -> Vec<String> {
    let mut lines: Vec<String> = text.split('\n').map(|line| format!("{line}\n")).collect();
    if text.ends_with('\n') {
        lines.pop();
    } else if let Some(last) = lines.pop() {
        lines.push(last[..last.len() - 1].to_owned());
    }
    lines
}

struct Part {
    lines: Vec<String>,
    added: bool,
    removed: bool,
}

struct Hunk {
    old_start: isize,
    old_lines: isize,
    new_start: isize,
    new_lines: isize,
    lines: Vec<String>,
}

fn context_lines(lines: &[String]) -> Vec<String> {
    lines.iter().map(|line| format!(" {line}")).collect()
}

fn structured_hunks(old: &str, new: &str, context: usize) -> Vec<Hunk> {
    let mut parts: Vec<Part> = diff_lines(old, new)
        .into_iter()
        .map(|change| Part {
            lines: split_lines(&change.value),
            added: change.added,
            removed: change.removed,
        })
        .collect();
    parts.push(Part {
        lines: Vec::new(),
        added: false,
        removed: false,
    });

    let context_size_limit = context as isize;
    let mut hunks = Vec::new();
    let mut old_range_start: isize = 0;
    let mut new_range_start: isize = 0;
    let mut current: Vec<String> = Vec::new();
    let mut old_line: isize = 1;
    let mut new_line: isize = 1;
    let count = parts.len();
    for index in 0..count {
        let part = &parts[index];
        if part.added || part.removed {
            if old_range_start == 0 {
                old_range_start = old_line;
                new_range_start = new_line;
                if index > 0 {
                    let previous = &parts[index - 1];
                    let kept = if context_size_limit > 0 {
                        let from = previous.lines.len().saturating_sub(context);
                        context_lines(&previous.lines[from..])
                    } else {
                        Vec::new()
                    };
                    current = kept;
                    old_range_start -= current.len() as isize;
                    new_range_start -= current.len() as isize;
                }
            }
            let sign = if part.added { '+' } else { '-' };
            for line in &part.lines {
                current.push(format!("{sign}{line}"));
            }
            if part.added {
                new_line += part.lines.len() as isize;
            } else {
                old_line += part.lines.len() as isize;
            }
        } else {
            if old_range_start != 0 {
                if part.lines.len() <= context * 2 && index + 2 < count {
                    current.extend(context_lines(&part.lines));
                } else {
                    let context_size = part.lines.len().min(context);
                    current.extend(context_lines(&part.lines[..context_size]));
                    let context_size = context_size as isize;
                    hunks.push(Hunk {
                        old_start: old_range_start,
                        old_lines: old_line - old_range_start + context_size,
                        new_start: new_range_start,
                        new_lines: new_line - new_range_start + context_size,
                        lines: std::mem::take(&mut current),
                    });
                    old_range_start = 0;
                    new_range_start = 0;
                }
            }
            old_line += part.lines.len() as isize;
            new_line += part.lines.len() as isize;
        }
    }

    for hunk in &mut hunks {
        let mut fixed = Vec::with_capacity(hunk.lines.len());
        for line in hunk.lines.drain(..) {
            match line.strip_suffix('\n') {
                Some(stripped) => fixed.push(stripped.to_owned()),
                None => {
                    fixed.push(line);
                    fixed.push("\\ No newline at end of file".to_owned());
                }
            }
        }
        hunk.lines = fixed;
    }
    hunks
}

pub fn create_file_patch(file_name: &str, old: &str, new: &str, context: usize) -> String {
    let mut out = vec![format!("--- {file_name}"), format!("+++ {file_name}")];
    for mut hunk in structured_hunks(old, new, context) {
        if hunk.old_lines == 0 {
            hunk.old_start -= 1;
        }
        if hunk.new_lines == 0 {
            hunk.new_start -= 1;
        }
        out.push(format!(
            "@@ -{},{} +{},{} @@",
            hunk.old_start, hunk.old_lines, hunk.new_start, hunk.new_lines
        ));
        out.extend(hunk.lines);
    }
    out.join("\n") + "\n"
}
