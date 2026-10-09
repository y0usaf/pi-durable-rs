use std::collections::{HashMap, HashSet};

use crate::ai::types::{
    Message, Sections, SystemContent, SystemMessage, SystemRole, Tool, ToolReference,
};
use crate::ai::utils::transcript::{declarations_equal, get_current_tools, to_tool_declaration};
use crate::cancel::CancelToken;
use crate::entries::system_entry;
use crate::errors::{Error, Result};
use crate::harness::context::ContextView;
use crate::harness::types::{PromptInput, PromptSection};
use crate::types::{ContextAction, ContextEdit, EntryDraft};

pub type OrderedSections = Vec<(String, String)>;

fn lookup<'a>(map: &'a OrderedSections, key: &str) -> Option<&'a String> {
    map.iter()
        .find(|(name, _)| name == key)
        .map(|(_, value)| value)
}

fn set_section(map: &mut OrderedSections, key: &str, value: String) {
    match map.iter_mut().find(|(name, _)| name == key) {
        Some(slot) => slot.1 = value,
        None => map.push((key.to_owned(), value)),
    }
}

pub fn replay_sections(messages: &[Message]) -> OrderedSections {
    let mut shown = OrderedSections::new();
    for message in messages {
        let Message::System(system) = message else {
            continue;
        };
        let Some(sections) = &system.sections else {
            continue;
        };
        for (key, value) in &sections.0 {
            match value {
                None => shown.retain(|(name, _)| name != key),
                Some(value) => set_section(&mut shown, key, value.clone()),
            }
        }
    }
    shown
}

pub fn render_sections(
    sections: &[PromptSection],
    input: &PromptInput,
    shown: &OrderedSections,
    report: &mut dyn FnMut(&Error),
    cancel: &CancelToken,
) -> Result<OrderedSections> {
    let mut desired = OrderedSections::new();
    for section in sections {
        let text = match (section.render)(input, cancel) {
            Ok(text) => text,
            Err(error) => {
                if cancel.is_cancelled() {
                    return Err(error);
                }
                report(&error);
                if let Some(kept) = lookup(shown, &section.key) {
                    set_section(&mut desired, &section.key, kept.clone());
                }
                continue;
            }
        };
        let Some(text) = text else {
            continue;
        };
        let value = if section.tag == Some(false) {
            text
        } else {
            format!("<{}>\n{}\n</{}>", section.key, text, section.key)
        };
        set_section(&mut desired, &section.key, value);
    }
    Ok(desired)
}

struct ToolChanges {
    removed: Vec<ToolReference>,
    added: Vec<Tool>,
}

pub fn plan_system_entries(
    view: &ContextView,
    desired: &OrderedSections,
    tools: &[Tool],
    timestamp: u64,
) -> Vec<EntryDraft> {
    let system = system_entry();
    let kind = system.kind().to_owned();
    if view.head.as_ref().is_some_and(|head| {
        !view
            .entries
            .iter()
            .any(|entry| system.is(Some(entry)) && entry.id > head.id)
    }) {
        let edits: Vec<ContextEdit> = view
            .entries
            .iter()
            .filter(|entry| system.is(Some(entry)))
            .map(|entry| ContextEdit {
                target: entry.id,
                action: ContextAction::Omit,
            })
            .collect();
        let changes = ToolChanges {
            removed: Vec::new(),
            added: tools.iter().map(to_tool_declaration).collect(),
        };
        let message = system_message(Some(sections_of(desired)), Some(&changes), timestamp);
        return vec![draft(&kind, message, (!edits.is_empty()).then_some(edits))];
    }
    let sections = plan_sections(&replay_sections(&view.messages), desired);
    let changes = plan_tools(&get_current_tools(&view.messages), tools);
    let changed = !changes.removed.is_empty() || !changes.added.is_empty();
    if !changed {
        return sections
            .into_iter()
            .map(|patch| draft(&kind, system_message(Some(patch), None, timestamp), None))
            .collect();
    }
    if sections.is_empty() {
        let message = system_message(None, Some(&changes), timestamp);
        return vec![draft(&kind, message, None)];
    }
    let last = sections.len() - 1;
    sections
        .into_iter()
        .enumerate()
        .map(|(index, patch)| {
            let tools = (index == last).then_some(&changes);
            draft(&kind, system_message(Some(patch), tools, timestamp), None)
        })
        .collect()
}

fn sections_of(desired: &OrderedSections) -> Sections {
    Sections(
        desired
            .iter()
            .map(|(key, value)| (key.clone(), Some(value.clone())))
            .collect(),
    )
}

fn plan_tools(offered: &[Tool], desired: &[Tool]) -> ToolChanges {
    let wanted: HashMap<&str, &Tool> = desired
        .iter()
        .map(|tool| (tool.name.as_str(), tool))
        .collect();
    let kept: Vec<&Tool> = offered
        .iter()
        .filter(|tool| {
            wanted
                .get(tool.name.as_str())
                .is_some_and(|next| declarations_equal(tool, next))
        })
        .collect();
    let kept_names: HashSet<&str> = kept.iter().map(|tool| tool.name.as_str()).collect();
    let added: Vec<&Tool> = desired
        .iter()
        .filter(|tool| !kept_names.contains(tool.name.as_str()))
        .collect();
    let replayed_names: Vec<&str> = kept
        .iter()
        .chain(added.iter())
        .map(|tool| tool.name.as_str())
        .collect();
    let reordered = replayed_names
        .iter()
        .enumerate()
        .any(|(index, name)| desired.get(index).map(|tool| tool.name.as_str()) != Some(*name));
    if reordered {
        return ToolChanges {
            removed: offered.iter().map(reference).collect(),
            added: desired.iter().map(to_tool_declaration).collect(),
        };
    }
    ToolChanges {
        removed: offered
            .iter()
            .filter(|tool| !kept_names.contains(tool.name.as_str()))
            .map(reference)
            .collect(),
        added: added.into_iter().map(to_tool_declaration).collect(),
    }
}

fn reference(tool: &Tool) -> ToolReference {
    ToolReference {
        name: tool.name.clone(),
    }
}

fn plan_sections(shown: &OrderedSections, desired: &OrderedSections) -> Vec<Sections> {
    let desired_has = |key: &str| lookup(desired, key).is_some();
    let shown_has = |key: &str| lookup(shown, key).is_some();
    let patched_order: Vec<&str> = shown
        .iter()
        .map(|(key, _)| key.as_str())
        .filter(|key| desired_has(key))
        .chain(
            desired
                .iter()
                .map(|(key, _)| key.as_str())
                .filter(|key| !shown_has(key)),
        )
        .collect();
    let desired_order: Vec<&str> = desired.iter().map(|(key, _)| key.as_str()).collect();
    if patched_order != desired_order {
        return vec![
            Sections(shown.iter().map(|(key, _)| (key.clone(), None)).collect()),
            sections_of(desired),
        ];
    }
    let mut patch: Vec<(String, Option<String>)> = Vec::new();
    for (key, value) in shown {
        let next = lookup(desired, key);
        if next != Some(value) {
            patch.push((key.clone(), next.cloned()));
        }
    }
    for (key, value) in desired {
        if !shown_has(key) {
            patch.push((key.clone(), Some(value.clone())));
        }
    }
    if patch.is_empty() {
        Vec::new()
    } else {
        vec![Sections(patch)]
    }
}

fn system_message(
    sections: Option<Sections>,
    tools: Option<&ToolChanges>,
    timestamp: u64,
) -> Message {
    let (tools_removed, tools_added) = match tools {
        Some(changes) => (
            (!changes.removed.is_empty()).then(|| changes.removed.clone()),
            (!changes.added.is_empty()).then(|| changes.added.clone()),
        ),
        None => (None, None),
    };
    Message::System(SystemMessage {
        role: SystemRole,
        content: SystemContent::Text(String::new()),
        sections,
        tools_added,
        tools_removed,
        timestamp,
    })
}

fn draft(kind: &str, message: Message, edits: Option<Vec<ContextEdit>>) -> EntryDraft {
    EntryDraft {
        kind: kind.to_owned(),
        model: Some(vec![message]),
        data: None,
        head: None,
        edits,
    }
}
