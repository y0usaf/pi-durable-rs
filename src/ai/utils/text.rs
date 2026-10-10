use crate::ai::types::{SystemBlock, SystemContent, SystemMessage};

pub fn utf16_len(text: &str) -> usize {
    text.bytes()
        .map(|b| usize::from(b & 0xC0 != 0x80) + usize::from(b >= 0xF0))
        .sum()
}

pub fn system_content_text(content: &SystemContent) -> String {
    match content {
        SystemContent::Text(text) => text.clone(),
        SystemContent::Blocks(blocks) => blocks
            .iter()
            .map(|SystemBlock::Text(block)| block.text.as_str())
            .collect::<Vec<_>>()
            .join("\n"),
    }
}

pub fn system_message_text(message: &SystemMessage) -> String {
    let mut parts = vec![system_content_text(&message.content)];
    if let Some(sections) = &message.sections {
        parts.extend(sections.0.iter().filter_map(|(_, text)| text.clone()));
    }
    parts
        .into_iter()
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("\n\n")
}
