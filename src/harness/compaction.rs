use crate::ai::types::{AssistantMessage, Message};
use crate::ai::utils::estimate::{calculate_context_tokens, estimate_message_tokens};
use crate::harness::context::ContextView;

pub fn estimate_context(view: &ContextView, extra: &[Message]) -> u64 {
    let after = view.head.as_ref().map(|head| head.id);
    let measured: Option<&AssistantMessage> = (0..view.entries.len())
        .rev()
        .filter(|&index| after.is_none_or(|after| view.entries[index].id > after))
        .find_map(|index| {
            view.contributions[index]
                .iter()
                .rev()
                .find_map(|message| match message {
                    Message::Assistant(assistant)
                        if calculate_context_tokens(&assistant.usage) > 0 =>
                    {
                        Some(assistant.as_ref())
                    }
                    _ => None,
                })
        });
    let (from, mut tokens) = match measured {
        None => (0, 0),
        Some(measured) => {
            let position = view
                .messages
                .iter()
                .rposition(
                    |message| matches!(message, Message::Assistant(a) if a.as_ref() == measured),
                )
                .map_or(0, |index| index + 1);
            (position, calculate_context_tokens(&measured.usage))
        }
    };
    for message in view.messages.iter().skip(from).chain(extra) {
        tokens += estimate_message_tokens(message);
    }
    tokens
}
