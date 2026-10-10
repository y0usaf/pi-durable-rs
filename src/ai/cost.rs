use super::types::{Model, Usage};

pub fn calculate_cost(model: &Model, usage: &mut Usage) {
    let Some(cost) = &model.cost else {
        return;
    };
    let input_tokens = (usage.input + usage.cache_read + usage.cache_write) as f64;
    let mut rates = (cost.input, cost.output, cost.cache_read, cost.cache_write);
    let mut matched = -1.0;
    for tier in &cost.tiers {
        let above = tier.input_tokens_above as f64;
        if input_tokens > above && above > matched {
            rates = (tier.input, tier.output, tier.cache_read, tier.cache_write);
            matched = above;
        }
    }
    let (input, output, cache_read, cache_write) = rates;
    let long_write = usage.cache_write_1h.unwrap_or(0) as f64;
    let short_write = usage.cache_write as f64 - long_write;
    usage.cost.input = input / 1_000_000.0 * usage.input as f64;
    usage.cost.output = output / 1_000_000.0 * usage.output as f64;
    usage.cost.cache_read = cache_read / 1_000_000.0 * usage.cache_read as f64;
    usage.cost.cache_write = (cache_write * short_write + input * 2.0 * long_write) / 1_000_000.0;
    usage.cost.total =
        usage.cost.input + usage.cost.output + usage.cost.cache_read + usage.cost.cache_write;
}
