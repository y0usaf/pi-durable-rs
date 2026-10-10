use crate::ai::types::Model;

#[cfg(feature = "anthropic")]
pub fn anthropic() -> Vec<Model> {
    Vec::new()
}

#[cfg(feature = "vercel-gateway")]
pub fn vercel_gateway() -> Vec<Model> {
    Vec::new()
}

#[cfg(feature = "openai")]
pub fn openai() -> Vec<Model> {
    Vec::new()
}
