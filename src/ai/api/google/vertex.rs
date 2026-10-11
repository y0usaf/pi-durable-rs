use std::env;

use regex::Regex;
use std::sync::LazyLock;

use super::Target;
use crate::ai::types::{Model, SimpleStreamOptions};

static API_VERSION_PATH: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?:^|/)v\d+(?:beta\d*)?(?:/|$)").expect("valid pattern"));

pub fn target(model: &Model, options: &SimpleStreamOptions) -> Result<Target, String> {
    if let Some(key) = api_key(options.api_key.as_deref()) {
        return Ok(Target {
            base_url: base_url(custom_base(model, options).as_deref(), None),
            headers: vec![("x-goog-api-key".to_owned(), key)],
        });
    }
    let project = env_value("GOOGLE_CLOUD_PROJECT")
        .or_else(|| env_value("GCLOUD_PROJECT"))
        .ok_or_else(|| {
            "Vertex AI requires a project ID. Set GOOGLE_CLOUD_PROJECT/GCLOUD_PROJECT or pass project in options.".to_owned()
        })?;
    let location = env_value("GOOGLE_CLOUD_LOCATION").ok_or_else(|| {
        "Vertex AI requires a location. Set GOOGLE_CLOUD_LOCATION or pass location in options."
            .to_owned()
    })?;
    let token = super::adc::access_token()?;
    Ok(Target {
        base_url: base_url(
            custom_base(model, options).as_deref(),
            Some((&project, &location)),
        ),
        headers: vec![("Authorization".to_owned(), format!("Bearer {token}"))],
    })
}

fn api_key(key: Option<&str>) -> Option<String> {
    let key = key?.trim();
    let placeholder = key == "gcp-vertex-credentials"
        || (key.len() > 2
            && key.starts_with('<')
            && key.ends_with('>')
            && !key[1..key.len() - 1].contains('>'));
    (!key.is_empty() && !placeholder).then(|| key.to_owned())
}

fn custom_base(model: &Model, options: &SimpleStreamOptions) -> Option<String> {
    options
        .base_url
        .as_deref()
        .map(str::trim)
        .filter(|url| !url.is_empty())
        .or_else(|| {
            Some(model.base_url.trim()).filter(|url| !url.is_empty() && !url.contains("{location}"))
        })
        .map(str::to_owned)
}

fn base_url(custom: Option<&str>, project: Option<(&str, &str)>) -> String {
    if let Some(custom) = custom {
        let rest = custom.split_once("://").map_or(custom, |(_, rest)| rest);
        let path = rest.find('/').map_or("", |index| &rest[index..]);
        let root = custom.trim_end_matches('/');
        let versioned = if API_VERSION_PATH.is_match(path) {
            root.to_owned()
        } else {
            format!("{root}/v1")
        };
        return format!("{versioned}/publishers/google");
    }
    match project {
        None => "https://aiplatform.googleapis.com/v1/publishers/google".to_owned(),
        Some((project, location)) => {
            let host = if location == "global" {
                "aiplatform.googleapis.com".to_owned()
            } else {
                format!("{location}-aiplatform.googleapis.com")
            };
            format!("https://{host}/v1/projects/{project}/locations/{location}/publishers/google")
        }
    }
}

fn env_value(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}
