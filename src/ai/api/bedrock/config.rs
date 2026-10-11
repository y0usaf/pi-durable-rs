use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

const UNSUPPORTED: [&str; 6] = [
    "role_arn",
    "source_profile",
    "credential_process",
    "sso_session",
    "sso_start_url",
    "web_identity_token_file",
];

pub struct Credentials {
    pub access_key: String,
    pub secret_key: String,
    pub session_token: Option<String>,
}

pub fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

pub fn credentials() -> Result<Credentials, String> {
    let profile = env("AWS_PROFILE");
    if profile.is_none()
        && let (Some(access_key), Some(secret_key)) =
            (env("AWS_ACCESS_KEY_ID"), env("AWS_SECRET_ACCESS_KEY"))
    {
        return Ok(Credentials {
            access_key,
            secret_key,
            session_token: env("AWS_SESSION_TOKEN"),
        });
    }
    profile_credentials(profile.as_deref().unwrap_or("default"))
}

pub fn profile_region(name: &str) -> Result<Option<String>, String> {
    Ok(section(config_file().as_deref(), &config_header(name))?.remove("region"))
}

fn profile_credentials(name: &str) -> Result<Credentials, String> {
    let shared = section(credentials_file().as_deref(), name)?;
    let config = section(config_file().as_deref(), &config_header(name))?;
    if shared.is_empty() && config.is_empty() {
        return Err(format!(
            "amazon-bedrock: no AWS credentials: set AWS_BEARER_TOKEN_BEDROCK or AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY, or profile {name} in the AWS credentials file"
        ));
    }
    if let Some(key) = UNSUPPORTED
        .into_iter()
        .find(|key| shared.contains_key(*key) || config.contains_key(*key))
    {
        return Err(format!(
            "amazon-bedrock: profile {name} sets {key}, which is not supported; use static keys or AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY"
        ));
    }
    let lookup = |key: &str| shared.get(key).or_else(|| config.get(key)).cloned();
    match (lookup("aws_access_key_id"), lookup("aws_secret_access_key")) {
        (Some(access_key), Some(secret_key)) => Ok(Credentials {
            access_key,
            secret_key,
            session_token: lookup("aws_session_token"),
        }),
        _ => Err(format!(
            "amazon-bedrock: profile {name} lacks aws_access_key_id or aws_secret_access_key"
        )),
    }
}

fn config_header(name: &str) -> String {
    if name == "default" {
        name.to_owned()
    } else {
        format!("profile {name}")
    }
}

fn credentials_file() -> Option<PathBuf> {
    aws_file("AWS_SHARED_CREDENTIALS_FILE", "credentials")
}

fn config_file() -> Option<PathBuf> {
    aws_file("AWS_CONFIG_FILE", "config")
}

fn aws_file(var: &str, leaf: &str) -> Option<PathBuf> {
    env(var)
        .map(PathBuf::from)
        .or_else(|| env("HOME").map(|home| PathBuf::from(home).join(".aws").join(leaf)))
}

fn section(path: Option<&Path>, header: &str) -> Result<BTreeMap<String, String>, String> {
    let Some(path) = path else {
        return Ok(BTreeMap::new());
    };
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(BTreeMap::new());
        }
        Err(error) => return Err(format!("amazon-bedrock: read {}: {error}", path.display())),
    };
    let mut values = BTreeMap::new();
    let mut inside = false;
    for line in text.lines().map(str::trim) {
        if line.is_empty() || line.starts_with(['#', ';']) {
            continue;
        }
        if let Some(name) = line
            .strip_prefix('[')
            .and_then(|rest| rest.strip_suffix(']'))
        {
            inside = name.trim() == header;
        } else if let Some((key, value)) = line.split_once('=')
            && inside
        {
            values.insert(key.trim().to_owned(), value.trim().to_owned());
        }
    }
    Ok(values)
}
