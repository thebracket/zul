use crate::config::{Settings, ZulipConfig};
use anyhow::Result;
use serde::Serialize;

/// Credentials supplied by a caller when saving Zulip authentication settings.
#[derive(Debug, Clone)]
pub struct AuthLogin {
    /// Base URL of the Zulip realm, for example `https://chat.example.com`.
    pub base_url: String,
    /// Email address used for Zulip API authentication.
    pub email: String,
    /// Optional API key to persist in the local secret file.
    pub api_key: Option<String>,
}

/// Current authentication readiness for the configured Zulip account.
#[derive(Debug, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum AuthStatus {
    /// Required local settings are present and the account check succeeded.
    Ready {
        /// Zulip realm URL.
        base_url: String,
        /// Zulip account email.
        email: String,
    },
    /// One or more settings or remote checks are missing or failed.
    Missing {
        /// Human-readable reasons authentication is incomplete.
        problems: Vec<String>,
    },
}

/// Result returned after saving authentication settings.
#[derive(Debug, Serialize)]
pub struct SavedAuth {
    /// Saved Zulip realm URL.
    pub base_url: String,
    /// Saved Zulip account email.
    pub email: String,
    /// Path to the config file that was written.
    pub config_file: String,
    /// Whether an API key was provided and stored.
    pub secret_saved: bool,
    /// Path to the local API key file.
    pub secret_file: String,
}

/// Save Zulip realm/account settings and optionally persist the API key.
pub fn login(args: AuthLogin) -> Result<SavedAuth> {
    let mut settings = Settings::load()?;
    settings.zulip = ZulipConfig {
        base_url: Some(args.base_url.trim_end_matches('/').to_string()),
        email: Some(args.email),
        api_key: None,
    };
    settings.save_without_secret()?;
    let secret_saved = if let Some(api_key) = args
        .api_key
        .as_deref()
        .map(str::trim)
        .filter(|key| !key.is_empty())
    {
        settings.save_api_key(api_key)?;
        true
    } else {
        false
    };
    Ok(SavedAuth {
        base_url: settings.zulip.base_url.unwrap(),
        email: settings.zulip.email.unwrap(),
        config_file: settings.paths.config_file.display().to_string(),
        secret_saved,
        secret_file: settings.paths.secret_file.display().to_string(),
    })
}
