use crate::config::{Settings, ZulipConfig};
use anyhow::Result;
use clap::Args;
use serde::Serialize;

#[derive(Debug, Args)]
pub struct AuthLoginArgs {
    #[arg(long, env = "ZULIP_BASE_URL")]
    pub base_url: String,
    #[arg(long, env = "ZULIP_EMAIL")]
    pub email: String,
    #[arg(long, env = "ZULIP_API_KEY", hide_env_values = true)]
    pub api_key: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum AuthStatus {
    Ready { base_url: String, email: String },
    Missing { problems: Vec<String> },
}

#[derive(Debug, Serialize)]
pub struct SavedAuth {
    pub base_url: String,
    pub email: String,
    pub config_file: String,
    pub secret_saved: bool,
    pub secret_file: String,
}

pub fn login(args: AuthLoginArgs) -> Result<SavedAuth> {
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
