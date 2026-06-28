use anyhow::{Context, Result};
use directories::ProjectDirs;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::io::Write;
use std::path::PathBuf;

/// Complete runtime settings after config files, secrets, and environment are merged.
#[derive(Debug, Clone, Serialize)]
pub struct Settings {
    /// Zulip realm and account settings.
    pub zulip: ZulipConfig,
    /// Cache and sync behavior.
    pub cache: CacheConfig,
    /// Terminal UI preferences.
    pub ui: UiConfig,
    /// Safety defaults for potentially mutating actions.
    pub safety: SafetyConfig,
    /// Digest construction preferences.
    pub digest: DigestConfig,
    #[serde(default)]
    /// Named TUI themes keyed by theme name.
    pub themes: BTreeMap<String, ThemeConfig>,
    #[serde(skip)]
    /// Resolved config, cache, and secret paths.
    pub paths: AppPaths,
}

/// Zulip realm and API authentication settings.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ZulipConfig {
    /// Base URL of the Zulip realm.
    pub base_url: Option<String>,
    /// Email address for Zulip API authentication.
    pub email: Option<String>,
    #[serde(skip_serializing, default)]
    /// API key loaded from environment or the local secret file.
    pub api_key: Option<String>,
}

/// Local cache configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    /// Intended cache time-to-live in minutes.
    pub ttl_minutes: i64,
    /// Default recent-message sync window.
    pub default_sync_hours: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Optional override path for the SQLite cache.
    pub path: Option<PathBuf>,
}

/// Terminal UI configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct UiConfig {
    /// Initial view name for UI implementations.
    pub start_view: String,
    /// Selected theme name.
    pub theme: String,
    /// Inline image behavior in the terminal UI.
    pub images: ImageMode,
}

/// Inline image behavior for terminal UI implementations.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ImageMode {
    /// Render images when the terminal advertises a supported protocol.
    Auto,
    /// Show image placeholders without fetching or rendering bytes.
    Placeholder,
    /// Hide image attachment placeholders and rendering entirely.
    Off,
}

/// Optional color overrides for the terminal UI.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ThemeConfig {
    /// Main background color.
    pub background: Option<String>,
    /// Main foreground text color.
    pub text: Option<String>,
    /// Pane border color.
    pub pane_border: Option<String>,
    /// Active pane title color.
    pub active_title: Option<String>,
    /// Inactive pane title color.
    pub inactive_title: Option<String>,
    /// Active selection foreground color.
    pub active_selection_fg: Option<String>,
    /// Active selection background color.
    pub active_selection_bg: Option<String>,
    /// Inactive selection foreground color.
    pub inactive_selection_fg: Option<String>,
    /// Inactive selection background color.
    pub inactive_selection_bg: Option<String>,
    /// Message ID color.
    pub message_id: Option<String>,
    /// Sender-name color.
    pub sender: Option<String>,
    /// Status bar foreground color.
    pub status_fg: Option<String>,
    /// Status bar background color.
    pub status_bg: Option<String>,
    /// Modal and overlay text color.
    pub modal_text: Option<String>,
}

/// Safety configuration for write operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SafetyConfig {
    /// Whether commands should avoid writes unless explicitly enabled.
    pub read_only_by_default: bool,
    /// Whether sends require an explicit confirmation flag.
    pub require_confirmation_for_sends: bool,
}

/// Digest configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DigestConfig {
    #[serde(default)]
    /// Names or phrases that should count as mentions of the user.
    pub aliases: Vec<String>,
}

/// Filesystem paths used by the application.
#[derive(Debug, Clone, Serialize)]
pub struct AppPaths {
    /// Path to the TOML config file.
    pub config_file: PathBuf,
    /// Path to the SQLite cache file.
    pub cache_file: PathBuf,
    /// Path to the API key secret file.
    pub secret_file: PathBuf,
}

#[derive(Debug, Deserialize, Default)]
struct FileSettings {
    zulip: Option<ZulipConfig>,
    cache: Option<CacheConfig>,
    ui: Option<UiConfig>,
    safety: Option<SafetyConfig>,
    digest: Option<DigestConfig>,
    themes: Option<BTreeMap<String, ThemeConfig>>,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            ttl_minutes: 30,
            default_sync_hours: 24,
            path: None,
        }
    }
}

impl Default for UiConfig {
    fn default() -> Self {
        Self {
            start_view: "streams".to_string(),
            theme: "zenburn".to_string(),
            images: ImageMode::Auto,
        }
    }
}

impl Default for SafetyConfig {
    fn default() -> Self {
        Self {
            read_only_by_default: true,
            require_confirmation_for_sends: true,
        }
    }
}

impl Settings {
    /// Load settings from XDG paths, secret files, and environment variables.
    pub fn load() -> Result<Self> {
        let paths = AppPaths::discover()?;
        let mut settings = if paths.config_file.exists() {
            let body = fs::read_to_string(&paths.config_file)
                .with_context(|| format!("reading {}", paths.config_file.display()))?;
            Self::from_file_settings(
                toml::from_str(&body).context("parsing zul config TOML")?,
                paths,
            )
        } else {
            Self::from_file_settings(FileSettings::default(), paths)
        };

        settings.load_secret_file()?;
        settings.apply_env();
        if let Some(path) = settings.cache.path.clone() {
            settings.paths.cache_file = expand_tilde(path);
        }

        Ok(settings)
    }

    fn from_file_settings(file: FileSettings, paths: AppPaths) -> Self {
        Self {
            zulip: file.zulip.unwrap_or_default(),
            cache: file.cache.unwrap_or_default(),
            ui: file.ui.unwrap_or_default(),
            safety: file.safety.unwrap_or_default(),
            digest: file.digest.unwrap_or_default(),
            themes: file.themes.unwrap_or_default(),
            paths,
        }
    }

    fn apply_env(&mut self) {
        if let Ok(value) = env::var("ZULIP_BASE_URL").or_else(|_| env::var("ZULIP_SITE")) {
            self.zulip.base_url = Some(value);
        }
        if let Ok(value) = env::var("ZULIP_EMAIL") {
            self.zulip.email = Some(value);
        }
        if let Ok(value) = env::var("ZULIP_API_KEY") {
            self.zulip.api_key = Some(value);
        }
        if let Ok(value) = env::var("ZUL_CACHE") {
            self.paths.cache_file = PathBuf::from(value);
        }
    }

    /// Return human-readable problems with the current auth configuration.
    pub fn auth_problems(&self) -> Vec<String> {
        let mut problems = Vec::new();
        if self
            .zulip
            .base_url
            .as_deref()
            .unwrap_or_default()
            .is_empty()
        {
            problems.push(
                "missing Zulip base URL; set ZULIP_BASE_URL or config [zulip].base_url".to_string(),
            );
        }
        if self.zulip.email.as_deref().unwrap_or_default().is_empty() {
            problems
                .push("missing Zulip email; set ZULIP_EMAIL or config [zulip].email".to_string());
        }
        if self.zulip.api_key.as_deref().unwrap_or_default().is_empty() {
            problems.push(
                "missing Zulip API key; run zul-cli auth login --api-key ... or set ZULIP_API_KEY"
                    .to_string(),
            );
        }
        problems
    }

    /// Return required auth values or an error describing missing settings.
    pub fn require_auth(&self) -> Result<(String, String, String)> {
        let problems = self.auth_problems();
        if !problems.is_empty() {
            anyhow::bail!(
                "{}\n{}",
                "Zulip auth/config incomplete",
                problems.join("\n")
            );
        }
        Ok((
            self.zulip.base_url.clone().unwrap(),
            self.zulip.email.clone().unwrap(),
            self.zulip.api_key.clone().unwrap(),
        ))
    }

    /// Save non-secret settings to the config file.
    pub fn save_without_secret(&self) -> Result<()> {
        if let Some(parent) = self.paths.config_file.parent() {
            fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
        }
        let serializable = Settings {
            zulip: ZulipConfig {
                base_url: self.zulip.base_url.clone(),
                email: self.zulip.email.clone(),
                api_key: None,
            },
            cache: self.cache.clone(),
            ui: self.ui.clone(),
            safety: self.safety.clone(),
            digest: self.digest.clone(),
            themes: self.themes.clone(),
            paths: self.paths.clone(),
        };
        let mut value = toml::Value::try_from(&serializable)?;
        if let Some(table) = value.as_table_mut() {
            table.remove("paths");
        }
        fs::write(&self.paths.config_file, toml::to_string_pretty(&value)?)
            .with_context(|| format!("writing {}", self.paths.config_file.display()))?;
        Ok(())
    }

    fn load_secret_file(&mut self) -> Result<()> {
        if self.zulip.api_key.is_some() || !self.paths.secret_file.exists() {
            return Ok(());
        }
        let api_key = fs::read_to_string(&self.paths.secret_file)
            .with_context(|| format!("reading {}", self.paths.secret_file.display()))?
            .trim()
            .to_string();
        if !api_key.is_empty() {
            self.zulip.api_key = Some(api_key);
        }
        Ok(())
    }

    /// Save the Zulip API key to the local secret file.
    pub fn save_api_key(&self, api_key: &str) -> Result<()> {
        if let Some(parent) = self.paths.secret_file.parent() {
            fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
        }
        let mut file = fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&self.paths.secret_file)
            .with_context(|| format!("opening {}", self.paths.secret_file.display()))?;
        file.write_all(api_key.as_bytes())
            .with_context(|| format!("writing {}", self.paths.secret_file.display()))?;
        file.write_all(b"\n")?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&self.paths.secret_file, fs::Permissions::from_mode(0o600))
                .with_context(|| format!("chmod 0600 {}", self.paths.secret_file.display()))?;
        }

        Ok(())
    }
}

impl AppPaths {
    /// Discover default XDG paths for config, cache, and secrets.
    pub fn discover() -> Result<Self> {
        let dirs = ProjectDirs::from("", "", "zul").context("unable to discover XDG paths")?;
        Ok(Self {
            config_file: dirs.config_dir().join("config.toml"),
            cache_file: dirs.cache_dir().join("cache.sqlite"),
            secret_file: dirs.config_dir().join("api_key"),
        })
    }
}

fn expand_tilde(path: PathBuf) -> PathBuf {
    let Some(raw) = path.to_str() else {
        return path;
    };
    if raw == "~" {
        return env::var_os("HOME").map(PathBuf::from).unwrap_or(path);
    }
    if let Some(rest) = raw.strip_prefix("~/") {
        if let Some(home) = env::var_os("HOME") {
            return PathBuf::from(home).join(rest);
        }
    }
    path
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_overrides_config() {
        env::set_var("ZULIP_BASE_URL", "https://env.example");
        env::set_var("ZULIP_EMAIL", "env@example.com");
        env::set_var("ZULIP_API_KEY", "secret");
        let mut settings = Settings::from_file_settings(
            FileSettings {
                zulip: Some(ZulipConfig {
                    base_url: Some("https://file.example".to_string()),
                    email: Some("file@example.com".to_string()),
                    api_key: None,
                }),
                ..FileSettings::default()
            },
            AppPaths {
                config_file: PathBuf::from("/tmp/config.toml"),
                cache_file: PathBuf::from("/tmp/cache.sqlite"),
                secret_file: PathBuf::from("/tmp/api_key"),
            },
        );
        settings.apply_env();
        assert_eq!(
            settings.zulip.base_url.as_deref(),
            Some("https://env.example")
        );
        assert_eq!(settings.zulip.email.as_deref(), Some("env@example.com"));
        assert_eq!(settings.zulip.api_key.as_deref(), Some("secret"));
        env::remove_var("ZULIP_BASE_URL");
        env::remove_var("ZULIP_EMAIL");
        env::remove_var("ZULIP_API_KEY");
    }

    #[test]
    fn loads_api_key_from_secret_file() {
        let dir = tempfile::TempDir::new().unwrap();
        let paths = AppPaths {
            config_file: dir.path().join("config.toml"),
            cache_file: dir.path().join("cache.sqlite"),
            secret_file: dir.path().join("api_key"),
        };
        let settings = Settings::from_file_settings(FileSettings::default(), paths.clone());
        settings.save_api_key("local-secret").unwrap();

        let mut loaded = Settings::from_file_settings(FileSettings::default(), paths);
        loaded.load_secret_file().unwrap();

        assert_eq!(loaded.zulip.api_key.as_deref(), Some("local-secret"));
    }

    #[test]
    fn parses_ui_theme_and_custom_theme_from_toml() {
        let file: FileSettings = toml::from_str(
            r##"
            [ui]
            theme = "cozy"
            images = "placeholder"

            [themes.cozy]
            background = "#101820"
            sender = "#BFEBBF"
            "##,
        )
        .unwrap();

        let ui = file.ui.unwrap();
        let themes = file.themes.unwrap();

        assert_eq!(ui.start_view, "streams");
        assert_eq!(ui.theme, "cozy");
        assert_eq!(ui.images, ImageMode::Placeholder);
        assert_eq!(themes["cozy"].background.as_deref(), Some("#101820"));
    }
}
