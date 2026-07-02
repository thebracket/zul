use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ego_tree::NodeRef;
#[cfg(feature = "tui-images")]
use image::ImageReader;
use libzul::app::{App, PollResult};
use libzul::config::{
    CacheConfig, DigestConfig, ImageMode, SafetyConfig, Settings, ThemeConfig, UiConfig,
    ZulipConfig,
};
use libzul::digest::{self, Digest};
use libzul::zulip::client::ZulipClient;
use libzul::zulip::models::{
    ImageAttachment, ImageBytes, ZulipDirectConversation, ZulipMessage, ZulipMessageType,
    ZulipStream, ZulipTopic,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, Paragraph, Wrap};
use ratatui::{Frame, Terminal};
#[cfg(feature = "tui-images")]
use ratatui_image::picker::{Picker, ProtocolType};
#[cfg(feature = "tui-images")]
use ratatui_image::protocol::StatefulProtocol;
#[cfg(feature = "tui-images")]
use ratatui_image::{Resize, StatefulImage};
use scraper::{ElementRef, Html, Node};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
#[cfg(feature = "tui-images")]
use std::io::Cursor;
use std::io::{self, Stdout};
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, Sender};
use std::time::{Duration, Instant};
use unicode_segmentation::UnicodeSegmentation;
use unicode_width::UnicodeWidthStr;

const POLL_INTERVAL: Duration = Duration::from_secs(5);
const IMAGE_ROWS: u16 = 8;

type PollEvent = Result<PollResult, String>;
type ImageEvent = (ImageAttachment, Result<ImageBytes, String>);
#[cfg(feature = "tui-images")]
type ImagePicker = Picker;
#[cfg(not(feature = "tui-images"))]
struct ImagePicker;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Focus {
    Streams,
    Topics,
    Thread,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Mode {
    Browser,
    SearchInput,
    SearchResults,
    Digest,
    Composer,
    ConfirmSend,
    Command,
    Auth,
    Login,
    Config,
    Help,
}

struct TuiState {
    streams: Vec<ZulipStream>,
    topics: Vec<ZulipTopic>,
    direct_conversations: Vec<ZulipDirectConversation>,
    messages: Vec<ZulipMessage>,
    search_results: Vec<ZulipMessage>,
    digest: Option<Digest>,
    stream_activity: BTreeMap<String, i64>,
    direct_message_count: i64,
    stream_idx: usize,
    topic_idx: usize,
    message_idx: usize,
    popup_scroll: usize,
    focus: Focus,
    mode: Mode,
    input: String,
    draft: String,
    form: Option<ConfigForm>,
    status: String,
    last_poll_at: Option<DateTime<Utc>>,
    last_poll_new: usize,
    poll_error: Option<String>,
    image_entries: BTreeMap<String, ThreadImageEntry>,
    image_inflight: BTreeSet<String>,
}

struct ThreadImageEntry {
    #[cfg(feature = "tui-images")]
    protocol: Option<StatefulProtocol>,
    #[cfg(feature = "tui-images")]
    error: Option<String>,
}

enum ReplyDestination {
    Stream { stream: String, topic: String },
    Direct(ZulipDirectConversation),
}

/// Raw Zulip credentials found in the process environment after dotenv loading.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StartupCredentials {
    /// Zulip realm base URL from `ZULIP_BASE_URL` or `ZULIP_SITE`.
    pub base_url: String,
    /// Zulip email from `ZULIP_EMAIL`.
    pub email: String,
    /// Zulip API key from `ZULIP_API_KEY`.
    pub api_key: String,
}

impl StartupCredentials {
    /// Read startup credentials directly from the environment without defaults.
    pub fn from_env() -> Self {
        Self {
            base_url: std::env::var("ZULIP_BASE_URL")
                .or_else(|_| std::env::var("ZULIP_SITE"))
                .unwrap_or_default(),
            email: std::env::var("ZULIP_EMAIL").unwrap_or_default(),
            api_key: std::env::var("ZULIP_API_KEY").unwrap_or_default(),
        }
    }

    fn is_complete(&self) -> bool {
        !self.base_url.trim().is_empty()
            && !self.email.trim().is_empty()
            && !self.api_key.trim().is_empty()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConfigPurpose {
    Login,
    Config,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConfigField {
    BaseUrl,
    Email,
    ApiKey,
    CacheTtlMinutes,
    SyncHours,
    CachePath,
    StartView,
    Theme,
    Images,
    ReadOnlyByDefault,
    ConfirmSends,
    DigestAliases,
}

struct FormField {
    key: ConfigField,
    label: &'static str,
    value: String,
    secret: bool,
}

struct ConfigForm {
    purpose: ConfigPurpose,
    fields: Vec<FormField>,
    selected: usize,
    message: String,
}

impl ConfigForm {
    fn login(settings: &Settings) -> Self {
        Self::login_with_values(
            settings.zulip.base_url.clone().unwrap_or_default(),
            settings.zulip.email.clone().unwrap_or_default(),
            String::new(),
        )
    }

    fn login_from_startup(credentials: &StartupCredentials) -> Self {
        Self::login_with_values(
            credentials.base_url.clone(),
            credentials.email.clone(),
            credentials.api_key.clone(),
        )
    }

    fn login_with_values(base_url: String, email: String, api_key: String) -> Self {
        Self {
            purpose: ConfigPurpose::Login,
            fields: vec![
                FormField::new(ConfigField::BaseUrl, "Base URL", base_url),
                FormField::new(ConfigField::Email, "Email", email),
                FormField::secret(ConfigField::ApiKey, "API key", api_key),
            ],
            selected: 0,
            message: "Enter Zulip credentials, then press Ctrl-S to save.".to_string(),
        }
    }

    fn config(settings: &Settings) -> Self {
        Self {
            purpose: ConfigPurpose::Config,
            fields: vec![
                FormField::new(
                    ConfigField::BaseUrl,
                    "Base URL",
                    settings.zulip.base_url.clone().unwrap_or_default(),
                ),
                FormField::new(
                    ConfigField::Email,
                    "Email",
                    settings.zulip.email.clone().unwrap_or_default(),
                ),
                FormField::secret(ConfigField::ApiKey, "API key", String::new()),
                FormField::new(
                    ConfigField::CacheTtlMinutes,
                    "Cache TTL minutes",
                    settings.cache.ttl_minutes.to_string(),
                ),
                FormField::new(
                    ConfigField::SyncHours,
                    "Sync hours",
                    settings.cache.default_sync_hours.to_string(),
                ),
                FormField::new(
                    ConfigField::CachePath,
                    "Cache path",
                    settings
                        .cache
                        .path
                        .as_ref()
                        .map(|path| path.display().to_string())
                        .unwrap_or_default(),
                ),
                FormField::new(
                    ConfigField::StartView,
                    "Start view",
                    settings.ui.start_view.clone(),
                ),
                FormField::new(ConfigField::Theme, "Theme", settings.ui.theme.clone()),
                FormField::new(
                    ConfigField::Images,
                    "Images",
                    image_mode_value(settings.ui.images).to_string(),
                ),
                FormField::new(
                    ConfigField::ReadOnlyByDefault,
                    "Read only by default",
                    settings.safety.read_only_by_default.to_string(),
                ),
                FormField::new(
                    ConfigField::ConfirmSends,
                    "Confirm sends",
                    settings.safety.require_confirmation_for_sends.to_string(),
                ),
                FormField::new(
                    ConfigField::DigestAliases,
                    "Digest aliases",
                    settings.digest.aliases.join(", "),
                ),
            ],
            selected: 0,
            message: "Edit values, then press Ctrl-S to save. Blank API key keeps the current key."
                .to_string(),
        }
    }

    fn selected_field_mut(&mut self) -> Option<&mut FormField> {
        self.fields.get_mut(self.selected)
    }

    fn selected_field(&self) -> Option<&FormField> {
        self.fields.get(self.selected)
    }

    fn move_selection(&mut self, delta: isize) {
        self.selected = move_index(self.selected, self.fields.len(), delta);
    }

    fn value(&self, key: ConfigField) -> &str {
        self.fields
            .iter()
            .find(|field| field.key == key)
            .map(|field| field.value.as_str())
            .unwrap_or("")
    }
}

impl FormField {
    fn new(key: ConfigField, label: &'static str, value: String) -> Self {
        Self {
            key,
            label,
            value,
            secret: false,
        }
    }

    fn secret(key: ConfigField, label: &'static str, value: String) -> Self {
        Self {
            key,
            label,
            value,
            secret: true,
        }
    }

    fn display_value(&self) -> String {
        if self.secret && !self.value.is_empty() {
            "*".repeat(self.value.graphemes(true).count())
        } else {
            self.value.clone()
        }
    }
}

impl TuiState {
    fn load(app: &App) -> Result<Self> {
        Self::load_inner(app, None)
    }

    fn load_for_startup(app: &App, credentials: &StartupCredentials) -> Result<Self> {
        Self::load_inner(app, Some(credentials))
    }

    fn load_inner(app: &App, startup_credentials: Option<&StartupCredentials>) -> Result<Self> {
        let mut state = Self {
            streams: app.streams()?,
            topics: Vec::new(),
            direct_conversations: Vec::new(),
            messages: Vec::new(),
            search_results: Vec::new(),
            digest: None,
            stream_activity: app
                .stream_activity()?
                .into_iter()
                .map(|activity| (activity.stream_name, activity.message_count))
                .collect(),
            direct_message_count: app.direct_message_count()?,
            stream_idx: 0,
            topic_idx: 0,
            message_idx: 0,
            popup_scroll: 0,
            focus: Focus::Streams,
            mode: Mode::Browser,
            input: String::new(),
            draft: String::new(),
            form: None,
            status: "cache ready".to_string(),
            last_poll_at: None,
            last_poll_new: 0,
            poll_error: None,
            image_entries: BTreeMap::new(),
            image_inflight: BTreeSet::new(),
        };
        state.reload_topics(app)?;
        state.reload_messages(app)?;
        state.select_latest_message();
        if let Some(credentials) =
            startup_credentials.filter(|credentials| !credentials.is_complete())
        {
            state.form = Some(ConfigForm::login_from_startup(credentials));
            state.mode = Mode::Login;
            state.status = "credentials required".to_string();
        } else if !app.settings().auth_problems().is_empty() {
            state.form = Some(ConfigForm::login(app.settings()));
            state.mode = Mode::Login;
            state.status = "credentials required".to_string();
        }
        Ok(state)
    }

    fn selected_stream(&self) -> Option<&str> {
        if self.selected_direct_messages() {
            return None;
        }
        self.streams
            .get(self.stream_idx)
            .map(|stream| stream.name.as_str())
    }

    fn selected_direct_messages(&self) -> bool {
        self.stream_idx == self.streams.len()
    }

    fn selected_topic(&self) -> Option<&str> {
        self.topics
            .get(self.topic_idx)
            .map(|topic| topic.name.as_str())
    }

    fn selected_direct_conversation(&self) -> Option<&ZulipDirectConversation> {
        self.selected_direct_messages()
            .then(|| self.direct_conversations.get(self.topic_idx))
            .flatten()
    }

    fn middle_len(&self) -> usize {
        if self.selected_direct_messages() {
            self.direct_conversations.len()
        } else {
            self.topics.len()
        }
    }

    fn reload_topics(&mut self, app: &App) -> Result<()> {
        if self.selected_direct_messages() {
            self.topics = Vec::new();
            self.direct_conversations = app.direct_conversations()?;
        } else {
            self.direct_conversations = Vec::new();
            self.topics = match self.selected_stream() {
                Some(stream) => app.topics(stream)?,
                None => Vec::new(),
            };
        }
        self.topic_idx = self.topic_idx.min(self.middle_len().saturating_sub(1));
        Ok(())
    }

    fn reload_messages(&mut self, app: &App) -> Result<()> {
        self.messages = if let Some(conversation) = self.selected_direct_conversation() {
            app.direct_conversation(&conversation.key)?
        } else {
            match (self.selected_stream(), self.selected_topic()) {
                (Some(stream), Some(topic)) => app.topic(stream, topic)?,
                _ => Vec::new(),
            }
        };
        self.message_idx = self.message_idx.min(self.messages.len().saturating_sub(1));
        Ok(())
    }

    fn select_latest_message(&mut self) {
        self.message_idx = self.messages.len().saturating_sub(1);
    }

    fn move_selection(&mut self, delta: isize, app: &App) -> Result<()> {
        match self.focus {
            Focus::Streams => {
                self.stream_idx = move_index(self.stream_idx, self.streams.len() + 1, delta);
                self.topic_idx = 0;
                self.message_idx = 0;
                self.reload_topics(app)?;
                self.reload_messages(app)?;
                self.select_latest_message();
            }
            Focus::Topics => {
                self.topic_idx = move_index(self.topic_idx, self.middle_len(), delta);
                self.message_idx = 0;
                self.reload_messages(app)?;
                self.select_latest_message();
            }
            Focus::Thread => {
                self.message_idx = move_index(self.message_idx, self.messages.len(), delta);
            }
        }
        Ok(())
    }

    fn thread_at_end(&self) -> bool {
        self.messages.is_empty() || self.message_idx >= self.messages.len().saturating_sub(1)
    }

    fn refresh_from_cache(&mut self, app: &App, follow_if_was_at_end: bool) -> Result<()> {
        let selected_stream = self.selected_stream().map(ToString::to_string);
        let selected_topic = self.selected_topic().map(ToString::to_string);
        let selected_direct = self.selected_direct_messages();
        let selected_direct_key = self
            .selected_direct_conversation()
            .map(|conversation| conversation.key.clone());
        let selected_message_id = self
            .messages
            .get(self.message_idx)
            .map(|message| message.id);

        self.streams = app.streams()?;
        self.stream_activity = app
            .stream_activity()?
            .into_iter()
            .map(|activity| (activity.stream_name, activity.message_count))
            .collect();
        self.direct_message_count = app.direct_message_count()?;

        self.stream_idx = if selected_direct {
            self.streams.len()
        } else {
            selected_stream
                .as_ref()
                .and_then(|name| self.streams.iter().position(|stream| &stream.name == name))
                .unwrap_or_else(|| self.stream_idx.min(self.streams.len()))
        };

        self.reload_topics(app)?;
        self.topic_idx = if selected_direct {
            selected_direct_key
                .as_ref()
                .and_then(|key| {
                    self.direct_conversations
                        .iter()
                        .position(|conversation| &conversation.key == key)
                })
                .unwrap_or_else(|| self.topic_idx.min(self.middle_len().saturating_sub(1)))
        } else {
            selected_topic
                .as_ref()
                .and_then(|name| self.topics.iter().position(|topic| &topic.name == name))
                .unwrap_or_else(|| self.topic_idx.min(self.middle_len().saturating_sub(1)))
        };

        self.reload_messages(app)?;
        if follow_if_was_at_end {
            self.message_idx = self.messages.len().saturating_sub(1);
        } else if let Some(message_id) = selected_message_id {
            self.message_idx = self
                .messages
                .iter()
                .position(|message| message.id == message_id)
                .unwrap_or_else(|| self.message_idx.min(self.messages.len().saturating_sub(1)));
        } else {
            self.message_idx = self.message_idx.min(self.messages.len().saturating_sub(1));
        }

        Ok(())
    }

    fn focus_left(&mut self) {
        self.focus = match self.focus {
            Focus::Streams => Focus::Streams,
            Focus::Topics => Focus::Streams,
            Focus::Thread => Focus::Topics,
        };
    }

    fn focus_right(&mut self) {
        self.focus = match self.focus {
            Focus::Streams => Focus::Topics,
            Focus::Topics => Focus::Thread,
            Focus::Thread => Focus::Thread,
        };
    }
}

pub async fn run(app: App) -> Result<()> {
    run_with_startup_credentials(app, StartupCredentials::from_env()).await
}

/// Run the TUI using an already captured startup credential snapshot.
pub async fn run_with_startup_credentials(
    app: App,
    startup_credentials: StartupCredentials,
) -> Result<()> {
    let theme = TuiTheme::resolve(app.settings())?;
    let mut terminal = setup_terminal()?;
    let image_picker = detect_image_picker(app.settings());
    let result = run_loop(&mut terminal, app, theme, image_picker, startup_credentials).await;
    restore_terminal(&mut terminal)?;
    result
}

async fn run_loop(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    mut app: App,
    mut theme: TuiTheme,
    mut image_picker: Option<ImagePicker>,
    startup_credentials: StartupCredentials,
) -> Result<()> {
    let mut state = TuiState::load_for_startup(&app, &startup_credentials)?;
    let (poll_tx, poll_rx) = mpsc::channel();
    let (image_tx, image_rx) = mpsc::channel();
    let mut next_poll_at = Instant::now() + POLL_INTERVAL;
    let mut poll_inflight = false;

    loop {
        drain_poll_events(&poll_rx, &mut poll_inflight, &mut state, &app)?;
        drain_image_events(&image_rx, &mut state, image_picker.as_ref());
        if state.mode != Mode::Login
            && !app.offline()
            && !poll_inflight
            && Instant::now() >= next_poll_at
        {
            state.status = "polling Zulip...".to_string();
            spawn_poll(app.settings().clone(), app.offline(), poll_tx.clone());
            poll_inflight = true;
            next_poll_at = Instant::now() + POLL_INTERVAL;
        }
        if state.mode != Mode::Login {
            maybe_spawn_selected_image_fetch(
                &mut state,
                &app,
                image_tx.clone(),
                image_picker.as_ref().is_some(),
            );
        }

        terminal.draw(|frame| draw(frame, &mut state, &app, &theme, image_picker.as_ref()))?;
        if !event::poll(Duration::from_millis(150))? {
            continue;
        }
        let Event::Key(key) = event::read()? else {
            continue;
        };
        if handle_key(key, &mut state, &mut app, &mut theme, &mut image_picker).await? {
            break;
        }
    }
    Ok(())
}

async fn handle_key(
    key: KeyEvent,
    state: &mut TuiState,
    app: &mut App,
    theme: &mut TuiTheme,
    image_picker: &mut Option<ImagePicker>,
) -> Result<bool> {
    match state.mode {
        Mode::SearchInput | Mode::Command | Mode::Composer => {
            return handle_input_key(key, state, app).await;
        }
        Mode::SearchResults | Mode::Digest | Mode::Auth | Mode::Help => {
            return handle_scrollable_mode_key(key, state);
        }
        Mode::Login | Mode::Config => {
            return handle_config_key(key, state, app, theme, image_picker).await;
        }
        Mode::ConfirmSend => {
            match key.code {
                KeyCode::Char('y') | KeyCode::Char('Y') => {
                    let Some(destination) = selected_reply_destination(state) else {
                        state.status = "no conversation selected".to_string();
                        state.mode = Mode::Browser;
                        return Ok(false);
                    };
                    let draft = state.draft.clone();
                    let send_result = match destination {
                        ReplyDestination::Stream { stream, topic } => {
                            app.reply(&stream, &topic, &draft, false, true).await
                        }
                        ReplyDestination::Direct(conversation) => {
                            app.reply_direct(&conversation, &draft, false, true).await
                        }
                    };
                    match send_result {
                        Ok(_) => {
                            let was_at_end = state.thread_at_end();
                            match app.poll_new_messages().await {
                                Ok(result) => {
                                    state.refresh_from_cache(app, true)?;
                                    state.last_poll_at = Some(Utc::now());
                                    state.last_poll_new = result.new_messages;
                                    state.poll_error = None;
                                    state.status = format!(
                                        "sent draft; {} new messages, {} refreshed",
                                        result.new_messages, result.refreshed_messages
                                    );
                                }
                                Err(err) => {
                                    if was_at_end {
                                        state.refresh_from_cache(app, true)?;
                                    }
                                    state.status = format!("sent draft; refresh failed: {err}");
                                }
                            }
                            state.draft.clear();
                            state.mode = Mode::Browser;
                        }
                        Err(err) => {
                            state.status = format!("send failed: {err}");
                            state.mode = Mode::Composer;
                        }
                    }
                }
                KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => {
                    state.status = "send cancelled".to_string();
                    state.mode = Mode::Composer;
                }
                _ => {}
            }
            return Ok(false);
        }
        _ => {}
    }

    match key.code {
        KeyCode::Char('q') => {
            if state.mode == Mode::Browser {
                return Ok(true);
            }
            state.mode = Mode::Browser;
        }
        KeyCode::Char('j') | KeyCode::Down => state.move_selection(1, app)?,
        KeyCode::Char('k') | KeyCode::Up => state.move_selection(-1, app)?,
        KeyCode::Char('h') | KeyCode::Left | KeyCode::BackTab => state.focus_left(),
        KeyCode::Char('l') | KeyCode::Right | KeyCode::Tab => state.focus_right(),
        KeyCode::Char('/') => {
            state.input.clear();
            state.mode = Mode::SearchInput;
            state.status = "search cache".to_string();
        }
        KeyCode::Char(':') => {
            state.input.clear();
            state.mode = Mode::Command;
        }
        KeyCode::Char('p') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            state.input.clear();
            state.mode = Mode::Command;
        }
        KeyCode::Char('r') => {
            state.draft.clear();
            state.mode = Mode::Composer;
        }
        KeyCode::Char('R') => match app.sync(app.settings().cache.default_sync_hours).await {
            Ok(sync) => {
                let was_at_end = state.thread_at_end();
                state.refresh_from_cache(app, was_at_end)?;
                state.status = format!("synced {} messages", sync.messages);
            }
            Err(err) => state.status = format!("sync failed: {err}"),
        },
        KeyCode::Char('o') | KeyCode::Char('y') => {
            if let Some(message) = state.messages.get(state.message_idx) {
                state.status = message
                    .permalink
                    .clone()
                    .unwrap_or_else(|| format!("message id {}", message.id));
            }
        }
        KeyCode::Char('a') => {
            state.popup_scroll = 0;
            state.mode = Mode::Auth;
        }
        KeyCode::Char('c') => {
            state.form = Some(ConfigForm::config(app.settings()));
            state.popup_scroll = 0;
            state.mode = Mode::Config;
        }
        KeyCode::Char('?') => {
            state.popup_scroll = 0;
            state.mode = Mode::Help;
        }
        KeyCode::Char('L') => {
            logout(state, app, theme, image_picker)?;
        }
        KeyCode::Char('d') => {
            state.digest = Some(digest::build_digest(
                app.recent_messages(24)?,
                24,
                &app.settings().digest.aliases,
            ));
            state.popup_scroll = 0;
            state.mode = Mode::Digest;
        }
        _ => {}
    }
    Ok(false)
}

fn drain_poll_events(
    poll_rx: &Receiver<PollEvent>,
    poll_inflight: &mut bool,
    state: &mut TuiState,
    app: &App,
) -> Result<()> {
    while let Ok(event) = poll_rx.try_recv() {
        *poll_inflight = false;
        match event {
            Ok(result) => {
                let was_at_end = state.thread_at_end();
                state.refresh_from_cache(app, was_at_end)?;
                state.last_poll_at = Some(Utc::now());
                state.last_poll_new = result.new_messages;
                state.poll_error = None;
                if result.new_messages > 0 {
                    state.status = format!("{} new messages", result.new_messages);
                } else if result.refreshed_messages > 0 {
                    state.status =
                        format!("{} authored messages refreshed", result.refreshed_messages);
                } else if state.status == "polling Zulip..." {
                    state.status = "no new messages".to_string();
                }
            }
            Err(err) => {
                state.poll_error = Some(err.clone());
                state.status = format!("poll failed: {err}");
            }
        }
    }
    Ok(())
}

fn spawn_poll(settings: Settings, offline: bool, poll_tx: Sender<PollEvent>) {
    tokio::spawn(async move {
        let result = App::poll_new_messages_with_settings(settings, offline)
            .await
            .map_err(|err| err.to_string());
        let _ = poll_tx.send(result);
    });
}

#[cfg(feature = "tui-images")]
fn detect_image_picker(settings: &Settings) -> Option<ImagePicker> {
    if settings.ui.images != ImageMode::Auto {
        return None;
    }
    let Ok(mut picker) = Picker::from_query_stdio() else {
        return None;
    };
    if terminal_env_prefers_iterm2_protocol(
        std::env::var("TERM_PROGRAM").ok().as_deref(),
        std::env::var("LC_TERMINAL").ok().as_deref(),
        std::env::var("ITERM_SESSION_ID").ok().as_deref(),
    ) {
        picker.set_protocol_type(ProtocolType::Iterm2);
    }
    (picker.protocol_type() != ProtocolType::Halfblocks).then_some(picker)
}

#[cfg(not(feature = "tui-images"))]
fn detect_image_picker(_settings: &Settings) -> Option<ImagePicker> {
    None
}

#[cfg(feature = "tui-images")]
fn terminal_env_prefers_iterm2_protocol(
    term_program: Option<&str>,
    lc_terminal: Option<&str>,
    iterm_session_id: Option<&str>,
) -> bool {
    term_program.is_some_and(|value| value.contains("iTerm"))
        || lc_terminal.is_some_and(|value| value.contains("iTerm"))
        || iterm_session_id.is_some_and(|value| !value.is_empty())
}

#[cfg(feature = "tui-images")]
fn drain_image_events(
    image_rx: &Receiver<ImageEvent>,
    state: &mut TuiState,
    picker: Option<&ImagePicker>,
) {
    while let Ok((attachment, result)) = image_rx.try_recv() {
        let key = attachment.cache_key();
        state.image_inflight.remove(&key);
        let mut entry = ThreadImageEntry {
            protocol: None,
            error: None,
        };
        match (result, picker) {
            (Ok(image), Some(picker)) => match decode_image(image.bytes) {
                Ok(decoded) => {
                    entry.protocol = Some(picker.new_resize_protocol(decoded));
                    state.status = "image ready".to_string();
                }
                Err(err) => {
                    entry.error = Some(format!("image decode failed: {err}"));
                    state.status = "image decode failed".to_string();
                }
            },
            (Ok(_), None) => {
                entry.error = Some("terminal image protocol unavailable".to_string());
            }
            (Err(err), _) => {
                state.status = format!("image unavailable: {err}");
                entry.error = Some(err);
            }
        }
        state.image_entries.insert(key, entry);
    }
}

#[cfg(not(feature = "tui-images"))]
fn drain_image_events(
    image_rx: &Receiver<ImageEvent>,
    state: &mut TuiState,
    _picker: Option<&ImagePicker>,
) {
    while let Ok((attachment, result)) = image_rx.try_recv() {
        state.image_inflight.remove(&attachment.cache_key());
        if let Err(err) = result {
            state.status = format!("image unavailable: {err}");
        }
    }
}

#[cfg(feature = "tui-images")]
fn decode_image(bytes: Vec<u8>) -> Result<image::DynamicImage> {
    ImageReader::new(Cursor::new(bytes))
        .with_guessed_format()
        .context("detecting image format")?
        .decode()
        .context("decoding image")
}

fn maybe_spawn_selected_image_fetch(
    state: &mut TuiState,
    app: &App,
    image_tx: Sender<ImageEvent>,
    terminal_images_supported: bool,
) {
    if app.settings().ui.images != ImageMode::Auto || !terminal_images_supported {
        return;
    }
    let Some(message) = state.messages.get(state.message_idx) else {
        return;
    };
    let Some(attachment) = app.image_attachments(message).into_iter().next() else {
        return;
    };
    let key = attachment.cache_key();
    if state.image_entries.contains_key(&key) || state.image_inflight.contains(&key) {
        return;
    }
    state.image_inflight.insert(key);
    let settings = app.settings().clone();
    let offline = app.offline();
    tokio::spawn(async move {
        let result = App::fetch_image_bytes_with_settings(settings, offline, attachment.clone())
            .await
            .map_err(|err| err.to_string());
        let _ = image_tx.send((attachment, result));
    });
}

async fn handle_input_key(key: KeyEvent, state: &mut TuiState, app: &App) -> Result<bool> {
    let target = if state.mode == Mode::Composer {
        &mut state.draft
    } else {
        &mut state.input
    };
    match key.code {
        KeyCode::Esc => state.mode = Mode::Browser,
        KeyCode::Backspace => {
            target.pop();
        }
        KeyCode::Enter => match state.mode {
            Mode::SearchInput => {
                state.search_results = app.search(&state.input)?;
                state.status = format!("{} cached results", state.search_results.len());
                state.popup_scroll = 0;
                state.mode = Mode::SearchResults;
            }
            Mode::Command => run_command(state, app).await?,
            Mode::Composer => {
                target.push('\n');
            }
            _ => {}
        },
        KeyCode::Char('s') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            if state.mode == Mode::Composer && !state.draft.trim().is_empty() {
                state.mode = Mode::ConfirmSend;
            }
        }
        KeyCode::Char(c) => target.push(c),
        _ => {}
    }
    Ok(false)
}

async fn handle_config_key(
    key: KeyEvent,
    state: &mut TuiState,
    app: &mut App,
    theme: &mut TuiTheme,
    image_picker: &mut Option<ImagePicker>,
) -> Result<bool> {
    let Some(form) = state.form.as_mut() else {
        state.form = Some(if state.mode == Mode::Login {
            ConfigForm::login(app.settings())
        } else {
            ConfigForm::config(app.settings())
        });
        return Ok(false);
    };
    match key.code {
        KeyCode::Esc => {
            if form.purpose == ConfigPurpose::Login {
                form.message = "Credentials are required before opening the browser.".to_string();
            } else {
                state.mode = Mode::Browser;
                state.popup_scroll = 0;
            }
        }
        KeyCode::Tab | KeyCode::Down => form.move_selection(1),
        KeyCode::BackTab | KeyCode::Up => form.move_selection(-1),
        KeyCode::Backspace => {
            if let Some(field) = form.selected_field_mut() {
                field.value.pop();
            }
        }
        KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            if let Some(field) = form.selected_field_mut() {
                field.value.clear();
            }
        }
        KeyCode::Char('s') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            if let Err(err) = save_config_form(state, app, theme, image_picker).await {
                if let Some(form) = state.form.as_mut() {
                    form.message = format!("Save failed: {err}");
                } else {
                    state.status = format!("save failed: {err}");
                }
            }
        }
        KeyCode::Enter => {
            if let Err(err) = save_config_form(state, app, theme, image_picker).await {
                if let Some(form) = state.form.as_mut() {
                    form.message = format!("Save failed: {err}");
                } else {
                    state.status = format!("save failed: {err}");
                }
            }
        }
        KeyCode::Char(c) => {
            if let Some(field) = form.selected_field_mut() {
                field.value.push(c);
            }
        }
        _ => {}
    }
    Ok(false)
}

async fn save_config_form(
    state: &mut TuiState,
    app: &mut App,
    theme: &mut TuiTheme,
    image_picker: &mut Option<ImagePicker>,
) -> Result<()> {
    let form = state
        .form
        .as_ref()
        .context("configuration form is not active")?;
    let purpose = form.purpose;
    let mut settings = app.settings().clone();
    let base_url = required_value(form, ConfigField::BaseUrl, "base URL")?
        .trim_end_matches('/')
        .to_string();
    let email = required_value(form, ConfigField::Email, "email")?;
    let api_key_input = form.value(ConfigField::ApiKey).trim().to_string();
    let current_api_key = settings.zulip.api_key.clone().unwrap_or_default();
    let api_key = if api_key_input.is_empty() && purpose == ConfigPurpose::Config {
        current_api_key
    } else {
        api_key_input.clone()
    };
    if api_key.trim().is_empty() {
        anyhow::bail!("API key is required");
    }

    settings.zulip = ZulipConfig {
        base_url: Some(base_url),
        email: Some(email),
        api_key: Some(api_key.clone()),
    };

    if purpose == ConfigPurpose::Config {
        settings.cache = CacheConfig {
            ttl_minutes: positive_i64(form.value(ConfigField::CacheTtlMinutes), "cache TTL")?,
            default_sync_hours: positive_i64(form.value(ConfigField::SyncHours), "sync hours")?,
            path: optional_path(form.value(ConfigField::CachePath)),
        };
        settings.ui = UiConfig {
            start_view: nonempty_or_default(form.value(ConfigField::StartView), "streams"),
            theme: nonempty_or_default(form.value(ConfigField::Theme), "zenburn"),
            images: parse_image_mode(form.value(ConfigField::Images))?,
        };
        settings.safety = SafetyConfig {
            read_only_by_default: parse_bool(
                form.value(ConfigField::ReadOnlyByDefault),
                "read only by default",
            )?,
            require_confirmation_for_sends: parse_bool(
                form.value(ConfigField::ConfirmSends),
                "confirm sends",
            )?,
        };
        settings.digest = DigestConfig {
            aliases: split_aliases(form.value(ConfigField::DigestAliases)),
        };
    }

    validate_credentials(&settings, app.offline()).await?;
    settings.save_without_secret()?;
    if purpose == ConfigPurpose::Login || !api_key_input.is_empty() {
        settings.save_api_key(&api_key)?;
    }
    reload_app(state, app, theme, image_picker, "configuration saved")?;
    Ok(())
}

fn reload_app(
    state: &mut TuiState,
    app: &mut App,
    theme: &mut TuiTheme,
    image_picker: &mut Option<ImagePicker>,
    status: &str,
) -> Result<()> {
    let offline = app.offline();
    let settings = Settings::load()?;
    *app = App::open(settings, offline)?;
    *theme = TuiTheme::resolve(app.settings())?;
    *image_picker = detect_image_picker(app.settings());
    *state = TuiState::load(app)?;
    state.status = status.to_string();
    Ok(())
}

fn logout(
    state: &mut TuiState,
    app: &mut App,
    theme: &mut TuiTheme,
    image_picker: &mut Option<ImagePicker>,
) -> Result<()> {
    let mut settings = app.settings().clone();
    settings.clear_local_auth()?;
    reload_app(state, app, theme, image_picker, "local credentials cleared")?;
    if app.settings().auth_problems().is_empty() && env_auth_present() {
        state.status =
            "local credentials cleared; environment credentials are still active".to_string();
    }
    Ok(())
}

async fn validate_credentials(settings: &Settings, offline: bool) -> Result<()> {
    let (base_url, email, api_key) = settings.require_auth()?;
    if offline {
        return Ok(());
    }
    ZulipClient::new(base_url, email, api_key)
        .current_user()
        .await
        .context("validating Zulip credentials")?;
    Ok(())
}

fn required_value(form: &ConfigForm, field: ConfigField, label: &str) -> Result<String> {
    let value = form.value(field).trim();
    if value.is_empty() {
        anyhow::bail!("{label} is required");
    }
    Ok(value.to_string())
}

fn positive_i64(value: &str, label: &str) -> Result<i64> {
    let parsed = value
        .trim()
        .parse::<i64>()
        .with_context(|| format!("{label} must be a number"))?;
    if parsed <= 0 {
        anyhow::bail!("{label} must be greater than zero");
    }
    Ok(parsed)
}

fn optional_path(value: &str) -> Option<PathBuf> {
    let trimmed = value.trim();
    (!trimmed.is_empty()).then(|| PathBuf::from(trimmed))
}

fn nonempty_or_default(value: &str, default: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        default.to_string()
    } else {
        trimmed.to_string()
    }
}

fn parse_bool(value: &str, label: &str) -> Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "yes" | "on" | "1" => Ok(true),
        "false" | "no" | "off" | "0" => Ok(false),
        _ => anyhow::bail!("{label} must be true or false"),
    }
}

fn parse_image_mode(value: &str) -> Result<ImageMode> {
    match value.trim().to_ascii_lowercase().as_str() {
        "" | "auto" => Ok(ImageMode::Auto),
        "placeholder" | "placeholders" => Ok(ImageMode::Placeholder),
        "off" | "none" => Ok(ImageMode::Off),
        _ => anyhow::bail!("images must be auto, placeholder, or off"),
    }
}

fn image_mode_value(mode: ImageMode) -> &'static str {
    match mode {
        ImageMode::Auto => "auto",
        ImageMode::Placeholder => "placeholder",
        ImageMode::Off => "off",
    }
}

fn split_aliases(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|alias| !alias.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn env_auth_present() -> bool {
    std::env::var_os("ZULIP_BASE_URL").is_some()
        || std::env::var_os("ZULIP_SITE").is_some()
        || std::env::var_os("ZULIP_EMAIL").is_some()
        || std::env::var_os("ZULIP_API_KEY").is_some()
}

async fn run_command(state: &mut TuiState, app: &App) -> Result<()> {
    let command = state.input.trim().to_string();
    if command == "sync now" || command == "sync" {
        match app.sync(app.settings().cache.default_sync_hours).await {
            Ok(sync) => {
                *state = TuiState::load(app)?;
                state.status = format!("synced {} messages", sync.messages);
            }
            Err(err) => state.status = format!("sync failed: {err}"),
        }
    } else if let Some(query) = command.strip_prefix("search ") {
        state.search_results = app.search(query)?;
        state.status = format!("{} cached results", state.search_results.len());
        state.popup_scroll = 0;
        state.mode = Mode::SearchResults;
    } else if let Some(hours) = command.strip_prefix("digest ") {
        let hours = hours.parse::<i64>().unwrap_or(24);
        state.digest = Some(digest::build_digest(
            app.recent_messages(hours)?,
            hours,
            &app.settings().digest.aliases,
        ));
        state.popup_scroll = 0;
        state.mode = Mode::Digest;
    } else if command == "reply" {
        state.mode = Mode::Composer;
    } else if command == "discard draft" {
        state.draft.clear();
        state.mode = Mode::Browser;
        state.status = "draft discarded".to_string();
    } else if command == "send draft" {
        state.mode = Mode::ConfirmSend;
    } else if command == "auth status" {
        state.popup_scroll = 0;
        state.mode = Mode::Auth;
    } else {
        state.status = format!("unknown command: {command}");
        state.mode = Mode::Browser;
    }
    state.input.clear();
    Ok(())
}

fn selected_reply_destination(state: &TuiState) -> Option<ReplyDestination> {
    if let Some(conversation) = state.selected_direct_conversation() {
        return Some(ReplyDestination::Direct(conversation.clone()));
    }
    Some(ReplyDestination::Stream {
        stream: state.selected_stream()?.to_string(),
        topic: state.selected_topic()?.to_string(),
    })
}

fn selected_destination_label(state: &TuiState) -> String {
    if let Some(conversation) = state.selected_direct_conversation() {
        format!("DM: {}", direct_conversation_label(conversation))
    } else {
        match (state.selected_stream(), state.selected_topic()) {
            (Some(stream), Some(topic)) => format!("#{stream} > {topic}"),
            (Some(stream), None) => format!("#{stream} > no topic"),
            _ => "no conversation".to_string(),
        }
    }
}

fn message_destination_label(message: &ZulipMessage) -> String {
    if message.message_type == ZulipMessageType::Direct {
        format!("DM: {}", direct_message_label(message))
    } else {
        format!("#{} > {}", message.stream_name, message.topic)
    }
}

fn direct_message_label(message: &ZulipMessage) -> String {
    if message.dm_participants.is_empty() {
        return message.topic.clone();
    }
    message
        .dm_participants
        .iter()
        .map(|participant| participant.name.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

fn direct_conversation_label(conversation: &ZulipDirectConversation) -> String {
    if conversation.participants.is_empty() {
        return "Direct message".to_string();
    }
    conversation
        .participants
        .iter()
        .map(|participant| participant.name.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

fn handle_scrollable_mode_key(key: KeyEvent, state: &mut TuiState) -> Result<bool> {
    match key.code {
        KeyCode::Char('q') | KeyCode::Esc => {
            state.mode = Mode::Browser;
            state.popup_scroll = 0;
        }
        KeyCode::Char('j') | KeyCode::Down => {
            state.popup_scroll = state.popup_scroll.saturating_add(1);
        }
        KeyCode::Char('k') | KeyCode::Up => {
            state.popup_scroll = state.popup_scroll.saturating_sub(1);
        }
        KeyCode::PageDown => {
            state.popup_scroll = state.popup_scroll.saturating_add(10);
        }
        KeyCode::PageUp => {
            state.popup_scroll = state.popup_scroll.saturating_sub(10);
        }
        KeyCode::Home => {
            state.popup_scroll = 0;
        }
        _ => {}
    }
    Ok(false)
}

fn draw(
    frame: &mut Frame<'_>,
    state: &mut TuiState,
    app: &App,
    theme: &TuiTheme,
    image_picker: Option<&ImagePicker>,
) {
    let outer = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(5), Constraint::Length(1)])
        .split(frame.area());
    let panes = Layout::default()
        .direction(Direction::Horizontal)
        .constraints(pane_constraints(state.focus))
        .split(outer[0]);

    draw_streams(frame, panes[0], state, theme);
    draw_topics(frame, panes[1], state, theme);
    draw_thread(frame, panes[2], state, app, theme, image_picker);
    draw_status(frame, outer[1], state, app, theme);

    match state.mode {
        Mode::SearchInput
        | Mode::Command
        | Mode::Composer
        | Mode::ConfirmSend
        | Mode::Auth
        | Mode::Login
        | Mode::Config
        | Mode::Help => draw_overlay(frame, state, app, theme),
        Mode::SearchResults => draw_search_results(frame, state, theme),
        Mode::Digest => draw_digest(frame, state, theme),
        Mode::Browser => {}
    }
}

fn draw_streams(frame: &mut Frame<'_>, area: Rect, state: &TuiState, theme: &TuiTheme) {
    let content_height = list_content_height(area);
    let item_count = state.streams.len() + 1;
    let start = visible_start(state.stream_idx, item_count, content_height);
    let active = state.focus == Focus::Streams;
    let items = (0..item_count)
        .skip(start)
        .take(content_height)
        .map(|idx| {
            let (count, name) = if idx == state.streams.len() {
                (state.direct_message_count, "Direct messages".to_string())
            } else {
                let stream = &state.streams[idx];
                (
                    state
                        .stream_activity
                        .get(&stream.name)
                        .copied()
                        .unwrap_or(0),
                    stream.name.clone(),
                )
            };
            selected_item(
                idx == state.stream_idx,
                active,
                theme,
                &format!("{count:>4}  {name}"),
            )
        })
        .collect::<Vec<_>>();
    frame.render_widget(
        List::new(items)
            .style(Style::default().fg(theme.text).bg(theme.background))
            .block(block("Streams", active, theme)),
        area,
    );
}

fn draw_topics(frame: &mut Frame<'_>, area: Rect, state: &TuiState, theme: &TuiTheme) {
    let content_height = list_content_height(area);
    let item_count = state.middle_len();
    let start = visible_start(state.topic_idx, item_count, content_height);
    let active = state.focus == Focus::Topics;
    let items = if state.selected_direct_messages() {
        state
            .direct_conversations
            .iter()
            .enumerate()
            .skip(start)
            .take(content_height)
            .map(|(idx, conversation)| {
                selected_item(
                    idx == state.topic_idx,
                    active,
                    theme,
                    &format!(
                        "{}  {}",
                        conversation.message_count,
                        direct_conversation_label(conversation)
                    ),
                )
            })
            .collect::<Vec<_>>()
    } else {
        state
            .topics
            .iter()
            .enumerate()
            .skip(start)
            .take(content_height)
            .map(|(idx, topic)| {
                selected_item(
                    idx == state.topic_idx,
                    active,
                    theme,
                    &format!("{}  {}", topic.message_count, topic.name),
                )
            })
            .collect::<Vec<_>>()
    };
    frame.render_widget(
        List::new(items)
            .style(Style::default().fg(theme.text).bg(theme.background))
            .block(block(
                if state.selected_direct_messages() {
                    "Participants"
                } else {
                    "Topics"
                },
                active,
                theme,
            )),
        area,
    );
}

fn draw_thread(
    frame: &mut Frame<'_>,
    area: Rect,
    state: &mut TuiState,
    app: &App,
    theme: &TuiTheme,
    image_picker: Option<&ImagePicker>,
) {
    let content_height = list_content_height(area);
    let active = state.focus == Focus::Thread;
    let width = area.width.saturating_sub(4).max(1) as usize;
    let image_slot_rows = image_slot_rows(app.settings().ui.images, image_picker.is_some());
    let all_lines = thread_lines(
        &state.messages,
        state.message_idx,
        width,
        theme,
        app,
        image_slot_rows,
    );
    let selected_line = selected_message_line_start(&state.messages, state.message_idx, width, app);
    let start = visible_line_start(selected_line, all_lines.len(), content_height);
    let lines = all_lines
        .into_iter()
        .skip(start)
        .take(content_height)
        .collect::<Vec<_>>();
    frame.render_widget(
        Paragraph::new(lines)
            .style(Style::default().fg(theme.text).bg(theme.background))
            .block(block("Thread", active, theme)),
        area,
    );
    render_selected_image(frame, area, state, app, width, start, image_slot_rows);
}

#[cfg(feature = "tui-images")]
fn render_selected_image(
    frame: &mut Frame<'_>,
    area: Rect,
    state: &mut TuiState,
    app: &App,
    width: usize,
    viewport_start: usize,
    image_slot_rows: u16,
) {
    if image_slot_rows == 0 {
        return;
    }
    let Some(message) = state.messages.get(state.message_idx) else {
        return;
    };
    let Some(attachment) = app.image_attachments(message).into_iter().next() else {
        return;
    };
    let Some(label_line) =
        selected_image_label_line_start(&state.messages, state.message_idx, width, app)
    else {
        return;
    };
    let image_line = label_line + 1;
    let content_height = list_content_height(area);
    if image_line < viewport_start || image_line >= viewport_start + content_height {
        return;
    }
    let inner = Rect {
        x: area.x.saturating_add(1),
        y: area.y.saturating_add(1),
        width: area.width.saturating_sub(2),
        height: area.height.saturating_sub(2),
    };
    let visible_y = inner.y + (image_line - viewport_start) as u16;
    let available_rows = inner
        .y
        .saturating_add(inner.height)
        .saturating_sub(visible_y)
        .min(image_slot_rows);
    if available_rows == 0 {
        return;
    }
    let image_area = Rect {
        x: inner.x,
        y: visible_y,
        width: inner.width,
        height: available_rows,
    };
    let key = attachment.cache_key();
    let Some(entry) = state.image_entries.get_mut(&key) else {
        return;
    };
    let Some(protocol) = entry.protocol.as_mut() else {
        return;
    };

    frame.render_widget(Clear, image_area);
    frame.render_stateful_widget(
        StatefulImage::default().resize(Resize::Fit(None)),
        image_area,
        protocol,
    );
    if let Some(Err(err)) = protocol.last_encoding_result() {
        entry.error = Some(err.to_string());
    }
}

#[cfg(not(feature = "tui-images"))]
fn render_selected_image(
    _frame: &mut Frame<'_>,
    _area: Rect,
    _state: &mut TuiState,
    _app: &App,
    _width: usize,
    _viewport_start: usize,
    _image_slot_rows: u16,
) {
}

fn image_slot_rows(mode: ImageMode, terminal_images_supported: bool) -> u16 {
    if mode == ImageMode::Auto && terminal_images_supported {
        IMAGE_ROWS
    } else {
        0
    }
}

#[cfg(feature = "tui-images")]
fn selected_image_label_line_start(
    messages: &[ZulipMessage],
    selected_idx: usize,
    width: usize,
    app: &App,
) -> Option<usize> {
    let message = messages.get(selected_idx)?;
    if app.settings().ui.images == ImageMode::Off || app.image_attachments(message).is_empty() {
        return None;
    }
    let preceding = selected_message_line_start(messages, selected_idx, width, app);
    Some(preceding + 1 + message_body_line_count(message, width) + reaction_line_count(message))
}

fn draw_status(frame: &mut Frame<'_>, area: Rect, state: &TuiState, app: &App, theme: &TuiTheme) {
    let account = app
        .settings()
        .zulip
        .email
        .as_deref()
        .unwrap_or("no account");
    let realm = app
        .settings()
        .zulip
        .base_url
        .as_deref()
        .unwrap_or("no realm");
    let offline = if app.offline() { "offline" } else { "online" };
    let draft = if state.draft.is_empty() { "" } else { " draft" };
    let sync = app
        .sync_state()
        .ok()
        .flatten()
        .map(|sync| format!("last sync {}", sync.synced_at.format("%H:%M UTC")))
        .unwrap_or_else(|| "not synced".to_string());
    let poll = if let Some(error) = &state.poll_error {
        format!("poll error {error}")
    } else if let Some(last_poll_at) = state.last_poll_at {
        format!(
            "poll {} +{}",
            last_poll_at.format("%H:%M:%S UTC"),
            state.last_poll_new
        )
    } else if app.offline() {
        "poll off".to_string()
    } else {
        "poll pending".to_string()
    };
    let text = format!(
        "{} | {} | {} | {} | {}{} | ? c L j/k h/l / r R : d o y q | {}",
        account, realm, sync, poll, offline, draft, state.status
    );
    frame.render_widget(
        Paragraph::new(text).style(Style::default().fg(theme.status_fg).bg(theme.status_bg)),
        area,
    );
}

fn draw_overlay(frame: &mut Frame<'_>, state: &TuiState, app: &App, theme: &TuiTheme) {
    let area = if matches!(state.mode, Mode::Config | Mode::Help) {
        centered_rect(82, 78, frame.area())
    } else {
        centered_rect(70, 55, frame.area())
    };
    frame.render_widget(Clear, area);
    let text = match state.mode {
        Mode::SearchInput => format!("Search cached messages\n\n{}", state.input),
        Mode::Command => format!("Command palette\n\n{}", state.input),
        Mode::Composer => {
            let destination = selected_destination_label(state);
            let realm = app
                .settings()
                .zulip
                .base_url
                .as_deref()
                .unwrap_or("no realm");
            let account = app
                .settings()
                .zulip
                .email
                .as_deref()
                .unwrap_or("no account");
            format!(
                "Reply draft\nRealm: {realm}\nAccount: {account}\nDestination: {destination}\n\n{}\n\nCtrl-S previews send, Esc cancels",
                state.draft
            )
        }
        Mode::ConfirmSend => {
            let destination = selected_destination_label(state);
            format!(
                "Send this draft?\nDestination: {destination}\n\n{}\n\nPress y to send, n to cancel",
                state.draft
            )
        }
        Mode::Auth => {
            let realm = app
                .settings()
                .zulip
                .base_url
                .as_deref()
                .unwrap_or("missing");
            let account = app.settings().zulip.email.as_deref().unwrap_or("missing");
            let secret = if app.settings().zulip.api_key.is_some() {
                "present"
            } else {
                "missing"
            };
            format!(
                "Auth/config status\n\nRealm: {realm}\nAccount: {account}\nAPI key: {secret}\nConfig: {}\nSecret: {}\nCache: {}",
                app.settings().paths.config_file.display(),
                app.settings().paths.secret_file.display(),
                app.settings().paths.cache_file.display()
            )
        }
        Mode::Login | Mode::Config => form_overlay_text(state),
        Mode::Help => help_text(app),
        _ => String::new(),
    };
    frame.render_widget(
        Paragraph::new(text)
            .style(Style::default().fg(theme.modal_text).bg(theme.background))
            .block(block("zul-tui", true, theme))
            .scroll((state.popup_scroll as u16, 0))
            .wrap(Wrap { trim: false }),
        area,
    );
    if let Some((x, y)) = overlay_cursor_position(state, area) {
        frame.set_cursor_position((x, y));
    }
}

fn overlay_cursor_position(state: &TuiState, area: Rect) -> Option<(u16, u16)> {
    match state.mode {
        Mode::SearchInput | Mode::Command => {
            input_cursor_position(area, 2, state.input.lines().next().unwrap_or(""))
        }
        Mode::Composer => draft_cursor_position(area, &state.draft),
        Mode::Login | Mode::Config => form_cursor_position(area, state.form.as_ref()?),
        _ => None,
    }
}

fn form_overlay_text(state: &TuiState) -> String {
    let Some(form) = &state.form else {
        return String::new();
    };
    let title = match form.purpose {
        ConfigPurpose::Login => "Login",
        ConfigPurpose::Config => "Configuration",
    };
    let mut text = format!("{title}\n{}\n\n", form.message);
    for (idx, field) in form.fields.iter().enumerate() {
        let marker = if idx == form.selected { ">" } else { " " };
        text.push_str(&format!(
            "{marker} {:<22} {}\n",
            field.label,
            field.display_value()
        ));
    }
    text.push_str("\nTab/Shift-Tab moves fields, Ctrl-U clears a field, Ctrl-S saves, Esc closes.");
    if form.purpose == ConfigPurpose::Config {
        text.push_str("\nImages accepts auto, placeholder, or off. Booleans accept true/false.");
    }
    text
}

fn help_text(app: &App) -> String {
    let env_note = if env_auth_present() {
        "\nEnvironment credentials are set, so logout only clears local files."
    } else {
        ""
    };
    format!(
        "Keys\n\n\
         ?  Show this help\n\
         c  Edit configuration\n\
         L  Logout and clear local credentials\n\
         a  Show auth/config status\n\
         q  Quit from the browser, close overlays elsewhere\n\n\
         Navigation\n\n\
         j/k or Up/Down  Move selection\n\
         h/l or Left/Right  Move between panes\n\n\
         Messages\n\n\
         /  Search cached messages\n\
         r  Reply to selected topic\n\
         R  Sync recent messages\n\
         d  Build digest\n\
         o or y  Show selected message permalink\n\
         : or Ctrl-P  Command palette\n\n\
         Editing\n\n\
         Tab/Shift-Tab  Move form fields\n\
         Ctrl-U  Clear current form field\n\
         Ctrl-S  Save draft or form\n\
         Esc  Cancel overlay\n\n\
         Config file: {}\n\
         Secret file: {}\n\
         Cache file: {}{}",
        app.settings().paths.config_file.display(),
        app.settings().paths.secret_file.display(),
        app.settings().paths.cache_file.display(),
        env_note
    )
}

fn form_cursor_position(area: Rect, form: &ConfigForm) -> Option<(u16, u16)> {
    let field = form.selected_field()?;
    let line_index = 3 + form.selected;
    let label_width = 25;
    let inner = overlay_inner_area(area)?;
    let x_offset = (label_width + display_width(&field.display_value()))
        .min(inner.width.saturating_sub(1) as usize) as u16;
    let y_offset = (line_index as u16).min(inner.height.saturating_sub(1));
    Some((
        inner.x.saturating_add(x_offset),
        inner.y.saturating_add(y_offset),
    ))
}

fn draft_cursor_position(area: Rect, draft: &str) -> Option<(u16, u16)> {
    let inner = overlay_inner_area(area)?;
    let width = inner.width.max(1) as usize;
    let mut visual_line_index = 5usize;
    let mut cursor_x = 0usize;

    let mut lines = draft.split('\n').peekable();
    while let Some(line) = lines.next() {
        let widths = wrapped_line_widths(line, width);
        visual_line_index += widths.len().saturating_sub(1);
        cursor_x = widths.last().copied().unwrap_or(0);
        if lines.peek().is_some() {
            visual_line_index += 1;
        }
    }

    Some((
        inner
            .x
            .saturating_add(cursor_x.min(inner.width.saturating_sub(1) as usize) as u16),
        inner
            .y
            .saturating_add((visual_line_index as u16).min(inner.height.saturating_sub(1))),
    ))
}

fn input_cursor_position(area: Rect, line_index: usize, text: &str) -> Option<(u16, u16)> {
    let inner = overlay_inner_area(area)?;
    let x_offset = display_width(text).min(inner.width.saturating_sub(1) as usize) as u16;
    let y_offset = (line_index as u16).min(inner.height.saturating_sub(1));
    Some((
        inner.x.saturating_add(x_offset),
        inner.y.saturating_add(y_offset),
    ))
}

fn overlay_inner_area(area: Rect) -> Option<Rect> {
    let width = area.width.checked_sub(2)?;
    let height = area.height.checked_sub(2)?;
    if width == 0 || height == 0 {
        return None;
    }
    Some(Rect {
        x: area.x.saturating_add(1),
        y: area.y.saturating_add(1),
        width,
        height,
    })
}

fn draw_search_results(frame: &mut Frame<'_>, state: &TuiState, theme: &TuiTheme) {
    let area = centered_rect(86, 76, frame.area());
    frame.render_widget(Clear, area);
    let content_height = list_content_height(area);
    let start = state
        .popup_scroll
        .min(state.search_results.len().saturating_sub(content_height));
    let items = state
        .search_results
        .iter()
        .skip(start)
        .take(content_height)
        .map(|message| {
            ListItem::new(Line::from(vec![
                Span::styled(
                    format!("[{}] ", message.id),
                    Style::default().fg(theme.message_id),
                ),
                Span::styled(
                    format!("{} | ", message_destination_label(message)),
                    Style::default().fg(theme.inactive_title),
                ),
                Span::styled(
                    format!("{}: ", message.sender_name),
                    Style::default()
                        .fg(theme.sender)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::styled(message.plain_text.clone(), Style::default().fg(theme.text)),
            ]))
        })
        .collect::<Vec<_>>();
    frame.render_widget(
        List::new(items)
            .style(Style::default().fg(theme.text).bg(theme.background))
            .block(block("Search results", true, theme)),
        area,
    );
}

fn draw_digest(frame: &mut Frame<'_>, state: &TuiState, theme: &TuiTheme) {
    let area = centered_rect(86, 76, frame.area());
    frame.render_widget(Clear, area);
    let mut lines = Vec::new();
    if let Some(digest) = &state.digest {
        lines.push(Line::from(format!(
            "{} messages in last {}h",
            digest.total_messages, digest.hours
        )));
        lines.push(Line::from(""));
        lines.push(Line::from("Most active topics"));
        for topic in digest.topics.iter().take(12) {
            lines.push(Line::from(format!(
                "#{} > {}: {}",
                topic.stream, topic.topic, topic.message_count
            )));
        }
        lines.push(Line::from(""));
        lines.push(Line::from("Unanswered questions"));
        for item in digest.unanswered_questions.iter().take(8) {
            lines.push(Line::from(format!(
                "[{}] {} | {}",
                item.id,
                digest_item_destination(item),
                item.text
            )));
        }
        lines.push(Line::from(""));
        lines.push(Line::from("Attention items"));
        for item in digest.likely_attention_items.iter().take(12) {
            lines.push(Line::from(format!(
                "[{}] {} | {}",
                item.id,
                digest_item_destination(item),
                item.text
            )));
        }
    }
    frame.render_widget(
        Paragraph::new(lines)
            .style(Style::default().fg(theme.modal_text).bg(theme.background))
            .block(block("Digest", true, theme))
            .scroll((state.popup_scroll as u16, 0))
            .wrap(Wrap { trim: false }),
        area,
    );
}

fn selected_item(selected: bool, active: bool, theme: &TuiTheme, text: &str) -> ListItem<'static> {
    if selected {
        let (fg, bg) = if active {
            (theme.active_selection_fg, theme.active_selection_bg)
        } else {
            (theme.inactive_selection_fg, theme.inactive_selection_bg)
        };
        ListItem::new(Line::from(vec![Span::styled(
            text.to_string(),
            Style::default().fg(fg).bg(bg),
        )]))
    } else {
        ListItem::new(Line::from(Span::styled(
            text.to_string(),
            Style::default().fg(theme.text).bg(theme.background),
        )))
    }
}

fn block<'a>(title: &'a str, focused: bool, theme: &TuiTheme) -> Block<'a> {
    let style = if focused {
        Style::default()
            .fg(theme.active_title)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(theme.inactive_title)
    };
    Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(theme.pane_border))
        .title_style(style)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TuiTheme {
    background: Color,
    text: Color,
    pane_border: Color,
    active_title: Color,
    inactive_title: Color,
    active_selection_fg: Color,
    active_selection_bg: Color,
    inactive_selection_fg: Color,
    inactive_selection_bg: Color,
    message_id: Color,
    sender: Color,
    status_fg: Color,
    status_bg: Color,
    modal_text: Color,
}

impl TuiTheme {
    fn resolve(settings: &Settings) -> Result<Self> {
        let name = settings.ui.theme.trim();
        let theme_name = if name.is_empty() { "zenburn" } else { name };
        let mut theme = if theme_name.eq_ignore_ascii_case("zenburn") {
            Self::zenburn()
        } else if settings.themes.contains_key(theme_name) {
            Self::zenburn()
        } else {
            anyhow::bail!("unknown TUI theme '{theme_name}'");
        };

        if let Some(config) = settings.themes.get(theme_name).or_else(|| {
            if theme_name.eq_ignore_ascii_case("zenburn") {
                settings.themes.get("zenburn")
            } else {
                None
            }
        }) {
            theme.apply_config(config)?;
        }

        Ok(theme)
    }

    fn zenburn() -> Self {
        Self {
            background: rgb(0x3f, 0x3f, 0x3f),
            text: rgb(0xdc, 0xdc, 0xcc),
            pane_border: rgb(0x6f, 0x6f, 0x6f),
            active_title: rgb(0xf0, 0xdf, 0xaf),
            inactive_title: rgb(0x7f, 0x9f, 0x7f),
            active_selection_fg: rgb(0x3f, 0x3f, 0x3f),
            active_selection_bg: rgb(0xf0, 0xdf, 0xaf),
            inactive_selection_fg: rgb(0xdc, 0xdc, 0xcc),
            inactive_selection_bg: rgb(0x4f, 0x4f, 0x4f),
            message_id: rgb(0x8c, 0xd0, 0xd3),
            sender: rgb(0xbf, 0xeb, 0xbf),
            status_fg: rgb(0x3f, 0x3f, 0x3f),
            status_bg: rgb(0xd0, 0xbf, 0x8f),
            modal_text: rgb(0xdc, 0xdc, 0xcc),
        }
    }

    fn apply_config(&mut self, config: &ThemeConfig) -> Result<()> {
        apply_color(&mut self.background, "background", &config.background)?;
        apply_color(&mut self.text, "text", &config.text)?;
        apply_color(&mut self.pane_border, "pane_border", &config.pane_border)?;
        apply_color(&mut self.active_title, "active_title", &config.active_title)?;
        apply_color(
            &mut self.inactive_title,
            "inactive_title",
            &config.inactive_title,
        )?;
        apply_color(
            &mut self.active_selection_fg,
            "active_selection_fg",
            &config.active_selection_fg,
        )?;
        apply_color(
            &mut self.active_selection_bg,
            "active_selection_bg",
            &config.active_selection_bg,
        )?;
        apply_color(
            &mut self.inactive_selection_fg,
            "inactive_selection_fg",
            &config.inactive_selection_fg,
        )?;
        apply_color(
            &mut self.inactive_selection_bg,
            "inactive_selection_bg",
            &config.inactive_selection_bg,
        )?;
        apply_color(&mut self.message_id, "message_id", &config.message_id)?;
        apply_color(&mut self.sender, "sender", &config.sender)?;
        apply_color(&mut self.status_fg, "status_fg", &config.status_fg)?;
        apply_color(&mut self.status_bg, "status_bg", &config.status_bg)?;
        apply_color(&mut self.modal_text, "modal_text", &config.modal_text)?;
        Ok(())
    }
}

fn apply_color(target: &mut Color, field: &str, value: &Option<String>) -> Result<()> {
    if let Some(value) = value {
        *target = parse_hex_color(value).with_context(|| format!("invalid theme color {field}"))?;
    }
    Ok(())
}

fn parse_hex_color(value: &str) -> Result<Color> {
    let value = value.trim().trim_start_matches('#');
    if value.len() != 6 || !value.chars().all(|ch| ch.is_ascii_hexdigit()) {
        anyhow::bail!("expected #RRGGBB");
    }
    let red = u8::from_str_radix(&value[0..2], 16)?;
    let green = u8::from_str_radix(&value[2..4], 16)?;
    let blue = u8::from_str_radix(&value[4..6], 16)?;
    Ok(rgb(red, green, blue))
}

const fn rgb(red: u8, green: u8, blue: u8) -> Color {
    Color::Rgb(red, green, blue)
}

fn pane_constraints(focus: Focus) -> [Constraint; 3] {
    let [streams, topics, thread] = pane_percentages(focus);
    [
        Constraint::Percentage(streams),
        Constraint::Percentage(topics),
        Constraint::Percentage(thread),
    ]
}

fn pane_percentages(focus: Focus) -> [u16; 3] {
    match focus {
        Focus::Streams => [40, 30, 30],
        Focus::Topics => [20, 45, 35],
        Focus::Thread => [14, 18, 68],
    }
}

#[derive(Clone)]
struct StyledSegment {
    text: String,
    style: Style,
}

struct StyledWord {
    text: String,
    style: Style,
    space_before: bool,
}

struct ReactionSummary {
    display: String,
    count: usize,
}

fn thread_lines(
    messages: &[ZulipMessage],
    selected_idx: usize,
    width: usize,
    theme: &TuiTheme,
    app: &App,
    image_slot_rows: u16,
) -> Vec<Line<'static>> {
    let mut lines = Vec::new();
    for (idx, message) in messages.iter().enumerate() {
        let selected = idx == selected_idx;
        let header_style = Style::default().bg(theme.background);
        let marker_style = Style::default().fg(theme.text).bg(theme.background);
        let selection_marker = if selected { " > " } else { "   " };
        lines.push(Line::from(vec![
            Span::styled(selection_marker, marker_style),
            Span::styled(
                format!("[{}]", message.id),
                header_style.fg(theme.message_id),
            ),
            Span::raw(" "),
            Span::styled(
                message.sender_name.clone(),
                header_style.fg(theme.sender).add_modifier(Modifier::BOLD),
            ),
        ]));

        let body_style = Style::default().fg(theme.text).bg(theme.background);
        for line in message_body_lines(message, width, body_style) {
            lines.push(line);
        }

        let reaction_style = Style::default()
            .fg(theme.inactive_title)
            .bg(theme.background);
        if let Some(line) = reaction_line(message, reaction_style) {
            lines.push(line);
        }

        if app.settings().ui.images != ImageMode::Off {
            let attachments = app.image_attachments(message);
            for (image_idx, attachment) in attachments.iter().enumerate() {
                let style = Style::default()
                    .fg(theme.inactive_title)
                    .bg(theme.background);
                lines.push(Line::from(Span::styled(
                    format!("[image: {}]", attachment.label()),
                    style,
                )));
                if selected && image_idx == 0 {
                    for _ in 0..image_slot_rows {
                        lines.push(Line::from(Span::styled(
                            "",
                            Style::default().bg(theme.background),
                        )));
                    }
                }
            }
        }
        lines.push(Line::from(Span::styled(
            "",
            Style::default().bg(theme.background),
        )));
    }

    lines
}

fn message_body_lines(message: &ZulipMessage, width: usize, style: Style) -> Vec<Line<'static>> {
    let mut segments = html_segments(&message.rendered_html, style);
    if segments
        .iter()
        .all(|segment| segment.text.trim().is_empty())
    {
        segments = vec![StyledSegment {
            text: message.plain_text.clone(),
            style,
        }];
    }
    wrap_styled_segments(&segments, width, style)
}

fn message_body_line_count(message: &ZulipMessage, width: usize) -> usize {
    message_body_lines(message, width, Style::default()).len()
}

fn html_segments(html: &str, style: Style) -> Vec<StyledSegment> {
    let fragment = Html::parse_fragment(html);
    let root = fragment.root_element();
    let mut segments = Vec::new();
    for child in root.children() {
        collect_html_segments(child, style, &mut segments);
    }
    segments
}

fn collect_html_segments(node: NodeRef<'_, Node>, style: Style, segments: &mut Vec<StyledSegment>) {
    match node.value() {
        Node::Text(text) => push_text_segments(segments, text, style),
        Node::Element(_) => {
            let Some(element) = ElementRef::wrap(node) else {
                return;
            };
            let name = element.value().name();
            if name == "br" {
                push_segment(segments, " ".to_string(), style);
                return;
            }
            if is_html_spacing_tag(name) {
                push_segment(segments, " ".to_string(), style);
            }
            if let Some(emoji) = emoji_text(element) {
                push_segment(segments, emoji, style);
            } else {
                let child_style = html_child_style(style, name);
                for child in element.children() {
                    collect_html_segments(child, child_style, segments);
                }
            }
            if is_html_spacing_tag(name) {
                push_segment(segments, " ".to_string(), style);
            }
        }
        _ => {}
    }
}

fn push_text_segments(segments: &mut Vec<StyledSegment>, text: &str, style: Style) {
    let mut emit_from = 0;
    let mut search_from = 0;
    while let Some(relative_start) = text[search_from..].find(':') {
        let start = search_from + relative_start;
        let Some(relative_end) = text[start + 1..].find(':') else {
            break;
        };
        let end = start + 1 + relative_end;
        let candidate = &text[start..=end];
        let Some(emoji) = emoji_from_shortcode(candidate) else {
            search_from = end + 1;
            continue;
        };
        if emit_from < start {
            push_segment(segments, text[emit_from..start].to_string(), style);
        }
        push_segment(segments, emoji, style);
        emit_from = end + 1;
        search_from = emit_from;
    }
    if emit_from < text.len() {
        push_segment(segments, text[emit_from..].to_string(), style);
    }
}

fn push_segment(segments: &mut Vec<StyledSegment>, text: String, style: Style) {
    if text.is_empty() {
        return;
    }
    if let Some(previous) = segments.last_mut() {
        if previous.style == style {
            previous.text.push_str(&text);
            return;
        }
    }
    segments.push(StyledSegment { text, style });
}

fn html_child_style(style: Style, tag: &str) -> Style {
    match tag {
        "b" | "strong" => style.add_modifier(Modifier::BOLD),
        "em" | "i" => style.add_modifier(Modifier::ITALIC),
        "code" | "pre" => style.add_modifier(Modifier::DIM),
        _ => style,
    }
}

fn is_html_spacing_tag(tag: &str) -> bool {
    matches!(
        tag,
        "p" | "div" | "li" | "ul" | "ol" | "blockquote" | "pre" | "tr" | "td" | "th"
    )
}

fn digest_item_destination(item: &libzul::digest::DigestItem) -> String {
    if item.stream == "Direct messages" {
        format!("DM: {}", item.topic)
    } else {
        format!("#{} > {}", item.stream, item.topic)
    }
}

fn emoji_text(element: ElementRef<'_>) -> Option<String> {
    if !is_emoji_element(element) {
        return None;
    }
    if let Some(emoji) = element.attr("class").and_then(emoji_from_class) {
        return Some(emoji);
    }
    let text = element.text().collect::<String>().trim().to_string();
    if let Some(emoji) = emoji_from_shortcode(&text) {
        return Some(emoji);
    }
    if !text.is_empty() && !looks_like_shortcode(&text) {
        return Some(text);
    }
    for attr in ["alt", "title", "aria-label", "data-emoji-name"] {
        if let Some(value) = element
            .attr(attr)
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            if let Some(emoji) = emoji_from_shortcode(value) {
                return Some(emoji);
            }
            return Some(format_shortcode(value));
        }
    }
    (!text.is_empty()).then_some(text)
}

fn is_emoji_element(element: ElementRef<'_>) -> bool {
    element
        .attr("class")
        .is_some_and(|classes| classes.split_whitespace().any(is_emoji_class))
        || element.attr("data-emoji-name").is_some()
        || (element.value().name() == "img"
            && element
                .attr("alt")
                .is_some_and(|alt| looks_like_shortcode(alt.trim())))
}

fn is_emoji_class(class: &str) -> bool {
    class == "emoji" || class.starts_with("emoji-")
}

fn emoji_from_class(classes: &str) -> Option<String> {
    classes
        .split_whitespace()
        .filter_map(|class| class.strip_prefix("emoji-"))
        .find_map(emoji_from_code)
}

fn emoji_from_code(code: &str) -> Option<String> {
    let mut emoji = String::new();
    for part in code.split('-') {
        let value = u32::from_str_radix(part, 16).ok()?;
        emoji.push(char::from_u32(value)?);
    }
    (!emoji.is_empty()).then_some(emoji)
}

fn emoji_from_shortcode(value: &str) -> Option<String> {
    let normalized = normalize_shortcode(value);
    emojis::get_by_shortcode(shortcode_alias(&normalized)).map(|emoji| emoji.as_str().to_string())
}

fn looks_like_shortcode(value: &str) -> bool {
    value.len() > 2 && value.starts_with(':') && value.ends_with(':')
}

fn format_shortcode(value: &str) -> String {
    let name = normalize_shortcode(value);
    format!(":{name}:")
}

fn normalize_shortcode(value: &str) -> String {
    value
        .trim()
        .trim_matches(':')
        .trim_end_matches(" emoji")
        .trim()
        .replace(' ', "_")
        .to_ascii_lowercase()
}

fn shortcode_alias(value: &str) -> &str {
    match value {
        "thumbs_up" => "+1",
        "thumbs_down" => "-1",
        "slight_smile" => "slightly_smiling_face",
        _ => value,
    }
}

fn wrap_styled_segments(
    segments: &[StyledSegment],
    width: usize,
    empty_style: Style,
) -> Vec<Line<'static>> {
    let words = styled_words(segments);
    if words.is_empty() {
        return vec![Line::from(Span::styled(String::new(), empty_style))];
    }
    let width = width.max(1);
    let mut lines = Vec::new();
    let mut spans = Vec::new();
    let mut line_width = 0;

    for word in words {
        push_styled_word(
            word,
            width,
            &mut lines,
            &mut spans,
            &mut line_width,
            empty_style,
        );
    }

    if !spans.is_empty() {
        lines.push(Line::from(spans));
    }
    if lines.is_empty() {
        lines.push(Line::from(Span::styled(String::new(), empty_style)));
    }
    lines
}

fn styled_words(segments: &[StyledSegment]) -> Vec<StyledWord> {
    let mut words = Vec::new();
    let mut pending_space = false;
    for segment in segments {
        let mut current = String::new();
        let mut current_space_before = false;
        for ch in segment.text.chars() {
            if ch.is_whitespace() {
                if !current.is_empty() {
                    words.push(StyledWord {
                        text: std::mem::take(&mut current),
                        style: segment.style,
                        space_before: current_space_before,
                    });
                }
                pending_space = true;
                current_space_before = false;
            } else {
                if current.is_empty() {
                    current_space_before = pending_space && !words.is_empty();
                    pending_space = false;
                }
                current.push(ch);
            }
        }
        if !current.is_empty() {
            words.push(StyledWord {
                text: current,
                style: segment.style,
                space_before: current_space_before,
            });
        }
    }
    words
}

fn push_styled_word(
    word: StyledWord,
    width: usize,
    lines: &mut Vec<Line<'static>>,
    spans: &mut Vec<Span<'static>>,
    line_width: &mut usize,
    empty_style: Style,
) {
    let word_width = display_width(&word.text);
    let space_width = usize::from(*line_width > 0 && word.space_before);
    if *line_width > 0 && *line_width + space_width + word_width > width {
        lines.push(Line::from(std::mem::take(spans)));
        *line_width = 0;
    }
    if *line_width > 0 && word.space_before {
        spans.push(Span::styled(" ".to_string(), word.style));
        *line_width += 1;
    }
    if word_width <= width {
        spans.push(Span::styled(word.text, word.style));
        *line_width += word_width;
    } else {
        push_long_styled_word(word.text, word.style, width, lines, spans, line_width);
    }
    if spans.is_empty() && *line_width == 0 {
        spans.push(Span::styled(String::new(), empty_style));
    }
}

fn push_long_styled_word(
    word: String,
    style: Style,
    width: usize,
    lines: &mut Vec<Line<'static>>,
    spans: &mut Vec<Span<'static>>,
    line_width: &mut usize,
) {
    let mut chunk = String::new();
    let mut chunk_width = 0;
    for grapheme in word.graphemes(true) {
        let grapheme_width = display_width(grapheme);
        if chunk_width > 0 && chunk_width + grapheme_width > width {
            spans.push(Span::styled(std::mem::take(&mut chunk), style));
            lines.push(Line::from(std::mem::take(spans)));
            *line_width = 0;
            chunk_width = 0;
        }
        if chunk_width == 0 && grapheme_width > width {
            spans.push(Span::styled(grapheme.to_string(), style));
            lines.push(Line::from(std::mem::take(spans)));
            *line_width = 0;
            continue;
        }
        chunk.push_str(grapheme);
        chunk_width += grapheme_width;
    }
    if !chunk.is_empty() {
        spans.push(Span::styled(chunk, style));
        *line_width += chunk_width;
    }
}

fn display_width(value: &str) -> usize {
    UnicodeWidthStr::width(value)
}

fn wrapped_line_widths(line: &str, width: usize) -> Vec<usize> {
    let width = width.max(1);
    let mut lines = Vec::new();
    let mut pending_line_width = 0usize;
    let mut word_width = 0usize;
    let mut whitespace_width = 0usize;
    let mut pending_whitespace = VecDeque::new();
    let mut non_whitespace_previous = false;

    for grapheme in line.graphemes(true) {
        let symbol_width = display_width(grapheme);
        if symbol_width > width {
            continue;
        }

        let is_whitespace = grapheme.chars().all(char::is_whitespace);
        let word_found = non_whitespace_previous && is_whitespace;
        let untrimmed_overflow =
            pending_line_width == 0 && word_width + whitespace_width + symbol_width > width;

        if word_found || untrimmed_overflow {
            pending_line_width += whitespace_width + word_width;
            pending_whitespace.clear();
            whitespace_width = 0;
            word_width = 0;
        }

        let line_full = pending_line_width >= width;
        let pending_word_overflow =
            symbol_width > 0 && pending_line_width + whitespace_width + word_width >= width;

        if line_full || pending_word_overflow {
            let mut remaining_width = width.saturating_sub(pending_line_width);
            lines.push(pending_line_width);
            pending_line_width = 0;

            while let Some(width) = pending_whitespace.front().copied() {
                if width > remaining_width {
                    break;
                }
                whitespace_width = whitespace_width.saturating_sub(width);
                remaining_width -= width;
                pending_whitespace.pop_front();
            }

            if is_whitespace && pending_whitespace.is_empty() {
                continue;
            }
        }

        if is_whitespace {
            whitespace_width += symbol_width;
            pending_whitespace.push_back(symbol_width);
        } else {
            word_width += symbol_width;
        }

        non_whitespace_previous = !is_whitespace;
    }

    pending_line_width += whitespace_width + word_width;
    if pending_line_width > 0 {
        lines.push(pending_line_width);
    }
    if lines.is_empty() {
        lines.push(0);
    }
    lines
}

fn reaction_line(message: &ZulipMessage, style: Style) -> Option<Line<'static>> {
    let summaries = reaction_summaries(message);
    if summaries.is_empty() {
        return None;
    }
    let mut spans = Vec::new();
    for (idx, summary) in summaries.iter().enumerate() {
        if idx > 0 {
            spans.push(Span::styled("  ".to_string(), style));
        }
        spans.push(Span::styled(
            format!("{} {}", summary.display, summary.count),
            style,
        ));
    }
    Some(Line::from(spans))
}

fn reaction_line_count(message: &ZulipMessage) -> usize {
    usize::from(!reaction_summaries(message).is_empty())
}

fn reaction_summaries(message: &ZulipMessage) -> Vec<ReactionSummary> {
    let Some(reactions) = message
        .raw_json
        .get("reactions")
        .and_then(|value| value.as_array())
    else {
        return Vec::new();
    };
    let mut summaries: Vec<(String, ReactionSummary)> = Vec::new();
    for reaction in reactions {
        let emoji_name = reaction
            .get("emoji_name")
            .and_then(|value| value.as_str())
            .unwrap_or("emoji");
        let emoji_code = reaction
            .get("emoji_code")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        let reaction_type = reaction
            .get("reaction_type")
            .and_then(|value| value.as_str())
            .unwrap_or("");
        let key = format!("{reaction_type}:{emoji_code}:{emoji_name}");
        if let Some((_, summary)) = summaries.iter_mut().find(|(existing, _)| existing == &key) {
            summary.count += 1;
            continue;
        }
        summaries.push((
            key,
            ReactionSummary {
                display: reaction_display(reaction_type, emoji_code, emoji_name),
                count: 1,
            },
        ));
    }
    summaries.into_iter().map(|(_, summary)| summary).collect()
}

fn reaction_display(reaction_type: &str, emoji_code: &str, emoji_name: &str) -> String {
    if reaction_type == "unicode_emoji" {
        if let Some(emoji) = emoji_from_code(emoji_code) {
            return emoji;
        }
    }
    emoji_from_shortcode(emoji_name).unwrap_or_else(|| format_shortcode(emoji_name))
}

fn selected_message_line_start(
    messages: &[ZulipMessage],
    selected_idx: usize,
    width: usize,
    app: &App,
) -> usize {
    messages
        .iter()
        .take(selected_idx)
        .map(|message| message_line_height(message, width, app, 0))
        .sum()
}

fn message_line_height(
    message: &ZulipMessage,
    width: usize,
    app: &App,
    image_slot_rows: u16,
) -> usize {
    let attachment_rows = if app.settings().ui.images == ImageMode::Off {
        0
    } else {
        app.image_attachments(message).len() + image_slot_rows as usize
    };
    1 + message_body_line_count(message, width) + reaction_line_count(message) + attachment_rows + 1
}

fn visible_line_start(selected_line: usize, total_lines: usize, height: usize) -> usize {
    if total_lines <= height || height == 0 {
        return 0;
    }
    let half = height / 2;
    selected_line.saturating_sub(half).min(total_lines - height)
}

fn move_index(current: usize, len: usize, delta: isize) -> usize {
    if len == 0 {
        return 0;
    }
    let next = current as isize + delta;
    next.clamp(0, len.saturating_sub(1) as isize) as usize
}

fn list_content_height(area: Rect) -> usize {
    area.height.saturating_sub(2).max(1) as usize
}

fn visible_start(selected: usize, len: usize, height: usize) -> usize {
    if len <= height || height == 0 {
        return 0;
    }
    let half = height / 2;
    selected.saturating_sub(half).min(len - height)
}

fn centered_rect(percent_x: u16, percent_y: u16, area: Rect) -> Rect {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(area);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(vertical[1])[1]
}

fn setup_terminal() -> Result<Terminal<CrosstermBackend<Stdout>>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    Terminal::new(backend).map_err(Into::into)
}

fn restore_terminal(terminal: &mut Terminal<CrosstermBackend<Stdout>>) -> Result<()> {
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use chrono::Utc;
    use libzul::cache::Cache;
    use libzul::config::{
        AppPaths, CacheConfig, DigestConfig, SafetyConfig, Settings, UiConfig, ZulipConfig,
    };
    use libzul::zulip::models::ZulipDmParticipant;
    use serde_json::json;
    use tempfile::TempDir;

    #[test]
    fn move_index_clamps() {
        assert_eq!(move_index(0, 3, -1), 0);
        assert_eq!(move_index(1, 3, 1), 2);
        assert_eq!(move_index(2, 3, 1), 2);
        assert_eq!(move_index(0, 0, 1), 0);
    }

    #[test]
    fn visible_start_keeps_selection_in_view() {
        assert_eq!(visible_start(0, 100, 10), 0);
        assert_eq!(visible_start(5, 100, 10), 0);
        assert_eq!(visible_start(50, 100, 10), 45);
        assert_eq!(visible_start(99, 100, 10), 90);
        assert_eq!(visible_start(2, 3, 10), 0);
    }

    #[test]
    fn default_theme_is_zenburn() {
        let settings = test_settings(TempDir::new().unwrap().path());
        let theme = TuiTheme::resolve(&settings).unwrap();

        assert_eq!(theme.background, rgb(0x3f, 0x3f, 0x3f));
        assert_eq!(theme.active_title, rgb(0xf0, 0xdf, 0xaf));
    }

    #[test]
    fn custom_theme_overrides_builtin_fields() {
        let mut settings = test_settings(TempDir::new().unwrap().path());
        settings.ui.theme = "cozy".to_string();
        settings.themes.insert(
            "cozy".to_string(),
            ThemeConfig {
                background: Some("#101820".to_string()),
                active_title: Some("f2aa4c".to_string()),
                ..ThemeConfig::default()
            },
        );

        let theme = TuiTheme::resolve(&settings).unwrap();

        assert_eq!(theme.background, rgb(0x10, 0x18, 0x20));
        assert_eq!(theme.active_title, rgb(0xf2, 0xaa, 0x4c));
        assert_eq!(theme.text, TuiTheme::zenburn().text);
    }

    #[test]
    fn invalid_theme_color_reports_field() {
        let mut settings = test_settings(TempDir::new().unwrap().path());
        settings.ui.theme = "bad".to_string();
        settings.themes.insert(
            "bad".to_string(),
            ThemeConfig {
                sender: Some("not-a-color".to_string()),
                ..ThemeConfig::default()
            },
        );

        let error = TuiTheme::resolve(&settings).unwrap_err().to_string();

        assert!(error.contains("sender"));
    }

    #[test]
    fn unknown_theme_name_errors() {
        let mut settings = test_settings(TempDir::new().unwrap().path());
        settings.ui.theme = "missing".to_string();

        let error = TuiTheme::resolve(&settings).unwrap_err().to_string();

        assert!(error.contains("unknown TUI theme"));
    }

    #[test]
    fn focus_weighted_layout_ratios_are_stable() {
        assert_eq!(pane_percentages(Focus::Streams), [40, 30, 30]);
        assert_eq!(pane_percentages(Focus::Topics), [20, 45, 35]);
        assert_eq!(pane_percentages(Focus::Thread), [14, 18, 68]);
    }

    #[test]
    fn wrapped_thread_viewport_uses_message_heights() {
        let fixture = TuiFixture::new(&[]);
        let app = App::open(fixture.settings.clone(), false).unwrap();
        let messages = vec![
            test_message_with_text(1, "short"),
            test_message_with_text(2, "one two three four five six seven eight"),
            test_message_with_text(3, "tail"),
        ];

        assert_eq!(selected_message_line_start(&messages, 0, 10, &app), 0);
        assert!(selected_message_line_start(&messages, 2, 10, &app) > 6);
        assert!(visible_line_start(selected_message_line_start(&messages, 2, 10, &app), 12, 5) > 0);
    }

    #[test]
    fn refresh_follows_when_thread_was_at_end() {
        let fixture = TuiFixture::new(&[1, 2, 3]);
        let app = App::open(fixture.settings.clone(), false).unwrap();
        let mut state = TuiState::load(&app).unwrap();
        state.message_idx = 2;

        fixture.insert_messages(&[4]);
        state.refresh_from_cache(&app, true).unwrap();

        assert_eq!(state.messages[state.message_idx].id, 4);
    }

    #[test]
    fn thread_opens_at_newest_message() {
        let fixture = TuiFixture::new(&[1, 2, 3]);
        let app = App::open(fixture.settings.clone(), false).unwrap();
        let state = TuiState::load(&app).unwrap();

        assert_eq!(state.messages[state.message_idx].id, 3);
    }

    #[test]
    fn missing_credentials_open_login_form() {
        let fixture = TuiFixture::new(&[]);
        let app = App::open(fixture.settings.clone(), false).unwrap();

        let state = TuiState::load(&app).unwrap();

        assert_eq!(state.mode, Mode::Login);
        assert_eq!(state.form.as_ref().unwrap().purpose, ConfigPurpose::Login);
    }

    #[test]
    fn incomplete_startup_env_forces_login_even_when_settings_have_auth() {
        let mut fixture = TuiFixture::new(&[]);
        fixture.settings.zulip = ZulipConfig {
            base_url: Some("https://stale.example".to_string()),
            email: Some("dotenv@example.com".to_string()),
            api_key: Some("stale-secret".to_string()),
        };
        let app = App::open(fixture.settings.clone(), false).unwrap();
        let startup = StartupCredentials {
            base_url: String::new(),
            email: String::new(),
            api_key: String::new(),
        };

        let state = TuiState::load_for_startup(&app, &startup).unwrap();
        let form = state.form.as_ref().unwrap();

        assert_eq!(state.mode, Mode::Login);
        assert_eq!(form.value(ConfigField::BaseUrl), "");
        assert_eq!(form.value(ConfigField::Email), "");
        assert_eq!(form.value(ConfigField::ApiKey), "");
    }

    #[test]
    fn complete_startup_env_does_not_force_login() {
        let mut fixture = TuiFixture::new(&[]);
        fixture.settings.zulip = ZulipConfig {
            base_url: Some("https://zulip.example".to_string()),
            email: Some("ada@example.com".to_string()),
            api_key: Some("secret".to_string()),
        };
        let app = App::open(fixture.settings.clone(), false).unwrap();
        let startup = StartupCredentials {
            base_url: "https://zulip.example".to_string(),
            email: "ada@example.com".to_string(),
            api_key: "secret".to_string(),
        };

        let state = TuiState::load_for_startup(&app, &startup).unwrap();

        assert_eq!(state.mode, Mode::Browser);
    }

    #[test]
    fn complete_credentials_open_browser() {
        let mut fixture = TuiFixture::new(&[]);
        fixture.settings.zulip = ZulipConfig {
            base_url: Some("https://zulip.example".to_string()),
            email: Some("ada@example.com".to_string()),
            api_key: Some("secret".to_string()),
        };
        let app = App::open(fixture.settings.clone(), false).unwrap();

        let state = TuiState::load(&app).unwrap();

        assert_eq!(state.mode, Mode::Browser);
    }

    #[test]
    fn secret_form_fields_are_masked() {
        let field = FormField::secret(ConfigField::ApiKey, "API key", "secret".to_string());

        assert_eq!(field.display_value(), "******");
    }

    #[test]
    fn config_parsers_accept_expected_values() {
        assert_eq!(
            parse_image_mode("placeholder").unwrap(),
            ImageMode::Placeholder
        );
        assert_eq!(parse_image_mode("off").unwrap(), ImageMode::Off);
        assert!(parse_image_mode("sparkles").is_err());
        assert!(parse_bool("yes", "field").unwrap());
        assert!(!parse_bool("0", "field").unwrap());
        assert_eq!(
            split_aliases("Ada, The Maintainer, "),
            ["Ada", "The Maintainer"]
        );
    }

    #[test]
    fn refresh_keeps_position_when_reading_older_messages() {
        let fixture = TuiFixture::new(&[1, 2, 3]);
        let app = App::open(fixture.settings.clone(), false).unwrap();
        let mut state = TuiState::load(&app).unwrap();
        state.message_idx = 0;

        fixture.insert_messages(&[4]);
        state.refresh_from_cache(&app, false).unwrap();

        assert_eq!(state.messages[state.message_idx].id, 1);
    }

    #[test]
    fn direct_messages_load_as_synthetic_left_pane_entry() {
        let fixture = TuiFixture::new(&[1]);
        fixture.insert_direct_messages(&[10, 11], "id:7");
        let app = App::open(fixture.settings.clone(), false).unwrap();
        let mut state = TuiState::load(&app).unwrap();

        state.stream_idx = state.streams.len();
        state.reload_topics(&app).unwrap();
        state.reload_messages(&app).unwrap();

        assert!(state.selected_direct_messages());
        assert_eq!(state.direct_message_count, 2);
        assert_eq!(state.middle_len(), 1);
        assert_eq!(
            direct_conversation_label(state.selected_direct_conversation().unwrap()),
            "Grace Hopper"
        );
        assert_eq!(
            state
                .messages
                .iter()
                .map(|message| message.id)
                .collect::<Vec<_>>(),
            [10, 11]
        );
        assert_eq!(selected_destination_label(&state), "DM: Grace Hopper");
    }

    #[test]
    fn thread_lines_render_html_emoji_and_bold_spans() {
        let settings = test_settings(TempDir::new().unwrap().path());
        let app = App::open(settings, false).unwrap();
        let messages = vec![test_message_with_html(
            1,
            r#"<p>Awesome <span class="emoji emoji-1f60e" title="sunglasses">:sunglasses:</span> <strong>bold</strong></p>"#,
            "ignored",
        )];
        let theme = TuiTheme::zenburn();

        let lines = thread_lines(&messages, 0, 80, &theme, &app, 0);
        let body = &lines[1];

        assert_eq!(line_text(body), "Awesome 😎 bold");
        assert!(body.spans.iter().any(|span| {
            span.content.as_ref() == "bold" && span.style.add_modifier.contains(Modifier::BOLD)
        }));
    }

    #[test]
    fn selected_thread_message_uses_marker_without_selection_colors() {
        let settings = test_settings(TempDir::new().unwrap().path());
        let app = App::open(settings, false).unwrap();
        let messages = vec![
            test_message_with_text(1, "first"),
            test_message_with_text(2, "second"),
        ];
        let theme = TuiTheme::zenburn();

        let lines = thread_lines(&messages, 1, 80, &theme, &app, 0);

        assert_eq!(line_text(&lines[0]), "   [1] Ada");
        assert_eq!(line_text(&lines[3]), " > [2] Ada");
        assert_eq!(lines[3].spans[0].style.bg, Some(theme.background));
        assert_eq!(lines[3].spans[1].style.bg, Some(theme.background));
        assert_eq!(lines[4].spans[0].style.bg, Some(theme.background));
        assert_ne!(lines[3].spans[0].style.bg, Some(theme.active_selection_bg));
    }

    #[test]
    fn literal_shortcodes_expand_using_emoji_dataset() {
        let message = test_message_with_html(
            1,
            r#"<p>launch :rocket: :sunglasses: :not_real:</p>"#,
            "ignored",
        );

        let lines = message_body_lines(&message, 80, Style::default());

        assert_eq!(line_text(&lines[0]), "launch 🚀 😎 :not_real:");
    }

    #[test]
    fn styled_html_wrapping_preserves_span_styles() {
        let message =
            test_message_with_html(1, r#"<p>one two <strong>three</strong></p>"#, "ignored");

        let lines = message_body_lines(&message, 8, Style::default());

        assert_eq!(
            lines.iter().map(line_text).collect::<Vec<_>>(),
            ["one two", "three"]
        );
        assert!(lines[1].spans.iter().any(|span| {
            span.content.as_ref() == "three" && span.style.add_modifier.contains(Modifier::BOLD)
        }));
    }

    #[test]
    fn empty_input_cursor_starts_on_input_line() {
        let area = Rect::new(10, 20, 30, 10);

        assert_eq!(input_cursor_position(area, 2, ""), Some((11, 23)));
    }

    #[test]
    fn command_cursor_follows_text_width() {
        let area = Rect::new(10, 20, 30, 10);

        assert_eq!(input_cursor_position(area, 2, "abc"), Some((14, 23)));
    }

    #[test]
    fn input_cursor_counts_wide_emoji_cells() {
        let area = Rect::new(10, 20, 30, 10);

        assert_eq!(input_cursor_position(area, 2, "a😎"), Some((14, 23)));
    }

    #[test]
    fn input_cursor_clamps_to_overlay_width() {
        let area = Rect::new(10, 20, 10, 10);

        assert_eq!(
            input_cursor_position(area, 2, "way too long for the modal"),
            Some((18, 23))
        );
    }

    #[test]
    fn draft_cursor_follows_last_draft_line() {
        let area = Rect::new(10, 20, 30, 10);

        assert_eq!(draft_cursor_position(area, "one\ntwo"), Some((14, 27)));
    }

    #[test]
    fn draft_cursor_follows_wrapped_sentence() {
        let area = Rect::new(10, 20, 10, 10);

        assert_eq!(draft_cursor_position(area, "one two three"), Some((16, 27)));
    }

    #[test]
    fn draft_cursor_follows_wrapped_long_word() {
        let area = Rect::new(10, 20, 10, 10);

        assert_eq!(draft_cursor_position(area, "abcdefghijk"), Some((14, 27)));
    }

    #[test]
    fn emoji_width_uses_terminal_cell_width() {
        assert_eq!(display_width("a"), 1);
        assert_eq!(display_width("😎"), 2);
        assert_eq!(display_width("a😎"), 3);
    }

    #[test]
    fn wrapping_keeps_multi_codepoint_emoji_graphemes_intact() {
        let segments = [StyledSegment {
            text: "❤️x".to_string(),
            style: Style::default(),
        }];

        let lines = wrap_styled_segments(&segments, 1, Style::default());

        assert_eq!(lines.iter().map(line_text).collect::<Vec<_>>(), ["❤️", "x"]);
    }

    #[test]
    fn wrapping_counts_zwj_emoji_as_single_wide_grapheme() {
        let segments = [StyledSegment {
            text: "a👨‍💻b".to_string(),
            style: Style::default(),
        }];

        let lines = wrap_styled_segments(&segments, 3, Style::default());

        assert_eq!(
            lines.iter().map(line_text).collect::<Vec<_>>(),
            ["a👨‍💻", "b"]
        );
    }

    #[test]
    fn thread_lines_show_aggregated_reactions_under_message() {
        let settings = test_settings(TempDir::new().unwrap().path());
        let app = App::open(settings, false).unwrap();
        let mut message = test_message_with_text(1, "hello");
        message.raw_json = json!({
            "id": 1,
            "reactions": [
                {"emoji_name": "thumbs_up", "emoji_code": "1f44d", "reaction_type": "unicode_emoji"},
                {"emoji_name": "thumbs_up", "emoji_code": "1f44d", "reaction_type": "unicode_emoji"},
                {"emoji_name": "sunglasses", "emoji_code": "1f60e", "reaction_type": "unicode_emoji"}
            ]
        });
        let theme = TuiTheme::zenburn();

        let lines = thread_lines(&[message], 0, 80, &theme, &app, 0);
        let text = lines.iter().map(line_text).collect::<Vec<_>>();

        assert!(text.iter().any(|line| line == "👍 2  😎 1"));
    }

    #[test]
    fn reaction_summary_falls_back_to_shortcode_for_custom_emoji() {
        let mut message = test_message_with_text(1, "hello");
        message.raw_json = json!({
            "id": 1,
            "reactions": [
                {"emoji_name": "party parrot", "emoji_code": "123", "reaction_type": "realm_emoji"}
            ]
        });

        let line = reaction_line(&message, Style::default()).unwrap();

        assert_eq!(line_text(&line), ":party_parrot: 1");
    }

    #[test]
    fn thread_lines_show_image_placeholders_and_selected_slot() {
        let mut settings = test_settings(TempDir::new().unwrap().path());
        settings.zulip.base_url = Some("https://zulip.example".to_string());
        let app = App::open(settings, false).unwrap();
        let messages = vec![test_message_with_html(
            1,
            r#"<p>look</p><img src="/user_uploads/1/rack.png" alt="rack">"#,
            "look",
        )];
        let theme = TuiTheme::zenburn();

        let lines = thread_lines(&messages, 0, 40, &theme, &app, IMAGE_ROWS);
        let text = lines.iter().map(line_text).collect::<Vec<_>>();

        assert!(text.iter().any(|line| line == "[image: rack.png]"));
        assert_eq!(
            lines.len(),
            message_line_height(&messages[0], 40, &app, IMAGE_ROWS)
        );
    }

    #[test]
    fn image_mode_off_hides_placeholders() {
        let mut settings = test_settings(TempDir::new().unwrap().path());
        settings.zulip.base_url = Some("https://zulip.example".to_string());
        settings.ui.images = ImageMode::Off;
        let app = App::open(settings, false).unwrap();
        let messages = vec![test_message_with_html(
            1,
            r#"<p>look</p><img src="/user_uploads/1/rack.png" alt="rack">"#,
            "look",
        )];
        let theme = TuiTheme::zenburn();

        let lines = thread_lines(&messages, 0, 40, &theme, &app, 0);
        let text = lines.iter().map(line_text).collect::<Vec<_>>();

        assert!(!text.iter().any(|line| line.contains("[image:")));
    }

    #[cfg(feature = "tui-images")]
    #[test]
    fn iterm_env_prefers_iterm2_protocol() {
        assert!(terminal_env_prefers_iterm2_protocol(
            Some("iTerm.app"),
            None,
            None
        ));
        assert!(terminal_env_prefers_iterm2_protocol(
            None,
            Some("iTerm2"),
            None
        ));
        assert!(terminal_env_prefers_iterm2_protocol(
            None,
            None,
            Some("w0t0p0")
        ));
    }

    #[cfg(feature = "tui-images")]
    #[test]
    fn non_iterm_env_keeps_detected_protocol() {
        assert!(!terminal_env_prefers_iterm2_protocol(
            Some("ghostty"),
            None,
            None
        ));
        assert!(!terminal_env_prefers_iterm2_protocol(None, None, Some("")));
    }

    struct TuiFixture {
        _dir: TempDir,
        settings: Settings,
    }

    impl TuiFixture {
        fn new(message_ids: &[i64]) -> Self {
            let dir = TempDir::new().unwrap();
            let settings = Settings {
                zulip: ZulipConfig::default(),
                cache: CacheConfig::default(),
                ui: UiConfig::default(),
                safety: SafetyConfig::default(),
                digest: DigestConfig::default(),
                themes: BTreeMap::new(),
                paths: AppPaths {
                    config_file: dir.path().join("config.toml"),
                    cache_file: dir.path().join("cache.sqlite"),
                    secret_file: dir.path().join("api_key"),
                },
            };
            let fixture = Self {
                _dir: dir,
                settings,
            };
            let cache = Cache::open(&fixture.settings.paths.cache_file).unwrap();
            cache
                .upsert_streams(&[ZulipStream {
                    id: 1,
                    name: "core".to_string(),
                    description: None,
                }])
                .unwrap();
            drop(cache);
            fixture.insert_messages(message_ids);
            fixture
        }

        fn insert_messages(&self, message_ids: &[i64]) {
            let cache = Cache::open(&self.settings.paths.cache_file).unwrap();
            let messages = message_ids
                .iter()
                .map(|id| test_message(*id))
                .collect::<Vec<_>>();
            cache.upsert_messages(&messages).unwrap();
        }

        fn insert_direct_messages(&self, message_ids: &[i64], key: &str) {
            let cache = Cache::open(&self.settings.paths.cache_file).unwrap();
            let participants = vec![ZulipDmParticipant {
                id: Some(7),
                name: "Grace Hopper".to_string(),
                email: Some("grace@example.com".to_string()),
            }];
            let messages = message_ids
                .iter()
                .map(|id| test_direct_message(*id, key, participants.clone()))
                .collect::<Vec<_>>();
            cache.upsert_messages(&messages).unwrap();
        }
    }

    fn test_message(id: i64) -> ZulipMessage {
        test_message_with_text(id, &format!("message {id}"))
    }

    fn test_message_with_text(id: i64, text: &str) -> ZulipMessage {
        test_message_with_html(id, &format!("<p>{text}</p>"), text)
    }

    fn test_message_with_html(id: i64, html: &str, text: &str) -> ZulipMessage {
        ZulipMessage {
            id,
            message_type: ZulipMessageType::Stream,
            stream_id: Some(1),
            stream_name: "core".to_string(),
            topic: "ops".to_string(),
            dm_conversation_key: None,
            dm_participants: Vec::new(),
            sender_id: Some(2),
            sender_name: "Ada".to_string(),
            sender_email: Some("ada@example.com".to_string()),
            timestamp: Utc::now(),
            rendered_html: html.to_string(),
            plain_text: text.to_string(),
            permalink: None,
            raw_json: json!({ "id": id }),
        }
    }

    fn test_direct_message(
        id: i64,
        key: &str,
        participants: Vec<ZulipDmParticipant>,
    ) -> ZulipMessage {
        ZulipMessage {
            id,
            message_type: ZulipMessageType::Direct,
            stream_id: None,
            stream_name: "Direct messages".to_string(),
            topic: "Grace Hopper".to_string(),
            dm_conversation_key: Some(key.to_string()),
            dm_participants: participants,
            sender_id: Some(7),
            sender_name: "Grace Hopper".to_string(),
            sender_email: Some("grace@example.com".to_string()),
            timestamp: Utc.timestamp_opt(1_700_000_000 + id, 0).unwrap(),
            rendered_html: format!("<p>dm {id}</p>"),
            plain_text: format!("dm {id}"),
            permalink: None,
            raw_json: json!({ "id": id }),
        }
    }

    fn line_text(line: &Line<'_>) -> String {
        line.spans
            .iter()
            .map(|span| span.content.as_ref())
            .collect()
    }

    fn test_settings(path: &std::path::Path) -> Settings {
        Settings {
            zulip: ZulipConfig::default(),
            cache: CacheConfig::default(),
            ui: UiConfig::default(),
            safety: SafetyConfig::default(),
            digest: DigestConfig::default(),
            themes: BTreeMap::new(),
            paths: AppPaths {
                config_file: path.join("config.toml"),
                cache_file: path.join("cache.sqlite"),
                secret_file: path.join("api_key"),
            },
        }
    }
}
