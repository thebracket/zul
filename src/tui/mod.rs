use crate::app::{App, PollResult};
use crate::config::{Settings, ThemeConfig};
use crate::digest::{self, Digest};
use crate::zulip::models::{ZulipMessage, ZulipStream, ZulipTopic};
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, Paragraph, Wrap};
use ratatui::{Frame, Terminal};
use std::collections::BTreeMap;
use std::io::{self, Stdout};
use std::sync::mpsc::{self, Receiver, Sender};
use std::time::{Duration, Instant};

const POLL_INTERVAL: Duration = Duration::from_secs(5);

type PollEvent = Result<PollResult, String>;

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
}

struct TuiState {
    streams: Vec<ZulipStream>,
    topics: Vec<ZulipTopic>,
    messages: Vec<ZulipMessage>,
    search_results: Vec<ZulipMessage>,
    digest: Option<Digest>,
    stream_activity: BTreeMap<String, i64>,
    stream_idx: usize,
    topic_idx: usize,
    message_idx: usize,
    popup_scroll: usize,
    focus: Focus,
    mode: Mode,
    input: String,
    draft: String,
    status: String,
    last_poll_at: Option<DateTime<Utc>>,
    last_poll_new: usize,
    poll_error: Option<String>,
}

impl TuiState {
    fn load(app: &App) -> Result<Self> {
        let mut state = Self {
            streams: app.streams()?,
            topics: Vec::new(),
            messages: Vec::new(),
            search_results: Vec::new(),
            digest: None,
            stream_activity: app
                .stream_activity()?
                .into_iter()
                .map(|activity| (activity.stream_name, activity.message_count))
                .collect(),
            stream_idx: 0,
            topic_idx: 0,
            message_idx: 0,
            popup_scroll: 0,
            focus: Focus::Streams,
            mode: Mode::Browser,
            input: String::new(),
            draft: String::new(),
            status: "cache ready".to_string(),
            last_poll_at: None,
            last_poll_new: 0,
            poll_error: None,
        };
        state.reload_topics(app)?;
        state.reload_messages(app)?;
        state.select_latest_message();
        Ok(state)
    }

    fn selected_stream(&self) -> Option<&str> {
        self.streams
            .get(self.stream_idx)
            .map(|stream| stream.name.as_str())
    }

    fn selected_topic(&self) -> Option<&str> {
        self.topics
            .get(self.topic_idx)
            .map(|topic| topic.name.as_str())
    }

    fn reload_topics(&mut self, app: &App) -> Result<()> {
        self.topics = match self.selected_stream() {
            Some(stream) => app.topics(stream)?,
            None => Vec::new(),
        };
        self.topic_idx = self.topic_idx.min(self.topics.len().saturating_sub(1));
        Ok(())
    }

    fn reload_messages(&mut self, app: &App) -> Result<()> {
        self.messages = match (self.selected_stream(), self.selected_topic()) {
            (Some(stream), Some(topic)) => app.topic(stream, topic)?,
            _ => Vec::new(),
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
                self.stream_idx = move_index(self.stream_idx, self.streams.len(), delta);
                self.topic_idx = 0;
                self.message_idx = 0;
                self.reload_topics(app)?;
                self.reload_messages(app)?;
                self.select_latest_message();
            }
            Focus::Topics => {
                self.topic_idx = move_index(self.topic_idx, self.topics.len(), delta);
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

        self.stream_idx = selected_stream
            .as_ref()
            .and_then(|name| self.streams.iter().position(|stream| &stream.name == name))
            .unwrap_or_else(|| self.stream_idx.min(self.streams.len().saturating_sub(1)));

        self.reload_topics(app)?;
        self.topic_idx = selected_topic
            .as_ref()
            .and_then(|name| self.topics.iter().position(|topic| &topic.name == name))
            .unwrap_or_else(|| self.topic_idx.min(self.topics.len().saturating_sub(1)));

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
    let theme = TuiTheme::resolve(app.settings())?;
    let mut terminal = setup_terminal()?;
    let result = run_loop(&mut terminal, app, theme).await;
    restore_terminal(&mut terminal)?;
    result
}

async fn run_loop(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    app: App,
    theme: TuiTheme,
) -> Result<()> {
    let mut state = TuiState::load(&app)?;
    let (poll_tx, poll_rx) = mpsc::channel();
    let mut next_poll_at = Instant::now() + POLL_INTERVAL;
    let mut poll_inflight = false;

    loop {
        drain_poll_events(&poll_rx, &mut poll_inflight, &mut state, &app)?;
        if !app.offline() && !poll_inflight && Instant::now() >= next_poll_at {
            state.status = "polling Zulip...".to_string();
            spawn_poll(app.settings().clone(), app.offline(), poll_tx.clone());
            poll_inflight = true;
            next_poll_at = Instant::now() + POLL_INTERVAL;
        }

        terminal.draw(|frame| draw(frame, &state, &app, &theme))?;
        if !event::poll(Duration::from_millis(150))? {
            continue;
        }
        let Event::Key(key) = event::read()? else {
            continue;
        };
        if handle_key(key, &mut state, &app).await? {
            break;
        }
    }
    Ok(())
}

async fn handle_key(key: KeyEvent, state: &mut TuiState, app: &App) -> Result<bool> {
    match state.mode {
        Mode::SearchInput | Mode::Command | Mode::Composer => {
            return handle_input_key(key, state, app).await;
        }
        Mode::SearchResults | Mode::Digest | Mode::Auth => {
            return handle_scrollable_mode_key(key, state);
        }
        Mode::ConfirmSend => {
            match key.code {
                KeyCode::Char('y') | KeyCode::Char('Y') => {
                    let Some(stream) = state.selected_stream().map(ToString::to_string) else {
                        state.status = "no stream selected".to_string();
                        state.mode = Mode::Browser;
                        return Ok(false);
                    };
                    let Some(topic) = state.selected_topic().map(ToString::to_string) else {
                        state.status = "no topic selected".to_string();
                        state.mode = Mode::Browser;
                        return Ok(false);
                    };
                    let draft = state.draft.clone();
                    match app.reply(&stream, &topic, &draft, false, true).await {
                        Ok(_) => {
                            let was_at_end = state.thread_at_end();
                            match app.poll_new_messages().await {
                                Ok(result) => {
                                    state.refresh_from_cache(app, true)?;
                                    state.last_poll_at = Some(Utc::now());
                                    state.last_poll_new = result.new_messages;
                                    state.poll_error = None;
                                    state.status =
                                        format!("sent draft; {} new messages", result.new_messages);
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

fn draw(frame: &mut Frame<'_>, state: &TuiState, app: &App, theme: &TuiTheme) {
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
    draw_thread(frame, panes[2], state, theme);
    draw_status(frame, outer[1], state, app, theme);

    match state.mode {
        Mode::SearchInput | Mode::Command | Mode::Composer | Mode::ConfirmSend | Mode::Auth => {
            draw_overlay(frame, state, app, theme);
        }
        Mode::SearchResults => draw_search_results(frame, state, theme),
        Mode::Digest => draw_digest(frame, state, theme),
        Mode::Browser => {}
    }
}

fn draw_streams(frame: &mut Frame<'_>, area: Rect, state: &TuiState, theme: &TuiTheme) {
    let content_height = list_content_height(area);
    let start = visible_start(state.stream_idx, state.streams.len(), content_height);
    let active = state.focus == Focus::Streams;
    let items = state
        .streams
        .iter()
        .enumerate()
        .skip(start)
        .take(content_height)
        .map(|(idx, stream)| {
            let count = state
                .stream_activity
                .get(&stream.name)
                .copied()
                .unwrap_or(0);
            selected_item(
                idx == state.stream_idx,
                active,
                theme,
                &format!("{count:>4}  {}", stream.name),
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
    let start = visible_start(state.topic_idx, state.topics.len(), content_height);
    let active = state.focus == Focus::Topics;
    let items = state
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
        .collect::<Vec<_>>();
    frame.render_widget(
        List::new(items)
            .style(Style::default().fg(theme.text).bg(theme.background))
            .block(block("Topics", active, theme)),
        area,
    );
}

fn draw_thread(frame: &mut Frame<'_>, area: Rect, state: &TuiState, theme: &TuiTheme) {
    let content_height = list_content_height(area);
    let active = state.focus == Focus::Thread;
    let width = area.width.saturating_sub(4).max(1) as usize;
    let all_lines = thread_lines(&state.messages, state.message_idx, width, active, theme);
    let selected_line = selected_message_line_start(&state.messages, state.message_idx, width);
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
        "{} | {} | {} | {} | {}{} | j/k h/l / r R : d o y q | {}",
        account, realm, sync, poll, offline, draft, state.status
    );
    frame.render_widget(
        Paragraph::new(text).style(Style::default().fg(theme.status_fg).bg(theme.status_bg)),
        area,
    );
}

fn draw_overlay(frame: &mut Frame<'_>, state: &TuiState, app: &App, theme: &TuiTheme) {
    let area = centered_rect(70, 55, frame.area());
    frame.render_widget(Clear, area);
    let text = match state.mode {
        Mode::SearchInput => format!("Search cached messages\n\n{}", state.input),
        Mode::Command => format!("Command palette\n\n{}", state.input),
        Mode::Composer => {
            let stream = state.selected_stream().unwrap_or("no stream");
            let topic = state.selected_topic().unwrap_or("no topic");
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
                "Reply draft\nRealm: {realm}\nAccount: {account}\nDestination: #{stream} > {topic}\n\n{}\n\nCtrl-S previews send, Esc cancels",
                state.draft
            )
        }
        Mode::ConfirmSend => {
            let stream = state.selected_stream().unwrap_or("no stream");
            let topic = state.selected_topic().unwrap_or("no topic");
            format!(
                "Send this draft?\nDestination: #{stream} > {topic}\n\n{}\n\nPress y to send, n to cancel",
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
            format!("Auth/config status\n\nRealm: {realm}\nAccount: {account}\nAPI key: {secret}\nCache: {}", app.settings().paths.cache_file.display())
        }
        _ => String::new(),
    };
    frame.render_widget(
        Paragraph::new(text)
            .style(Style::default().fg(theme.modal_text).bg(theme.background))
            .block(block("zq", true, theme))
            .scroll((state.popup_scroll as u16, 0))
            .wrap(Wrap { trim: false }),
        area,
    );
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
                    format!("#{} > {} | ", message.stream_name, message.topic),
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
                "[{}] #{} > {} | {}",
                item.id, item.stream, item.topic, item.text
            )));
        }
        lines.push(Line::from(""));
        lines.push(Line::from("Attention items"));
        for item in digest.likely_attention_items.iter().take(12) {
            lines.push(Line::from(format!(
                "[{}] #{} > {} | {}",
                item.id, item.stream, item.topic, item.text
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

fn thread_lines<'a>(
    messages: &'a [ZulipMessage],
    selected_idx: usize,
    width: usize,
    active: bool,
    theme: &TuiTheme,
) -> Vec<Line<'a>> {
    let mut lines = Vec::new();
    for (idx, message) in messages.iter().enumerate() {
        let selected = idx == selected_idx;
        let selection_style = selection_style(selected, active, theme);
        let header_style = selection_style.unwrap_or_else(|| Style::default().bg(theme.background));
        lines.push(Line::from(vec![
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

        for wrapped in wrap_text(&message.plain_text, width) {
            let style = selection_style
                .unwrap_or_else(|| Style::default().fg(theme.text).bg(theme.background));
            lines.push(Line::from(Span::styled(wrapped, style)));
        }
        lines.push(Line::from(Span::styled(
            "",
            selection_style.unwrap_or_else(|| Style::default().bg(theme.background)),
        )));
    }

    lines
}

fn selection_style(selected: bool, active: bool, theme: &TuiTheme) -> Option<Style> {
    if !selected {
        return None;
    }
    let (fg, bg) = if active {
        (theme.active_selection_fg, theme.active_selection_bg)
    } else {
        (theme.inactive_selection_fg, theme.inactive_selection_bg)
    };
    Some(Style::default().fg(fg).bg(bg))
}

fn selected_message_line_start(
    messages: &[ZulipMessage],
    selected_idx: usize,
    width: usize,
) -> usize {
    messages
        .iter()
        .take(selected_idx)
        .map(|message| message_line_height(message, width))
        .sum()
}

fn message_line_height(message: &ZulipMessage, width: usize) -> usize {
    1 + wrap_text(&message.plain_text, width).len() + 1
}

fn visible_line_start(selected_line: usize, total_lines: usize, height: usize) -> usize {
    if total_lines <= height || height == 0 {
        return 0;
    }
    let half = height / 2;
    selected_line.saturating_sub(half).min(total_lines - height)
}

fn wrap_text(text: &str, width: usize) -> Vec<String> {
    let width = width.max(1);
    let mut lines = Vec::new();

    for source_line in text.lines().map(str::trim).filter(|line| !line.is_empty()) {
        let mut current = String::new();
        for word in source_line.split_whitespace() {
            if current.is_empty() {
                push_wrapped_word(&mut lines, &mut current, word, width);
            } else if current.len() + 1 + word.len() <= width {
                current.push(' ');
                current.push_str(word);
            } else {
                lines.push(current);
                current = String::new();
                push_wrapped_word(&mut lines, &mut current, word, width);
            }
        }
        if !current.is_empty() {
            lines.push(current);
        }
    }

    if lines.is_empty() {
        lines.push(String::new());
    }

    lines
}

fn push_wrapped_word(lines: &mut Vec<String>, current: &mut String, word: &str, width: usize) {
    if word.len() <= width {
        current.push_str(word);
        return;
    }

    let mut chunk = String::new();
    for ch in word.chars() {
        if chunk.len() + ch.len_utf8() > width {
            lines.push(chunk);
            chunk = String::new();
        }
        chunk.push(ch);
    }
    *current = chunk;
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
    use crate::cache::Cache;
    use crate::config::{
        AppPaths, CacheConfig, DigestConfig, SafetyConfig, Settings, UiConfig, ZulipConfig,
    };
    use chrono::Utc;
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
        let messages = vec![
            test_message_with_text(1, "short"),
            test_message_with_text(2, "one two three four five six seven eight"),
            test_message_with_text(3, "tail"),
        ];

        assert_eq!(selected_message_line_start(&messages, 0, 10), 0);
        assert!(selected_message_line_start(&messages, 2, 10) > 6);
        assert!(visible_line_start(selected_message_line_start(&messages, 2, 10), 12, 5) > 0);
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
    fn refresh_keeps_position_when_reading_older_messages() {
        let fixture = TuiFixture::new(&[1, 2, 3]);
        let app = App::open(fixture.settings.clone(), false).unwrap();
        let mut state = TuiState::load(&app).unwrap();
        state.message_idx = 0;

        fixture.insert_messages(&[4]);
        state.refresh_from_cache(&app, false).unwrap();

        assert_eq!(state.messages[state.message_idx].id, 1);
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
    }

    fn test_message(id: i64) -> ZulipMessage {
        test_message_with_text(id, &format!("message {id}"))
    }

    fn test_message_with_text(id: i64, text: &str) -> ZulipMessage {
        ZulipMessage {
            id,
            stream_id: Some(1),
            stream_name: "core".to_string(),
            topic: "ops".to_string(),
            sender_id: Some(2),
            sender_name: "Ada".to_string(),
            sender_email: Some("ada@example.com".to_string()),
            timestamp: Utc::now(),
            rendered_html: format!("<p>{text}</p>"),
            plain_text: text.to_string(),
            permalink: None,
            raw_json: json!({ "id": id }),
        }
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
