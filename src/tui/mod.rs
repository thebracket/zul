use crate::app::App;
use crate::digest::{self, Digest};
use crate::zulip::models::{ZulipMessage, ZulipStream, ZulipTopic};
use anyhow::Result;
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
use std::time::Duration;

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
        };
        state.reload_topics(app)?;
        state.reload_messages(app)?;
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

    fn move_selection(&mut self, delta: isize, app: &App) -> Result<()> {
        match self.focus {
            Focus::Streams => {
                self.stream_idx = move_index(self.stream_idx, self.streams.len(), delta);
                self.topic_idx = 0;
                self.message_idx = 0;
                self.reload_topics(app)?;
                self.reload_messages(app)?;
            }
            Focus::Topics => {
                self.topic_idx = move_index(self.topic_idx, self.topics.len(), delta);
                self.message_idx = 0;
                self.reload_messages(app)?;
            }
            Focus::Thread => {
                self.message_idx = move_index(self.message_idx, self.messages.len(), delta);
            }
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
    let mut terminal = setup_terminal()?;
    let result = run_loop(&mut terminal, app).await;
    restore_terminal(&mut terminal)?;
    result
}

async fn run_loop(terminal: &mut Terminal<CrosstermBackend<Stdout>>, app: App) -> Result<()> {
    let mut state = TuiState::load(&app)?;
    loop {
        terminal.draw(|frame| draw(frame, &state, &app))?;
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
                            state.status = "sent draft".to_string();
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
                *state = TuiState::load(app)?;
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

fn draw(frame: &mut Frame<'_>, state: &TuiState, app: &App) {
    let outer = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(5), Constraint::Length(1)])
        .split(frame.area());
    let panes = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(24),
            Constraint::Percentage(28),
            Constraint::Percentage(48),
        ])
        .split(outer[0]);

    draw_streams(frame, panes[0], state);
    draw_topics(frame, panes[1], state);
    draw_thread(frame, panes[2], state);
    draw_status(frame, outer[1], state, app);

    match state.mode {
        Mode::SearchInput | Mode::Command | Mode::Composer | Mode::ConfirmSend | Mode::Auth => {
            draw_overlay(frame, state, app);
        }
        Mode::SearchResults => draw_search_results(frame, state),
        Mode::Digest => draw_digest(frame, state),
        Mode::Browser => {}
    }
}

fn draw_streams(frame: &mut Frame<'_>, area: Rect, state: &TuiState) {
    let content_height = list_content_height(area);
    let start = visible_start(state.stream_idx, state.streams.len(), content_height);
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
                &format!("{count:>4}  {}", stream.name),
            )
        })
        .collect::<Vec<_>>();
    frame.render_widget(
        List::new(items).block(block("Streams", state.focus == Focus::Streams)),
        area,
    );
}

fn draw_topics(frame: &mut Frame<'_>, area: Rect, state: &TuiState) {
    let content_height = list_content_height(area);
    let start = visible_start(state.topic_idx, state.topics.len(), content_height);
    let items = state
        .topics
        .iter()
        .enumerate()
        .skip(start)
        .take(content_height)
        .map(|(idx, topic)| {
            selected_item(
                idx == state.topic_idx,
                &format!("{}  {}", topic.message_count, topic.name),
            )
        })
        .collect::<Vec<_>>();
    frame.render_widget(
        List::new(items).block(block("Topics", state.focus == Focus::Topics)),
        area,
    );
}

fn draw_thread(frame: &mut Frame<'_>, area: Rect, state: &TuiState) {
    let content_height = list_content_height(area);
    let start = visible_start(state.message_idx, state.messages.len(), content_height);
    let items = state
        .messages
        .iter()
        .enumerate()
        .skip(start)
        .take(content_height)
        .map(|(idx, message)| {
            selected_item(
                idx == state.message_idx,
                &format!(
                    "[{}] {}: {}",
                    message.id,
                    message.sender_name,
                    message.plain_text.replace('\n', " ")
                ),
            )
        })
        .collect::<Vec<_>>();
    frame.render_widget(
        List::new(items).block(block("Thread", state.focus == Focus::Thread)),
        area,
    );
}

fn draw_status(frame: &mut Frame<'_>, area: Rect, state: &TuiState, app: &App) {
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
    let text = format!(
        "{} | {} | {} | {}{} | j/k h/l / r R : d o y q | {}",
        account, realm, sync, offline, draft, state.status
    );
    frame.render_widget(
        Paragraph::new(text).style(Style::default().fg(Color::Black).bg(Color::Gray)),
        area,
    );
}

fn draw_overlay(frame: &mut Frame<'_>, state: &TuiState, app: &App) {
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
            .block(Block::default().title("zq").borders(Borders::ALL))
            .scroll((state.popup_scroll as u16, 0))
            .wrap(Wrap { trim: false }),
        area,
    );
}

fn draw_search_results(frame: &mut Frame<'_>, state: &TuiState) {
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
            ListItem::new(format!(
                "[{}] #{} > {} | {}: {}",
                message.id,
                message.stream_name,
                message.topic,
                message.sender_name,
                message.plain_text
            ))
        })
        .collect::<Vec<_>>();
    frame.render_widget(
        List::new(items).block(
            Block::default()
                .title("Search results")
                .borders(Borders::ALL),
        ),
        area,
    );
}

fn draw_digest(frame: &mut Frame<'_>, state: &TuiState) {
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
            .block(Block::default().title("Digest").borders(Borders::ALL))
            .scroll((state.popup_scroll as u16, 0))
            .wrap(Wrap { trim: false }),
        area,
    );
}

fn selected_item(selected: bool, text: &str) -> ListItem<'static> {
    if selected {
        ListItem::new(Line::from(vec![Span::styled(
            text.to_string(),
            Style::default().fg(Color::Black).bg(Color::Cyan),
        )]))
    } else {
        ListItem::new(text.to_string())
    }
}

fn block(title: &str, focused: bool) -> Block<'_> {
    let style = if focused {
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD)
    } else {
        Style::default()
    };
    Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(style)
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
}
