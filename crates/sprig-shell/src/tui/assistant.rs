//! The assistant pane (Ctrl-A): the tenant's data specialist or DBA in
//! place of a data tab's results, over `POST /v1/ai/chat`.
//!
//! The answer streams in: tool calls as quiet rows, the text, results the
//! user may see, and proposals. A proposal runs only after the usual
//! confirm (Enter, then `y`); `e` puts it in the editor, `d` dismisses it,
//! `y` copies it, `?` shows what was sent to the provider. `/dba`, `/data`
//! and `/new` switch the assistant or start a new conversation.

use std::fmt::Write as _;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Paragraph};
use serde_json::{Value, json};
use unicode_width::{UnicodeWidthChar, UnicodeWidthStr};

use crate::lang::Lang;
use crate::tui::editor::highlight;
use crate::tui::theme;

/// Which assistant answers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Role {
    /// The data specialist.
    #[default]
    Data,
    /// The DBA.
    Dba,
}

impl Role {
    /// The API's name.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Data => "data",
            Role::Dba => "dba",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Role::Data => "Data",
            Role::Dba => "DBA",
        }
    }
}

/// Where a proposal stands.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum State {
    /// Waiting for the user.
    Shown,
    /// Accepted; the gateway runs it.
    Running,
    /// Done, with what happened.
    Accepted(String),
    /// It failed, with why.
    Failed(String),
    /// Turned down.
    Dismissed,
    /// Put in the editor (nothing ran).
    Edited,
}

/// A proposal the user reviews.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Proposal {
    /// Its id, for `:accept` and `:dismiss`.
    pub id: String,
    /// `sql`, `generate_rows`, `recommendation`, `glossary` or `tile`.
    pub kind: String,
    /// The statement (or what it is about).
    pub sql: String,
    /// The database it runs in.
    pub db: Option<String>,
    /// Why, in a sentence.
    pub why: String,
    /// What it will do.
    pub estimate: String,
    /// Where it stands.
    pub state: State,
}

impl Proposal {
    /// Whether it is a statement the user can run here.
    #[must_use]
    pub fn runnable(&self) -> bool {
        (self.kind == "sql" || self.kind == "generate_rows") && !self.sql.trim().is_empty()
    }
}

/// One entry of the conversation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Item {
    /// The user's question.
    User(String),
    /// A tool call, as a quiet row.
    Tool {
        /// The call's id.
        id: String,
        /// What it did and on what.
        label: String,
        /// Undefined while it runs.
        ok: Option<bool>,
    },
    /// Answer text.
    Text(String),
    /// A result shown to the user only.
    Result(String),
    /// A proposal.
    Proposal(Proposal),
    /// An error.
    Error(String),
    /// A note from the TUI itself.
    Note(String),
}

/// What a key asks the app to do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Act {
    /// Nothing more.
    None,
    /// Send a question.
    Send(String),
    /// Stop the answer.
    Stop,
    /// Run the proposal at this item (after the confirm).
    Run(usize),
    /// Put it in the editor.
    Edit(usize),
    /// Dismiss it.
    Dismiss(usize),
    /// Copy text to the clipboard.
    Copy(String),
    /// Show what was sent.
    Sent,
    /// Back to the editor.
    Leave,
}

/// The pane's state.
#[derive(Debug, Default)]
pub struct Assistant {
    /// Shown in place of the results.
    pub open: bool,
    /// The assistant.
    pub role: Role,
    /// The conversation so far.
    pub items: Vec<Item>,
    /// The question being typed.
    pub input: String,
    /// An answer is streaming.
    pub running: bool,
    /// The conversation's id (continued by the next question).
    pub conversation: Option<String>,
    /// `ai.share_data` as the last turn applied it.
    pub share_data: Option<bool>,
    /// The model of the last turn.
    pub model: Option<String>,
    /// The selected proposal (an index into `items`).
    pub selected: Option<usize>,
    /// Lines scrolled up from the end.
    pub scroll: usize,
}

fn s(v: &Value, k: &str) -> String {
    v.get(k).and_then(Value::as_str).unwrap_or("").to_owned()
}

/// A tool call as a quiet row: what it did, and on what.
#[must_use]
pub fn tool_label(name: &str, input: &Value) -> String {
    let what = match name {
        "search_docs" => "searched docs",
        "catalog" => "read catalog",
        "describe" => "described",
        "relationships" => "read relationships",
        "workload" => "read workload",
        "fingerprint" => "read a statement's workload",
        "explain" => "explained",
        "indexes" => "checked indexes",
        "auto_indexes" => "checked automatic indexes",
        "settings" => "read settings",
        "column_stats" => "read column statistics",
        "run_query" => "ran read-only query",
        "test_policy" => "tried a policy",
        "compile_logic" => "compiled logic",
        "sensitive_columns" => "scanned for sensitive data",
        "check_ddl" => "checked schema statements",
        "migration_check" => "checked a migration",
        "decision_log" => "read the decision log",
        "propose_sql" | "propose_policy" | "propose_logic" => "proposed a statement",
        "propose_recommendation" => "proposed a recommendation",
        "propose_tile" => "proposed a chart",
        "propose_glossary" => "proposed a glossary term",
        "generate_rows" => "generated test rows",
        other => return other.replace('_', " "),
    };
    let db = s(input, "db");
    let table = s(input, "table");
    let about = match (db.is_empty(), table.is_empty()) {
        (_, false) if !db.is_empty() => format!("{db}.{table}"),
        (_, false) => table,
        (false, true) => db,
        (true, true) => s(input, "query"),
    };
    if about.is_empty() {
        what.to_owned()
    } else {
        format!("{what} · {about}")
    }
}

fn proposal_of(p: &Value) -> Proposal {
    let body = p.get("body").cloned().unwrap_or(Value::Null);
    let kind = s(p, "kind");
    let sql = match kind.as_str() {
        "generate_rows" => p.get("generated").map(|g| s(g, "sql")).unwrap_or_default(),
        "recommendation" => format!("{}: {}", s(&body, "subject"), s(&body, "body")),
        "glossary" => format!("{} = {}", s(&body, "term"), s(&body, "definition")),
        _ => s(&body, "sql"),
    };
    let db = Some(s(&body, "db")).filter(|d| !d.is_empty());
    Proposal {
        id: s(p, "id"),
        kind,
        sql,
        db,
        why: s(&body, "why"),
        estimate: s(&body, "estimate"),
        state: State::Shown,
    }
}

/// A `run_query` result in a line or two: its columns and first rows.
fn result_text(r: &Value) -> String {
    let cols: Vec<String> = r
        .get("columns")
        .and_then(Value::as_array)
        .map(|c| c.iter().map(|c| s(c, "name")).collect())
        .unwrap_or_default();
    let rows = r
        .get("rows")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let n = r
        .get("row_count")
        .and_then(Value::as_u64)
        .unwrap_or(rows.len() as u64);
    let mut out = format!(
        "{n} row{} · {}",
        if n == 1 { "" } else { "s" },
        cols.join(", ")
    );
    for row in rows.iter().take(5) {
        let cells: Vec<String> = row
            .as_array()
            .map(|r| {
                r.iter()
                    .map(|c| match c {
                        Value::Null => "NULL".to_owned(),
                        Value::String(t) => t.clone(),
                        other => other.to_string(),
                    })
                    .collect()
            })
            .unwrap_or_default();
        out.push_str("\n  ");
        out.push_str(&cells.join(" | "));
    }
    out
}

impl Assistant {
    /// Applies one line of the chat stream.
    pub fn apply(&mut self, v: &Value) {
        let at_end = self.scroll == 0;
        match v.get("type").and_then(Value::as_str).unwrap_or("") {
            "start" => {
                if let Some(c) = v.get("conversation").and_then(Value::as_str) {
                    self.conversation = Some(c.to_owned());
                }
                self.share_data = v.get("share_data").and_then(Value::as_bool);
                self.model = Some(s(v, "model")).filter(|m| !m.is_empty());
            }
            "text" => {
                let t = s(v, "text");
                if let Some(Item::Text(last)) = self.items.last_mut() {
                    last.push_str(&t);
                } else if !t.trim().is_empty() {
                    self.items
                        .push(Item::Text(t.trim_start_matches('\n').to_owned()));
                }
            }
            "tool_start" => {
                let input = v.get("input").cloned().unwrap_or(Value::Null);
                self.items.push(Item::Tool {
                    id: s(v, "id"),
                    label: tool_label(&s(v, "name"), &input),
                    ok: None,
                });
            }
            "tool_end" => {
                let id = s(v, "id");
                let ok = v.get("ok").and_then(Value::as_bool);
                for it in &mut self.items {
                    if let Item::Tool { id: i, ok: o, .. } = it
                        && *i == id
                    {
                        *o = ok;
                    }
                }
            }
            "result" => {
                if let Some(r) = v.get("result") {
                    self.items.push(Item::Result(result_text(r)));
                }
            }
            "proposal" => {
                if let Some(p) = v.get("proposal") {
                    let p = proposal_of(p);
                    let dup = self
                        .items
                        .iter()
                        .any(|i| matches!(i, Item::Proposal(x) if x.id == p.id));
                    if !dup {
                        self.items.push(Item::Proposal(p));
                    }
                }
            }
            "error" => {
                let e = v.get("error").cloned().unwrap_or(Value::Null);
                let code = e.get("code").and_then(Value::as_u64);
                let msg = s(&e, "message");
                self.items.push(Item::Error(match code {
                    Some(c) => format!("{c}: {msg}"),
                    None => msg,
                }));
            }
            "end" => match v.get("detail").and_then(Value::as_str) {
                Some("max_tool_rounds") => self.items.push(Item::Note(
                    "the answer reached its limit of tool rounds".to_owned(),
                )),
                Some("stopped") => self.items.push(Item::Note("stopped".to_owned())),
                _ if s(v, "stop") == "max_tokens" => self.items.push(Item::Note(
                    "the answer was cut off at its length limit".to_owned(),
                )),
                _ => {}
            },
            _ => {}
        }
        if at_end {
            self.scroll = 0;
        }
    }

    /// The answer ended (with the error that ended it, if any). The
    /// newest proposal waiting is selected.
    pub fn done(&mut self, error: Option<String>) {
        self.running = false;
        if let Some(e) = error {
            self.items.push(Item::Error(e));
        }
        self.selected = self.waiting().last().copied();
    }

    /// Items holding proposals that wait for the user.
    #[must_use]
    pub fn waiting(&self) -> Vec<usize> {
        self.items
            .iter()
            .enumerate()
            .filter(|(_, i)| matches!(i, Item::Proposal(p) if p.state == State::Shown))
            .map(|(i, _)| i)
            .collect()
    }

    /// The proposal at `i`.
    pub fn proposal_mut(&mut self, i: usize) -> Option<&mut Proposal> {
        match self.items.get_mut(i) {
            Some(Item::Proposal(p)) => Some(p),
            _ => None,
        }
    }

    /// The request body of a question, with the editor's statement as
    /// context and the tab's database.
    #[must_use]
    pub fn body(&self, message: &str, database: Option<&str>, query: Option<&str>) -> Value {
        let mut b = json!({"role": self.role.as_str(), "message": message});
        if let Some(c) = &self.conversation {
            b["conversation"] = json!(c);
        }
        if let Some(d) = database.filter(|d| !d.is_empty()) {
            b["database"] = json!(d);
        }
        if let Some(q) = query.filter(|q| !q.trim().is_empty()) {
            b["context"] = json!([{"kind": "query", "ref": q}]);
        }
        b
    }

    /// A slash command (`/dba`, `/data`, `/new`); false for a question.
    pub fn command(&mut self, text: &str) -> bool {
        let role = |r: Role, me: &mut Self| {
            if me.role != r {
                me.role = r;
                me.conversation = None;
                me.items.push(Item::Note(format!(
                    "now asking the {} assistant (a new conversation)",
                    r.label()
                )));
            }
        };
        match text.trim() {
            "/dba" => role(Role::Dba, self),
            "/data" => role(Role::Data, self),
            "/new" => {
                self.items.clear();
                self.conversation = None;
                self.selected = None;
                self.items.push(Item::Note("a new conversation".to_owned()));
            }
            _ => return false,
        }
        true
    }

    /// Handles a key while the pane has the keys.
    pub fn key(&mut self, k: KeyEvent) -> Act {
        let ctrl = k.modifiers.contains(KeyModifiers::CONTROL);
        if ctrl && k.code == KeyCode::Char('c') {
            if self.running {
                return Act::Stop;
            }
            self.input.clear();
            return Act::None;
        }
        match k.code {
            KeyCode::PageUp => {
                self.scroll += 10;
                return Act::None;
            }
            KeyCode::PageDown => {
                self.scroll = self.scroll.saturating_sub(10);
                return Act::None;
            }
            _ => {}
        }
        if let Some(sel) = self.selected {
            match k.code {
                KeyCode::Enter => return Act::Run(sel),
                KeyCode::Char('e') if !ctrl => return Act::Edit(sel),
                KeyCode::Char('d') if !ctrl => return Act::Dismiss(sel),
                KeyCode::Char('y') if !ctrl => {
                    return match self.items.get(sel) {
                        Some(Item::Proposal(p)) => Act::Copy(p.sql.clone()),
                        _ => Act::None,
                    };
                }
                KeyCode::Char('?') => return Act::Sent,
                KeyCode::Up | KeyCode::Down => {
                    let w = self.waiting();
                    let at = w.iter().position(|i| *i == sel).unwrap_or(0);
                    let next = if k.code == KeyCode::Up {
                        at.checked_sub(1)
                    } else {
                        Some(at + 1).filter(|n| *n < w.len())
                    };
                    self.selected = match next {
                        Some(n) => w.get(n).copied(),
                        None if k.code == KeyCode::Down => None,
                        None => Some(sel),
                    };
                    return Act::None;
                }
                KeyCode::Tab | KeyCode::Esc => {
                    self.selected = None;
                    return Act::None;
                }
                KeyCode::Char(c) if !ctrl => {
                    // Typing goes back to the question.
                    self.selected = None;
                    self.input.push(c);
                    return Act::None;
                }
                _ => return Act::None,
            }
        }
        match k.code {
            KeyCode::Enter => {
                let text = std::mem::take(&mut self.input);
                if text.trim().is_empty() || self.running {
                    self.input = text;
                    return Act::None;
                }
                if self.command(&text) {
                    return Act::None;
                }
                self.scroll = 0;
                Act::Send(text.trim().to_owned())
            }
            KeyCode::Backspace => {
                self.input.pop();
                Act::None
            }
            KeyCode::Tab | KeyCode::Up if self.input.is_empty() => {
                self.selected = self.waiting().last().copied();
                Act::None
            }
            KeyCode::Esc => Act::Leave,
            KeyCode::Char('u') if ctrl => {
                self.input.clear();
                Act::None
            }
            KeyCode::Char(c) if !ctrl => {
                self.input.push(c);
                Act::None
            }
            _ => Act::None,
        }
    }

    /// The pane's title.
    #[must_use]
    pub fn title(&self) -> String {
        let data = match self.share_data {
            Some(true) => "data shared",
            _ => "schema only",
        };
        format!(" * Assistant · {} · {data} ", self.role.label())
    }

    /// The conversation as lines `w` columns wide.
    #[must_use]
    pub fn lines(&self, w: usize) -> Vec<Line<'static>> {
        let w = w.max(20);
        let mut out: Vec<Line<'static>> = Vec::new();
        if self.items.is_empty() {
            out.push(Line::styled(
                format!(
                    "Ask the {} assistant about {}. It sees your schema and statistics; nothing runs until you confirm.",
                    self.role.label(),
                    if self.role == Role::Dba { "tuning, indexes, policies and schema" } else { "your data" },
                ),
                theme::muted(),
            ));
            for l in wrap(
                "Try: which tables are largest?  ·  /dba or /data switches the assistant  ·  /new starts over",
                w,
            ) {
                out.push(Line::styled(l, theme::muted()));
            }
            return out;
        }
        for (i, it) in self.items.iter().enumerate() {
            match it {
                Item::User(t) => {
                    for (j, l) in wrap(t, w.saturating_sub(5)).into_iter().enumerate() {
                        out.push(Line::from(vec![
                            Span::styled(if j == 0 { "you  " } else { "     " }, theme::accent()),
                            Span::raw(l),
                        ]));
                    }
                }
                Item::Tool { label, ok, .. } => {
                    let tail = match ok {
                        None => " …",
                        Some(false) => " · failed",
                        Some(true) => "",
                    };
                    out.push(Line::styled(
                        crate::tui::clip(&format!("▸ {label}{tail}"), w),
                        theme::muted(),
                    ));
                }
                Item::Text(t) => {
                    for l in t.lines() {
                        if l.trim().is_empty() {
                            out.push(Line::from(""));
                            continue;
                        }
                        for x in wrap(l.trim_end(), w) {
                            out.push(Line::raw(x));
                        }
                    }
                }
                Item::Result(t) => {
                    for (j, l) in t.lines().enumerate() {
                        let st = if j == 0 { theme::muted() } else { Style::new() };
                        out.push(Line::styled(crate::tui::clip(l, w), st));
                    }
                }
                Item::Proposal(p) => self.proposal_lines(i, p, w, &mut out),
                Item::Error(e) => {
                    for l in wrap(e, w) {
                        out.push(Line::styled(l, theme::error()));
                    }
                }
                Item::Note(n) => out.push(Line::styled(crate::tui::clip(n, w), theme::muted())),
            }
        }
        out
    }

    fn proposal_lines(&self, i: usize, p: &Proposal, w: usize, out: &mut Vec<Line<'static>>) {
        let selected = self.selected == Some(i);
        let edge = if p.state == State::Shown {
            theme::accent()
        } else {
            theme::border()
        };
        let inner = w.saturating_sub(4);
        let head = match p.kind.as_str() {
            "recommendation" => "recommendation",
            "glossary" => "glossary term",
            "generate_rows" => "test rows",
            "tile" => "chart",
            _ => "proposed",
        };
        let dash = w.saturating_sub(head.width() + 4);
        out.push(Line::from(vec![
            Span::styled("┌ ", edge),
            Span::styled(head.to_owned(), theme::accent()),
            Span::styled(format!(" {}┐", "─".repeat(dash)), edge),
        ]));
        let boxed = |spans: Vec<Span<'static>>, used: usize| {
            let mut v = vec![Span::styled("│ ", edge)];
            v.extend(spans);
            v.push(Span::raw(" ".repeat(inner.saturating_sub(used))));
            v.push(Span::styled(" │", edge));
            Line::from(v)
        };
        let shown_sql = if p.sql.chars().count() > 2000 {
            format!("{}…", p.sql.chars().take(2000).collect::<String>())
        } else {
            p.sql.clone()
        };
        for l in shown_sql.lines() {
            for x in wrap(l, inner) {
                let used = x.width();
                let spans: Vec<Span<'static>> = if p.runnable() {
                    highlight(&x, Lang::Sql)
                        .into_iter()
                        .map(|(st, t)| Span::styled(t, st))
                        .collect()
                } else {
                    vec![Span::raw(x)]
                };
                out.push(boxed(spans, used));
            }
        }
        for extra in [&p.estimate, &p.why] {
            for x in wrap(extra, inner) {
                let used = x.width();
                out.push(boxed(vec![Span::styled(x, theme::muted())], used));
            }
        }
        let status = match &p.state {
            State::Shown => None,
            State::Running => Some(("running…".to_owned(), theme::muted())),
            State::Accepted(t) => Some((format!("done · {t}"), theme::added())),
            State::Failed(t) => Some((format!("failed · {t}"), theme::error())),
            State::Dismissed => Some(("dismissed".to_owned(), theme::muted())),
            State::Edited => Some(("put in the editor; nothing ran".to_owned(), theme::muted())),
        };
        if let Some((t, st)) = status {
            for x in wrap(&t, inner) {
                let used = x.width();
                out.push(boxed(vec![Span::styled(x, st)], used));
            }
        }
        out.push(Line::styled(
            format!("└{}┘", "─".repeat(w.saturating_sub(2))),
            edge,
        ));
        if selected {
            let run = if p.runnable() {
                " Enter run (asks first) "
            } else if p.kind == "recommendation" {
                " Enter send to approvals "
            } else {
                " Enter accept "
            };
            out.push(Line::from(vec![
                Span::styled(run, theme::selected()),
                Span::raw(if p.runnable() {
                    "  e edit  d dismiss  y copy  ? what was sent"
                } else {
                    "  d dismiss   y copy   ? what was sent"
                }),
            ]));
        }
    }

    /// Draws the pane in `area`.
    pub fn draw(&self, f: &mut Frame<'_>, area: Rect, focused: bool) {
        let style = if focused {
            theme::focus_border()
        } else {
            theme::accent()
        };
        let block = Block::bordered()
            .border_style(style)
            .title(Span::styled(self.title(), theme::accent()))
            .title_top(Line::from(" Ctrl-A ").right_aligned());
        let inner = block.inner(area);
        f.render_widget(block, area);
        if inner.height < 2 || inner.width < 10 {
            return;
        }
        let w = inner.width as usize;
        let body_h = inner.height.saturating_sub(1) as usize;
        let all = self.lines(w);
        let end = all
            .len()
            .saturating_sub(self.scroll.min(all.len().saturating_sub(body_h)));
        let start = end.saturating_sub(body_h);
        let shown: Vec<Line<'static>> = all[start..end].to_vec();
        f.render_widget(
            Paragraph::new(shown),
            Rect {
                height: inner.height - 1,
                ..inner
            },
        );
        let prompt = if self.running {
            Line::from(vec![
                Span::styled("› ", theme::accent()),
                Span::styled("answering… Ctrl-C stops", theme::muted()),
            ])
        } else if self.input.is_empty() {
            Line::from(vec![
                Span::styled("› ", theme::accent()),
                Span::styled(
                    if self.items.is_empty() {
                        "ask a question..."
                    } else {
                        "ask a follow-up..."
                    },
                    theme::muted(),
                ),
            ])
        } else {
            Line::from(vec![
                Span::styled("› ", theme::accent()),
                Span::raw(tail_fit(&self.input, w.saturating_sub(3))),
            ])
        };
        let input = Rect {
            y: inner.y + inner.height - 1,
            height: 1,
            ..inner
        };
        f.render_widget(Paragraph::new(prompt), input);
        if focused && self.selected.is_none() && !self.running {
            let x = input.x
                + 2
                + u16::try_from(tail_fit(&self.input, w.saturating_sub(3)).width()).unwrap_or(0);
            if x < input.right() {
                f.set_cursor_position((x, input.y));
            }
        }
    }
}

/// The end of `s` that fits `w` columns.
fn tail_fit(s: &str, w: usize) -> String {
    if s.width() <= w {
        return s.to_owned();
    }
    let mut out: Vec<char> = Vec::new();
    let mut used = 0;
    for c in s.chars().rev() {
        let cw = c.width().unwrap_or(0);
        if used + cw + 1 > w {
            break;
        }
        used += cw;
        out.push(c);
    }
    out.push('…');
    out.into_iter().rev().collect()
}

/// `text` wrapped at word boundaries to `w` columns (long words split).
#[must_use]
pub fn wrap(text: &str, w: usize) -> Vec<String> {
    let w = w.max(1);
    let mut out = Vec::new();
    let mut line = String::new();
    let mut used = 0;
    for word in text.split(' ') {
        let ww = word.width();
        if used > 0 && used + 1 + ww > w {
            out.push(std::mem::take(&mut line));
            used = 0;
        }
        if ww > w {
            for c in word.chars() {
                let cw = c.width().unwrap_or(0);
                if used + cw > w {
                    out.push(std::mem::take(&mut line));
                    used = 0;
                }
                line.push(c);
                used += cw;
            }
            continue;
        }
        if used > 0 {
            line.push(' ');
            used += 1;
        }
        line.push_str(word);
        used += ww;
    }
    if !line.is_empty() || out.is_empty() {
        out.push(line);
    }
    out
}

/// "What was sent", from a stored conversation with its payloads.
#[must_use]
pub fn sent_text(conv: &Value) -> String {
    let mut out = String::from(
        "What was sent to the AI provider in this conversation (payloads are kept for ai.audit_payload_days):\n",
    );
    let mut n = 0;
    for m in conv
        .get("messages")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        if m.get("kind").and_then(Value::as_str) != Some("system_note") {
            continue;
        }
        let b = m.get("body").cloned().unwrap_or(Value::Null);
        n += 1;
        let usage = b.get("usage").cloned().unwrap_or(Value::Null);
        let _ = write!(
            out,
            "\ncall {n} · {} · {} · {} in, {} out\nsha256 {}\n",
            s(&b, "provider"),
            s(&b, "model"),
            usage
                .get("input_tokens")
                .and_then(Value::as_u64)
                .unwrap_or(0),
            usage
                .get("output_tokens")
                .and_then(Value::as_u64)
                .unwrap_or(0),
            s(&b, "sha256"),
        );
        match b.get("payload") {
            Some(p) => {
                let text = serde_json::to_string_pretty(p).unwrap_or_default();
                out.push_str(&crate::tui::clip_lines(&text, 40));
                out.push('\n');
            }
            None => out.push_str("(the payload is no longer kept)\n"),
        }
    }
    if n == 0 {
        out.push_str("\nnothing yet\n");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wraps_words_and_splits_long_ones() {
        assert_eq!(wrap("a bb ccc dddd", 6), vec!["a bb", "ccc", "dddd"]);
        assert_eq!(wrap("abcdefgh", 3), vec!["abc", "def", "gh"]);
        assert_eq!(wrap("", 5), vec![""]);
    }

    #[test]
    fn labels_tools() {
        assert_eq!(
            tool_label("describe", &json!({"db": "shop", "table": "orders"})),
            "described · shop.orders"
        );
        assert_eq!(
            tool_label("workload", &json!({"db": "shop"})),
            "read workload · shop"
        );
        assert_eq!(tool_label("new_thing", &json!({})), "new thing");
    }

    #[test]
    fn slash_commands_switch_and_start_over() {
        let mut a = Assistant {
            conversation: Some("4".into()),
            ..Assistant::default()
        };
        assert!(a.command("/dba"));
        assert_eq!(a.role, Role::Dba);
        assert_eq!(a.conversation, None);
        assert!(a.command("/new"));
        assert!(!a.command("why is it slow?"));
        assert_eq!(
            a.body("q", Some("shop"), Some("SELECT 1")),
            json!({"role": "dba", "message": "q", "database": "shop",
                   "context": [{"kind": "query", "ref": "SELECT 1"}]})
        );
    }

    #[test]
    fn what_was_sent_lists_calls() {
        let t = sent_text(&json!({"messages": [
            {"kind": "user", "body": {"text": "q"}},
            {"kind": "system_note", "body": {"provider": "anthropic", "model": "m", "sha256": "ab",
                "usage": {"input_tokens": 5, "output_tokens": 2}, "payload": {"x": 1}}}]}));
        assert!(t.contains("call 1 · anthropic · m · 5 in, 2 out"));
        assert!(t.contains("\"x\": 1"));
    }
}
