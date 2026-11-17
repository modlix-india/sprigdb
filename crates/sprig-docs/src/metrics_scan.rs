//! Finds SprigDB's metrics in the source: every `counter!`, `gauge!` and
//! `histogram!` call (the `metrics` crate's macros, with or without a
//! `metrics::` path) named `sprig_*`, with its label keys.
//!
//! It is a scanner, not a parser: it skips comments and string contents
//! while looking for the macros, and reads the call up to its matching
//! parenthesis. It understands:
//!
//! - a name written as a literal, or as a `const` or `static` `&str`
//!   (`NAME`, `module::NAME`, `sprig_crate::NAME`). The constant is looked
//!   up in the crate the path names, else in the same file, then the same
//!   crate, then the whole tree; the first of those that defines it decides,
//!   and only when its definitions agree;
//! - label keys written as `"key" => value`, or `KEY => value` with `KEY` a
//!   constant as above;
//! - labels passed as one expression: an array or `vec!` of `("key", value)`
//!   tuples (or `Label::new("key", value)`), written in the call or bound by
//!   a `let` earlier in the same function
//!   (`let labels = [("dc", …), …]; gauge!("…", &labels)`).
//!
//! What it can't read is left out and listed in [`Scan::unresolved`] with
//! its place, so the caller can say so instead of guessing.

use std::collections::{BTreeMap, BTreeSet};

/// The kind of a metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MetricKind {
    /// A monotonically increasing count.
    Counter,
    /// A value that goes up and down.
    Gauge,
    /// A distribution.
    Histogram,
}

impl MetricKind {
    /// The kind as written in docs.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            MetricKind::Counter => "counter",
            MetricKind::Gauge => "gauge",
            MetricKind::Histogram => "histogram",
        }
    }
}

/// One metric, merged over every place it is recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Metric {
    /// `sprig_…`.
    pub name: String,
    /// Its kind; a name recorded with two kinds keeps both.
    pub kinds: BTreeSet<MetricKind>,
    /// Label keys seen at any call.
    pub labels: BTreeSet<String>,
    /// Crates that record it (`sprig-gateway`, …).
    pub crates: BTreeSet<String>,
}

/// A part of a metric call the scanner couldn't read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unresolved {
    /// The crate of the call.
    pub krate: String,
    /// The file, as given to [`Scanner::scan_file`].
    pub file: String,
    /// The call's line, from 1.
    pub line: usize,
    /// The metric, when its name was read and only labels were not; `None`
    /// when the name itself wasn't, so the call is missing from
    /// [`Scan::metrics`].
    pub metric: Option<String>,
    /// The expression not read, as written.
    pub expr: String,
}

/// What a scan found.
#[derive(Debug, Clone, Default)]
pub struct Scan {
    /// Every metric, by name.
    pub metrics: Vec<Metric>,
    /// Names and labels not read, in scan order.
    pub unresolved: Vec<Unresolved>,
}

/// A name or label key: a literal, or a path to a constant.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Ident {
    Lit(String),
    Path(String),
}

impl Ident {
    fn text(&self) -> String {
        match self {
            Ident::Lit(s) => format!("\"{s}\""),
            Ident::Path(p) => p.clone(),
        }
    }
}

#[derive(Debug)]
struct Call {
    krate: String,
    file: String,
    file_id: usize,
    line: usize,
    kind: MetricKind,
    /// `Err` holds a name expression that is neither a literal nor a path.
    name: Result<Ident, String>,
    keys: Vec<Ident>,
    /// Label expressions not understood.
    opaque: Vec<String>,
}

#[derive(Debug)]
struct Const {
    krate: String,
    file_id: usize,
    name: String,
    value: String,
}

/// Collects metrics from source files.
#[derive(Debug, Default)]
pub struct Scanner {
    calls: Vec<Call>,
    consts: Vec<Const>,
    files: usize,
}

impl Scanner {
    /// Scans one file's text, attributing what it finds to `krate`.
    pub fn scan(&mut self, krate: &str, text: &str) {
        self.scan_file(krate, "", text);
    }

    /// Scans one file's text, attributing what it finds to `krate`; `file`
    /// names the file in [`Unresolved`] entries.
    pub fn scan_file(&mut self, krate: &str, file: &str, text: &str) {
        let file_id = self.files;
        self.files += 1;
        let b = text.as_bytes();
        let (mut line, mut line_at) = (1, 0);
        let mut i = 0;
        while i < b.len() {
            // Skip comments and string literals.
            if b[i] == b'/' && b.get(i + 1) == Some(&b'/') {
                i = memchr(b, i, b'\n');
                continue;
            }
            if b[i] == b'/' && b.get(i + 1) == Some(&b'*') {
                i = text[i + 2..].find("*/").map_or(b.len(), |p| i + 2 + p + 2);
                continue;
            }
            if let Some(end) = raw_string_end(b, i) {
                i = end;
                continue;
            }
            // A quote as a char literal ('"', '\"', b'"') starts no string.
            if b[i] == b'\'' {
                if b[i + 1..].starts_with(b"\"'") {
                    i += 3;
                    continue;
                }
                if b[i + 1..].starts_with(b"\\\"'") {
                    i += 4;
                    continue;
                }
            }
            if b[i] == b'"' {
                i = string_end(b, i);
                continue;
            }
            if word_at(b, i, "const") || word_at(b, i, "static") {
                if let Some((name, value, end)) = const_str(text, i) {
                    self.consts.push(Const {
                        krate: krate.to_owned(),
                        file_id,
                        name: name.to_owned(),
                        value: value.to_owned(),
                    });
                    i = end;
                } else {
                    i += 1;
                }
                continue;
            }
            let kind = [
                ("counter!(", MetricKind::Counter),
                ("gauge!(", MetricKind::Gauge),
                ("histogram!(", MetricKind::Histogram),
            ]
            .into_iter()
            .find(|(m, _)| b[i..].starts_with(m.as_bytes()) && (i == 0 || !is_ident(b[i - 1])));
            let Some((mac, kind)) = kind else {
                i += 1;
                continue;
            };
            // Both offsets are at a macro's first letter, so on char bounds.
            line += text[line_at..i].matches('\n').count();
            line_at = i;
            let open = i + mac.len() - 1;
            let close = matching_paren(b, open);
            if let Some((name, keys, opaque)) = read_call(&text[open + 1..close], &text[..i]) {
                self.calls.push(Call {
                    krate: krate.to_owned(),
                    file: file.to_owned(),
                    file_id,
                    line,
                    kind,
                    name,
                    keys,
                    opaque,
                });
            }
            i = close + 1;
        }
    }

    /// Everything found, by name.
    #[must_use]
    pub fn metrics(self) -> Vec<Metric> {
        self.finish().metrics
    }

    /// Resolves constants and returns the metrics with what wasn't read.
    #[must_use]
    pub fn finish(self) -> Scan {
        let mut found: BTreeMap<String, Metric> = BTreeMap::new();
        let mut unresolved = Vec::new();
        for c in &self.calls {
            let miss = |metric: Option<&str>, expr: String| Unresolved {
                krate: c.krate.clone(),
                file: c.file.clone(),
                line: c.line,
                metric: metric.map(str::to_owned),
                expr,
            };
            let resolved = match &c.name {
                Ok(id) => self.resolve(c, id).ok_or_else(|| id.text()),
                Err(expr) => Err(expr.clone()),
            };
            let name = match resolved {
                Ok(n) => n,
                Err(expr) => {
                    unresolved.push(miss(None, expr));
                    continue;
                }
            };
            if !name.starts_with("sprig_") {
                continue;
            }
            let m = found.entry(name.clone()).or_insert_with(|| Metric {
                name: name.clone(),
                kinds: BTreeSet::new(),
                labels: BTreeSet::new(),
                crates: BTreeSet::new(),
            });
            m.kinds.insert(c.kind);
            m.crates.insert(c.krate.clone());
            for k in &c.keys {
                match self.resolve(c, k) {
                    Some(k) => {
                        m.labels.insert(k);
                    }
                    None => unresolved.push(miss(Some(&name), k.text())),
                }
            }
            for o in &c.opaque {
                unresolved.push(miss(Some(&name), o.clone()));
            }
        }
        Scan {
            metrics: found.into_values().collect(),
            unresolved,
        }
    }

    /// The value of `id` as seen from call `c`.
    fn resolve(&self, c: &Call, id: &Ident) -> Option<String> {
        let path = match id {
            Ident::Lit(s) => return Some(s.clone()),
            Ident::Path(p) => p,
        };
        let segs: Vec<&str> = path.split("::").collect();
        let last = *segs.last()?;
        // Constants are upper case; anything else is a variable.
        if !last
            .bytes()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == b'_')
        {
            return None;
        }
        let values = |scope: &dyn Fn(&Const) -> bool| -> BTreeSet<&str> {
            self.consts
                .iter()
                .filter(|k| k.name == last && scope(k))
                .map(|k| k.value.as_str())
                .collect()
        };
        // A path through another crate (`sprig_gateway::session::NAME`).
        let other = (segs.len() > 1)
            .then(|| segs[0].replace('_', "-"))
            .filter(|k| *k != c.krate && self.consts.iter().any(|d| d.krate == *k));
        let found = if let Some(krate) = other {
            values(&|k| k.krate == krate)
        } else {
            let file = |k: &Const| k.file_id == c.file_id;
            let krate = |k: &Const| k.krate == c.krate;
            let all = |_: &Const| true;
            [&file as &dyn Fn(&Const) -> bool, &krate, &all]
                .into_iter()
                .map(values)
                .find(|v| !v.is_empty())
                .unwrap_or_default()
        };
        // Defined more than once with different values: not guessed.
        (found.len() == 1)
            .then(|| found.first().map(|v| (*v).to_owned()))
            .flatten()
    }
}

/// A call's name (`Err`: an expression that is no literal or path), label
/// keys, and label expressions not understood.
type CallParts = (Result<Ident, String>, Vec<Ident>, Vec<String>);

/// A metric call's parts; `None` for a call with no arguments. `before` is
/// the file up to the call.
fn read_call(args: &str, before: &str) -> Option<CallParts> {
    let parts = split_top(args);
    let first = parts.first()?;
    let name = key_of(first).ok_or_else(|| (*first).to_owned());
    let (mut keys, mut opaque) = (Vec::new(), Vec::new());
    for p in parts.iter().skip(1) {
        let pb = p.as_bytes();
        if let Some(arrow) = top_level(p)
            .into_iter()
            .find(|&i| pb[i] == b'=' && pb.get(i + 1) == Some(&b'>'))
        {
            match key_of(&p[..arrow]) {
                Some(k) => keys.push(k),
                None => opaque.push((*p).to_owned()),
            }
        } else {
            match label_keys(p, before) {
                Some(ks) => keys.extend(ks),
                None => opaque.push((*p).to_owned()),
            }
        }
    }
    Some((name, keys, opaque))
}

/// The keys of a label expression passed whole (`&labels`, `&[("k", v)]`).
fn label_keys(expr: &str, before: &str) -> Option<Vec<Ident>> {
    let mut e = expr.trim();
    e = e.strip_prefix('&').unwrap_or(e).trim();
    for suffix in [".clone()", ".to_vec()", "[..]"] {
        e = e.strip_suffix(suffix).unwrap_or(e);
    }
    if let Some(keys) = tuple_keys(e) {
        return Some(keys);
    }
    if e.bytes().all(is_ident) && !e.is_empty() {
        return tuple_keys(let_init(before, e)?);
    }
    None
}

/// The first element of each tuple in an array or `vec!` of tuples
/// (`[("k", v), …]`, `vec![Label::new("k", v), …]`).
fn tuple_keys(expr: &str) -> Option<Vec<Ident>> {
    let e = expr.trim();
    let e = e.strip_prefix("vec!").unwrap_or(e).trim_start();
    let inner = e.strip_prefix('[')?.strip_suffix(']')?;
    split_top(inner)
        .into_iter()
        .map(|el| {
            let open = el.find('(')?;
            let head = el[..open].trim();
            if !head.is_empty() && !matches!(key_of(head), Some(Ident::Path(_))) {
                return None;
            }
            let args = el[open + 1..].strip_suffix(')')?;
            key_of(split_top(args).first()?)
        })
        .collect()
}

/// The initializer of the last `let name = …;` before the end of `before`,
/// within the function the end lies in.
fn let_init<'a>(before: &'a str, name: &str) -> Option<&'a str> {
    let b = before.as_bytes();
    let scope = (0..b.len())
        .rev()
        .find(|&i| word_at(b, i, "fn"))
        .unwrap_or(0);
    let mut at = None;
    for (i, _) in before[scope..].match_indices("let") {
        let i = scope + i;
        if !word_at(b, i, "let") {
            continue;
        }
        let mut c = Cur {
            t: before,
            i: i + 3,
        };
        c.ws();
        if c.word("mut") {
            c.ws();
        }
        if c.ident() == Some(name) {
            at = Some(c.i);
        }
    }
    let rest = &before[at?..];
    let rb = rest.as_bytes();
    let eq = top_level(rest)
        .into_iter()
        .find(|&i| rb[i] == b'=' && rb.get(i + 1).is_none_or(|&c| c != b'=' && c != b'>'))?;
    let init = &rest[eq + 1..];
    let semi = top_level(init)
        .into_iter()
        .find(|&i| init.as_bytes()[i] == b';')?;
    Some(init[..semi].trim())
}

/// A literal key (`"k"`) or a path (`a::B`); `None` for anything else.
fn key_of(expr: &str) -> Option<Ident> {
    let e = expr.trim();
    let b = e.as_bytes();
    if b.first() == Some(&b'"') {
        return (b.len() >= 2 && string_end(b, 0) == b.len())
            .then(|| Ident::Lit(e[1..e.len() - 1].to_owned()));
    }
    let segs: Vec<&str> = e.split("::").map(str::trim).collect();
    let path = segs
        .iter()
        .all(|s| !s.is_empty() && s.bytes().all(is_ident) && !s.as_bytes()[0].is_ascii_digit());
    path.then(|| Ident::Path(segs.join("::")))
}

/// `const NAME: &str = "…";` (or `static`, or `&'static str`) at `at`: the
/// name, the value and the index just past the `;`.
fn const_str(t: &str, at: usize) -> Option<(&str, &str, usize)> {
    let mut c = Cur { t, i: at };
    if !c.word("const") && !c.word("static") {
        return None;
    }
    c.ws();
    let name = c.ident()?;
    c.ws();
    c.eat(":")?;
    c.ws();
    c.eat("&")?;
    c.ws();
    if c.eat("'static").is_some() {
        c.ws();
    }
    if !c.word("str") {
        return None;
    }
    c.ws();
    c.eat("=")?;
    c.ws();
    let value = c.string()?;
    c.ws();
    c.eat(";")?;
    Some((name, value, c.i))
}

/// A cursor over source text.
struct Cur<'a> {
    t: &'a str,
    i: usize,
}

impl<'a> Cur<'a> {
    fn rest(&self) -> &'a [u8] {
        self.t.as_bytes().get(self.i..).unwrap_or_default()
    }

    fn ws(&mut self) {
        self.i += self
            .rest()
            .iter()
            .take_while(|c| c.is_ascii_whitespace())
            .count();
    }

    fn eat(&mut self, s: &str) -> Option<()> {
        self.rest().starts_with(s.as_bytes()).then(|| {
            self.i += s.len();
        })
    }

    /// Eats `w` when it is a whole word.
    fn word(&mut self, w: &str) -> bool {
        let ok = self.rest().starts_with(w.as_bytes())
            && self.rest().get(w.len()).is_none_or(|&c| !is_ident(c));
        if ok {
            self.i += w.len();
        }
        ok
    }

    fn ident(&mut self) -> Option<&'a str> {
        let n = self.rest().iter().take_while(|&&c| is_ident(c)).count();
        let s = self.i;
        self.i += n;
        (n > 0).then(|| &self.t[s..s + n])
    }

    fn string(&mut self) -> Option<&'a str> {
        if self.rest().first() != Some(&b'"') {
            return None;
        }
        let b = self.t.as_bytes();
        let end = string_end(b, self.i);
        if end < self.i + 2 || b[end - 1] != b'"' {
            return None;
        }
        let s = &self.t[self.i + 1..end - 1];
        self.i = end;
        Some(s)
    }
}

fn is_ident(c: u8) -> bool {
    c.is_ascii_alphanumeric() || c == b'_'
}

/// Whether the word `w` starts at `i` (not inside a longer identifier).
fn word_at(b: &[u8], i: usize, w: &str) -> bool {
    b[i..].starts_with(w.as_bytes())
        && (i == 0 || !is_ident(b[i - 1]))
        && b.get(i + w.len()).is_none_or(|&c| !is_ident(c))
}

fn memchr(b: &[u8], from: usize, c: u8) -> usize {
    b[from..]
        .iter()
        .position(|&x| x == c)
        .map_or(b.len(), |p| from + p)
}

/// The index just past the raw string literal (`r"..."`, `r#"..."#`,
/// `br"..."`) whose `r` is at `start`; `None` when none starts there.
fn raw_string_end(b: &[u8], start: usize) -> Option<usize> {
    if b[start] != b'r' {
        return None;
    }
    let before = |k: usize| start.checked_sub(k).map(|j| b[j]);
    let prefix_ok = match before(1) {
        None => true,
        Some(b'b') => before(2).is_none_or(|c| !is_ident(c)),
        Some(c) => !is_ident(c),
    };
    if !prefix_ok {
        return None;
    }
    let hashes = b[start + 1..].iter().take_while(|&&c| c == b'#').count();
    let open = start + 1 + hashes;
    if b.get(open) != Some(&b'"') {
        return None;
    }
    let mut close = Vec::with_capacity(hashes + 1);
    close.push(b'"');
    close.extend(std::iter::repeat_n(b'#', hashes));
    let end = b[open + 1..]
        .windows(close.len())
        .position(|w| w == close.as_slice())
        .map_or(b.len(), |p| open + 1 + p + close.len());
    Some(end)
}

/// The index just past the string literal starting at `start` (a `"`).
fn string_end(b: &[u8], start: usize) -> usize {
    let mut i = start + 1;
    while i < b.len() {
        match b[i] {
            b'\\' => i += 2,
            b'"' => return i + 1,
            _ => i += 1,
        }
    }
    b.len()
}

/// The index of the `)` matching the `(` at `open`, skipping strings.
fn matching_paren(b: &[u8], open: usize) -> usize {
    let mut depth = 0usize;
    let mut i = open;
    while i < b.len() {
        match b[i] {
            b'"' => {
                i = string_end(b, i);
                continue;
            }
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return i;
                }
            }
            _ => {}
        }
        i += 1;
    }
    b.len().saturating_sub(1)
}

/// The byte offsets in `s` outside strings, char literals and brackets
/// (brackets themselves excluded).
fn top_level(s: &str) -> Vec<usize> {
    let b = s.as_bytes();
    let mut out = Vec::new();
    let mut depth = 0usize;
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'"' => {
                i = string_end(b, i);
                continue;
            }
            // A char literal; anything else after a quote is a lifetime.
            b'\'' if b.get(i + 2) == Some(&b'\'') => {
                i += 3;
                continue;
            }
            b'\'' if b.get(i + 1) == Some(&b'\\') => {
                i = memchr(b, i + 2, b'\'') + 1;
                continue;
            }
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => depth = depth.saturating_sub(1),
            _ if depth == 0 => out.push(i),
            _ => {}
        }
        i += 1;
    }
    out
}

/// `s` split at its top-level commas, each part trimmed; empty parts (after
/// a trailing comma) dropped.
fn split_top(s: &str) -> Vec<&str> {
    let b = s.as_bytes();
    let mut out = Vec::new();
    let mut from = 0;
    for i in top_level(s).into_iter().filter(|&i| b[i] == b',') {
        out.push(s[from..i].trim());
        from = i + 1;
    }
    out.push(s[from..].trim());
    out.retain(|p| !p.is_empty());
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn labels(m: &Metric) -> Vec<&str> {
        m.labels.iter().map(String::as_str).collect()
    }

    #[test]
    fn finds_metrics_labels_and_kinds() {
        let src = r#"
            // counter!("sprig_commented_total") is ignored
            metrics::counter!("sprig_mover_errors_total", "code" => c.name()).increment(1);
            let s = "histogram!(\"sprig_in_a_string\")";
            histogram!(
                "sprig_turn_seconds",
                "role" => role.as_str(),
                "model" => m,
            ).record(t);
            gauge!("sprig_backlog", "x" => f("y")).set(1.0);
            counter!("other_total").increment(1);
            my_counter!("sprig_not_a_macro_of_ours");
        "#;
        let mut s = Scanner::default();
        s.scan("sprig-x", src);
        let scan = s.finish();
        assert!(scan.unresolved.is_empty(), "{:?}", scan.unresolved);
        let m = scan.metrics;
        let names: Vec<_> = m.iter().map(|m| m.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "sprig_backlog",
                "sprig_mover_errors_total",
                "sprig_turn_seconds"
            ]
        );
        assert_eq!(labels(&m[2]), ["model", "role"]);
        assert_eq!(labels(&m[0]), ["x"]);
        assert!(m[1].kinds.contains(&MetricKind::Counter));
        assert!(m[0].crates.contains("sprig-x"));
    }

    #[test]
    fn a_match_as_a_label_value_is_one_label() {
        let mut s = Scanner::default();
        s.scan(
            "sprig-x",
            r#"counter!("sprig_kept_total", "reason" => match k { A => "a", B => "b" }, "x" => 1);"#,
        );
        let scan = s.finish();
        assert!(scan.unresolved.is_empty(), "{:?}", scan.unresolved);
        assert_eq!(labels(&scan.metrics[0]), ["reason", "x"]);
    }

    #[test]
    fn non_ascii_source_is_fine() {
        let mut s = Scanner::default();
        s.scan(
            "sprig-y",
            "/// p95 4.2 s → ~40 ms · é\nlet x = \"ä\"; counter!(\"sprig_ok_total\", \"kind\" => \"π\");",
        );
        assert_eq!(s.metrics()[0].name, "sprig_ok_total");
    }

    #[test]
    fn a_quote_char_literal_starts_no_string() {
        let mut s = Scanner::default();
        s.scan(
            "sprig-z",
            "let q = matches!(c, '\\'' | '\"'); let e = b'\"'; let f = '\\\"';\n\
             histogram!(\"sprig_after_quotes_seconds\").record(1.0);",
        );
        assert_eq!(s.metrics()[0].name, "sprig_after_quotes_seconds");
    }

    #[test]
    fn raw_strings_are_skipped() {
        let mut s = Scanner::default();
        s.scan(
            "sprig-z",
            "let a = r#\"histogram!(\"sprig_in_raw\") \\\"#; let b = br\"x\\\";\n\
             counter!(\"sprig_after_raw_total\").increment(1);",
        );
        let names: Vec<_> = s.metrics().into_iter().map(|m| m.name).collect();
        assert_eq!(names, ["sprig_after_raw_total"]);
    }

    #[test]
    fn reads_str_constants() {
        assert_eq!(
            const_str(r#"const A: &str = "x";"#, 0).map(|(n, v, _)| (n, v)),
            Some(("A", "x"))
        );
        assert_eq!(
            const_str(r#"static  B : &'static str="y" ;rest"#, 0),
            Some(("B", "y", 30))
        );
        assert_eq!(const_str(r#"const C: &[&str] = &["x"];"#, 0), None);
        assert_eq!(const_str(r#"const D: u32 = 1;"#, 0), None);
        assert_eq!(const_str("const fn f() {}", 0), None);
        assert_eq!(const_str(r#"constant E: &str = "z";"#, 0), None);
    }

    #[test]
    fn names_from_constants_in_the_file() {
        let mut s = Scanner::default();
        s.scan_file(
            "sprig-gateway",
            "task.rs",
            r#"
            pub const PANICS_METRIC: &str = "sprig_gateway_session_panics_total";
            const KEY: &str = "protocol";
            fn f() { metrics::counter!(PANICS_METRIC, KEY => p.as_str()).increment(1); }
            fn g() { gauge!(self::LATER, "k" => 1).set(1.0); }
            pub(crate) static LATER: &'static str = "sprig_later";
            "#,
        );
        let scan = s.finish();
        assert!(scan.unresolved.is_empty(), "{:?}", scan.unresolved);
        let m = &scan.metrics;
        assert_eq!(m[0].name, "sprig_gateway_session_panics_total");
        assert_eq!(labels(&m[0]), ["protocol"]);
        assert_eq!(m[1].name, "sprig_later");
    }

    #[test]
    fn names_from_constants_across_crates() {
        let mut s = Scanner::default();
        // Defined in another file of the crate, and used by path.
        s.scan_file(
            "sprig-a",
            "a/defs.rs",
            r#"pub const METRIC: &str = "sprig_a_total";"#,
        );
        s.scan_file(
            "sprig-a",
            "a/use.rs",
            "counter!(defs::METRIC).increment(1);",
        );
        // Another crate's constant of the same name, named by crate path.
        s.scan_file(
            "sprig-b",
            "b/lib.rs",
            r#"pub const METRIC: &str = "sprig_b_seconds";"#,
        );
        s.scan_file(
            "sprig-c",
            "c/lib.rs",
            "histogram!(sprig_b::METRIC).record(1.0); gauge!(ONLY_B).set(1.0);",
        );
        s.scan_file(
            "sprig-b",
            "b/more.rs",
            r#"pub const ONLY_B: &str = "sprig_b_only";"#,
        );
        let scan = s.finish();
        assert!(scan.unresolved.is_empty(), "{:?}", scan.unresolved);
        let got: Vec<_> = scan
            .metrics
            .iter()
            .map(|m| (m.name.as_str(), m.crates.iter().next().map(String::as_str)))
            .collect();
        assert_eq!(
            got,
            [
                ("sprig_a_total", Some("sprig-a")),
                ("sprig_b_only", Some("sprig-c")),
                ("sprig_b_seconds", Some("sprig-c")),
            ]
        );
    }

    #[test]
    fn ambiguous_and_unknown_names_are_reported() {
        let mut s = Scanner::default();
        s.scan_file("sprig-a", "a.rs", r#"pub const M: &str = "sprig_one";"#);
        s.scan_file("sprig-b", "b.rs", r#"pub const M: &str = "sprig_two";"#);
        s.scan_file(
            "sprig-c",
            "c.rs",
            "\n\ncounter!(M).increment(1);\ncounter!(name).increment(1);\n\
             counter!(format!(\"sprig_{x}\")).increment(1);",
        );
        let scan = s.finish();
        assert!(scan.metrics.is_empty());
        let got: Vec<_> = scan
            .unresolved
            .iter()
            .map(|u| {
                (
                    u.file.as_str(),
                    u.line,
                    u.metric.as_deref(),
                    u.expr.as_str(),
                )
            })
            .collect();
        assert_eq!(
            got,
            [
                ("c.rs", 3, None, "M"),
                ("c.rs", 4, None, "name"),
                ("c.rs", 5, None, "format!(\"sprig_{x}\")"),
            ]
        );
    }

    #[test]
    fn labels_from_a_local_array() {
        let mut s = Scanner::default();
        s.scan(
            "sprig-controller",
            r#"
            fn other() { let labels = [("wrong", 1)]; }
            async fn probe(&self) {
                let labels = [
                    ("dc", self.registry.dc().to_string()),
                    ("cluster", id.to_string()),
                ];
                metrics::histogram!("sprig_cluster_probe_duration_seconds", &labels)
                    .record(1.0);
                let health = d.health(&probes, &profile);
                metrics::gauge!("sprig_cluster_health_state", &labels).set(1.0);
            }
            "#,
        );
        let scan = s.finish();
        assert!(scan.unresolved.is_empty(), "{:?}", scan.unresolved);
        for m in &scan.metrics {
            assert_eq!(labels(m), ["cluster", "dc"], "{}", m.name);
        }
    }

    #[test]
    fn labels_from_inline_vec_and_label_new() {
        let mut s = Scanner::default();
        s.scan(
            "sprig-x",
            r#"
            const KEY: &str = "tenant";
            fn f() {
                let mut ls: Vec<Label> = vec![Label::new("a", x), Label::new(KEY, y)];
                counter!("sprig_one_total", &ls).increment(1);
                counter!("sprig_two_total", &[("b", format!("{}, {}", 1, 2))]).increment(1);
            }
            "#,
        );
        let scan = s.finish();
        assert!(scan.unresolved.is_empty(), "{:?}", scan.unresolved);
        assert_eq!(labels(&scan.metrics[0]), ["a", "tenant"]);
        assert_eq!(labels(&scan.metrics[1]), ["b"]);
    }

    #[test]
    fn unread_labels_are_reported() {
        let mut s = Scanner::default();
        s.scan_file(
            "sprig-x",
            "x.rs",
            r#"fn f(labels: Vec<Label>) {
                let other = [("k", 1)];
                gauge!("sprig_param", labels.clone()).set(1.0);
                gauge!("sprig_built", build_labels()).set(1.0);
                gauge!("sprig_var_key", key => 1).set(1.0);
            }"#,
        );
        let scan = s.finish();
        assert_eq!(scan.metrics.len(), 3);
        assert!(scan.metrics.iter().all(|m| m.labels.is_empty()));
        let got: Vec<_> = scan
            .unresolved
            .iter()
            .map(|u| (u.line, u.metric.as_deref(), u.expr.as_str()))
            .collect();
        assert_eq!(
            got,
            [
                (3, Some("sprig_param"), "labels.clone()"),
                (4, Some("sprig_built"), "build_labels()"),
                (5, Some("sprig_var_key"), "key"),
            ]
        );
    }
}
