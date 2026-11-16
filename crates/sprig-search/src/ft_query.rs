//! Redis Search's query language (`FT.SEARCH`, `FT.AGGREGATE`,
//! `FT.EXPLAIN`) as the search AST.
//!
//! ```text
//! query  := inter ('|' inter)*                  union binds loosest
//! inter  := item+                              every item required
//! item   := '-' item | '~' item | atom         not / optional
//! atom   := '(' query ')' | '@f[|g]:' fatom | word | "phrase" | '*'
//! fatom  := atom | '{' tag ('|' tag)* '}' | '[' lo hi ']'
//! word   := text | prefix* | %fuzzy% | %%fuzzy%% | $param
//! ```
//!
//! Dialect 1 (Redis's default) differs as Redis Search's does: `-x y z`
//! negates the rest of the intersection, `~x y z` makes the rest optional,
//! and `@f:x y` applies the field to the plain words after it too. From
//! dialect 2 each takes one atom.
//!
//! A bare word searches every `TEXT` field (or `INFIELDS`) and must match
//! all its tokens; a word that is all stop words is left out of its
//! intersection, as Redis Search drops it. Tags are compared lowercased
//! unless the field is `CASESENSITIVE`. Numeric bounds take `(` for
//! exclusive and `-inf`, `+inf`, `inf`.

use std::ops::Bound;

use sprig_common::{Result, SprigCode, SprigError};

use crate::def::{FieldKind, IndexDef};
use crate::query::{Lit, SearchQuery};

fn syntax(msg: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::SearchQuerySyntax, msg)
}

#[derive(Debug, Clone, PartialEq)]
enum Tk {
    Word(String),
    Phrase(String),
    Field(Vec<String>),
    Tags(Vec<String>),
    Range(String, String),
    Minus,
    Tilde,
    Pipe,
    Open,
    Close,
    Star,
}

const SPECIAL: &str = "()|{}[]@\"~";

fn lex(q: &str, params: &[(String, String)]) -> Result<Vec<Tk>> {
    let c: Vec<char> = q.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    // Whether the previous character started a new item (for `-` and `~`).
    let at_start = |out: &Vec<Tk>, c: &[char], i: usize| {
        i == 0
            || c[i - 1].is_whitespace()
            || matches!(out.last(), Some(Tk::Open | Tk::Pipe | Tk::Field(_)))
    };
    while i < c.len() {
        let ch = c[i];
        if ch.is_whitespace() {
            i += 1;
            continue;
        }
        match ch {
            '(' => {
                out.push(Tk::Open);
                i += 1;
            }
            ')' => {
                out.push(Tk::Close);
                i += 1;
            }
            '|' => {
                out.push(Tk::Pipe);
                i += 1;
            }
            '-' if at_start(&out, &c, i) && c.get(i + 1).is_some_and(|n| !n.is_whitespace()) => {
                out.push(Tk::Minus);
                i += 1;
            }
            '~' => {
                out.push(Tk::Tilde);
                i += 1;
            }
            '"' => {
                let mut s = String::new();
                i += 1;
                while i < c.len() && c[i] != '"' {
                    if c[i] == '\\' && i + 1 < c.len() {
                        i += 1;
                    }
                    s.push(c[i]);
                    i += 1;
                }
                if i >= c.len() {
                    return Err(syntax("Syntax error: unterminated quote"));
                }
                i += 1;
                out.push(Tk::Phrase(s));
            }
            '@' => {
                let mut names = vec![String::new()];
                i += 1;
                while i < c.len() && c[i] != ':' {
                    match c[i] {
                        '|' => names.push(String::new()),
                        '\\' if i + 1 < c.len() => {
                            i += 1;
                            if let Some(n) = names.last_mut() {
                                n.push(c[i]);
                            }
                        }
                        x if x.is_whitespace() => {
                            return Err(syntax("Syntax error: expected ':' after the field"));
                        }
                        x => {
                            if let Some(n) = names.last_mut() {
                                n.push(x);
                            }
                        }
                    }
                    i += 1;
                }
                if i >= c.len() || names.iter().any(String::is_empty) {
                    return Err(syntax("Syntax error: expected '@field:'"));
                }
                i += 1;
                out.push(Tk::Field(names));
            }
            '{' => {
                let mut tags = vec![String::new()];
                i += 1;
                while i < c.len() && c[i] != '}' {
                    match c[i] {
                        '|' => tags.push(String::new()),
                        '\\' if i + 1 < c.len() => {
                            i += 1;
                            if let Some(t) = tags.last_mut() {
                                t.push(c[i]);
                            }
                        }
                        x => {
                            if let Some(t) = tags.last_mut() {
                                t.push(x);
                            }
                        }
                    }
                    i += 1;
                }
                if i >= c.len() {
                    return Err(syntax("Syntax error: unterminated '{'"));
                }
                i += 1;
                let tags: Vec<String> = tags
                    .into_iter()
                    .map(|t| param_of(t.trim(), params))
                    .collect::<Result<_>>()?;
                out.push(Tk::Tags(tags));
            }
            '[' => {
                let mut s = String::new();
                i += 1;
                while i < c.len() && c[i] != ']' {
                    s.push(c[i]);
                    i += 1;
                }
                if i >= c.len() {
                    return Err(syntax("Syntax error: unterminated '['"));
                }
                i += 1;
                let parts: Vec<&str> = s.split([' ', ',']).filter(|p| !p.is_empty()).collect();
                if parts.len() != 2 {
                    return Err(syntax("Syntax error: a numeric range needs two bounds"));
                }
                out.push(Tk::Range(
                    param_of(parts[0], params)?,
                    param_of(parts[1], params)?,
                ));
            }
            _ => {
                let mut s = String::new();
                while i < c.len() && !c[i].is_whitespace() && !SPECIAL.contains(c[i]) {
                    if c[i] == '\\' && i + 1 < c.len() {
                        i += 1;
                    }
                    s.push(c[i]);
                    i += 1;
                }
                if s.is_empty() {
                    return Err(syntax(format!("Syntax error at '{ch}'")));
                }
                if s == "*" {
                    out.push(Tk::Star);
                } else {
                    out.push(Tk::Word(param_of(&s, params)?));
                }
            }
        }
    }
    Ok(out)
}

/// `$name` from `PARAMS`, else the text.
fn param_of(s: &str, params: &[(String, String)]) -> Result<String> {
    if let Some(name) = s.strip_prefix('$') {
        if name.is_empty() {
            return Ok(s.to_owned());
        }
        return params
            .iter()
            .find(|(k, _)| k == name)
            .map(|(_, v)| v.clone())
            .ok_or_else(|| syntax(format!("No such parameter `{name}`")));
    }
    Ok(s.to_owned())
}

/// What a query is parsed against.
pub struct FtContext<'a> {
    /// The index.
    pub def: &'a IndexDef,
    /// `DIALECT`.
    pub dialect: u32,
    /// Fields bare words search (`INFIELDS`); empty: every text field.
    pub infields: Vec<String>,
    /// `SLOP` for phrases.
    pub slop: u32,
    /// Whether a word is only stop words (left out of intersections).
    pub stopword: &'a dyn Fn(&str) -> bool,
}

impl std::fmt::Debug for FtContext<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FtContext")
            .field("index", &self.def.name)
            .field("dialect", &self.dialect)
            .finish_non_exhaustive()
    }
}

/// One item of an intersection.
enum Item {
    Must(SearchQuery),
    Not(SearchQuery),
    Optional(SearchQuery),
    /// A stop word: nothing.
    Skip,
}

struct P<'a, 'c> {
    t: Vec<Tk>,
    i: usize,
    cx: &'c FtContext<'a>,
}

fn bool_of(items: Vec<Item>) -> SearchQuery {
    let mut must = Vec::new();
    let mut must_not = Vec::new();
    let mut should = Vec::new();
    for it in items {
        match it {
            Item::Must(q) => must.push(q),
            Item::Not(q) => must_not.push(q),
            Item::Optional(q) => should.push(q),
            Item::Skip => {}
        }
    }
    if must.is_empty() && should.is_empty() && must_not.is_empty() {
        // Only stop words: Redis Search matches nothing.
        return SearchQuery::Bool {
            must: Vec::new(),
            should: Vec::new(),
            must_not: Vec::new(),
            filter: Vec::new(),
            min_should: 1,
        };
    }
    if must.len() == 1 && must_not.is_empty() && should.is_empty() {
        return must.remove(0);
    }
    if must.is_empty() {
        must.push(SearchQuery::All);
    }
    SearchQuery::Bool {
        must,
        should,
        must_not,
        filter: Vec::new(),
        min_should: 0,
    }
}

impl P<'_, '_> {
    fn peek(&self) -> Option<&Tk> {
        self.t.get(self.i)
    }

    fn union(&mut self) -> Result<SearchQuery> {
        let mut alts = vec![self.inter()?];
        while self.peek() == Some(&Tk::Pipe) {
            self.i += 1;
            alts.push(self.inter()?);
        }
        Ok(if alts.len() == 1 {
            alts.remove(0)
        } else {
            SearchQuery::Bool {
                must: Vec::new(),
                should: alts,
                must_not: Vec::new(),
                filter: Vec::new(),
                min_should: 1,
            }
        })
    }

    fn at_end_of_inter(&self) -> bool {
        matches!(self.peek(), None | Some(Tk::Pipe | Tk::Close))
    }

    fn inter(&mut self) -> Result<SearchQuery> {
        let mut items = Vec::new();
        if self.at_end_of_inter() {
            return Err(syntax("Syntax error: empty expression"));
        }
        while !self.at_end_of_inter() {
            items.push(self.item()?);
        }
        Ok(bool_of(items))
    }

    fn item(&mut self) -> Result<Item> {
        match self.peek() {
            Some(Tk::Minus) => {
                self.i += 1;
                if self.cx.dialect == 1 {
                    // The rest of the intersection.
                    let rest = self.inter()?;
                    return Ok(Item::Not(rest));
                }
                Ok(match self.item()? {
                    Item::Must(q) | Item::Optional(q) => Item::Not(q),
                    Item::Not(q) => Item::Must(q),
                    Item::Skip => Item::Skip,
                })
            }
            Some(Tk::Tilde) => {
                self.i += 1;
                if self.cx.dialect == 1 {
                    // The rest of the intersection, as `-`.
                    let rest = self.inter()?;
                    return Ok(Item::Optional(rest));
                }
                Ok(match self.item()? {
                    Item::Must(q) => Item::Optional(q),
                    other => other,
                })
            }
            _ => self.atom(None),
        }
    }

    /// One atom, searching `fields` (a field modifier) or the defaults.
    fn atom(&mut self, fields: Option<&[String]>) -> Result<Item> {
        let tk = self
            .t
            .get(self.i)
            .cloned()
            .ok_or_else(|| syntax("Syntax error: unexpected end of query"))?;
        self.i += 1;
        Ok(match tk {
            Tk::Open => {
                let q = self.scoped_union(fields)?;
                if self.peek() != Some(&Tk::Close) {
                    return Err(syntax("Syntax error: expected ')'"));
                }
                self.i += 1;
                Item::Must(q)
            }
            Tk::Star => Item::Must(SearchQuery::All),
            Tk::Word(w) => self.word(&w, fields),
            Tk::Phrase(p) => {
                if (self.cx.stopword)(&p) {
                    Item::Skip
                } else {
                    Item::Must(SearchQuery::Phrase {
                        fields: self.text_fields(fields),
                        text: p,
                        slop: self.cx.slop,
                    })
                }
            }
            Tk::Field(names) => self.field_atom(&names)?,
            Tk::Tags(_) | Tk::Range(..) => {
                return Err(syntax("Syntax error: tags and ranges need '@field:'"));
            }
            Tk::Minus | Tk::Tilde | Tk::Pipe | Tk::Close => {
                return Err(syntax("Syntax error: unexpected operator"));
            }
        })
    }

    /// A parenthesized union whose plain words search `fields`.
    fn scoped_union(&mut self, fields: Option<&[String]>) -> Result<SearchQuery> {
        let Some(f) = fields else {
            return self.union();
        };
        // Words inside take the field: rewrite them as field atoms.
        let mut depth = 0usize;
        let mut j = self.i;
        let mut out: Vec<Tk> = Vec::new();
        while j < self.t.len() {
            match &self.t[j] {
                Tk::Open => depth += 1,
                Tk::Close if depth == 0 => break,
                Tk::Close => depth -= 1,
                _ => {}
            }
            match &self.t[j] {
                w @ (Tk::Word(_) | Tk::Phrase(_)) if !matches!(out.last(), Some(Tk::Field(_))) => {
                    out.push(Tk::Field(f.to_vec()));
                    out.push(w.clone());
                }
                other => out.push(other.clone()),
            }
            j += 1;
        }
        let mut inner = P {
            t: out,
            i: 0,
            cx: self.cx,
        };
        let q = inner.union()?;
        if inner.i != inner.t.len() {
            return Err(syntax("Syntax error: unbalanced parentheses"));
        }
        self.i = j;
        Ok(q)
    }

    fn text_fields(&self, fields: Option<&[String]>) -> Vec<String> {
        match fields {
            Some(f) => f.to_vec(),
            None => self.cx.infields.clone(),
        }
    }

    fn word(&self, w: &str, fields: Option<&[String]>) -> Item {
        let fs = self.text_fields(fields);
        let fuzzy = |s: &str| -> Option<(String, u8)> {
            for n in [3u8, 2, 1] {
                let m = "%".repeat(usize::from(n));
                if let Some(x) = s.strip_prefix(&m).and_then(|x| x.strip_suffix(&m))
                    && !x.is_empty()
                    && !x.contains('%')
                {
                    return Some((x.to_owned(), n.min(2)));
                }
            }
            None
        };
        if let Some((t, d)) = fuzzy(w) {
            return Item::Must(SearchQuery::Match {
                fields: fs,
                text: t,
                all: true,
                fuzzy: Some(d),
            });
        }
        if let Some(p) = w.strip_suffix('*')
            && !p.is_empty()
        {
            return Item::Must(SearchQuery::Prefix {
                fields: fs,
                text: p.to_owned(),
            });
        }
        if (self.cx.stopword)(w) {
            return Item::Skip;
        }
        Item::Must(SearchQuery::Match {
            fields: fs,
            text: w.to_owned(),
            all: true,
            fuzzy: None,
        })
    }

    fn kind_of(&self, name: &str) -> Option<FieldKind> {
        let fs = self.cx.def.fields_named(name);
        if fs.iter().any(|f| f.kind == FieldKind::Number) {
            Some(FieldKind::Number)
        } else if fs.iter().any(|f| f.kind.is_text()) {
            Some(FieldKind::Text)
        } else if fs.iter().any(|f| f.kind == FieldKind::Keyword) {
            Some(FieldKind::Keyword)
        } else {
            fs.first().map(|f| f.kind)
        }
    }

    fn field_atom(&mut self, names: &[String]) -> Result<Item> {
        match self.peek().cloned() {
            Some(Tk::Tags(tags)) => {
                self.i += 1;
                let mut alts = Vec::new();
                for n in names {
                    let f = self
                        .cx
                        .def
                        .fields_named(n)
                        .into_iter()
                        .find(|f| f.kind == FieldKind::Keyword);
                    for t in &tags {
                        if t.is_empty() {
                            continue;
                        }
                        let (value, prefix) = match t.strip_suffix('*') {
                            Some(p) if !p.is_empty() => (p, true),
                            _ => (t.as_str(), false),
                        };
                        let value = f.map_or_else(|| value.to_owned(), |f| f.tag_case(value));
                        alts.push(if prefix {
                            SearchQuery::Prefix {
                                fields: vec![n.clone()],
                                text: value,
                            }
                        } else {
                            SearchQuery::Equals {
                                field: n.clone(),
                                value: Lit::Str(value),
                            }
                        });
                    }
                }
                Ok(Item::Must(if alts.len() == 1 {
                    alts.remove(0)
                } else {
                    SearchQuery::any(alts)
                }))
            }
            Some(Tk::Range(lo, hi)) => {
                self.i += 1;
                let lo = bound(&lo, true)?;
                let hi = bound(&hi, false)?;
                let alts: Vec<SearchQuery> = names
                    .iter()
                    .map(|n| SearchQuery::Range {
                        field: n.clone(),
                        lo: lo.clone(),
                        hi: hi.clone(),
                    })
                    .collect();
                Ok(Item::Must(if alts.len() == 1 {
                    alts.into_iter().next().unwrap_or(SearchQuery::All)
                } else {
                    SearchQuery::any(alts)
                }))
            }
            Some(Tk::Minus) if self.cx.dialect > 1 => {
                self.i += 1;
                Ok(match self.field_atom(names)? {
                    Item::Must(q) => Item::Not(q),
                    other => other,
                })
            }
            _ => {
                for n in names {
                    if matches!(
                        self.kind_of(n),
                        Some(FieldKind::Number | FieldKind::Keyword)
                    ) {
                        return Err(syntax(format!(
                            "Syntax error: field `{n}` needs a {} query",
                            if self.kind_of(n) == Some(FieldKind::Number) {
                                "[range]"
                            } else {
                                "{tag}"
                            }
                        )));
                    }
                }
                let word = matches!(self.peek(), Some(Tk::Word(_)));
                let first = self.atom(Some(names))?;
                if self.cx.dialect > 1 || !word {
                    return Ok(first);
                }
                // Dialect 1: the plain words after it take the field too.
                let mut items = vec![first];
                while let Some(Tk::Word(_)) = self.peek() {
                    items.push(self.atom(Some(names))?);
                }
                Ok(if items.len() == 1 {
                    items.remove(0)
                } else {
                    Item::Must(bool_of(items))
                })
            }
        }
    }
}

/// A numeric bound: `(` for exclusive, `inf`, `-inf`, `+inf`.
fn bound(s: &str, low: bool) -> Result<Bound<Lit>> {
    let (excl, v) = match s.strip_prefix('(') {
        Some(v) => (true, v),
        None => (false, s),
    };
    let lv = v.to_ascii_lowercase();
    if lv == "inf" || lv == "+inf" || lv == "-inf" {
        if (lv == "-inf") != low {
            return Err(syntax(format!("Syntax error: bad range bound {s}")));
        }
        return Ok(Bound::Unbounded);
    }
    let n: f64 = v
        .parse()
        .map_err(|_| syntax(format!("Syntax error: bad number {s}")))?;
    Ok(if excl {
        Bound::Excluded(Lit::Num(n))
    } else {
        Bound::Included(Lit::Num(n))
    })
}

/// A `FILTER field min max` option as a range.
///
/// # Errors
/// 39822 for bounds that aren't numbers.
pub fn filter_range(field: &str, lo: &str, hi: &str) -> Result<SearchQuery> {
    let lo = bound(lo, true)?;
    let hi = bound(hi, false)?;
    if matches!((&lo, &hi), (Bound::Unbounded, Bound::Unbounded)) {
        return Ok(SearchQuery::Exists(field.to_owned()));
    }
    Ok(SearchQuery::Range {
        field: field.to_owned(),
        lo,
        hi,
    })
}

/// Parses a Redis Search query.
///
/// # Errors
/// 39822 (`Syntax error ...`).
pub fn parse(q: &str, params: &[(String, String)], cx: &FtContext<'_>) -> Result<SearchQuery> {
    let t = lex(q, params)?;
    if t.is_empty() {
        return Err(syntax("Syntax error: empty query"));
    }
    let mut p = P { t, i: 0, cx };
    let q = p.union()?;
    if p.i != p.t.len() {
        return Err(syntax("Syntax error: unexpected ')'"));
    }
    Ok(q)
}

/// The query as an indented tree (`FT.EXPLAIN`; SprigDB's own form).
#[must_use]
pub fn explain(q: &SearchQuery) -> String {
    fn fields(f: &[String]) -> String {
        if f.is_empty() {
            String::new()
        } else {
            format!("@{}:", f.join("|"))
        }
    }
    fn walk(q: &SearchQuery, depth: usize, out: &mut String) {
        let pad = "  ".repeat(depth);
        let line = |out: &mut String, s: &str| {
            out.push_str(&pad);
            out.push_str(s);
            out.push('\n');
        };
        match q {
            SearchQuery::All => line(out, "<WILDCARD>"),
            SearchQuery::Match {
                fields: f,
                text,
                fuzzy,
                ..
            } => match fuzzy {
                Some(d) => line(out, &format!("{}FUZZY{{{text}}}~{d}", fields(f))),
                None => line(out, &format!("{}{text}", fields(f))),
            },
            SearchQuery::Phrase {
                fields: f, text, ..
            } => line(out, &format!("{}EXACT {{{text}}}", fields(f))),
            SearchQuery::Prefix { fields: f, text } => {
                line(out, &format!("{}PREFIX{{{text}*}}", fields(f)));
            }
            SearchQuery::Wildcard { fields: f, pattern }
            | SearchQuery::Regex { fields: f, pattern } => {
                line(out, &format!("{}WILDCARD{{{pattern}}}", fields(f)));
            }
            SearchQuery::Exists(f) => line(out, &format!("EXISTS {{@{f}}}")),
            SearchQuery::Range { field, lo, hi } => {
                let b = |x: &Bound<Lit>, low: bool| match x {
                    Bound::Included(Lit::Num(n)) => format!("{n} <="),
                    Bound::Excluded(Lit::Num(n)) => format!("{n} <"),
                    _ if low => "-inf <=".to_owned(),
                    _ => "+inf".to_owned(),
                };
                let hi_s = match hi {
                    Bound::Included(Lit::Num(n)) => format!("<= {n}"),
                    Bound::Excluded(Lit::Num(n)) => format!("< {n}"),
                    _ => "<= +inf".to_owned(),
                };
                line(out, &format!("NUMERIC {{{} @{field} {hi_s}}}", b(lo, true)));
            }
            SearchQuery::Equals { field, value } => {
                let v = match value {
                    Lit::Str(s) => s.clone(),
                    Lit::Num(n) => n.to_string(),
                    Lit::Bool(b) => b.to_string(),
                    Lit::DateMs(d) => d.to_string(),
                };
                line(out, &format!("TAG:@{field} {{{v}}}"));
            }
            SearchQuery::Bool {
                must,
                should,
                must_not,
                filter,
                min_should,
            } => {
                let union = must.is_empty() && *min_should >= 1;
                if union {
                    line(out, "UNION {");
                    for x in should {
                        walk(x, depth + 1, out);
                    }
                } else {
                    line(out, "INTERSECT {");
                    for x in must.iter().chain(filter) {
                        if !matches!(x, SearchQuery::All) || must_not.is_empty() {
                            walk(x, depth + 1, out);
                        }
                    }
                    for x in must_not {
                        out.push_str(&pad);
                        out.push_str("  NOT{\n");
                        walk(x, depth + 2, out);
                        out.push_str(&pad);
                        out.push_str("  }\n");
                    }
                    for x in should {
                        out.push_str(&pad);
                        out.push_str("  OPTIONAL{\n");
                        walk(x, depth + 2, out);
                        out.push_str(&pad);
                        out.push_str("  }\n");
                    }
                }
                line(out, "}");
            }
            SearchQuery::Boost(x, _) => walk(x, depth, out),
            SearchQuery::Keys(k) => line(out, &format!("KEYS {{{} keys}}", k.len())),
        }
    }
    let mut out = String::new();
    walk(q, 0, &mut out);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::def::{FieldDef, Flavor, Model, Source};

    fn def() -> IndexDef {
        let mut d = IndexDef::new("idx", "redis", "db0", Model::Kv, Flavor::Sql);
        d.push(FieldDef::new("title", Source::KvField, FieldKind::Text))
            .unwrap();
        d.push(FieldDef::new("body", Source::KvField, FieldKind::Text))
            .unwrap();
        d.push(FieldDef::new("price", Source::KvField, FieldKind::Number))
            .unwrap();
        let mut tags = FieldDef::new("tags", Source::KvField, FieldKind::Keyword);
        tags.separator = Some(",".into());
        d.push(tags).unwrap();
        d
    }

    fn stop(w: &str) -> bool {
        matches!(w.to_ascii_lowercase().as_str(), "a" | "the" | "is")
    }

    fn p(q: &str, dialect: u32) -> SearchQuery {
        let d = def();
        let cx = FtContext {
            def: &d,
            dialect,
            infields: Vec::new(),
            slop: 0,
            stopword: &stop,
        };
        parse(q, &[("x".into(), "hello".into())], &cx).unwrap()
    }

    fn m(f: &[&str], t: &str) -> SearchQuery {
        SearchQuery::Match {
            fields: f.iter().map(|x| (*x).to_owned()).collect(),
            text: t.into(),
            all: true,
            fuzzy: None,
        }
    }

    fn and(must: Vec<SearchQuery>, must_not: Vec<SearchQuery>) -> SearchQuery {
        SearchQuery::Bool {
            must,
            should: vec![],
            must_not,
            filter: vec![],
            min_should: 0,
        }
    }

    fn or(should: Vec<SearchQuery>) -> SearchQuery {
        SearchQuery::Bool {
            must: vec![],
            should,
            must_not: vec![],
            filter: vec![],
            min_should: 1,
        }
    }

    #[test]
    fn intersections_bind_tighter_than_unions() {
        assert_eq!(
            p("hello world|foo", 1),
            or(vec![
                and(vec![m(&[], "hello"), m(&[], "world")], vec![]),
                m(&[], "foo")
            ])
        );
        // A stop word drops out of its intersection.
        assert_eq!(p("the hello", 2), m(&[], "hello"));
        assert_eq!(p("$x", 2), m(&[], "hello"));
    }

    #[test]
    fn dialects_scope_fields_and_negation() {
        let q1 = p("@title:hello world -fox @price:[10 (20]", 1);
        let range = SearchQuery::Range {
            field: "price".into(),
            lo: Bound::Included(Lit::Num(10.0)),
            hi: Bound::Excluded(Lit::Num(20.0)),
        };
        assert_eq!(
            q1,
            and(
                vec![and(
                    vec![m(&["title"], "hello"), m(&["title"], "world")],
                    vec![]
                )],
                vec![and(vec![m(&[], "fox"), range.clone()], vec![])]
            )
        );
        let q2 = p("@title:hello world -fox @price:[10 (20]", 2);
        assert_eq!(
            q2,
            and(
                vec![m(&["title"], "hello"), m(&[], "world"), range],
                vec![m(&[], "fox")]
            )
        );
    }

    #[test]
    fn tags_prefixes_fuzzy_phrases() {
        assert_eq!(
            p("@tags:{Red | blue*}", 2),
            SearchQuery::any(vec![
                SearchQuery::Equals {
                    field: "tags".into(),
                    value: Lit::Str("red".into())
                },
                SearchQuery::Prefix {
                    fields: vec!["tags".into()],
                    text: "blue".into()
                }
            ])
        );
        assert_eq!(
            p("hel*", 2),
            SearchQuery::Prefix {
                fields: vec![],
                text: "hel".into()
            }
        );
        assert_eq!(
            p("%fux%", 2),
            SearchQuery::Match {
                fields: vec![],
                text: "fux".into(),
                all: true,
                fuzzy: Some(1)
            }
        );
        assert_eq!(
            p("\"quick brown\"", 2),
            SearchQuery::Phrase {
                fields: vec![],
                text: "quick brown".into(),
                slop: 0
            }
        );
        assert_eq!(p("*", 1), SearchQuery::All);
        assert_eq!(
            p("@title:(hello|bye) world", 1),
            and(
                vec![
                    or(vec![m(&["title"], "hello"), m(&["title"], "bye")]),
                    m(&[], "world")
                ],
                vec![]
            )
        );
    }

    #[test]
    fn errors() {
        let d = def();
        let cx = FtContext {
            def: &d,
            dialect: 2,
            infields: Vec::new(),
            slop: 0,
            stopword: &stop,
        };
        for q in [
            "(hello",
            "@title hello",
            "@price:hello",
            "@price:[1]",
            "\"open",
            "$missing",
            "",
        ] {
            assert!(parse(q, &[], &cx).is_err(), "{q}");
        }
        assert!(explain(&p("hello -world", 2)).contains("NOT{"));
    }
}
