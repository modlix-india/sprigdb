//! Highlights.
//!
//! Text is never stored in the index: the gateway highlights the text the
//! lookup fetched from MariaDB. The field's analyzer runs over it, tokens
//! matching a query term (or an expansion of one) are marked, and the
//! fragment of about `chars` characters with the most distinct hits is
//! returned with the hits' byte ranges inside it.

use std::collections::HashSet;

use serde::Serialize;

use crate::analyze::{Analyzer, Analyzers};
use crate::query::SearchQuery;

/// One highlighted fragment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Fragment {
    /// The text.
    pub text: String,
    /// Byte ranges of hits inside `text`.
    pub hits: Vec<(usize, usize)>,
}

/// The best fragment of `text` for `terms` (analyzed), or `None` when no
/// token matches.
#[must_use]
pub fn fragment<S: std::hash::BuildHasher>(
    analyzer: &Analyzer,
    text: &str,
    terms: &HashSet<String, S>,
    chars: usize,
) -> Option<Fragment> {
    let toks: Vec<_> = analyzer
        .index_tokens(text)
        .into_iter()
        .filter(|t| terms.contains(&t.text))
        .collect();
    if toks.is_empty() {
        return None;
    }
    let width = chars.max(20);
    // The window starting at each hit; the one with most distinct terms wins.
    let mut best = (0usize, 0usize);
    for (i, t) in toks.iter().enumerate() {
        let mut seen = HashSet::new();
        for u in &toks[i..] {
            if text[t.from..u.to].chars().count() > width {
                break;
            }
            seen.insert(u.text.as_str());
        }
        if seen.len() > best.1 {
            best = (i, seen.len());
        }
    }
    let start_tok = &toks[best.0];
    // Start a little before the first hit, on a char boundary.
    let mut start = start_tok.from;
    let lead = width / 5;
    let mut back = 0;
    while start > 0 && back < lead {
        start -= 1;
        while !text.is_char_boundary(start) {
            start -= 1;
        }
        back += 1;
    }
    let mut end = start;
    for (n, (i, c)) in text[start..].char_indices().enumerate() {
        if n == width {
            end = start + i;
            break;
        }
        end = start + i + c.len_utf8();
    }
    let hits = toks
        .iter()
        .filter(|t| t.from >= start && t.to <= end)
        .map(|t| (t.from - start, t.to - start))
        .collect();
    Some(Fragment {
        text: text[start..end].to_owned(),
        hits,
    })
}

/// The words a query highlights (Redis's `HIGHLIGHT` and `SUMMARIZE`):
/// its analyzed terms, its prefixes and its fuzzy words. Negated clauses
/// highlight nothing; a term is highlighted in every field, as in Redis.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Marks {
    /// Analyzed terms (stemmed: `shoes` marks `shoe`).
    pub terms: HashSet<String>,
    /// Lowercased prefixes: a word starting with one is marked.
    pub prefixes: Vec<String>,
    /// Lowercased words with their edit distance.
    pub fuzzy: Vec<(String, u8)>,
}

impl Marks {
    /// The marks of `q`, analyzed by every analyzer of the index.
    #[must_use]
    pub fn of(q: &SearchQuery, analyzers: &Analyzers) -> Self {
        let mut m = Self::default();
        m.walk(q, analyzers);
        m
    }

    fn walk(&mut self, q: &SearchQuery, analyzers: &Analyzers) {
        match q {
            SearchQuery::Match {
                text,
                fuzzy: Some(d),
                ..
            } => {
                for w in text.split_whitespace() {
                    self.fuzzy.push((w.to_lowercase(), *d));
                }
            }
            SearchQuery::Match { text, .. } | SearchQuery::Phrase { text, .. } => {
                for a in analyzers.iter() {
                    self.terms
                        .extend(a.query_tokens(text).into_iter().map(|t| t.text));
                }
            }
            SearchQuery::Prefix { text, .. } => {
                let p = text.trim().to_lowercase();
                if !p.is_empty() && !self.prefixes.contains(&p) {
                    self.prefixes.push(p);
                }
            }
            SearchQuery::Bool {
                must,
                should,
                filter,
                ..
            } => {
                for x in must.iter().chain(should).chain(filter) {
                    self.walk(x, analyzers);
                }
            }
            SearchQuery::Boost(x, _) => self.walk(x, analyzers),
            _ => {}
        }
    }

    /// Whether the word `raw` (as written) is highlighted.
    fn hit(&self, analyzer: &Analyzer, raw: &str) -> bool {
        let lower = raw.to_lowercase();
        self.prefixes.iter().any(|p| lower.starts_with(p.as_str()))
            || self
                .fuzzy
                .iter()
                .any(|(w, d)| within(w, &lower, usize::from(*d)))
            || analyzer
                .index_tokens(raw)
                .iter()
                .any(|t| self.terms.contains(&t.text))
    }

    /// Whether nothing would be highlighted.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.terms.is_empty() && self.prefixes.is_empty() && self.fuzzy.is_empty()
    }
}

/// Whether `a` and `b` are at most `d` edits apart (Levenshtein).
fn within(a: &str, b: &str, d: usize) -> bool {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    if a.len().abs_diff(b.len()) > d {
        return false;
    }
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.iter().enumerate() {
        let mut cur = vec![i + 1; b.len() + 1];
        for (j, cb) in b.iter().enumerate() {
            cur[j + 1] = (prev[j] + usize::from(ca != cb))
                .min(prev[j + 1] + 1)
                .min(cur[j] + 1);
        }
        prev = cur;
    }
    prev[b.len()] <= d
}

/// The words of `text`: runs of letters, digits and `_`, by byte range.
fn words(text: &str) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let mut start = None;
    for (i, c) in text.char_indices() {
        let w = c.is_alphanumeric() || c == '_';
        match (w, start) {
            (true, None) => start = Some(i),
            (false, Some(s)) => {
                out.push((s, i));
                start = None;
            }
            _ => {}
        }
    }
    if let Some(s) = start {
        out.push((s, text.len()));
    }
    out
}

/// `text[from..to]` with each highlighted word of `hits` (byte ranges)
/// between `open` and `close`.
fn wrap(text: &str, from: usize, to: usize, hits: &[(usize, usize)], tags: (&str, &str)) -> String {
    let mut out = String::with_capacity(to - from + hits.len() * 8);
    let mut at = from;
    for &(a, b) in hits.iter().filter(|(a, b)| *a >= from && *b <= to) {
        out.push_str(&text[at..a]);
        out.push_str(tags.0);
        out.push_str(&text[a..b]);
        out.push_str(tags.1);
        at = b;
    }
    out.push_str(&text[at..to]);
    out
}

/// `text` with every word `marks` highlights between `open` and `close`
/// (Redis's `HIGHLIGHT`).
#[must_use]
pub fn mark(analyzer: &Analyzer, text: &str, marks: &Marks, open: &str, close: &str) -> String {
    let hits: Vec<(usize, usize)> = words(text)
        .into_iter()
        .filter(|&(a, b)| marks.hit(analyzer, &text[a..b]))
        .collect();
    wrap(text, 0, text.len(), &hits, (open, close))
}

/// Redis's `SUMMARIZE`: up to `frags` fragments of `len` words around the
/// highlighted words, the ones with most distinct words first, in text
/// order, each followed by `separator`; words inside highlighted when
/// `tags` is given. A field with no highlighted word keeps its first
/// `len` words.
#[must_use]
pub fn summarize(
    analyzer: &Analyzer,
    text: &str,
    marks: &Marks,
    frags: usize,
    len: usize,
    separator: &str,
    tags: Option<(&str, &str)>,
) -> String {
    let ws = words(text);
    let len = len.max(1);
    let hit: Vec<bool> = ws
        .iter()
        .map(|&(a, b)| marks.hit(analyzer, &text[a..b]))
        .collect();
    let hits: Vec<(usize, usize)> = ws
        .iter()
        .zip(&hit)
        .filter(|(_, h)| **h)
        .map(|(w, _)| *w)
        .collect();
    let piece = |s: usize, e: usize| -> String {
        let (from, to) = (ws[s].0, ws[e - 1].1);
        match tags {
            Some(t) => wrap(text, from, to, &hits, t),
            None => text[from..to].to_owned(),
        }
    };
    if hits.is_empty() {
        if ws.len() <= len {
            return text.to_owned();
        }
        return format!("{}{separator}", piece(0, len));
    }
    // A window of `len` words around each highlighted word; overlapping
    // windows join.
    let mut windows: Vec<(usize, usize)> = Vec::new();
    for (i, _) in hit.iter().enumerate().filter(|(_, h)| **h) {
        let s = i.saturating_sub(len / 2);
        let e = (s + len).min(ws.len());
        let s = e.saturating_sub(len).min(s);
        match windows.last_mut() {
            Some(last) if s <= last.1 => last.1 = last.1.max(e),
            _ => windows.push((s, e)),
        }
    }
    let distinct = |&(s, e): &(usize, usize)| -> usize {
        let mut seen = HashSet::new();
        for i in s..e {
            if hit[i] {
                seen.insert(text[ws[i].0..ws[i].1].to_lowercase());
            }
        }
        seen.len()
    };
    let mut ranked: Vec<(usize, usize)> = windows.clone();
    ranked.sort_by(|x, y| distinct(y).cmp(&distinct(x)).then(x.0.cmp(&y.0)));
    ranked.truncate(frags.max(1));
    ranked.sort_unstable();
    let mut out = String::new();
    for (s, e) in ranked {
        out.push_str(&piece(s, e));
        out.push_str(separator);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analyze::builtin;

    #[test]
    fn marks_stemmed_hits() {
        let a = builtin("english").unwrap();
        let terms: HashSet<String> = ["run".to_owned()].into();
        let f = fragment(&a, "Shoes for Running and more running", &terms, 150).unwrap();
        assert_eq!(f.hits.len(), 2);
        assert_eq!(&f.text[f.hits[0].0..f.hits[0].1], "Running");
        assert!(fragment(&a, "nothing here", &terms, 150).is_none());
    }

    fn marks_of(q: &SearchQuery) -> Marks {
        let an = Analyzers::resolve(["english"], &[]).unwrap();
        Marks::of(q, &an)
    }

    fn word(text: &str) -> SearchQuery {
        SearchQuery::Match {
            fields: Vec::new(),
            text: text.into(),
            all: false,
            fuzzy: None,
        }
    }

    #[test]
    fn marks_words_as_redis_highlights_them() {
        let a = builtin("english").unwrap();
        let q = SearchQuery::Bool {
            must: vec![word("running fox")],
            should: Vec::new(),
            must_not: vec![word("dogs")],
            filter: Vec::new(),
            min_should: 0,
        };
        let m = marks_of(&q);
        assert_eq!(
            mark(
                &a,
                "The quick brown foxes jumped over lazy dogs running",
                &m,
                "<b>",
                "</b>"
            ),
            "The quick brown <b>foxes</b> jumped over lazy dogs <b>running</b>"
        );
        assert_eq!(
            mark(
                &a,
                "Shoes, shoes: SHOES!",
                &marks_of(&word("shoe")),
                "[",
                "]"
            ),
            "[Shoes], [shoes]: [SHOES]!"
        );
        let p = marks_of(&SearchQuery::Prefix {
            fields: Vec::new(),
            text: "Run".into(),
        });
        assert_eq!(
            mark(&a, "Running runners ran", &p, "<", ">"),
            "<Running> <runners> ran"
        );
        let f = marks_of(&SearchQuery::Match {
            fields: Vec::new(),
            text: "runing".into(),
            all: false,
            fuzzy: Some(1),
        });
        assert_eq!(
            mark(&a, "running run ruining", &f, "<", ">"),
            "<running> run <ruining>"
        );
        assert_eq!(
            mark(&a, "nothing", &marks_of(&word("shoe")), "<", ">"),
            "nothing"
        );
        assert!(marks_of(&SearchQuery::All).is_empty());
    }

    #[test]
    fn summarizes_around_the_highlighted_words() {
        let a = builtin("english").unwrap();
        let m = marks_of(&word("red"));
        let text =
            "one two three four red five six seven eight nine ten eleven twelve red thirteen";
        assert_eq!(
            summarize(&a, text, &m, 2, 4, " | ", Some(("<b>", "</b>"))),
            "three four <b>red</b> five | eleven twelve <b>red</b> thirteen | "
        );
        // One fragment: the first, as both hold one distinct word.
        assert_eq!(
            summarize(&a, text, &m, 1, 4, "...", None),
            "three four red five..."
        );
        // Close words share a fragment.
        assert_eq!(
            summarize(&a, "a red b red c d e f g", &m, 3, 6, "|", None),
            "a red b red c d|"
        );
        // No highlighted word: the first words.
        assert_eq!(
            summarize(&a, "alpha beta gamma", &m, 3, 2, "...", None),
            "alpha beta..."
        );
        assert_eq!(
            summarize(&a, "alpha beta", &m, 3, 2, "...", None),
            "alpha beta"
        );
    }
}
