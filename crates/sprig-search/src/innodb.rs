//! SQL `MATCH ... AGAINST` as `InnoDB` serves it: word rules, the query
//! languages of natural-language and boolean mode, and relevance.
//!
//! Everything here was learnt from the answers of MySQL 8.4 and MariaDB
//! 11.8 as black boxes (both agree on all of it); nothing comes from their
//! sources.
//!
//! **Words.** A word is a run of letters, digits and `_`; an apostrophe
//! ends a word (`don't` is `don` and `t`), whatever the manuals say.
//! A word shorter than the minimum token size (3) or longer than the
//! maximum (84), in characters, or one of the stop words, is not *indexed*:
//! it can't be searched for, but it still takes a position, so a phrase
//! with a stop word inside matches only text with a word there. Words
//! compare by the column's collation ([`Fold`]).
//!
//! **Relevance.** A row's score is the sum, over the query's words it
//! holds, of `tf * idf^2`, where `tf` counts the word in the row (every
//! column of the key), `idf = log10(N / (df * r))`, `N` is the rows in
//! the index, `df` the rows holding the word and `r` how often the query
//! names the word. When `df * r` equals `N` the idf is
//! `log10(1.0001)`. A prefix (`w*`) counts as one word whose `df` is the
//! rows holding any word it expands to. Scores are single precision.
//!
//! **Boolean mode.** Each group (the query, or `( ... )`) evaluates its
//! operands: plain words, phrases and groups are optional, and a row
//! holding any of them is in the group's result; `+x` must be held, `-x`
//! must not; `>x` and `<x` are optional and move the row's score by +1 and
//! -1 (once per group, however many such operands it holds); `~x` adds
//! its score to rows the optional operands before it already found, and
//! takes 1 off such a row (once). With a `+` operand the result is the
//! rows holding every `+` operand, with the optional words adding to their
//! score; optional *groups* add their rows to the result even then.
//! `"a b"` is a phrase (consecutive positions); `"a b" @n` a proximity
//! search (the indexed words within a window of `n` positions, in any
//! order). Syntax `InnoDB` rejects gets its error 1064 and message.
//!
//! **Natural-language mode** is the same language with every operator
//! ignored except quotes (phrases, proximity): all words and phrases are
//! optional and no prefix searches are made.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

/// `InnoDB`'s default stop words (`information_schema.INNODB_FT_DEFAULT_STOPWORD`
/// on both servers).
pub const DEFAULT_STOPWORDS: [&str; 35] = [
    "a", "about", "an", "are", "as", "at", "be", "by", "com", "de", "en", "for", "from", "how",
    "i", "in", "is", "it", "la", "of", "on", "or", "that", "the", "this", "to", "und", "was",
    "what", "when", "where", "who", "will", "with", "www",
];

/// `innodb_ft_min_token_size`'s default.
pub const MIN_TOKEN: usize = 3;
/// `innodb_ft_max_token_size`'s default.
pub const MAX_TOKEN: usize = 84;
/// Positions between two columns of one key: no phrase spans columns.
pub const COLUMN_GAP: u32 = 100;
/// The prefix of the term of a word that is not indexed (a stop word, or
/// a word of the wrong length): such terms only serve phrases.
pub const UNINDEXED: char = '_';

/// How words compare: a word's term, the text every equal word shares.
/// Terms must keep order and prefixes (a prefix of a word has a term that
/// is a prefix of the word's term), so that `w*` is a term range.
pub trait Fold: Send + Sync + std::fmt::Debug {
    /// The term of `word`.
    fn term(&self, word: &str) -> String;
}

/// Lower case, for tests and binary-free callers.
#[derive(Debug, Clone, Copy, Default)]
pub struct LowerFold;

impl Fold for LowerFold {
    fn term(&self, word: &str) -> String {
        word.to_lowercase()
    }
}

/// The parser of a `FULLTEXT` index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Parser {
    /// `InnoDB`'s built-in word parser.
    Words,
    /// MySQL's `WITH PARSER ngram`: every run of `n` characters of a
    /// run of non-space characters.
    Ngram(usize),
}

/// The word rules of one index, fixed when it is created.
#[derive(Debug, Clone)]
pub struct Rules {
    /// Shortest indexed word, in characters.
    pub min: usize,
    /// Longest indexed word, in characters.
    pub max: usize,
    /// Stop words (as written; compared by their terms).
    pub stopwords: Vec<String>,
    /// The parser.
    pub parser: Parser,
}

impl Default for Rules {
    fn default() -> Self {
        Self {
            min: MIN_TOKEN,
            max: MAX_TOKEN,
            stopwords: DEFAULT_STOPWORDS.iter().map(|s| (*s).to_owned()).collect(),
            parser: Parser::Words,
        }
    }
}

/// Words to terms under an index's rules.
#[derive(Debug, Clone)]
pub struct Analyzer {
    rules: Rules,
    fold: Arc<dyn Fold>,
    stop: HashSet<String>,
}

/// One word of a text: its term and position.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tok {
    /// The term ([`Analyzer::term`]).
    pub term: String,
    /// The word's position.
    pub pos: u32,
}

fn is_word_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// The byte spans of the words of `text` (the built-in parser's rules).
#[must_use]
pub fn word_spans(text: &str) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    let mut start: Option<usize> = None;
    for (i, c) in text.char_indices() {
        if is_word_char(c) {
            if start.is_none() {
                start = Some(i);
            }
            continue;
        }
        if let Some(s) = start.take() {
            out.push((s, i));
        }
    }
    if let Some(s) = start {
        out.push((s, text.len()));
    }
    out
}

impl Analyzer {
    /// An analyzer of `rules` comparing words with `fold`.
    #[must_use]
    pub fn new(rules: Rules, fold: Arc<dyn Fold>) -> Self {
        let stop = rules.stopwords.iter().map(|w| fold.term(w)).collect();
        Self { rules, fold, stop }
    }

    /// The rules.
    #[must_use]
    pub fn rules(&self) -> &Rules {
        &self.rules
    }

    /// Whether `word` is searchable (its length, not a stop word).
    #[must_use]
    pub fn indexed(&self, word: &str, folded: &str) -> bool {
        if let Parser::Ngram(_) = self.rules.parser {
            return !self.stop.contains(folded);
        }
        let n = word.chars().count();
        n >= self.rules.min && n <= self.rules.max && !self.stop.contains(folded)
    }

    /// The term `word` is indexed and searched under: its folded text, or
    /// [`UNINDEXED`] then the folded text when it isn't searchable.
    #[must_use]
    pub fn term(&self, word: &str) -> (String, bool) {
        let folded = self.fold.term(word);
        if self.indexed(word, &folded) {
            (folded, true)
        } else {
            let mut t = String::with_capacity(folded.len() + 1);
            t.push(UNINDEXED);
            t.push_str(&folded);
            (t, false)
        }
    }

    /// The folded text of a prefix (`w*`): no length or stop word rule.
    #[must_use]
    pub fn prefix_term(&self, word: &str) -> String {
        self.fold.term(word)
    }

    /// The words of `text`, positions from `pos`; returns the position
    /// after the last word.
    pub fn tokens(&self, text: &str, pos: u32, out: &mut Vec<Tok>) -> u32 {
        let mut p = pos;
        match self.rules.parser {
            Parser::Words => {
                for (s, e) in word_spans(text) {
                    let (term, _) = self.term(&text[s..e]);
                    out.push(Tok { term, pos: p });
                    p += 1;
                }
            }
            Parser::Ngram(n) => {
                for run in text.split(|c: char| c.is_whitespace() || !is_ngram_char(c)) {
                    let chars: Vec<(usize, char)> = run.char_indices().collect();
                    if chars.is_empty() {
                        continue;
                    }
                    if chars.len() < n {
                        continue;
                    }
                    for w in 0..=(chars.len() - n) {
                        let s = chars[w].0;
                        let e = chars.get(w + n).map_or(run.len(), |c| c.0);
                        let (term, _) = self.term(&run[s..e]);
                        out.push(Tok { term, pos: p });
                        p += 1;
                    }
                }
            }
        }
        p
    }
}

fn is_ngram_char(c: char) -> bool {
    !c.is_ascii_punctuation()
}

// ---------------------------------------------------------------------------
// Queries.

/// An operand's operator in boolean mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    /// None: optional.
    Opt,
    /// `+`
    Must,
    /// `-`
    Not,
    /// `>`
    Incr,
    /// `<`
    Decr,
    /// `~`
    Negate,
}

/// One word of a phrase.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PhraseWord {
    /// Its term.
    pub term: String,
    /// Whether it is searchable.
    pub indexed: bool,
}

/// A query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Node {
    /// A word; `prefix` for `w*`. A word that isn't searchable never
    /// matches.
    Word {
        /// Its term (a prefix's folded text).
        term: String,
        /// `w*`.
        prefix: bool,
        /// Searchable.
        indexed: bool,
    },
    /// `"a b c"`, optionally `@n`.
    Phrase {
        /// Its words in order.
        words: Vec<PhraseWord>,
        /// `@n`: a window of `n` positions, any order.
        near: Option<u32>,
    },
    /// A group of operands.
    Group(Vec<(Op, Node)>),
}

/// A syntax error `InnoDB` reports as 1064, with its message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyntaxError(pub String);

impl std::fmt::Display for SyntaxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Lex {
    /// A word; `true` when a `*` follows it at once (a prefix).
    Word(String, bool),
    Num(String),
    /// A quoted text (without its quotes); `None` when unclosed.
    Text(String),
    Star,
    At,
    Open,
    Close,
    Prefix(Op),
}

fn describe(t: Option<&Lex>) -> String {
    match t {
        None => "$end".into(),
        Some(Lex::Word(..)) => "FTS_TERM".into(),
        Some(Lex::Num(_)) => "FTS_NUMB".into(),
        Some(Lex::Text(_)) => "FTS_TEXT".into(),
        Some(Lex::Star) => "'*'".into(),
        Some(Lex::At) => "'@'".into(),
        Some(Lex::Open) => "'('".into(),
        Some(Lex::Close) => "')'".into(),
        Some(Lex::Prefix(op)) => format!(
            "'{}'",
            match op {
                Op::Must => '+',
                Op::Not => '-',
                Op::Incr => '>',
                Op::Decr => '<',
                Op::Negate => '~',
                Op::Opt => ' ',
            }
        ),
    }
}

fn lex(q: &str) -> Vec<Lex> {
    let mut out = Vec::new();
    let mut it = q.char_indices().peekable();
    while let Some(&(i, c)) = it.peek() {
        match c {
            '"' => {
                it.next();
                let start = i + 1;
                let mut end = None;
                for (j, d) in it.by_ref() {
                    if d == '"' {
                        end = Some(j);
                        break;
                    }
                }
                match end {
                    Some(e) => out.push(Lex::Text(q[start..e].to_owned())),
                    // An unclosed quote: the rest are words.
                    None => {
                        for (s, e) in word_spans(&q[start..]) {
                            out.push(Lex::Word(q[start + s..start + e].to_owned(), false));
                        }
                    }
                }
            }
            '+' | '-' | '>' | '<' | '~' => {
                it.next();
                out.push(Lex::Prefix(match c {
                    '+' => Op::Must,
                    '-' => Op::Not,
                    '>' => Op::Incr,
                    '<' => Op::Decr,
                    _ => Op::Negate,
                }));
            }
            '(' => {
                it.next();
                out.push(Lex::Open);
            }
            ')' => {
                it.next();
                out.push(Lex::Close);
            }
            '*' => {
                it.next();
                out.push(Lex::Star);
            }
            '@' => {
                it.next();
                out.push(Lex::At);
            }
            c if is_word_char(c) => {
                let mut end = i;
                while let Some(&(j, d)) = it.peek() {
                    if is_word_char(d) {
                        it.next();
                        end = j + d.len_utf8();
                        continue;
                    }
                    // An apostrophe between word characters stays in the
                    // query's token (its words are split later).
                    if d == '\'' {
                        let mut look = it.clone();
                        look.next();
                        if look.peek().is_some_and(|&(_, n)| is_word_char(n)) {
                            it.next();
                            continue;
                        }
                    }
                    break;
                }
                let w = &q[i..end];
                let star = it.peek().is_some_and(|&(_, d)| d == '*');
                if star {
                    it.next();
                }
                if !star && w.chars().all(|c| c.is_ascii_digit()) {
                    out.push(Lex::Num(w.to_owned()));
                } else {
                    out.push(Lex::Word(w.to_owned(), star));
                }
            }
            _ => {
                it.next();
            }
        }
    }
    out
}

struct BoolParser<'a> {
    toks: Vec<Lex>,
    at: usize,
    an: &'a Analyzer,
}

impl BoolParser<'_> {
    fn peek(&self) -> Option<&Lex> {
        self.toks.get(self.at)
    }

    fn unexpected(&self, expecting: Option<&str>) -> SyntaxError {
        let mut m = format!("syntax error, unexpected {}", describe(self.peek()));
        if let Some(e) = expecting {
            m.push_str(", expecting ");
            m.push_str(e);
        }
        SyntaxError(m)
    }

    /// Operands up to `)` (inside a group) or the end.
    fn list(&mut self, nested: bool) -> Result<Vec<(Op, Node)>, SyntaxError> {
        let mut ops = Vec::new();
        loop {
            match self.peek() {
                None => {
                    if nested {
                        return Err(self.unexpected(None));
                    }
                    return Ok(ops);
                }
                Some(Lex::Close) => {
                    if nested {
                        self.at += 1;
                        return Ok(ops);
                    }
                    return Err(self.unexpected(Some("$end")));
                }
                Some(Lex::At) => return Err(self.unexpected(Some("$end"))),
                _ => {}
            }
            let op = match self.peek() {
                Some(Lex::Prefix(op)) => {
                    let op = *op;
                    self.at += 1;
                    op
                }
                _ => Op::Opt,
            };
            let node = match self.peek().cloned() {
                Some(Lex::Open) => {
                    self.at += 1;
                    Node::Group(self.list(true)?)
                }
                Some(Lex::Word(w, star)) => {
                    self.at += 1;
                    word_node(self.an, &w, star)
                }
                Some(Lex::Num(w)) => {
                    self.at += 1;
                    let (term, indexed) = self.an.term(&w);
                    Node::Word {
                        term,
                        prefix: false,
                        indexed,
                    }
                }
                Some(Lex::Text(t)) => {
                    self.at += 1;
                    let near = if self.peek() == Some(&Lex::At) {
                        self.at += 1;
                        match self.peek() {
                            Some(Lex::Num(n)) => {
                                let n = n.parse::<u32>().unwrap_or(u32::MAX);
                                self.at += 1;
                                Some(n)
                            }
                            _ => return Err(self.unexpected(Some("FTS_NUMB"))),
                        }
                    } else {
                        None
                    };
                    phrase(self.an, &t, near)
                }
                Some(Lex::Star) => {
                    // A `*` on its own needs something after it.
                    self.at += 1;
                    if self.peek().is_none() {
                        return Err(self.unexpected(Some("FTS_TERM or FTS_NUMB or '*'")));
                    }
                    continue;
                }
                _ => return Err(self.unexpected(None)),
            };
            ops.push((op, node));
        }
    }
}

/// A query token (`don't`, `o'neil`, `word*`): its words, without the
/// unsearchable ones before the first searchable one (`o'neil` is `neil`);
/// one word is that word, several a phrase. A prefix takes the last word.
fn word_node(an: &Analyzer, raw: &str, prefix: bool) -> Node {
    let spans = word_spans(raw);
    if prefix {
        let last = spans.last().map_or(raw, |&(s, e)| &raw[s..e]);
        let lead = spans.len() > 1;
        return Node::Word {
            term: an.prefix_term(if lead { last } else { raw }),
            prefix: true,
            indexed: true,
        };
    }
    if spans.len() <= 1 {
        let (term, indexed) = an.term(raw);
        return Node::Word {
            term,
            prefix: false,
            indexed,
        };
    }
    let words: Vec<(String, bool)> = spans.iter().map(|&(s, e)| an.term(&raw[s..e])).collect();
    let searchable: Vec<&(String, bool)> = words.iter().filter(|w| w.1).collect();
    match searchable.as_slice() {
        [] => Node::Word {
            term: words[0].0.clone(),
            prefix: false,
            indexed: false,
        },
        [one] => Node::Word {
            term: one.0.clone(),
            prefix: false,
            indexed: true,
        },
        _ => Node::Phrase {
            words: searchable
                .iter()
                .map(|w| PhraseWord {
                    term: w.0.clone(),
                    indexed: true,
                })
                .collect(),
            near: None,
        },
    }
}

fn phrase(an: &Analyzer, text: &str, near: Option<u32>) -> Node {
    let mut words = Vec::new();
    for (s, e) in word_spans(text) {
        let (term, indexed) = an.term(&text[s..e]);
        // Words that can't be searched for, before the first that can,
        // are dropped; later ones keep their place.
        if indexed || !words.is_empty() {
            words.push(PhraseWord { term, indexed });
        }
    }
    Node::Phrase { words, near }
}

/// Parses a boolean-mode query.
///
/// # Errors
/// Syntax `InnoDB` rejects (1064, with its message).
pub fn parse_boolean(q: &str, an: &Analyzer) -> Result<Node, SyntaxError> {
    let mut p = BoolParser {
        toks: lex(q),
        at: 0,
        an,
    };
    Ok(Node::Group(p.list(false)?))
}

/// Parses a natural-language query: every word and phrase optional.
#[must_use]
pub fn parse_natural(q: &str, an: &Analyzer) -> Node {
    let toks = lex(q);
    let mut ops = Vec::new();
    let mut i = 0;
    while i < toks.len() {
        match &toks[i] {
            Lex::Word(w, _) | Lex::Num(w) => ops.push((Op::Opt, word_node(an, w, false))),
            Lex::Text(t) => {
                let mut near = None;
                if toks.get(i + 1) == Some(&Lex::At)
                    && let Some(Lex::Num(n)) = toks.get(i + 2)
                {
                    near = Some(n.parse::<u32>().unwrap_or(u32::MAX));
                    i += 2;
                }
                ops.push((Op::Opt, phrase(an, t, near)));
            }
            _ => {}
        }
        i += 1;
    }
    Node::Group(ops)
}

/// `WITH QUERY EXPANSION`'s second query: the first (natural-language)
/// query with every indexed word of `terms` (the words of the rows it
/// found best) added as an optional word, each once.
#[must_use]
pub fn expand(first: Node, terms: impl IntoIterator<Item = String>) -> Node {
    let mut have: HashMap<(String, bool), u32> = HashMap::new();
    repeats(&first, &mut have);
    let mut ops = match first {
        Node::Group(ops) => ops,
        other => vec![(Op::Opt, other)],
    };
    let mut added: HashSet<String> = HashSet::new();
    for t in terms {
        if t.starts_with(UNINDEXED) || have.contains_key(&(t.clone(), false)) {
            continue;
        }
        if added.insert(t.clone()) {
            ops.push((
                Op::Opt,
                Node::Word {
                    term: t,
                    prefix: false,
                    indexed: true,
                },
            ));
        }
    }
    Node::Group(ops)
}

// ---------------------------------------------------------------------------
// Relevance.

/// A document of an index (the caller's numbering).
pub type DocId = u64;

/// What a prefix (`w*`) finds: each live document holding a term it
/// expands to, with the count of the term that comes first in the
/// document; and the sums of the terms' document counts, live and raw
/// ([`Postings::raw_df`]), which `InnoDB` takes as the prefix's `df`.
#[derive(Debug, Clone, Default)]
pub struct Prefixed {
    /// (document, count of its first expansion), in document order.
    pub docs: Vec<(DocId, u32)>,
    /// Sum of the expansions' live document counts.
    pub df: usize,
    /// Sum of their raw counts.
    pub raw_df: usize,
}

/// What relevance reads from an index.
pub trait Postings {
    /// Rows in the index.
    fn num_docs(&self) -> u64;
    /// The documents holding `term`, with how often, in document order.
    fn docs(&self, term: &str) -> Vec<(DocId, u32)>;
    /// Documents that ever held `term` since the index was built: the
    /// live ones and the versions rows had before an update or a delete.
    /// `InnoDB` keeps those in its index (until `OPTIMIZE TABLE`) and
    /// counts them in every query but one of a single plain word.
    fn raw_df(&self, term: &str) -> usize {
        self.docs(term).len()
    }
    /// The documents holding an indexed term starting with `prefix`
    /// ([`Prefixed`]).
    fn prefix_docs(&self, prefix: &str) -> Prefixed;
    /// The positions of `term` in each of `docs` (ascending ids) that
    /// holds it, ascending.
    fn positions(&self, term: &str, docs: &[DocId]) -> HashMap<DocId, Vec<u32>>;
}

/// The first byte string after every string starting with `p` (`None`:
/// there is none).
#[must_use]
pub fn prefix_end(p: &[u8]) -> Option<Vec<u8>> {
    let mut v = p.to_vec();
    while let Some(last) = v.pop() {
        if last < 0xFF {
            v.push(last + 1);
            return Some(v);
        }
    }
    None
}

/// `log10(1.0001)`: the idf of a word every row holds.
fn floor_idf() -> f64 {
    1.0001f64.log10()
}

#[allow(clippy::cast_precision_loss)] // counts far below 2^52
fn idf(n: u64, df: usize, r: u32) -> f64 {
    let d = df as f64 * f64::from(r.max(1));
    #[allow(clippy::cast_precision_loss)]
    let n = n as f64;
    if d <= 0.0 {
        return 0.0;
    }
    if (n - d).abs() < f64::EPSILON {
        floor_idf()
    } else {
        (n / d).log10()
    }
}

/// How often each indexed term (and prefix) is named in `node`.
fn repeats(node: &Node, out: &mut HashMap<(String, bool), u32>) {
    match node {
        Node::Word {
            term,
            prefix,
            indexed,
        } => {
            if *indexed {
                *out.entry((term.clone(), *prefix)).or_default() += 1;
            }
        }
        Node::Phrase { words, .. } => {
            for w in words.iter().filter(|w| w.indexed) {
                *out.entry((w.term.clone(), false)).or_default() += 1;
            }
        }
        Node::Group(ops) => {
            for (_, n) in ops {
                repeats(n, out);
            }
        }
    }
}

fn consecutive<'p>(words: &[PhraseWord], at: &dyn Fn(&str) -> &'p Vec<u32>) -> bool {
    let Some(first) = words.first() else {
        return false;
    };
    at(&first.term).iter().any(|&p0| {
        words.iter().enumerate().skip(1).all(|(i, w)| {
            u32::try_from(i).is_ok_and(|i| at(&w.term).binary_search(&(p0 + i)).is_ok())
        })
    })
}

fn within<'p>(words: &[&PhraseWord], n: u32, at: &dyn Fn(&str) -> &'p Vec<u32>) -> bool {
    // Every (position, word) pair, sorted; a window holding each word.
    let mut distinct: Vec<&str> = Vec::new();
    for w in words {
        if !distinct.contains(&w.term.as_str()) {
            distinct.push(&w.term);
        }
    }
    let mut all: Vec<(u32, usize)> = Vec::new();
    for (k, t) in distinct.iter().enumerate() {
        for &p in at(t) {
            all.push((p, k));
        }
    }
    all.sort_unstable();
    let need = distinct.len();
    let mut count = vec![0usize; need];
    let mut have = 0usize;
    let mut lo = 0usize;
    for hi in 0..all.len() {
        let k = all[hi].1;
        if count[k] == 0 {
            have += 1;
        }
        count[k] += 1;
        while have == need {
            if all[hi].0 - all[lo].0 < n {
                return true;
            }
            let k = all[lo].1;
            count[k] -= 1;
            if count[k] == 0 {
                have -= 1;
            }
            lo += 1;
        }
    }
    false
}

struct Ranker<'a> {
    p: &'a dyn Postings,
    n: u64,
    /// The query is one plain word: its live document count is its `df`.
    simple: bool,
    r: HashMap<(String, bool), u32>,
    /// Each term's documents and per-document score, once.
    cache: HashMap<(String, bool), Arc<HashMap<DocId, f64>>>,
}

/// What a row collected: each distinct word's score once (a word named
/// twice counts once), and the operators' steps (+1, -1).
#[derive(Debug, Clone, Default)]
struct Acc {
    words: HashMap<(Arc<str>, bool), f64>,
    bonus: f64,
}

impl Acc {
    fn add(&mut self, o: &Acc) {
        for (k, v) in &o.words {
            self.words.entry(k.clone()).or_insert(*v);
        }
        self.bonus += o.bonus;
    }

    fn total(&self) -> f64 {
        self.words.values().sum::<f64>() + self.bonus
    }
}

type Scores = HashMap<DocId, Acc>;

impl Ranker<'_> {
    fn word(&mut self, term: &str, prefix: bool) -> Arc<HashMap<DocId, f64>> {
        let k = (term.to_owned(), prefix);
        if let Some(s) = self.cache.get(&k) {
            return Arc::clone(s);
        }
        let (docs, df) = if prefix {
            let p = self.p.prefix_docs(term);
            let df = if self.simple { p.df } else { p.raw_df };
            (p.docs, df)
        } else {
            let docs = self.p.docs(term);
            let df = if self.simple {
                docs.len()
            } else {
                self.p.raw_df(term)
            };
            (docs, df)
        };
        let r = self.r.get(&k).copied().unwrap_or(1);
        let i = idf(self.n, df, r);
        let w = i * i;
        let s: HashMap<DocId, f64> = docs
            .into_iter()
            .map(|(d, tf)| (d, f64::from(tf) * w))
            .collect();
        let s = Arc::new(s);
        self.cache.insert(k, Arc::clone(&s));
        s
    }

    fn phrase(&mut self, words: &[PhraseWord], near: Option<u32>) -> Scores {
        let idx: Vec<&PhraseWord> = words.iter().filter(|w| w.indexed).collect();
        if idx.is_empty() {
            return Scores::new();
        }
        let sets: Vec<Arc<HashMap<DocId, f64>>> =
            idx.iter().map(|w| self.word(&w.term, false)).collect();
        let Some(smallest) = sets.iter().min_by_key(|s| s.len()) else {
            return Scores::new();
        };
        let mut cand: Vec<DocId> = smallest
            .keys()
            .copied()
            .filter(|d| sets.iter().all(|s| s.contains_key(d)))
            .collect();
        cand.sort_unstable();
        let mut out = Scores::new();
        let single = idx.len() == 1 && words.len() == 1;
        // Positions of every word of the phrase in the candidates.
        let mut pos: HashMap<&str, HashMap<DocId, Vec<u32>>> = HashMap::new();
        if !single {
            for w in words {
                if !pos.contains_key(w.term.as_str()) {
                    pos.insert(&w.term, self.p.positions(&w.term, &cand));
                }
            }
        }
        let empty = Vec::new();
        for d in cand {
            let at =
                |t: &str| -> &Vec<u32> { pos.get(t).and_then(|m| m.get(&d)).unwrap_or(&empty) };
            let ok = if single {
                true
            } else if let Some(n) = near {
                within(&idx, n, &at)
            } else {
                consecutive(words, &at)
            };
            if ok {
                let mut acc = Acc::default();
                for (w, s) in idx.iter().zip(&sets) {
                    acc.words
                        .entry((Arc::from(w.term.as_str()), false))
                        .or_insert_with(|| s.get(&d).copied().unwrap_or(0.0));
                }
                out.insert(d, acc);
            }
        }
        out
    }

    fn node(&mut self, node: &Node) -> Scores {
        match node {
            Node::Word {
                term,
                prefix,
                indexed,
            } => {
                if !*indexed || term.is_empty() {
                    return Scores::new();
                }
                let key: (Arc<str>, bool) = (Arc::from(term.as_str()), *prefix);
                self.word(term, *prefix)
                    .iter()
                    .map(|(&d, &v)| {
                        let mut a = Acc::default();
                        a.words.insert(key.clone(), v);
                        (d, a)
                    })
                    .collect()
            }
            Node::Phrase { words, near } => self.phrase(words, *near),
            Node::Group(ops) => self.group(ops),
        }
    }

    fn group(&mut self, ops: &[(Op, Node)]) -> Scores {
        // Rows the optional operands found, in order (what `~` looks at).
        let mut found: HashSet<DocId> = HashSet::new();
        let mut musts: Vec<Scores> = Vec::new();
        let mut groups: HashSet<DocId> = HashSet::new();
        let mut not: HashSet<DocId> = HashSet::new();
        let mut incr: HashSet<DocId> = HashSet::new();
        let mut decr: HashSet<DocId> = HashSet::new();
        let mut neg: HashSet<DocId> = HashSet::new();
        // What each row collects from operands other than `+`.
        let mut extra: Scores = Scores::new();
        for (op, x) in ops {
            let s = self.node(x);
            match op {
                Op::Opt | Op::Incr | Op::Decr => {
                    for (&d, a) in &s {
                        found.insert(d);
                        extra.entry(d).or_default().add(a);
                        match op {
                            Op::Incr => {
                                incr.insert(d);
                            }
                            Op::Decr => {
                                decr.insert(d);
                            }
                            _ => {}
                        }
                        if matches!(x, Node::Group(_)) {
                            groups.insert(d);
                        }
                    }
                }
                Op::Must => musts.push(s),
                Op::Not => not.extend(s.keys().copied()),
                Op::Negate => {
                    for (&d, a) in &s {
                        if found.contains(&d) {
                            extra.entry(d).or_default().add(a);
                            neg.insert(d);
                        }
                    }
                }
            }
        }
        let mut rows: BTreeSet<DocId> = BTreeSet::new();
        if musts.is_empty() {
            rows.extend(found.iter().copied());
        } else {
            let Some(first) = musts.iter().min_by_key(|m| m.len()) else {
                return Scores::new();
            };
            rows.extend(
                first
                    .keys()
                    .copied()
                    .filter(|d| musts.iter().all(|m| m.contains_key(d))),
            );
            rows.extend(groups.iter().copied());
        }
        let mut out = Scores::new();
        for d in rows {
            if not.contains(&d) {
                continue;
            }
            let mut a = extra.remove(&d).unwrap_or_default();
            for m in &musts {
                if let Some(x) = m.get(&d) {
                    a.add(x);
                }
            }
            if incr.contains(&d) {
                a.bonus += 1.0;
            }
            if decr.contains(&d) {
                a.bonus -= 1.0;
            }
            if neg.contains(&d) {
                a.bonus -= 1.0;
            }
            out.insert(d, a);
        }
        out
    }
}

/// The rows `query` finds in `p`, with their scores (single precision, as
/// the servers give them). A row not listed scores 0.
#[must_use]
pub fn rank(query: &Node, p: &dyn Postings) -> Vec<(DocId, f32)> {
    let mut r = HashMap::new();
    repeats(query, &mut r);
    let simple = matches!(query, Node::Group(ops)
        if ops.len() == 1
            && matches!(&ops[0], (Op::Opt, Node::Word { prefix: false, .. })));
    let mut ranker = Ranker {
        p,
        n: p.num_docs(),
        simple,
        r,
        cache: HashMap::new(),
    };
    let s = ranker.node(query);
    #[allow(clippy::cast_possible_truncation)]
    let mut out: Vec<(DocId, f32)> = s.into_iter().map(|(d, a)| (d, a.total() as f32)).collect();
    out.sort_unstable_by_key(|(d, _)| *d);
    out
}

/// Orders hits as `WHERE MATCH` returns rows: by score descending, then by
/// `seq` (the document's age) ascending.
pub fn order_hits<T>(hits: &mut [(T, f32, u64)]) {
    hits.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.2.cmp(&b.2)));
}

// ---------------------------------------------------------------------------
// An index in memory (tests, and small callers).

/// Documents in memory: each a list of tokens.
#[derive(Debug, Default, Clone)]
pub struct MemIndex {
    docs: Vec<Option<Vec<Tok>>>,
    /// Removed documents' words.
    dead: Vec<Vec<Tok>>,
}

impl MemIndex {
    /// Adds a document of `texts` (one per column); returns its id.
    pub fn add(&mut self, an: &Analyzer, texts: &[&str]) -> DocId {
        let mut toks = Vec::new();
        let mut pos = 0;
        for t in texts {
            pos = an.tokens(t, pos, &mut toks) + COLUMN_GAP;
        }
        self.docs.push(Some(toks));
        self.docs.len() as DocId - 1
    }

    /// Removes document `d` (its terms stay in the raw counts).
    pub fn remove(&mut self, d: DocId) {
        if let Some(x) = usize::try_from(d).ok().and_then(|d| self.docs.get_mut(d))
            && let Some(t) = x.take()
        {
            self.dead.push(t);
        }
    }

    /// The terms of document `d`.
    #[must_use]
    pub fn terms_of(&self, d: DocId) -> Vec<String> {
        usize::try_from(d)
            .ok()
            .and_then(|i| self.docs.get(i))
            .and_then(Option::as_ref)
            .map(|t| t.iter().map(|x| x.term.clone()).collect())
            .unwrap_or_default()
    }

    fn live(&self) -> impl Iterator<Item = (DocId, &Vec<Tok>)> {
        self.docs
            .iter()
            .enumerate()
            .filter_map(|(i, d)| d.as_ref().map(|t| (i as DocId, t)))
    }
}

impl Postings for MemIndex {
    fn num_docs(&self) -> u64 {
        self.live().count() as u64
    }

    fn docs(&self, term: &str) -> Vec<(DocId, u32)> {
        self.live()
            .filter_map(|(d, t)| {
                let n = t.iter().filter(|x| x.term == term).count();
                (n > 0).then(|| (d, u32::try_from(n).unwrap_or(u32::MAX)))
            })
            .collect()
    }

    fn raw_df(&self, term: &str) -> usize {
        self.docs(term).len()
            + self
                .dead
                .iter()
                .filter(|t| t.iter().any(|x| x.term == term))
                .count()
    }

    fn prefix_docs(&self, prefix: &str) -> Prefixed {
        let hit = |x: &Tok| !x.term.starts_with(UNINDEXED) && x.term.starts_with(prefix);
        let mut out = Prefixed::default();
        let mut terms: BTreeSet<&str> = BTreeSet::new();
        for (d, t) in self.live() {
            if let Some(first) = t.iter().filter(|x| hit(x)).min_by_key(|x| x.pos) {
                let n = t.iter().filter(|x| x.term == first.term).count();
                out.docs.push((d, u32::try_from(n).unwrap_or(u32::MAX)));
            }
            terms.extend(t.iter().filter(|x| hit(x)).map(|x| x.term.as_str()));
        }
        for t in &self.dead {
            terms.extend(t.iter().filter(|x| hit(x)).map(|x| x.term.as_str()));
        }
        for t in terms {
            out.df += self.docs(t).len();
            out.raw_df += self.raw_df(t);
        }
        out
    }

    fn positions(&self, term: &str, docs: &[DocId]) -> HashMap<DocId, Vec<u32>> {
        docs.iter()
            .filter_map(|&d| {
                let t = usize::try_from(d)
                    .ok()
                    .and_then(|i| self.docs.get(i))
                    .and_then(Option::as_ref)?;
                let p: Vec<u32> = t.iter().filter(|x| x.term == term).map(|x| x.pos).collect();
                (!p.is_empty()).then_some((d, p))
            })
            .collect()
    }
}

#[cfg(test)]
mod tests;
