//! MongoDB's `textScore` for `$text`, computed from a document alone.
//!
//! MongoDB scores a text index's terms when it writes a document, and a
//! `$text` search adds up the scores of the query's terms:
//!
//! ```text
//! for each string the index covers, with its field's weight w:
//!   words  := the string's words (letters, digits, `_`, `'`), lowercased,
//!             stop words left out, stemmed, diacritics folded
//!   n      := how many words
//!   for each distinct term t among them, seen c times:
//!     freq   := 1 + 1/2 + 1/4 + ... (c terms)
//!     coeff  := 0.5 × c / n + 0.5
//!     adjust := 1.1 if the whole string is t (ignoring case), else 1
//!     score[t] += w × freq × coeff × adjust
//! textScore := Σ score[t] over the query's terms (not negated; a phrase's
//!              words count as terms)
//! ```
//!
//! Checked against MongoDB 8.0.34: `"shoe"` 1.1, `"shoe shoe"` 1.5,
//! `"red shoe blue"` 0.6667, an array `["shoe", "red shoe"]` 1.85, weights
//! 10 and 2 over `"shoe"` and `"shoe shoe red"` 13.5. English stop words
//! are MongoDB's own 174 (`don't` is one word, and a stop word); other
//! languages use the analyzer's lists, which can differ from MongoDB's.
//!
//! Nothing here depends on other documents, so a user a row policy binds
//! gets the same score as anyone else for the same document.

use std::collections::{HashMap, HashSet};

use sprig_common::{Result, SprigCode, SprigError};
use tantivy::tokenizer::{
    AsciiFoldingFilter, Language, RawTokenizer, Stemmer, StopWordFilter, TextAnalyzer, TokenStream,
};

/// MongoDB's English stop words (its `stop_words_english.txt`, as the
/// server shows them: each is left out of scores).
const ENGLISH_STOP: [&str; 174] = [
    "a",
    "about",
    "above",
    "after",
    "again",
    "against",
    "all",
    "am",
    "an",
    "and",
    "any",
    "are",
    "aren't",
    "as",
    "at",
    "be",
    "because",
    "been",
    "before",
    "being",
    "below",
    "between",
    "both",
    "but",
    "by",
    "can't",
    "cannot",
    "could",
    "couldn't",
    "did",
    "didn't",
    "do",
    "does",
    "doesn't",
    "doing",
    "don't",
    "down",
    "during",
    "each",
    "few",
    "for",
    "from",
    "further",
    "had",
    "hadn't",
    "has",
    "hasn't",
    "have",
    "haven't",
    "having",
    "he",
    "he'd",
    "he'll",
    "he's",
    "her",
    "here",
    "here's",
    "hers",
    "herself",
    "him",
    "himself",
    "his",
    "how",
    "how's",
    "i",
    "i'd",
    "i'll",
    "i'm",
    "i've",
    "if",
    "in",
    "into",
    "is",
    "isn't",
    "it",
    "it's",
    "its",
    "itself",
    "let's",
    "me",
    "more",
    "most",
    "mustn't",
    "my",
    "myself",
    "no",
    "nor",
    "not",
    "of",
    "off",
    "on",
    "once",
    "only",
    "or",
    "other",
    "ought",
    "our",
    "ours",
    "ourselves",
    "out",
    "over",
    "own",
    "same",
    "shan't",
    "she",
    "she'd",
    "she'll",
    "she's",
    "should",
    "shouldn't",
    "so",
    "some",
    "such",
    "than",
    "that",
    "that's",
    "the",
    "their",
    "theirs",
    "them",
    "themselves",
    "then",
    "there",
    "there's",
    "these",
    "they",
    "they'd",
    "they'll",
    "they're",
    "they've",
    "this",
    "those",
    "through",
    "to",
    "too",
    "under",
    "until",
    "up",
    "very",
    "was",
    "wasn't",
    "we",
    "we'd",
    "we'll",
    "we're",
    "we've",
    "were",
    "weren't",
    "what",
    "what's",
    "when",
    "when's",
    "where",
    "where's",
    "which",
    "while",
    "who",
    "who's",
    "whom",
    "why",
    "why's",
    "with",
    "won't",
    "would",
    "wouldn't",
    "you",
    "you'd",
    "you'll",
    "you're",
    "you've",
    "your",
    "yours",
    "yourself",
    "yourselves",
];

/// Whether `c` belongs to a word, as MongoDB's text tokenizer splits:
/// letters, digits and combining marks, `_`, the ASCII apostrophe and the
/// middle dot. Everything else (spaces, punctuation, `-`, `.`, `’`,
/// symbols, emoji) separates words.
fn word_char(c: char) -> bool {
    c.is_alphanumeric()
        || matches!(c, '_' | '\'' | '\u{00B7}')
        || ('\u{0300}'..='\u{036F}').contains(&c)
}

/// The words of `text`, as written.
fn words(text: &str) -> impl Iterator<Item = &str> {
    text.split(|c: char| !word_char(c))
        .filter(|w| !w.is_empty())
}

fn one(a: &mut TextAnalyzer, w: &str) -> Option<String> {
    let mut s = a.token_stream(w);
    let mut out = None;
    while s.advance() {
        out = Some(s.token().text.clone());
    }
    out
}

/// A `$text` query's terms and the language's word rules.
pub struct MongoTextScore {
    english: bool,
    stop: TextAnalyzer,
    stem: TextAnalyzer,
    fold: TextAnalyzer,
    terms: HashSet<String>,
}

impl std::fmt::Debug for MongoTextScore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MongoTextScore")
            .field("english", &self.english)
            .field("terms", &self.terms)
            .finish_non_exhaustive()
    }
}

impl MongoTextScore {
    /// The scorer of `$search` text `query` for an index whose language is
    /// `language` (`english`, `none`, ...).
    ///
    /// # Errors
    /// 39828 for a language without a stemmer.
    pub fn new(language: &str, query: &str) -> Result<Self> {
        let none = language.eq_ignore_ascii_case("none");
        let lang = if none {
            None
        } else {
            Some(crate::analyze::language(language).ok_or_else(|| {
                SprigError::new(
                    SprigCode::SearchAnalyzerUnknown,
                    format!("unsupported language: \"{language}\""),
                )
            })?)
        };
        let raw = || TextAnalyzer::builder(RawTokenizer::default()).dynamic();
        let stop = match lang.and_then(StopWordFilter::new) {
            Some(f) if lang != Some(Language::English) => raw().filter_dynamic(f).build(),
            _ => raw().build(),
        };
        let stem = match lang {
            Some(l) => raw().filter_dynamic(Stemmer::new(l)).build(),
            None => raw().build(),
        };
        let fold = raw().filter_dynamic(AsciiFoldingFilter).build();
        let mut s = Self {
            english: lang == Some(Language::English),
            stop,
            stem,
            fold,
            terms: HashSet::new(),
        };
        s.terms = positive_words(query)
            .into_iter()
            .filter_map(|w| s.term(w))
            .collect();
        Ok(s)
    }

    /// A word's term: lowercased, `None` for a stop word, stemmed, folded.
    fn term(&mut self, w: &str) -> Option<String> {
        let lower = w.to_lowercase();
        if self.english {
            if ENGLISH_STOP.contains(&lower.as_str()) {
                return None;
            }
        } else {
            one(&mut self.stop, &lower)?;
        }
        let stemmed = one(&mut self.stem, &lower)?;
        one(&mut self.fold, &stemmed)
    }

    /// Adds the term scores of one string of a field weighted `weight`.
    pub fn add(&mut self, raw: &str, weight: f64, acc: &mut HashMap<String, f64>) {
        let mut seen: HashMap<String, (u32, f64)> = HashMap::new();
        let mut n = 0u32;
        let ws: Vec<&str> = words(raw).collect();
        for w in ws {
            let Some(t) = self.term(w) else { continue };
            n += 1;
            let e = seen.entry(t).or_insert((0, 0.0));
            e.1 += 1.0 / f64::from(1u32 << e.0.min(31));
            e.0 += 1;
        }
        for (t, (count, freq)) in seen {
            let coeff = 0.5 * f64::from(count) / f64::from(n) + 0.5;
            let adjust = if raw.chars().count() == t.chars().count() && raw.to_lowercase() == t {
                1.1
            } else {
                1.0
            };
            *acc.entry(t).or_insert(0.0) += weight * freq * coeff * adjust;
        }
    }

    /// The document's score from its accumulated term scores.
    #[must_use]
    pub fn total(&self, acc: &HashMap<String, f64>) -> f64 {
        self.terms.iter().filter_map(|t| acc.get(t)).sum()
    }
}

/// The words of a `$search` string that score: every word outside a
/// negation, inside phrases too. `-word` and `-"a phrase"` negate.
fn positive_words(query: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut in_phrase = false;
    let mut negated_phrase = false;
    let b = query.as_bytes();
    let mut i = 0;
    while i < query.len() {
        let c = query[i..].chars().next().unwrap_or(' ');
        if c == '"' {
            if in_phrase {
                in_phrase = false;
                negated_phrase = false;
            } else {
                in_phrase = true;
                negated_phrase =
                    i > 0 && b[i - 1] == b'-' && (i < 2 || !word_char(char::from(b[i - 2])));
            }
            i += 1;
            continue;
        }
        if !word_char(c) {
            i += c.len_utf8();
            continue;
        }
        let start = i;
        while i < query.len() {
            let d = query[i..].chars().next().unwrap_or(' ');
            if !word_char(d) {
                break;
            }
            i += d.len_utf8();
        }
        let negated = if in_phrase {
            negated_phrase
        } else {
            start > 0 && b[start - 1] == b'-' && (start < 2 || !word_char(char::from(b[start - 2])))
        };
        if !negated {
            out.push(&query[start..i]);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn score(q: &str, fields: &[(&[&str], f64)]) -> f64 {
        let mut s = MongoTextScore::new("english", q).unwrap();
        let mut acc = HashMap::new();
        for (strings, w) in fields {
            for x in *strings {
                s.add(x, *w, &mut acc);
            }
        }
        s.total(&acc)
    }

    fn close(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-9
    }

    #[test]
    fn scores_as_mongodb_8_0_does() {
        // Each case's MongoDB 8.0.34 textScore.
        for (q, text, want) in [
            ("shoe", "shoe", 1.1),
            ("shoe", "Shoe", 1.1),
            ("shoe", "shoes", 1.0),
            ("shoe", "a shoe", 1.0),
            ("shoe", "red shoe", 0.75),
            ("shoe", "shoe shoe", 1.5),
            ("shoe", "shoe shoe shoe", 1.75),
            ("shoe", "red shoe blue", 2.0 / 3.0),
            ("shoe", "don't shoe", 1.0),
            ("shoe", "e-mail shoe", 2.0 / 3.0),
            ("shoe", "café shoe", 0.75),
            ("run", "running shoes run", 1.25),
            ("shoe red", "red shoe", 1.5),
            ("shoe red", "red shoe blue", 4.0 / 3.0),
            ("\"red shoe\"", "red shoe", 1.5),
            ("shoe -red", "shoe", 1.1),
            ("john", "john's", 1.0),
        ] {
            let got = score(q, &[(&[text], 1.0)]);
            assert!(close(got, want), "{q} over {text:?}: {got}, MongoDB {want}");
        }
        assert!(close(score("shoe", &[(&["shoe", "red shoe"], 1.0)]), 1.85));
        assert!(close(
            score("shoe", &[(&["shoe"], 10.0), (&["shoe shoe red"], 2.0)]),
            13.5
        ));
        assert!(close(
            score("shoe", &[(&["x y shoe"], 10.0), (&["z"], 2.0)]),
            20.0 / 3.0
        ));
        assert!(close(score("zebra", &[(&["shoe"], 1.0)]), 0.0));
    }

    #[test]
    fn negations_and_phrases() {
        assert_eq!(
            positive_words("a -b \"c d\" -\"e f\" g-h"),
            vec!["a", "c", "d", "g", "h"]
        );
        assert_eq!(ENGLISH_STOP.len(), 174);
        assert!(MongoTextScore::new("klingon", "x").is_err());
        assert!(MongoTextScore::new("none", "the shoe").is_ok());
    }
}
