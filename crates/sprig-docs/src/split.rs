//! Cutting any text into chunks for search, as retrieval indexes do with
//! their tables' rows (the docs pack cuts pages with [`crate::chunk`]):
//!
//! - [`by_headings`]: Markdown, at its headings, through the docs pack's
//!   own chunker ([`crate::chunk::chunk_page`]), so a long section is cut
//!   between blocks and code blocks stay whole;
//! - [`by_tokens`]: plain text, into pieces of at most about `max`
//!   tokens ([`crate::chunk::estimate_tokens`]), cut between paragraphs,
//!   then between sentences, then between words.

use crate::chunk::{PATH_SEP, chunk_page, estimate_tokens};
use crate::page::Page;

/// One chunk of a text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TextChunk {
    /// The title and the headings above the chunk (`Title > Section`);
    /// empty without either.
    pub heading: String,
    /// The chunk.
    pub text: String,
}

/// `text` cut at its Markdown headings, `title` (which may be empty)
/// heading every chunk's path.
#[must_use]
pub fn by_headings(title: &str, text: &str) -> Vec<TextChunk> {
    let page = Page {
        path: String::new(),
        title: title.to_owned(),
        audience: Vec::new(),
        body: text.to_owned(),
    };
    chunk_page(&page)
        .into_iter()
        .filter(|c| !c.text.trim().is_empty())
        .map(|c| TextChunk {
            heading: c
                .heading_path
                .iter()
                .filter(|h| !h.trim().is_empty())
                .cloned()
                .collect::<Vec<_>>()
                .join(PATH_SEP),
            text: c.text,
        })
        .collect()
}

/// Splits `s` after each sentence end (`.`, `!` or `?` and a space).
fn sentences(s: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let mut start = 0;
    let b = s.as_bytes();
    for i in 0..b.len().saturating_sub(1) {
        if matches!(b[i], b'.' | b'!' | b'?') && b[i + 1].is_ascii_whitespace() {
            out.push(&s[start..=i]);
            start = i + 1;
        }
    }
    if start < s.len() {
        out.push(&s[start..]);
    }
    out.into_iter()
        .map(str::trim)
        .filter(|x| !x.is_empty())
        .collect()
}

/// Pieces of `s`, none over `max` tokens: whole when it fits, else its
/// sentences, else its words.
fn pieces(s: &str, max: usize) -> Vec<String> {
    if estimate_tokens(s) <= max {
        return vec![s.to_owned()];
    }
    let parts = sentences(s);
    if parts.len() > 1 {
        return pack(parts.into_iter().map(str::to_owned).collect(), " ", max);
    }
    pack(
        s.split_whitespace().map(str::to_owned).collect(),
        " ",
        max,
    )
}

/// Joins consecutive `parts` with `sep` into pieces of at most `max`
/// tokens (a part longer by itself is a piece of its own, cut later).
fn pack(parts: Vec<String>, sep: &str, max: usize) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut cur = String::new();
    let mut cur_tokens = 0;
    for p in parts {
        let cut = if estimate_tokens(&p) > max && p.contains(char::is_whitespace) {
            pieces(&p, max)
        } else {
            vec![p]
        };
        for p in cut {
            let t = estimate_tokens(&p);
            if !cur.is_empty() && cur_tokens + t > max {
                out.push(std::mem::take(&mut cur));
                cur_tokens = 0;
            }
            if !cur.is_empty() {
                cur.push_str(sep);
            }
            cur.push_str(&p);
            cur_tokens += t;
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

/// `text` cut into pieces of at most about `max` tokens: whole paragraphs
/// (split at blank lines) while they fit, long paragraphs between
/// sentences, long sentences between words.
#[must_use]
pub fn by_tokens(text: &str, max: usize) -> Vec<String> {
    let max = max.max(8);
    let paragraphs: Vec<String> = text
        .replace("\r\n", "\n")
        .split("\n\n")
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .map(str::to_owned)
        .collect();
    pack(paragraphs, "\n\n", max)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_text_is_one_chunk() {
        assert_eq!(by_tokens("  hello there  ", 400), ["hello there"]);
        assert!(by_tokens(" \n\n ", 400).is_empty());
    }

    #[test]
    fn long_text_is_cut_between_paragraphs_then_sentences() {
        let para = "The quick brown fox jumps over the lazy dog. ".repeat(10);
        let text = format!("{para}\n\n{para}\n\n{para}");
        let chunks = by_tokens(&text, 120);
        assert!(chunks.len() >= 3, "{chunks:?}");
        for c in &chunks {
            assert!(estimate_tokens(c) <= 120, "{c}");
            assert!(c.ends_with('.'), "cut inside a sentence: {c}");
        }
        // Nothing is lost.
        let words = |s: &str| s.split_whitespace().count();
        assert_eq!(
            chunks.iter().map(|c| words(c)).sum::<usize>(),
            words(&text)
        );
        // A sentence with no ends is cut between words.
        let long = "word ".repeat(500);
        let chunks = by_tokens(&long, 100);
        assert!(chunks.len() >= 5);
        assert!(chunks.iter().all(|c| estimate_tokens(c) <= 100));
    }

    #[test]
    fn headings_name_their_chunks() {
        let body = "Intro text that is long enough to stand alone as its own chunk here.\n\n\
                    ## Reset\n\nTo reset your password open settings and choose reset.\n";
        let c = by_headings("Account help", body);
        assert!(!c.is_empty());
        assert!(c.iter().any(|x| x.heading.starts_with("Account help")));
        let untitled = by_headings("", "## Only\n\nsome text\n");
        assert_eq!(untitled[0].heading, "Only");
    }
}
