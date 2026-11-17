//! A page's chunks in `seq` order give its Markdown back
//! ([`sprig_docs::chunk::rebuild`]): for the chunking fixture and for every
//! page of the repository's docs, the rebuilt page has the same lines as
//! the page body, apart from blank lines, HTML comment lines and the
//! title heading.

use std::path::PathBuf;

use sprig_docs::chunk::rebuild;
use sprig_docs::pack::{Chunk, chunk_pages};
use sprig_docs::page::{Page, parse_page, read_pages};

/// The lines that must survive: not blank, not a comment line, not a `#`
/// title. Code is compared as written; other lines trimmed.
fn lines(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut fence = false;
    for l in text.lines() {
        let t = l.trim();
        if t.starts_with("```") || t.starts_with("~~~") {
            fence = !fence;
            out.push(t.to_owned());
            continue;
        }
        if fence {
            out.push(l.to_owned());
            continue;
        }
        if t.is_empty() || (t.starts_with("<!--") && t.ends_with("-->")) {
            continue;
        }
        if t.starts_with("# ") || t == "#" {
            continue;
        }
        // Headings are compared without closing hashes.
        if t.starts_with("##") {
            out.push(t.trim_end_matches('#').trim_end().to_owned());
            continue;
        }
        out.push(t.to_owned());
    }
    // Blank lines inside code don't matter either.
    out.retain(|l| !l.trim().is_empty());
    out
}

fn level(l: &str) -> Option<usize> {
    let n = l.chars().take_while(|&c| c == '#').count();
    (2..=3).contains(&n).then_some(n)
}

/// `lines` without the headings of sections that have neither text nor
/// sub-sections: they make no chunk, so a rebuilt page doesn't have them.
fn without_empty_sections(mut ls: Vec<String>) -> Vec<String> {
    let mut i = 0;
    while i < ls.len() {
        let empty = level(&ls[i]).is_some_and(|n| {
            ls.get(i + 1)
                .is_none_or(|next| level(next).is_some_and(|m| m <= n))
        });
        if empty {
            ls.remove(i);
            i = i.saturating_sub(1);
        } else {
            i += 1;
        }
    }
    ls
}

fn rebuilt(page: &Page) -> String {
    let mut chunks: Vec<Chunk> = chunk_pages("0.0.0-test", std::slice::from_ref(page));
    // As read back from the table: in chunk_id order, then by position.
    chunks.sort_by_key(|c| c.chunk_id);
    chunks.sort_by_key(|c| c.seq);
    rebuild(
        chunks
            .iter()
            .map(|c| (c.heading_path.as_str(), c.text.as_str())),
    )
}

fn check(page: &Page) -> Result<(), String> {
    let got = lines(&rebuilt(page));
    let want = without_empty_sections(lines(&page.body));
    if got == want {
        return Ok(());
    }
    let at = got.iter().zip(&want).take_while(|(a, b)| a == b).count();
    Err(format!(
        "{}: line {at} differs:\n  rebuilt: {:?}\n  page:    {:?}",
        page.path,
        got.get(at),
        want.get(at)
    ))
}

#[test]
fn the_fixture_page_comes_back() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    let text = std::fs::read_to_string(dir.join("chunking.md")).expect("fixture");
    let page = parse_page("fixtures/chunking.md", &text).expect("page");
    check(&page).unwrap();
    let md = rebuilt(&page);
    assert!(md.starts_with("# "), "{md:.80}");
}

#[test]
fn every_docs_page_comes_back() {
    let docs = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../docs");
    let pages = read_pages(&docs).expect("docs");
    assert!(pages.len() > 20, "{} pages", pages.len());
    let bad: Vec<String> = pages.iter().filter_map(|p| check(p).err()).collect();
    assert!(
        bad.is_empty(),
        "{} of {} pages:\n{}",
        bad.len(),
        pages.len(),
        bad.join("\n")
    );
}

#[test]
fn headings_come_from_paths_and_repeated_leads_go() {
    let md = rebuild([
        ("Guide", "Intro."),
        ("Guide > Setup", "First. More of setup."),
        ("Guide > Setup", "First.\n\nThe rest of setup."),
        (
            "Guide > Setup > Keys",
            "Keys text.\n\n## Merged\n\nMerged text.",
        ),
        ("Guide > Merged > Deep", "Deep text."),
    ]);
    assert_eq!(
        md,
        "# Guide\n\nIntro.\n\n## Setup\n\nFirst. More of setup.\n\nThe rest of setup.\n\n\
         ### Keys\n\nKeys text.\n\n## Merged\n\nMerged text.\n\n### Deep\n\nDeep text.\n"
    );
}
