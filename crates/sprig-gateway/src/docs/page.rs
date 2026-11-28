//! One docs page, rebuilt from its chunks: `GET /v1/docs/pages/{page}`.
//! From the local pack when this gateway has one ([`local_page`]); else
//! on a cluster:
//!
//! ```text
//! rows  := SELECT chunk_id, title, heading_path, audience, text, seq
//!          FROM __sprig_docs.chunks WHERE version = ? AND page = ?
//! order := by seq when any row has one (pack format 2);
//!          else by heading path, title section first, sibling sections
//!          alphabetically, a long section's pieces by their chunk ids
//!          (`ordered: false`: the sections may be out of order)
//! page  := sprig_docs::chunk::rebuild(rows in order)
//! ```
//!
//! A cluster whose table predates `seq` (a controller of an older
//! release hasn't added it yet) is read without it, as unordered.

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, Value};
use serde::Serialize;
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_docs::chunk::{PATH_SEP, rebuild};
use sprig_docs::index::DocsIndex;
use sprig_docs::pack::{chunk_id, hex};
use sprig_docs::table::{self, MAX_PAGE};

use super::DocsService;

/// One chunk of a page, in page order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PageChunk {
    /// The chunk's id, hexadecimal.
    pub chunk_id: String,
    /// The headings above it.
    pub heading_path: String,
    /// Its Markdown.
    pub text: String,
    /// Its position in the page, from 1; 0 when the pack doesn't say.
    pub seq: u16,
}

/// A page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PageReply {
    /// The release read.
    pub version: String,
    /// The page's path under `docs/`.
    pub page: String,
    /// Its title.
    pub title: String,
    /// Its audiences.
    pub audience: Vec<String>,
    /// The chunks came in the pack's own order; `false` when it had none
    /// (a pack older than format 2) and the order was worked out from the
    /// headings, so sections may be out of order.
    pub ordered: bool,
    /// The page's Markdown, rebuilt from the chunks.
    pub markdown: String,
    /// The chunks, in order.
    pub chunks: Vec<PageChunk>,
}

fn invalid(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::AdminRequestInvalid, m)
}

/// A page path as the pack names it: `tools/studio.md`. A missing `.md`
/// is added, a directory (`tools/`, or nothing) is its `README.md`.
///
/// # Errors
/// A path that isn't one: `..`, a character outside letters, digits,
/// `-`, `_`, `.` and `/`, or longer than the column.
pub fn normalize(path: &str) -> Result<String> {
    let p = path.trim().trim_start_matches('/');
    let p = if p.is_empty() || p.ends_with('/') {
        format!("{p}README.md")
    } else if p.to_ascii_lowercase().ends_with(".md") {
        // `.MD` as `.md`: the extension is three ASCII bytes.
        format!("{}.md", &p[..p.len() - 3])
    } else {
        format!("{p}.md")
    };
    let ok = p
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'/'));
    if !ok || p.split('/').any(|s| s.is_empty() || s == "." || s == "..") {
        return Err(invalid(format!("{path:?} isn't a docs page path")));
    }
    if p.chars().count() > MAX_PAGE {
        return Err(invalid(format!(
            "a docs page path is at most {MAX_PAGE} characters"
        )));
    }
    Ok(p)
}

/// A row as read: id, heading path, text, seq.
pub type Row = (Vec<u8>, String, String, u16);

/// `rows` in page order (pure, for tests): by `seq` when any row has one;
/// else by heading path (the title section first, siblings by name) and,
/// within one heading path, by the occurrence its chunk id encodes.
/// Returns whether the pack's order was used.
#[must_use]
pub fn order(version: &str, page: &str, mut rows: Vec<Row>) -> (Vec<Row>, bool) {
    if rows.iter().any(|r| r.3 > 0) {
        rows.sort_by(|a, b| a.3.cmp(&b.3).then_with(|| a.0.cmp(&b.0)));
        return (rows, true);
    }
    let occurrence = |r: &Row| {
        (0..rows.len())
            .find(|&n| chunk_id(version, page, &r.1, n).as_slice() == r.0.as_slice())
            .unwrap_or(usize::MAX)
    };
    let mut keyed: Vec<(Vec<String>, usize, Row)> = rows
        .iter()
        .map(|r| {
            let path = r.1.split(PATH_SEP).map(str::to_owned).collect();
            (path, occurrence(r), r.clone())
        })
        .collect();
    keyed.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    (keyed.into_iter().map(|(_, _, r)| r).collect(), false)
}

fn db(what: &str, e: mysql_async::Error) -> SprigError {
    SprigError::new(SprigCode::BackendUnavailable, format!("docs page: {what}")).with_source(e)
}

type Fetched = (Vec<u8>, String, String, String, String, u16);

async fn fetch(conn: &mut Conn, version: &str, page: &str) -> Result<Vec<Fetched>> {
    let sql = |seq: &str| {
        format!(
            "SELECT chunk_id, title, heading_path, CAST(audience AS CHAR), text, {seq} FROM {} \
             WHERE version = ? AND page = ?",
            table::qualified(table::CHUNKS)
        )
    };
    let params = || vec![Value::from(version), Value::from(page)];
    match conn.exec(sql("seq"), params()).await {
        Ok(rows) => Ok(rows),
        // 1054: the table has no seq yet.
        Err(mysql_async::Error::Server(s)) if s.code == 1054 => conn
            .exec(sql("0"), params())
            .await
            .map_err(|e| db("reading chunks", e)),
        Err(e) => Err(db("reading chunks", e)),
    }
}

/// Page `page` (normalized) of the local pack.
///
/// # Errors
/// 39792 when the release has no such page.
pub fn local_page(ix: &DocsIndex, page: String) -> Result<PageReply> {
    let Some(members) = ix.page(&page).filter(|m| !m.is_empty()) else {
        return Err(invalid(format!(
            "release {} has no docs page {page}",
            ix.version()
        )));
    };
    let first = &ix.chunks()[members[0]];
    let (title, audience) = (first.title.clone(), first.audience.clone());
    let (rows, ordered) = order(
        ix.version(),
        &page,
        members
            .iter()
            .map(|&i| {
                let c = &ix.chunks()[i];
                (
                    c.chunk_id.to_vec(),
                    c.heading_path.clone(),
                    c.text.clone(),
                    c.seq,
                )
            })
            .collect(),
    );
    let markdown = rebuild(rows.iter().map(|r| (r.1.as_str(), r.2.as_str())));
    Ok(PageReply {
        version: ix.version().to_owned(),
        page,
        title,
        audience,
        ordered,
        markdown,
        chunks: rows
            .into_iter()
            .map(|(id, heading_path, text, seq)| PageChunk {
                chunk_id: hex(&id),
                heading_path,
                text,
                seq,
            })
            .collect(),
    })
}

impl DocsService {
    /// The page `path` (see the module docs) of `version`, or of the
    /// release a search would read.
    ///
    /// # Errors
    /// A bad path, or no such page (39792); 39989
    /// when docs are unavailable; the backend's.
    pub async fn page(
        &self,
        tenant: Option<TenantId>,
        path: &str,
        version: Option<&str>,
    ) -> Result<PageReply> {
        let page = normalize(path)?;
        if let Some(ix) = self.local_for(version) {
            return local_page(&ix, page);
        }
        let mut t = self.target(tenant, version).await?;
        let rows = fetch(&mut t.conn, &t.version, &page).await?;
        let Some(first) = rows.first() else {
            return Err(invalid(format!(
                "release {} has no docs page {page}",
                t.version
            )));
        };
        let title = first.1.clone();
        let audience = first
            .3
            .split(',')
            .filter(|a| !a.is_empty())
            .map(str::to_owned)
            .collect();
        let (rows, ordered) = order(
            &t.version,
            &page,
            rows.into_iter()
                .map(|(id, _, hp, _, text, seq)| (id, hp, text, seq))
                .collect(),
        );
        let markdown = rebuild(rows.iter().map(|r| (r.1.as_str(), r.2.as_str())));
        Ok(PageReply {
            version: t.version,
            page,
            title,
            audience,
            ordered,
            markdown,
            chunks: rows
                .into_iter()
                .map(|(id, heading_path, text, seq)| PageChunk {
                    chunk_id: hex(&id),
                    heading_path,
                    text,
                    seq,
                })
                .collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sprig_docs::pack::chunk_pages;
    use sprig_docs::page::parse_page;

    #[test]
    fn page_paths() {
        assert_eq!(normalize("tools/studio.md").unwrap(), "tools/studio.md");
        assert_eq!(normalize("/tools/studio").unwrap(), "tools/studio.md");
        assert_eq!(normalize("tools/").unwrap(), "tools/README.md");
        assert_eq!(normalize("").unwrap(), "README.md");
        assert_eq!(normalize("a/B.MD").unwrap(), "a/B.md");
        for bad in [
            "../etc/passwd",
            "a//b.md",
            "a b.md",
            "a/./b.md",
            "a%2e.md",
            "q?.md",
        ] {
            assert!(normalize(bad).is_err(), "{bad}");
        }
        assert!(normalize(&"a".repeat(300)).is_err());
    }

    fn page() -> Vec<Row> {
        let p = parse_page(
            "guides/x.md",
            &format!(
                "---\ntitle: X\naudience: [user]\n---\n\n# X\n\nIntro.\n\n## Zed\n\n{}\n\n## Alpha\n\nAlpha text that is long enough not to be merged into the chunk before it, \
                 which needs eighty tokens or more: one two three four five six seven eight nine ten eleven twelve \
                 thirteen fourteen fifteen sixteen seventeen eighteen nineteen twenty twenty-one twenty-two \
                 twenty-three twenty-four twenty-five twenty-six twenty-seven twenty-eight twenty-nine thirty.\n",
                (0..120)
                    .map(|i| format!("Sentence {i} of a long section that is split into pieces."))
                    .collect::<Vec<_>>()
                    .join("\n\n")
            ),
        )
        .unwrap();
        chunk_pages("1.0.0", &[p])
            .into_iter()
            .map(|c| (c.chunk_id.to_vec(), c.heading_path, c.text, c.seq))
            .collect()
    }

    #[test]
    fn pages_follow_seq_else_the_headings() {
        let want = page();
        assert!(want.len() >= 4, "{}", want.len());
        let mut shuffled = want.clone();
        shuffled.sort_by(|a, b| a.0.cmp(&b.0));
        let (got, ordered) = order("1.0.0", "guides/x.md", shuffled.clone());
        assert!(ordered);
        assert_eq!(got, want);
        // Without positions: the title, then siblings by name, each long
        // section's pieces in order.
        let unknown: Vec<Row> = shuffled
            .into_iter()
            .map(|(id, hp, t, _)| (id, hp, t, 0))
            .collect();
        let (got, ordered) = order("1.0.0", "guides/x.md", unknown);
        assert!(!ordered);
        let paths: Vec<&str> = got.iter().map(|r| r.1.as_str()).collect();
        assert_eq!(paths[0], "X");
        assert_eq!(paths[1], "X > Alpha");
        assert!(paths[2..].iter().all(|p| *p == "X > Zed"));
        let zed: Vec<&str> = want
            .iter()
            .filter(|r| r.1 == "X > Zed")
            .map(|r| r.2.as_str())
            .collect();
        let got_zed: Vec<&str> = got[2..].iter().map(|r| r.2.as_str()).collect();
        assert_eq!(got_zed, zed);
    }
}
