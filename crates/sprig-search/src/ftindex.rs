//! The Tantivy files of one apply-time full-text index: one `FULLTEXT` key
//! of one table on one unit, kept by the node's apply loop and read with
//! `InnoDB`'s rules ([`crate::innodb`]).
//!
//! | Field | Kind | Holds |
//! |---|---|---|
//! | `_key` | bytes, indexed, fast | the row's key after its prefix (the memcomparable primary key); deletes go by it |
//! | `_pk` | bytes, fast | the primary key's values, encoded by the caller, for point reads |
//! | `_seq` | u64, fast | the document's age: ties in `WHERE MATCH` come back oldest first |
//! | `_h` | u64, fast | a hash of the row's words: a change that leaves them as they were is no new version |
//! | `_t` | text, stored | the row's distinct searchable terms (for its tombstone) |
//! | `w` | text, positions | every word's term ([`crate::innodb::Analyzer::tokens`]) |
//! | `d` | text | tombstones: the searchable terms of row versions replaced or deleted |
//!
//! **Tombstones.** `InnoDB` keeps a row's old version in its index after an
//! update or a delete (until `OPTIMIZE TABLE`), and every query other than
//! a single plain word counts it in a word's document count. When a row
//! changes, its old version's terms go into a document of their own under
//! `d`, which nothing searches; [`crate::innodb::Postings::raw_df`] counts
//! them.
//!
//! Text is never stored: rows are read from the keyspace. Every change is
//! delete-then-add by `_key` (Tantivy applies a delete only to documents
//! added before it), so replaying log entries after a crash is harmless.
//! Each commit carries the last log index it covers as its payload.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;
use std::sync::{Mutex, MutexGuard, PoisonError};

use sprig_common::{Result, SprigError};
use tantivy::directory::MmapDirectory;
use tantivy::postings::Postings as _;
use tantivy::schema::{
    FAST, Field, INDEXED, IndexRecordOption, STORED, Schema, TextFieldIndexing, TextOptions,
    Value as _,
};
use tantivy::tokenizer::{PreTokenizedString, Token};
use tantivy::{
    DocAddress, DocSet, Index, IndexReader, IndexWriter, ReloadPolicy, Searcher, TERMINATED,
    TantivyDocument, Term,
};

use crate::innodb::{DocId, Postings, Prefixed, Tok, UNINDEXED};

/// Tantivy's smallest writer budget.
pub const MIN_WRITER_BYTES: usize = 15_000_000;

/// The term every tombstone holds under `d`: their count.
const TOMB: &str = "\u{1}";

fn terr(what: &str, e: impl std::fmt::Display) -> SprigError {
    SprigError::internal(format!("full-text index {what}: {e}"))
}

/// The fields.
#[derive(Debug, Clone, Copy)]
struct Fields {
    key: Field,
    pk: Field,
    seq: Field,
    hash: Field,
    terms: Field,
    w: Field,
    dead: Field,
}

fn schema() -> (Schema, Fields) {
    let mut b = Schema::builder();
    let key = b.add_bytes_field("_key", INDEXED | FAST);
    let pk = b.add_bytes_field("_pk", FAST);
    let seq = b.add_u64_field("_seq", FAST);
    let hash = b.add_u64_field("_h", FAST);
    let terms = b.add_text_field("_t", STORED);
    let opts = TextOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer("raw")
            .set_fieldnorms(false)
            .set_index_option(IndexRecordOption::WithFreqsAndPositions),
    );
    let w = b.add_text_field("w", opts);
    let dopts = TextOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer("raw")
            .set_fieldnorms(false)
            .set_index_option(IndexRecordOption::Basic),
    );
    let dead = b.add_text_field("d", dopts);
    (
        b.build(),
        Fields {
            key,
            pk,
            seq,
            hash,
            terms,
            w,
            dead,
        },
    )
}

/// A row version's identity in the index: its words' hash and its
/// distinct searchable terms.
type Version = (u64, Vec<String>);

/// What changed since the last commit (the reader doesn't see it yet).
#[derive(Debug, Default)]
struct Pending {
    /// Each key written: its version now (`None`: deleted).
    rows: HashMap<Vec<u8>, Option<Version>>,
    /// Every row was deleted.
    cleared: bool,
}

/// One index's files.
pub struct FtStore {
    index: Index,
    writer: Mutex<IndexWriter<TantivyDocument>>,
    reader: IndexReader,
    pending: Mutex<Pending>,
    f: Fields,
}

impl std::fmt::Debug for FtStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FtStore").finish_non_exhaustive()
    }
}

/// A hit: the row's key, its primary key values (as stored), its score and
/// age.
#[derive(Debug, Clone, PartialEq)]
pub struct Hit {
    /// `_key`.
    pub key: Vec<u8>,
    /// `_pk`.
    pub pk: Vec<u8>,
    /// The score.
    pub score: f32,
    /// `_seq`.
    pub seq: u64,
}

/// FNV-1a over the words and positions: stable across builds.
fn hash_of(toks: &[Tok]) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    let mut eat = |b: &[u8]| {
        for x in b {
            h ^= u64::from(*x);
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
    };
    for t in toks {
        eat(t.term.as_bytes());
        eat(&[0]);
        eat(&t.pos.to_le_bytes());
    }
    h
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl FtStore {
    /// Opens the index in `dir` (created when missing), or in memory when
    /// `dir` is `None`.
    ///
    /// # Errors
    /// Tantivy's (a directory holding another schema, I/O).
    pub fn open(dir: Option<&Path>, writer_bytes: usize) -> Result<Self> {
        let (schema, f) = schema();
        let index = match dir {
            None => Index::create_in_ram(schema),
            Some(d) => {
                std::fs::create_dir_all(d).map_err(|e| terr("directory", e))?;
                let md = MmapDirectory::open(d).map_err(|e| terr("directory", e))?;
                Index::open_or_create(md, schema).map_err(|e| terr("open", e))?
            }
        };
        let writer = index
            .writer_with_num_threads(1, writer_bytes.max(MIN_WRITER_BYTES))
            .map_err(|e| terr("writer", e))?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .map_err(|e| terr("reader", e))?;
        Ok(Self {
            index,
            writer: Mutex::new(writer),
            reader,
            pending: Mutex::new(Pending::default()),
            f,
        })
    }

    fn w(&self) -> MutexGuard<'_, IndexWriter<TantivyDocument>> {
        lock(&self.writer)
    }

    /// The last log index the committed files cover (`None`: never
    /// committed).
    ///
    /// # Errors
    /// Tantivy's.
    pub fn applied(&self) -> Result<Option<u64>> {
        let metas = self.index.load_metas().map_err(|e| terr("metas", e))?;
        Ok(metas.payload.and_then(|p| p.trim().parse::<u64>().ok()))
    }

    /// The version of the row of `key` the index holds (pending changes
    /// first, then the last commit).
    fn version_of(&self, p: &Pending, key: &[u8]) -> Option<Version> {
        if let Some(v) = p.rows.get(key) {
            return v.clone();
        }
        if p.cleared {
            return None;
        }
        let searcher = self.reader.searcher();
        let t = Term::from_field_bytes(self.f.key, key);
        for (si, seg) in searcher.segment_readers().iter().enumerate() {
            let Ok(inv) = seg.inverted_index(self.f.key) else {
                continue;
            };
            let Ok(Some(mut post)) = inv.read_postings(&t, IndexRecordOption::Basic) else {
                continue;
            };
            let mut doc = post.doc();
            while doc != TERMINATED {
                if !seg.is_deleted(doc) {
                    let h = seg
                        .fast_fields()
                        .u64("_h")
                        .ok()
                        .and_then(|c| c.first(doc))
                        .unwrap_or(0);
                    let terms = searcher
                        .doc::<TantivyDocument>(DocAddress::new(
                            u32::try_from(si).unwrap_or(u32::MAX),
                            doc,
                        ))
                        .ok()
                        .and_then(|d| {
                            d.get_first(self.f.terms)
                                .and_then(|v| v.as_str().map(str::to_owned))
                        })
                        .unwrap_or_default();
                    return Some((
                        h,
                        terms
                            .split(' ')
                            .filter(|x| !x.is_empty())
                            .map(str::to_owned)
                            .collect(),
                    ));
                }
                doc = post.advance();
            }
        }
        None
    }

    /// A tombstone of a replaced or deleted version.
    fn tombstone(&self, w: &IndexWriter<TantivyDocument>, terms: &[String]) -> Result<()> {
        let mut d = TantivyDocument::new();
        let mut toks: Vec<Token> = Vec::with_capacity(terms.len() + 1);
        for (i, t) in std::iter::once(TOMB)
            .chain(terms.iter().map(String::as_str))
            .enumerate()
        {
            toks.push(Token {
                offset_from: 0,
                offset_to: 0,
                position: i,
                text: t.to_owned(),
                position_length: 1,
            });
        }
        d.add_pre_tokenized_text(
            self.f.dead,
            PreTokenizedString {
                text: String::new(),
                tokens: toks,
            },
        );
        w.add_document(d).map_err(|e| terr("add", e))?;
        Ok(())
    }

    /// Replaces the row of `key` (pending until the next commit). A row
    /// whose words didn't change keeps its document (and its age).
    ///
    /// # Errors
    /// Tantivy's.
    pub fn upsert(&self, key: &[u8], pk: &[u8], seq: u64, toks: Vec<Tok>) -> Result<()> {
        let h = hash_of(&toks);
        let mut p = lock(&self.pending);
        let old = self.version_of(&p, key);
        if old.as_ref().is_some_and(|(oh, _)| *oh == h) {
            return Ok(());
        }
        let mut distinct: Vec<String> = toks
            .iter()
            .filter(|t| !t.term.starts_with(UNINDEXED))
            .map(|t| t.term.clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        distinct.sort_unstable();
        let mut d = TantivyDocument::new();
        d.add_bytes(self.f.key, key);
        d.add_bytes(self.f.pk, pk);
        d.add_u64(self.f.seq, seq);
        d.add_u64(self.f.hash, h);
        d.add_text(self.f.terms, distinct.join(" "));
        let tokens = toks
            .into_iter()
            .map(|t| Token {
                offset_from: 0,
                offset_to: 0,
                position: t.pos as usize,
                text: t.term,
                position_length: 1,
            })
            .collect();
        d.add_pre_tokenized_text(
            self.f.w,
            PreTokenizedString {
                text: String::new(),
                tokens,
            },
        );
        let w = self.w();
        if let Some((_, terms)) = &old {
            self.tombstone(&w, terms)?;
        }
        w.delete_term(Term::from_field_bytes(self.f.key, key));
        w.add_document(d).map_err(|e| terr("add", e))?;
        p.rows.insert(key.to_vec(), Some((h, distinct)));
        Ok(())
    }

    /// Deletes the row of `key` (pending until the next commit).
    ///
    /// # Errors
    /// Tantivy's.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let mut p = lock(&self.pending);
        let old = self.version_of(&p, key);
        let w = self.w();
        if let Some((_, terms)) = &old {
            self.tombstone(&w, terms)?;
        }
        w.delete_term(Term::from_field_bytes(self.f.key, key));
        p.rows.insert(key.to_vec(), None);
        Ok(())
    }

    /// Deletes every row and tombstone (pending until the next commit): a
    /// table emptied starts a new index, as `InnoDB`'s does.
    ///
    /// # Errors
    /// Tantivy's.
    pub fn clear(&self) -> Result<()> {
        let mut p = lock(&self.pending);
        self.w()
            .delete_all_documents()
            .map_err(|e| terr("clear", e))?;
        p.rows.clear();
        p.cleared = true;
        Ok(())
    }

    /// Commits what is pending as covering log index `applied`, and makes
    /// it searchable.
    ///
    /// # Errors
    /// Tantivy's.
    pub fn commit(&self, applied: u64) -> Result<()> {
        let mut p = lock(&self.pending);
        {
            let mut w = self.w();
            let mut c = w.prepare_commit().map_err(|e| terr("prepare", e))?;
            c.set_payload(&applied.to_string());
            c.commit().map_err(|e| terr("commit", e))?;
        }
        self.reader.reload().map_err(|e| terr("reload", e))?;
        *p = Pending::default();
        Ok(())
    }

    /// The last commit, to search.
    #[must_use]
    pub fn snapshot(&self) -> FtSnapshot {
        FtSnapshot {
            searcher: self.reader.searcher(),
            f: self.f,
        }
    }

    /// Waits for background merges (tests, and before a directory is
    /// removed).
    ///
    /// # Errors
    /// Tantivy's.
    pub fn wait_merges(self) -> Result<()> {
        let w = self
            .writer
            .into_inner()
            .unwrap_or_else(PoisonError::into_inner);
        w.wait_merging_threads().map_err(|e| terr("merges", e))
    }
}

/// One commit of an index, searchable.
pub struct FtSnapshot {
    searcher: Searcher,
    f: Fields,
}

fn split(d: DocId) -> (usize, u32) {
    #[allow(clippy::cast_possible_truncation)]
    ((d >> 32) as usize, d as u32)
}

fn join(seg: usize, doc: u32) -> DocId {
    ((seg as u64) << 32) | u64::from(doc)
}

impl FtSnapshot {
    /// The live documents of `term`: (document, frequency, positions when
    /// `positions` lists the documents wanted).
    fn read(&self, term: &str, positions: Option<&[DocId]>) -> Vec<(DocId, u32, Vec<u32>)> {
        let t = Term::from_field_text(self.f.w, term);
        let opt = if positions.is_some() {
            IndexRecordOption::WithFreqsAndPositions
        } else {
            IndexRecordOption::WithFreqs
        };
        let mut out = Vec::new();
        for (si, seg) in self.searcher.segment_readers().iter().enumerate() {
            let Ok(inv) = seg.inverted_index(self.f.w) else {
                continue;
            };
            let Ok(Some(mut p)) = inv.read_postings(&t, opt) else {
                continue;
            };
            let alive = seg.alive_bitset();
            match positions {
                None => {
                    let mut doc = p.doc();
                    while doc != TERMINATED {
                        if alive.is_none_or(|a| a.is_alive(doc)) {
                            out.push((join(si, doc), p.term_freq(), Vec::new()));
                        }
                        doc = p.advance();
                    }
                }
                Some(want) => {
                    for &d in want {
                        let (s, doc) = split(d);
                        if s != si || p.doc() > doc {
                            continue;
                        }
                        if p.seek(doc) == doc {
                            let mut v = Vec::new();
                            p.positions(&mut v);
                            out.push((d, p.term_freq(), v));
                        }
                    }
                }
            }
        }
        out
    }

    /// Tombstones holding `term`.
    fn dead_df(&self, term: &str) -> usize {
        self.searcher
            .doc_freq(&Term::from_field_text(self.f.dead, term))
            .map_or(0, |n| usize::try_from(n).unwrap_or(usize::MAX))
    }

    /// The hits of `ranked` (documents and scores), with their keys.
    #[must_use]
    pub fn hits(&self, ranked: &[(DocId, f32)]) -> Vec<Hit> {
        let mut by_seg: HashMap<usize, Vec<(u32, f32)>> = HashMap::new();
        for &(d, s) in ranked {
            let (seg, doc) = split(d);
            by_seg.entry(seg).or_default().push((doc, s));
        }
        let mut out = Vec::with_capacity(ranked.len());
        let segs = self.searcher.segment_readers();
        for (seg, docs) in by_seg {
            let Some(r) = segs.get(seg) else {
                continue;
            };
            let ff = r.fast_fields();
            let (Ok(Some(key)), Ok(Some(pk)), Ok(seq)) =
                (ff.bytes("_key"), ff.bytes("_pk"), ff.u64("_seq"))
            else {
                continue;
            };
            for (doc, score) in docs {
                let mut k = Vec::new();
                let mut p = Vec::new();
                if let Some(o) = key.term_ords(doc).next() {
                    let _ = key.ord_to_bytes(o, &mut k);
                }
                if let Some(o) = pk.term_ords(doc).next() {
                    let _ = pk.ord_to_bytes(o, &mut p);
                }
                out.push(Hit {
                    key: k,
                    pk: p,
                    score,
                    seq: seq.first(doc).unwrap_or(0),
                });
            }
        }
        out
    }

    /// Every live row's key, in no order (tests and checks).
    #[must_use]
    pub fn keys(&self) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        for r in self.searcher.segment_readers() {
            let Ok(Some(key)) = r.fast_fields().bytes("_key") else {
                continue;
            };
            for doc in 0..r.max_doc() {
                if r.is_deleted(doc) {
                    continue;
                }
                let mut k = Vec::new();
                if let Some(o) = key.term_ords(doc).next() {
                    let _ = key.ord_to_bytes(o, &mut k);
                }
                if !k.is_empty() {
                    out.push(k);
                }
            }
        }
        out
    }

    /// The terms of field `f` starting with `prefix`, searchable ones only.
    fn expansions(&self, f: Field, prefix: &str) -> Vec<String> {
        let lo = prefix.as_bytes().to_vec();
        let hi = crate::innodb::prefix_end(&lo);
        let mut out = std::collections::BTreeSet::new();
        for seg in self.searcher.segment_readers() {
            let Ok(inv) = seg.inverted_index(f) else {
                continue;
            };
            let mut b = inv.terms().range().ge(&lo);
            if let Some(h) = &hi {
                b = b.lt(h);
            }
            let Ok(mut st) = b.into_stream() else {
                continue;
            };
            while st.advance() {
                let k = st.key();
                if k.first() == Some(&(UNINDEXED as u8)) || k == TOMB.as_bytes() {
                    continue;
                }
                if let Ok(s) = std::str::from_utf8(k) {
                    out.insert(s.to_owned());
                }
            }
        }
        out.into_iter().collect()
    }
}

impl Postings for FtSnapshot {
    fn num_docs(&self) -> u64 {
        // Tombstones are documents too.
        self.searcher
            .num_docs()
            .saturating_sub(self.dead_df(TOMB) as u64)
    }

    fn docs(&self, term: &str) -> Vec<(DocId, u32)> {
        self.read(term, None)
            .into_iter()
            .map(|(d, tf, _)| (d, tf))
            .collect()
    }

    fn raw_df(&self, term: &str) -> usize {
        self.read(term, None).len() + self.dead_df(term)
    }

    fn prefix_docs(&self, prefix: &str) -> Prefixed {
        let mut out = Prefixed::default();
        // Per document: the first position of any expansion, and that
        // expansion's count.
        let mut first: BTreeMap<DocId, (u32, u32)> = BTreeMap::new();
        let live = self.expansions(self.f.w, prefix);
        for t in &live {
            let docs: Vec<DocId> = self.docs(t).into_iter().map(|(d, _)| d).collect();
            out.df += docs.len();
            out.raw_df += docs.len() + self.dead_df(t);
            for (d, tf, pos) in self.read(t, Some(&docs)) {
                let p0 = pos.first().copied().unwrap_or(u32::MAX);
                let e = first.entry(d).or_insert((p0, tf));
                if p0 < e.0 {
                    *e = (p0, tf);
                }
            }
        }
        for t in self.expansions(self.f.dead, prefix) {
            if live.binary_search(&t).is_err() {
                out.raw_df += self.dead_df(&t);
            }
        }
        out.docs = first.into_iter().map(|(d, (_, tf))| (d, tf)).collect();
        out
    }

    fn positions(&self, term: &str, docs: &[DocId]) -> HashMap<DocId, Vec<u32>> {
        let mut sorted = docs.to_vec();
        sorted.sort_unstable();
        self.read(term, Some(&sorted))
            .into_iter()
            .map(|(d, _, p)| (d, p))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::innodb::{Analyzer, LowerFold, Rules, parse_boolean, parse_natural, rank};

    fn toks(a: &Analyzer, t: &str) -> Vec<Tok> {
        let mut v = Vec::new();
        a.tokens(t, 0, &mut v);
        v
    }

    #[test]
    fn same_answers_as_memory() {
        let a = Analyzer::new(Rules::default(), Arc::new(LowerFold));
        let st = FtStore::open(None, MIN_WRITER_BYTES).unwrap_or_else(|e| panic!("{e}"));
        let rows = [
            "mysql database tutorial",
            "mysql mysql performance tuning",
            "postgres database guide",
            "database database database",
            "redis cache tutorial",
            "the quick brown fox",
            "mysql security",
            "database security guide tutorial",
        ];
        for (i, t) in rows.iter().enumerate() {
            let k = [u8::try_from(i).unwrap_or(0)];
            st.upsert(&k, &k, 2 + i as u64, toks(&a, t))
                .unwrap_or_else(|e| panic!("{e}"));
        }
        st.commit(5).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(st.applied().unwrap_or_else(|e| panic!("{e}")), Some(5));
        let snap = st.snapshot();
        assert_eq!(snap.num_docs(), 8);
        let score_of = |snap: &FtSnapshot, q: &crate::innodb::Node| -> Vec<(u8, f32)> {
            let r = rank(q, snap);
            let mut h: Vec<(u8, f32)> = snap
                .hits(&r)
                .into_iter()
                .map(|h| (h.key[0], h.score))
                .collect();
            h.sort_by_key(|x| x.0);
            h
        };
        let q = parse_natural("mysql", &a);
        assert_eq!(
            score_of(&snap, &q),
            [(0, 0.181_449_35), (1, 0.362_898_7), (6, 0.181_449_35)]
        );
        let q = parse_boolean("tu* \"database tutorial\"", &a).unwrap_or_else(|e| panic!("{e}"));
        let got = score_of(&snap, &q);
        assert_eq!(got.iter().map(|x| x.0).collect::<Vec<_>>(), [0, 1, 4, 7]);
        let q = parse_boolean("\"guide tutorial\"", &a).unwrap_or_else(|e| panic!("{e}"));
        assert_eq!(
            score_of(&snap, &q).iter().map(|x| x.0).collect::<Vec<_>>(),
            [7]
        );
        // An extra row, then deleted; a row rewritten with the same words
        // (no new version); one rewritten with others (a tombstone).
        st.upsert(&[99], &[99], 99, toks(&a, "mysql"))
            .unwrap_or_else(|e| panic!("{e}"));
        st.commit(6).unwrap_or_else(|e| panic!("{e}"));
        st.delete(&[99]).unwrap_or_else(|e| panic!("{e}"));
        st.upsert(&[0], &[0], 100, toks(&a, rows[0]))
            .unwrap_or_else(|e| panic!("{e}"));
        st.upsert(&[6], &[6], 101, toks(&a, "postgres security"))
            .unwrap_or_else(|e| panic!("{e}"));
        st.commit(7).unwrap_or_else(|e| panic!("{e}"));
        let snap = st.snapshot();
        assert_eq!(snap.num_docs(), 8);
        // Live: mysql in 0, 1. Raw: also 99's and 6's old versions.
        assert_eq!(snap.docs(&a.term("mysql").0).len(), 2);
        assert_eq!(snap.raw_df(&a.term("mysql").0), 4);
        let h = snap.hits(&rank(&parse_natural("mysql", &a), &snap));
        assert_eq!(h.iter().find(|h| h.key == [0]).map(|h| h.seq), Some(2));
    }
}
