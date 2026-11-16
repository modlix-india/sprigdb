//! A search index's partitions, and searching them with global BM25.
//!
//! ```text
//! search(query, k):
//!   build the query once (fuzzy and prefix terms expanded over every partition)
//!   global: N := Σ N_p, T[f] := Σ T_p[f], n[t] := Σ n_p[t] over the partitions
//!   each partition scores its top k through a statistics provider returning
//!     the global values
//!   merge by (score desc, partition, key)
//! matched: N, T[f] and n[t] of the documents the query matches only (a
//!   caller bound by row-level security searches within its visible hits)
//! plain: N = 1, n[t] = 1, T[f] = PLAIN_AVERAGE_LENGTH: no statistics of
//!   the index at all
//! expansions: a prefix or fuzzy term becomes the most frequent
//!   max_expansions terms it matches, counted over every document, or,
//!   when the query is restricted to keys, over those keys' documents
//!   only (a bound caller's terms depend on no row it can't read)
//! every(query, max): every hit with every term each prefix or fuzzy
//!   term matches (the candidates a bound caller re-reads)
//! ```
//!
//! Each document is in exactly one partition, so the sums are the
//! statistics one unsharded index would have, and a document scores the
//! same in any partition. **Partitions here are key-hash buckets**, not
//! the object's shards: every copy holds all partitions of its index,
//! so a split or a move of a shard changes nothing in the index; the feed carries the rows.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Mutex;

use sprig_common::{Result, SprigCode, SprigError};
use tantivy::collector::{Count, DocSetCollector, TopDocs};
use tantivy::query::{Bm25StatisticsProvider, BooleanQuery, Occur, Query, TermQuery, TermSetQuery};
use tantivy::schema::{Field, IndexRecordOption, Value};
use tantivy::{DocAddress, Searcher, TantivyDocument, Term};

use crate::build::{Builder, Expander, Expansion, TermBase};
use crate::partition::Partition;
use crate::query::SearchQuery;
use crate::schema::{IndexSchema, Key, Record};

/// How scores are computed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Scoring {
    /// Statistics summed over every partition (`search.scoring = global`).
    Global,
    /// Each partition's own statistics (`local`).
    Local,
    /// Statistics of the documents the query matches, and of no other: a
    /// caller that row-level security binds searches within the hits it
    /// may see, so its scores depend on nothing it can't read.
    Matched,
    /// No statistics of the index: every term weighs the same, and field
    /// lengths compare with [`PLAIN_AVERAGE_LENGTH`]. A bound caller's
    /// search that matches more than the gateway rechecks.
    Plain,
}

/// The average field length, in tokens, that [`Scoring::Plain`] assumes.
pub const PLAIN_AVERAGE_LENGTH: u64 = 16;

/// One hit.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Hit {
    /// BM25 score.
    pub score: f32,
    /// The partition.
    pub part: u32,
    /// The key.
    pub key: Key,
}

fn order(a: &Hit, b: &Hit) -> std::cmp::Ordering {
    b.score
        .total_cmp(&a.score)
        .then(a.part.cmp(&b.part))
        .then_with(|| a.key.cmp(&b.key))
}

fn terr(e: impl std::fmt::Display) -> SprigError {
    SprigError::internal(format!("search: {e}"))
}

/// FNV-1a, for placing keys in partitions.
#[must_use]
pub fn key_hash(k: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in k {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    h
}

/// Statistics of one partition for a query's terms.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PartStats {
    /// Documents as Tantivy's BM25 counts them: deleted ones too, until a
    /// merge drops them, as `doc_freq` counts them (with live documents
    /// only, a copy holding deleted versions broke Tantivy's
    /// `doc_count >= doc_freq` assertion).
    pub num_docs: u64,
    /// Tokens per field.
    pub tokens: HashMap<Field, u64>,
    /// Documents per term.
    pub doc_freq: HashMap<Term, u64>,
}

/// Statistics summed over partitions, served to Tantivy's BM25.
#[derive(Debug, Default)]
pub struct GlobalStats {
    sum: PartStats,
    /// Searchers for anything the query's terms didn't list.
    searchers: Vec<Searcher>,
    extra: Mutex<HashMap<Term, u64>>,
}

impl GlobalStats {
    /// The sum of `parts`; `searchers` answer what they don't hold.
    #[must_use]
    pub fn sum(parts: &[PartStats], searchers: Vec<Searcher>) -> Self {
        let mut s = PartStats::default();
        for p in parts {
            s.num_docs += p.num_docs;
            for (f, n) in &p.tokens {
                *s.tokens.entry(*f).or_default() += n;
            }
            for (t, n) in &p.doc_freq {
                *s.doc_freq.entry(t.clone()).or_default() += n;
            }
        }
        Self {
            sum: s,
            searchers,
            extra: Mutex::new(HashMap::new()),
        }
    }

    /// The summed statistics.
    #[must_use]
    pub fn stats(&self) -> &PartStats {
        &self.sum
    }
}

impl Bm25StatisticsProvider for GlobalStats {
    fn total_num_tokens(&self, field: Field) -> tantivy::Result<u64> {
        if let Some(n) = self.sum.tokens.get(&field) {
            return Ok(*n);
        }
        let mut n = 0;
        for s in &self.searchers {
            n += s.total_num_tokens(field)?;
        }
        Ok(n)
    }

    fn total_num_docs(&self) -> tantivy::Result<u64> {
        Ok(self.sum.num_docs)
    }

    fn doc_freq(&self, term: &Term) -> tantivy::Result<u64> {
        if let Some(n) = self.sum.doc_freq.get(term) {
            return Ok(*n);
        }
        let mut cache = self
            .extra
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(n) = cache.get(term) {
            return Ok(*n);
        }
        let mut n = 0;
        for s in &self.searchers {
            n += s.doc_freq(term)?;
        }
        cache.insert(term.clone(), n);
        Ok(n)
    }
}

/// BM25 statistics of the documents `query` matches ([`Scoring::Matched`]):
/// their number, their tokens per field (from the field norms) and, per
/// term, how many of them hold it. Terms and fields are counted when
/// Tantivy first asks for them.
struct MatchedStats {
    searchers: Vec<Searcher>,
    query: Box<dyn Query>,
    /// The matched documents of each searcher, by segment.
    docs: Vec<Vec<DocAddress>>,
    num_docs: u64,
    tokens: Mutex<HashMap<Field, u64>>,
    freq: Mutex<HashMap<Term, u64>>,
}

impl MatchedStats {
    fn new(searchers: &[Searcher], query: &dyn Query) -> Result<Self> {
        let mut docs = Vec::with_capacity(searchers.len());
        let mut num_docs = 0u64;
        for s in searchers {
            let mut d: Vec<DocAddress> = s
                .search(query, &DocSetCollector)
                .map_err(terr)?
                .into_iter()
                .collect();
            d.sort_unstable();
            num_docs += d.len() as u64;
            docs.push(d);
        }
        Ok(Self {
            searchers: searchers.to_vec(),
            query: query.box_clone(),
            docs,
            num_docs,
            tokens: Mutex::new(HashMap::new()),
            freq: Mutex::new(HashMap::new()),
        })
    }
}

impl Bm25StatisticsProvider for MatchedStats {
    fn total_num_tokens(&self, field: Field) -> tantivy::Result<u64> {
        let mut cache = self
            .tokens
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(n) = cache.get(&field) {
            return Ok(*n);
        }
        let mut n = 0u64;
        for (s, docs) in self.searchers.iter().zip(&self.docs) {
            let mut norms: Option<(u32, tantivy::fieldnorm::FieldNormReader)> = None;
            for a in docs {
                let reader = match &norms {
                    Some((seg, r)) if *seg == a.segment_ord => r,
                    _ => {
                        // A field without norms: one token per document.
                        let Ok(r) = s.segment_reader(a.segment_ord).get_fieldnorms_reader(field)
                        else {
                            n += 1;
                            continue;
                        };
                        &norms.insert((a.segment_ord, r)).1
                    }
                };
                n += u64::from(reader.fieldnorm(a.doc_id));
            }
        }
        cache.insert(field, n);
        Ok(n)
    }

    fn total_num_docs(&self) -> tantivy::Result<u64> {
        // Never 0: BM25 divides by it.
        Ok(self.num_docs.max(1))
    }

    fn doc_freq(&self, term: &Term) -> tantivy::Result<u64> {
        let mut cache = self
            .freq
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(n) = cache.get(term) {
            return Ok(*n);
        }
        let q = BooleanQuery::new(vec![
            (Occur::Must, self.query.box_clone()),
            (
                Occur::Must,
                Box::new(TermQuery::new(term.clone(), IndexRecordOption::Basic)),
            ),
        ]);
        let mut n = 0u64;
        for s in &self.searchers {
            n += s.search(&q, &Count)? as u64;
        }
        cache.insert(term.clone(), n);
        Ok(n)
    }
}

/// [`Scoring::Plain`]'s statistics: one document holding every term, of
/// [`PLAIN_AVERAGE_LENGTH`] tokens in every field.
struct PlainStats;

impl Bm25StatisticsProvider for PlainStats {
    fn total_num_tokens(&self, _field: Field) -> tantivy::Result<u64> {
        Ok(PLAIN_AVERAGE_LENGTH)
    }

    fn total_num_docs(&self) -> tantivy::Result<u64> {
        Ok(1)
    }

    fn doc_freq(&self, _term: &Term) -> tantivy::Result<u64> {
        Ok(1)
    }
}

/// A partition's statistics for `query`'s terms (the `Stats` RPC).
///
/// # Errors
/// Tantivy's.
pub fn part_stats(searcher: &Searcher, query: &dyn Query) -> Result<PartStats> {
    let mut terms: Vec<Term> = Vec::new();
    query.query_terms(&mut |t, _| terms.push(t.clone()));
    let mut s = PartStats {
        num_docs: searcher
            .segment_readers()
            .iter()
            .map(|r| u64::from(r.max_doc()))
            .sum(),
        ..PartStats::default()
    };
    for t in terms {
        let f = t.field();
        if let std::collections::hash_map::Entry::Vacant(e) = s.tokens.entry(f) {
            e.insert(searcher.total_num_tokens(f).map_err(terr)?);
        }
        let n = searcher.doc_freq(&t).map_err(terr)?;
        s.doc_freq.insert(t, n);
    }
    Ok(s)
}

/// Bounded Levenshtein distance (`None` above `max`).
fn within(a: &str, b: &str, max: usize) -> bool {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    if a.len().abs_diff(b.len()) > max {
        return false;
    }
    let mut prev: Vec<usize> = (0..=b.len()).collect();
    for (i, ca) in a.iter().enumerate() {
        let mut cur = vec![i + 1; b.len() + 1];
        let mut best = cur[0];
        for (j, cb) in b.iter().enumerate() {
            let cost = usize::from(ca != cb);
            cur[j + 1] = (prev[j] + cost).min(prev[j + 1] + 1).min(cur[j] + 1);
            best = best.min(cur[j + 1]);
        }
        if best > max {
            return false;
        }
        prev = cur;
    }
    prev[b.len()] <= max
}

/// The documents of each searcher, by segment, ascending.
type DocsBySegment = Vec<BTreeMap<u32, Vec<u32>>>;

/// Expands over a set of searchers (every partition of the index), or
/// over some documents of them only.
struct Over<'a> {
    searchers: &'a [Searcher],
    /// Count only these documents: a query restricted to keys (a caller
    /// bound by row-level security) chooses its terms among the
    /// documents it may see.
    within: Option<DocsBySegment>,
}

/// How many of `ids` (ascending) hold the term of `info`.
fn freq_within(
    inv: &tantivy::InvertedIndexReader,
    info: &tantivy::postings::TermInfo,
    ids: &[u32],
) -> Result<u64> {
    use tantivy::{DocSet, TERMINATED};
    let mut p = inv
        .read_postings_from_terminfo(info, IndexRecordOption::Basic)
        .map_err(terr)?;
    let mut n = 0u64;
    if (info.doc_freq as usize) <= ids.len() {
        // Walk the postings; look each document up.
        let mut d = p.doc();
        while d != TERMINATED {
            if ids.binary_search(&d).is_ok() {
                n += 1;
            }
            d = p.advance();
        }
    } else {
        // Seek each document in the postings (only forward: a seek past
        // one document may land beyond the next).
        for &id in ids {
            let mut d = p.doc();
            if d < id {
                d = p.seek(id);
            }
            if d == TERMINATED {
                break;
            }
            if d == id {
                n += 1;
            }
        }
    }
    Ok(n)
}

impl Expander for Over<'_> {
    fn expand(&self, base: &TermBase, how: &Expansion, max: usize) -> Result<Vec<String>> {
        let pre = base.value_prefix();
        let mut found: BTreeMap<String, u64> = BTreeMap::new();
        for (i, s) in self.searchers.iter().enumerate() {
            for (ord, seg) in s.segment_readers().iter().enumerate() {
                let ids = match &self.within {
                    None => None,
                    Some(w) => match w
                        .get(i)
                        .zip(u32::try_from(ord).ok())
                        .and_then(|(m, o)| m.get(&o))
                    {
                        Some(ids) if !ids.is_empty() => Some(ids.as_slice()),
                        _ => continue,
                    },
                };
                let inv = seg.inverted_index(base.field).map_err(terr)?;
                let dict = inv.terms();
                let start: Vec<u8> = match how {
                    Expansion::Prefix(p) => [pre.as_slice(), p.as_bytes()].concat(),
                    Expansion::Fuzzy(..) => pre.clone(),
                };
                let mut st = dict.range().ge(&start).into_stream().map_err(terr)?;
                while st.advance() {
                    let k = st.key();
                    if !k.starts_with(&pre) {
                        break;
                    }
                    let Ok(text) = std::str::from_utf8(&k[pre.len()..]) else {
                        continue;
                    };
                    let keep = match how {
                        Expansion::Prefix(p) => {
                            if !text.starts_with(p.as_str()) {
                                break;
                            }
                            true
                        }
                        Expansion::Fuzzy(w, d) => within(w, text, usize::from(*d)),
                    };
                    if keep {
                        let n = match ids {
                            None => u64::from(st.value().doc_freq),
                            Some(ids) => freq_within(&inv, st.value(), ids)?,
                        };
                        if n > 0 {
                            *found.entry(text.to_owned()).or_default() += n;
                        }
                    }
                }
            }
        }
        // The most frequent terms, as Lucene's top-terms rewrite keeps.
        let mut v: Vec<(String, u64)> = found.into_iter().collect();
        v.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        v.truncate(max.max(1));
        let mut out: Vec<String> = v.into_iter().map(|(t, _)| t).collect();
        out.sort();
        Ok(out)
    }
}

/// The key sets `q` is restricted to: `Keys` among the clauses every
/// match must meet (`must` and `filter` of `Bool`s, all the way down).
fn key_sets<'q>(q: &'q SearchQuery, out: &mut Vec<&'q [Key]>) {
    match q {
        SearchQuery::Keys(k) => out.push(k),
        SearchQuery::Bool { must, filter, .. } => {
            for x in must.iter().chain(filter) {
                key_sets(x, out);
            }
        }
        _ => {}
    }
}

/// The refusal of a search matching more than `max` documents, without
/// the count (it would include rows the caller may not see).
fn window_exceeded(max: usize) -> SprigError {
    SprigError::new(
        SprigCode::SearchWindowExceeded,
        format!("the search matched more than search.max_window ({max}) rows"),
    )
}

/// A search index's partitions.
#[derive(Debug)]
pub struct SearchIndex {
    /// The schema.
    pub schema: IndexSchema,
    parts: Vec<Partition>,
}

/// One facet bucket.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Bucket {
    /// The value (keyword) or the lower boundary (numbers).
    pub value: serde_json::Value,
    /// Matching documents.
    pub count: u64,
}

impl SearchIndex {
    /// Opens (or creates) `partitions` partitions under `dir`
    /// (`p<n>` subdirectories), or in memory.
    ///
    /// # Errors
    /// Tantivy's.
    pub fn open(schema: IndexSchema, dir: Option<&Path>, writer_bytes: usize) -> Result<Self> {
        let n = schema.def.partitions.max(1);
        let mut parts = Vec::new();
        for i in 0..n {
            let d = dir.map(|d| d.join(format!("p{i}")));
            parts.push(Partition::open(i, &schema, d.as_deref(), writer_bytes)?);
        }
        Ok(Self { schema, parts })
    }

    /// The partitions.
    #[must_use]
    pub fn partitions(&self) -> &[Partition] {
        &self.parts
    }

    /// The partition of a key.
    #[must_use]
    pub fn part_of(&self, key: &Key) -> usize {
        #[allow(clippy::cast_possible_truncation)]
        let i = (key_hash(&key.0) % self.parts.len() as u64) as usize;
        i
    }

    /// Indexes `r` (replacing the key's document).
    ///
    /// # Errors
    /// Tantivy's.
    pub fn upsert(&self, r: &Record) -> Result<()> {
        self.parts[self.part_of(r.key())].index(&self.schema, r)
    }

    /// Removes a key.
    pub fn delete(&self, key: &Key) {
        self.parts[self.part_of(key)].delete(key);
    }

    /// Removes everything.
    ///
    /// # Errors
    /// Tantivy's.
    pub fn delete_all(&self) -> Result<()> {
        for p in &self.parts {
            p.delete_all()?;
        }
        Ok(())
    }

    /// Commits every partition with `payload`.
    ///
    /// # Errors
    /// Tantivy's.
    pub fn commit(&self, payload: &str) -> Result<()> {
        for p in &self.parts {
            p.commit(payload)?;
        }
        Ok(())
    }

    /// The oldest payload of the partitions (where to resume), `None` if
    /// any partition has none.
    ///
    /// # Errors
    /// Tantivy's.
    pub fn payloads(&self) -> Result<Vec<Option<String>>> {
        self.parts.iter().map(Partition::payload).collect()
    }

    /// Live documents.
    #[must_use]
    pub fn num_docs(&self) -> u64 {
        self.parts.iter().map(Partition::num_docs).sum()
    }

    fn searchers(&self) -> Vec<Searcher> {
        self.parts.iter().map(Partition::searcher).collect()
    }

    /// The Tantivy query of `q` over these searchers.
    ///
    /// # Errors
    /// As [`Builder::build`].
    pub fn compile(
        &self,
        q: &SearchQuery,
        searchers: &[Searcher],
        max_expansions: usize,
    ) -> Result<Box<dyn Query>> {
        let ex = self.expander(q, searchers)?;
        Builder::new(&self.schema, &ex, max_expansions).build(q)
    }

    /// The expander of `q`: over every document, or, when `q` is
    /// restricted to keys, over those keys' documents only, so the
    /// prefix and fuzzy terms chosen depend on nothing else.
    fn expander<'s>(&self, q: &SearchQuery, searchers: &'s [Searcher]) -> Result<Over<'s>> {
        let mut sets = Vec::new();
        key_sets(q, &mut sets);
        let Some((first, rest)) = sets.split_first() else {
            return Ok(Over {
                searchers,
                within: None,
            });
        };
        let docs = |keys: &[Key]| -> Result<Vec<std::collections::HashSet<DocAddress>>> {
            let q = TermSetQuery::new(
                keys.iter()
                    .map(|k| Term::from_field_bytes(self.schema.key, &k.0)),
            );
            searchers
                .iter()
                .map(|s| s.search(&q, &DocSetCollector).map_err(terr))
                .collect()
        };
        let mut all = docs(first)?;
        for keys in rest {
            for (mine, other) in all.iter_mut().zip(docs(keys)?) {
                mine.retain(|a| other.contains(a));
            }
        }
        let within = all
            .into_iter()
            .map(|set| {
                let mut m: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
                for a in set {
                    m.entry(a.segment_ord).or_default().push(a.doc_id);
                }
                for ids in m.values_mut() {
                    ids.sort_unstable();
                }
                m
            })
            .collect();
        Ok(Over {
            searchers,
            within: Some(within),
        })
    }

    fn key_of(&self, s: &Searcher, a: DocAddress) -> Result<Key> {
        let d: TantivyDocument = s.doc(a).map_err(terr)?;
        d.get_first(self.schema.key)
            .and_then(|v| v.as_bytes())
            .map(|b| Key(b.to_vec()))
            .ok_or_else(|| SprigError::internal("a search document without _key"))
    }

    /// The best `k` hits.
    ///
    /// # Errors
    /// 39822 and the other errors of [`Builder::build`]; Tantivy's.
    pub fn top(
        &self,
        q: &SearchQuery,
        k: usize,
        scoring: Scoring,
        max_expansions: usize,
    ) -> Result<Vec<Hit>> {
        let searchers = self.searchers();
        let query = self.compile(q, &searchers, max_expansions)?;
        if k == 0 {
            return Ok(Vec::new());
        }
        let stats: Option<Box<dyn Bm25StatisticsProvider>> = match scoring {
            Scoring::Global => {
                let parts = searchers
                    .iter()
                    .map(|s| part_stats(s, query.as_ref()))
                    .collect::<Result<Vec<_>>>()?;
                Some(Box::new(GlobalStats::sum(&parts, searchers.clone())))
            }
            Scoring::Local => None,
            Scoring::Matched => Some(Box::new(MatchedStats::new(&searchers, query.as_ref())?)),
            Scoring::Plain => Some(Box::new(PlainStats)),
        };
        let mut hits = Vec::new();
        for (i, s) in searchers.iter().enumerate() {
            let c = TopDocs::with_limit(k).order_by_score();
            let top = match &stats {
                Some(g) => s.search_with_statistics_provider(query.as_ref(), &c, g.as_ref()),
                None => s.search(query.as_ref(), &c),
            }
            .map_err(terr)?;
            for (score, a) in top {
                hits.push(Hit {
                    score,
                    part: self.parts[i].no,
                    key: self.key_of(s, a)?,
                });
            }
        }
        hits.sort_by(order);
        hits.truncate(k);
        Ok(hits)
    }

    /// Every hit, up to `max` (`WHERE MATCH`); 39827 past it.
    ///
    /// # Errors
    /// As [`SearchIndex::top`]; 39827.
    pub fn all(&self, q: &SearchQuery, max: usize, max_expansions: usize) -> Result<Vec<Hit>> {
        let n = self.count(q, max_expansions)?;
        if n > max as u64 {
            return Err(window_exceeded(max));
        }
        #[allow(clippy::cast_possible_truncation)]
        self.top(q, n as usize, Scoring::Global, max_expansions)
    }

    /// Every hit, up to `max` (39827 past it), with each prefix and fuzzy
    /// term expanded to **every** term it matches rather than the most
    /// frequent `search.max_expansions`: the hits a caller bound by
    /// row-level security re-reads before its terms are chosen among the
    /// rows it may see. Scores are 0 and the order is the key order.
    ///
    /// # Errors
    /// As [`SearchIndex::all`].
    pub fn every(&self, q: &SearchQuery, max: usize) -> Result<Vec<Hit>> {
        let searchers = self.searchers();
        let ex = self.expander(q, &searchers)?;
        let query = Builder::new(&self.schema, &ex, usize::MAX)
            .every_term()
            .build(q)?;
        let mut n = 0u64;
        for s in &searchers {
            n += s.search(query.as_ref(), &Count).map_err(terr)? as u64;
        }
        if n > max as u64 {
            return Err(window_exceeded(max));
        }
        let mut hits = Vec::new();
        for (i, s) in searchers.iter().enumerate() {
            for a in s.search(query.as_ref(), &DocSetCollector).map_err(terr)? {
                hits.push(Hit {
                    score: 0.0,
                    part: self.parts[i].no,
                    key: self.key_of(s, a)?,
                });
            }
        }
        hits.sort_by(order);
        Ok(hits)
    }

    /// Matching documents.
    ///
    /// # Errors
    /// As [`SearchIndex::top`].
    pub fn count(&self, q: &SearchQuery, max_expansions: usize) -> Result<u64> {
        let searchers = self.searchers();
        let query = self.compile(q, &searchers, max_expansions)?;
        let mut n = 0u64;
        for s in &searchers {
            n += s.search(query.as_ref(), &Count).map_err(terr)? as u64;
        }
        Ok(n)
    }

    /// Facet counts of `field` over the documents matching `q`: keyword
    /// values (the top `limit`), or numbers in `boundaries` buckets.
    ///
    /// # Errors
    /// 39829 for a field that isn't a facet; Tantivy's.
    pub fn facet(
        &self,
        q: &SearchQuery,
        field: &str,
        limit: usize,
        boundaries: Option<&[f64]>,
        max_expansions: usize,
    ) -> Result<Vec<Bucket>> {
        let def = &self.schema.def;
        let want_num = boundaries.is_some();
        let f = def
            .fields
            .iter()
            .filter(|f| f.name == field && f.fast())
            .find(|f| {
                if want_num {
                    matches!(f.kind, crate::def::FieldKind::Number)
                } else {
                    matches!(f.kind, crate::def::FieldKind::Keyword)
                }
            })
            .ok_or_else(|| {
                SprigError::new(
                    SprigCode::SearchFieldType,
                    format!("'{field}' is not a facet field of this index"),
                )
            })?;
        let name = format!("f{}", f.field_no);
        let searchers = self.searchers();
        let query = self.compile(q, &searchers, max_expansions)?;
        let mut counts: BTreeMap<String, u64> = BTreeMap::new();
        let mut buckets: Vec<u64> = vec![0; boundaries.map_or(0, |b| b.len().saturating_sub(1))];
        for s in &searchers {
            let docs = s.search(query.as_ref(), &DocSetCollector).map_err(terr)?;
            let mut by_seg: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
            for a in docs {
                by_seg.entry(a.segment_ord).or_default().push(a.doc_id);
            }
            for (seg, ids) in by_seg {
                let ff = s.segment_reader(seg).fast_fields();
                if let Some(b) = boundaries {
                    let col = ff.f64(&name).map_err(terr)?;
                    for d in ids {
                        for v in col.values_for_doc(d) {
                            if let Some(i) = b.windows(2).position(|w| v >= w[0] && v < w[1]) {
                                buckets[i] += 1;
                            }
                        }
                    }
                } else if let Some(col) = ff.str(&name).map_err(terr)? {
                    let mut out = String::new();
                    for d in ids {
                        let ords: Vec<u64> = col.term_ords(d).collect();
                        for o in ords {
                            out.clear();
                            if col.ord_to_str(o, &mut out).map_err(terr)? {
                                *counts.entry(out.clone()).or_default() += 1;
                            }
                        }
                    }
                }
            }
        }
        if let Some(b) = boundaries {
            return Ok(b
                .iter()
                .zip(buckets)
                .map(|(lo, count)| Bucket {
                    value: serde_json::json!(lo),
                    count,
                })
                .collect());
        }
        let mut v: Vec<(String, u64)> = counts.into_iter().collect();
        v.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        v.truncate(limit);
        Ok(v.into_iter()
            .map(|(value, count)| Bucket {
                value: serde_json::Value::String(value),
                count,
            })
            .collect())
    }
}

#[cfg(test)]
mod tests;
