//! The docs index in memory: one release's pack, searched in process.
//!
//! Every node keeps the pack of its own release in memory and answers docs
//! searches, pages and the `sprig_docs` database from it. Nothing is
//! written anywhere, so the index is read-only by construction, and a new
//! release brings its own pack: the index is replaced when the node starts
//! with it.
//!
//! ```text
//! load    := Pack::from_bytes (the trailing SHA-256 checked, every chunk read)
//! keyword := Bm25 over heading path + text, identifiers as exact keys,
//!            the best CANDIDATES (audience-filtered after scoring)
//! vector  := exact cosine over every chunk's vector (a dot product: the
//!            vectors are unit length), the best CANDIDATES
//! hybrid  := RRF(keyword weighted keyword_weight(query), vector), k = 60,
//!            at most MAX_PER_PAGE per page, the first `limit`
//! ```
//!
//! An exact scan is cheap at this size (a few thousand chunks of 384
//! values, well under a millisecond), so the docs index needs no vector
//! index.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use crate::pack::{Chunk, DIM, Pack, PackError, pack_sha256};
use crate::search::{
    Bm25, CANDIDATES, MAX_PER_PAGE, RRF_K, cap_per_page, keyword_weight, rrf_weighted,
};

/// One fused hit: the chunk's position in [`DocsIndex::chunks`], its
/// score, and its ranks (from 1) in the keyword and vector lists.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Fused {
    /// The chunk.
    pub chunk: usize,
    /// The fused score.
    pub score: f64,
    /// Rank in the keyword list, if it is there.
    pub bm25_rank: Option<usize>,
    /// Rank in the vector list, if it is there.
    pub vector_rank: Option<usize>,
}

/// One release's docs, searchable in memory.
#[derive(Debug, Clone)]
pub struct DocsIndex {
    version: String,
    sha256: [u8; 32],
    model_id: String,
    model_sha256: [u8; 32],
    /// The chunks in `chunk_id` order, their embeddings moved to
    /// `vectors`.
    chunks: Vec<Chunk>,
    /// Every chunk's vector, `DIM` values each, in chunk order.
    vectors: Vec<f32>,
    bm25: Bm25,
    /// Each page's chunks, in `chunk_id` order.
    pages: BTreeMap<String, Vec<usize>>,
    by_id: HashMap<[u8; 16], usize>,
}

impl DocsIndex {
    /// Reads and indexes a pack file's bytes.
    ///
    /// # Errors
    /// The pack's ([`PackError`]): a changed or damaged file fails its
    /// checksum.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, PackError> {
        let pack = Pack::from_bytes(bytes)?;
        let sha = pack_sha256(bytes).ok_or(PackError::Truncated)?;
        Ok(Self::from_pack(pack, sha))
    }

    /// Reads and indexes the pack at `path`.
    ///
    /// # Errors
    /// The file can't be read, or isn't a good pack.
    pub fn load(path: &Path) -> Result<Self, String> {
        let bytes = std::fs::read(path).map_err(|e| format!("{}: {e}", path.display()))?;
        Self::from_bytes(&bytes).map_err(|e| format!("{}: {e}", path.display()))
    }

    /// Indexes `pack`, whose file's SHA-256 is `sha256`.
    #[must_use]
    pub fn from_pack(pack: Pack, sha256: [u8; 32]) -> Self {
        let bm25 = Bm25::for_chunks(&pack.chunks);
        let mut chunks = pack.chunks;
        let mut vectors = Vec::with_capacity(chunks.len() * DIM);
        let mut pages: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        let mut by_id = HashMap::with_capacity(chunks.len());
        for (i, c) in chunks.iter_mut().enumerate() {
            vectors.extend(c.embedding_f32());
            c.embedding = Vec::new();
            pages.entry(c.page.clone()).or_default().push(i);
            by_id.insert(c.chunk_id, i);
        }
        Self {
            version: pack.version,
            sha256,
            model_id: pack.model_id,
            model_sha256: pack.model_sha256,
            chunks,
            vectors,
            bm25,
            pages,
            by_id,
        }
    }

    /// The release the docs describe.
    #[must_use]
    pub fn version(&self) -> &str {
        &self.version
    }

    /// The pack file's SHA-256.
    #[must_use]
    pub fn sha256(&self) -> &[u8; 32] {
        &self.sha256
    }

    /// The embedding model the pack was built with.
    #[must_use]
    pub fn model_id(&self) -> &str {
        &self.model_id
    }

    /// A digest of that model's files.
    #[must_use]
    pub fn model_sha256(&self) -> &[u8; 32] {
        &self.model_sha256
    }

    /// The chunks, in `chunk_id` order. Their `embedding` is empty: see
    /// [`DocsIndex::vector`].
    #[must_use]
    pub fn chunks(&self) -> &[Chunk] {
        &self.chunks
    }

    /// Chunk `i`'s vector ([`DIM`] values, unit length).
    #[must_use]
    pub fn vector(&self, i: usize) -> &[f32] {
        &self.vectors[i * DIM..(i + 1) * DIM]
    }

    /// The chunk with id `id`.
    #[must_use]
    pub fn by_id(&self, id: &[u8]) -> Option<usize> {
        let id: [u8; 16] = id.try_into().ok()?;
        self.by_id.get(&id).copied()
    }

    /// Page `page`'s chunks, in `chunk_id` order.
    #[must_use]
    pub fn page(&self, page: &str) -> Option<&[usize]> {
        self.pages.get(page).map(Vec::as_slice)
    }

    /// The keyword index.
    #[must_use]
    pub fn bm25(&self) -> &Bm25 {
        &self.bm25
    }

    fn audience_ok(&self, i: usize, audience: Option<&str>) -> bool {
        audience.is_none_or(|a| self.chunks[i].audience.iter().any(|x| x == a))
    }

    /// The best `n` chunks for `query` by BM25, as (chunk, score), best
    /// first; ties go to the lower chunk.
    #[must_use]
    pub fn keyword(&self, query: &str, audience: Option<&str>, n: usize) -> Vec<(usize, f64)> {
        self.bm25
            .search_where(query, n, |i| self.audience_ok(i, audience))
    }

    /// The `n` chunks nearest `q` by cosine, as (chunk, similarity),
    /// nearest first; ties go to the lower chunk. An exact scan.
    #[must_use]
    pub fn nearest(&self, q: &[f32], audience: Option<&str>, n: usize) -> Vec<(usize, f64)> {
        if q.len() != DIM {
            return Vec::new();
        }
        let mut v: Vec<(usize, f64)> = (0..self.chunks.len())
            .filter(|&i| self.audience_ok(i, audience))
            .map(|i| {
                let dot: f32 = self.vector(i).iter().zip(q).map(|(a, b)| a * b).sum();
                (i, f64::from(dot))
            })
            .collect();
        let n = n.min(v.len());
        if n == 0 {
            return Vec::new();
        }
        let cmp = |a: &(usize, f64), b: &(usize, f64)| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0));
        if n < v.len() {
            v.select_nth_unstable_by(n - 1, cmp);
            v.truncate(n);
        }
        v.sort_by(cmp);
        v
    }

    /// Hybrid search (see the module docs): `q` is the query's embedding,
    /// `None` for keyword only.
    #[must_use]
    pub fn hybrid(
        &self,
        query: &str,
        q: Option<&[f32]>,
        audience: Option<&str>,
        limit: usize,
    ) -> Vec<Fused> {
        let k: Vec<usize> = self
            .keyword(query, audience, CANDIDATES)
            .into_iter()
            .map(|h| h.0)
            .collect();
        let v: Vec<usize> = q
            .map(|q| self.nearest(q, audience, CANDIDATES))
            .unwrap_or_default()
            .into_iter()
            .map(|h| h.0)
            .collect();
        let fused = rrf_weighted(&[(&k, keyword_weight(query)), (&v, 1.0)], RRF_K);
        let capped = cap_per_page(
            &fused,
            |i| self.chunks[i].page.as_str(),
            MAX_PER_PAGE,
            limit,
        );
        capped
            .into_iter()
            .map(|(chunk, score)| Fused {
                chunk,
                score,
                bm25_rank: k.iter().position(|x| *x == chunk).map(|r| r + 1),
                vector_rank: v.iter().position(|x| *x == chunk).map(|r| r + 1),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pack::chunk_pages;
    use crate::page::parse_page;

    fn pack() -> Pack {
        let pages = [
            parse_page(
                "guides/moves.md",
                "---\ntitle: Moves\naudience: [user, dba]\n---\n\n# Moves\n\nA shard move copies rows between clusters.\n\n## Errors\n\nError 39134 means the source diverged during the move.\n",
            )
            .unwrap(),
            parse_page(
                "guides/fruit.md",
                "---\ntitle: Fruit\naudience: [operator]\n---\n\n# Fruit\n\nBananas are yellow and grow in bunches.\n",
            )
            .unwrap(),
        ];
        let mut chunks = chunk_pages("1.0.0", &pages);
        for (n, c) in chunks.iter_mut().enumerate() {
            let mut v = vec![0.0f32; DIM];
            v[n % DIM] = 1.0;
            c.set_embedding(&v);
        }
        Pack {
            version: "1.0.0".into(),
            model_id: "test".into(),
            model_sha256: [0; 32],
            chunks,
        }
    }

    #[test]
    fn loads_checks_and_searches() {
        let bytes = pack().to_bytes().unwrap();
        let ix = DocsIndex::from_bytes(&bytes).unwrap();
        assert_eq!(ix.version(), "1.0.0");
        assert!(ix.chunks().iter().all(|c| c.embedding.is_empty()));
        // A changed byte fails the checksum.
        let mut bad = bytes.clone();
        bad[20] ^= 1;
        assert!(DocsIndex::from_bytes(&bad).is_err());
        // Keyword: the identifier and the words.
        let hits = ix.keyword("what does 39134 mean", None, 5);
        assert!(!hits.is_empty());
        assert!(ix.chunks()[hits[0].0].text.contains("39134"));
        let bananas = ix.keyword("bananas", None, 5);
        assert_eq!(ix.chunks()[bananas[0].0].page, "guides/fruit.md");
        // The audience filter applies after scoring.
        assert!(ix.keyword("bananas", Some("user"), 5).is_empty());
        // Vector: exact; chunk n's vector is the unit vector n.
        let mut q = vec![0.0f32; DIM];
        q[1] = 1.0;
        let near = ix.nearest(&q, None, 2);
        assert_eq!(near[0].0, 1);
        assert!((near[0].1 - 1.0).abs() < 1e-6);
        // Hybrid: both ranks, a page capped at two hits.
        let fused = ix.hybrid("shard move clusters", Some(&q), None, 10);
        assert!(!fused.is_empty());
        let mut per_page: HashMap<&str, usize> = HashMap::new();
        for f in &fused {
            *per_page
                .entry(ix.chunks()[f.chunk].page.as_str())
                .or_default() += 1;
        }
        assert!(per_page.values().all(|n| *n <= MAX_PER_PAGE));
        assert!(fused.iter().any(|f| f.bm25_rank.is_some()));
        assert!(fused.iter().any(|f| f.vector_rank.is_some()));
        // Pages and ids.
        let p = ix.page("guides/moves.md").unwrap();
        assert!(!p.is_empty());
        let id = ix.chunks()[p[0]].chunk_id;
        assert_eq!(ix.by_id(&id), Some(p[0]));
        assert!(ix.page("nope.md").is_none());
    }
}
