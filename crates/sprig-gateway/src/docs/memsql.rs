//! SQL over the local pack: `sprig_docs.chunks` as an in-memory table that
//! the engine's planner runs, on nodes without a MariaDB copy of the docs.
//!
//! ```text
//! table    sprig_docs.chunks, the pack's rows (this release only):
//!          version, chunk_id (PRIMARY KEY), page, title, heading_path,
//!          audience (its members joined by commas: FIND_IN_SET works),
//!          text, tokens, terms, embedding (little-endian f32 bytes), seq
//! MATCH    any of (text, heading_path, terms): the pack's BM25 over heading
//!          path, text and identifiers (sprig_docs::search::Bm25), the
//!          docs search's own keyword ranking; rows by score, best first
//! embed    sprig_docs.embed('<text>') becomes the query's vector as bytes,
//!          for VEC_DISTANCE_COSINE
//! describe DESCRIBE / SHOW COLUMNS: the table's columns
//! ```
//!
//! The table is built once per pack and shared. Statements are read-only
//! by construction: the planner here has no writable table.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use bytes::Bytes;
use sprig_common::{Result, SprigError};
use sprig_docs::index::DocsIndex;
use sprig_engine::row::{LobSize, Value};
use sprig_exec::{FtSpec, FullText, Persona, SqlError};
use sprig_plan::catalog::{ColumnDef, IndexDef, IndexPart, TableDef};
use sprig_plan::mem::MemDb;
use sprig_proto_mysql::ColumnDef as WireColumn;

use crate::engine::meta::Results;
use crate::engine::table::{ColType, collation_named};

/// The schema and table the docs are read as.
const SCHEMA: &str = sprig_docs::table::VIRTUAL_DATABASE;
const TABLE: &str = sprig_docs::table::CHUNKS;

/// The memory a statement's operators (sorts, joins, groups) may hold.
const MEMORY_LIMIT: usize = 64 << 20;
/// The largest answer, in bytes of text.
const MAX_RESULT_BYTES: usize = 32 << 20;
/// The most rows a statement may read: a self-join of the whole table is
/// a few million, far past any question over the docs.
const MAX_EXAMINED: u64 = 1_000_000;

/// A statement may name `sprig_docs.chunks` once: a join of the docs with
/// themselves reads millions of rows (1041 before anything runs).
///
/// # Errors
/// 1041.
pub fn check_refs(analysis: &super::sql::Analysis) -> Result<()> {
    let refs = analysis
        .edits
        .iter()
        .filter(|e| matches!(e, super::sql::Edit::Chunks { .. }))
        .count();
    if refs > 1 {
        return Err(SprigError::standard(
            1041,
            *b"HY000",
            "Out of memory: a statement over sprig_docs may name sprig_docs.chunks once (a join \
             of the docs with themselves reads millions of rows)",
        ));
    }
    Ok(())
}

/// 1041 for a statement past [`MAX_RESULT_BYTES`] or [`MAX_EXAMINED`].
fn too_big() -> SprigError {
    SprigError::standard(
        1041,
        *b"HY000",
        "Out of memory: a statement over sprig_docs may read at most 1,000,000 rows and return \
         32 MiB; narrow it with WHERE or LIMIT",
    )
}

/// The columns `MATCH` may name.
const SEARCHABLE: [&str; 3] = ["text", "heading_path", "terms"];

fn def() -> TableDef {
    let ci = collation_named("utf8mb4_unicode_ci")
        .or_else(|| collation_named("utf8mb4_general_ci"))
        .unwrap_or(sprig_engine::keys::collation::Collation::Binary);
    let vc = |n: &str, chars: u32| {
        ColumnDef::new(
            n,
            ColType::VarChar {
                chars,
                collation: ci,
            },
        )
        .not_null()
    };
    let text = |n: &str| {
        ColumnDef::new(
            n,
            ColType::Text {
                size: LobSize::Medium,
                collation: ci,
            },
        )
        .not_null()
    };
    let int = |n: &str| {
        ColumnDef::new(
            n,
            ColType::Int {
                bytes: 2,
                unsigned: true,
            },
        )
        .not_null()
    };
    TableDef {
        schema: SCHEMA.into(),
        name: TABLE.into(),
        columns: vec![
            vc("version", 32),
            ColumnDef::new("chunk_id", ColType::VarBinary { bytes: 16 }).not_null(),
            vc("page", 255),
            vc("title", 255),
            vc("heading_path", 1024),
            vc("audience", 32),
            text("text"),
            int("tokens"),
            text("terms"),
            ColumnDef::new(
                "embedding",
                ColType::Blob {
                    size: LobSize::Normal,
                },
            )
            .not_null(),
            int("seq"),
        ],
        primary: Some(IndexDef {
            name: "PRIMARY".into(),
            unique: true,
            parts: vec![IndexPart::asc(1)],
            ignored: false,
            paths: Vec::new(),
        }),
        indexes: Vec::new(),
    }
}

/// The docs as an in-memory database (built once per pack).
/// The pack's table, by the pack's SHA-256.
type Cached = Mutex<Option<([u8; 32], Arc<MemDb>)>>;

fn db_of(ix: &DocsIndex) -> Result<Arc<MemDb>> {
    static CACHE: OnceLock<Cached> = OnceLock::new();
    let cache = CACHE.get_or_init(Mutex::default);
    if let Some((sha, db)) = &*cache.lock().unwrap_or_else(PoisonError::into_inner)
        && sha == ix.sha256()
    {
        return Ok(Arc::clone(db));
    }
    let mut db = MemDb::new(SCHEMA);
    db.create_table(def());
    let s = |x: &str| Value::Str(Bytes::copy_from_slice(x.as_bytes()));
    for (i, c) in ix.chunks().iter().enumerate() {
        db.insert(
            TABLE,
            vec![
                s(ix.version()),
                Value::Bytes(Bytes::copy_from_slice(&c.chunk_id)),
                s(&c.page),
                s(&c.title),
                s(&c.heading_path),
                s(&c.audience.join(",")),
                s(&c.text),
                Value::UInt(u64::from(c.tokens)),
                s(&c.terms.join(" ")),
                Value::Bytes(Bytes::from(sprig_ai::retrieval::vector_bytes(ix.vector(i)))),
                Value::UInt(u64::from(c.seq)),
            ],
        )
        .map_err(crate::engine::general::sql_error)?;
    }
    let db = Arc::new(db);
    *cache.lock().unwrap_or_else(PoisonError::into_inner) = Some((*ix.sha256(), Arc::clone(&db)));
    Ok(db)
}

/// `MATCH` over the docs: BM25 scores, computed once per query text.
#[derive(Debug)]
struct DocsText {
    ix: Arc<DocsIndex>,
    scores: Mutex<HashMap<Vec<u8>, Arc<Vec<f64>>>>,
}

impl DocsText {
    fn scores(&self, query: &[u8]) -> Arc<Vec<f64>> {
        let mut m = self.scores.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(s) = m.get(query) {
            return Arc::clone(s);
        }
        let s = Arc::new(self.ix.bm25().scores(&String::from_utf8_lossy(query)));
        m.insert(query.to_vec(), Arc::clone(&s));
        s
    }
}

impl FullText for DocsText {
    fn check(&self, spec: &FtSpec) -> std::result::Result<(), SqlError> {
        let ok = spec.table.eq_ignore_ascii_case(TABLE)
            && !spec.columns.is_empty()
            && spec
                .columns
                .iter()
                .all(|c| SEARCHABLE.iter().any(|s| s.eq_ignore_ascii_case(c)));
        if ok {
            Ok(())
        } else {
            Err(SqlError::new(
                1191,
                "HY000",
                "Can't find FULLTEXT index matching the column list",
            ))
        }
    }

    fn score(
        &self,
        _spec: &FtSpec,
        query: &[u8],
        pk: &[Value],
    ) -> std::result::Result<f64, SqlError> {
        let Some(Value::Bytes(id)) = pk.first() else {
            return Ok(0.0);
        };
        Ok(self
            .ix
            .by_id(id)
            .and_then(|i| self.scores(query).get(i).copied())
            .unwrap_or(0.0))
    }

    fn hits(
        &self,
        _spec: &FtSpec,
        query: &[u8],
    ) -> std::result::Result<Option<(String, Vec<Vec<Value>>)>, SqlError> {
        let scores = self.scores(query);
        let mut v: Vec<(usize, f64)> = scores
            .iter()
            .copied()
            .enumerate()
            .filter(|(_, s)| *s > 0.0)
            .collect();
        v.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
        Ok(Some((
            "ft_search".into(),
            v.into_iter()
                .map(|(i, _)| {
                    vec![Value::Bytes(Bytes::copy_from_slice(
                        &self.ix.chunks()[i].chunk_id,
                    ))]
                })
                .collect(),
        )))
    }
}

/// `text` for the in-memory table: each `sprig_docs.embed('...')` call
/// becomes its vector's bytes (`X'..'`, little-endian `f32`); the table's
/// name stays as written.
#[must_use]
pub fn render(text: &[u8], analysis: &super::sql::Analysis, embeds: &[Vec<f32>]) -> String {
    let mut out = String::with_capacity(text.len() + 64);
    let mut at = 0;
    let mut embeds = embeds.iter();
    for e in &analysis.edits {
        if let super::sql::Edit::Embed(range, _) = e {
            out.push_str(&String::from_utf8_lossy(&text[at..range.start]));
            let v = embeds
                .next()
                .map_or_else(Vec::new, |v| sprig_ai::retrieval::vector_bytes(v));
            out.push_str("X'");
            out.push_str(&sprig_ai::retrieval::hex(&v));
            out.push('\'');
            at = range.end;
        }
    }
    out.push_str(&String::from_utf8_lossy(&text[at..]));
    out
}

/// Whether a description statement is one [`describe`] answers
/// (`DESCRIBE`, `DESC`, `EXPLAIN` of a table, `SHOW [FULL] COLUMNS|FIELDS`).
#[must_use]
pub fn describes_columns(text: &[u8]) -> bool {
    let t = String::from_utf8_lossy(text)
        .trim_start()
        .to_ascii_uppercase();
    let w: Vec<&str> = t.split_whitespace().take(3).collect();
    matches!(w.first(), Some(&("DESCRIBE" | "DESC" | "EXPLAIN")))
        || (w.first() == Some(&"SHOW") && w[1..].iter().any(|x| *x == "COLUMNS" || *x == "FIELDS"))
}

/// Runs read statement `sql` (its `sprig_docs.embed(...)` calls already
/// replaced) over the docs: its rows, as the port `mysql` reports them.
///
/// # Errors
/// The planner's or executor's (as MariaDB's codes), or 1064 for a text
/// that doesn't parse.
pub fn query(ix: &Arc<DocsIndex>, sql: &str, mysql: bool) -> Result<sprig_types::Rows> {
    use sqlparser::dialect::MySqlDialect;
    use sqlparser::parser::Parser;
    let stmts = Parser::parse_sql(&MySqlDialect {}, sql).map_err(|e| {
        SprigError::standard(
            1064,
            *b"42000",
            format!("You have an error in your SQL syntax: {e}"),
        )
    })?;
    let [stmt] = stmts.as_slice() else {
        return Err(SprigError::standard(
            1064,
            *b"42000",
            "one statement over sprig_docs at a time",
        ));
    };
    let db = db_of(ix)?;
    let persona = if mysql {
        Persona::MySql
    } else {
        Persona::MariaDb
    };
    let mut session = sprig_exec::Session::new(persona);
    session.info.fulltext = Some(Arc::new(DocsText {
        ix: Arc::clone(ix),
        scores: Mutex::default(),
    }));
    let storage: Arc<dyn sprig_plan::access::Storage> = Arc::new((*db).clone());
    let mut planner = sprig_plan::Planner::new(&*db, storage, &session);
    planner.options = sprig_plan::PlanOptions {
        memory_limit: Some(MEMORY_LIMIT),
        ..sprig_plan::PlanOptions::for_persona(session.persona)
    };
    let mut plan = planner
        .plan(stmt, &[])
        .map_err(crate::engine::general::sql_error)?;
    let mut ctx = sprig_exec::Ctx::with_params(&session, &[]);
    let results = Results { cs: None, mysql };
    let columns: Vec<WireColumn> = plan
        .columns
        .iter()
        .map(|c| crate::engine::general::results::value_def(&c.name, &c.ty, results))
        .collect();
    let tys: Vec<sprig_exec::ExprType> = plan.columns.iter().map(|c| c.ty).collect();
    let mut rows: Vec<Vec<Option<Vec<u8>>>> = Vec::new();
    let mut bytes = 0usize;
    while let Some(r) = plan
        .source
        .next(&mut ctx)
        .map_err(crate::engine::general::sql_error)?
    {
        let row: Vec<Option<Vec<u8>>> = r
            .iter()
            .zip(&tys)
            .map(|(v, ty)| sprig_exec::value_text(v, ty, persona).map(|b| b.to_vec()))
            .collect();
        bytes += row
            .iter()
            .map(|v| v.as_ref().map_or(1, Vec::len))
            .sum::<usize>();
        if bytes > MAX_RESULT_BYTES || plan.rows_examined() > MAX_EXAMINED {
            return Err(too_big());
        }
        rows.push(row);
    }
    if plan.rows_examined() > MAX_EXAMINED {
        return Err(too_big());
    }
    Ok(sprig_types::Rows { columns, rows })
}

/// `DESCRIBE sprig_docs.chunks` as the MariaDB copy answers it.
#[must_use]
pub fn describe() -> sprig_types::Rows {
    let col = |name: &str| {
        crate::engine::general::results::value_def(
            name,
            &sprig_exec::ExprType::string(
                64,
                sprig_engine::keys::collation::Collation::Utf8mb4Bin,
                sprig_exec::types::Derivation::Implicit,
            ),
            Results {
                cs: None,
                mysql: false,
            },
        )
    };
    let rows: &[[&str; 6]] = &[
        ["version", "varchar(32)", "NO", "PRI", "", ""],
        ["chunk_id", "binary(16)", "NO", "PRI", "", ""],
        ["page", "varchar(255)", "NO", "", "", ""],
        ["title", "varchar(255)", "NO", "", "", ""],
        ["heading_path", "varchar(1024)", "NO", "", "", ""],
        ["audience", "set('user','dba','operator')", "NO", "", "", ""],
        ["text", "mediumtext", "NO", "MUL", "", ""],
        ["tokens", "smallint(5) unsigned", "NO", "", "", ""],
        ["terms", "text", "NO", "", "", ""],
        ["embedding", "vector(384)", "NO", "MUL", "", ""],
        ["seq", "smallint(5) unsigned", "NO", "", "0", ""],
    ];
    sprig_types::Rows {
        columns: ["Field", "Type", "Null", "Key", "Default", "Extra"]
            .iter()
            .map(|n| col(n))
            .collect(),
        rows: rows
            .iter()
            .map(|r| {
                r.iter()
                    .enumerate()
                    .map(|(i, v)| {
                        if i == 4 && v.is_empty() {
                            None
                        } else {
                            Some(v.as_bytes().to_vec())
                        }
                    })
                    .collect()
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sprig_docs::pack::{DIM, Pack, chunk_pages};
    use sprig_docs::page::parse_page;

    fn index() -> Arc<DocsIndex> {
        let pages = [
            parse_page(
                "ops/tenants.md",
                "---\ntitle: Tenants\naudience: [operator]\n---\n\n# Tenants\n\nSuspend a tenant with sprigctl tenant suspend.\n",
            )
            .unwrap(),
            parse_page(
                "guides/fruit.md",
                "---\ntitle: Fruit\naudience: [user]\n---\n\n# Fruit\n\nBananas grow in bunches.\n",
            )
            .unwrap(),
        ];
        let mut chunks = chunk_pages("1.0.0", &pages);
        for (n, c) in chunks.iter_mut().enumerate() {
            let mut v = vec![0.0f32; DIM];
            v[n] = 1.0;
            c.set_embedding(&v);
        }
        let pack = Pack {
            version: "1.0.0".into(),
            model_id: "test".into(),
            model_sha256: [0; 32],
            chunks,
        };
        Arc::new(DocsIndex::from_bytes(&pack.to_bytes().unwrap()).unwrap())
    }

    fn text(r: &sprig_types::Rows) -> Vec<Vec<String>> {
        r.rows
            .iter()
            .map(|row| {
                row.iter()
                    .map(|v| {
                        v.as_ref()
                            .map_or("NULL".into(), |b| String::from_utf8_lossy(b).into_owned())
                    })
                    .collect()
            })
            .collect()
    }

    #[test]
    fn selects_match_and_audience() {
        let ix = index();
        let r = query(
            &ix,
            "SELECT page, heading_path FROM sprig_docs.chunks WHERE MATCH(text) AGAINST ('suspend tenant') LIMIT 3",
            false,
        )
        .unwrap();
        assert_eq!(text(&r)[0][0], "ops/tenants.md");
        let r = query(
            &ix,
            "SELECT COUNT(*) > 0 AS has_operator_pages FROM sprig_docs.chunks WHERE FIND_IN_SET('operator', audience)",
            false,
        )
        .unwrap();
        assert_eq!(text(&r), [["1"]]);
        let r = query(
            &ix,
            "SELECT page, MATCH(text, heading_path) AGAINST ('bananas') AS s FROM sprig_docs.chunks ORDER BY s DESC, page LIMIT 1",
            true,
        )
        .unwrap();
        assert_eq!(text(&r)[0][0], "guides/fruit.md");
        assert!(
            query(
                &ix,
                "SELECT page FROM sprig_docs.chunks WHERE MATCH(page) AGAINST ('x')",
                false
            )
            .is_err()
        );
        // Two references to the table (a join of the docs with
        // themselves) are refused before anything runs.
        let one = super::super::sql::analyze(
            b"SELECT page FROM sprig_docs.chunks LIMIT 1",
            &sprig_sql::lex::LexOptions::mysql84(),
        )
        .unwrap()
        .unwrap();
        assert!(check_refs(&one).is_ok());
        let two = super::super::sql::analyze(
            b"SELECT COUNT(*) FROM sprig_docs.chunks a, sprig_docs.chunks b",
            &sprig_sql::lex::LexOptions::mysql84(),
        )
        .unwrap()
        .unwrap();
        assert!(check_refs(&two).is_err());
        assert_eq!(describe().rows.len(), 11);
    }
}
