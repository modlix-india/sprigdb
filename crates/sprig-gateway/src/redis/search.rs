//! `FT.*` (Redis Search's commands) on SprigDB's search indexes.
//!
//! ```text
//! FT.CREATE / FT.ALTER: CREATE SEARCH INDEX / ALTER SEARCH INDEX ... ADD FIELD
//!   on the selected database's namespace (`<redis.logical_database>.db<n>`),
//!   run on the login's session: SQL's privileges, catalog and copies
//! FT.DROPINDEX idx [DD]: DROP SEARCH INDEX; DD first deletes the hashes
//!   under the index's prefixes
//! FT.SEARCH / FT.AGGREGATE:
//!   the index by name in the logical database, else SEARCH_INDEX_NOT_FOUND
//!   SELECT on the namespace, checked as a read of it
//!   the Redis Search query as the search AST (sprig_search::ft_query)
//!   a copy answers once it has applied every cluster's position (Redis
//!     indexes a hash as it is written, so a write is found at once)
//!   the hits re-read through the login's session: keys that are gone,
//!     expired or hidden are left out
//!   SORTBY and FT.AGGREGATE read every match (up to search.max_window,
//!     else 39827) and sort or aggregate in the gateway
//! FT.INFO, FT._LIST, FT.EXPLAIN(CLI), FT.TAGVALS: from the catalog and a copy
//! ```
//!
//! Errors follow Redis 8's texts where clients look at them
//! (`SEARCH_INDEX_NOT_FOUND`, `SEARCH_INDEX_EXISTS`); syntax errors are
//! `-ERR ...`; what SprigDB doesn't offer is `-SPRIG SEARCH_UNSUPPORTED`.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;
use sprig_common::ids::TenantId;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_kv::{SqlError, SqlRunner};
use sprig_proto_redis::Frame;
use sprig_redis::search::{FtCall, SearchCalls};
use sprig_search::def::{FieldKind, IndexDef, Model, Source};
use sprig_search::ft_agg::{AggVal, Row};
use sprig_search::query::SearchQuery;
use sprig_search::rpc::{Answer, Op};
use sprig_search::schema::Key;
use sprig_search::{Hit, Scoring, ft, ft_agg, ft_query};

use crate::search::catalog as scat;
use crate::search::{Consistency, SearchService};

/// The gateway's [`SearchCalls`].
#[derive(Debug, Default)]
pub struct GatewaySearch;

#[async_trait]
impl SearchCalls for GatewaySearch {
    async fn ft(&self, sql: &mut dyn SqlRunner, call: FtCall<'_>) -> Frame {
        match run(sql, &call).await {
            Ok(f) => f,
            Err(e) => error_frame(&e),
        }
    }
}

/// An error as Redis shows it.
fn error_frame(e: &SprigError) -> Frame {
    match e.code() {
        Some(SprigCode::SearchIndexNotFound) => Frame::error(e.message.clone()),
        Some(SprigCode::SearchQuerySyntax | SprigCode::SearchFieldType) => Frame::err(&e.message),
        Some(c) => sprig_redis::reply::sprig(c, &e.message),
        None => match errno(e) {
            Some(1142 | 1143 | 1044 | 1045) => Frame::error(format!(
                "NOPERM this user has no permissions to run this command on this data: {}",
                e.message
            )),
            _ => Frame::err(&e.message),
        },
    }
}

/// A MariaDB or MySQL error's number.
fn errno(e: &SprigError) -> Option<u16> {
    match e.kind {
        sprig_common::ErrorKind::Backend { errno, .. }
        | sprig_common::ErrorKind::Standard { errno, .. } => Some(errno),
        sprig_common::ErrorKind::Sprig(_) => None,
    }
}

fn sql_err(e: &SqlError) -> SprigError {
    match e.errno {
        Some(n) if (39000..40000).contains(&n) => {
            let code = SprigCode::from_number(n).unwrap_or(SprigCode::Internal);
            SprigError::new(code, e.message.clone())
        }
        Some(n) => SprigError::standard(n, *b"42000", e.message.clone()),
        None => SprigError::internal(e.message.clone()),
    }
}

fn not_found(name: &str) -> SprigError {
    SprigError::new(
        SprigCode::SearchIndexNotFound,
        format!("SEARCH_INDEX_NOT_FOUND Index not found: {name}"),
    )
}

fn text(b: &[u8]) -> String {
    String::from_utf8_lossy(b).into_owned()
}

fn q(s: &str) -> String {
    sprig_common::names::quote(s)
}

fn hex(b: &[u8]) -> String {
    sprig_kv::sql::lit(b)
}

fn bulk(s: impl Into<String>) -> Frame {
    Frame::bulk(Bytes::from(s.into().into_bytes()))
}

fn service() -> Result<&'static Arc<SearchService>> {
    crate::search::service().ok_or_else(|| {
        let mut e = SprigError::new(
            SprigCode::SearchUnavailable,
            "search isn't running on this gateway",
        );
        e.retryable = true;
        e
    })
}

/// Re-read keys: each key's fields and values.
type Hashes = HashMap<Vec<u8>, Vec<(Vec<u8>, Vec<u8>)>>;

/// What one command needs.
struct Cx<'a, 'c> {
    sql: &'a mut dyn SqlRunner,
    call: &'a FtCall<'c>,
    svc: &'static Arc<SearchService>,
    tenant: TenantId,
}

async fn run(sql: &mut dyn SqlRunner, call: &FtCall<'_>) -> Result<Frame> {
    let tenant = TenantId(call.scope.parse().unwrap_or(0));
    if tenant.0 == 0 {
        return Err(SprigError::new(
            SprigCode::SearchUnavailable,
            "search indexes belong to tenants",
        ));
    }
    let mut cx = Cx {
        sql,
        call,
        svc: service()?,
        tenant,
    };
    let a: Vec<&[u8]> = call.args.iter().map(AsRef::as_ref).collect();
    match call.command {
        "ft.create" => cx.create(&a).await,
        "ft.alter" => cx.alter(&a).await,
        "ft.dropindex" => cx.drop_index(&a).await,
        "ft._list" => cx.list().await,
        "ft.info" => cx.info(&a).await,
        "ft.search" => cx.search(&a).await,
        "ft.aggregate" => cx.aggregate(&a).await,
        "ft.explain" | "ft.explaincli" => cx.explain(&a).await,
        "ft.tagvals" => cx.tagvals(&a).await,
        other => Err(SprigError::new(
            SprigCode::SearchUnsupported,
            format!("{} is not supported", other.to_ascii_uppercase()),
        )),
    }
}

impl Cx<'_, '_> {
    fn namespace(&self) -> String {
        format!("db{}", self.call.db)
    }

    async fn exec(&mut self, stmt: &str) -> Result<()> {
        self.sql.query(stmt).await.map_err(|e| sql_err(&e))?;
        Ok(())
    }

    async fn rows(&mut self, stmt: &str) -> Result<Vec<Vec<Option<Vec<u8>>>>> {
        self.sql.query(stmt).await.map_err(|e| sql_err(&e))
    }

    /// The index named `name` in the logical database.
    async fn index(
        &mut self,
        name: &str,
    ) -> Result<(Arc<IndexDef>, Vec<sprig_search::analyze::CustomAnalyzer>)> {
        let cat = self.svc.catalog().snapshot(self.tenant, 0).await?;
        let db_id = self
            .svc
            .catalog()
            .db_id(&cat, self.call.database)
            .ok_or_else(|| not_found(name))?;
        let st = scat::state(&cat);
        let def = st
            .of_db(db_id.0)
            .iter()
            .find(|d| d.name == name)
            .cloned()
            .ok_or_else(|| not_found(name))?;
        Ok((def, st.analyzers.clone()))
    }

    async fn create(&mut self, a: &[&[u8]]) -> Result<Frame> {
        let c = ft::parse_create(a)?;
        if self.index(&c.name).await.is_ok() {
            return Ok(Frame::error("SEARCH_INDEX_EXISTS Index already exists"));
        }
        let ddl = c.ddl(self.call.database, &self.namespace());
        match self.exec(&ddl).await {
            Ok(()) => Ok(Frame::ok()),
            Err(e) if errno(&e) == Some(1061) => {
                Ok(Frame::error("SEARCH_INDEX_EXISTS Index already exists"))
            }
            Err(e) => Err(e),
        }
    }

    async fn alter(&mut self, a: &[&[u8]]) -> Result<Frame> {
        let name = a.first().map(|x| text(x)).unwrap_or_default();
        let (def, _) = self.index(&name).await?;
        let language = def
            .fields
            .iter()
            .find(|f| f.kind == FieldKind::Text)
            .and_then(|f| f.analyzer.clone())
            .unwrap_or_else(|| def.default_analyzer.clone());
        let language = language
            .trim_end_matches("_nostem")
            .trim_end_matches("_nostop")
            .to_owned();
        let language = if language == "standard" {
            "english".to_owned()
        } else {
            language
        };
        let (_, stmts) = ft::alter_ddl(a, self.call.database, &language)?;
        for s in stmts {
            self.exec(&s).await?;
        }
        Ok(Frame::ok())
    }

    async fn drop_index(&mut self, a: &[&[u8]]) -> Result<Frame> {
        let name = a.first().map(|x| text(x)).unwrap_or_default();
        let dd = match a.get(1).map(|x| text(x).to_ascii_uppercase()).as_deref() {
            None => false,
            Some("DD") => true,
            Some(o) => return Err(ft::arg_error(format!("Unknown argument `{o}`"))),
        };
        let (def, _) = self.index(&name).await?;
        if dd {
            self.delete_documents(&def).await?;
        }
        self.exec(&format!(
            "DROP SEARCH INDEX {}.{}",
            q(&def.db),
            q(&def.name)
        ))
        .await?;
        Ok(Frame::ok())
    }

    /// `DD`: deletes the hashes under the index's prefixes.
    async fn delete_documents(&mut self, def: &IndexDef) -> Result<()> {
        let keys_t = format!("{}.{}", q(&def.db), q(&def.table));
        let elems_t = format!("{}.{}", q(&def.db), q(&format!("__sprig_ke_{}", def.table)));
        let cond = if def.key_prefixes.is_empty() {
            String::new()
        } else {
            let p: Vec<String> = def
                .key_prefixes
                .iter()
                .map(|p| format!("LEFT(k, {}) = {}", p.len(), hex(p.as_bytes())))
                .collect();
            format!(" AND ({})", p.join(" OR "))
        };
        loop {
            let rows = self
                .rows(&format!(
                    "SELECT k FROM {keys_t} WHERE type = 'hash'{cond} LIMIT 500"
                ))
                .await?;
            if rows.is_empty() {
                return Ok(());
            }
            let list: Vec<String> = rows
                .iter()
                .filter_map(|r| r.first().cloned().flatten())
                .map(|k| hex(&k))
                .collect();
            let list = list.join(", ");
            self.exec(&format!("DELETE FROM {elems_t} WHERE k IN ({list})"))
                .await?;
            self.exec(&format!(
                "DELETE FROM {keys_t} WHERE k IN ({list}) AND type = 'hash'"
            ))
            .await?;
            if rows.len() < 500 {
                return Ok(());
            }
        }
    }

    async fn list(&mut self) -> Result<Frame> {
        let cat = self.svc.catalog().snapshot(self.tenant, 0).await?;
        let Some(db_id) = self.svc.catalog().db_id(&cat, self.call.database) else {
            return Ok(Frame::Array(Vec::new()));
        };
        let mut names: Vec<String> = scat::state(&cat)
            .of_db(db_id.0)
            .iter()
            .filter(|d| d.model == Model::Kv)
            .map(|d| d.name.clone())
            .collect();
        names.sort();
        let items = names.into_iter().map(bulk).collect();
        Ok(if self.call.resp3 {
            Frame::Set(items)
        } else {
            Frame::Array(items)
        })
    }

    /// Runs `ops` over `query` on a copy, after every write before now.
    async fn query(
        &mut self,
        def: &Arc<IndexDef>,
        customs: &[sprig_search::analyze::CustomAnalyzer],
        query: SearchQuery,
        ops: Vec<Op>,
    ) -> Result<Vec<Answer>> {
        let (answers, _) = self
            .svc
            .query(self.tenant, def, customs, query, ops, Consistency::Latest)
            .await?;
        Ok(answers)
    }

    async fn info(&mut self, a: &[&[u8]]) -> Result<Frame> {
        let name = a.first().map(|x| text(x)).unwrap_or_default();
        let (def, customs) = self.index(&name).await?;
        self.check_read(&def).await?;
        let (ready, _) = self.svc.state(self.tenant, &def).await;
        let docs = match self
            .query(&def, &customs, SearchQuery::All, vec![Op::Count])
            .await
        {
            Ok(v) => match v.first() {
                Some(Answer::Count(n)) => *n,
                _ => 0,
            },
            Err(_) => 0,
        };
        let docs = i64::try_from(docs).unwrap_or(i64::MAX);
        let mut attrs = Vec::new();
        let mut seen: Vec<&str> = Vec::new();
        for f in &def.fields {
            if seen.contains(&f.name.as_str()) {
                continue;
            }
            seen.push(&f.name);
            let same: Vec<_> = def.fields_named(&f.name);
            let mut v = vec![
                bulk("identifier"),
                bulk(f.source_ref.clone()),
                bulk("attribute"),
                bulk(f.name.clone()),
                bulk("type"),
            ];
            if let Some(t) = same.iter().find(|x| x.kind == FieldKind::Text) {
                v.push(bulk("TEXT"));
                v.push(bulk("WEIGHT"));
                v.push(bulk(ft_agg::fmt_g(t.boost)));
                if same.iter().any(|x| x.kind == FieldKind::Keyword && x.sort) {
                    v.push(bulk("SORTABLE"));
                }
            } else if same.iter().any(|x| x.kind == FieldKind::Number) {
                v.push(bulk("NUMERIC"));
            } else {
                v.push(bulk("TAG"));
                if let Some(s) = &f.separator {
                    v.push(bulk("SEPARATOR"));
                    v.push(bulk(s.clone()));
                }
                if f.case_sensitive {
                    v.push(bulk("CASESENSITIVE"));
                }
            }
            attrs.push(Frame::Array(v));
        }
        let kv = |k: &str, v: Frame| (bulk(k), v);
        Ok(Frame::Map(vec![
            kv("index_name", bulk(def.name.clone())),
            kv("index_options", Frame::Array(Vec::new())),
            kv(
                "index_definition",
                Frame::Map(vec![
                    kv("key_type", bulk("HASH")),
                    kv(
                        "prefixes",
                        Frame::Array(def.key_prefixes.iter().cloned().map(bulk).collect()),
                    ),
                    kv("default_score", bulk("1")),
                ]),
            ),
            kv("attributes", Frame::Array(attrs)),
            kv("num_docs", Frame::Integer(docs)),
            kv("max_doc_id", Frame::Integer(docs)),
            kv("hash_indexing_failures", Frame::Integer(0)),
            kv("indexing", Frame::Integer(i64::from(!ready))),
            kv("percent_indexed", bulk(if ready { "1" } else { "0" })),
            kv("generation", Frame::Integer(i64::from(def.generation))),
        ]))
    }

    /// `SELECT` on the namespace, checked as a read of it.
    async fn check_read(&mut self, def: &IndexDef) -> Result<()> {
        self.exec(&format!(
            "SELECT 1 FROM {}.{} LIMIT 0",
            q(&def.db),
            q(&def.table)
        ))
        .await
    }

    /// The AST of a Redis Search query on `def`.
    fn parse_query(
        def: &IndexDef,
        customs: &[sprig_search::analyze::CustomAnalyzer],
        query: &str,
        params: &[(String, String)],
        dialect: u32,
        infields: Vec<String>,
        slop: u32,
    ) -> Result<SearchQuery> {
        let analyzers = sprig_search::analyze::Analyzers::resolve(
            def.analyzer_names().iter().map(String::as_str),
            customs,
        )?;
        let text_an = def
            .default_fields()
            .first()
            .map_or(def.default_analyzer.clone(), |f| {
                f.index_analyzer(&def.default_analyzer).to_owned()
            });
        let words = analyzers.get(&text_an)?;
        let is_stopword = |w: &str| words.query_tokens(w).is_empty();
        let cx = ft_query::FtContext {
            def,
            dialect,
            infields,
            slop,
            stopword: &is_stopword,
        };
        ft_query::parse(query, params, &cx)
    }

    async fn explain(&mut self, a: &[&[u8]]) -> Result<Frame> {
        let name = a.first().map(|x| text(x)).unwrap_or_default();
        let query = a.get(1).map(|x| text(x)).unwrap_or_default();
        let (def, customs) = self.index(&name).await?;
        let opts = ft::parse_search_opts(a.get(2..).unwrap_or_default())?;
        let ast = Self::parse_query(
            &def,
            &customs,
            &query,
            &opts.params,
            opts.dialect,
            Vec::new(),
            opts.slop.unwrap_or(0),
        )?;
        let t = ft_query::explain(&ast);
        Ok(if self.call.command == "ft.explaincli" {
            Frame::Array(t.lines().map(|l| bulk(l.to_owned())).collect())
        } else {
            bulk(t)
        })
    }

    async fn tagvals(&mut self, a: &[&[u8]]) -> Result<Frame> {
        let name = a.first().map(|x| text(x)).unwrap_or_default();
        let field = a.get(1).map(|x| text(x)).unwrap_or_default();
        let (def, customs) = self.index(&name).await?;
        self.check_read(&def).await?;
        if !def
            .fields_named(&field)
            .iter()
            .any(|f| f.kind == FieldKind::Keyword && f.separator.is_some())
        {
            return Err(ft::arg_error(format!("Not a tag field: {field}")));
        }
        let ans = self
            .query(
                &def,
                &customs,
                SearchQuery::All,
                vec![Op::Facet {
                    path: field,
                    n: usize::MAX,
                    bounds: None,
                }],
            )
            .await?;
        let mut vals: Vec<String> = match ans.into_iter().next() {
            Some(Answer::Buckets(b)) => b
                .into_iter()
                .filter_map(|x| x.value.as_str().map(str::to_owned))
                .collect(),
            _ => Vec::new(),
        };
        vals.sort();
        let items = vals.into_iter().map(bulk).collect();
        Ok(if self.call.resp3 {
            Frame::Set(items)
        } else {
            Frame::Array(items)
        })
    }

    /// The live hashes (and string keys) among `keys`, with their fields
    /// (`only`: those fields; `None`: all), read through the session.
    async fn reread(
        &mut self,
        def: &IndexDef,
        keys: &[Key],
        only: Option<&[String]>,
    ) -> Result<Hashes> {
        let mut out: Hashes = HashMap::new();
        let keys_t = format!("{}.{}", q(&def.db), q(&def.table));
        let elems_t = format!("{}.{}", q(&def.db), q(&format!("__sprig_ke_{}", def.table)));
        let field_cond = match only {
            Some([]) => " AND FALSE".to_owned(),
            Some(f) => format!(
                " AND e.e IN ({})",
                f.iter()
                    .map(|x| hex(x.as_bytes()))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            None => String::new(),
        };
        for chunk in keys.chunks(500) {
            let list: Vec<String> = chunk.iter().map(|k| hex(&k.0)).collect();
            let rows = self
                .rows(&format!(
                    "SELECT t.k, e.e, e.v FROM {keys_t} AS t LEFT JOIN {elems_t} AS e \
                     ON e.k = t.k AND e.g = t.g AND t.type = 'hash'{field_cond} \
                     WHERE t.k IN ({}) AND t.type IN ('hash', 'string') \
                     AND (t.expires_at IS NULL OR t.expires_at > NOW(3)) ORDER BY t.k, e.o, e.e",
                    list.join(", ")
                ))
                .await?;
            for r in rows {
                let mut it = r.into_iter();
                let k = it.next().flatten().unwrap_or_default();
                let fields = out.entry(k).or_default();
                if let (Some(Some(e)), Some(v)) = (it.next(), it.next()) {
                    fields.push((e, v.unwrap_or_default()));
                }
            }
        }
        Ok(out)
    }

    /// The query and hits of `FT.SEARCH` or `FT.AGGREGATE`: every match
    /// (`all`), else the best `top` and the number of matches.
    async fn matches(
        &mut self,
        def: &Arc<IndexDef>,
        customs: &[sprig_search::analyze::CustomAnalyzer],
        query: SearchQuery,
        all: bool,
        top: usize,
    ) -> Result<(Vec<Hit>, u64)> {
        let max = self.svc.knobs().max_window;
        let ops = if all {
            vec![Op::All { max }]
        } else if top == 0 {
            vec![Op::Count]
        } else {
            vec![
                Op::Top {
                    k: top,
                    scoring: Scoring::Global,
                },
                Op::Count,
            ]
        };
        let mut hits = Vec::new();
        let mut total = None;
        for ans in self.query(def, customs, query, ops).await? {
            match ans {
                Answer::Hits(h) => hits = h,
                Answer::Count(n) => total = Some(n),
                Answer::Buckets(_) => {}
            }
        }
        let total = total.unwrap_or(hits.len() as u64);
        Ok((hits, total))
    }

    // Options, the sort, the page and both reply shapes in one place.
    #[allow(clippy::too_many_lines)]
    async fn search(&mut self, a: &[&[u8]]) -> Result<Frame> {
        let name = a.first().map(|x| text(x)).unwrap_or_default();
        let query = a.get(1).map(|x| text(x)).unwrap_or_default();
        let opts = ft::parse_search_opts(a.get(2..).unwrap_or_default())?;
        let (def, customs) = self.index(&name).await?;
        self.check_read(&def).await?;
        let mut ast = Self::parse_query(
            &def,
            &customs,
            &query,
            &opts.params,
            opts.dialect,
            opts.infields.clone().unwrap_or_default(),
            opts.slop.unwrap_or(0),
        )?;
        let mut filter = Vec::new();
        for (f, lo, hi) in &opts.filters {
            filter.push(ft_query::filter_range(f, lo, hi)?);
        }
        if let Some(k) = &opts.inkeys {
            filter.push(SearchQuery::Keys(
                k.iter().map(|x| Key(x.clone())).collect(),
            ));
        }
        if !filter.is_empty() {
            ast = SearchQuery::Bool {
                must: vec![ast],
                should: Vec::new(),
                must_not: Vec::new(),
                filter,
                min_should: 0,
            };
        }
        let end = opts.offset.saturating_add(opts.num);
        let (mut hits, total) = self
            .matches(&def, &customs, ast, opts.sortby.is_some(), end)
            .await?;
        let mut sort_keys: HashMap<Key, Option<String>> = HashMap::new();
        if let Some((field, desc)) = &opts.sortby {
            let src = def
                .fields_named(field)
                .first()
                .map_or_else(|| field.clone(), |f| f.source_ref.clone());
            let numeric = def
                .fields_named(field)
                .iter()
                .any(|f| f.kind == FieldKind::Number);
            let keys: Vec<Key> = hits.iter().map(|h| h.key.clone()).collect();
            let vals = self
                .reread(&def, &keys, Some(std::slice::from_ref(&src)))
                .await?;
            for h in &hits {
                let v = vals
                    .get(&h.key.0)
                    .and_then(|f| f.first())
                    .map(|(_, v)| text(v));
                sort_keys.insert(h.key.clone(), v);
            }
            let num = |s: &Option<String>| s.as_deref().and_then(|x| x.trim().parse::<f64>().ok());
            hits.sort_by(|x, y| {
                let (a, b) = (&sort_keys[&x.key], &sort_keys[&y.key]);
                let o = match (a.is_some(), b.is_some()) {
                    (true, false) => return std::cmp::Ordering::Less,
                    (false, true) => return std::cmp::Ordering::Greater,
                    _ if numeric => num(a)
                        .partial_cmp(&num(b))
                        .unwrap_or(std::cmp::Ordering::Equal),
                    _ => a.cmp(b),
                };
                let o = if *desc { o.reverse() } else { o };
                o.then_with(|| x.key.cmp(&y.key))
            });
        }
        let page: Vec<Hit> = hits.into_iter().skip(opts.offset).take(opts.num).collect();
        // The fields to return, by hash field, with their reply names.
        let returns: Option<Vec<(String, String)>> = opts.returns.as_ref().map(|r| {
            r.iter()
                .map(|(f, alias)| {
                    let src = def
                        .fields_named(f)
                        .first()
                        .map_or_else(|| f.clone(), |d| d.source_ref.clone());
                    (src, alias.clone())
                })
                .collect()
        });
        let only: Option<Vec<String>> = if opts.nocontent {
            Some(Vec::new())
        } else {
            returns
                .as_ref()
                .map(|r| r.iter().map(|(s, _)| s.clone()).collect())
        };
        let keys: Vec<Key> = page.iter().map(|h| h.key.clone()).collect();
        let found = self.reread(&def, &keys, only.as_deref()).await?;
        let resp3 = self.call.resp3;
        let total = i64::try_from(total).unwrap_or(i64::MAX);
        let mut flat = vec![Frame::Integer(total)];
        let mut results = Vec::new();
        for h in &page {
            let Some(fields) = found.get(&h.key.0) else {
                continue;
            };
            let pairs: Vec<(Frame, Frame)> = match &returns {
                Some(r) => r
                    .iter()
                    .filter_map(|(src, alias)| {
                        fields
                            .iter()
                            .find(|(e, _)| e.as_slice() == src.as_bytes())
                            .map(|(_, v)| (bulk(alias.clone()), Frame::bulk_from(v)))
                    })
                    .collect(),
                None => fields
                    .iter()
                    .map(|(e, v)| (Frame::bulk_from(e), Frame::bulk_from(v)))
                    .collect(),
            };
            let score = f64::from(h.score);
            let sortkey = sort_keys.get(&h.key).map(|v| match v {
                Some(s) => bulk(format!("${s}")),
                None => Frame::null(),
            });
            if resp3 {
                let mut m = vec![(bulk("id"), Frame::bulk_from(&h.key.0))];
                if opts.withscores {
                    m.push((bulk("score"), Frame::Double(score)));
                }
                if opts.withsortkeys {
                    m.push((bulk("sortkey"), sortkey.unwrap_or_else(Frame::null)));
                }
                if !opts.nocontent {
                    m.push((bulk("extra_attributes"), Frame::Map(pairs)));
                }
                m.push((bulk("values"), Frame::Array(Vec::new())));
                results.push(Frame::Map(m));
            } else {
                flat.push(Frame::bulk_from(&h.key.0));
                if opts.withscores {
                    flat.push(bulk(score.to_string()));
                }
                if opts.withsortkeys {
                    flat.push(sortkey.unwrap_or_else(Frame::null));
                }
                if !opts.nocontent {
                    flat.push(Frame::Array(
                        pairs.into_iter().flat_map(|(k, v)| [k, v]).collect(),
                    ));
                }
            }
        }
        Ok(if resp3 {
            Frame::Map(vec![
                (bulk("attributes"), Frame::Array(Vec::new())),
                (bulk("warning"), Frame::Array(Vec::new())),
                (bulk("total_results"), Frame::Integer(total)),
                (bulk("format"), bulk("STRING")),
                (bulk("results"), Frame::Array(results)),
            ])
        } else {
            Frame::Array(flat)
        })
    }

    async fn aggregate(&mut self, a: &[&[u8]]) -> Result<Frame> {
        let name = a.first().map(|x| text(x)).unwrap_or_default();
        let query = a.get(1).map(|x| text(x)).unwrap_or_default();
        let plan = ft_agg::parse_plan(a.get(2..).unwrap_or_default())?;
        let (def, customs) = self.index(&name).await?;
        self.check_read(&def).await?;
        let ast = Self::parse_query(
            &def,
            &customs,
            &query,
            &plan.params,
            plan.dialect,
            Vec::new(),
            0,
        )?;
        let (hits, _) = self.matches(&def, &customs, ast, true, 0).await?;
        // Fields by attribute name: hash fields named by an alias are read
        // under their own name and shown under the alias too.
        let needed = plan.fields_needed();
        let src_of = |n: &str| -> String {
            def.fields_named(n)
                .first()
                .filter(|f| f.source == Source::KvField)
                .map_or_else(|| n.to_owned(), |f| f.source_ref.clone())
        };
        let only: Option<Vec<String>> = needed
            .as_ref()
            .map(|v| v.iter().map(|n| src_of(n)).collect());
        let keys: Vec<Key> = hits.iter().map(|h| h.key.clone()).collect();
        let found = self.reread(&def, &keys, only.as_deref()).await?;
        let mut rows: Vec<Row> = Vec::new();
        for h in &hits {
            let Some(fields) = found.get(&h.key.0) else {
                continue;
            };
            let mut row: Row = Vec::new();
            match &needed {
                None => {
                    for (e, v) in fields {
                        row.push((text(e), AggVal::Str(text(v))));
                    }
                }
                Some(names) => {
                    for n in names {
                        let src = src_of(n);
                        if let Some((_, v)) =
                            fields.iter().find(|(e, _)| e.as_slice() == src.as_bytes())
                        {
                            row.push((n.clone(), AggVal::Str(text(v))));
                        }
                    }
                }
            }
            rows.push(row);
        }
        let (count, rows) = plan.run(rows)?;
        let resp3 = self.call.resp3;
        let count = i64::try_from(count).unwrap_or(i64::MAX);
        let row_frame = |r: &Row| -> Vec<(Frame, Frame)> {
            // A group of hashes without the field shows it as nil.
            r.iter()
                .map(|(k, v)| (bulk(k.clone()), agg_frame(v)))
                .collect()
        };
        Ok(if resp3 {
            Frame::Map(vec![
                (bulk("attributes"), Frame::Array(Vec::new())),
                (bulk("warning"), Frame::Array(Vec::new())),
                (bulk("total_results"), Frame::Integer(count)),
                (bulk("format"), bulk("STRING")),
                (
                    bulk("results"),
                    Frame::Array(
                        rows.iter()
                            .map(|r| {
                                Frame::Map(vec![
                                    (bulk("extra_attributes"), Frame::Map(row_frame(r))),
                                    (bulk("values"), Frame::Array(Vec::new())),
                                ])
                            })
                            .collect(),
                    ),
                ),
            ])
        } else {
            let mut out = vec![Frame::Integer(count)];
            for r in &rows {
                out.push(Frame::Array(
                    row_frame(r).into_iter().flat_map(|(k, v)| [k, v]).collect(),
                ));
            }
            Frame::Array(out)
        })
    }
}

/// A value of `FT.AGGREGATE`'s reply.
fn agg_frame(v: &AggVal) -> Frame {
    match v {
        AggVal::List(l) => Frame::Array(l.iter().map(agg_frame).collect()),
        other => other.reply_text().map_or_else(Frame::null, bulk),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn errors_as_redis_shows_them() {
        assert_eq!(
            error_frame(&not_found("idx")),
            Frame::error("SEARCH_INDEX_NOT_FOUND Index not found: idx")
        );
        assert_eq!(
            error_frame(&ft::arg_error("Unknown argument `X`")),
            Frame::err("Unknown argument `X`")
        );
        assert_eq!(
            error_frame(&SprigError::new(SprigCode::SearchUnsupported, "no GEO")),
            sprig_redis::reply::sprig(SprigCode::SearchUnsupported, "no GEO")
        );
        assert_eq!(
            agg_frame(&AggVal::List(vec![AggVal::Num(2.5), AggVal::Null])),
            Frame::Array(vec![bulk("2.5"), Frame::null()])
        );
    }
}
