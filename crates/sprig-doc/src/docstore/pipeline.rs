//! Aggregation over the store: the leading `$match` (with a following
//! `$sort` and `$limit`) becomes the read's plan; every other stage runs
//! in memory with the same stage code the SQL path uses
//! ([`crate::agg::run_stage`]), except the stages that read or write other
//! collections (`$lookup`, `$graphLookup`, `$unionWith`, `$out`,
//! `$merge`), which read and write the store.

use std::collections::HashSet;
use std::sync::Arc;

use bson::{Bson, Document, doc};
use sprig_proto_mongo::errors::{MongoError, code};

use super::read::{self, Scan, Want};
use super::{Coll, DocStore, DocTxn, Name};
use crate::agg::{self, AggLimits, GraphSpec};
use crate::filter::{self, Pred};

fn unsupported(stage: &str) -> MongoError {
    MongoError::new(
        code::SPRIG_DOC_FEATURE_UNSUPPORTED,
        format!("{stage} is not supported on collections stored on the SprigDB engine yet"),
    )
}

/// Where an aggregation's reads come from: the caller's transaction, or
/// one opened for the whole aggregation (one snapshot for every stage).
#[allow(clippy::large_enum_variant)] // one per aggregation
enum Reader<'a> {
    Txn(&'a mut DocTxn),
    Own(DocTxn),
}

impl Reader<'_> {
    fn txn(&mut self) -> &mut DocTxn {
        match self {
            Reader::Txn(t) => t,
            Reader::Own(t) => t,
        }
    }
}

impl DocStore {
    /// Documents of `c` matching `pred` through `plan`, stopping after
    /// `limit` (0: all), in `t`.
    fn read_with(
        &self,
        t: &mut DocTxn,
        c: &Coll,
        pred: &Pred,
        plan: &read::Plan,
        limit: usize,
        max_bytes: usize,
    ) -> Result<Vec<Document>, MongoError> {
        let cx = agg::AggExpr { now: self.now() };
        let mut out = Vec::new();
        let mut bytes = 0usize;
        let mut over = false;
        Scan::new(plan).run(t, c, pred, &cx, |_, d| {
            bytes += crate::spill::doc_bytes(&d);
            if bytes > max_bytes {
                over = true;
                return false;
            }
            out.push(d);
            limit == 0 || out.len() < limit
        })?;
        if over {
            return Err(MongoError::new(
                code::QUERY_EXCEEDED_MEMORY_LIMIT,
                format!(
                    "Exceeded memory limit for $group, but didn't allow external sort. Pass allowDiskUse:true to opt in. (limit {max_bytes} bytes)"
                ),
            ));
        }
        Ok(out)
    }

    /// Every document of `db.coll` matching `filter`, in `t` (none when the
    /// collection is missing).
    fn read_filter(
        &self,
        t: &mut DocTxn,
        tenant: u64,
        db: &str,
        coll: &str,
        f: &Document,
    ) -> Result<Vec<Document>, MongoError> {
        let Some(c) = self.coll(tenant, &Name::new(db, coll))? else {
            return Ok(Vec::new());
        };
        let pred = filter::parse(f)?;
        let plan = read::plan(&c, &pred, &Want::default())?;
        self.read_with(t, &c, &pred, &plan, 0, self.stage_bytes)
    }

    /// Runs `pipeline` over collection `db.coll` (`coll` empty:
    /// `aggregate: 1`), reading in `txn` when given.
    ///
    /// # Errors
    /// The stages' errors; 292 past the stage memory limit.
    pub async fn aggregate(
        &self,
        tenant: u64,
        db: &str,
        coll: &str,
        pipeline: &[Document],
        txn: Option<&mut DocTxn>,
    ) -> Result<Vec<Document>, MongoError> {
        let in_txn = txn.is_some();
        let mut r = match txn {
            Some(t) => Reader::Txn(t),
            None => Reader::Own(self.begin(false)),
        };
        let res = Box::pin(self.aggregate_in(&mut r, tenant, db, coll, pipeline)).await;
        if let Reader::Own(t) = r {
            t.finish_read().await?;
        }
        let (docs, writes) = res?;
        if let Some(w) = writes {
            if in_txn {
                return Err(MongoError::new(
                    code::OPERATION_NOT_SUPPORTED_IN_TRANSACTION,
                    "$out and $merge cannot be used in a transaction",
                ));
            }
            Box::pin(self.write_out(tenant, db, &docs, &w)).await?;
            return Ok(Vec::new());
        }
        Ok(docs)
    }

    /// The pipeline's documents, and its final `$out` or `$merge` stage
    /// (run by the caller after the reads end).
    async fn aggregate_in(
        &self,
        r: &mut Reader<'_>,
        tenant: u64,
        db: &str,
        coll: &str,
        pipeline: &[Document],
    ) -> Result<(Vec<Document>, Option<Document>), MongoError> {
        let now = self.now();
        let lim = AggLimits {
            stage_bytes: self.stage_bytes,
            now,
        };
        // A database-level pipeline's first stage is checked first.
        if coll.is_empty()
            && let Some(s0) = pipeline.first()
        {
            let name = s0.keys().next().map_or("", String::as_str);
            if name != "$documents" {
                if let Some(e) = agg::db_level_error(name) {
                    return Err(e);
                }
                return Err(MongoError::new(
                    code::INVALID_NAMESPACE,
                    format!(
                        "{{aggregate: 1}} is not valid for '{name}'; a collection is required."
                    ),
                ));
            }
        }
        for (i, s) in pipeline.iter().enumerate() {
            let name = s.keys().next().map_or("", String::as_str);
            if matches!(name, "$out" | "$merge") && i + 1 != pipeline.len() {
                return Err(MongoError::new(
                    40601,
                    format!("{name} can only be the final stage in the pipeline"),
                ));
            }
            if name == "$changeStream" {
                return Err(MongoError::new(
                    code::COMMAND_NOT_SUPPORTED,
                    "change streams are not supported on collections stored on the SprigDB engine yet",
                ));
            }
            if i > 0
                && matches!(
                    name,
                    "$documents"
                        | "$geoNear"
                        | "$collStats"
                        | "$indexStats"
                        | "$search"
                        | "$vectorSearch"
                )
            {
                return Err(MongoError::new(
                    40602,
                    format!("{name} is only valid as the first stage in a pipeline"),
                ));
            }
        }
        let mut stages = pipeline;
        let mut docs: Vec<Document>;
        if coll.is_empty() {
            let Some(s0) = stages.first() else {
                return Ok((Vec::new(), None));
            };
            let (name, arg) = s0
                .iter()
                .next()
                .map_or(("", &Bson::Null), |(k, v)| (k.as_str(), v));
            if name == "$documents" {
                docs = agg::documents(arg, &lim)?;
                stages = &stages[1..];
            } else if let Some(e) = agg::db_level_error(name) {
                return Err(e);
            } else {
                return Err(MongoError::new(
                    code::INVALID_NAMESPACE,
                    format!("{name} is not allowed on a database-level aggregation"),
                ));
            }
        } else {
            if let Some(s0) = stages.first() {
                let name = s0.keys().next().map_or("", String::as_str);
                if matches!(
                    name,
                    "$geoNear"
                        | "$search"
                        | "$searchMeta"
                        | "$vectorSearch"
                        | "$collStats"
                        | "$indexStats"
                        | "$planCacheStats"
                        | "$listSearchIndexes"
                        | "$rankFusion"
                        | "$scoreFusion"
                ) {
                    return Err(unsupported(name));
                }
            }
            // Leading `$match`es, then a `$sort` and a `$limit`.
            let mut conds = Vec::new();
            while let Some(m) = stages
                .first()
                .filter(|s| s.len() == 1)
                .and_then(|s| s.get_document("$match").ok())
            {
                if crate::text::split_text(m)?.is_some() {
                    return Err(unsupported("$text"));
                }
                if !m.is_empty() {
                    conds.push(Bson::Document(m.clone()));
                }
                stages = &stages[1..];
            }
            let f = if conds.is_empty() {
                Document::new()
            } else {
                doc! {"$and": conds}
            };
            let pred = filter::parse(&f)?;
            let c = self.coll(tenant, &Name::new(db, coll))?;
            let sort = stages
                .first()
                .and_then(|s| s.get_document("$sort").ok())
                .map(agg::parse_sort)
                .transpose()?;
            docs = match c {
                None => Vec::new(),
                Some(c) => {
                    let want = Want {
                        sort: sort.clone().unwrap_or_default(),
                        ..Want::default()
                    };
                    let plan = read::plan(&c, &pred, &want)?;
                    let mut limit = 0usize;
                    if sort.is_some() && plan.sorted {
                        stages = &stages[1..];
                        if let Some(n) = stages
                            .first()
                            .and_then(|s| s.get("$limit"))
                            .and_then(filter::as_i64)
                            .filter(|n| *n > 0)
                        {
                            limit = usize::try_from(n).unwrap_or(usize::MAX);
                        }
                    }
                    self.read_with(r.txn(), &c, &pred, &plan, limit, self.stage_bytes)?
                }
            };
        }
        let mut tail = None;
        for s in stages {
            let name = s.keys().next().map_or("", String::as_str).to_owned();
            let arg = s.get(&name).cloned().unwrap_or(Bson::Null);
            docs = match name.as_str() {
                "$lookup" => {
                    let spec = arg.as_document().ok_or_else(|| {
                        MongoError::new(
                            code::FAILED_TO_PARSE,
                            "the $lookup specification must be an Object",
                        )
                    })?;
                    Box::pin(self.lookup(r, tenant, db, docs, spec, &lim)).await?
                }
                "$graphLookup" => {
                    let spec = GraphSpec::parse(&arg)?;
                    let t = r.txn();
                    let mut out = Vec::with_capacity(docs.len());
                    for d in docs {
                        out.push(
                            spec.search(d, &lim, |f| {
                                self.read_filter(t, tenant, db, &spec.from, f)
                            })?,
                        );
                    }
                    out
                }
                "$unionWith" => {
                    let (other, pipe) = match &arg {
                        Bson::String(c) => (c.clone(), Vec::new()),
                        Bson::Document(d) => (
                            d.get_str("coll").unwrap_or_default().to_owned(),
                            d.get_array("pipeline")
                                .map(|a| {
                                    a.iter().filter_map(|x| x.as_document().cloned()).collect()
                                })
                                .unwrap_or_default(),
                        ),
                        _ => {
                            return Err(MongoError::new(
                                code::FAILED_TO_PARSE,
                                "the $unionWith stage specification must be an object or string",
                            ));
                        }
                    };
                    let (more, w) =
                        Box::pin(self.aggregate_in(r, tenant, db, &other, &pipe)).await?;
                    if w.is_some() {
                        return Err(MongoError::new(
                            31441,
                            "$out and $merge are not allowed inside $unionWith",
                        ));
                    }
                    docs.extend(more);
                    docs
                }
                "$out" | "$merge" => {
                    tail = Some(s.clone());
                    docs
                }
                "$geoNear" | "$search" | "$searchMeta" | "$vectorSearch" | "$collStats"
                | "$indexStats" | "$planCacheStats" => return Err(unsupported(&name)),
                _ => agg::run_stage(docs, s, &lim)?,
            };
        }
        Ok((docs, tail))
    }

    async fn lookup(
        &self,
        r: &mut Reader<'_>,
        tenant: u64,
        db: &str,
        docs: Vec<Document>,
        spec: &Document,
        lim: &AggLimits,
    ) -> Result<Vec<Document>, MongoError> {
        let from = match spec.get("from") {
            Some(Bson::String(s)) => s.clone(),
            Some(Bson::Document(d)) => d.get_str("coll").unwrap_or_default().to_owned(),
            _ => {
                return Err(MongoError::new(
                    code::FAILED_TO_PARSE,
                    "missing 'from' option to $lookup stage specification",
                ));
            }
        };
        let as_f = spec.get_str("as").map_err(|_| {
            MongoError::new(
                code::FAILED_TO_PARSE,
                "must specify 'as' field for a $lookup",
            )
        })?;
        let local = spec.get_str("localField").ok();
        let foreign = spec.get_str("foreignField").ok();
        let pipe: Vec<Document> = spec
            .get_array("pipeline")
            .map(|a| a.iter().filter_map(|x| x.as_document().cloned()).collect())
            .unwrap_or_default();
        let lets = spec.get_document("let").ok();
        let mut out = Vec::with_capacity(docs.len());
        let mut bytes = 0usize;
        if lets.is_some_and(|l| !l.is_empty()) {
            // A correlated pipeline: run once per input document with its
            // variables bound.
            let lets = lets.cloned().unwrap_or_default();
            for d in docs {
                let vars = agg::Vars::new(&d, lim.now);
                let mut bound = Document::new();
                for (k, e) in &lets {
                    bound.insert(k.clone(), agg::eval(e, &vars)?);
                }
                let p = agg::bind_pipeline(&pipe, &bound);
                let mut p2 = Vec::new();
                if let (Some(l), Some(f)) = (local, foreign) {
                    let mut keys: Vec<Bson> = filter::values_at(&d, l)
                        .vals
                        .into_iter()
                        .filter(|v| !matches!(v, Bson::Array(_)))
                        .cloned()
                        .collect();
                    if keys.is_empty() {
                        keys.push(Bson::Null);
                    }
                    p2.push(doc! {"$match": {f: {"$in": keys}}});
                }
                p2.extend(p);
                let (fd, _) = Box::pin(self.aggregate_in(r, tenant, db, &from, &p2)).await?;
                let joined = crate::engine::join_chunk(
                    std::slice::from_ref(&d),
                    &fd,
                    local.zip(foreign),
                    as_f,
                    lim,
                    &mut bytes,
                )?;
                out.extend(joined);
            }
            return Ok(out);
        }
        for chunk in docs.chunks(500) {
            let p = match (local, foreign) {
                (Some(l), Some(f)) => {
                    let mut keys: Vec<Bson> = Vec::new();
                    let mut seen = HashSet::new();
                    for d in chunk {
                        let vs = filter::values_at(d, l);
                        if vs.vals.iter().all(|v| matches!(v, Bson::Array(_)))
                            && seen.insert(crate::keyenc::eq_key(&Bson::Null))
                        {
                            keys.push(Bson::Null);
                        }
                        for v in vs.vals {
                            if !matches!(v, Bson::Array(_)) && seen.insert(crate::keyenc::eq_key(v))
                            {
                                keys.push(v.clone());
                            }
                        }
                    }
                    let mut p = vec![doc! {"$match": {f: {"$in": keys}}}];
                    p.extend(pipe.iter().cloned());
                    p
                }
                _ => pipe.clone(),
            };
            let (fd, _) = Box::pin(self.aggregate_in(r, tenant, db, &from, &p)).await?;
            out.extend(crate::engine::join_chunk(
                chunk,
                &fd,
                local.zip(foreign),
                as_f,
                lim,
                &mut bytes,
            )?);
        }
        Ok(out)
    }

    /// `$out` or `$merge`: writes `docs` into the target.
    async fn write_out(
        &self,
        tenant: u64,
        db: &str,
        docs: &[Document],
        stage: &Document,
    ) -> Result<(), MongoError> {
        let (name, arg) = stage
            .iter()
            .next()
            .map_or(("", &Bson::Null), |(k, v)| (k.as_str(), v));
        let target = match arg {
            Bson::String(c) => Name::new(db, c),
            Bson::Document(d) => {
                let into = if name == "$merge" {
                    d.get("into").cloned().unwrap_or(Bson::Null)
                } else {
                    arg.clone()
                };
                match into {
                    Bson::String(c) => Name::new(db, &c),
                    Bson::Document(i) => Name::new(
                        i.get_str("db").unwrap_or(db),
                        i.get_str("coll").unwrap_or_default(),
                    ),
                    _ => {
                        return Err(MongoError::new(
                            code::FAILED_TO_PARSE,
                            format!("{name} needs a target collection"),
                        ));
                    }
                }
            }
            _ => {
                return Err(MongoError::new(
                    code::FAILED_TO_PARSE,
                    format!("{name} needs a target collection"),
                ));
            }
        };
        // The guard first: the index list read after it is the one every
        // write here maintains.
        let _g = self.write_guard(tenant).await;
        let c = self.coll_or_create(tenant, &target).await?;
        if name == "$out" {
            return self.replace_all(&c, docs).await;
        }
        let d = arg.as_document().cloned().unwrap_or_default();
        let on: Vec<String> = match d.get("on") {
            None => vec!["_id".to_owned()],
            Some(Bson::String(s)) => vec![s.clone()],
            Some(Bson::Array(a)) => a
                .iter()
                .filter_map(|x| x.as_str().map(str::to_owned))
                .collect(),
            Some(_) => {
                return Err(MongoError::new(
                    51186,
                    "$merge 'on' field must be either a string or an array of strings",
                ));
            }
        };
        if on != ["_id"] {
            let mut want: Vec<&str> = on.iter().map(String::as_str).collect();
            want.sort_unstable();
            let ok = c.meta.indexes.iter().any(|i| {
                let mut f: Vec<&str> = i.fields.iter().map(|x| x.0.as_str()).collect();
                f.sort_unstable();
                i.unique && i.partial.is_none() && f == want
            });
            if !ok {
                return Err(MongoError::new(
                    51183,
                    "Cannot find index to verify that join fields will be unique",
                ));
            }
        }
        let when_matched = match d.get("whenMatched") {
            None => "merge".to_owned(),
            Some(Bson::String(s)) => s.clone(),
            Some(_) => return Err(unsupported("$merge with a whenMatched pipeline")),
        };
        let when_not = d.get_str("whenNotMatched").unwrap_or("insert").to_owned();
        let mut t = self.begin(false);
        let r = async {
            for nd in docs {
                let mut f = Document::new();
                for p in &on {
                    let v = crate::update::get_path(nd, p).cloned().unwrap_or(Bson::Null);
                    f.insert(p.clone(), doc! {"$eq": v});
                }
                let pred = filter::parse(&f)?;
                let hit = self.targets(&mut t, &c, &pred, &Vec::new(), None, 1)?;
                match hit.into_iter().next() {
                    Some((idk, old)) => {
                        let new = match when_matched.as_str() {
                            "replace" => {
                                let mut n = Document::new();
                                n.insert("_id", old.get("_id").cloned().unwrap_or(Bson::Null));
                                for (k, v) in nd {
                                    if k != "_id" {
                                        n.insert(k.clone(), v.clone());
                                    }
                                }
                                n
                            }
                            "merge" => {
                                // The stored `_id` stays (an `on` other than
                                // `_id` may match a document with another).
                                let mut n = old.clone();
                                for (k, v) in nd {
                                    if k != "_id" {
                                        n.insert(k.clone(), v.clone());
                                    }
                                }
                                n
                            }
                            "keepExisting" => continue,
                            "fail" => {
                                return Err(MongoError::new(
                                    code::DUPLICATE_KEY,
                                    format!(
                                        "$merge with whenMatched: fail found an existing document with the same values for the 'on' fields: {f}"
                                    ),
                                ));
                            }
                            other => {
                                return Err(MongoError::new(
                                    code::BAD_VALUE,
                                    format!("Enumeration value '{other}' for field 'whenMatched' is not a valid value."),
                                ));
                            }
                        };
                        let Some(cur) = self.lock_current(&mut t, &c, &idk, &Pred::True).await? else {
                            continue;
                        };
                        let op = if when_matched == "replace" {
                            sprig_engine::observe::hint::HintOp::Replace
                        } else {
                            sprig_engine::observe::hint::HintOp::Update
                        };
                        self.replace_doc(&mut t, &c, &idk, &cur, &new, op).await?;
                    }
                    None => match when_not.as_str() {
                        "insert" => {
                            self.insert_doc(&mut t, &c, nd.clone()).await?;
                        }
                        "discard" => {}
                        "fail" => {
                            return Err(MongoError::new(
                                13113,
                                "$merge could not find a matching document in the target collection for at least one document in the source collection",
                            ));
                        }
                        other => {
                            return Err(MongoError::new(
                                code::BAD_VALUE,
                                format!("Enumeration value '{other}' for field 'whenNotMatched' is not a valid value."),
                            ));
                        }
                    },
                }
            }
            Ok::<(), MongoError>(())
        }
        .await;
        match r {
            Ok(()) => t.commit().await,
            Err(e) => {
                t.rollback();
                Err(e)
            }
        }
    }

    /// `$out`: the target's documents become `docs`, its indexes stay.
    async fn replace_all(&self, c: &Arc<Coll>, docs: &[Document]) -> Result<(), MongoError> {
        let mut t = self.begin(false);
        let r = async {
            let existing = self.targets(&mut t, c, &Pred::True, &Vec::new(), None, 0)?;
            for (idk, _) in existing {
                if let Some(cur) = self.lock_current(&mut t, c, &idk, &Pred::True).await? {
                    self.delete_doc(&mut t, c, &idk, &cur).await?;
                }
            }
            for d in docs {
                self.insert_doc(&mut t, c, d.clone()).await?;
            }
            Ok::<(), MongoError>(())
        }
        .await;
        match r {
            Ok(()) => t.commit().await,
            Err(e) => {
                t.rollback();
                Err(e)
            }
        }
    }

    /// Counts the documents of `c` matching `filter`, after `skip`, at most
    /// `limit` (0: all).
    ///
    /// # Errors
    /// Parse errors; a bad hint.
    pub async fn count(
        &self,
        c: &Coll,
        f: &Document,
        skip: u64,
        limit: u64,
        hint: Option<&Bson>,
        txn: Option<&mut DocTxn>,
    ) -> Result<u64, MongoError> {
        let pred = filter::parse(f)?;
        let want = Want {
            hint,
            ..Want::default()
        };
        let plan = read::plan(c, &pred, &want)?;
        let cx = agg::AggExpr { now: self.now() };
        let mut n = 0u64;
        let cap = if limit == 0 {
            u64::MAX
        } else {
            skip.saturating_add(limit)
        };
        let mut scan = Scan::new(&plan);
        let mut take = |_: Vec<u8>, _: Document| {
            n += 1;
            n < cap
        };
        match txn {
            Some(t) => scan.run(t, c, &pred, &cx, &mut take)?,
            None => {
                let mut t = self.begin(false);
                let r = scan.run(&mut t, c, &pred, &cx, &mut take);
                t.finish_read().await?;
                r?;
            }
        }
        Ok(n.saturating_sub(skip)
            .min(if limit == 0 { u64::MAX } else { limit }))
    }

    /// The distinct values of `key` over the documents of `c` matching
    /// `filter` (arrays contribute their elements).
    ///
    /// # Errors
    /// Parse errors.
    pub async fn distinct(
        &self,
        c: &Coll,
        key: &str,
        f: &Document,
        txn: Option<&mut DocTxn>,
    ) -> Result<Vec<Bson>, MongoError> {
        let pred = filter::parse(f)?;
        let plan = read::plan(c, &pred, &Want::default())?;
        let docs = self
            .collect(c, &pred, &plan, txn, Some(self.stage_bytes))
            .await?;
        Ok(distinct_values(&docs, key))
    }
}

/// The distinct values of `key` over `docs`, MongoDB's way.
#[must_use]
pub fn distinct_values(docs: &[Document], key: &str) -> Vec<Bson> {
    {
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        for d in docs {
            // A leaf array gives its elements (an inner array stays one
            // value); a missing field gives nothing.
            for v in filter::values_at(d, key).leaves {
                let vals: Vec<&Bson> = match v {
                    Bson::Array(a) => a.iter().collect(),
                    other => vec![other],
                };
                for x in vals {
                    if seen.insert(crate::keyenc::eq_key(x)) {
                        out.push(x.clone());
                    }
                }
            }
        }
        out.sort_by(crate::keyenc::compare);
        out
    }
}
