//! Compiled `db.call` and `db.publish`: what a deploy tells the compiler
//! about other modules and streams, and the check that keeps compiled
//! callers working when a module they call is deployed again.
//!
//! ```text
//! compile(version v of module m in database d):
//!   callees := every function and procedure of d whose active version
//!              (and candidate, mid-rollout) is compiled, with the same
//!              parameters and fields in both, and fields that aren't JSON
//!   streams := the gateway's list for d, kept when the outbox
//!              __sprig_eo_<id> is a table of the code's shard schema
//! deploy(version v of module m):
//!   for each compiled caller of m (an active or candidate version whose
//!   placement lists m in `calls`):
//!     v must be compiled, take the same parameters, return the same
//!     fields, and write nothing if the caller was made for a callee
//!     that wrote nothing                                     else 39875
//! ```
//!
//! A call reaches the callee through its call entry `__sprig_l<m>_call`
//! ([`crate::deploy::call_entry`]), which every deploy, rollout stage and
//! rollback of the callee re-creates, so a compiled caller follows its
//! versions and rollout without being compiled again.

use std::collections::BTreeMap;

use serde_json::Value;
use sprig_common::{Result, SprigCode, SprigError};
use sprig_logic_compile::ir::{Function, Ty};
use sprig_logic_compile::link::{CallDep, Callee, StreamTarget};
use sprig_logic_compile::{Columns, StaticColumns};

/// The fields a call of `f` returns, as its wrapper's columns: every
/// event's fields once, in event order, without `event`; booleans as
/// integers, since the wrapper casts them.
#[must_use]
pub fn call_fields(f: &Function) -> Vec<(String, Ty)> {
    let mut cols: Vec<(String, Ty)> = Vec::new();
    for e in &f.events {
        for (n, t) in &e.fields {
            if n != "event" && !cols.iter().any(|(c, _)| c == n) {
                cols.push((n.clone(), if *t == Ty::Bool { Ty::Int } else { *t }));
            }
        }
    }
    cols
}

/// The shard schema's columns plus the callees and streams of the code's
/// database.
#[derive(Debug, Clone, Default)]
pub struct Env {
    /// The shard schema's columns.
    pub cols: StaticColumns,
    /// The database's name (a `db.call("shop.quote")` names it).
    pub db_name: String,
    callees: BTreeMap<String, std::result::Result<Callee, String>>,
    streams: BTreeMap<String, std::result::Result<StreamTarget, String>>,
}

impl Env {
    /// `cols` alone.
    #[must_use]
    pub fn new(cols: StaticColumns, db_name: &str) -> Self {
        Self {
            cols,
            db_name: db_name.to_owned(),
            ..Self::default()
        }
    }

    /// Adds the callees [`callees`] read.
    #[must_use]
    pub fn with_callees(
        mut self,
        list: Vec<(String, std::result::Result<Callee, String>)>,
    ) -> Self {
        for (n, c) in list {
            self.callees.insert(n.to_lowercase(), c);
        }
        self
    }

    /// Adds the database's streams; one whose outbox isn't a table of the
    /// shard schema can't be published to from compiled code.
    #[must_use]
    pub fn with_streams(
        mut self,
        list: &[(String, std::result::Result<StreamTarget, String>)],
    ) -> Self {
        for (n, s) in list {
            let s = match s {
                Ok(t) if !self.cols.has_table(&t.outbox) => Err(format!(
                    "stream {n}'s outbox isn't in this code's shard schema (it is attached to \
                     another table's group): publishing to it runs in the engine tier"
                )),
                other => other.clone(),
            };
            self.streams.insert(n.to_lowercase(), s);
        }
        self
    }

    fn local<'n>(&self, name: &'n str, what: &str) -> std::result::Result<&'n str, String> {
        match name.split_once('.') {
            Some((d, n)) if d.eq_ignore_ascii_case(&self.db_name) => Ok(n),
            Some((d, _)) => Err(format!(
                "{what} of database {d} runs in the engine tier: compiled code reaches only its \
                 own database"
            )),
            None => Ok(name),
        }
    }
}

impl Columns for Env {
    fn column(&self, table: &str, column: &str) -> Option<Ty> {
        self.cols.column(table, column)
    }

    fn has_table(&self, table: &str) -> bool {
        self.cols.has_table(table)
    }

    fn callee(&self, name: &str) -> Option<std::result::Result<Callee, String>> {
        let n = match self.local(name, "a call") {
            Ok(n) => n,
            Err(e) => return Some(Err(e)),
        };
        self.callees.get(&n.to_lowercase()).cloned()
    }

    fn stream(&self, name: &str) -> Option<std::result::Result<StreamTarget, String>> {
        let n = match self.local(name, "a publish to a stream") {
            Ok(n) => n,
            Err(e) => return Some(Err(e)),
        };
        self.streams.get(&n.to_lowercase()).cloned()
    }
}

fn strings(v: &Value) -> Vec<String> {
    v.as_array()
        .into_iter()
        .flatten()
        .filter_map(|x| x.as_str().map(str::to_owned))
        .collect()
}

/// One version of a callee, as the catalog has it.
struct Ver {
    version: u64,
    tier: String,
    func: Function,
    placement: Value,
}

/// Every function and procedure of database `db_id`, by name: what a
/// compiled `db.call` of it needs, or why calls of it stay in the engine
/// tier.
///
/// # Errors
/// The catalog's.
pub async fn callees(
    c: &mut dyn sprig_catalog::records::RecordTx,
    db_id: u32,
) -> Result<Vec<(String, std::result::Result<Callee, String>)>> {
    let rows: Vec<(u64, String, String, u64, String, Value, Value)> = crate::LogicStore::new("")
        .live_versions(c, db_id)
        .await?
        .into_iter()
        .filter(|v| matches!(v.kind.as_str(), "function" | "procedure"))
        .map(|v| {
            (
                v.module_id,
                v.name,
                v.kind,
                v.version,
                v.tier,
                v.signature,
                v.placement,
            )
        })
        .collect();
    let mut by: BTreeMap<u64, (String, String, Vec<Ver>)> = BTreeMap::new();
    for (m, name, kind, version, tier, sig, placement) in rows {
        let e = by
            .entry(m)
            .or_insert_with(|| (name.clone(), kind, Vec::new()));
        e.2.push(Ver {
            version,
            tier,
            func: crate::rollout::function_of(&name, &sig),
            placement,
        });
    }
    let mut names: BTreeMap<String, usize> = BTreeMap::new();
    for (name, _, _) in by.values() {
        *names.entry(name.to_lowercase()).or_default() += 1;
    }
    let mut out = Vec::new();
    for (module_id, (name, kind, vers)) in by {
        if names.get(&name.to_lowercase()).copied().unwrap_or(0) > 1 {
            out.push((
                name.clone(),
                Err(format!(
                    "{name} names both a function and a procedure: calls of it run in the engine tier"
                )),
            ));
            continue;
        }
        out.push((name.clone(), callee_of(module_id, &name, &kind, &vers)));
    }
    Ok(out)
}

fn callee_of(
    module_id: u64,
    name: &str,
    kind: &str,
    vers: &[Ver],
) -> std::result::Result<Callee, String> {
    let Some(first) = vers.first() else {
        return Err(format!("{name} has no active version"));
    };
    let mut callee = Callee {
        module_id,
        name: name.to_owned(),
        params: first.func.params.iter().map(|p| p.ty).collect(),
        fields: call_fields(&first.func),
        reads: Vec::new(),
        writes: Vec::new(),
        reaches: Vec::new(),
        depth: 0,
    };
    for v in vers {
        if v.tier != "mariadb" {
            return Err(format!(
                "{kind} {name} version {} runs in the engine tier, which compiled code can't call",
                v.version
            ));
        }
        let params: Vec<Ty> = v.func.params.iter().map(|p| p.ty).collect();
        if params != callee.params || call_fields(&v.func) != callee.fields {
            return Err(format!(
                "{name} is in a staged rollout whose versions take or return different values: \
                 calls of it run in the engine tier until it ends"
            ));
        }
        for r in strings(&v.placement["reads"]) {
            if !callee.reads.contains(&r) {
                callee.reads.push(r);
            }
        }
        for w in strings(&v.placement["writes"]) {
            if !callee.writes.contains(&w) {
                callee.writes.push(w);
            }
        }
        let reaches = v.placement["reaches"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_u64)
            .chain(
                v.placement["calls"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|c| c["module"].as_u64()),
            );
        for m in reaches {
            if !callee.reaches.contains(&m) {
                callee.reaches.push(m);
            }
        }
        let depth = v.placement["call_depth"].as_u64().unwrap_or(0);
        callee.depth = callee.depth.max(u32::try_from(depth).unwrap_or(u32::MAX));
    }
    if let Some((f, t)) = callee
        .fields
        .iter()
        .find(|(_, t)| matches!(t, Ty::Json | Ty::Null))
    {
        return Err(format!(
            "{name} returns {f} as {t}, which a caller reads differently in each tier: calls of \
             it run in the engine tier"
        ));
    }
    Ok(callee)
}

/// A version about to deploy, as [`check_callers`] sees it.
#[derive(Debug, Clone, Copy)]
pub struct NewVersion<'a> {
    /// The module.
    pub module_id: u64,
    /// Its name.
    pub name: &'a str,
    /// Its signature.
    pub func: &'a Function,
    /// It runs compiled.
    pub compiled: bool,
    /// It writes anything.
    pub writes: bool,
}

/// Refuses deploying version `v` of a module of database `db_id` when
/// compiled code calls the module and was made for another signature or
/// tier: 39875 naming the callers.
///
/// # Errors
/// 39875; the catalog's.
pub async fn check_callers(
    c: &mut dyn sprig_catalog::records::RecordTx,
    db_id: u32,
    v: NewVersion<'_>,
) -> Result<()> {
    type Row = (String, String, u64, String);
    let NewVersion {
        module_id,
        name,
        func: f,
        compiled,
        writes,
    } = v;
    let rows: Vec<Row> = crate::LogicStore::new("")
        .live_versions(c, db_id)
        .await?
        .into_iter()
        .filter(|x| {
            x.module_id != module_id && x.tier == "mariadb" && x.placement.get("calls").is_some()
        })
        .map(|x| (x.name, x.kind, x.version, x.placement.to_string()))
        .collect();
    let params: Vec<String> = f.params.iter().map(|p| p.ty.name().to_owned()).collect();
    let fields: Vec<(String, String)> = call_fields(f)
        .into_iter()
        .map(|(n, t)| (n, t.name().to_owned()))
        .collect();
    let mut callers = Vec::new();
    let mut why = String::new();
    for (cname, ckind, cver, placement) in rows {
        let p: Value = serde_json::from_str(&placement).unwrap_or(Value::Null);
        let deps: Vec<CallDep> = p["calls"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|d| {
                Some(CallDep {
                    module: d["module"].as_u64()?,
                    name: d["name"].as_str()?.to_owned(),
                    params: strings(&d["params"]),
                    fields: d["fields"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter_map(|x| {
                            Some((x[0].as_str()?.to_owned(), x[1].as_str()?.to_owned()))
                        })
                        .collect(),
                    writes: d["writes"].as_bool().unwrap_or(true),
                })
            })
            .filter(|d| d.module == module_id)
            .collect();
        for d in deps {
            let reason = if !compiled {
                Some("runs in the engine tier".to_owned())
            } else if d.params != params {
                Some(format!(
                    "takes ({}) where they were compiled for ({})",
                    params.join(", "),
                    d.params.join(", ")
                ))
            } else if d.fields != fields {
                Some("returns other fields than they were compiled for".to_owned())
            } else if writes && !d.writes {
                Some(
                    "writes data, and they were compiled for a version that writes nothing"
                        .to_owned(),
                )
            } else {
                None
            };
            if let Some(r) = reason {
                if why.is_empty() {
                    why = r;
                }
                callers.push(format!("{ckind} {cname} (version {cver})"));
            }
        }
    }
    if callers.is_empty() {
        return Ok(());
    }
    Err(SprigError::new(
        SprigCode::LogicInUse,
        format!(
            "{name} is called by compiled code: {}; this version {why}. Deploy those callers \
             again WITH (SPRIG_TIER = 'engine') first, or keep {name} compiled with the same \
             parameters and fields",
            callers.join(", ")
        ),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sprig_logic_compile::ir::EventDef;

    fn func(events: Vec<(&str, Vec<(&str, Ty)>)>) -> Function {
        Function {
            name: "f".into(),
            events: events
                .into_iter()
                .map(|(n, fs)| EventDef {
                    name: n.into(),
                    fields: fs.into_iter().map(|(a, t)| (a.into(), t)).collect(),
                })
                .collect(),
            ..Function::default()
        }
    }

    #[test]
    fn fields_follow_the_wrapper() {
        let f = func(vec![
            ("output", vec![("total", Ty::Float), ("ok", Ty::Bool)]),
            ("error", vec![("message", Ty::Str), ("total", Ty::Int)]),
        ]);
        assert_eq!(
            call_fields(&f),
            vec![
                ("total".to_owned(), Ty::Float),
                ("ok".to_owned(), Ty::Int),
                ("message".to_owned(), Ty::Str)
            ]
        );
    }

    fn ver(version: u64, tier: &str, f: Function, placement: Value) -> Ver {
        Ver {
            version,
            tier: tier.into(),
            func: f,
            placement,
        }
    }

    #[test]
    fn callees_need_compiled_versions_of_one_shape() {
        let f = func(vec![("output", vec![("v", Ty::Int)])]);
        let ok = callee_of(
            7,
            "q",
            "function",
            &[ver(
                2,
                "mariadb",
                f.clone(),
                serde_json::json!({"reads": ["t"], "writes": ["u"], "calls": [{"module": 3}], "reaches": [4], "call_depth": 2}),
            )],
        )
        .unwrap();
        assert_eq!(ok.reads, vec!["t".to_owned()]);
        assert_eq!(ok.writes, vec!["u".to_owned()]);
        assert_eq!(ok.reaches, vec![4, 3]);
        assert_eq!(ok.depth, 2);
        assert_eq!(ok.entry(), "__sprig_l7_call");
        let engine = callee_of(
            7,
            "q",
            "function",
            &[ver(2, "engine", f.clone(), Value::Null)],
        );
        assert!(engine.unwrap_err().contains("engine tier"));
        let g = func(vec![("output", vec![("v", Ty::Str)])]);
        let mixed = callee_of(
            7,
            "q",
            "function",
            &[
                ver(2, "mariadb", f, Value::Null),
                ver(3, "mariadb", g, Value::Null),
            ],
        );
        assert!(mixed.unwrap_err().contains("rollout"));
        let j = func(vec![("output", vec![("doc", Ty::Json)])]);
        assert!(callee_of(7, "q", "function", &[ver(1, "mariadb", j, Value::Null)]).is_err());
    }

    #[test]
    fn names_of_other_databases_stay_in_the_engine_tier() {
        let env = Env::new(
            StaticColumns::new().table("__sprig_eo_5", &[("k", Ty::Str)]),
            "shop",
        )
        .with_callees(vec![("Quote".into(), Err("no".into()))])
        .with_streams(&[
            (
                "orders".into(),
                Ok(StreamTarget {
                    name: "orders".into(),
                    outbox: "__sprig_eo_5".into(),
                    max_bytes: 10,
                    compact: false,
                }),
            ),
            (
                "elsewhere".into(),
                Ok(StreamTarget {
                    name: "elsewhere".into(),
                    outbox: "__sprig_eo_6".into(),
                    max_bytes: 10,
                    compact: false,
                }),
            ),
        ]);
        assert!(env.callee("shop.quote").is_some());
        assert!(env.callee("other.quote").unwrap().is_err());
        assert!(env.callee("nothing").is_none());
        assert!(env.stream("SHOP.orders").unwrap().is_ok());
        assert!(env.stream("elsewhere").unwrap().is_err());
    }
}
