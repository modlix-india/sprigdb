//! Staged rollout of functions and procedures.
//!
//! ```text
//! CREATE OR REPLACE FUNCTION f ... WITH (SPRIG_ROLLOUT = 10
//!     [, SPRIG_ROLLOUT_BY = 'user' | 'tenant' | 'key:<parameter>']) AS ...
//!   → version n+1 is the candidate for 10% of callers; the active version
//!     answers the rest
//! ALTER FUNCTION f ROLLOUT TO 50 [PERCENT] [BY USER | TENANT]   another stage
//! ALTER FUNCTION f ROLLOUT PROMOTE        (or TO 100) the candidate becomes active
//! ALTER FUNCTION f ROLLOUT ABORT          (or TO 0)   the active version alone again
//! ```
//!
//! **Who gets the candidate.** The function's wrapper procedure (the one
//! every caller reaches, whatever the protocol) holds both versions'
//! bodies and picks one per call:
//!
//! ```text
//! BY USER (default): CRC32(CONCAT(<the caller's backend account>, ':', module)) % 100 < pct
//! BY TENANT:         CRC32(CONCAT('t<tenant>', ':', module)) % 100 < pct, decided at deploy
//! BY KEY p:          CRC32(CONCAT(COALESCE(CAST(<argument p> AS CHAR), ''), ':', module))
//!                    % 100 < pct: one key (a customer, a tenant id passed in) keeps
//!                    its version whoever calls
//! ```
//!
//! **Triggers on streams and queues** roll out by key too: `CREATE OR
//! REPLACE TRIGGER ... ON STREAM | ON QUEUE ... WITH (SPRIG_ROLLOUT = n)`
//! makes the new version the candidate for the records whose key (a
//! stream record's key; a queue message's `message-id` property, or its
//! own id when it has none) falls in the first n buckets, by the same
//! [`bucket`]. The gateway picks for each run, so a key gets the same
//! version whichever tier each version runs in. A record without a key
//! gets the active version. `ALTER TRIGGER t ROLLOUT TO n | PROMOTE |
//! ABORT` moves it on. After-commit triggers too: the worker picks per
//! change by the row's primary key as the trigger runner writes it
//! (`id=7`), counts both versions' runs and rolls a candidate that fails
//! more back (`crate::triggers::maria::CatalogVersions`).
//!
//! So a caller keeps the version it got while the stage lasts, and a
//! larger stage keeps everyone the smaller one had. The gateway computes
//! the same pick ([`picks_candidate`]) to count each call against the
//! version that ran (engine-tier calls name their version anyway).
//!
//! **Numbers and the automatic rollback.** Gateways count calls, errors
//! and a latency histogram per version (`Stats`) and add them to
//! the version's numbers (records) every few seconds. After adding, a gateway
//! compares the candidate with the active version ([`verdict`]): with at
//! least [`MIN_CALLS`] candidate calls, an error rate above twice the
//! active one's and above 1%, or (both with [`MIN_CALLS`]) a p95 above
//! twice the active one's, aborts the rollout as `ROLLOUT ABORT` would,
//! and says why in the run log. Counting starts again for both versions
//! when a rollout starts.
//!
//! As built there is no soak timer: each stage is set by hand (or by
//! `sprigctl logic deploy --rollout`), and the automatic rollback is the
//! guard while it runs.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sprig_catalog::records::{Pk, RecordTx, RowTable};
use sprig_common::{Result, SprigCode, SprigError};
use sprig_logic_compile::ir::{EventDef, Function, Param, Ty};

/// Calls a version needs before its numbers decide anything.
pub const MIN_CALLS: u64 = 20;

/// Upper bounds of the latency histogram's buckets, in milliseconds; the
/// last bucket holds everything slower.
pub const BUCKETS_MS: [u64; 11] = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 5000];

fn syntax(m: impl Into<String>) -> SprigError {
    SprigError::new(SprigCode::LogicSyntax, m)
}

/// What a caller's pick is keyed on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RolloutBy {
    /// The caller (its backend account): each user keeps one version.
    #[default]
    User,
    /// The tenant: all of a tenant's calls get the same version, for code
    /// deployed to many tenants at once.
    Tenant,
    /// A key from the call: a function's or procedure's argument at this
    /// position; `None` for a trigger on a stream or queue (the record's
    /// or message's key).
    Key(Option<u16>),
}

impl RolloutBy {
    /// The catalog's text.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            RolloutBy::User => "user",
            RolloutBy::Tenant => "tenant",
            RolloutBy::Key(_) => "key",
        }
    }

    /// Reads `user`, `tenant` or `key` (a key whose parameter is named
    /// apart, [`key_param`]).
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "user" | "caller" => Some(RolloutBy::User),
            "tenant" => Some(RolloutBy::Tenant),
            "key" => Some(RolloutBy::Key(None)),
            k if k.starts_with("key:") => Some(RolloutBy::Key(k[4..].trim().parse().ok())),
            _ => None,
        }
    }

    /// The catalog's `by_key` text: `key:<position>` for a parameter.
    #[must_use]
    pub fn stored(self) -> String {
        match self {
            RolloutBy::Key(Some(i)) => format!("key:{i}"),
            other => other.as_str().to_owned(),
        }
    }
}

/// The parameter `SPRIG_ROLLOUT_BY = 'key:<parameter>'` names.
#[must_use]
pub fn key_param(s: &str) -> Option<String> {
    let t = s.trim();
    let (k, p) = t.split_once(':')?;
    (k.trim().eq_ignore_ascii_case("key") && !p.trim().is_empty())
        .then(|| p.trim().to_ascii_lowercase())
}

/// A stage: the candidate's share and the key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct RolloutSpec {
    /// 1 to 99.
    pub pct: u8,
    /// What the pick is keyed on.
    pub by: RolloutBy,
}

/// `ALTER {FUNCTION | PROCEDURE} f ROLLOUT ...`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RolloutOp {
    /// `TO n [PERCENT] [BY ...]`, 1 to 99 (the key stays when not given).
    To(u8, Option<RolloutBy>),
    /// `PROMOTE` or `TO 100`.
    Promote,
    /// `ABORT` or `TO 0`.
    Abort,
}

/// Parses the words after `ROLLOUT` (lower case).
///
/// # Errors
/// 39861 for anything else.
pub fn parse_op(rest: &[String]) -> Result<RolloutOp> {
    let usage =
        || syntax("expected ROLLOUT {TO n [PERCENT] [BY USER | TENANT | KEY] | PROMOTE | ABORT}");
    match rest {
        [p] if p == "promote" => Ok(RolloutOp::Promote),
        [a] if a == "abort" => Ok(RolloutOp::Abort),
        [t, n, more @ ..] if t == "to" => {
            let n: u32 = n
                .trim_end_matches('%')
                .parse()
                .map_err(|_| syntax(format!("ROLLOUT TO takes a percentage, not {n}")))?;
            let more: Vec<&str> = more
                .iter()
                .map(String::as_str)
                .filter(|w| *w != "percent")
                .collect();
            let by = match more.as_slice() {
                [] => None,
                ["by", k] => Some(RolloutBy::parse(k).ok_or_else(usage)?),
                _ => return Err(usage()),
            };
            match n {
                0 => Ok(RolloutOp::Abort),
                100 => Ok(RolloutOp::Promote),
                1..=99 => Ok(RolloutOp::To(u8::try_from(n).unwrap_or(99), by)),
                _ => Err(syntax("a rollout stage is 0 to 100 percent")),
            }
        }
        _ => Err(usage()),
    }
}

/// The bucket, 0 to 99, of `key` for `module`: MariaDB's
/// `CRC32(CONCAT(key, ':', module)) % 100`.
#[must_use]
pub fn bucket(key: &str, module_id: u64) -> u32 {
    crc32fast::hash(format!("{key}:{module_id}").as_bytes()) % 100
}

/// Whether a call by `account` (the backend account, `t<tenant>_u<user>`)
/// of tenant `tenant` gets the candidate.
#[must_use]
pub fn picks_candidate(spec: RolloutSpec, account: &str, tenant: u64, module_id: u64) -> bool {
    let key = match spec.by {
        RolloutBy::User | RolloutBy::Key(_) => account.to_owned(),
        RolloutBy::Tenant => format!("t{tenant}"),
    };
    bucket(&key, module_id) < u32::from(spec.pct)
}

/// Whether a call or record keyed `key` gets the candidate of a `BY KEY`
/// stage (`None`, no key: the active version).
#[must_use]
pub fn key_picks_candidate(spec: RolloutSpec, key: Option<&str>, module_id: u64) -> bool {
    key.is_some_and(|k| bucket(k, module_id) < u32::from(spec.pct))
}

/// The SQL condition the wrapper uses for a `BY KEY` stage on parameter
/// `param` (its routine name, `p_<name>`): the same [`bucket`] of the
/// argument's text, the empty text for `NULL`.
#[must_use]
pub fn key_condition(module_id: u64, pct: u8, param: &str) -> String {
    format!("CRC32(CONCAT(COALESCE(CAST({param} AS CHAR), ''), ':{module_id}')) % 100 < {pct}")
}

/// The SQL condition the wrapper uses for a `BY USER` stage.
#[must_use]
pub fn user_condition(module_id: u64, pct: u8) -> String {
    format!("CRC32(CONCAT(SUBSTRING_INDEX(USER(), '@', 1), ':{module_id}')) % 100 < {pct}")
}

fn ty_of(name: &str) -> Ty {
    match name {
        "i64" => Ty::Int,
        "f64" => Ty::Float,
        "bool" => Ty::Bool,
        "json" => Ty::Json,
        "null" => Ty::Null,
        _ => Ty::Str,
    }
}

/// A version's wrapper signature (parameters and events) from its stored
/// signature, as the wrapper generator needs it.
#[must_use]
pub fn function_of(name: &str, signature: &Value) -> Function {
    Function {
        name: name.to_owned(),
        params: signature["params"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|p| Param {
                name: p["name"].as_str().unwrap_or_default().to_owned(),
                ty: ty_of(p["type"].as_str().unwrap_or("")),
                min: None,
                max: None,
            })
            .collect(),
        events: signature["events"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|e| EventDef {
                name: e["name"].as_str().unwrap_or("output").to_owned(),
                fields: e["fields"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|f| {
                        (
                            f["name"].as_str().unwrap_or_default().to_owned(),
                            ty_of(f["type"].as_str().unwrap_or("")),
                        )
                    })
                    .collect(),
            })
            .collect(),
        locals: Vec::new(),
        body: Vec::new(),
    }
}

/// Whether two versions can share one wrapper: the same parameters, by
/// name and type, in the same order.
#[must_use]
pub fn same_params(a: &Function, b: &Function) -> bool {
    a.params.len() == b.params.len()
        && a.params
            .iter()
            .zip(&b.params)
            .all(|(x, y)| x.name == y.name && x.ty == y.ty)
}

/// A module's rollout, as `logic_rollouts` holds it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Rollout {
    /// The module.
    pub module_id: u64,
    /// The version most callers get.
    pub active_version: u64,
    /// The version on trial.
    pub candidate_version: u64,
    /// The candidate's share.
    pub pct: u8,
    /// What the pick is keyed on.
    pub by: RolloutBy,
    /// When the rollout started (UTC).
    pub started_at: String,
}

impl Rollout {
    /// The stage.
    #[must_use]
    pub fn spec(&self) -> RolloutSpec {
        RolloutSpec {
            pct: self.pct,
            by: self.by,
        }
    }
}

/// Rollouts by module.
const ROLLOUTS: RowTable = RowTable::new(crate::catalog::MODULE, "rollouts");
/// Call numbers by module and version.
const STATS: RowTable = RowTable::new(crate::catalog::MODULE, "stats");

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RollRec {
    module_id: u64,
    active_version: u64,
    candidate_version: u64,
    pct: u8,
    by_key: String,
    started_us: u64,
}

impl RollRec {
    fn rollout(&self) -> Rollout {
        Rollout {
            module_id: self.module_id,
            active_version: self.active_version,
            candidate_version: self.candidate_version,
            pct: self.pct,
            by: RolloutBy::parse(&self.by_key).unwrap_or_default(),
            started_at: iso(self.started_us),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StatRec {
    calls: u64,
    errors: u64,
    total_us: u64,
    hist: [u64; 12],
    since_us: u64,
}

fn now_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_micros()).unwrap_or(u64::MAX))
}

/// `us` as `YYYY-MM-DDThh:mm:ss.ffffffZ`.
fn iso(us: u64) -> String {
    let (t, f) = crate::catalog::utc_text(us);
    format!("{}T{}.{f:06}Z", &t[..10], &t[11..])
}

/// Starts or changes `module_id`'s rollout of `candidate` over `active`
/// (a new pair of versions starts its numbers again).
///
/// # Errors
/// The record store's.
pub async fn put(
    c: &mut dyn RecordTx,
    module_id: u64,
    active: u64,
    candidate: u64,
    spec: RolloutSpec,
) -> Result<()> {
    let pk = Pk::new().u64(module_id);
    let prev = ROLLOUTS.get_for_update::<RollRec>(c, &pk).await?;
    let fresh = prev
        .as_ref()
        .is_none_or(|p| (p.active_version, p.candidate_version) != (active, candidate));
    if fresh {
        for (spk, _) in STATS.scan::<StatRec>(c, &pk).await? {
            STATS.delete(c, &spk).await?;
        }
    }
    let started_us = match (&prev, fresh) {
        (Some(p), false) => p.started_us,
        _ => now_us(),
    };
    let rec = RollRec {
        module_id,
        active_version: active,
        candidate_version: candidate,
        pct: spec.pct,
        by_key: spec.by.stored(),
        started_us,
    };
    ROLLOUTS.put(c, &pk, &rec).await?;
    crate::LogicStore::set_candidate(c, module_id, Some((candidate, spec.pct))).await?;
    crate::LogicStore::set_versions_state(c, module_id, Some(candidate), None, "rolling_out").await
}

/// Ends `module_id`'s rollout: the candidate goes back to superseded.
/// Returns the rollout it ended.
///
/// # Errors
/// The record store's.
pub async fn clear(c: &mut dyn RecordTx, module_id: u64) -> Result<Option<Rollout>> {
    let prev = get(c, module_id).await?;
    if prev.is_some() {
        ROLLOUTS.delete(c, &Pk::new().u64(module_id)).await?;
    }
    crate::LogicStore::set_candidate(c, module_id, None).await?;
    crate::LogicStore::set_versions_state(c, module_id, None, Some("rolling_out"), "superseded")
        .await?;
    Ok(prev)
}

/// `module_id`'s rollout.
///
/// # Errors
/// The record store's.
pub async fn get(c: &mut dyn RecordTx, module_id: u64) -> Result<Option<Rollout>> {
    Ok(ROLLOUTS
        .get::<RollRec>(c, &Pk::new().u64(module_id))
        .await?
        .map(|r| r.rollout()))
}

/// Every rollout of a live function or procedure, with its database and
/// name.
///
/// # Errors
/// The record store's.
pub async fn all(c: &mut dyn RecordTx) -> Result<Vec<(u32, String, Rollout)>> {
    let store = crate::LogicStore::new("");
    let mut out = Vec::new();
    for r in ROLLOUTS.rows::<RollRec>(c, &Pk::new()).await? {
        if let Some(m) = store.module_by_id(c, r.module_id).await?
            && m.state == "active"
            && matches!(m.kind.as_str(), "function" | "procedure")
        {
            out.push((m.db_id, m.name, r.rollout()));
        }
    }
    Ok(out)
}

/// Calls of one version counted by one gateway since its last flush.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Acc {
    /// Calls.
    pub calls: u64,
    /// Calls that failed.
    pub errors: u64,
    /// Their total duration, microseconds.
    pub total_us: u64,
    /// Calls per latency bucket ([`BUCKETS_MS`], then slower).
    pub hist: [u64; 12],
}

impl Acc {
    /// Counts one call.
    pub fn add(&mut self, elapsed: std::time::Duration, failed: bool) {
        self.calls += 1;
        if failed {
            self.errors += 1;
        }
        self.total_us += u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX);
        let ms = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
        let i = BUCKETS_MS.iter().position(|b| ms < *b).unwrap_or(11);
        self.hist[i] += 1;
    }
}

/// Adds one gateway's counts to the versions' numbers.
///
/// # Errors
/// The record store's.
pub async fn flush(c: &mut dyn RecordTx, rows: &[((u64, u64), Acc)]) -> Result<()> {
    for ((module, version), a) in rows {
        let pk = Pk::new().u64(*module).u64(*version);
        let mut st = STATS
            .get_for_update::<StatRec>(c, &pk)
            .await?
            .unwrap_or(StatRec {
                calls: 0,
                errors: 0,
                total_us: 0,
                hist: [0; 12],
                since_us: now_us(),
            });
        st.calls += a.calls;
        st.errors += a.errors;
        st.total_us += a.total_us;
        for (x, y) in st.hist.iter_mut().zip(a.hist) {
            *x += y;
        }
        STATS.put(c, &pk, &st).await?;
    }
    Ok(())
}

/// One version's numbers.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VersionStats {
    /// The version.
    pub version: u64,
    /// Calls.
    pub calls: u64,
    /// Failed calls.
    pub errors: u64,
    /// `errors / calls`.
    pub error_rate: f64,
    /// Mean duration, milliseconds.
    pub mean_ms: f64,
    /// The 95th percentile, as the upper bound of its bucket in
    /// milliseconds (`None` past the last bound).
    pub p95_ms: Option<u64>,
    /// Calls per bucket: the bounds are [`BUCKETS_MS`], then slower.
    pub histogram: Vec<u64>,
    /// Counted since (UTC).
    pub since: String,
}

fn p95(h: &[u64; 12], calls: u64) -> Option<u64> {
    if calls == 0 {
        return None;
    }
    let need = calls.saturating_mul(95).div_ceil(100);
    let mut seen = 0;
    for (i, n) in h.iter().enumerate() {
        seen += n;
        if seen >= need {
            return BUCKETS_MS.get(i).copied();
        }
    }
    None
}

/// The numbers of `module_id`'s versions, newest first.
///
/// # Errors
/// The record store's.
pub async fn stats(c: &mut dyn RecordTx, module_id: u64) -> Result<Vec<VersionStats>> {
    let mut rows: Vec<(u64, u64, u64, u64, [u64; 12], String)> = STATS
        .scan::<StatRec>(c, &Pk::new().u64(module_id))
        .await?
        .into_iter()
        .filter_map(|(pk, s)| {
            let v = u64::from_be_bytes(pk.0.get(pk.0.len().checked_sub(8)?..)?.try_into().ok()?);
            Some((v, s.calls, s.errors, s.total_us, s.hist, iso(s.since_us)))
        })
        .collect();
    rows.reverse();
    Ok(rows
        .into_iter()
        .map(|(version, calls, errors, total_us, h, since)| {
            #[allow(clippy::cast_precision_loss)]
            let (rate, mean) = if calls == 0 {
                (0.0, 0.0)
            } else {
                (
                    errors as f64 / calls as f64,
                    total_us as f64 / calls as f64 / 1000.0,
                )
            };
            VersionStats {
                version,
                calls,
                errors,
                error_rate: rate,
                mean_ms: mean,
                p95_ms: p95(&h, calls),
                histogram: h.to_vec(),
                since,
            }
        })
        .collect())
}

/// Whether the candidate should be rolled back, and why (see the module
/// docs); `None` while it holds up or has too few calls.
#[must_use]
pub fn verdict(active: Option<&VersionStats>, candidate: &VersionStats) -> Option<String> {
    if candidate.calls < MIN_CALLS {
        return None;
    }
    let a_rate = active
        .filter(|a| a.calls >= MIN_CALLS)
        .map_or(0.0, |a| a.error_rate);
    if candidate.error_rate > 0.01 && candidate.error_rate > 2.0 * a_rate {
        return Some(format!(
            "the candidate's error rate is {:.1}% ({} of {} calls) against {:.1}% for the active version",
            candidate.error_rate * 100.0,
            candidate.errors,
            candidate.calls,
            a_rate * 100.0
        ));
    }
    if let Some(a) = active.filter(|a| a.calls >= MIN_CALLS)
        && let (Some(ap), Some(cp)) = (a.p95_ms, candidate.p95_ms.or(Some(u64::MAX)))
        && cp > 2 * ap
    {
        return Some(format!(
            "the candidate's p95 is {} against {ap} ms for the active version",
            candidate
                .p95_ms
                .map_or_else(|| "over 5000 ms".to_owned(), |p| format!("{p} ms"))
        ));
    }
    None
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    fn w(s: &str) -> Vec<String> {
        s.split_whitespace().map(str::to_owned).collect()
    }

    #[test]
    fn ops() {
        assert_eq!(parse_op(&w("promote")).unwrap(), RolloutOp::Promote);
        assert_eq!(parse_op(&w("abort")).unwrap(), RolloutOp::Abort);
        assert_eq!(
            parse_op(&w("to 25 percent")).unwrap(),
            RolloutOp::To(25, None)
        );
        assert_eq!(
            parse_op(&w("to 5% by tenant")).unwrap(),
            RolloutOp::To(5, Some(RolloutBy::Tenant))
        );
        assert_eq!(parse_op(&w("to 100")).unwrap(), RolloutOp::Promote);
        assert_eq!(parse_op(&w("to 0")).unwrap(), RolloutOp::Abort);
        assert!(parse_op(&w("to 101")).is_err());
        assert!(parse_op(&w("to x")).is_err());
        assert!(parse_op(&w("sideways")).is_err());
        assert!(parse_op(&w("to 5 by row")).is_err());
    }

    #[test]
    fn picks_match_mariadb_and_grow_with_the_stage() {
        // MariaDB: SELECT CRC32('t42_u7:1001') → 1719640002, % 100 → 2.
        assert_eq!(crc32fast::hash(b"t42_u7:1001"), 1_719_640_002);
        assert_eq!(bucket("t42_u7", 1001), 2);
        let users: Vec<String> = (0..1000).map(|u| format!("t42_u{u}")).collect();
        let mut prev = 0;
        for pct in [1u8, 10, 50, 99] {
            let spec = RolloutSpec {
                pct,
                by: RolloutBy::User,
            };
            let n = users
                .iter()
                .filter(|u| picks_candidate(spec, u, 42, 1001))
                .count();
            assert!(n >= prev, "a larger stage keeps the smaller one's callers");
            assert!(
                n.abs_diff(usize::from(pct) * 10) < 60,
                "{pct}%: {n} of 1000"
            );
            prev = n;
        }
        let t = RolloutSpec {
            pct: 50,
            by: RolloutBy::Tenant,
        };
        let a = picks_candidate(t, "t42_u1", 42, 1001);
        assert!(users.iter().all(|u| picks_candidate(t, u, 42, 1001) == a));
        assert_eq!(
            user_condition(1001, 10),
            "CRC32(CONCAT(SUBSTRING_INDEX(USER(), '@', 1), ':1001')) % 100 < 10"
        );
    }

    #[test]
    fn keys_pick_as_mariadb_does() {
        // MariaDB: SELECT CRC32(CONCAT(COALESCE(CAST(7 AS CHAR), ''), ':1001'))
        // → 2705881612, % 100 → 12; CRC32('k7:1001') % 100 → 70.
        assert_eq!(crc32fast::hash(b"7:1001"), 2_705_881_612);
        let spec = |pct| RolloutSpec {
            pct,
            by: RolloutBy::Key(None),
        };
        assert!(key_picks_candidate(spec(13), Some("7"), 1001));
        assert!(!key_picks_candidate(spec(12), Some("7"), 1001));
        assert!(key_picks_candidate(spec(71), Some("k7"), 1001));
        assert!(
            !key_picks_candidate(spec(99), None, 1001),
            "no key: the active version"
        );
        assert_eq!(
            key_condition(1001, 13, "p_k"),
            "CRC32(CONCAT(COALESCE(CAST(p_k AS CHAR), ''), ':1001')) % 100 < 13"
        );
        assert_eq!(
            RolloutBy::parse("key:customer_id"),
            Some(RolloutBy::Key(None))
        );
        assert_eq!(
            key_param("KEY: Customer_Id").as_deref(),
            Some("customer_id")
        );
        assert_eq!(key_param("key"), None);
        for by in [
            RolloutBy::User,
            RolloutBy::Tenant,
            RolloutBy::Key(None),
            RolloutBy::Key(Some(2)),
        ] {
            assert_eq!(RolloutBy::parse(&by.stored()), Some(by));
        }
        assert_eq!(
            parse_op(&w("to 30 by key")).unwrap(),
            RolloutOp::To(30, Some(RolloutBy::Key(None)))
        );
    }

    #[test]
    fn signatures_and_numbers() {
        let sig = serde_json::json!({
            "params": [{"name": "id", "type": "i64"}, {"name": "q", "type": "f64", "sql": "double"}],
            "events": [{"name": "output", "fields": [{"name": "total", "type": "f64"}]}],
        });
        let f = function_of("quote", &sig);
        assert_eq!(f.params[1].ty, Ty::Float);
        assert_eq!(f.events[0].fields[0], ("total".to_owned(), Ty::Float));
        let mut g = f.clone();
        assert!(same_params(&f, &g));
        g.params[0].ty = Ty::Str;
        assert!(!same_params(&f, &g));

        let mut a = Acc::default();
        for i in 0..100u64 {
            a.add(std::time::Duration::from_millis(i % 10), i % 50 == 0);
        }
        assert_eq!((a.calls, a.errors), (100, 2));
        assert_eq!(a.hist.iter().sum::<u64>(), 100);
        assert_eq!(p95(&a.hist, a.calls), Some(10));
        let vs = |calls, errors, p95_ms| VersionStats {
            version: 1,
            calls,
            errors,
            #[allow(clippy::cast_precision_loss)]
            error_rate: if calls == 0 {
                0.0
            } else {
                errors as f64 / calls as f64
            },
            mean_ms: 1.0,
            p95_ms,
            histogram: vec![],
            since: String::new(),
        };
        assert!(
            verdict(Some(&vs(100, 0, Some(5))), &vs(10, 5, Some(5))).is_none(),
            "too few"
        );
        assert!(verdict(Some(&vs(100, 0, Some(5))), &vs(40, 4, Some(5))).is_some());
        assert!(
            verdict(Some(&vs(100, 10, Some(5))), &vs(40, 4, Some(5))).is_none(),
            "as bad as active"
        );
        assert!(
            verdict(None, &vs(40, 1, Some(5))).is_some(),
            "2.5% with no active numbers"
        );
        assert!(
            verdict(Some(&vs(100, 0, Some(5))), &vs(40, 0, Some(20))).is_some(),
            "p95"
        );
        assert!(verdict(Some(&vs(100, 0, Some(10))), &vs(40, 0, Some(20))).is_none());
    }
}
