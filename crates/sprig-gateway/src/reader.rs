//! Reader sessions: connections that read from a copy of their tenant's
//! data instead of its leader.
//!
//! Every other session of a tenant runs on the node leading its unit
//! ([`crate::forward`]). A login that asks to read from copies is served on a
//! node holding a replica of the unit (a voter that doesn't lead it, or a
//! learner), so the reads cost the leader nothing:
//!
//! - MySQL and MariaDB: the connection attribute `sprig_read_from`
//!   (`follower`: exact, as fresh as the leader through a read barrier;
//!   `nearest`: bounded, see below), with `sprig_max_lag_ms` for the bound;
//!   or a database named `<db>@replica` (bounded, as `nearest`).
//!
//! A reader session is read-only: its statements run as in a `READ ONLY`
//! transaction, so writes fail with the servers' read-only error. Each
//! statement reads under a [`ReadPolicy`]:
//!
//! - bounded reads are served from what the copy holds when it holds the
//!   session's token and is at most the bound behind the leader (measured
//!   from its latest read barrier), else after a short wait or a barrier;
//! - exact reads take a read barrier per read view, shared with the other
//!   readers of the unit on this node.
//!
//! `/*+ READ_FROM(leader | follower | nearest [, MAX_LAG = n]) */` in a
//! statement chooses for that statement (`leader` is an exact read here: the
//! answer the leader would give, without moving the session).
//!
//! The session's token (an HLC timestamp) is its user variable
//! `@sprig_read_token`: readable on any session, and `SET @sprig_read_token
//! = '<n>'` on a reader session makes its later reads at least that fresh
//! (a token taken from a session that wrote, on another connection).

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::{Duration, Instant};

use sprig_common::ids::TenantId;
use sprig_engine::reads::{ReadMode, ReadPolicy};
use sprig_settings::{Apply, HardBounds, LiveSettings, Scopes, Setting, SettingCtx, SettingValue};

/// How long a bounded read waits for its copy to catch up before it asks
/// the leader: `reads.follower_wait_ms`'s default.
pub const FOLLOWER_WAIT: Duration = Duration::from_millis(100);

/// The default bound of `nearest` reads: `reads.nearest_max_lag_ms`'s
/// default.
pub const NEAREST_MAX_LAG: Duration = Duration::from_millis(5_000);

/// `reads.follower_wait_ms`: how long a bounded read on a copy waits for
/// the copy to catch up before it runs a read barrier.
pub static FOLLOWER_WAIT_MS: Setting<u64> = Setting {
    key: "reads.follower_wait_ms",
    default: 100,
    scopes: Scopes::ALL,
    hard: HardBounds::range(0, 60_000),
    apply: Apply::Live,
    operator_only: false,
    owner: 61,
    doc: "How long a bounded read on a copy waits for the copy to catch up before it asks the leader",
};
sprig_settings::register!(FOLLOWER_WAIT_MS);

/// `reads.nearest_max_lag_ms`: the bound of `nearest` reads that name
/// none.
pub static NEAREST_MAX_LAG_MS: Setting<u64> = Setting {
    key: "reads.nearest_max_lag_ms",
    default: 5_000,
    scopes: Scopes::ALL,
    hard: HardBounds::range(0, 86_400_000),
    apply: Apply::Live,
    operator_only: false,
    owner: 24,
    doc: "Default bound for nearest reads (and MongoDB secondary reads without maxStalenessSeconds)",
};
sprig_settings::register!(NEAREST_MAX_LAG_MS);

/// `reads.read_from`: where a tenant's sessions read by default (the
/// tenant form of `SET sprig.read_from`; the registry keeps the `sprig.`
/// prefix for internal keys).
pub static READ_FROM: Setting<Cow<'static, str>> = Setting {
    key: "reads.read_from",
    default: Cow::Borrowed("leader"),
    scopes: Scopes::TREE,
    hard: HardBounds::one_of(&["leader", "follower", "nearest"]),
    apply: Apply::Live,
    operator_only: false,
    owner: 61,
    doc: "leader, follower or nearest: where the tenant's SQL sessions read unless they ask otherwise (SET sprig.read_from for one session)",
};
sprig_settings::register!(READ_FROM);

static LIVE: OnceLock<LiveSettings> = OnceLock::new();

/// Gives reads from copies the settings they read (the tenant registry's
/// start calls it).
pub(crate) fn set_settings(live: &LiveSettings) {
    let _ = LIVE.set(live.clone());
}

static TENANTS: OnceLock<crate::limits::TenantSettingsLookup> = OnceLock::new();

/// Gives reads from copies the tenants' own settings (rows set at tenant,
/// database or object scope live in each tenant's cached catalog).
pub(crate) fn set_tenant_settings(lookup: crate::limits::TenantSettingsLookup) {
    let _ = TENANTS.set(lookup);
}

static CACHE: OnceLock<sprig_catalog::TenantCache> = OnceLock::new();

/// Gives reads from copies the tenant catalog cache, for values that must
/// not wait for a session to reload a tenant's catalog.
pub(crate) fn set_tenant_cache(cache: sprig_catalog::TenantCache) {
    let _ = CACHE.set(cache);
}

/// [`tenant_value`] from the tenant's latest catalog (loaded when the
/// cached one is older).
pub(crate) async fn tenant_value_latest<T: SettingValue>(setting: &Setting<T>, tenant: u64) -> T {
    let (Some(live), Some(cache)) = (LIVE.get(), CACHE.get()) else {
        return tenant_value(setting, tenant);
    };
    let Ok(cat) = cache
        .get(TenantId(tenant), sprig_catalog::Freshness::Latest)
        .await
    else {
        return tenant_value(setting, tenant);
    };
    let dc = live.stores().dc_id().clone();
    let snap = live.snapshot();
    cat.settings
        .resolve(&snap, setting.key, &cat.settings.ctx(&dc))
        .ok()
        .and_then(|r| T::from_value(&r.value))
        .unwrap_or_else(|| snap.get(setting, &SettingCtx::process(&dc)))
}

/// `setting`'s value for `tenant`: the tenant's own rows when its catalog
/// is cached here, else the DC's and global ones.
pub(crate) fn tenant_value<T: SettingValue>(setting: &Setting<T>, tenant: u64) -> T {
    let Some(live) = LIVE.get() else {
        return setting.default.clone();
    };
    let dc = live.stores().dc_id().clone();
    let snap = live.snapshot();
    if let Some(ts) = TENANTS.get().and_then(|l| l(TenantId(tenant)))
        && let Some(v) = ts
            .resolve(&snap, setting.key, &ts.ctx(&dc))
            .ok()
            .and_then(|r| T::from_value(&r.value))
    {
        return v;
    }
    snap.get(setting, &SettingCtx::process(&dc))
}

/// A tenant's `reads.*` values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Knobs {
    /// `reads.follower_wait_ms`.
    pub wait: Duration,
    /// `reads.nearest_max_lag_ms`.
    pub nearest: Duration,
    /// `reads.read_from`.
    pub read_from: ReadFrom,
}

impl Default for Knobs {
    fn default() -> Self {
        Self {
            wait: FOLLOWER_WAIT,
            nearest: NEAREST_MAX_LAG,
            read_from: ReadFrom::Leader,
        }
    }
}

/// How long a tenant's values are reused before they are read again.
const KNOBS_TTL: Duration = Duration::from_secs(1);

/// `tenant`'s values (read at most once a second per tenant).
#[must_use]
pub fn knobs(tenant: u64) -> Knobs {
    static CACHE: OnceLock<Mutex<HashMap<u64, (Instant, Knobs)>>> = OnceLock::new();
    if LIVE.get().is_none() {
        return Knobs::default();
    }
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some((at, k)) = cache
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .get(&tenant)
        && at.elapsed() < KNOBS_TTL
    {
        return *k;
    }
    let k = Knobs {
        wait: Duration::from_millis(tenant_value(&FOLLOWER_WAIT_MS, tenant)),
        nearest: Duration::from_millis(tenant_value(&NEAREST_MAX_LAG_MS, tenant)),
        read_from: parse_read_from(&tenant_value(&READ_FROM, tenant)).unwrap_or(ReadFrom::Leader),
    };
    let mut c = cache.lock().unwrap_or_else(PoisonError::into_inner);
    if c.len() > 100_000 {
        c.clear();
    }
    c.insert(tenant, (Instant::now(), k));
    k
}

/// `leader`, `follower` or `nearest` (any case).
#[must_use]
pub fn parse_read_from(v: &str) -> Option<ReadFrom> {
    match v.trim().to_ascii_lowercase().as_str() {
        "leader" => Some(ReadFrom::Leader),
        "follower" => Some(ReadFrom::Follower(None)),
        "nearest" => Some(ReadFrom::Nearest(None)),
        _ => None,
    }
}

/// The database-name suffix that asks for a reader session.
pub const REPLICA_SUFFIX: &str = "@replica";

/// The user variable holding the session's token.
pub const TOKEN_VAR: &str = "sprig_read_token";

/// Where a statement or session reads from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadFrom {
    /// The leader (on a copy: an exact read).
    Leader,
    /// A copy, exact unless a bound is given.
    Follower(Option<Duration>),
    /// The closest copy, at most the bound (default
    /// [`NEAREST_MAX_LAG`]) old.
    Nearest(Option<Duration>),
}

impl ReadFrom {
    /// The engine's read mode for it, with `nearest` the bound of a
    /// `nearest` read that names none.
    #[must_use]
    pub fn mode(self, nearest: Duration) -> ReadMode {
        match self {
            Self::Leader | Self::Follower(None) => ReadMode::Exact,
            Self::Follower(Some(max_lag)) => ReadMode::Bounded { max_lag },
            Self::Nearest(lag) => ReadMode::Bounded {
                max_lag: lag.unwrap_or(nearest),
            },
        }
    }
}

/// A reader session's state: its default read mode and its token.
#[derive(Debug)]
pub struct Reader {
    default: std::sync::Mutex<ReadFrom>,
    token: Arc<AtomicU64>,
    tenant: u64,
}

impl Reader {
    /// A reader session of `tenant` reading from `from` by default.
    #[must_use]
    pub fn new(from: ReadFrom, tenant: u64) -> Self {
        Self {
            default: std::sync::Mutex::new(from),
            token: Arc::new(AtomicU64::new(0)),
            tenant,
        }
    }

    /// The session's default.
    #[must_use]
    pub fn default_from(&self) -> ReadFrom {
        *self
            .default
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// `SET sprig.read_from = ...`.
    pub fn set_default(&self, from: ReadFrom) {
        *self
            .default
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = from;
    }

    /// The token now.
    #[must_use]
    pub fn token(&self) -> u64 {
        self.token.load(Ordering::Acquire)
    }

    /// Raises the token (never lowers it).
    pub fn raise(&self, ts: u64) {
        self.token.fetch_max(ts, Ordering::AcqRel);
    }

    /// The policy for one statement: its hint, else the session's default.
    #[must_use]
    pub fn policy(&self, hint: Option<ReadFrom>) -> ReadPolicy {
        let from = hint.unwrap_or_else(|| self.default_from());
        let k = knobs(self.tenant);
        ReadPolicy {
            mode: from.mode(k.nearest),
            wait: k.wait,
            token: Arc::clone(&self.token),
        }
    }
}

/// What a MySQL or MariaDB login asks for: `(read from, database without
/// the `@replica` suffix)`. `None`: a normal session.
#[must_use]
pub fn mysql_login(
    database: Option<&[u8]>,
    attrs: &[(String, String)],
) -> Option<(ReadFrom, Option<String>)> {
    let db = database.map(|d| String::from_utf8_lossy(d).into_owned());
    let attr = |k: &str| {
        attrs
            .iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(k))
            .map(|(_, v)| v.trim())
    };
    let lag = attr("sprig_max_lag_ms")
        .and_then(|v| v.parse::<u64>().ok())
        .map(Duration::from_millis);
    let (db, suffixed) = match db {
        Some(d) if d.len() > REPLICA_SUFFIX.len() && ends_with_ci(&d, REPLICA_SUFFIX) => {
            (Some(d[..d.len() - REPLICA_SUFFIX.len()].to_owned()), true)
        }
        d => (d, false),
    };
    let from = match attr("sprig_read_from")
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("follower") => Some(ReadFrom::Follower(lag)),
        Some("nearest") => Some(ReadFrom::Nearest(lag)),
        Some("leader") => None,
        _ if suffixed => Some(ReadFrom::Nearest(lag)),
        _ => None,
    }?;
    Some((from, db))
}

fn ends_with_ci(s: &str, suffix: &str) -> bool {
    s.len() >= suffix.len()
        && s.as_bytes()[s.len() - suffix.len()..].eq_ignore_ascii_case(suffix.as_bytes())
}

/// A statement's `/*+ READ_FROM(...) */` hint, if it has one in its first
/// optimizer-hint comment (MySQL places hints right after the first
/// keyword, so only the start of the statement is searched).
#[must_use]
pub fn hint(sql: &[u8]) -> Option<ReadFrom> {
    let head = &sql[..sql.len().min(256)];
    let start = head.windows(3).position(|w| w == b"/*+")? + 3;
    let rest = &sql[start..];
    let end = rest.windows(2).position(|w| w == b"*/")?;
    let body = std::str::from_utf8(&rest[..end]).ok()?;
    let upper = body.to_ascii_uppercase();
    let at = upper.find("READ_FROM")?;
    let args = upper[at + "READ_FROM".len()..]
        .trim_start()
        .strip_prefix('(')?;
    let args = &args[..args.find(')')?];
    let mut parts = args.split(',').map(str::trim);
    let which = parts.next()?;
    let mut lag = None;
    for p in parts {
        let (k, v) = p.split_once('=')?;
        if k.trim() != "MAX_LAG" {
            return None;
        }
        lag = Some(duration(v.trim())?);
    }
    match which {
        "LEADER" => Some(ReadFrom::Leader),
        "FOLLOWER" => Some(ReadFrom::Follower(lag)),
        "NEAREST" => Some(ReadFrom::Nearest(lag)),
        _ => None,
    }
}

/// `5s`, `500ms`, `2m`, or a number of seconds.
fn duration(v: &str) -> Option<Duration> {
    let v = v.trim_matches(|c| c == '\'' || c == '"').trim();
    let (num, unit) = match v.find(|c: char| !c.is_ascii_digit() && c != '.') {
        Some(i) => (&v[..i], v[i..].trim()),
        None => (v, "S"),
    };
    let n: f64 = num.parse().ok()?;
    if !n.is_finite() || n < 0.0 {
        return None;
    }
    let ms = match unit.to_ascii_uppercase().as_str() {
        "MS" => n,
        "S" => n * 1000.0,
        "M" | "MIN" => n * 60_000.0,
        _ => return None,
    };
    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    Some(Duration::from_millis(ms.min(86_400_000.0) as u64))
}

/// `SET sprig.read_from = <value>` (quotes optional): the value.
#[must_use]
pub fn parse_set_read_from(sql: &[u8]) -> Option<ReadFrom> {
    let s = std::str::from_utf8(sql)
        .ok()?
        .trim()
        .trim_end_matches(';')
        .trim();
    let rest = strip_ci(s, "SET")?.trim_start();
    let rest = strip_ci(rest, "SESSION").map_or(rest, str::trim_start);
    let rest = strip_ci(rest, "sprig.read_from")?.trim_start();
    let rest = rest.strip_prefix('=').or_else(|| rest.strip_prefix(":="))?;
    parse_read_from(rest.trim().trim_matches(|c| c == '\'' || c == '"'))
}

/// `SET @sprig_read_token = '<n>'` (quotes optional): the token.
#[must_use]
pub fn parse_set_token(sql: &[u8]) -> Option<u64> {
    let s = std::str::from_utf8(sql)
        .ok()?
        .trim()
        .trim_end_matches(';')
        .trim();
    let rest = strip_ci(s, "SET")?.trim_start();
    let rest = rest.strip_prefix('@')?;
    let rest = strip_ci(rest, TOKEN_VAR)?.trim_start();
    let rest = rest.strip_prefix(":=").or_else(|| rest.strip_prefix('='))?;
    rest.trim()
        .trim_matches(|c| c == '\'' || c == '"')
        .parse()
        .ok()
}

/// Whether `sql` names the token variable (cheap: most statements have
/// no `@`).
#[must_use]
pub fn mentions_token(sql: &[u8]) -> bool {
    sql.contains(&b'@')
        && sql
            .windows(TOKEN_VAR.len())
            .any(|w| w.eq_ignore_ascii_case(TOKEN_VAR.as_bytes()))
}

fn strip_ci<'a>(s: &'a str, word: &str) -> Option<&'a str> {
    let head = s.get(..word.len())?;
    if !head.eq_ignore_ascii_case(word) {
        return None;
    }
    let rest = &s[word.len()..];
    // A whole word: what follows can't continue the name.
    if rest
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
        && !word.ends_with('.')
    {
        return None;
    }
    Some(rest)
}

/// How far behind its known commit index a copy may have applied and still
/// take reader logins.
const READY_LAG: u64 = 256;

/// Whether this node's replica of `tenant`'s unit can serve reader
/// sessions now: it knows its group's configuration and leader and has
/// applied nearly all it knows committed. A learner that just joined has
/// none of that until its snapshot arrives; reader logins landing on it go
/// to another copy (or the leader) meanwhile, instead of waiting there for
/// catalog reads that can't complete. True where nothing replicates.
#[must_use]
pub fn replica_ready(tenant: u64) -> bool {
    let Some(node) = crate::node_engine::raft_node() else {
        return true;
    };
    node.status(tenant).is_some_and(|st| {
        (!st.voters.is_empty() || !st.learners.is_empty())
            && st.leader.is_some()
            && st.applied > 0
            && st.committed.saturating_sub(st.applied) <= READY_LAG
    })
}

/// The token a session on this node can hand to a reader elsewhere: every
/// commit this node made visible so far (on the leader, at least the
/// session's own commits). 0 when the engine isn't open.
#[must_use]
pub fn node_token() -> u64 {
    crate::node_engine::engine().map_or(0, |e| e.txn_shared().visible_ts().0)
}

/// The running session's token: its own last commit (or a token it was
/// given), not every commit the node made since. Outside a client
/// connection (no session scope), the node's [`node_token`], which covers
/// any session's.
#[must_use]
pub fn session_token() -> u64 {
    sprig_engine::reads::session_token().unwrap_or_else(node_token)
}

/// Raises the running session's token to `t` (`SET @sprig_read_token` on a
/// session that writes): its later reads from copies are at least that
/// fresh. Returns the token now.
#[must_use]
pub fn raise_session_token(t: u64) -> u64 {
    match sprig_engine::reads::session_cell() {
        Some(c) => c.fetch_max(t, Ordering::AcqRel).max(t),
        None => t,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn attrs(v: &[(&str, &str)]) -> Vec<(String, String)> {
        v.iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[test]
    fn a_login_asks_by_attribute_or_by_database_suffix() {
        assert_eq!(mysql_login(Some(b"shop"), &[]), None);
        assert_eq!(
            mysql_login(Some(b"shop@replica"), &[]),
            Some((ReadFrom::Nearest(None), Some("shop".into())))
        );
        assert_eq!(
            mysql_login(
                Some(b"shop@REPLICA"),
                &attrs(&[("sprig_max_lag_ms", "250")])
            ),
            Some((
                ReadFrom::Nearest(Some(Duration::from_millis(250))),
                Some("shop".into())
            ))
        );
        assert_eq!(
            mysql_login(None, &attrs(&[("sprig_read_from", "Follower")])),
            Some((ReadFrom::Follower(None), None))
        );
        assert_eq!(
            mysql_login(Some(b"x"), &attrs(&[("sprig_read_from", "leader")])),
            None
        );
        assert_eq!(mysql_login(Some(b"@replica"), &[]), None);
    }

    #[test]
    fn read_from_hints() {
        assert_eq!(
            hint(b"SELECT /*+ READ_FROM(follower) */ * FROM t"),
            Some(ReadFrom::Follower(None))
        );
        assert_eq!(
            hint(b"select /*+ read_from(follower, max_lag = 5s) */ a from t"),
            Some(ReadFrom::Follower(Some(Duration::from_secs(5))))
        );
        assert_eq!(
            hint(b"SELECT /*+ NO_INDEX(t) READ_FROM( nearest , MAX_LAG=250ms) */ 1"),
            Some(ReadFrom::Nearest(Some(Duration::from_millis(250))))
        );
        assert_eq!(
            hint(b"SELECT /*+ READ_FROM(leader) */ 1"),
            Some(ReadFrom::Leader)
        );
        assert_eq!(hint(b"SELECT /*+ READ_FROM(elsewhere) */ 1"), None);
        assert_eq!(hint(b"SELECT 1 /* READ_FROM(follower) */"), None);
        assert_eq!(hint(b"SELECT 1"), None);
        assert_eq!(
            ReadFrom::Nearest(None).mode(NEAREST_MAX_LAG),
            ReadMode::Bounded {
                max_lag: NEAREST_MAX_LAG
            }
        );
        assert_eq!(ReadFrom::Leader.mode(NEAREST_MAX_LAG), ReadMode::Exact);
    }

    #[test]
    fn set_statements() {
        assert_eq!(
            parse_set_read_from(b"SET sprig.read_from = 'nearest'"),
            Some(ReadFrom::Nearest(None))
        );
        assert_eq!(
            parse_set_read_from(b"set session sprig.read_from=follower;"),
            Some(ReadFrom::Follower(None))
        );
        assert_eq!(
            parse_set_read_from(b"SET sprig.read_fromx = 'leader'"),
            None
        );
        assert_eq!(
            parse_set_token(b"SET @sprig_read_token = '1234'"),
            Some(1234)
        );
        assert_eq!(parse_set_token(b"set @SPRIG_READ_TOKEN:=99"), Some(99));
        assert_eq!(parse_set_token(b"SET @sprig_read_tokens = 1"), None);
        assert_eq!(parse_set_token(b"SET @sprig_read_token = 1, @a = 2"), None);
        assert!(mentions_token(b"SELECT @sprig_read_token"));
        assert!(!mentions_token(b"SELECT 1"));
    }

    #[test]
    fn a_reader_policy_shares_the_token() {
        let r = Reader::new(ReadFrom::Nearest(None), 1);
        r.raise(10);
        let p = r.policy(Some(ReadFrom::Leader));
        assert_eq!(p.mode, ReadMode::Exact);
        p.raise(20);
        assert_eq!(r.token(), 20);
        r.raise(5);
        assert_eq!(r.token(), 20);
    }
}
