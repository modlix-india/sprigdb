//! Index specifications, names and the loop's records.

use std::hash::Hasher;

use serde::{Deserialize, Serialize};
use twox_hash::XxHash3_64;

/// The prefix of every automatic index's name.
pub const AUTO_PREFIX: &str = "sprig_auto_";

/// Whether `name` is an automatic index's (origin `auto`): any case.
#[must_use]
pub fn is_auto_name(name: &str) -> bool {
    name.len() >= AUTO_PREFIX.len() && name[..AUTO_PREFIX.len()].eq_ignore_ascii_case(AUTO_PREFIX)
}

/// One key column of an index.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct KeyCol {
    /// The column's name, as the table declares it.
    pub column: String,
    /// Descending.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub desc: bool,
}

impl KeyCol {
    /// An ascending column.
    #[must_use]
    pub fn asc(column: &str) -> Self {
        Self {
            column: column.to_owned(),
            desc: false,
        }
    }
}

/// A secondary index on one table.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IndexSpec {
    /// The logical database.
    pub db: String,
    /// The table.
    pub table: String,
    /// The key columns, in order.
    pub cols: Vec<KeyCol>,
}

impl IndexSpec {
    /// `secondary(a,b desc)`: the definition in canonical form, lower case.
    #[must_use]
    pub fn canonical(&self) -> String {
        let parts: Vec<String> = self
            .cols
            .iter()
            .map(|c| {
                let n = c.column.to_lowercase();
                if c.desc { format!("{n} desc") } else { n }
            })
            .collect();
        format!("secondary({})", parts.join(","))
    }

    /// `xxh3_64` of the canonical form: the same definition on two tables
    /// compares equal.
    #[must_use]
    pub fn def_hash(&self) -> u64 {
        let mut h = XxHash3_64::with_seed(0);
        h.write(self.canonical().as_bytes());
        h.finish()
    }

    /// `sprig_auto_<8 hex>`: the top 32 bits of `xxh3_64` over the
    /// database and table (lower case, NUL-separated) and the canonical
    /// form. The same index gets the same name on every retry.
    #[must_use]
    pub fn name(&self) -> String {
        let mut h = XxHash3_64::with_seed(0);
        h.write(self.db.to_lowercase().as_bytes());
        h.write(&[0]);
        h.write(self.table.to_lowercase().as_bytes());
        h.write(&[0]);
        h.write(self.canonical().as_bytes());
        format!("{AUTO_PREFIX}{:08x}", h.finish() >> 32)
    }

    /// The key a record of this table is found under: `db.table`, lower
    /// case.
    #[must_use]
    pub fn table_key(&self) -> String {
        format!("{}.{}", self.db.to_lowercase(), self.table.to_lowercase())
    }

    /// Whether this index's columns start with all of `other`'s, in order
    /// and direction (so it serves every lookup `other` serves).
    #[must_use]
    pub fn extends(&self, other: &IndexSpec) -> bool {
        self.table_key() == other.table_key()
            && other.cols.len() <= self.cols.len()
            && other
                .cols
                .iter()
                .zip(&self.cols)
                .all(|(a, b)| a.column.eq_ignore_ascii_case(&b.column) && a.desc == b.desc)
    }

    /// `ALTER TABLE` text that adds it: `ADD INDEX name (cols)`.
    #[must_use]
    pub fn add_clause(&self) -> String {
        let cols: Vec<String> = self
            .cols
            .iter()
            .map(|c| {
                let q = format!("`{}`", c.column.replace('`', "``"));
                if c.desc { format!("{q} DESC") } else { q }
            })
            .collect();
        format!("ADD INDEX `{}` ({})", self.name(), cols.join(", "))
    }
}

/// Where an automatic index is in its life.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    /// Mode `recommend`: waiting for an approval.
    Recommended,
    /// Being built online.
    Building,
    /// Built; watched for `auto_index.observe_days`.
    Observing,
    /// Watched and kept; still dropped when it goes unused.
    Active,
    /// Dropped (terminal).
    Dropped,
    /// The approval was refused, or it expired (terminal).
    Rejected,
    /// The build failed (terminal).
    Failed,
}

impl State {
    /// The name `sprigctl index` prints.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Recommended => "recommended",
            Self::Building => "building",
            Self::Observing => "observing",
            Self::Active => "active",
            Self::Dropped => "dropped",
            Self::Rejected => "rejected",
            Self::Failed => "failed",
        }
    }

    /// Whether the index exists on the table in this state.
    #[must_use]
    pub const fn built(self) -> bool {
        matches!(self, Self::Observing | Self::Active)
    }

    /// Dropped, rejected or failed.
    #[must_use]
    pub const fn terminal(self) -> bool {
        matches!(self, Self::Dropped | Self::Rejected | Self::Failed)
    }
}

/// A query pattern's figures at the time an index was decided.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Baseline {
    /// Runs in the window.
    pub runs: u64,
    /// p95, ms.
    pub p95_ms: f64,
    /// Mean, ms.
    pub mean_ms: f64,
}

/// What an online build did.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct BuildReport {
    /// The schema-change states passed, in order (`absent`,
    /// `delete_only`, `write_only`, `backfill`, `public`).
    pub states: Vec<String>,
    /// Rows backfilled.
    pub rows: u64,
    /// Wall time of the whole build, ms.
    pub ms: u64,
    /// Batches backfilled.
    pub batches: u64,
}

/// One automatic index: `m/autoindex/idx/<name>`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IndexRecord {
    /// `sprig_auto_<8 hex>`.
    pub name: String,
    /// What it indexes.
    pub spec: IndexSpec,
    /// Always `auto`.
    pub origin: String,
    /// Its state.
    pub state: State,
    /// The query patterns (fingerprint ids) it serves.
    pub serves: Vec<u64>,
    /// Those patterns' figures when it was decided, by fingerprint.
    pub baseline: Vec<(u64, Baseline)>,
    /// The planner's rows-examined estimates over the replayed
    /// statements: without and with it (medians).
    pub est_before: f64,
    /// See `est_before`.
    pub est_after: f64,
    /// Estimated time saved, ms per hour.
    pub benefit_ms_per_hour: f64,
    /// Why it was decided (`slow_p95`, `top_total_time`, `large_scan`).
    pub reason: String,
    /// When it was decided, µs since the epoch.
    pub created_us: u64,
    /// When its state last changed.
    pub changed_us: u64,
    /// When its build finished.
    #[serde(default)]
    pub built_us: Option<u64>,
    /// The build.
    #[serde(default)]
    pub build: Option<BuildReport>,
    /// When a plan last read it (as far as the loop has seen).
    #[serde(default)]
    pub last_used_us: Option<u64>,
    /// Plans that read it, as last counted by this process.
    #[serde(default)]
    pub reads_seen: u64,
    /// Plans that read it since it was built (across counts).
    #[serde(default)]
    pub reads_total: u64,
    /// The approval request (mode `recommend`).
    #[serde(default)]
    pub approval: Option<String>,
    /// Why it ended (`unused`, `no_gain`, `rejected`, `expired`,
    /// `table_gone`, `operator`, `unexplained`, a build error).
    #[serde(default)]
    pub end_reason: Option<String>,
    /// Pinned by an operator (`sprigctl index pin`, or a restore): the
    /// watch never drops it.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub pinned: bool,
}

impl IndexRecord {
    /// A record for `spec`, decided at `now_us` for `reason`, in `state`.
    #[must_use]
    pub fn new(spec: IndexSpec, state: State, reason: &str, now_us: u64) -> Self {
        Self {
            name: spec.name(),
            spec,
            origin: "auto".into(),
            state,
            serves: Vec::new(),
            baseline: Vec::new(),
            est_before: 0.0,
            est_after: 0.0,
            benefit_ms_per_hour: 0.0,
            reason: reason.into(),
            created_us: now_us,
            changed_us: now_us,
            built_us: None,
            build: None,
            last_used_us: None,
            reads_seen: 0,
            reads_total: 0,
            approval: None,
            end_reason: None,
            pinned: false,
        }
    }
}

/// What an operator asks of an automatic index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    /// Drop it now; its definition is then blocked like a refused one.
    Drop,
    /// Keep it whatever the watch finds.
    Pin,
    /// Let the watch judge it again.
    Unpin,
    /// Build a dropped (or failed) index again, pinned.
    Restore,
}

impl Action {
    /// The action called `s`: `drop`, `pin` (`freeze`), `unpin`
    /// (`unfreeze`), `restore`; any case.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "drop" => Some(Self::Drop),
            "pin" | "freeze" => Some(Self::Pin),
            "unpin" | "unfreeze" => Some(Self::Unpin),
            "restore" => Some(Self::Restore),
            _ => None,
        }
    }

    /// The name `sprigctl index` and the records use.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Drop => "drop",
            Self::Pin => "pin",
            Self::Unpin => "unpin",
            Self::Restore => "restore",
        }
    }

    /// Whether the action applies to `rec` as it stands: `Err` with why
    /// not.
    ///
    /// # Errors
    /// A short reason when the record's state doesn't allow it.
    pub fn allowed(self, rec: &IndexRecord) -> Result<(), String> {
        let state = rec.state.name();
        match self {
            Self::Drop | Self::Pin if rec.state.terminal() => {
                Err(format!("{} is {state}", rec.name))
            }
            Self::Pin if rec.pinned => Err(format!("{} is pinned already", rec.name)),
            Self::Unpin if !rec.pinned => Err(format!("{} isn't pinned", rec.name)),
            Self::Restore if !matches!(rec.state, State::Dropped | State::Failed) => Err(format!(
                "{} is {state}: only a dropped or failed index is restored",
                rec.name
            )),
            _ => Ok(()),
        }
    }
}

/// A table's figures as the generator last measured them:
/// `m/autoindex/stats/<db.table>`. Kept across restarts and reused while
/// fresh, so a table is sampled at most every [`STATS_MAX_AGE_US`] (or when
/// its definition changes).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableStats {
    /// `db.table`, lower case.
    pub table: String,
    /// The table's schema version the figures were taken at.
    pub version: u64,
    /// Rows counted (at most the sample's size when the table is larger).
    pub rows: u64,
    /// Distinct values per column (estimated beyond the sample).
    pub distinct: Vec<(String, f64)>,
    /// When, µs since the epoch.
    pub at_us: u64,
}

/// How long a table's figures are reused: 6 hours.
pub const STATS_MAX_AGE_US: u64 = 6 * 3_600_000_000;

/// An operator's pending request: `m/autoindex/req/<name>`. The node that
/// runs the tenant's loop applies it within a minute and deletes it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperatorRequest {
    /// The index's name.
    pub index: String,
    /// What to do.
    pub action: Action,
    /// Who asked (`op:<name>`, as the Admin API's guard sets it).
    #[serde(default)]
    pub actor: String,
    /// Why, as given.
    #[serde(default)]
    pub reason: String,
    /// When, µs since the epoch.
    pub at_us: u64,
}

/// One decision of the loop, for `sprigctl index` and Studio:
/// `m/autoindex/events/<µs><seq>`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Event {
    /// µs since the epoch.
    pub at_us: u64,
    /// `candidate`, `rejected_candidate`, `recommended`, `approved`,
    /// `built`, `build_failed`, `observed`, `dropped`, `deferred`.
    pub kind: String,
    /// The index's name, or the candidate's would-be name.
    pub index: String,
    /// The definition, canonical.
    pub def: String,
    /// `db.table`.
    pub table: String,
    /// The patterns concerned.
    pub fingerprints: Vec<u64>,
    /// Why, in a few words.
    pub reason: String,
    /// Estimates: rows examined without and with the index.
    #[serde(default)]
    pub est: Option<(f64, f64)>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(cols: &[&str]) -> IndexSpec {
        IndexSpec {
            db: "Shop".into(),
            table: "Orders".into(),
            cols: cols.iter().map(|c| KeyCol::asc(c)).collect(),
        }
    }

    #[test]
    fn names_depend_on_table_and_definition_only() {
        let a = spec(&["customer_id", "created_at"]);
        assert_eq!(a.canonical(), "secondary(customer_id,created_at)");
        assert!(a.name().starts_with(AUTO_PREFIX));
        assert_eq!(a.name().len(), AUTO_PREFIX.len() + 8);
        assert!(is_auto_name(&a.name().to_uppercase()));
        // Case doesn't change the name.
        let mut b = a.clone();
        b.db = "shop".into();
        b.cols[0].column = "CUSTOMER_ID".into();
        assert_eq!(a.name(), b.name());
        // Another table: another name, the same definition hash.
        let mut c = a.clone();
        c.table = "invoices".into();
        assert_ne!(a.name(), c.name());
        assert_eq!(a.def_hash(), c.def_hash());
        let mut d = a.clone();
        d.cols[1].desc = true;
        assert_eq!(d.canonical(), "secondary(customer_id,created_at desc)");
        assert_ne!(a.name(), d.name());
        assert!(!is_auto_name("k_customer"));
    }

    #[test]
    fn operator_actions_follow_the_state() {
        let mut r = IndexRecord::new(spec(&["a"]), State::Active, "slow_p95", 1);
        assert_eq!(Action::parse("FREEZE"), Some(Action::Pin));
        assert_eq!(Action::parse("unfreeze"), Some(Action::Unpin));
        assert_eq!(Action::parse("approve"), None);
        assert!(Action::Drop.allowed(&r).is_ok());
        assert!(Action::Pin.allowed(&r).is_ok());
        assert!(Action::Unpin.allowed(&r).is_err());
        assert!(Action::Restore.allowed(&r).is_err());
        r.pinned = true;
        assert!(Action::Pin.allowed(&r).is_err());
        assert!(Action::Unpin.allowed(&r).is_ok());
        r.state = State::Dropped;
        assert!(Action::Drop.allowed(&r).is_err());
        assert!(Action::Restore.allowed(&r).is_ok());
        r.state = State::Rejected;
        assert!(Action::Restore.allowed(&r).is_err());
    }

    #[test]
    fn longer_indexes_extend_their_prefixes() {
        let ab = spec(&["a", "b"]);
        assert!(ab.extends(&spec(&["A"])));
        assert!(ab.extends(&ab));
        assert!(!ab.extends(&spec(&["b"])));
        assert!(!spec(&["a"]).extends(&ab));
        assert_eq!(
            ab.add_clause(),
            format!("ADD INDEX `{}` (`a`, `b`)", ab.name())
        );
    }
}
