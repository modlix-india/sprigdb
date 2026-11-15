//! Exactly once in change streams across a copy's first seed (decision
//! RL71): which feed delivers a change of an inherited record to a
//! subscriber whose tenant reads it through a materialized copy.
//!
//! Before a copy's first seed ends, the child reads its parent through
//! (the share view), and a change stream maps the parent's changes. After
//! it, the copy is the stream's source: its rows' changes are mapped, the
//! parent's are not. A change made around that moment could come twice
//! (once from each side) or not at all (RL69's "not exact"). Now the
//! worker records the **handover** atomically with the flip to `serving`
//! (`copy::STATE_TABLE`'s `handover_source` and `handover_child`):
//!
//! ```text
//! seed(copy):
//!   state := seeding (handover kept when the copy had served before: a
//!            reseed; else none: the first seed)
//!   START TRANSACTION WITH CONSISTENT SNAPSHOT on the source's cluster
//!   source := Binlog_snapshot_file/position → BINLOG_GTID_POS(...)
//!   read the source's view in that snapshot; COMMIT
//!   write the copy
//!   child := @@gtid_binlog_pos on the copy's cluster
//!   UPDATE state SET state = 'serving', handover = (source, child)  -- one row, one statement
//! ```
//!
//! Every change of the source with a GTID at or before `source` is in the
//! seed, every later one reaches the copy after the flip (the copy worker
//! applies a family's changes one at a time, after the seed); every write
//! of the seed is at or before `child`, every later write of the copy
//! after it. So:
//!
//! | change | no copy | first seed running | handover `(s, c)` | before RL71 |
//! |---|---|---|---|---|
//! | the source's row, GTID `g` | counts | held | counts iff `g <= s` | dropped |
//! | the copy's row, GTID `w` | — | dropped | counts iff `w > c` | counts |
//!
//! A stream reads the copy's state after it has the change, so "no copy"
//! means any seed's snapshot comes later: the change is in it. While the
//! first seed runs it can't tell which side will deliver a source change,
//! so it holds it until the handover is recorded. A reseed (the share's
//! filter changed, a schema change) keeps the handover: the copy stays the
//! source, and its seed's writes are the changes the stream hasn't sent
//! yet. A copy that served before handovers were recorded counts as
//! handed over before every change ("before RL71").
//!
//! GTIDs compare within one domain and server (one cluster's primary, with
//! `gtid_strict_mode`); an event from another server isn't comparable and
//! the caller falls back to RL69's rule (the view's sources now).

/// When a copy became its subscribers' source.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub enum Handover {
    /// Its first seed hasn't ended.
    #[default]
    Pending,
    /// It served before handovers were recorded: before every change.
    Always,
    /// At its first seed's end: `source`, the GTID position of the
    /// source's cluster the seed read at; `child`, the copy's cluster's
    /// position just before the flip.
    At {
        /// `@@gtid_binlog_pos` form, of the source's cluster.
        source: String,
        /// `@@gtid_binlog_pos` form, of the copy's cluster.
        child: String,
    },
}

impl Handover {
    /// From the state row's columns (`NULL`: pending; empty: always).
    #[must_use]
    pub fn from_columns(source: Option<&str>, child: Option<&str>) -> Self {
        match (source, child) {
            (Some(s), Some(c)) if s.is_empty() && c.is_empty() => Self::Always,
            (Some(s), Some(c)) => Self::At {
                source: s.to_owned(),
                child: c.to_owned(),
            },
            _ => Self::Pending,
        }
    }

    /// The state row's columns, as SQL (`NULL` or a string literal).
    #[must_use]
    pub fn columns_sql(&self) -> (String, String) {
        let lit = |s: &str| format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''"));
        match self {
            Self::Pending => ("NULL".into(), "NULL".into()),
            Self::Always => ("''".into(), "''".into()),
            Self::At { source, child } => (lit(source), lit(child)),
        }
    }

    /// Whether the copy is its subscribers' source (its first seed ended).
    #[must_use]
    pub fn handed_over(&self) -> bool {
        !matches!(self, Self::Pending)
    }
}

/// A GTID: domain, server, sequence number.
pub type Gtid = (u32, u32, u64);

/// The GTIDs of a `@@gtid_binlog_pos` text (`0-11-90812,7-21-1550`);
/// malformed parts are skipped.
#[must_use]
pub fn parse_pos(pos: &str) -> Vec<Gtid> {
    pos.split(',')
        .filter_map(|p| {
            let mut it = p.trim().splitn(3, '-');
            Some((
                it.next()?.parse().ok()?,
                it.next()?.parse().ok()?,
                it.next()?.parse().ok()?,
            ))
        })
        .collect()
}

/// Whether `g` is at or before `pos`: `None` when `pos` has no GTID of
/// `g`'s domain and server (another cluster's, or a failover's).
#[must_use]
pub fn at_or_before(pos: &str, g: Gtid) -> Option<bool> {
    parse_pos(pos)
        .into_iter()
        .find(|(d, s, _)| *d == g.0 && *s == g.1)
        .map(|(_, _, seq)| g.2 <= seq)
}

/// What a stream does with a change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Send it (mapped as the subscriber's view has it now).
    Count,
    /// Send nothing: the other side delivers it.
    Drop,
    /// Ask again later: the copy's first seed is running.
    Hold,
    /// The positions don't compare: decide by what the view reads now.
    Unknown,
}

/// A change `g` of a row the copy is filled from (a parent tenant's root
/// row, or its own copy's row), against the copy's handover (`None`: the
/// edge has no copy).
#[must_use]
pub fn source_change(copy: Option<&Handover>, g: Gtid) -> Verdict {
    match copy {
        None => Verdict::Count,
        Some(Handover::Pending) => Verdict::Hold,
        Some(Handover::Always) => Verdict::Drop,
        Some(Handover::At { source, .. }) => match at_or_before(source, g) {
            Some(true) => Verdict::Count,
            Some(false) => Verdict::Drop,
            None => Verdict::Unknown,
        },
    }
}

/// A change `w` of the copy's own row (`None`: the copy is gone).
#[must_use]
pub fn copy_change(copy: Option<&Handover>, w: Gtid) -> Verdict {
    match copy {
        None | Some(Handover::Pending) => Verdict::Drop,
        Some(Handover::Always) => Verdict::Count,
        Some(Handover::At { child, .. }) => match at_or_before(child, w) {
            Some(true) => Verdict::Drop,
            Some(false) => Verdict::Count,
            None => Verdict::Unknown,
        },
    }
}

/// A change along a line of tenant edges: `edges` are the copies of the
/// edges from the changed row's tenant down to the subscriber's, nearest
/// the change first (`None`: read through). A change of a copy's own row
/// first passes [`copy_change`] against its copy (`own`), then every edge
/// below as a change of its source. The first verdict that isn't `Count`
/// decides.
#[must_use]
pub fn along(own: Option<Option<&Handover>>, edges: &[Option<&Handover>], g: Gtid) -> Verdict {
    if let Some(copy) = own {
        let v = copy_change(copy, g);
        if v != Verdict::Count {
            return v;
        }
    }
    for e in edges {
        let v = source_change(*e, g);
        if v != Verdict::Count {
            return v;
        }
    }
    Verdict::Count
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(s: &str, c: &str) -> Handover {
        Handover::At {
            source: s.into(),
            child: c.into(),
        }
    }

    #[test]
    fn positions_compare_within_one_domain_and_server() {
        assert_eq!(
            parse_pos("0-11-90812,7-21-1550"),
            [(0, 11, 90812), (7, 21, 1550)]
        );
        assert_eq!(parse_pos(""), []);
        assert_eq!(at_or_before("1-101-50", (1, 101, 50)), Some(true));
        assert_eq!(at_or_before("1-101-50", (1, 101, 51)), Some(false));
        assert_eq!(at_or_before("1-101-50,2-201-9", (2, 201, 3)), Some(true));
        assert_eq!(at_or_before("1-101-50", (2, 201, 3)), None);
        assert_eq!(at_or_before("1-101-50", (1, 102, 3)), None);
    }

    #[test]
    fn the_handover_decides_which_side_sends_a_change() {
        let h = at("1-101-50", "1-101-80");
        // The source's changes: in the seed, or the copy's after the flip.
        assert_eq!(source_change(None, (1, 101, 99)), Verdict::Count);
        assert_eq!(
            source_change(Some(&Handover::Pending), (1, 101, 1)),
            Verdict::Hold
        );
        assert_eq!(source_change(Some(&h), (1, 101, 50)), Verdict::Count);
        assert_eq!(source_change(Some(&h), (1, 101, 51)), Verdict::Drop);
        assert_eq!(
            source_change(Some(&Handover::Always), (1, 101, 1)),
            Verdict::Drop
        );
        assert_eq!(source_change(Some(&h), (2, 201, 1)), Verdict::Unknown);
        // The copy's: the seed's writes are the source's changes already
        // sent.
        assert_eq!(copy_change(Some(&h), (1, 101, 80)), Verdict::Drop);
        assert_eq!(copy_change(Some(&h), (1, 101, 81)), Verdict::Count);
        assert_eq!(
            copy_change(Some(&Handover::Pending), (1, 101, 81)),
            Verdict::Drop
        );
        assert_eq!(copy_change(None, (1, 101, 81)), Verdict::Drop);
        assert_eq!(
            copy_change(Some(&Handover::Always), (1, 101, 1)),
            Verdict::Count
        );
    }

    #[test]
    fn every_change_comes_from_exactly_one_side() {
        // One source and one copy on one server: source changes at 1..=60,
        // the seed reads at 40, writes the copy at 41..=45 (the server's
        // sequence goes on), flips after 45; the worker then applies 41..
        // as copy writes 61.. .
        let h = at("1-1-40", "1-1-45");
        for g in 1..=60u64 {
            let from_source = source_change(Some(&h), (1, 1, g)) == Verdict::Count;
            // The copy write carrying change g: in the seed (≤ 40), or
            // applied after the flip.
            let w = if g <= 40 { 41 + g % 5 } else { 60 + g };
            let from_copy = copy_change(Some(&h), (1, 1, w)) == Verdict::Count;
            assert!(
                from_source ^ from_copy,
                "change {g}: source {from_source}, copy {from_copy}"
            );
        }
    }

    #[test]
    fn a_line_of_edges_passes_a_change_only_through_read_through_or_before_handover() {
        let up = at("1-1-40", "1-1-45");
        let down = at("1-1-70", "1-1-75");
        // A grandparent's change below a parent copy handed over at 40.
        assert_eq!(along(None, &[Some(&up), None], (1, 1, 30)), Verdict::Count);
        assert_eq!(along(None, &[Some(&up), None], (1, 1, 50)), Verdict::Drop);
        // The parent's copy row, then the subscriber's copy pending.
        assert_eq!(
            along(Some(Some(&up)), &[Some(&Handover::Pending)], (1, 1, 50)),
            Verdict::Hold
        );
        assert_eq!(
            along(Some(Some(&up)), &[Some(&down)], (1, 1, 60)),
            Verdict::Count
        );
        assert_eq!(
            along(Some(Some(&up)), &[Some(&down)], (1, 1, 71)),
            Verdict::Drop
        );
        // The seed's own write of the parent's copy.
        assert_eq!(
            along(Some(Some(&up)), &[Some(&down)], (1, 1, 44)),
            Verdict::Drop
        );
        // The subscriber's own copy row.
        assert_eq!(along(Some(Some(&down)), &[], (1, 1, 76)), Verdict::Count);
        assert_eq!(Handover::from_columns(Some("1-1-40"), Some("1-1-45")), up);
        assert_eq!(Handover::from_columns(Some(""), Some("")), Handover::Always);
        assert_eq!(Handover::from_columns(None, None), Handover::Pending);
        assert_eq!(up.columns_sql(), ("'1-1-40'".into(), "'1-1-45'".into()));
    }
}
