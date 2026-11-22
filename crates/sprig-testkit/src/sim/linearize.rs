//! A linearizability checker for register histories.
//!
//! The search is Porcupine's: Wing and Gong's backtracking over the
//! history's call and return events, with a cache of (set of operations
//! linearized, register value) pairs already explored, so equivalent
//! orders are tried once. Each register (key) is checked on its own, which
//! is sound because registers are independent.
//!
//! An operation whose outcome the client never learned (a write that timed
//! out) has its return at the end of time: it may take effect at any point
//! after its call, or (placed last) not at all.

use std::collections::HashSet;

/// What one operation did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegOp {
    /// Wrote the value.
    Write(u64),
    /// Read the value (`None`: never written).
    Read(Option<u64>),
}

/// One operation of a history, with its call and return times.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Op {
    /// When the client called.
    pub call: u64,
    /// When the client got the answer; `None` when it never did.
    pub ret: Option<u64>,
    /// What it did.
    pub op: RegOp,
}

/// The default bound on search steps.
pub const MAX_STEPS: u64 = 20_000_000;

const NIL: usize = usize::MAX;

/// Checks that `ops` on one register, starting unwritten, are
/// linearizable.
///
/// # Errors
/// The history is not linearizable (with a description), or the search
/// passed `max_steps`.
pub fn check_register(ops: &[Op], max_steps: u64) -> Result<(), String> {
    let n = ops.len();
    if n == 0 {
        return Ok(());
    }
    // Events: (time, is_return, op). Calls sort before returns at the same
    // time (overlapping, the permissive reading).
    let mut evs: Vec<(u64, bool, usize)> = Vec::with_capacity(2 * n);
    for (i, o) in ops.iter().enumerate() {
        evs.push((o.call, false, i));
        evs.push((o.ret.unwrap_or(u64::MAX), true, i));
    }
    evs.sort_unstable();
    let m = evs.len();
    let (head, tail) = (m, m + 1);
    let mut next = vec![NIL; m + 2];
    let mut prev = vec![NIL; m + 2];
    let mut ret_ev = vec![NIL; n];
    let mut order = Vec::with_capacity(m + 2);
    order.push(head);
    for (k, (_, is_ret, op)) in evs.iter().enumerate() {
        if *is_ret {
            ret_ev[*op] = k;
        }
        order.push(k);
    }
    order.push(tail);
    for w in order.windows(2) {
        next[w[0]] = w[1];
        prev[w[1]] = w[0];
    }
    let lift = |next: &mut Vec<usize>, prev: &mut Vec<usize>, e: usize| {
        let (p, q) = (prev[e], next[e]);
        next[p] = q;
        prev[q] = p;
    };
    let unlift = |next: &mut Vec<usize>, prev: &mut Vec<usize>, e: usize| {
        let (p, q) = (prev[e], next[e]);
        next[p] = e;
        prev[q] = e;
    };
    let words = n.div_ceil(64);
    let mut lin = vec![0u64; words];
    let mut state: Option<u64> = None;
    let mut cache: HashSet<(Vec<u64>, Option<u64>)> = HashSet::new();
    let mut stack: Vec<(usize, Option<u64>)> = Vec::new();
    let mut best = 0usize;
    let mut entry = next[head];
    let mut steps = 0u64;
    while next[head] != tail {
        steps += 1;
        if steps > max_steps {
            return Err(format!(
                "linearizability search passed {max_steps} steps ({n} operations)"
            ));
        }
        if entry == tail {
            // Unreachable for well-formed histories: every call has a later
            // return.
            return Err("malformed history".into());
        }
        let (_, is_ret, op) = evs[entry];
        if is_ret {
            // An operation returned before being placed: undo the last
            // choice and try the next one.
            let Some((e, s)) = stack.pop() else {
                return Err(describe(ops, best));
            };
            let (_, _, op) = evs[e];
            state = s;
            lin[op / 64] &= !(1 << (op % 64));
            unlift(&mut next, &mut prev, ret_ev[op]);
            unlift(&mut next, &mut prev, e);
            entry = next[e];
        } else {
            let (ok, ns) = match ops[op].op {
                RegOp::Write(v) => (true, Some(v)),
                RegOp::Read(r) => (r == state, state),
            };
            if ok {
                let mut l2 = lin.clone();
                l2[op / 64] |= 1 << (op % 64);
                if cache.insert((l2.clone(), ns)) {
                    stack.push((entry, state));
                    best = best.max(stack.len());
                    state = ns;
                    lin = l2;
                    lift(&mut next, &mut prev, entry);
                    lift(&mut next, &mut prev, ret_ev[op]);
                    entry = next[head];
                    continue;
                }
            }
            entry = next[entry];
        }
    }
    Ok(())
}

fn describe(ops: &[Op], best: usize) -> String {
    let mut sorted: Vec<&Op> = ops.iter().collect();
    sorted.sort_by_key(|o| o.call);
    let lines: Vec<String> = sorted
        .iter()
        .take(60)
        .map(|o| {
            format!(
                "  {:?} [{} .. {}]",
                o.op,
                o.call,
                o.ret.map_or_else(|| "?".to_owned(), |r| r.to_string())
            )
        })
        .collect();
    format!(
        "not linearizable: {} operations, at most {best} placed in order; first operations:\n{}",
        ops.len(),
        lines.join("\n")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn w(call: u64, ret: u64, v: u64) -> Op {
        Op {
            call,
            ret: Some(ret),
            op: RegOp::Write(v),
        }
    }
    fn r(call: u64, ret: u64, v: Option<u64>) -> Op {
        Op {
            call,
            ret: Some(ret),
            op: RegOp::Read(v),
        }
    }

    #[test]
    fn accepts_linearizable_histories() {
        // Sequential.
        assert!(check_register(&[w(0, 1, 1), r(2, 3, Some(1))], MAX_STEPS).is_ok());
        // A read overlapping a write may see either value.
        assert!(check_register(&[w(0, 10, 1), r(2, 3, None)], MAX_STEPS).is_ok());
        assert!(check_register(&[w(0, 10, 1), r(2, 3, Some(1))], MAX_STEPS).is_ok());
        // Concurrent writes, reads agreeing on one order.
        let h = [
            w(0, 10, 1),
            w(0, 10, 2),
            r(11, 12, Some(1)),
            r(13, 14, Some(1)),
        ];
        assert!(check_register(&h, MAX_STEPS).is_ok());
        // A write with an unknown outcome may never take effect...
        let unknown = Op {
            call: 0,
            ret: None,
            op: RegOp::Write(7),
        };
        assert!(check_register(&[unknown, r(5, 6, None)], MAX_STEPS).is_ok());
        // ...or take effect late.
        assert!(check_register(&[unknown, r(5, 6, None), r(7, 8, Some(7))], MAX_STEPS).is_ok());
    }

    #[test]
    fn rejects_stale_and_impossible_reads() {
        // A read after a completed write sees the old value: stale.
        assert!(check_register(&[w(0, 1, 1), w(2, 3, 2), r(4, 5, Some(1))], MAX_STEPS).is_err());
        // Two reads see the values of concurrent writes in opposite orders,
        // one after the other.
        let h = [
            w(0, 10, 1),
            w(0, 10, 2),
            r(11, 12, Some(1)),
            r(13, 14, Some(2)),
            r(15, 16, Some(1)),
        ];
        assert!(check_register(&h, MAX_STEPS).is_err());
        // A value nobody wrote.
        assert!(check_register(&[r(0, 1, Some(9))], MAX_STEPS).is_err());
        // An unknown write can't be seen before it was called.
        let late = Op {
            call: 10,
            ret: None,
            op: RegOp::Write(7),
        };
        assert!(check_register(&[late, r(0, 5, Some(7))], MAX_STEPS).is_err());
    }
}
