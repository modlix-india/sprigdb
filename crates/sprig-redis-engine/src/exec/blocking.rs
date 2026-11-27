//! The blocking commands' single attempts: `BLPOP`, `BRPOP`, `BLMOVE`,
//! `BRPOPLPUSH`, `BLMPOP`, `BZPOPMIN`, `BZPOPMAX`, `BZMPOP`. An attempt
//! that finds nothing to pop answers the command's timeout reply (a nil);
//! [`crate::db`] waits for a push to one of the keys and tries again, with
//! no engine lock held while it waits. Inside `MULTI` one attempt is all
//! there is, as in Redis.
//!
//! `XREAD` and `XREADGROUP` with `BLOCK` wait the same way; their attempts
//! are the plain commands ([`super::streams`]).

use bytes::Bytes;
use sprig_proto_redis::Frame;
use sprig_proto_redis::num::parse_f64;

use super::lists::{lmove, mpop_args, try_mpop};
use super::zsets::{mpop_reply, score, try_zmpop, z_pop, zmpop_args};
use super::{Arg, Cx, Out, tri};
use crate::store::Batch;

/// Whether `name` may block.
#[must_use]
pub fn is_blocking(name: &str) -> bool {
    matches!(
        name,
        "blpop" | "brpop" | "blmove" | "brpoplpush" | "blmpop" | "bzpopmin" | "bzpopmax" | "bzmpop"
    )
}

/// Whether command `a` (named `name`) waits when it finds nothing: the
/// blocking commands, and `XREAD`/`XREADGROUP` with `BLOCK` (a group read
/// only for new entries, `>`).
#[must_use]
pub fn blocks(name: &str, a: &[Bytes]) -> bool {
    match name {
        "xread" | "xreadgroup" => {
            let group = name == "xreadgroup";
            super::streams::read_args(a, group).is_ok_and(|r| {
                r.block.is_some()
                    && (!group || a[r.keys_at + r.n..].iter().all(|id| id.as_ref() == b">"))
            })
        }
        _ => is_blocking(name),
    }
}

/// Whether a blocked command is a stream read: every write to its keys
/// wakes it (readers don't take entries from each other).
#[must_use]
pub fn wakes_all(name: &str) -> bool {
    matches!(name, "xread" | "xreadgroup")
}

/// The reply when nothing arrives in time.
#[must_use]
pub fn timed_out(name: &str) -> Frame {
    if matches!(name, "blmove" | "brpoplpush") {
        Frame::null()
    } else {
        Frame::null_array()
    }
}

/// The timeout argument, in seconds (0: forever).
///
/// # Errors
/// The reply for a bad timeout.
pub fn timeout(name: &str, a: &[Bytes]) -> Arg<f64> {
    if wakes_all(name) {
        let r = super::streams::read_args(a, name == "xreadgroup")?;
        return Ok(r.block.unwrap_or(0) as f64 / 1000.0);
    }
    let t = match name {
        "blmove" => &a[5],
        "brpoplpush" => &a[3],
        "blmpop" | "bzmpop" => &a[1],
        _ => &a[a.len() - 1],
    };
    match parse_f64(t) {
        Some(t) if t < 0.0 => Err(Frame::err("timeout is negative")),
        Some(t) if t.is_finite() && t < 1e15 => Ok(t),
        _ => Err(Frame::err("timeout is not a float or out of range")),
    }
}

/// The keys a blocked command waits on (only the sources).
#[must_use]
pub fn wait_keys(name: &str, a: &[Bytes]) -> Vec<Bytes> {
    match name {
        "blmove" | "brpoplpush" => vec![a[1].clone()],
        "blmpop" => mpop_args(a, 2).map(|x| x.0).unwrap_or_default(),
        "bzmpop" => zmpop_args(a, 2).map(|x| x.0).unwrap_or_default(),
        "xread" | "xreadgroup" => super::streams::read_keys(a, name == "xreadgroup"),
        _ => a[1..a.len() - 1].to_vec(),
    }
}

/// One attempt: the reply, or the timeout reply when every source is
/// empty.
pub fn run<B: Batch>(cx: &mut Cx<'_, B>, name: &str, a: &[Bytes]) -> Option<Out> {
    if !is_blocking(name) {
        return None;
    }
    Some(attempt(cx, name, a))
}

fn attempt<B: Batch>(cx: &mut Cx<'_, B>, name: &str, a: &[Bytes]) -> Out {
    if !matches!(name, "blmpop" | "bzmpop" | "blmove") {
        tri!(timeout(name, a));
    }
    match name {
        "blpop" | "brpop" => {
            let keys = &a[1..a.len() - 1];
            Ok(match tri!(try_mpop(cx, keys, name == "blpop", 1)?) {
                None => timed_out(name),
                Some((k, mut items)) => Frame::Array(vec![
                    Frame::Bulk(k),
                    Frame::Bulk(items.pop().unwrap_or_default()),
                ]),
            })
        }
        "blmove" | "brpoplpush" => {
            let (from, to) = if name == "brpoplpush" {
                (false, true)
            } else {
                match (super::lists::end_of(&a[3]), super::lists::end_of(&a[4])) {
                    (Some(f), Some(t)) => {
                        tri!(timeout(name, a));
                        (f, t)
                    }
                    _ => return Ok(sprig_redis::reply::syntax()),
                }
            };
            lmove(cx, &a[1], &a[2], from, to).map(|r| r.unwrap_or_else(|f| f))
        }
        "blmpop" => {
            let (keys, left, count) = tri!(mpop_args(a, 2));
            tri!(timeout(name, a));
            Ok(match tri!(try_mpop(cx, &keys, left, count)?) {
                None => timed_out(name),
                Some((k, items)) => Frame::Array(vec![
                    Frame::Bulk(k),
                    Frame::Array(items.into_iter().map(Frame::Bulk).collect()),
                ]),
            })
        }
        "bzpopmin" | "bzpopmax" => {
            for k in &a[1..a.len() - 1] {
                let z = tri!(cx.zset(k)?);
                if z.body.is_some() {
                    let items = z_pop(cx, k, &z, 1, name == "bzpopmax")?;
                    if let Some((s, m)) = items.into_iter().next() {
                        return Ok(Frame::Array(vec![
                            Frame::Bulk(k.clone()),
                            Frame::Bulk(m),
                            score(s),
                        ]));
                    }
                }
            }
            Ok(timed_out(name))
        }
        _ => {
            let (keys, max, count) = tri!(zmpop_args(a, 2));
            tri!(timeout(name, a));
            Ok(match tri!(try_zmpop(cx, &keys, max, count)?) {
                None => timed_out(name),
                Some((k, items)) => mpop_reply(k, items),
            })
        }
    }
}
