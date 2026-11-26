//! Active expiry: deletes keys whose expiry passed and that nobody read
//! or wrote since, as Redis's own expiry cycle does. Reads already hide
//! them; this gives their space back.

use bytes::Bytes;

use crate::exec;
use crate::layout::Space;
use crate::notify::{self, Flags};
use crate::store::{Batch, StoreResult, Ticket, Unit};

/// Expiry entries handled per pass and database.
pub const PASS_LIMIT: usize = 10_000;

/// Expiry entries per commit.
const CHUNK: usize = 512;

/// Entries of dead collections deleted per commit.
const DEAD_PER_COMMIT: usize = 20_000;

/// Commits of dead-collection deletes per pass and database.
const DEAD_COMMITS_PER_PASS: usize = 25;

/// One pass over `space`: deletes up to `limit` keys due by the unit's
/// clock. Returns how many it deleted.
///
/// # Errors
/// The engine's.
pub async fn pass<U: Unit>(unit: &U, space: Space, limit: usize) -> StoreResult<usize> {
    pass_notifying(unit, space, limit, &Flags::default()).await
}

/// [`pass`], recording "expired" (and "hexpired") events with each
/// commit while the tenant's `flags` record them.
///
/// # Errors
/// The engine's.
pub async fn pass_notifying<U: Unit>(
    unit: &U,
    space: Space,
    limit: usize,
    flags: &Flags,
) -> StoreResult<usize> {
    let now = unit.now_ms();
    let (lo, hi) = space.expiry_due(now);
    // Only the unit's leader deletes: a replica that doesn't lead it
    // applies the leader's deletes instead.
    if unit.leader_epoch(&lo) == 0 {
        return Ok(0);
    }
    let due: Vec<(i64, Bytes)> = {
        let mut b = unit.begin(Vec::new()).await?;
        let got = b.scan(&lo, &hi, limit)?;
        b.commit()?.durable().await?;
        got.iter()
            .filter_map(|(k, _)| {
                space
                    .parse_expiry(k)
                    .map(|(at, key)| (at, Bytes::from(key)))
            })
            .collect()
    };
    let mut deleted = 0;
    for chunk in due.chunks(CHUNK) {
        let mut keys: Vec<Bytes> = chunk.iter().map(|(_, k)| space.meta(k)).collect();
        keys.sort_unstable();
        keys.dedup();
        let mut b = unit.begin(keys).await?;
        let gone = exec::expire_due_keys(&mut b, space, now, chunk)?;
        let f = flags.recording();
        if f != 0 {
            notify::add_note(&mut b, space.tenant, &notify::swept(space, &gone, &[], f))?;
        }
        b.commit()?.durable().await?;
        deleted += gone.len();
    }
    // Entries of big collections deleted earlier, a bounded batch at a
    // time.
    for _ in 0..DEAD_COMMITS_PER_PASS {
        let mut b = unit.begin(Vec::new()).await?;
        let n = exec::collect_dead(&mut b, space, DEAD_PER_COMMIT)?;
        b.commit()?.durable().await?;
        if n < DEAD_PER_COMMIT {
            break;
        }
    }
    // Hashes with fields whose lifetime has passed.
    let (lo, hi) = space.hdue_due(now);
    let hashes: Vec<(i64, Bytes)> = {
        let mut b = unit.begin(Vec::new()).await?;
        let got = b.scan(&lo, &hi, limit)?;
        b.commit()?.durable().await?;
        got.iter()
            .filter_map(|(k, _)| space.parse_hdue(k).map(|(at, key)| (at, Bytes::from(key))))
            .collect()
    };
    for chunk in hashes.chunks(CHUNK) {
        let mut keys: Vec<Bytes> = chunk.iter().map(|(_, k)| space.meta(k)).collect();
        keys.sort_unstable();
        keys.dedup();
        let mut b = unit.begin(keys).await?;
        let trimmed = exec::expire_fields_keys(&mut b, space, now, chunk)?;
        let f = flags.recording();
        if f != 0 {
            notify::add_note(
                &mut b,
                space.tenant,
                &notify::swept(space, &[], &trimmed, f),
            )?;
        }
        b.commit()?.durable().await?;
    }
    Ok(deleted)
}
