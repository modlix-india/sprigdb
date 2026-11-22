//! TTL indexes: a sweeper reads each TTL index's date entries older than
//! the index's `expireAfterSeconds` and deletes their documents, each
//! rechecked under its lock (MongoDB's TTL monitor, every 60 seconds by
//! default).

use std::sync::Arc;
use std::time::Duration;

use bson::Bson;
use sprig_engine::txn::Dir;
use sprig_proto_mongo::errors::MongoError;

use super::catalog::FieldKind;
use super::layout::after_prefix;
use super::read::decode;
use super::{Coll, DocStore};
use crate::keyenc::{self, class};

/// The earliest date at `path` in `d` (array elements included).
fn earliest(d: &bson::Document, path: &str) -> Option<i64> {
    let vs = crate::filter::values_at(d, path);
    let mut best: Option<i64> = None;
    for v in vs.vals {
        let items: Vec<&Bson> = match v {
            Bson::Array(a) => a.iter().collect(),
            other => vec![other],
        };
        for x in items {
            if let Bson::DateTime(t) = x {
                let ms = t.timestamp_millis();
                best = Some(best.map_or(ms, |b| b.min(ms)));
            }
        }
    }
    best
}

impl DocStore {
    /// One pass over every TTL index of the tenants this store has served:
    /// deletes up to `budget` expired documents. Returns how many it
    /// deleted.
    ///
    /// # Errors
    /// The store failed.
    pub async fn sweep_ttl(&self, budget: usize) -> Result<usize, MongoError> {
        let tenants: Vec<u64> = self.tenants();
        let now = self.now_ms();
        let mut deleted = 0usize;
        for tenant in tenants {
            for c in self.list(tenant, None)? {
                for idx in &c.meta.indexes {
                    let Some(secs) = idx.expire_after else {
                        continue;
                    };
                    if deleted >= budget {
                        return Ok(deleted);
                    }
                    let cutoff = now.saturating_sub(secs.saturating_mul(1000));
                    deleted += self
                        .sweep_index(tenant, &c, idx.id, cutoff, budget - deleted)
                        .await?;
                }
            }
        }
        Ok(deleted)
    }

    async fn sweep_index(
        &self,
        tenant: u64,
        listed: &Arc<Coll>,
        index_id: u16,
        cutoff: i64,
        budget: usize,
    ) -> Result<usize, MongoError> {
        // The guard first, then the collection as it is now: deletes
        // maintain the index list current under the guard.
        let _g = self.write_guard(tenant).await;
        let name = super::Name::new(&listed.meta.db, &listed.meta.name);
        let Some(c) = self.coll(tenant, &name)? else {
            return Ok(0);
        };
        let c = &c;
        let Some(idx) = c.meta.indexes.iter().find(|i| i.id == index_id) else {
            return Ok(0);
        };
        let Some((path, kind)) = idx.fields.first() else {
            return Ok(0);
        };
        let base = c.space.index(idx.id, idx.unique);
        let cut = keyenc::key(&Bson::DateTime(bson::DateTime::from_millis(cutoff)));
        let (lo, hi) = if *kind == FieldKind::Desc {
            let mut cc = cut.clone();
            keyenc::complement(&mut cc);
            let mut l = base.clone();
            l.extend_from_slice(&after_prefix(&cc));
            let mut h = base.clone();
            h.push((!class::DATE).saturating_add(1));
            (l, h)
        } else {
            let mut l = base.clone();
            l.push(class::DATE);
            let mut h = base.clone();
            h.extend_from_slice(&cut);
            (l, h)
        };
        let mut n = 0usize;
        let mut from = lo;
        // Batches of up to 1,000 until the expired range is read or the
        // budget is spent.
        while n < budget {
            let mut t = self.begin(false);
            let rows = t.scan(&from, &hi, Dir::Forward, (budget - n).min(1000))?;
            let Some((last, _)) = rows.last() else {
                t.rollback();
                break;
            };
            from = after_prefix(last);
            let full = rows.len() == (budget - n).min(1000);
            for (_, idk) in rows {
                let key = c.space.doc(&idk);
                t.lock_doc(&key).await?;
                let Some(v) = t.get_current(&key)? else {
                    continue;
                };
                let d = decode(&v)?;
                if earliest(&d, path).is_some_and(|ms| ms < cutoff) {
                    self.delete_doc(&mut t, c, &idk, &d).await?;
                    n += 1;
                }
            }
            t.commit().await?;
            if !full {
                break;
            }
        }
        if n > 0 {
            tracing::debug!(ns = %c.ns(), index = %idx.name, deleted = n, "ttl sweep");
        }
        Ok(n)
    }

    /// Starts the sweeper: a pass every `every`.
    pub fn spawn_ttl(self: &Arc<Self>, every: Duration) -> tokio::task::JoinHandle<()> {
        let me = Arc::clone(self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                loop {
                    match me.sweep_ttl(10_000).await {
                        Ok(n) if n >= 10_000 => {}
                        Ok(_) => break,
                        Err(e) => {
                            tracing::warn!(error = %e, "ttl sweep failed");
                            break;
                        }
                    }
                }
            }
        })
    }
}
