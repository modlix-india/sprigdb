//! The tenant wall on the Redis path. A tenant's databases reach the
//! engine only through a [`Walled`] unit built for that tenant (by
//! [`crate::Backend`], and by its sweeper for each tenant's databases), and
//! every batch of it refuses, at the call, a key, a range or a note that
//! lies outside the tenant's part of the keyspace. A command that built a
//! key for another tenant, by any mistake, fails instead of reading or
//! changing that tenant's data.
//!
//! A tenant's part of the keyspace is every key that starts with its id
//! (8 bytes, big-endian): the head of the engine's key prefix. A range must
//! start inside it and end no later than the next tenant's first key.

use std::sync::Arc;

use bytes::Bytes;

use crate::store::{Batch, StoreError, StoreResult, Unit};

/// One tenant's part of the keyspace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TenantWall {
    tenant: u64,
}

impl TenantWall {
    /// The wall around `tenant`'s keys.
    #[must_use]
    pub const fn new(tenant: u64) -> Self {
        Self { tenant }
    }

    /// The tenant inside the wall.
    #[must_use]
    pub const fn tenant(self) -> u64 {
        self.tenant
    }

    fn breach(self, what: &str) -> StoreError {
        StoreError::fatal(format!(
            "tenant wall: a batch of tenant {}'s Redis databases named {what} outside the tenant's keys",
            self.tenant
        ))
    }

    /// `key` is the tenant's.
    ///
    /// # Errors
    /// It is another tenant's (or no tenant's).
    pub fn key(self, key: &[u8]) -> StoreResult<()> {
        if key.starts_with(&self.tenant.to_be_bytes()) {
            Ok(())
        } else {
            Err(self.breach("a key"))
        }
    }

    /// Every one of `keys` is the tenant's.
    ///
    /// # Errors
    /// One is not.
    pub fn keys(self, keys: &[Bytes]) -> StoreResult<()> {
        keys.iter().try_for_each(|k| self.key(k))
    }

    /// `start <= key < end` holds only the tenant's keys.
    ///
    /// # Errors
    /// It starts outside them, or ends past the next tenant's first key.
    pub fn range(self, start: &[u8], end: &[u8]) -> StoreResult<()> {
        let inside = start.starts_with(&self.tenant.to_be_bytes())
            && self
                .tenant
                .checked_add(1)
                .is_none_or(|next| end <= &next.to_be_bytes()[..]);
        if inside {
            Ok(())
        } else {
            Err(self.breach("a range"))
        }
    }
}

/// A unit that serves one tenant's databases only.
#[derive(Debug)]
pub struct Walled<U> {
    unit: Arc<U>,
    wall: TenantWall,
}

impl<U: Unit> Walled<U> {
    /// `unit`, for `tenant`'s keys alone.
    #[must_use]
    pub fn new(unit: Arc<U>, tenant: u64) -> Self {
        Self {
            unit,
            wall: TenantWall::new(tenant),
        }
    }

    /// The wall.
    #[must_use]
    pub const fn wall(&self) -> TenantWall {
        self.wall
    }
}

impl<U: Unit> Unit for Walled<U> {
    type Batch = WalledBatch<U::Batch>;

    async fn begin(&self, keys: Vec<Bytes>) -> StoreResult<Self::Batch> {
        self.wall.keys(&keys)?;
        Ok(WalledBatch {
            inner: self.unit.begin(keys).await?,
            wall: self.wall,
        })
    }

    fn leader_epoch(&self, key: &[u8]) -> u64 {
        self.unit.leader_epoch(key)
    }

    fn now_ms(&self) -> i64 {
        self.unit.now_ms()
    }
}

/// A batch of one tenant's databases.
#[derive(Debug)]
pub struct WalledBatch<B> {
    inner: B,
    wall: TenantWall,
}

impl<B: Batch> Batch for WalledBatch<B> {
    type Ticket = B::Ticket;

    fn get(&mut self, key: &[u8]) -> StoreResult<Option<Bytes>> {
        self.wall.key(key)?;
        self.inner.get(key)
    }

    fn scan(&mut self, start: &[u8], end: &[u8], limit: usize) -> StoreResult<Vec<(Bytes, Bytes)>> {
        self.wall.range(start, end)?;
        self.inner.scan(start, end, limit)
    }

    fn scan_rev(
        &mut self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> StoreResult<Vec<(Bytes, Bytes)>> {
        self.wall.range(start, end)?;
        self.inner.scan_rev(start, end, limit)
    }

    fn scan_latest(
        &mut self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> StoreResult<Vec<(Bytes, Bytes)>> {
        self.wall.range(start, end)?;
        self.inner.scan_latest(start, end, limit)
    }

    fn put(&mut self, key: Bytes, value: Bytes) -> StoreResult<()> {
        self.wall.key(&key)?;
        self.inner.put(key, value)
    }

    fn delete(&mut self, key: Bytes) -> StoreResult<()> {
        self.wall.key(&key)?;
        self.inner.delete(key)
    }

    async fn hold(&mut self, keys: Vec<Bytes>) -> StoreResult<()> {
        self.wall.keys(&keys)?;
        self.inner.hold(keys).await
    }

    fn read_latest(&mut self) {
        self.inner.read_latest();
    }

    fn note(&mut self, tenant: u64, note: Vec<u8>) -> StoreResult<()> {
        if tenant != self.wall.tenant {
            return Err(self.wall.breach("a note"));
        }
        self.inner.note(tenant, note)
    }

    fn commit(self) -> StoreResult<B::Ticket> {
        self.inner.commit()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::layout::Space;
    use crate::mem::MemUnit;
    use crate::store::Ticket;

    fn b(v: &[u8]) -> Bytes {
        Bytes::copy_from_slice(v)
    }

    #[test]
    fn the_wall_holds_exactly_the_tenants_keys() {
        let w = TenantWall::new(7);
        let mine = Space::redis(7, 3).meta(b"k");
        let theirs = Space::redis(8, 3).meta(b"k");
        let below = Space::redis(6, 0).meta(b"k");
        assert!(w.key(&mine).is_ok());
        assert!(w.key(&theirs).is_err());
        assert!(w.key(&below).is_err());
        assert!(w.key(b"").is_err());
        // A whole database, and the whole tenant up to the next one.
        assert!(
            w.range(
                &Space::redis(7, 0).prefix(b'M'),
                &Space::redis(7, 15).prefix(b'Z')
            )
            .is_ok()
        );
        assert!(w.range(&7u64.to_be_bytes(), &8u64.to_be_bytes()).is_ok());
        // Past the next tenant's first key, or starting below the tenant.
        assert!(w.range(&7u64.to_be_bytes(), &theirs).is_err());
        assert!(w.range(&below, &mine).is_err());
        // The last tenant has no next one.
        let last = TenantWall::new(u64::MAX);
        assert!(last.range(&u64::MAX.to_be_bytes(), &[0xFF; 20]).is_ok());
        assert!(last.range(&7u64.to_be_bytes(), &[0xFF; 20]).is_err());
    }

    #[tokio::test]
    async fn a_walled_batch_refuses_another_tenants_keys_at_the_call() {
        let mem = Arc::new(MemUnit::default());
        // Tenant 8 owns a key.
        let theirs = Space::redis(8, 0).meta(b"k");
        let mut b8 = Walled::new(Arc::clone(&mem), 8)
            .begin(vec![b(&theirs)])
            .await
            .unwrap();
        b8.put(b(&theirs), b(b"8's")).unwrap();
        b8.commit().unwrap().durable().await.unwrap();

        let unit7 = Walled::new(Arc::clone(&mem), 7);
        assert!(
            unit7.begin(vec![b(&theirs)]).await.is_err(),
            "began holding 8's key"
        );
        let mut b7 = unit7.begin(Vec::new()).await.unwrap();
        assert!(b7.get(&theirs).is_err(), "read 8's key");
        assert!(b7.put(b(&theirs), b(b"7's")).is_err(), "wrote 8's key");
        assert!(b7.delete(b(&theirs)).is_err(), "deleted 8's key");
        assert!(b7.hold(vec![b(&theirs)]).await.is_err(), "held 8's key");
        assert!(b7.note(8, b"event".to_vec()).is_err(), "noted for 8");
        // A range from 7's keys into 8's.
        assert!(b7.scan(&7u64.to_be_bytes(), &theirs, 10).is_err());
        assert!(b7.scan_rev(&7u64.to_be_bytes(), &[0xFF; 9], 10).is_err());
        assert!(
            b7.scan_latest(&7u64.to_be_bytes(), &9u64.to_be_bytes(), 10)
                .is_err()
        );
        // Its own keys work as before.
        let mine = Space::redis(7, 0).meta(b"k");
        b7.hold(vec![b(&mine)]).await.unwrap();
        b7.put(b(&mine), b(b"7's")).unwrap();
        assert!(
            b7.scan(&7u64.to_be_bytes(), &8u64.to_be_bytes(), 10)
                .unwrap()
                .len()
                == 1
        );
        b7.commit().unwrap().durable().await.unwrap();
        // 8's key is untouched.
        let mut r = Walled::new(mem, 8).begin(Vec::new()).await.unwrap();
        assert_eq!(r.get(&theirs).unwrap(), Some(b(b"8's")));
    }

    /// The mistake the wall is for: tenant 7's database built over tenant
    /// 8's part of the keyspace. On the bare unit its commands read and
    /// overwrite 8's keys; on 7's walled unit they fail and change nothing.
    #[tokio::test]
    async fn commands_of_a_database_built_for_the_wrong_tenant_fail() {
        use crate::db::{EngineDb, Uids, Versions, Waiters};

        let mem = Arc::new(MemUnit::default());
        let wrong = Space::redis(8, 0);
        let set = [b(b"SET"), b(b"k"), b(b"7's")];
        let get = [b(b"GET"), b(b"k")];

        // Tenant 8 owns k.
        let eight = Arc::new(Walled::new(Arc::clone(&mem), 8));
        let d8 = EngineDb::new(
            Arc::clone(&eight),
            wrong,
            Arc::new([wrong]),
            Uids::new(eight, 8),
            Versions::new(),
            Arc::default(),
        );
        d8.run_batch(&[&[b(b"SET"), b(b"k"), b(b"8's")][..]]).await;

        // Without the wall, the mistake reaches 8's key.
        let bare = EngineDb::new(
            Arc::clone(&mem),
            wrong,
            Arc::new([wrong]),
            Uids::new(Arc::clone(&mem), 8),
            Versions::new(),
            Arc::<Waiters>::default(),
        );
        assert_eq!(
            bare.run_batch(&[&get[..]]).await,
            vec![sprig_proto_redis::Frame::Bulk(b(b"8's"))]
        );

        // With 7's wall, it doesn't.
        let seven = Arc::new(Walled::new(Arc::clone(&mem), 7));
        let d7 = EngineDb::new(
            Arc::clone(&seven),
            wrong,
            Arc::new([wrong]),
            Uids::new(seven, 7),
            Versions::new(),
            Arc::default(),
        );
        for cmd in [
            &set[..],
            &get[..],
            &[b(b"DEL"), b(b"k")][..],
            &[b(b"KEYS"), b(b"*")][..],
        ] {
            let reply = d7.run_batch(&[cmd]).await;
            assert!(
                matches!(&reply[..], [sprig_proto_redis::Frame::Error(e)]
                    if String::from_utf8_lossy(e).contains("tenant wall")),
                "{:?} on the wrong tenant's space answered {reply:?}",
                String::from_utf8_lossy(&cmd[0]),
            );
        }
        assert_eq!(
            d8.run_batch(&[&get[..]]).await,
            vec![sprig_proto_redis::Frame::Bulk(b(b"8's"))]
        );
    }
}
