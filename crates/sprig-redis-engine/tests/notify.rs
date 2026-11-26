//! Keyspace notifications' events, recorded with each batch: names and
//! order per command (Redis 7.4's), nothing while the tenant's flags are
//! off, the classes filter, and the sweeper's "expired".

#![allow(clippy::unwrap_used)]

use std::sync::Arc;

use bytes::Bytes;
use sprig_redis::keyspace::parse_flags;
use sprig_redis_engine::layout::Space;
use sprig_redis_engine::mem::MemUnit;
use sprig_redis_engine::{Backend, Tenants, notify};

const T: u64 = 77;

fn b(s: &str) -> Bytes {
    Bytes::copy_from_slice(s.as_bytes())
}

fn cmd(s: &str) -> Vec<Bytes> {
    s.split_whitespace().map(b).collect()
}

struct Bed {
    unit: Arc<MemUnit>,
    backend: Backend<MemUnit>,
}

impl Bed {
    fn new(flags: &str) -> Self {
        let unit = Arc::new(MemUnit::new());
        let backend = Backend::new(Arc::clone(&unit), Tenants::All, 16);
        backend.set_flags(T, parse_flags(flags.as_bytes()).unwrap());
        Self { unit, backend }
    }

    /// Runs `cmds` as one batch; the events recorded, as `key:event`.
    async fn run(&self, cmds: &[&str]) -> Vec<String> {
        let owned: Vec<Vec<Bytes>> = cmds.iter().map(|c| cmd(c)).collect();
        let refs: Vec<&[Bytes]> = owned.iter().map(Vec::as_slice).collect();
        self.backend.db(T, 0).unwrap().run_batch(&refs).await;
        self.events()
    }

    fn events(&self) -> Vec<String> {
        let mut out = Vec::new();
        for (tenant, note) in self.unit.take_notes() {
            assert_eq!(tenant, T);
            let (key, value) = sprig_engine::observe::event::decode(&note).unwrap();
            assert_eq!(key, notify::note_key(T).as_slice());
            for (db, name, k) in notify::decode(value).unwrap() {
                assert_eq!(db, 0);
                out.push(format!("{}:{name}", String::from_utf8_lossy(&k)));
            }
        }
        out
    }
}

#[tokio::test]
async fn events_per_command_in_redis_order() {
    let t = Bed::new("AKEn");
    let cases: &[(&[&str], &[&str])] = &[
        (&["SET a 1"], &["a:new", "a:set"]),
        (&["SET a 2 EX 100"], &["a:set", "a:expire"]),
        (&["SET a 3 NX"], &[]),
        (&["MSET a 1 b 2"], &["a:set", "b:new", "b:set"]),
        (&["DEL a b c"], &["a:del", "b:del"]),
        (&["INCR n"], &["n:new", "n:incrby"]),
        (&["RENAME n m"], &["m:new", "n:rename_from", "m:rename_to"]),
        (&["RPUSH l x y"], &["l:new", "l:rpush"]),
        (&["LPOP l", "LPOP l"], &["l:lpop", "l:lpop", "l:del"]),
        (&["RPUSH s v"], &["s:new", "s:rpush"]),
        (
            &["LMOVE s d RIGHT LEFT"],
            &["d:new", "d:lpush", "s:rpop", "s:del"],
        ),
        (&["HSET h f v"], &["h:new", "h:hset"]),
        (&["HDEL h f"], &["h:hdel", "h:del"]),
        (&["SADD x 1 2"], &["x:new", "x:sadd"]),
        (&["SADD x 1"], &[]),
        (&["SMOVE x y 1"], &["x:srem", "y:new", "y:sadd"]),
        (&["ZADD z 1 a"], &["z:new", "z:zadd"]),
        (&["ZADD z 2 a"], &["z:zadd"]),
        (&["ZADD z 2 a"], &[]),
        (&["ZINTERSTORE z 2 z nope"], &["z:del"]),
        (&["EXPIRE y -1"], &["y:del"]),
        (&["FLUSHDB"], &[]),
    ];
    for (cmds, want) in cases {
        assert_eq!(t.run(cmds).await, *want, "{cmds:?}");
    }
}

#[tokio::test]
async fn nothing_is_recorded_while_off_and_classes_filter() {
    let t = Bed::new("");
    assert!(t.run(&["SET a 1", "LPUSH l x"]).await.is_empty());
    // Classes without K or E send nothing, so nothing is recorded.
    t.backend.set_flags(T, parse_flags(b"A").unwrap());
    assert!(t.run(&["SET a 1"]).await.is_empty());
    // Lists only.
    t.backend.set_flags(T, parse_flags(b"Kl").unwrap());
    assert_eq!(
        t.run(&["SET a 2", "LPUSH l y", "DEL a"]).await,
        vec!["l:lpush"]
    );
    t.backend.set_flags(T, parse_flags(b"Eg").unwrap());
    assert_eq!(t.run(&["SET a 2", "DEL a"]).await, vec!["a:del"]);
}

#[tokio::test]
async fn the_sweeper_sends_expired_and_a_write_over_an_expired_key_too() {
    let t = Bed::new("AKE");
    t.unit.set_clock(Some(1_000_000));
    assert_eq!(
        t.run(&["SET a 1 PX 10", "SET b 1 PX 10"]).await,
        vec!["a:set", "a:expire", "b:set", "b:expire"]
    );
    t.unit.set_clock(Some(1_000_100));
    // A write over an expired key: Redis deletes it first.
    assert_eq!(t.run(&["LPUSH a x"]).await, vec!["a:expired", "a:lpush"]);
    let n = notify::Notifier::default();
    n.flags.set(parse_flags(b"AKE").unwrap());
    let gone =
        sprig_redis_engine::sweep::pass_notifying(&*t.unit, Space::redis(T, 0), 100, &n.flags)
            .await
            .unwrap();
    assert_eq!(gone, 1);
    assert_eq!(t.events(), vec!["b:expired"]);
}
