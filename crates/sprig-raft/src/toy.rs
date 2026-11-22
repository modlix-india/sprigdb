//! A toy key-value state machine for tests and the simulator: registers of
//! `u64` keys, each keeping every value ever written to it in order, so a
//! checker can see both the current value and the whole history.

use std::collections::BTreeMap;

use raft::prelude::Entry;

use crate::GroupId;
use crate::error::RaftError;
use crate::sm::StateMachine;

const PUT: u8 = b'P';
const GET: u8 = b'G';
const HISTORY: u8 = b'H';
const DUMP: u8 = b'D';

/// The proposal that writes `value` to register `key`.
#[must_use]
pub fn put(key: u64, value: u64) -> Vec<u8> {
    let mut b = Vec::with_capacity(17);
    b.push(PUT);
    b.extend_from_slice(&key.to_le_bytes());
    b.extend_from_slice(&value.to_le_bytes());
    b
}

/// The query for register `key`'s current value.
#[must_use]
pub fn get(key: u64) -> Vec<u8> {
    let mut b = vec![GET];
    b.extend_from_slice(&key.to_le_bytes());
    b
}

/// The query for every value register `key` ever held.
#[must_use]
pub fn history(key: u64) -> Vec<u8> {
    let mut b = vec![HISTORY];
    b.extend_from_slice(&key.to_le_bytes());
    b
}

/// The query for the whole state (as a snapshot encodes it).
#[must_use]
pub fn dump() -> Vec<u8> {
    vec![DUMP]
}

/// A `get` answer: the value, or `None` for a register never written.
#[must_use]
pub fn value(answer: &[u8]) -> Option<u64> {
    answer.try_into().ok().map(u64::from_le_bytes)
}

/// A `history` answer.
#[must_use]
pub fn values(answer: &[u8]) -> Vec<u64> {
    answer
        .chunks_exact(8)
        .map(|c| u64::from_le_bytes(c.try_into().unwrap_or([0; 8])))
        .collect()
}

fn u64_at(b: &[u8], at: usize) -> Option<u64> {
    b.get(at..at + 8)
        .and_then(|s| s.try_into().ok())
        .map(u64::from_le_bytes)
}

/// The toy state machine.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KvMachine {
    applied: u64,
    data: BTreeMap<u64, Vec<u64>>,
}

impl KvMachine {
    /// An empty machine.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A factory for nodes: an empty machine per group.
    pub fn factory() -> impl Fn(GroupId) -> Box<dyn StateMachine> + Send + Sync {
        |_| Box::new(KvMachine::new())
    }

    /// The registers.
    #[must_use]
    pub fn data(&self) -> &BTreeMap<u64, Vec<u64>> {
        &self.data
    }

    fn encode(&self) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&self.applied.to_le_bytes());
        b.extend_from_slice(&(self.data.len() as u64).to_le_bytes());
        for (k, vs) in &self.data {
            b.extend_from_slice(&k.to_le_bytes());
            b.extend_from_slice(&(vs.len() as u64).to_le_bytes());
            for v in vs {
                b.extend_from_slice(&v.to_le_bytes());
            }
        }
        b
    }

    /// Decodes a [`dump`] answer or snapshot.
    #[must_use]
    pub fn decode(b: &[u8]) -> Option<KvMachine> {
        let applied = u64_at(b, 0)?;
        let n = u64_at(b, 8)?;
        let mut at = 16;
        let mut data = BTreeMap::new();
        for _ in 0..n {
            let k = u64_at(b, at)?;
            let m = u64_at(b, at + 8)?;
            at += 16;
            let mut vs = Vec::new();
            for _ in 0..m {
                vs.push(u64_at(b, at)?);
                at += 8;
            }
            data.insert(k, vs);
        }
        (at == b.len()).then_some(KvMachine { applied, data })
    }
}

impl StateMachine for KvMachine {
    fn apply(&mut self, entries: &[Entry]) -> Result<(), RaftError> {
        for e in entries {
            let d = &e.data;
            if d.first() == Some(&PUT)
                && let (Some(k), Some(v)) = (u64_at(d, 1), u64_at(d, 9))
            {
                self.data.entry(k).or_default().push(v);
            }
            self.applied = e.index;
        }
        Ok(())
    }

    fn query(&self, q: &[u8]) -> Vec<u8> {
        match q.first() {
            Some(&GET) => u64_at(q, 1)
                .and_then(|k| self.data.get(&k))
                .and_then(|vs| vs.last())
                .map(|v| v.to_le_bytes().to_vec())
                .unwrap_or_default(),
            Some(&HISTORY) => u64_at(q, 1)
                .and_then(|k| self.data.get(&k))
                .map(|vs| vs.iter().flat_map(|v| v.to_le_bytes()).collect())
                .unwrap_or_default(),
            Some(&DUMP) => self.encode(),
            _ => Vec::new(),
        }
    }

    fn snapshot(&self) -> Result<Vec<u8>, RaftError> {
        Ok(self.encode())
    }

    fn restore(&mut self, index: u64, data: &[u8]) -> Result<(), RaftError> {
        if data.is_empty() {
            *self = KvMachine {
                applied: index,
                data: BTreeMap::new(),
            };
            return Ok(());
        }
        let mut m = KvMachine::decode(data)
            .ok_or_else(|| RaftError::Invalid("malformed toy snapshot".into()))?;
        m.applied = index;
        *self = m;
        Ok(())
    }
}
