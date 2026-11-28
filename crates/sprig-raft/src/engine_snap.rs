//! The engine's unit snapshots for the driver: a unit's keys at its
//! applied index, in the engine's stream form, restored by replacing what
//! the unit held.

use sprig_engine::{Engine, UnitId};

use crate::consensus::UnitSnapshots;
use crate::error::RaftError;

/// [`UnitSnapshots`] over an engine.
#[derive(Debug, Clone)]
pub struct EngineSnapshots {
    engine: Engine,
}

impl EngineSnapshots {
    /// Snapshots of `engine`'s units.
    #[must_use]
    pub fn new(engine: Engine) -> Self {
        Self { engine }
    }
}

fn storage(e: &sprig_common::SprigError) -> RaftError {
    RaftError::Storage(e.to_string())
}

impl UnitSnapshots for EngineSnapshots {
    fn export(&self, unit: UnitId) -> Result<Vec<u8>, RaftError> {
        let mut out = Vec::new();
        self.engine
            .export_unit_to(unit, &mut out)
            .map_err(|e| storage(&e))?;
        Ok(out)
    }

    fn ingest(&self, unit: UnitId, index: u64, data: &[u8]) -> Result<(), RaftError> {
        if data.is_empty() {
            let mut empty = Vec::new();
            // An empty unit: an export of nothing.
            empty.extend_from_slice(b"SPRIGUS1");
            empty.extend_from_slice(&u32::MAX.to_be_bytes());
            empty.extend_from_slice(&0u64.to_be_bytes());
            // The CRC-32 of no bytes is 0.
            empty.extend_from_slice(&0u32.to_be_bytes());
            // No parts.
            empty.extend_from_slice(&u32::MAX.to_be_bytes());
            self.engine
                .replace_unit_from(unit, &mut empty.as_slice())
                .map_err(|e| storage(&e))?;
            return Ok(());
        }
        let got = self
            .engine
            .replace_unit_from(unit, &mut &data[..])
            .map_err(|e| storage(&e))?;
        // The store's applied index counts entries with a payload: a
        // snapshot taken after a new leader's empty entry holds a lower one.
        if got > index {
            return Err(RaftError::Storage(format!(
                "a snapshot of unit {} holds index {got}, above {index}",
                unit.0
            )));
        }
        Ok(())
    }

    fn drop_unit(&self, unit: UnitId) -> Result<(), RaftError> {
        self.ingest(unit, 0, &[])?;
        self.engine.forget_unit_parts(unit);
        Ok(())
    }

    fn exports_off_driver(&self) -> bool {
        true
    }

    fn export_to(
        &self,
        unit: UnitId,
        fixed: &mut dyn FnMut(),
        out: &mut dyn std::io::Write,
    ) -> Result<(), RaftError> {
        self.engine
            .export_unit_fixed(unit, fixed, out)
            .map(|_| ())
            .map_err(|e| storage(&e))
    }

    fn ingest_from(
        &self,
        unit: UnitId,
        index: u64,
        input: &mut dyn std::io::Read,
    ) -> Result<(), RaftError> {
        let got = self
            .engine
            .replace_unit_durably(unit, input)
            .map_err(|e| storage(&e))?;
        if got > index {
            return Err(RaftError::Storage(format!(
                "a snapshot of unit {} holds index {got}, above {index}",
                unit.0
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_unit_round_trips_through_the_driver_trait() {
        let dir = std::env::temp_dir().join(format!("sprig-raft-snap-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let open = |p: &std::path::Path| {
            let mut c = sprig_engine::EngineConfig::new(p);
            c.store = sprig_engine::StoreKind::Mem;
            c.checkpoint_interval = std::time::Duration::ZERO;
            Engine::open(c).unwrap()
        };
        let a = open(&dir.join("a"));
        let mut k = sprig_engine::keys::KeyPrefix {
            tenant: 11,
            shard: 0,
            object: 1,
            kind: sprig_engine::keys::kind::PRIMARY,
            index: 0,
        }
        .to_vec();
        k.push(1);
        let mut t = a.begin(sprig_engine::txn::TxnOptions::default()).unwrap();
        t.put(k.clone(), bytes::Bytes::from_static(b"v")).unwrap();
        // Applied, not only durable: the export reads the store.
        t.commit()
            .unwrap()
            .log_ticket()
            .unwrap()
            .wait_applied()
            .unwrap();
        let sa = EngineSnapshots::new(a.clone());
        let data = sa.export(UnitId(11)).unwrap();
        let b = open(&dir.join("b"));
        let sb = EngineSnapshots::new(b.clone());
        sb.ingest(UnitId(11), 1, &data).unwrap();
        assert_eq!(
            b.unit_checksum(UnitId(11)).unwrap(),
            a.unit_checksum(UnitId(11)).unwrap()
        );
        assert!(sb.ingest(UnitId(11), 0, &data).is_err());
        // Restoring an empty unit clears it.
        sb.ingest(UnitId(11), 0, &[]).unwrap();
        assert_eq!(b.unit_checksum(UnitId(11)).unwrap().entries, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
