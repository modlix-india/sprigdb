//! Compaction of compacted streams: rolled segments are rewritten keeping,
//! for each key, only its newest record. Offsets keep their gaps; a batch
//! keeps its base offset, last offset delta, timestamps and producer
//! fields, so the offsets it spans and idempotent producers' retries are
//! unchanged. A tombstone (null value) goes once its batch is older than
//! `delete_retention_ms`. The active segment is never rewritten.
//!
//! The rewrite is written beside the segment and synced, then renamed over
//! it under the partition lock: a reader holding the old file keeps
//! reading the old bytes. Each pass cleans the rolled segments not cleaned
//! yet (`cleaned_to`). A blob segment's rewrite is a plain segment; its old
//! file goes once the manifest no longer names it.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::PathBuf;

use sprig_proto_kafka::records::RecordBatch;
use sprig_proto_kafka::wire::Reader;

use super::batch::Head;
use super::segment::Segment;
use super::{Part, Policy};

fn decode(bytes: &[u8]) -> io::Result<RecordBatch> {
    RecordBatch::decode(&mut Reader::new(bytes), 256 << 20)
        .map_err(|e| io::Error::other(format!("stored batch: {e}")))
}

/// A segment's batches as (head, bytes), up to `len`.
fn batches(s: &Segment) -> io::Result<Vec<(Head, Vec<u8>)>> {
    s.heads()?
        .into_iter()
        .map(|(pos, h)| s.batch_at(pos, &h).map(|b| (h, b)))
        .collect()
}

/// One compaction pass over a partition; returns whether it rewrote
/// anything. Files to remove after the next manifest go in `gone`.
///
/// # Errors
/// The files can't be read or written.
pub fn compact(
    p: &Part,
    pol: &Policy,
    now_ms: i64,
    interval: u64,
    gone: &mut Vec<PathBuf>,
) -> io::Result<bool> {
    // What to clean: rolled segments past `cleaned_to`. Each key's newest
    // offset is taken over every segment (the active one up to what it
    // holds now; later records only make us keep more), read from copies
    // outside the lock.
    let (rolled, cleaned_to, segs) = {
        let mut st = p.lock();
        if st.segments.len() < 2 {
            return Ok(false);
        }
        // The pass reads the files: pending batches go in first.
        for s in &mut st.segments {
            s.write_out()?;
        }
        let rolled = st.segments.len() - 1;
        let end = st.segments[rolled - 1].next;
        if end <= st.cleaned_to {
            return Ok(false);
        }
        let segs: Vec<Segment> = st.segments.iter().map(Segment::read_copy).collect();
        (rolled, st.cleaned_to, segs)
    };
    let mut newest: HashMap<Vec<u8>, i64> = HashMap::new();
    for s in &segs {
        for (_, bytes) in batches(s)? {
            let b = decode(&bytes)?;
            for r in &b.records {
                if let Some(k) = &r.key {
                    let o = b.base_offset + i64::from(r.offset_delta);
                    let e = newest.entry(k.clone()).or_insert(o);
                    *e = (*e).max(o);
                }
            }
        }
    }
    let horizon = now_ms.saturating_sub(pol.delete_retention_ms);
    let mut rewritten = Vec::new();
    for s in segs.iter().take(rolled) {
        let mut out = Vec::new();
        let mut changed = false;
        for (h, bytes) in batches(s)? {
            let mut b = decode(&bytes)?;
            let before = b.records.len();
            b.records.retain(|r| {
                let o = b.base_offset + i64::from(r.offset_delta);
                match &r.key {
                    None => true,
                    Some(k) => {
                        newest.get(k).is_none_or(|n| *n == o)
                            && (r.value.is_some() || h.max_ts >= horizon)
                    }
                }
            });
            if b.records.len() == before {
                out.extend_from_slice(&bytes);
                continue;
            }
            changed = true;
            if !b.records.is_empty() {
                out.extend_from_slice(
                    &b.to_bytes()
                        .map_err(|e| io::Error::other(format!("re-encode: {e}")))?,
                );
            }
        }
        if changed {
            let tmp = s.path.with_extension("cleaned");
            let f = fs::File::create(&tmp)?;
            f.write_all_at(&out, 0)?;
            f.sync_all()?;
            let path = s.path.with_file_name(super::segment::file_name(s.base));
            rewritten.push((s.base, path, tmp));
        }
    }
    let end = segs[rolled - 1].next.max(cleaned_to);
    let mut st = p.lock();
    for (base, path, tmp) in &rewritten {
        let Some(i) = st.segments.iter().position(|s| s.base == *base) else {
            let _ = fs::remove_file(tmp);
            continue;
        };
        fs::rename(tmp, path)?;
        if st.segments[i].is_refs() {
            gone.push(st.segments[i].path.clone());
        }
        let created = st.segments[i].created_ms;
        let mut s = Segment::open(path, *base, None, created, interval)?;
        // A rewritten segment still spans the offsets it did.
        s.next = s.next.max(st.segments[i].next);
        s.max_ts = s.max_ts.max(st.segments[i].max_ts);
        st.segments[i] = s;
    }
    if let Some(parent) = st.dir.parent() {
        super::sync_dir(&st.dir);
        super::sync_dir(parent);
    }
    st.cleaned_to = end;
    st.dirty = true;
    Ok(!rewritten.is_empty())
}
