//! The RocksDB sorted store, with the single-node profile.
//!
//! - Column families `data` and `local` (plus RocksDB's unused `default`).
//!   `data` is written with the WAL off (the commit log is its WAL); `local`
//!   with the WAL on.
//! - `atomic_flush`, so a flush never splits one batch across column
//!   families and a flushed `data` file never holds data without the unit's
//!   applied index.
//! - One `HyperClock` block cache shared by both column families. Memtables
//!   are charged to it through a write buffer manager, so the cache's
//!   capacity covers both budgets.
//! - A prefix extractor that strips the 8-byte version suffix, prefix Bloom
//!   filters (whole-key filtering off: every versioned read is a prefix
//!   seek), partitioned index and filters with the top level pinned, and a
//!   memtable prefix Bloom filter.
//! - Leveled compaction with dynamic level sizes; no compression on L0–L1,
//!   LZ4 on L2–L4, ZSTD below.

use std::cell::RefCell;
use std::ffi::CStr;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use rust_rocksdb::compaction_filter::CompactionFilter;
use rust_rocksdb::compaction_filter_factory::{CompactionFilterContext, CompactionFilterFactory};
use rust_rocksdb::{
    BlockBasedIndexType, BlockBasedOptions, BottommostLevelCompaction, Cache, ColumnFamily,
    ColumnFamilyDescriptor, CompactOptions, CompactionDecision, DB, DBCompactionStyle,
    DBCompressionType, DBRawIteratorWithThreadMode, ErrorKind, FlushOptions, Options, ReadOptions,
    ReadTier as RocksTier, SliceTransform, WriteBatch, WriteBufferManager, WriteOptions,
};
use sprig_common::{Result, SprigError};

use super::{
    BatchOp, Cf, GcDecider, GcRule, IterOpts, Read, ReadTier, SortedStore, StoreBatch, StoreIter,
    StoreStats, WalMode,
};
use crate::keys::{KeyRange, VERSION_LEN, strip_version};

/// What [`RocksStore::open`] needs.
#[derive(Clone)]
pub struct RocksOptions {
    /// Block cache bytes (data, index and filter blocks).
    pub block_cache_bytes: usize,
    /// Memtable bytes, charged to the block cache.
    pub memtable_bytes: usize,
    /// The rest of the tuning.
    pub tuning: crate::config::RocksTuning,
    /// The version GC rule compactions of `data` run, if any.
    pub gc: Option<GcRule>,
}

impl std::fmt::Debug for RocksOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RocksOptions")
            .field("block_cache_bytes", &self.block_cache_bytes)
            .field("memtable_bytes", &self.memtable_bytes)
            .field("tuning", &self.tuning)
            .field("gc", &self.gc.is_some())
            .finish()
    }
}

impl Default for RocksOptions {
    fn default() -> Self {
        Self {
            block_cache_bytes: 256 << 20,
            memtable_bytes: 128 << 20,
            tuning: crate::config::RocksTuning::default(),
            gc: None,
        }
    }
}

/// Runs a [`CompactionGc`] rule as RocksDB's compaction filter.
struct GcFactory(GcRule);

struct GcFilter(GcDecider);

impl CompactionFilter for GcFilter {
    fn filter(&mut self, _level: u32, key: &[u8], value: &[u8]) -> CompactionDecision {
        if (self.0)(key, value) {
            CompactionDecision::Remove
        } else {
            CompactionDecision::Keep
        }
    }

    fn name(&self) -> &CStr {
        c"sprig.version_gc"
    }
}

impl CompactionFilterFactory for GcFactory {
    type Filter = GcFilter;

    fn create(&mut self, context: CompactionFilterContext) -> GcFilter {
        GcFilter(self.0.start(context.is_full_compaction))
    }

    fn name(&self) -> &CStr {
        c"sprig.version_gc"
    }
}

/// RocksDB behind [`SortedStore`].
pub struct RocksStore {
    db: DB,
    cache: Cache,
    wbm: WriteBufferManager,
}

impl std::fmt::Debug for RocksStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RocksStore")
            .field("path", &self.db.path())
            .finish_non_exhaustive()
    }
}

fn rocks_err(e: &rust_rocksdb::Error, what: &str) -> SprigError {
    SprigError::internal(format!("storage: {what} failed: {e}"))
}

fn in_domain(k: &[u8]) -> bool {
    k.len() >= VERSION_LEN
}

fn cf_options(
    tuning: &crate::config::RocksTuning,
    memtable_bytes: usize,
    cache: &Cache,
    wbm: &WriteBufferManager,
    gc: Option<&GcRule>,
) -> Options {
    let mut t = BlockBasedOptions::default();
    t.set_block_cache(cache);
    t.set_block_size(tuning.block_size);
    t.set_format_version(6);
    t.set_bloom_filter(tuning.bloom_bits_per_key, false);
    t.set_whole_key_filtering(false);
    t.set_index_type(BlockBasedIndexType::TwoLevelIndexSearch);
    t.set_partition_filters(true);
    t.set_metadata_block_size(4096);
    t.set_cache_index_and_filter_blocks(true);
    t.set_cache_index_and_filter_blocks_with_high_priority(true);
    t.set_pin_top_level_index_and_filter(true);
    t.set_pin_l0_filter_and_index_blocks_in_cache(true);

    let mut o = Options::default();
    o.set_block_based_table_factory(&t);
    o.set_prefix_extractor(SliceTransform::create(
        "sprig.strip_version",
        strip_version,
        Some(in_domain),
    ));
    o.set_memtable_prefix_bloom_ratio(0.1);
    o.set_write_buffer_manager(wbm);
    // At least four memtables fit in the budget.
    o.set_write_buffer_size(
        tuning
            .write_buffer_size
            .min(memtable_bytes / 4)
            .max(1 << 20),
    );
    o.set_max_write_buffer_number(4);
    o.set_target_file_size_base(tuning.target_file_size);
    o.set_compaction_style(DBCompactionStyle::Level);
    o.set_level_compaction_dynamic_level_bytes(true);
    o.set_level_zero_file_num_compaction_trigger(4);
    o.set_level_zero_slowdown_writes_trigger(20);
    o.set_level_zero_stop_writes_trigger(36);
    o.set_compression_per_level(&[
        DBCompressionType::None,
        DBCompressionType::None,
        DBCompressionType::Lz4,
        DBCompressionType::Lz4,
        DBCompressionType::Lz4,
        DBCompressionType::Zstd,
        DBCompressionType::Zstd,
    ]);
    o.set_bottommost_compression_type(DBCompressionType::Zstd);
    if let Some(gc) = gc {
        o.set_compaction_filter_factory(GcFactory(Arc::clone(gc)));
    }
    o
}

thread_local! {
    /// Read options for point reads, one per tier, reused across calls.
    static GET_OPTS: RefCell<Option<[ReadOptions; 2]>> = const { RefCell::new(None) };
}

fn with_get_opts<R>(tier: ReadTier, f: impl FnOnce(&ReadOptions) -> R) -> R {
    GET_OPTS.with(|cell| {
        let mut slot = cell.borrow_mut();
        let opts = slot.get_or_insert_with(|| {
            let all = ReadOptions::default();
            let mut cached = ReadOptions::default();
            cached.set_read_tier(RocksTier::BlockCache);
            [all, cached]
        });
        f(&opts[usize::from(tier == ReadTier::CacheOnly)])
    })
}

impl RocksStore {
    /// Opens (or creates) the store under `path`.
    ///
    /// # Errors
    /// RocksDB failed to open.
    pub fn open(path: &Path, opts: &RocksOptions) -> Result<Self> {
        std::fs::create_dir_all(path).map_err(|e| {
            SprigError::internal(format!("storage: create {}", path.display())).with_source(e)
        })?;
        // The cache's capacity covers the memtables charged to it.
        let cache = Cache::new_hyper_clock_cache(opts.block_cache_bytes + opts.memtable_bytes, 0);
        // No write stall: past its budget the manager flushes the largest
        // memtable instead. (A stall waits for a flush, and empty memtables'
        // arena reservations alone can exceed a small budget, so a stall
        // could wait forever.)
        let wbm = WriteBufferManager::new_write_buffer_manager_with_cache(
            opts.memtable_bytes,
            false,
            cache.clone(),
        );

        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.set_atomic_flush(true);
        db_opts.set_max_background_jobs(opts.tuning.background_jobs);
        db_opts.set_max_subcompactions(2);
        db_opts.set_bytes_per_sync(1 << 20);
        db_opts.set_write_buffer_manager(&wbm);
        db_opts.set_avoid_flush_during_shutdown(opts.tuning.avoid_flush_on_close);
        if opts.tuning.statistics {
            db_opts.enable_statistics();
        }

        let cfs = Cf::ALL
            .iter()
            .map(|cf| {
                let gc = if *cf == Cf::Data {
                    opts.gc.as_ref()
                } else {
                    None
                };
                ColumnFamilyDescriptor::new(
                    cf.name(),
                    cf_options(&opts.tuning, opts.memtable_bytes, &cache, &wbm, gc),
                )
            })
            .collect::<Vec<_>>();
        let db = DB::open_cf_descriptors(&db_opts, path, cfs).map_err(|e| rocks_err(&e, "open"))?;
        Ok(Self { db, cache, wbm })
    }

    fn cf(&self, cf: Cf) -> &ColumnFamily {
        // Every Cf is created at open, so the handle exists.
        #[allow(clippy::expect_used)]
        self.db
            .cf_handle(cf.name())
            .expect("column family opened at start")
    }

    fn prop(&self, cf: Cf, name: &str) -> u64 {
        self.db
            .property_int_value_cf(self.cf(cf), name)
            .ok()
            .flatten()
            .unwrap_or(0)
    }

    /// RocksDB's statistics dump, when statistics are on.
    #[must_use]
    pub fn statistics(&self) -> Option<String> {
        self.db.property_value("rocksdb.stats").ok().flatten()
    }
}

fn read_result<T>(r: std::result::Result<T, rust_rocksdb::Error>, what: &str) -> Result<Read<T>> {
    match r {
        Ok(v) => Ok(Read::Ready(v)),
        Err(e) if e.kind() == ErrorKind::Incomplete => Ok(Read::WouldBlock),
        Err(e) => Err(rocks_err(&e, what)),
    }
}

impl SortedStore for RocksStore {
    fn get(&self, cf: Cf, key: &[u8], tier: ReadTier) -> Result<Read<Option<Bytes>>> {
        let h = self.cf(cf);
        with_get_opts(tier, |o| {
            let r = self
                .db
                .get_pinned_cf_opt(h, key, o)
                .map(|v| v.map(|s| Bytes::copy_from_slice(&s)));
            read_result(r, "get")
        })
    }

    fn get_into(
        &self,
        cf: Cf,
        key: &[u8],
        tier: ReadTier,
        out: &mut Vec<u8>,
    ) -> Result<Read<bool>> {
        out.clear();
        let h = self.cf(cf);
        with_get_opts(tier, |o| {
            let r = self.db.get_pinned_cf_opt(h, key, o).map(|v| match v {
                Some(s) => {
                    out.extend_from_slice(&s);
                    true
                }
                None => false,
            });
            read_result(r, "get")
        })
    }

    fn multi_get(
        &self,
        cf: Cf,
        keys: &[&[u8]],
        tier: ReadTier,
    ) -> Result<Read<Vec<Option<Bytes>>>> {
        let h = self.cf(cf);
        with_get_opts(tier, |o| {
            let res = self
                .db
                .batched_multi_get_cf_opt(h, keys.iter().copied(), false, o);
            let mut out = Vec::with_capacity(keys.len());
            for r in res {
                match read_result(r, "multi_get")? {
                    Read::Ready(v) => out.push(v.map(|s| Bytes::copy_from_slice(&s))),
                    Read::WouldBlock => return Ok(Read::WouldBlock),
                }
            }
            Ok(Read::Ready(out))
        })
    }

    fn iter(&self, cf: Cf, opts: IterOpts) -> Box<dyn StoreIter + '_> {
        let mut ro = ReadOptions::default();
        if let Some(l) = opts.lower {
            ro.set_iterate_lower_bound(l);
        }
        if let Some(u) = opts.upper {
            ro.set_iterate_upper_bound(u);
        }
        if opts.prefix_same_as_start {
            ro.set_prefix_same_as_start(true);
        } else {
            ro.set_total_order_seek(true);
        }
        if opts.tier == ReadTier::CacheOnly {
            ro.set_read_tier(RocksTier::BlockCache);
        }
        Box::new(RocksIter {
            it: self.db.raw_iterator_cf_opt(self.cf(cf), ro),
        })
    }

    fn write(&self, batch: &StoreBatch, wal: WalMode) -> Result<()> {
        let mut wb = WriteBatch::with_capacity_bytes(batch.bytes() + 16 * batch.ops().len());
        for op in batch.ops() {
            match op {
                BatchOp::Put { cf, key, value } => wb.put_cf(self.cf(*cf), key, value),
                BatchOp::Delete { cf, key } => wb.delete_cf(self.cf(*cf), key),
                BatchOp::DeleteRange { cf, start, end } => {
                    if start < end {
                        wb.delete_range_cf(self.cf(*cf), start, end);
                    }
                }
            }
        }
        let mut wo = WriteOptions::default();
        match wal {
            WalMode::Off => wo.disable_wal(true),
            WalMode::On => {}
            WalMode::Sync => wo.set_sync(true),
        }
        self.db
            .write_opt(&wb, &wo)
            .map_err(|e| rocks_err(&e, "write"))
    }

    fn flush_all(&self) -> Result<()> {
        let mut fo = FlushOptions::default();
        fo.set_wait(true);
        let handles: Vec<&ColumnFamily> = Cf::ALL.iter().map(|c| self.cf(*c)).collect();
        self.db
            .flush_cfs_opt(&handles, &fo)
            .map_err(|e| rocks_err(&e, "flush"))
    }

    fn compact_all(&self) -> Result<()> {
        let mut co = CompactOptions::default();
        co.set_bottommost_level_compaction(BottommostLevelCompaction::Force);
        co.set_exclusive_manual_compaction(false);
        for cf in Cf::ALL {
            self.db
                .compact_range_cf_opt(self.cf(cf), None::<&[u8]>, None::<&[u8]>, &co);
        }
        Ok(())
    }

    fn approx_size(&self, cf: Cf, range: &KeyRange) -> u64 {
        let end = range.end.clone().unwrap_or_else(|| vec![0xFF; 32]);
        self.db
            .get_approximate_sizes_cf(self.cf(cf), &[rust_rocksdb::Range::new(&range.start, &end)])
            .ok()
            .and_then(|v| v.first().copied())
            .unwrap_or(0)
    }

    fn stats(&self) -> StoreStats {
        let mut s = StoreStats {
            block_cache_bytes: self.cache.get_usage() as u64,
            ..StoreStats::default()
        };
        for cf in Cf::ALL {
            s.memtable_bytes += self.prop(cf, "rocksdb.cur-size-all-mem-tables");
            s.table_readers_bytes += self.prop(cf, "rocksdb.estimate-table-readers-mem");
            s.sst_bytes += self.prop(cf, "rocksdb.live-sst-files-size");
            s.keys += self.prop(cf, "rocksdb.estimate-num-keys");
        }
        let _ = self.wbm.get_usage();
        s
    }
}

struct RocksIter<'a> {
    it: DBRawIteratorWithThreadMode<'a, DB>,
}

impl StoreIter for RocksIter<'_> {
    fn seek(&mut self, key: &[u8]) {
        self.it.seek(key);
    }
    fn seek_for_prev(&mut self, key: &[u8]) {
        self.it.seek_for_prev(key);
    }
    fn seek_to_first(&mut self) {
        self.it.seek_to_first();
    }
    fn seek_to_last(&mut self) {
        self.it.seek_to_last();
    }
    fn valid(&self) -> bool {
        self.it.valid()
    }
    fn key(&self) -> &[u8] {
        self.it.key().unwrap_or(&[])
    }
    fn value(&self) -> &[u8] {
        self.it.value().unwrap_or(&[])
    }
    fn next(&mut self) {
        self.it.next();
    }
    fn prev(&mut self) {
        self.it.prev();
    }
    fn status(&self) -> Result<Read<()>> {
        read_result(self.it.status(), "iterate")
    }
}
