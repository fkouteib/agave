use {
    crate::{accounts_index::AccountsIndexRootsStats, append_vec::APPEND_VEC_STATS},
    solana_time_utils::AtomicInterval,
    std::{
        iter::Sum,
        num::Saturating,
        sync::atomic::{AtomicU64, AtomicUsize, Ordering},
    },
};

#[derive(Debug, Default)]
pub struct AccountsStats {
    pub delta_hash_scan_time_total_us: AtomicU64,
    pub delta_hash_accumulate_time_total_us: AtomicU64,
    pub delta_hash_num: AtomicU64,

    pub last_store_report: AtomicInterval,
    pub store_hash_accounts: AtomicU64,
    pub store_accounts: AtomicU64,
    pub store_update_index: AtomicU64,
    pub store_handle_reclaims: AtomicU64,
    pub store_append_accounts: AtomicU64,
    pub stakes_cache_check_and_store_us: AtomicU64,
    pub store_num_accounts: AtomicU64,
    pub store_total_data: AtomicU64,
    pub create_store_count: AtomicU64,
    pub store_get_slot_store: AtomicU64,
    pub store_find_existing: AtomicU64,
    pub dropped_stores: AtomicU64,
    pub handle_dead_keys_us: AtomicU64,
    pub purge_exact_us: AtomicU64,
    pub purge_exact_count: AtomicU64,
}

#[derive(Debug, Default)]
pub struct PurgeStats {
    pub last_report: AtomicInterval,
    pub safety_checks_elapsed: AtomicU64,
    pub remove_cache_elapsed: AtomicU64,
    pub remove_storage_entries_elapsed: AtomicU64,
    pub drop_storage_entries_elapsed: AtomicU64,
    pub num_cached_slots_removed: AtomicUsize,
    pub num_stored_slots_removed: AtomicUsize,
    pub total_removed_storage_entries: AtomicUsize,
    pub total_removed_cached_bytes: AtomicU64,
    pub total_removed_stored_bytes: AtomicU64,
    pub scan_storages_elapsed: AtomicU64,
    pub purge_accounts_index_elapsed: AtomicU64,
    pub handle_reclaims_elapsed: AtomicU64,
}

impl PurgeStats {
    pub fn report(&self, metric_name: &'static str, report_interval_ms: Option<u64>) {
        let should_report = report_interval_ms
            .map(|report_interval_ms| self.last_report.should_update(report_interval_ms))
            .unwrap_or(true);

        if should_report {
            datapoint_info!(
                metric_name,
                (
                    "safety_checks_elapsed",
                    self.safety_checks_elapsed.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "remove_cache_elapsed",
                    self.remove_cache_elapsed.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "remove_storage_entries_elapsed",
                    self.remove_storage_entries_elapsed
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "drop_storage_entries_elapsed",
                    self.drop_storage_entries_elapsed.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "num_cached_slots_removed",
                    self.num_cached_slots_removed.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "num_stored_slots_removed",
                    self.num_stored_slots_removed.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "total_removed_storage_entries",
                    self.total_removed_storage_entries
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "total_removed_cached_bytes",
                    self.total_removed_cached_bytes.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "total_removed_stored_bytes",
                    self.total_removed_stored_bytes.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "scan_storages_elapsed",
                    self.scan_storages_elapsed.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "purge_accounts_index_elapsed",
                    self.purge_accounts_index_elapsed.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "handle_reclaims_elapsed",
                    self.handle_reclaims_elapsed.swap(0, Ordering::Relaxed),
                    i64
                ),
            );
        }
    }
}

#[derive(Default, Debug)]
pub struct StoreAccountsTiming {
    pub store_accounts_elapsed: u64,
    pub update_index_elapsed: u64,
    pub handle_reclaims_elapsed: u64,
}

impl StoreAccountsTiming {
    pub fn accumulate(&mut self, other: &Self) {
        self.store_accounts_elapsed += other.store_accounts_elapsed;
        self.update_index_elapsed += other.update_index_elapsed;
        self.handle_reclaims_elapsed += other.handle_reclaims_elapsed;
    }
}

#[derive(Debug, Default)]
pub struct FlushStats {
    pub num_accounts_flushed: Saturating<usize>,
    pub num_bytes_flushed: Saturating<u64>,
    pub num_accounts_purged: Saturating<usize>,
    pub num_bytes_purged: Saturating<u64>,
    pub store_accounts_timing: StoreAccountsTiming,
    pub store_accounts_total_us: Saturating<u64>,
}

impl FlushStats {
    pub fn accumulate(&mut self, other: &Self) {
        self.num_accounts_flushed += other.num_accounts_flushed;
        self.num_bytes_flushed += other.num_bytes_flushed;
        self.num_accounts_purged += other.num_accounts_purged;
        self.num_bytes_purged += other.num_bytes_purged;
        self.store_accounts_timing
            .accumulate(&other.store_accounts_timing);
        self.store_accounts_total_us += other.store_accounts_total_us;
    }
}

#[derive(Debug, Default)]
pub struct LatestAccountsIndexRootsStats {
    pub roots_len: AtomicUsize,
    pub uncleaned_roots_len: AtomicUsize,
    pub roots_range: AtomicU64,
    pub rooted_cleaned_count: AtomicUsize,
    pub unrooted_cleaned_count: AtomicUsize,
    pub clean_unref_from_storage_us: AtomicU64,
    pub clean_dead_slot_us: AtomicU64,
}

impl LatestAccountsIndexRootsStats {
    pub fn update(&self, accounts_index_roots_stats: &AccountsIndexRootsStats) {
        if let Some(value) = accounts_index_roots_stats.roots_len {
            self.roots_len.store(value, Ordering::Relaxed);
        }
        if let Some(value) = accounts_index_roots_stats.uncleaned_roots_len {
            self.uncleaned_roots_len.store(value, Ordering::Relaxed);
        }
        if let Some(value) = accounts_index_roots_stats.roots_range {
            self.roots_range.store(value, Ordering::Relaxed);
        }
        self.rooted_cleaned_count.fetch_add(
            accounts_index_roots_stats.rooted_cleaned_count,
            Ordering::Relaxed,
        );
        self.unrooted_cleaned_count.fetch_add(
            accounts_index_roots_stats.unrooted_cleaned_count,
            Ordering::Relaxed,
        );
        self.clean_unref_from_storage_us.fetch_add(
            accounts_index_roots_stats.clean_unref_from_storage_us,
            Ordering::Relaxed,
        );
        self.clean_dead_slot_us.fetch_add(
            accounts_index_roots_stats.clean_dead_slot_us,
            Ordering::Relaxed,
        );
    }

    pub fn report(&self) {
        datapoint_info!(
            "accounts_index_roots_len",
            ("roots_len", self.roots_len.load(Ordering::Relaxed), i64),
            (
                "uncleaned_roots_len",
                self.uncleaned_roots_len.load(Ordering::Relaxed),
                i64
            ),
            (
                "roots_range_width",
                self.roots_range.load(Ordering::Relaxed),
                i64
            ),
            (
                "unrooted_cleaned_count",
                self.unrooted_cleaned_count.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "rooted_cleaned_count",
                self.rooted_cleaned_count.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "clean_unref_from_storage_us",
                self.clean_unref_from_storage_us.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "clean_dead_slot_us",
                self.clean_dead_slot_us.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "append_vecs_open",
                APPEND_VEC_STATS.files_open.load(Ordering::Relaxed),
                i64
            ),
            (
                "append_vecs_dirty",
                APPEND_VEC_STATS.files_dirty.load(Ordering::Relaxed),
                i64
            ),
            (
                "append_vecs_open_as_mmap",
                APPEND_VEC_STATS.open_as_mmap.load(Ordering::Relaxed),
                i64
            ),
            (
                "append_vecs_open_as_file_io",
                APPEND_VEC_STATS.open_as_file_io.load(Ordering::Relaxed),
                i64
            )
        );

        // Don't need to reset since this tracks the latest updates, not a cumulative total
    }
}

#[derive(Debug, Default)]
pub struct CleanAccountsStats {
    pub purge_stats: PurgeStats,
    pub latest_accounts_index_roots_stats: LatestAccountsIndexRootsStats,

    // stats held here and reported by clean_accounts
    pub clean_old_root_us: AtomicU64,
    pub clean_old_root_reclaim_us: AtomicU64,
    pub remove_dead_accounts_remove_us: AtomicU64,
    pub remove_dead_accounts_shrink_us: AtomicU64,
    pub clean_stored_dead_slots_us: AtomicU64,
    pub get_account_sizes_us: AtomicU64,
    pub slots_cleaned: AtomicU64,
}

impl CleanAccountsStats {
    pub fn report(&self) {
        self.purge_stats.report("clean_purge_slots_stats", None);
        self.latest_accounts_index_roots_stats.report();
    }
}

#[derive(Debug, Default)]
pub struct ShrinkAncientStats {
    pub shrink_stats: ShrinkStats,
    pub ancient_append_vecs_shrunk: AtomicU64,
    pub total_us: AtomicU64,
    pub random_shrink: AtomicU64,
    pub slots_considered: AtomicU64,
    pub ancient_scanned: AtomicU64,
    pub bytes_ancient_created: AtomicU64,
    pub bytes_from_must_shrink: AtomicU64,
    pub bytes_from_smallest_storages: AtomicU64,
    pub bytes_from_newest_storages: AtomicU64,
    pub many_ref_slots_skipped: AtomicU64,
    pub slots_cannot_move_count: AtomicU64,
    pub many_refs_old_alive: AtomicU64,
    pub slots_eligible_to_shrink: AtomicU64,
    pub total_dead_bytes: AtomicU64,
    pub total_alive_bytes: AtomicU64,
    pub slot: AtomicU64,
    pub ideal_storage_size: AtomicU64,
}

#[derive(Debug, Default)]
pub struct ShrinkStatsSub {
    pub store_accounts_timing: StoreAccountsTiming,
    pub rewrite_elapsed_us: Saturating<u64>,
    pub create_and_insert_store_elapsed_us: Saturating<u64>,
    pub unpackable_slots_count: Saturating<usize>,
    pub newest_alive_packed_count: Saturating<usize>,
}

impl ShrinkStatsSub {
    pub fn accumulate(&mut self, other: &Self) {
        self.store_accounts_timing
            .accumulate(&other.store_accounts_timing);
        self.rewrite_elapsed_us += other.rewrite_elapsed_us;
        self.create_and_insert_store_elapsed_us += other.create_and_insert_store_elapsed_us;
        self.unpackable_slots_count += other.unpackable_slots_count;
        self.newest_alive_packed_count += other.newest_alive_packed_count;
    }
}
#[derive(Debug, Default)]
pub struct ShrinkStats {
    pub last_report: AtomicInterval,
    pub num_slots_shrunk: AtomicUsize,
    pub storage_read_elapsed: AtomicU64,
    pub num_duplicated_accounts: AtomicU64,
    pub index_read_elapsed: AtomicU64,
    pub create_and_insert_store_elapsed: AtomicU64,
    pub store_accounts_elapsed: AtomicU64,
    pub update_index_elapsed: AtomicU64,
    pub handle_reclaims_elapsed: AtomicU64,
    pub remove_old_stores_shrink_us: AtomicU64,
    pub rewrite_elapsed: AtomicU64,
    pub unpackable_slots_count: AtomicU64,
    pub newest_alive_packed_count: AtomicU64,
    pub drop_storage_entries_elapsed: AtomicU64,
    pub accounts_removed: AtomicUsize,
    pub bytes_removed: AtomicU64,
    pub bytes_written: AtomicU64,
    pub skipped_shrink: AtomicU64,
    pub dead_accounts: AtomicU64,
    pub alive_accounts: AtomicU64,
    pub index_scan_returned_none: AtomicU64,
    pub index_scan_returned_some: AtomicU64,
    pub obsolete_accounts_filtered: AtomicU64,
    pub accounts_loaded: AtomicU64,
    pub initial_candidates_count: AtomicU64,
    pub purged_zero_lamports: AtomicU64,
    pub accounts_not_found_in_index: AtomicU64,
    pub num_ancient_slots_shrunk: AtomicU64,
    pub ancient_slots_added_to_shrink: AtomicU64,
    pub ancient_bytes_added_to_shrink: AtomicU64,
    pub num_dead_slots_added_to_clean: AtomicU64,
    pub num_slots_with_zero_lamport_accounts_added_to_shrink: AtomicU64,
    pub marking_zero_dead_accounts_in_non_shrinkable_store: AtomicU64,
    pub num_zero_lamport_single_ref_accounts_found: AtomicU64,
}

impl ShrinkStats {
    pub fn report(&self) {
        if self.last_report.should_update(1000) {
            datapoint_info!(
                "shrink_stats",
                (
                    "ancient_slots_added_to_shrink",
                    self.ancient_slots_added_to_shrink
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "ancient_bytes_added_to_shrink",
                    self.ancient_bytes_added_to_shrink
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "num_slots_shrunk",
                    self.num_slots_shrunk.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "obsolete_accounts_filtered",
                    self.obsolete_accounts_filtered.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "index_scan_returned_none",
                    self.index_scan_returned_none.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "index_scan_returned_some",
                    self.index_scan_returned_some.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "storage_read_elapsed",
                    self.storage_read_elapsed.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "num_duplicated_accounts",
                    self.num_duplicated_accounts.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "index_read_elapsed",
                    self.index_read_elapsed.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "create_and_insert_store_elapsed",
                    self.create_and_insert_store_elapsed
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "store_accounts_elapsed",
                    self.store_accounts_elapsed.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "update_index_elapsed",
                    self.update_index_elapsed.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "handle_reclaims_elapsed",
                    self.handle_reclaims_elapsed.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "remove_old_stores_shrink_us",
                    self.remove_old_stores_shrink_us.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "rewrite_elapsed",
                    self.rewrite_elapsed.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "drop_storage_entries_elapsed",
                    self.drop_storage_entries_elapsed.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "accounts_removed",
                    self.accounts_removed.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "bytes_removed",
                    self.bytes_removed.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "bytes_written",
                    self.bytes_written.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "skipped_shrink",
                    self.skipped_shrink.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "alive_accounts",
                    self.alive_accounts.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "dead_accounts",
                    self.dead_accounts.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "accounts_loaded",
                    self.accounts_loaded.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "purged_zero_lamports_count",
                    self.purged_zero_lamports.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "num_ancient_slots_shrunk",
                    self.num_ancient_slots_shrunk.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "accounts_not_found_in_index",
                    self.accounts_not_found_in_index.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "initial_candidates_count",
                    self.initial_candidates_count.swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "num_dead_slots_added_to_clean",
                    self.num_dead_slots_added_to_clean
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "num_slots_with_zero_lamport_accounts_added_to_shrink",
                    self.num_slots_with_zero_lamport_accounts_added_to_shrink
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "marking_zero_dead_accounts_in_non_shrinkable_store",
                    self.marking_zero_dead_accounts_in_non_shrinkable_store
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
                (
                    "num_zero_lamport_single_ref_accounts_found",
                    self.num_zero_lamport_single_ref_accounts_found
                        .swap(0, Ordering::Relaxed),
                    i64
                ),
            );
        }
    }
}

impl ShrinkAncientStats {
    pub fn report(&self) {
        datapoint_info!(
            "shrink_ancient_stats",
            (
                "num_slots_shrunk",
                self.shrink_stats
                    .num_slots_shrunk
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "index_scan_returned_none",
                self.shrink_stats
                    .index_scan_returned_none
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "index_scan_returned_some",
                self.shrink_stats
                    .index_scan_returned_some
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "storage_read_elapsed",
                self.shrink_stats
                    .storage_read_elapsed
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "num_duplicated_accounts",
                self.shrink_stats
                    .num_duplicated_accounts
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "index_read_elapsed",
                self.shrink_stats
                    .index_read_elapsed
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "create_and_insert_store_elapsed",
                self.shrink_stats
                    .create_and_insert_store_elapsed
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "store_accounts_elapsed",
                self.shrink_stats
                    .store_accounts_elapsed
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "update_index_elapsed",
                self.shrink_stats
                    .update_index_elapsed
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "handle_reclaims_elapsed",
                self.shrink_stats
                    .handle_reclaims_elapsed
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "remove_old_stores_shrink_us",
                self.shrink_stats
                    .remove_old_stores_shrink_us
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "rewrite_elapsed",
                self.shrink_stats.rewrite_elapsed.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "unpackable_slots_count",
                self.shrink_stats
                    .unpackable_slots_count
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "newest_alive_packed_count",
                self.shrink_stats
                    .newest_alive_packed_count
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "drop_storage_entries_elapsed",
                self.shrink_stats
                    .drop_storage_entries_elapsed
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "accounts_removed",
                self.shrink_stats
                    .accounts_removed
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "bytes_removed",
                self.shrink_stats.bytes_removed.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "bytes_written",
                self.shrink_stats.bytes_written.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "alive_accounts",
                self.shrink_stats.alive_accounts.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "dead_accounts",
                self.shrink_stats.dead_accounts.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "accounts_loaded",
                self.shrink_stats.accounts_loaded.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "ancient_append_vecs_shrunk",
                self.ancient_append_vecs_shrunk.swap(0, Ordering::Relaxed),
                i64
            ),
            ("random", self.random_shrink.swap(0, Ordering::Relaxed), i64),
            (
                "slots_eligible_to_shrink",
                self.slots_eligible_to_shrink.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "total_dead_bytes",
                self.total_dead_bytes.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "total_alive_bytes",
                self.total_alive_bytes.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "slots_considered",
                self.slots_considered.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "ancient_scanned",
                self.ancient_scanned.swap(0, Ordering::Relaxed),
                i64
            ),
            ("total_us", self.total_us.swap(0, Ordering::Relaxed), i64),
            (
                "bytes_ancient_created",
                self.bytes_ancient_created.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "bytes_from_must_shrink",
                self.bytes_from_must_shrink.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "bytes_from_smallest_storages",
                self.bytes_from_smallest_storages.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "bytes_from_newest_storages",
                self.bytes_from_newest_storages.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "many_ref_slots_skipped",
                self.many_ref_slots_skipped.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "slots_cannot_move_count",
                self.slots_cannot_move_count.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "many_refs_old_alive",
                self.many_refs_old_alive.swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "purged_zero_lamports_count",
                self.shrink_stats
                    .purged_zero_lamports
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            (
                "accounts_not_found_in_index",
                self.shrink_stats
                    .accounts_not_found_in_index
                    .swap(0, Ordering::Relaxed),
                i64
            ),
            ("slot", self.slot.load(Ordering::Relaxed), i64),
            (
                "ideal_storage_size",
                self.ideal_storage_size.swap(0, Ordering::Relaxed),
                i64
            ),
        );
    }
}

#[derive(Debug, Default)]
pub struct ObsoleteAccountsStats {
    pub accounts_marked_obsolete: u64,
    pub slots_removed: u64,
}

impl Sum<Self> for ObsoleteAccountsStats {
    fn sum<I>(iter: I) -> Self
    where
        I: Iterator<Item = Self>,
    {
        iter.fold(Self::default(), |mut accumulated_stats, item| {
            accumulated_stats.accounts_marked_obsolete += item.accounts_marked_obsolete;
            accumulated_stats.slots_removed += item.slots_removed;
            accumulated_stats
        })
    }
}
