# Performance issue: super-linear (≈O(N²)) slowdown of `create`/`restore` with many tables

**Version:** v2.7.2 (commit d6528283)
**Reported:** 2026-06-15
**Severity:** high for installations with tens/hundreds of thousands of tables (e.g. Visiology: ~50K MergeTree + ~500K Join ≈ 550K tables).

## Summary

Both `clickhouse-backup create` and `restore` slow down **super-linearly** as the number of
tables grows. Per-table throughput collapses as the total table count increases, which points to
per-table work whose cost is a function of the *global* table/part count (≈O(N²) overall), rather
than constant per-table cost.

## Empirical evidence (bench stand: 16 vCPU, 125 GB RAM, ClickHouse 23.3.4.17, clickhouse-backup v2.7.2, local storage)

Synthetic Visiology-like schema: `<guid>_expanded_<t>` MergeTree + 10× `<guid>_model_join_inverted_<t>_<col>` Join per expanded.

### `create` throughput collapses with table count

| Scale | Tables | `create` time | Rate |
|---|---|---|---|
| stage-0 | 11 000 (1 000 MergeTree + 10 000 Join), 2.92 GiB | **44 s** | ~250 tables/s |
| stage-1 | 55 000 (5 000 MergeTree + 50 000 Join), 93.6 GiB | killed at 38 401/55 000 after **24.5 min** | **~26 tables/s** |

5× more tables → **~10× slower per table**. Data volume is not the cause: Join tables are
schema-only (16 KiB) and MergeTree parts are hardlinked (near-instant). The wall-clock is consumed
by per-table operations, and each one gets slower as the total table count rises.

### `restore` schema phase is sequential and not parallelizable

stage-0, 11 000 tables: restore = 77 s. Breakdown: ~73 s = 11 000 `CREATE TABLE` executed
essentially sequentially (~6.6 ms each); only ~4 s = `ATTACH` of the 1 000 MergeTree data tables.
Raising `general.concurrency` 9 → 16 did **not** help (77 s → 79 s) — schema restore does not
parallelize CREATE TABLE.

### Join engine data is lost (separate correctness issue)

`engine=Join` does not support `ALTER TABLE FREEZE`, so `create` logs
`supports only schema backup ... engine=Join` (pkg/backup/create.go:937) and backs up schema only.
On `restore` this is **silent** (RC=0, no warning): all Join tables come back empty. Verified:
restored Join total rows = 0 vs 1.9M original. For schemas where Join tables hold real data this is
silent data loss.

## Suspected root cause (to confirm against source)

Per-table calls in the create loop (`Backuper.AddTableToLocalBackup` → `pkg/clickhouse/clickhouse.go`)
that scan `system.*` whose size grows with the global table/part count, e.g.:

- `SELECT mutation_id, command FROM system.mutations WHERE is_done=0` (pkg/clickhouse/clickhouse.go:~1249)
  — observed issued per table, no `WHERE database/table` filter → scans all in-progress mutations every time.
- per-table `system.parts` / `system.detached_parts` / `system.columns` lookups.
- `FreezeTable`, `GetPartitions`, `CheckSystemPartsColumns` and similar helpers re-querying `system.*`
  once per table instead of once per backup.

These should be hoisted out of the per-table loop and/or cached for the duration of one backup, or
constrained with a `WHERE database=... AND table=...` filter.

## Fix direction

1. **create:** fetch global/in-progress mutation state, disk list, and any whole-`system.*` scans
   ONCE per backup (cache on the Backuper/clickhouse client), not once per table; or add table filters.
2. **restore:** parallelize the schema-creation phase (CREATE TABLE) with bounded concurrency
   (respecting dependency ordering) instead of sequential execution.
3. **Join (correctness):** at minimum make the schema-only/empty-data situation explicit on restore
   (warn, or `--error-on-empty-restore`-style guard), since Join data cannot be physically backed up.

## Concrete hotspots (localized via `system.query_log` during a 55 000-table `create`)

Aggregated by normalized query over the create window (top by total time):

| Calls | Total time | Avg | Query |
|---|---|---|---|
| **38 404** | **9 226 s** | 240 ms | `SELECT mutation_id, command FROM system.mutations WHERE is_done=? AND database=? AND table=?` |
| 5 000 | 823 s | 165 ms | `SELECT name, hash_of_all_files FROM system.parts WHERE database=? AND table=?` (`fetchHashOfAllFiles`) |
| 5 000 | 614 s | 123 ms | `ALTER TABLE … FREEZE WITH NAME …` |
| 5 000 | 524 s | 105 ms | `ALTER TABLE … UNFREEZE WITH NAME …` |

The #1 cost by ~10× is **`GetInProgressMutations` called once per table** (`pkg/backup/create.go:376` →
`pkg/clickhouse/clickhouse.go:1357`). `system.mutations` enumerates every table on the server on each
query, so the `WHERE database=? AND table=?` filter does not bound the work: cost ≈ O(total tables)
per call × N calls = O(N²). It is gated by `backup_mutations` (default **true**).

## Patch (implemented and validated)

Fetch the in-progress mutation set **once per backup** with a single `system.mutations` scan, then look
it up per table from an in-memory map. New `GetInProgressMutationsBatch(ctx)` in `clickhouse.go`; in
`create.go` it is called once before the table loop and the per-table call becomes a map lookup. Behavior
is identical (same per-table `Mutations` in `TableMetadata`); only the query count changes (N → 1).

```
pkg/backup/create.go         | 22 +++++++--  (one-time fetch before loop; per-table → map lookup)
pkg/clickhouse/clickhouse.go | 25 +++++++++  (new GetInProgressMutationsBatch)
```

### Before/after (bench stand, stage-1 = 55 000 tables, 93.6 GiB, local storage)

| | `system.mutations` queries | `create` wall-clock |
|---|---|---|
| v2.7.2 stock | 38 404+ | killed at 38 401/55 000 after 24.5 min (full ≈ 35 min) |
| **patched** | **1** | **230 s (3.8 min)** — ~239 tables/s, back to small-N rate |

≈**9× faster** at 55 000 tables; the super-linear degradation is eliminated. Restore of the patched
backup verified correct for MergeTree (5 753 678 432 rows match); Join data is still empty (see below).

## Remaining bottlenecks (not addressed by this patch)

1. **Restore schema phase is sequential.** Restoring 55 000 tables took 708 s, dominated by
   ~55 000 sequential `CREATE TABLE` (~13 ms each, also mildly super-linear). Raising
   `general.concurrency` does not parallelize it. Needs bounded-concurrency schema restore.
2. **`fetchHashOfAllFiles` / `getTableSizeFromParts`** issue per-table `system.parts` scans (O(N²) in the
   same way). Secondary after the mutations fix; candidates for the same once-per-backup batching.
3. **Join (and other non-FREEZE) engines lose data silently on restore.** `engine=Join` is schema-only
   (warned at create, silent at restore: RC=0, tables come back empty). Real Join data must be
   reconstructed externally (clickhouse-backup has no equivalent). At minimum restore should warn.
