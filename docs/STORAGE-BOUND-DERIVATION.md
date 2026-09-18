# Derivation: the write-ahead log produced by one transaction

This note answers one question and nothing else: **what is the largest write-ahead log
that a single admitted transaction can produce?** Every storage budget in
`PARITY-MEMORY-DESIGN.md` depends on that figure, so it is derived and measured here
before any policy is written against it.

An earlier design bounded the log with `wal_autocheckpoint` and `journal_size_limit`.
That design was wrong and is withdrawn. `wal_autocheckpoint` triggers a passive
checkpoint, it does not cap anything, and a passive checkpoint can move nothing;
`journal_size_limit` governs the size retained after a reset, not the peak. Neither
bounds a transaction that is still running.

## 1. The arithmetic is exact

From the file format: a WAL is a 32-byte header followed by frames, and "each frame
consists of a 24-byte frame-header followed by a *page-size* bytes of page data"
(`fileformat.html#wal_file_format`). So for a log holding `F` frames:

```
wal_bytes = 32 + F * (24 + page_size)
```

At the 4096-byte page this store uses, one frame costs 4120 bytes. Nothing here is
estimated; the only unknown is `F`.

## 2. Bounding the frame count

A transaction appends a frame each time it writes a page to the log. It writes pages at
two moments: when the page cache *spills* mid-transaction, and at commit. Spilling is
what makes `F` exceed the number of distinct pages the transaction touched, because a
page that is spilled and then modified again is written again.

`PRAGMA cache_spill=OFF` removes the first moment. The documentation describes it as
disabling "the ability of the pager to spill dirty cache pages to the database file in
the middle of a transaction" (`pragma.html#pragma_cache_spill`). If nothing is written
before commit, then commit writes each dirty page exactly once, and

```
F = D,  where D = the distinct pages the transaction dirties
```

**This is evidence, not a contract.** The documentation does not promise that spilling
can never occur when the pragma is off, and it says nothing about behaviour under
memory exhaustion. Section 5 therefore keeps a runtime check rather than trusting the
bound. What the measurements do establish is that the pre-commit log was *exactly zero
bytes* for every operation tested, at full store size, on the runtime measured.

Since `D` cannot exceed the number of pages in the database, and `PRAGMA
max_page_count` caps that number in the engine:

```
D <= N        (N = max_page_count)
wal_max = 32 + N * (24 + page_size)
```

## 3. Measurements

Linux, Python 3.14.4, SQLite 3.46.1, page size 4096, `auto_vacuum=INCREMENTAL` set
before `journal_mode=WAL`. The store uses the real `SCHEMA_SQL` from `memory.py` and a
contentless FTS5 table. Bodies are distinct-token text, not repeated filler: filler
understates the index by two orders of magnitude. Every measurement begins from a
verified log reset, and the log file size is sampled every 0.5 ms so the figure is the
peak during the transaction, not the size left behind.

Filled to the logical ceiling: 3855 entries of 8192 bytes = 32.0 MiB logical.

| State | Data pages | Data | `-shm` |
|---|---|---|---|
| At the logical ceiling | 13274 | 51.85 MiB | 128 KiB |
| After one whole-store snapshot | 21437 | 83.74 MiB | 128 KiB |

**The logical ceiling costs 1.62x in data pages, and 2.62x once a snapshot of it
exists.** The index and per-row overhead are the difference.

One transaction per row. `pre-commit` is the log size observed before `COMMIT` ran.

| Operation | `cache_spill` | Pages | Log peak | Frames | Frames/page | Pre-commit log | RSS growth |
|---|---|---|---|---|---|---|---|
| Rebuild the whole index | ON | 13274 | 19.83 MiB | 5048 | 0.38 | 20571192 | 30.5 MiB |
| Rebuild the whole index | OFF | 13274 | 19.83 MiB | 5047 | 0.38 | **0** | 0.0 MiB |
| Snapshot the whole store | ON | 21437 | 32.31 MiB | 8224 | 0.38 | 31835272 | 0.0 MiB |
| Snapshot the whole store | OFF | 21437 | 32.31 MiB | 8224 | 0.38 | **0** | 0.0 MiB |
| Expire everything | ON | 22502 | 88.38 MiB | 22494 | 1.00 | 90162112 | 0.2 MiB |
| Expire everything | OFF | 22502 | 88.38 MiB | 22493 | 1.00 | **0** | 43.1 MiB |
| Incremental vacuum | ON | 22502 | **68.56 MiB** | 17449 | **3.77** | 71436712 | 0.0 MiB |
| Incremental vacuum | OFF | 22502 | **18.11 MiB** | 4609 | **1.00** | **0** | 3.2 MiB |

### What the table establishes

1. **Amplification is real, and it is the vacuum that produces it.** With spilling
   enabled, incremental vacuum wrote 3.77 frames for every page in the database and
   produced a 68.56 MiB log for an 88 MiB database. Vacuum moves pages repeatedly, so
   pages are spilled and then dirtied again. Synthetic repeated `UPDATE`s do *not*
   reproduce this: they stayed at 1.00 frames per page because SQLite overwrites a
   frame it wrote earlier in the same uncommitted transaction. Relying on that
   overwrite would have been relying on an undocumented optimisation that the vacuum
   path does not benefit from anyway.
2. **`cache_spill=OFF` removes the amplification.** The same vacuum produced 4609
   frames instead of 17449, and 18.11 MiB instead of 68.56 MiB. No operation exceeded
   **1.00 frames per page** with spilling off.
3. **The pre-commit log was zero for every operation with spilling off**, which is the
   observation the `F = D` argument rests on.
4. **The price is memory.** Expiring the whole store grew resident memory by 43.1 MiB,
   because 22493 dirty pages were held until commit instead of being spilled. The
   memory cost of this design is `D * page_size`, and it is bounded by `N * page_size`.

## 4. The engine-enforced durable ceiling

`PRAGMA max_page_count` was set to 6000 and the store driven past it inside one
transaction:

- The write failed with `database or disk is full`.
- The transaction rolled back completely: the page count returned to its pre-transaction
  value of 1911 and every row written by the failed transaction was gone.
- `PRAGMA integrity_check` returned `ok`.
- The failed transaction left **no log at all** — the peak was 0 bytes, because the
  allocation was refused before any page was written.
- Setting the limit below the current page count returned the current count, not the
  requested one. The limit cannot shrink an existing database, so the returned value
  must be read and checked rather than assumed, exactly as `auto_vacuum` taught.

## 5. A reset must be verified, and `busy` is not the test

With a reader pinned to an older snapshot while the owner committed new frames:

| Checkpoint | `busy` | `log_pages` | `checkpointed` | Log after |
|---|---|---|---|---|
| `TRUNCATE`, stale reader present | 1 | 400 | 0 | unchanged, 1.6 MiB |
| `PASSIVE`, stale reader present | **0** | 400 | **0** | unchanged |
| `TRUNCATE`, after the reader left | 0 | 0 | 0 | 0 bytes |

**A passive checkpoint reported `busy = 0` while moving zero pages.** A caller that
treats a clear busy flag as success will conclude the log was reset when nothing
happened. The success test is therefore `log_pages == 0`, not `busy == 0`, and the code
must stop discarding all three returned values.

This also shows the failure is a *recovery* condition, not capacity exhaustion: the log
could not be reset because another process held an old snapshot, which the single-owner
rule exists to prevent.

## 6. The resulting policy

1. **One owning connection.** All readers go through the control socket. No read
   transaction spans a response or an await. A foreign reader is the one thing that can
   block the reset, and it is designed out rather than handled.
2. **`cache_spill=OFF`, read back and confirmed.** This is what makes `F = D` hold. Its
   memory cost is stated, not hidden.
3. **`PRAGMA max_page_count = N`**, read back and checked against the requested value.
   It refuses an oversized incompatible store without deleting anything.
4. **Reset before every write transaction, and verify it.** `wal_checkpoint(TRUNCATE)`
   must return `log_pages == 0`. If it does not, admit no further writes, report
   `storage_blocked`, and keep reads and explicit recovery available.
5. **Check the log peak after commit.** Because section 2 rests on evidence rather than
   a documented guarantee, a log that exceeded its budget is detected and reported
   rather than assumed impossible.

### Sizing

```
DATA_BUDGET  = N * page_size
WAL_BUDGET   = 32 + N * (24 + page_size)
SHM          = 32768 + 8 * N            (measured 192 KiB at 22494 frames)
MAX_PHYSICAL = DATA_BUDGET + WAL_BUDGET + SHM
```

The measured requirement at a 32 MiB logical ceiling is 83.74 MiB of data pages. With a
maintenance reserve above that:

| Quantity | Value |
|---|---|
| `N` (`max_page_count`) | 26624 pages |
| `DATA_BUDGET` | 104.00 MiB |
| `WAL_BUDGET` | 104.61 MiB |
| `SHM` | 240 KiB |
| `MAX_PHYSICAL` | 208.85 MiB |

**The current constants are not achievable.** `MAX_PHYSICAL_BYTES` is 128 MiB while
`MAX_LOGICAL_BYTES` is 32 MiB, and a full store that is snapshotted and then expired
needs 83.74 MiB of data and an 88.38 MiB log at the same moment: 172 MiB. The physical
constant must rise to about 224 MiB, or the logical ceiling must fall. Raising the
physical constant is the recommendation, because halving the logical ceiling halves what
the service is for.

## 7. Limits of this derivation

- Measured on SQLite 3.46.1 and Python 3.14.4 on Linux only. The supported matrix is
  Python 3.11 to 3.13 on Linux and macOS, and those runtimes carry different SQLite
  builds. **The derivation is not validated across the matrix until a workflow run
  reports these same figures.** This is the same open item as the FTS5 matrix check.
- `F = D` rests on the measured absence of pre-commit writes, not on a documented
  guarantee. Policy point 5 exists because of that gap.
- Temporary files used for sorting during index maintenance are outside this budget.
  They live in the temporary directory, not beside the store, and bounding them is a
  separate question from bounding the store.
