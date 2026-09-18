# Derivation: the write-ahead log produced by one transaction

This note answers one question: **what is the largest write-ahead log a single admitted
transaction can produce?** Every storage budget in `PARITY-MEMORY-DESIGN.md` rests on
that figure, so it is derived from the SQLite sources, against pinned versions, before
any policy is written against it.

Two earlier designs are withdrawn.

- Bounding the log with `wal_autocheckpoint` and `journal_size_limit` was wrong.
  `wal_autocheckpoint` triggers a passive checkpoint rather than capping anything, and
  `journal_size_limit` governs what is retained after a reset, not the peak.
- Bounding it by measurement plus a check after commit was also wrong. A check after
  the fact is a diagnostic for a violated assumption; it cannot hold a peak below a hard
  limit. Sampling a file size, however finely, yields an observed maximum, not a bound.

Every term below is finite and traced to source. Measurements appear only to corroborate
a term that the source already establishes, and they are labelled by which kind of
workload produced them.

## 1. The frame arithmetic is exact

A WAL is a 32-byte header followed by frames, and "each frame consists of a 24-byte
frame-header followed by a *page-size* bytes of page data"
(`fileformat.html#wal_file_format`). For `F` frames at this store's 4096-byte page, one
frame costs 4120 bytes:

```
wal_bytes = 32 + F * (24 + page_size)
```

## 2. `F` has exactly two terms

```
F <= D + P
```

`D` is the count of distinct pages the transaction dirties. `P` is the sector-padding
term. Both are bounded below.

### 2.1 Why the commit list is the only write path: `D`

Source references are to SQLite 3.46.1, `src/pager.c`.

A page reaches the log from two call sites. The first is `pagerStress`, the cache-spill
path. `SPILLFLAG_OFF` is defined as "Never spill cache. Set via pragma" (line 447), and
the pager documents the consequence directly: "When bits SPILLFLAG_OFF or
SPILLFLAG_ROLLBACK of doNotSpill are set, writing to the database from pagerStress() is
disabled altogether" (lines 523-524). The guard is at line 4610, and the pragma sets and
clears the bit at lines 3630-3632.

With that path disabled the only remaining writer is the commit dirty list, which visits
each dirty page once. Hence `F = D` before padding, as a property of the code rather
than of an observation.

**Corroboration, raw-engine stress workload.** With spilling off the log held *zero
bytes* before `COMMIT` in every operation measured, and no operation exceeded 1.00 frames
per page. With spilling on, incremental vacuum reached **3.77 frames per page** and a
68.56 MiB log over an 88 MiB database, because vacuum moves pages repeatedly and each
spilled page is written again. The same vacuum with spilling off wrote 4609 frames
instead of 17449. Repeated `UPDATE`s do not reproduce the amplification, so a probe built
from them would have wrongly suggested spilling is free.

The price of disabling the spill is memory: the dirty set is held until commit. Expiring
a full store grew resident memory by 43.1 MiB. Section 6 states that term.

### 2.2 The padding term: `P`

Source references are to SQLite 3.46.1, `src/wal.c`, in `walFrames`.

At commit, when the sync flags are set, SQLite repeats the final frame to reach the next
sector boundary. The comment states it plainly: "If padding is needed, then the final
frame is repeated (with its commit mark) until the next sector boundary is crossed"
(lines 4141-4144). The loop follows at lines 4150-4160:

```c
if( pWal->padToSectorBoundary ){
  int sectorSize = sqlite3SectorSize(pWal->pWalFd);
  w.iSyncPoint = ((iOffset+sectorSize-1)/sectorSize)*sectorSize;
  bSync = (w.iSyncPoint==iOffset);
  while( iOffset<w.iSyncPoint ){
    rc = walWriteOneFrame(&w, pLast, nTruncate, iOffset);
    if( rc ) return rc;
    iOffset += szFrame;
    nExtra++;
  }
}
```

`padToSectorBoundary` is initialised to 1 (line 1707) and cleared only when the device
reports `SQLITE_IOCAP_POWERSAFE_OVERWRITE` (line 1725), so it must be assumed set.

The gap to the boundary is strictly less than one sector, and each iteration advances by
one frame, so the term is bounded by the sector-size ceiling. `sqlite3SectorSize` is
clamped -- its "return value is guaranteed to lie between 32 and MAX_SECTOR_SIZE"
(`pager.c`, line 2681) -- and `MAX_SECTOR_SIZE` is `0x10000` (`pager.c`, line 415).

```
P <= ceil((MAX_SECTOR_SIZE - 1) / (24 + page_size)) = ceil(65535 / 4120) = 16
```

Padding occurs only when `WAL_SYNC_FLAGS(sync_flags) != 0`, so lowering `synchronous`
would remove the term. **Durability is kept and the term is carried instead.** A storage
formula is not a reason to weaken a durability guarantee.

**Corroboration, raw-engine stress workload.** A transaction dirtying 4000 pages produced
4001 frames, and one dirtying 302 pages produced 304: one to two padding frames on a
4096-byte sector, consistent with the bound.

### 2.3 The bound

```
F    <= N + 16                       (N = max_page_count, since D <= N)
WAL  <= 32 + (N + 16) * (24 + page_size)
```

## 3. The durable ceiling is engine-enforced

`PRAGMA max_page_count` caps the page count inside the engine. Driven past a 6000-page
limit inside one transaction:

- the write failed with `database or disk is full`;
- the transaction rolled back completely, the page count returning to its pre-transaction
  value and every row from the failed transaction gone;
- `PRAGMA integrity_check` returned `ok`;
- the failed transaction wrote **no log at all**, because the allocation was refused
  before any page was written;
- asked to set the limit below the current page count, it returned the current count
  rather than the requested one.

The last point is the operative one: the returned value must be read and compared with
the request, exactly as `auto_vacuum` taught this project once already. The limit cannot
shrink an existing database, so an oversized incompatible store is refused without
deleting anything.

Allocation failure and rollback behaviour: a refused allocation raises before any page is
written, the transaction is rolled back whole, and the store is left at its previous
page count with integrity intact. A memory allocation failure while the dirty set is held
(section 6) fails the transaction the same way, by rollback, not by partial application.

## 4. The shared-memory term is zero, by exclusive mode

The wal-index file is allocated in fixed blocks, not arbitrary byte counts. From
`walformat.html`: "each hash table is 32768 bytes in size. Except, a 136-byte header is
carved out of the front of the very first hash table"; the first block maps 4062 frames
(`u32 aPgno[4062]`) and each later block maps 4096; "the total size of the shm file is
always a multiple of 32768". So where the file exists:

```
shm_bytes = 32768 * (1 + ceil(max(0, F - 4062) / 4096))
```

**It does not have to exist.** `wal.c` distinguishes `WAL_EXCLUSIVE_MODE` from
`WAL_HEAPMEMORY_MODE` (lines 554-555) and gates the shared-memory file on the mode
(lines 770, 911, 1606, 1613). Setting `locking_mode=EXCLUSIVE` before
`journal_mode=WAL` holds the wal-index in heap memory instead.

**Measured:** with `locking_mode=EXCLUSIVE` set first, no `-shm` file is created at all,
and a second connection to the store fails with `database is locked`. Without it, a
32768-byte `-shm` appears and a second connection is admitted.

This is adopted, and it does more than remove a term. Single ownership stops being a
convention the design asks callers to respect and becomes something the engine enforces.
Section 2.1's argument depends on there being no foreign reader; exclusive mode is what
makes that true rather than hoped for.

## 5. Auxiliary files are bounded, not excused

Temporary files live in another directory, which is not a reason to leave them out.

**Sorters and temporary tables** are moved into memory with `PRAGMA temp_store=MEMORY`,
and their memory cost is part of section 6.

**Sub-journals** have exactly two write sites in `pager.c`. One is inside `pagerStress`
(line 4620) and is therefore unreachable once spilling is disabled. The other is gated on
an open savepoint: `if( pPager->nSavepoint>0 ) subjournalPageIfRequired(pPg);`
(line 6090). The service opens no explicit savepoint, so the only source is a statement
journal that SQLite opens for a statement needing statement-level rollback.

Such a journal is memory-backed up to a threshold and spills beyond it. `openSubJournal`
takes `nStmtSpill` from `sqlite3Config` (line 4499), which `SQLITE_CONFIG_STMTJRNL_SPILL`
sets (`main.c`, lines 745-748). That entry point is a `sqlite3_config()` call and is not
reachable through Python's `sqlite3` module, so the threshold cannot be configured away
and the term is carried instead of dismissed.

Its size is bounded by the pages **one statement** must be able to undo, not by the whole
transaction, because the journal belongs to the statement. The design constraint that
keeps it finite is therefore: no explicit savepoint, and no single maintenance statement
whose undo set is unbounded.

**Measured:** a transaction performing four whole-table `UPDATE`s, a multi-row `DELETE`
and an incremental vacuum over 6000 rows produced **zero bytes** of sub-journal on disk.
The check inspected `/proc/self/fd` for unlinked files, because these are opened
`SQLITE_OPEN_DELETEONCLOSE` (line 4498) and never appear as a directory entry. That
technique is Linux-only, so the macOS verification is by directory inspection alone and
is weaker; this is recorded as a limit rather than glossed.

## 6. The memory term

Disabling the spill moves the cost from disk to memory. The resident cost of a
transaction is the dirty payload plus everything that is not payload:

```
memory >= D * page_size          (dirty page payload)
       +  per-page cache metadata
       +  FTS5 working structures
       +  the sorter and temporary tables moved in by section 5
       +  Python objects for the rows in flight
```

`D * page_size` is the payload term only and is **not** a bound on process memory.
Measured, expiring a full store grew resident memory by 43.1 MiB while its dirty payload
was 87.9 MiB, so the relationship is not even a simple ratio. The honest statement is
that the payload term is bounded by `N * page_size` and the remaining terms are bounded
by the same operation design that bounds `D`.

## 7. Verifying a reset

`PRAGMA wal_checkpoint` returns three values and the code must stop discarding them. It
must also not be reduced to a single one of them.

With a reader pinned to an older snapshot while the owner committed new frames:

| Checkpoint | `busy` | `log_pages` | `checkpointed` | Log after |
|---|---|---|---|---|
| `TRUNCATE`, stale reader present | 1 | 400 | 0 | unchanged, 1.6 MiB |
| `PASSIVE`, stale reader present | **0** | 400 | **0** | unchanged |
| `TRUNCATE`, after the reader left | 0 | 0 | 0 | 0 bytes |

A passive checkpoint reported `busy = 0` while moving nothing, so a clear busy flag is
not proof of a reset.

`log_pages == 0` alone is also not proof, because `(0, 0, 0)` is returned in three
different situations, measured: when no log file has ever existed, when a `TRUNCATE`
genuinely reset the log, and when the log was already empty. The return convention for a
missing log is therefore indistinguishable from success on the pragma's results alone.

**The reset test is the conjunction:** the checkpoint returned `busy == 0` **and**
`log_pages == 0`, **and** the `-wal` file is absent or exactly zero bytes. A failure is
reported as `storage_blocked`, admits no further writes, and leaves reads and explicit
recovery available. Under exclusive mode a foreign reader cannot exist, so a busy result
is a genuine recovery condition rather than ordinary contention.

## 8. Conformance, not reproduction

Different correct SQLite builds may allocate different numbers of pages for the same
content. A matrix check that demanded identical sizes would fail on a correct build and
would be testing the build rather than this design.

The supported matrix therefore validates the **invariants**, not the figures:

1. `cache_spill`, `locking_mode`, `max_page_count`, `auto_vacuum` and `journal_mode` read
   back the values that were requested.
2. No `-shm` file exists.
3. The log holds zero bytes before any `COMMIT`.
4. Frames per dirty page never exceed `1 + 16/D`.
5. A reset satisfies the section 7 conjunction.
6. A breach of `max_page_count` rolls back whole, with integrity intact.
7. No sub-journal reaches disk.

## 9. Sizing

The overall ceiling of 128 MiB and the logical ceiling of 32 MiB are retained. They are
independent upper limits; neither promises that a logical ceiling of payload plus a full
second copy of it must fit, and admission would refuse that combination anyway, because
frozen snapshot bytes count toward logical usage.

With the shared-memory term zero and auxiliary files reserved separately:

```
DATA  = N * page_size
WAL   = 32 + (N + 16) * (24 + page_size)
TOTAL = DATA + WAL <= MAX_PHYSICAL_BYTES
```

Solving at 128 MiB gives `N = 16327` pages:

| Quantity | Value |
|---|---|
| `N` (`max_page_count`) | 16327 pages |
| `DATA_BUDGET` | 63.78 MiB |
| `WAL_BUDGET` | 64.21 MiB |
| `SHM` | 0 |
| `TOTAL` | 127.99 MiB |

### What a full store actually needs

Admitted service workload: every byte counted here passed `admit()`. This is what sets
the reserve, and it is kept separate from the raw-engine stress figures above.

**The cost per entry is a step function, so a scan cannot establish it.** A store filled
with 6080-byte bodies occupied 7411 pages; the same store filled with 6144-byte bodies --
the same 4936 rows and 1% more content -- occupied 9884. The dominant term moved by 33%.

The cause is the record format, and it makes the cost derivable. For a table b-tree leaf
at usable page size `U`, with `X = U - 35` the largest payload held wholly in the leaf and
`M = ((U-12)*32/255) - 23`, a payload `P > X` is split:

```
K        = M + ((P - M) mod (U - 4))
local    = K if K <= X else M
overflow = ceil((P - local) / (U - 4))
per_leaf = floor((U - 12) / (local + 6))
pages_per_row = 1/per_leaf + overflow
```

`local` moves cyclically with `P`, so it crosses `(U-12)/2` and halves `per_leaf` from 2
to 1. That is the step. At a 6080-byte body `local` is 2035 and two cells share a leaf, so
the cost is 1.50 pages per row. Sixty-four bytes later `local` is 2099, one cell fills a
leaf, and the cost is 2.00.

So the bound is computed rather than sampled, by maximising `n(b) * pages_per_row(b)` over
every admissible body size, where `n(b) = min(MAX_ENTRIES - RESERVED_ENTRIES,
(MAX_LOGICAL_BYTES - RESERVED_BYTES) / (b + ENTRY_OVERHEAD))`:

| Quantity | Value |
|---|---|
| Worst body size | about 6082 bytes |
| Entries at that size | 4936, the entry cap |
| `local` | 2037, one cell per leaf |
| Pages per row | 2.00 |
| **`entries` table** | **9872 pages, 38.56 MiB** |

The maximum is insensitive to the estimate of the non-body columns: 45 through 48 stored
bytes all yield 9872 pages. Against measurement the model predicts 7404 pages where 7411
were observed, and 9872 where 9884 were observed -- within 0.12% at both points, including
across the step.

**The index term is measured, not derived.** A `dbstat` breakdown of the worst store
attributes 9884 pages to `entries`, 4634 to `search_data`, and 67 to `search_idx`,
`entries_live`, `search_docsize` and the small tables together. The index came to 0.626 of
the body bytes it covers. That ratio depends on token distribution, which the record
format does not constrain, so it is carried as a measured coefficient with margin rather
than presented as a bound.

| Term | Basis | Pages |
|---|---|---|
| `entries` | derived from the record format | 9872 |
| Search index | measured at 0.626, bounded at 0.70 | 5120 |
| Indexes and small tables | measured | 70 |
| **Worst admissible store** | | **15062 (58.84 MiB)** |

### The reserve

| Quantity | Pages | Bytes |
|---|---|---|
| Data budget, `N` | 16327 | 63.78 MiB |
| Worst admissible store | 15062 | 58.84 MiB |
| **Maintenance reserve** | **1265** | **4.94 MiB** |

The reserve covers what must never be refused: an acknowledgement, a retirement record, a
withdrawal and an index rebuild. A rebuild is the largest and, measured, adds no pages at
all, because it replaces index content rather than growing the store. The others cost a
handful of pages each.

**Because the index term is measured rather than derived, a store reaching `N` before its
logical ceiling stays possible.** That is a capacity outcome, not a correctness failure,
and the contract already requires the behaviour: refuse with `capacity`, leave the reserve
available so progress and withdrawal still commit, and keep recovery reachable. It is not
claimed to be impossible, because the evidence does not support that claim.

Attempting to add frozen snapshots to a store already at the entry cap is refused, because
admission counts reserved slots as well as bytes. That is a further reason the withdrawn
"data plus a full second copy" figure was never an admissible state.

## 10. Limits

- Source references are pinned to SQLite 3.46.1. A supported build carrying a different
  version is covered by the section 8 invariants, not by these line numbers, and the
  references must be re-pinned when the supported set changes.
- Measurements were taken on one host. They corroborate the derived terms; they do not
  establish them, and section 8 is what the matrix checks.
- The sub-journal verification is strong on Linux and weaker on macOS, as section 5
  records.
