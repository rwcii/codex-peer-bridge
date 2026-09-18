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

Source references name functions and identifiers rather than line numbers, because
lines move between builds and a stale number reads as authority it does not have. All are
in [`src/pager.c`](https://raw.githubusercontent.com/sqlite/sqlite/version-3.46.1/src/pager.c)
at tag `version-3.46.1`.

A page reaches the log from two call sites. The first is `pagerStress`, the cache-spill
path. `SPILLFLAG_OFF` is defined as "Never spill cache. Set via pragma", and the comment
on the `doNotSpill` field documents the consequence directly: "When bits SPILLFLAG_OFF or
SPILLFLAG_ROLLBACK of doNotSpill are set, writing to the database from pagerStress() is
disabled altogether". `pagerStress` carries the matching guard on `doNotSpill`, and
`sqlite3PagerSetSpillsize`'s neighbouring pragma handler sets and clears the bit.

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

In [`src/wal.c`](https://raw.githubusercontent.com/sqlite/sqlite/version-3.46.1/src/wal.c),
function `walFrames`.

At commit, when the sync flags are set, SQLite repeats the final frame to reach the next
sector boundary. The comment states it plainly: "If padding is needed, then the final
frame is repeated (with its commit mark) until the next sector boundary is crossed". The
loop follows:

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

`walOpen` initialises `padToSectorBoundary` to 1 and clears it only when the device
reports `SQLITE_IOCAP_POWERSAFE_OVERWRITE`, so it must be assumed set.

The gap to the boundary is strictly less than one sector, and each iteration advances by
one frame, so the term is bounded by the sector-size ceiling. `sqlite3SectorSize` is
clamped -- its "return value is guaranteed to lie between 32 and MAX_SECTOR_SIZE" -- and
`pager.c` defines `MAX_SECTOR_SIZE` as `0x10000`.

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

Allocation failure and rollback behaviour. In the case measured, the refusal came before
any page was written and the log stayed empty, but **that is what one observation showed,
not a promise about every failure.** The guarantee the design relies on is narrower and is
the one SQLite provides: the failing transaction is rolled back whole, so the store is left
at its previous page count with integrity intact, and a caller is told the write did not
happen. Whether frames were written before the failure is not claimed either way. The
runtime translates the engine's refusal rather than surfacing it raw, and reports it as
capacity for an ordinary write and as a blocked store when it strikes a progress
transition, because the reserve exists to stop the latter happening at all. A memory
allocation failure while the dirty set is held (section 6) ends the transaction the same
way, by rollback, never by partial application.

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
`WAL_HEAPMEMORY_MODE` and gates the shared-memory file on the mode, in `walIndexPage`,
`walIndexClose` and `walIndexReadHdr`. Setting `locking_mode=EXCLUSIVE` before
`journal_mode=WAL` holds the wal-index in heap memory instead.

**Measured:** with `locking_mode=EXCLUSIVE` set first, no `-shm` file is created at all,
and a second connection to the store fails with `database is locked`. Without it, a
32768-byte `-shm` appears and a second connection is admitted.

This is adopted, and it does more than remove a term. Single ownership stops being a
convention the design asks callers to respect and becomes something the engine enforces.
Section 2.1's argument depends on there being no foreign reader; exclusive mode is what
makes that true rather than hoped for.

## 5. Auxiliary files are bounded, not excused

Temporary files live in another directory, which is not a reason to leave them out of a
total. Review was right that an observed zero is not a zero bound, and right that the
answer is in the source rather than in a directory listing.

**Sorters and temporary tables** are moved into memory with `PRAGMA temp_store=MEMORY`,
and their cost becomes part of section 6.

**Sub-journals have an in-memory route, and the selected temp policy reaches it.** The
chain is entirely in the pinned sources:

1. [`src/btree.c`](https://raw.githubusercontent.com/sqlite/sqlite/version-3.46.1/src/btree.c),
   `sqlite3BtreeBeginTrans`, calls
   `sqlite3PagerBegin(pPager, wrflag>1, sqlite3TempInMemory(p->db))`. The third argument
   is the pager's `subjInMemory`.
2. [`src/pager.c`](https://raw.githubusercontent.com/sqlite/sqlite/version-3.46.1/src/pager.c),
   `sqlite3PagerBegin`, stores it: `pPager->subjInMemory = (u8)subjInMemory`. Its own
   comment says what it is for -- "If the subjInMemory argument is non-zero, then any
   sub-journal opened during this transaction will be opened as an in-memory file."
3. `openSubJournal` acts on it: `nStmtSpill` is taken from `sqlite3Config`, then
   `if( pPager->journalMode==PAGER_JOURNALMODE_MEMORY || pPager->subjInMemory )
   nStmtSpill = -1;`. A negative spill size makes `sqlite3JournalOpen` create a journal
   that is never backed by a file.
4. [`src/main.c`](https://raw.githubusercontent.com/sqlite/sqlite/version-3.46.1/src/main.c),
   `sqlite3TempInMemory`, decides the predicate from the compile-time setting and the
   pragma: it returns `db->temp_store==2` when `SQLITE_TEMP_STORE` is 1,
   `db->temp_store!=1` when it is 2, and 1 unconditionally when it is 3. Outside 1 to 3 it
   returns 0 whatever the pragma says.

`PRAGMA temp_store=MEMORY` sets `db->temp_store` to 2, so the predicate is true for every
supported value of `SQLITE_TEMP_STORE`, and the sub-journal is in memory for every
statement and virtual-table operation in the connection -- the argument is passed per
transaction at `sqlite3BtreeBeginTrans`, not per statement, so nothing escapes it.

**The one case that breaks the chain is detectable, and is refused.**
[`src/sqliteInt.h`](https://raw.githubusercontent.com/sqlite/sqlite/version-3.46.1/src/sqliteInt.h)
defines `SQLITE_TEMP_STORE` as 1 when nothing overrides it, and a build that overrides it
reports the value in `PRAGMA compile_options`. `Store.require_temp_in_memory` reads it,
treats an absent option as the documented default of 1, and refuses to open on any value
outside 1 to 3 with `unsupported_runtime`. So the store never runs on a build where this
term exists.

**Therefore the sub-journal contributes zero bytes to the filesystem total, and `N` is
not reduced for it.** That conclusion rests on the source route plus a verified
precondition, not on an observation. What it costs instead is memory, and section 6
carries it.

The runtime metadata that does reach the filesystem is declared: the database file, whose
pages `max_page_count` caps, and the log, whose peak section 2.3 bounds. The wal-index is
in heap memory under exclusive mode and is therefore a memory term, not a file. Section 9's
total is the sum of exactly these two files.

A Linux-only check corroborates the conclusion rather than establishing it: a transaction
performing whole-table `UPDATE`s, a multi-row `DELETE` and an incremental vacuum left no
unlinked descriptor holding bytes. It inspects `/proc/self/fd` because a sub-journal is
opened `SQLITE_OPEN_DELETEONCLOSE` and never appears as a directory entry, which is
precisely why a directory listing would have proved nothing. On other platforms the check
is weaker, and the conclusion there rests on the source route alone.

## 6. The memory term

Disabling the spill moves the cost from disk to memory. The resident cost of a
transaction is the dirty payload plus everything that is not payload:

```
memory >= D * page_size          (dirty page payload)
       +  per-page cache metadata
       +  FTS5 working structures
       +  the sorter and temporary tables moved in by section 5
       +  the sub-journal, which section 5 keeps in memory rather than on disk
       +  the wal-index, which section 4 keeps in heap memory rather than in a file
       +  Python objects for the rows in flight
```

`D * page_size` is the payload term only and is **not** a bound on process memory.
Measured, expiring a full store grew resident memory by 43.1 MiB while its dirty payload
was 87.9 MiB, so the relationship is not even a simple ratio. The honest statement is
that the payload term is bounded by `N * page_size` and the remaining terms are bounded by
the same operation design that bounds `D`.

Two of those terms are there by choice. Sections 4 and 5 remove a file from the filesystem
total by moving its contents into memory, so the declared disk total is smaller **because**
this term is larger. That trade is stated rather than banked silently: the wal-index is
bounded by the frame count, which section 2.3 bounds, and the sub-journal by the pages one
statement must undo.

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
`log_pages == 0`, **and** the `-wal` file is absent or exactly zero bytes. Under exclusive
mode a foreign reader cannot exist, so a busy result is a genuine recovery condition rather
than ordinary contention.

### Every transaction, not every few

The bound describes ONE transaction beginning with an empty log. Resetting once after several
have accumulated would bound their sum, which is a larger and different quantity, so the reset
belongs to the transaction rather than to the request. `Store.transaction` is the single write
boundary and every durable change goes through it, **including expiry, reclamation, index
maintenance, schema creation and the initial metadata writes**, each of which previously
committed on its own.

The page check runs **inside** that transaction. Running it after the context manager had
committed produced an error stating the change was rolled back when it had already been
applied -- for a cursor or a snapshot, a false report about durable progress.

### Every transaction in the runtime

Every entry below is an actual `BEGIN IMMEDIATE` in `memory.py`. Nothing else in the file
opens a transaction, and the connection is opened in autocommit mode so nothing can open one
implicitly. The driver starts an implicit transaction only for `INSERT`, `UPDATE`, `DELETE`
and `REPLACE`, so DDL ran in autocommit whatever it was wrapped in -- which is why the schema
was several separately committed transactions until these boundaries were made explicit.

| Transaction | Reset before | Commit | Rollback | Outcome on refusal |
|---|---|---|---|---|
| Initialisation: all 8 schema statements plus the 5 identity rows, one transaction | yes | on success | on any error, removing every table it created | the error, with no tables left behind |
| `_open_fts`: create the search table | yes | on success | on error | `capacity` is swallowed and FTS is reported absent, so the store opens on scans |
| `_reconcile_index`: mark the index invalid | yes | on success | on error | non-blocking; the store opens on scans |
| `_reconcile_index`: rebuild the index | yes | on success | on any error, leaving the index invalid | non-blocking `capacity`, caught, so the store opens on scans |
| `mutation`: an append, a control write, a registration | yes | on success | on refusal or error | `capacity`, rolled back, data intact |
| `progress`: page issuance, acknowledgement, refresh | yes | on success | on refusal or error | `capacity`; an engine refusal blocks the store |
| `expire`: one batch, with index maintenance | yes | on success | on refusal or error | non-blocking `capacity`, which the fallback below catches |
| `expire`: mark the index invalid, after that fallback | yes | on success | on error | the error |
| `expire`: the same batch without index maintenance | yes | on success | on error | the error |
| `expire`: snapshot, idempotency, retirement and cursor cleanup | yes | on success | on error | the error |
| `reclaim`: prune idempotency rows past retention | yes | on success | on error | the error |
| `reclaim`: `PRAGMA incremental_vacuum` (its own transaction) | yes, and reset again afterwards | by the pragma | by the pragma | the error |
| `recover`: `PRAGMA incremental_vacuum` (its own transaction) | yes, **verified** before it runs | by the pragma | by the pragma | the block is kept and nothing is written |

`reset_log` is the only reset, and it records the block itself. It is called before the
`BEGIN`, so a failure there raises before any transaction exists -- which is why the block
must be recorded inside it rather than by the caller's handler, as that handler is never
reached.

**An exception is a failed proof, not an absent problem.** A checkpoint or a `stat` that
raises has established nothing, so it records the block exactly as a bad result does. Letting
it propagate left the state unset, and once the condition cleared the next write proceeded as
though the log had been proved empty. For the log file itself only `FileNotFoundError` proves
absence: a permission or I/O error is a failure to obtain the proof and must not be read as an
empty log.

**A rolled-back initialisation must be completable, and nothing else may be.** The pragmas
applied at open write a database header, so an initialisation that rolls back leaves a nonempty
file with no tables. Reading that as a foreign store made the rollback clean and the store
permanently unopenable.

Widening the rule to fix that opened an adoption hole, and closing it properly took one
read-only classification, `Store.classify`, which runs **before** `configure` so that no file
is modified before it has been judged. It answers `empty`, `unfinished` or `initialised`, and
refuses everything else:

| File | Outcome |
|---|---|
| No objects at all, including a header-only file | `empty`, initialised |
| Exactly this schema, shapes matching, no data, no identity | `unfinished`, completed |
| An identity is recorded | `initialised`, judged by `inspect` |
| Any object this schema does not own | refused, the object named |
| A familiar name with a different definition | refused |
| Any stored row without an identity, whether `meta` is empty **or absent** | refused |
| A schema that cannot be read | refused, never treated as empty |
| A table the catalog lists but that cannot be read | refused, never treated as absent |

Two earlier attempts at this were wrong in instructive ways. Excluding every `search`-prefixed
name let a database whose only table was `search_history` be adopted: a prefix cannot prove a
table is an FTS5 shadow. The shadow set is now exact -- `search_config`, `search_data`,
`search_docsize`, `search_idx`, verified against a real contentless index rather than assumed
-- and admitted only when the virtual table that owns them is present and its definition
matches. And returning as soon as the metadata table was missing meant the check that stops
data being adopted ran only when an empty `meta` table happened to exist, so dropping the table
was enough to have an entry adopted with the head reset to zero.

Shapes are compared, not just names, because a familiar name with another definition is a
different table. The comparison normalises away `IF NOT EXISTS`, which SQLite strips from the
text it stores; without that every table in a healthy store read as differently defined.

**Only the catalog proves absence.** Reading `sqlite_master` successfully says nothing about
whether a later table read will succeed, so the identity and data checks take the catalog's
table list and skip only what it proves is not there. Any read failure on a table that does
exist is refused.

Swallowing those failures defeated the whole classification, in two ways that the tests now
hold shut. A file holding a saved entry was adopted when the count of `entries` failed,
because the check that refuses identity-free data could not see the data it exists to
protect. Worse, an unreadable metadata table looked exactly like an absent one, so a store
belonging to another repository with no entries to trip the data check was classified as an
unfinished start and had this repository's identity written over it. A failed read is not
evidence of emptiness.

Two pragmas run their own transactions rather than sitting inside one, because
`incremental_vacuum` cannot usefully be wrapped. Both are bracketed by resets.

### A blocked store is a recorded state, and its promises are real

A failed reset, or an engine refusal during a transition the reserve exists to protect,
records a blocked state on the store. Writes are then refused with `storage_blocked` until
recovery is asked for explicitly through the `recover` operation, rather than the next write
being accepted as though nothing had happened.

The error promises that reads, status and stop remain available, and they now are. Cleanup ran
ahead of **every** operation, including `hello`, `status`, `recall` and `stop`, so a failed
cleanup blocked exactly the operations the error said were still reachable. Cleanup now runs
only ahead of operations that write.

Recovery is itself a write, so it obeys the precondition it exists to restore: the log is
reset and **the result verified** before anything is written. Reclaiming pages first, or
swallowing the checkpoint's errors, would have written through the very unreset log being
recovered from and hidden the reason. The block is kept unless every postcondition holds, and
`recover` is reachable both as a service operation and as a command-line subcommand -- it was
documented before the subcommand existed, so an operator following the contract had no way to
take it.

The state is held by the running service rather than written into the store, because a store
that cannot be written cannot record that it cannot be written. A restart clears it and the
condition is re-detected on the next write.

## 8. Conformance, not reproduction

Different correct SQLite builds may allocate different numbers of pages for the same
content. A matrix check that demanded identical sizes would fail on a correct build and
would be testing the build rather than this design.

The supported matrix therefore validates the **invariants**, not the figures. Each is
asserted by a test in `test_memory.StorageBoundTests`:

| Invariant | Test |
|---|---|
| Every setting the bound depends on reads back as requested | `test_every_setting_the_bound_depends_on_reads_back_as_requested` |
| A setting that does not take effect refuses the store | `test_a_setting_that_does_not_take_effect_refuses_the_store` |
| No `-shm` file exists | `test_no_shared_memory_file_is_created` |
| The log holds nothing before a commit | `test_the_log_holds_nothing_before_a_commit` |
| Frames never exceed dirty pages plus the padding bound | `test_frames_never_exceed_dirty_pages_plus_the_padding_bound` |
| Each part of the reset conjunction refuses on its own | `test_a_reset_that_did_not_happen_is_reported_not_assumed` |
| Admission never admits an append enforcement will refuse | `test_admission_never_admits_an_append_that_enforcement_will_refuse` |
| A clean reset leaves no log | `test_a_clean_reset_leaves_no_log` |
| The engine ceiling rolls back whole, integrity intact | `test_the_engine_ceiling_rolls_back_whole_with_integrity_intact` |
| A build without in-memory temporaries is refused | `test_a_build_without_in_memory_temporaries_is_refused` |
| No sub-journal reaches disk (Linux) | `test_no_sub_journal_reaches_disk` |
| A known-incomplete index is never served, and the scan is complete | `test_a_known_incomplete_index_is_never_served_and_the_scan_is_complete` |
| No write path commits without resetting the log first | `test_no_write_path_commits_without_resetting_the_log_first` |
| A refused progress transition is really rolled back | `test_a_refused_progress_transition_is_really_rolled_back` |
| A blocked store still reads and recovers on request | `test_a_blocked_store_still_reads_and_recovers_on_request` |
| The schema statements are one transaction | `InitialisationBoundaryTests.test_the_schema_statements_are_one_transaction` |
| A store with tables but no identity completes initialisation | `InitialisationBoundaryTests.test_a_store_with_tables_but_no_identity_completes_initialisation` |
| A store holding entries without identity is refused | `InitialisationBoundaryTests.test_a_store_holding_entries_without_identity_is_refused` |
| A failed reset is recorded and holds later writes | `ResetFailureTests.test_a_failed_reset_is_recorded_and_holds_later_writes` |
| A blocked store answers status and search | `ResetFailureTests.test_a_blocked_store_answers_status_and_search` |
| A failed recovery keeps the block and writes nothing | `ResetFailureTests.test_a_failed_recovery_keeps_the_block_and_writes_nothing` |
| Recovery clears the block once the log can be reset | `ResetFailureTests.test_recovery_clears_the_block_once_the_log_can_be_reset` |
| Recovery is reachable from the command line | `LifecycleTests.test_recovery_is_reachable_from_the_command_line` |
| Another application's database is never adopted | `InitialisationBoundaryTests.test_another_application_database_is_never_adopted` |
| A search-prefixed table is not evidence of an index | `InitialisationBoundaryTests.test_a_search_prefixed_table_is_not_evidence_of_an_index` |
| A shadow table without its virtual table is not owned | `InitialisationBoundaryTests.test_a_shadow_table_without_its_virtual_table_is_not_owned` |
| A table named `search` that is not the index is not owned | `InitialisationBoundaryTests.test_a_table_named_search_that_is_not_the_index_is_not_owned` |
| Data without identity is refused whether `meta` is empty or absent | `InitialisationBoundaryTests.test_data_without_identity_is_refused_whether_meta_is_empty_or_absent` |
| A table defined differently is a different table | `InitialisationBoundaryTests.test_a_table_defined_differently_is_a_different_table` |
| An unfinished schema without data is completed | `InitialisationBoundaryTests.test_an_unfinished_schema_without_data_is_completed` |
| A schema that cannot be read is an error, not an empty file | `InitialisationBoundaryTests.test_a_schema_that_cannot_be_read_is_an_error_not_an_empty_file` |
| Data that cannot be read is not taken for an empty table | `InitialisationBoundaryTests.test_data_that_cannot_be_read_is_not_taken_for_an_empty_table` |
| An identity that cannot be read is not taken for an absent one | `InitialisationBoundaryTests.test_an_identity_that_cannot_be_read_is_not_taken_for_an_absent_one` |
| A table the catalog does not list is genuinely absent | `InitialisationBoundaryTests.test_a_table_the_catalog_does_not_list_is_genuinely_absent` |
| A raising checkpoint holds writes until recovery | `CheckpointExceptionTests.test_a_raising_checkpoint_holds_writes_until_recovery` |
| Only a missing log proves a missing log | `CheckpointExceptionTests.test_only_a_missing_log_proves_a_missing_log` |
| An unreadable log file is a failed proof | `CheckpointExceptionTests.test_an_unreadable_log_file_is_a_failed_proof` |
| A scan-mode store expires across batches without touching the index | `ScanModeExpiryTests.test_a_scan_mode_store_expires_across_batches_without_touching_the_index` |
| Index maintenance that cannot fit invalidates and still removes the rows | `ScanModeExpiryTests.test_index_maintenance_that_cannot_fit_invalidates_and_still_removes_the_rows` |
| A rebuild that cannot fit still opens the store in scan mode | `test_a_rebuild_that_cannot_fit_still_opens_the_store_in_scan_mode` |
| The reserve survives a full store for every promised transition | `test_the_reserve_survives_a_full_store_for_every_promised_transition` |

None of them asserts a page count, a file size or an entry total, so a build that allocates
differently still passes if it upholds the design.

## 9. Sizing

The overall ceiling of 128 MiB and the logical ceiling of 32 MiB are retained. They are
independent upper limits. Neither promises that a logical ceiling of payload plus a full
second copy of it must fit, and admission would refuse that combination anyway, because
frozen snapshot bytes count toward logical usage.

Two files occupy the total, and nothing else: the database, and the log. The shared-memory
file does not exist (section 4) and the sub-journal never reaches disk (section 5).

```
DATA  = N * page_size
WAL   = 32 + (N + PAD_FRAMES) * (24 + page_size)
TOTAL = DATA + WAL <= MAX_PHYSICAL_BYTES
```

`N` is computed from that inequality rather than chosen, so the ceiling cannot drift from
the constant:

| Quantity | Value |
|---|---|
| `PAD_FRAMES` | 16 |
| `MAX_PAGES` (`N`) | 16328 pages |
| `DATA` | 63.78 MiB |
| `WAL_BUDGET_BYTES` | 64.22 MiB |
| Shared memory | 0 |
| Sub-journal on disk | 0 |
| **`TOTAL`** | **127.9991 MiB**, inside 128 MiB |

### The reserve is enforced, not inferred

An index coefficient is a sizing observation and cannot be a limit, so it is not used as
one. The threshold below `N` is chosen explicitly, enforced on the transaction that is
actually running, and the pages above it are unreachable by ordinary writes:

| Constant | Pages | Bytes |
|---|---|---|
| `MAX_PAGES` | 16328 | 63.78 MiB |
| `RESERVE_PAGES` | 2048 | 8.00 MiB |
| `ORDINARY_MAX_PAGES` | 14280 | 55.78 MiB |

`Store.admit` and `Store.enforce_pages` share one effective limit through
`Store.page_cap`, and admission additionally leaves `APPEND_ALLOWANCE` pages for the growth
one append can cause. Enforcement re-reads the count **inside** the transaction and raises
on a breach, which rolls the transaction back.

**Both thresholds must subtract the commit-time allocation, not just one.** While only
enforcement did, a store resting between the two figures admitted every append and rolled
every one back -- which presents to a caller as a store that accepts writes and loses them.
`APPEND_ALLOWANCE` is 8 pages, sized from a leaf, two overflow pages for a body at
`MAX_BODY`, and a pointer-map page. **It is not a bound on what an append can allocate.** The
stored row carries more than the body, and FTS5 maintenance can allocate more than this. A
rolled-back capacity refusal remains a correct outcome; the allowance only stops it being the
usual one, so enforcement is the backstop it was meant to be rather than the ordinary path. A control or progress
transition is checked against `MAX_PAGES` instead, which is what lets it draw on the
reserve. The engine holds `max_page_count` at `MAX_PAGES` underneath both, so the ceiling
does not depend on either check being correct.

**A commit allocates pages the in-transaction count does not yet report**, so enforcement
leaves `COMMIT_SLACK` pages of room. With incremental auto-vacuum one pointer-map page
carries back pointers for `usable/5 = 819` pages, and a growing transaction allocates one as
it crosses that boundary, after the point where the count can be read. Without any allowance,
appends committed above the threshold every time.

**`COMMIT_SLACK` is a guard, not a bound.** The gap was one page on the runtime where it was
measured and as many as eight on another supported build, so the committed count can sit
slightly above the ordinary threshold. That is harmless and is stated rather than papered
over: the reserve is three orders of magnitude larger, `max_page_count` holds underneath
regardless, and the next append is refused at admission. What the tests assert is therefore
that the reserve survives, not that the threshold is exact -- an exact-threshold assertion
would be testing the build.

### Why the reserve is this size

It is justified by the transitions that must never be refused, not by an index ratio:

| Transition | Cost | Basis |
|---|---|---|
| Expiring entries | one batch of `EXPIRY_BATCH` rows, not the whole store | a contentless FTS5 delete writes a tombstone before the vacuum returns pages, and the index cost of arbitrary legal content has no derived bound, so a whole-store tombstone reserve is not a quantity this design can size |
| Withdrawal of a directive | a few pages: at most one leaf, two overflow pages and its index rows | derived from the record format, section 9's split arithmetic |
| Acknowledgement | a few pages, updating cursor and snapshot rows | derived |
| Retirement record | a few pages | derived |
| Index rebuild | not assumed to be free | see below |

**Expiry cannot depend on a reserve it cannot size.** It removes rows in batches of
`EXPIRY_BATCH`, so the reserve need only cover one batch's index maintenance. If even that
will not fit, the index is marked invalid and the rows are removed without maintaining it;
search continues on the complete scan and the index is rebuilt when reclamation has made
room. Reclamation therefore never needs room for a tombstone over the whole index.

**A rebuild is not assumed to add nothing.** It added no pages when measured, but one
observation does not cover every rebuild, so nothing depends on it. `REBUILD_HEADROOM` is a
guard against beginning work that is obviously unaffordable, **not** a proof that the work
fits: the rollback is what makes an unaffordable rebuild safe. The index is marked invalid
**before** the rebuild begins, so an interrupted or rolled-back rebuild leaves it invalid
rather than apparently complete.

**A rebuild that cannot fit must not stop the store opening.** It runs inside
`Store.__init__`, so a capacity failure there used to close the handle and prevent the service
starting -- and the fallback meant for exactly this case could then never be reached, because
nothing was serving. It is caught now: the store opens, the index stays invalid, and search
answers from the scan.

`Service.recall` selects its path on `Store.index_usable`, which is true only when the index
exists **and** covers the head, and otherwise scans `entries.body` directly. The reply states
which path answered. The fallback is complete rather than empty, and a known-incomplete index
is never served.

Selecting the path on trust matters more than it looks. Choosing it on whether the index
returned rows conflates "no such entry" with "index unusable" and answers the two
identically, so a caller cannot tell which it received.

### An example workload, not a worst case

The figures below describe **one admitted workload**. They are an example that sizes the
problem; they are **not** a worst-case bound, because the index term is not derivable from
the record format and the per-entry term is a step function whose peak a scan cannot
locate. The enforced limits above are what bound the store.

**The cost per entry is a step function.** A store of 4936 entries with 6080-byte bodies
occupied 7411 pages; the same row count with 6144-byte bodies and 1% more content occupied
9884. The dominant term moved by 33%.

The record format explains it. For a table b-tree leaf at usable page size `U`, with
`X = U - 35` the largest payload held wholly in the leaf and `M = ((U-12)*32/255) - 23`, a
payload `P > X` is split:

```
K        = M + ((P - M) mod (U - 4))
local    = K if K <= X else M
overflow = ceil((P - local) / (U - 4))
per_leaf = floor((U - 12) / (local + 6))
pages_per_row = 1/per_leaf + overflow
```

`local` moves cyclically with `P`, so it crosses `(U-12)/2` and halves `per_leaf` from 2 to
1. At a 6080-byte body `local` is 2035 and two cells share a leaf, costing 1.50 pages per
row. Sixty-four bytes later `local` is 2099, one cell fills a leaf, and the cost is 2.00.

Maximising `n(b) * pages_per_row(b)` over every admissible body size gives 9872 pages for
the `entries` table, at about a 6082-byte body with the entry cap binding at 4936 rows. The
result is insensitive to the estimate of the non-body columns across 45 to 48 stored bytes.
Against measurement the model predicts 7404 pages where 7411 were observed and 9872 where
9884 were observed: within 0.12% on both sides of the step.

A `dbstat` breakdown of that store attributes 9884 pages to `entries`, 4634 to
`search_data`, and 67 to `search_idx`, `entries_live`, `search_docsize` and the small
tables together. The index came to 0.626 of the body bytes it covers, a ratio that depends
on token distribution.

| Term | Pages |
|---|---|
| `entries`, from the record format | 9872 |
| Search index, at the observed 0.626 ratio with margin | 5120 |
| Indexes and small tables | 70 |
| **Example total** | **15062 (58.84 MiB)** |

That example exceeds `ORDINARY_MAX_PAGES`, which is the expected consequence of enforcing a
threshold rather than inferring one: such a store stops accepting ordinary appends before
its logical ceiling, reports `capacity`, keeps the reserve available so progress and
withdrawal still commit, and recovers through expiry and reclamation. This is a capacity
outcome with a defined recovery, not a correctness failure, and it is not claimed to be
impossible.

Attempting to add frozen snapshots to a store already at the entry cap is refused, because
admission counts reserved slots as well as bytes. That is a further reason the withdrawn
"data plus a full second copy" figure was never an admissible state.

## 10. Limits

- Source references are pinned to tag `version-3.46.1` and name functions and identifiers
  rather than line numbers, because lines move between builds. They must be re-checked
  when the supported set changes.
- Measurements were taken on one host, with SQLite 3.46.1 and Python 3.14.4 on Linux. They
  corroborate derived terms and they size the example workload; they do not establish any
  bound. Section 8 is what the supported matrix checks, and it asserts no figure.
- The sub-journal conclusion rests on the source route plus a precondition the store
  verifies at open. Its corroborating check is strong on Linux and weaker elsewhere, as
  section 5 records.
- The index page cost is not derived. It is why `ORDINARY_MAX_PAGES` is enforced rather
  than inferred, and why a store reaching its page threshold before its logical ceiling is
  documented as a handled capacity outcome rather than ruled out.
- A rebuild is not assumed to add no pages. It is guarded before it starts, rolled back if it
  does not fit, and search falls back to a complete scan.
- `APPEND_ALLOWANCE`, `REBUILD_HEADROOM` and `COMMIT_SLACK` are guards that keep refusals and
  overshoot rare, not bounds. None makes a rolled-back capacity refusal impossible, and none
  is claimed to. `max_page_count` is the only hard limit here, and the engine holds it.
- The blocked state is held by the running service, not written into the store, because a
  store that cannot be written cannot record that it cannot be written.
