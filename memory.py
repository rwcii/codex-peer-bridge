#!/usr/bin/env python3
"""Shared per-repository memory service. Python standard library only.

One service per repository, shared by every agent session working in it. It holds
an append-only event log and serves it over a private control socket, so a session
that started earlier can still learn what a later session decided.

The service stores reported data. Nothing here grants authority: an entry cannot
approve an action, widen a task scope, or change any configuration. See
`docs/PARITY-MEMORY-DESIGN.md` for the contract this implements.

Three rules shape the storage design, and each exists because its absence produced
a defect in review:

* **Liveness is recorded, never derived.** A supersession or revocation writes a
  durable link onto the row it replaces. Deriving liveness from rows that still
  exist lets reclamation of a replacement resurrect what it replaced.
* **A snapshot is frozen, not referenced.** Members are copied as immutable
  payloads at creation, so a concurrent revocation cannot change what a reader is
  still paging through, and reclamation cannot make a page unreachable.
* **The server owns protocol state.** Page issuance, completion and acknowledgement
  are recorded here, never inferred from a number the caller supplies.
"""
import argparse
import asyncio
import contextlib
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import signal
import socket
import sqlite3
import subprocess
import time
import uuid

from peer_transport import LIMIT, credentials, encode, private_dir
import platform_support
from peer_transport import control_exchange, NoControlReply, UnsafeServiceEndpoint

PROTOCOL = 1
SCHEMA = 3

# One statement per element. `executescript` runs a script in autocommit mode, so these
# would be one separately committed transaction each: a mid-script failure left the earlier
# tables behind, and only the first transaction would begin from an empty log. They are
# executed individually inside one explicit transaction instead, which SQLite supports for
# DDL -- a rollback removes every table the transaction created.
SCHEMA_STATEMENTS = (
"""CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT)""",
"""
CREATE TABLE IF NOT EXISTS entries(
    seq INTEGER PRIMARY KEY, ts REAL, type TEXT, scope TEXT, scope_target TEXT, path TEXT,
    body TEXT, author TEXT, author_pid INTEGER, consumer TEXT, revision INTEGER,
    supersedes INTEGER, revokes INTEGER, superseded_by INTEGER, revoked_by INTEGER,
    conflicts_with INTEGER, expires REAL)""",
"""CREATE TABLE IF NOT EXISTS idem(
    key TEXT PRIMARY KEY, fingerprint TEXT, seq INTEGER, ts REAL, deadline REAL)""",
"""CREATE TABLE IF NOT EXISTS cursors(
    consumer TEXT PRIMARY KEY, seq INTEGER, issued INTEGER, snapshot TEXT,
    bootstrapped INTEGER, resnapshot INTEGER, updated REAL)""",
"""CREATE TABLE IF NOT EXISTS retired(consumer TEXT PRIMARY KEY, seq INTEGER, at REAL)""",
"""CREATE TABLE IF NOT EXISTS snapshots(
    id TEXT PRIMARY KEY, consumer TEXT, head INTEGER, created REAL, items INTEGER,
    issued INTEGER, acked INTEGER, acked_at REAL)""",
"""CREATE TABLE IF NOT EXISTS snapshot_items(
    id TEXT, position INTEGER, seq INTEGER, payload TEXT, bytes INTEGER,
    PRIMARY KEY(id, position))""",
"""CREATE INDEX IF NOT EXISTS entries_live ON entries(superseded_by, revoked_by, expires)""",
)
# Objects this schema owns. A file carrying only these, with no application data and no
# identity, is an unfinished start and may be completed; anything else is another
# application's database and is refused untouched. Names alone are not enough: a definition
# that differs is a different table wearing the same name, so shapes are compared too.
SCHEMA_TABLES = frozenset(('meta', 'entries', 'idem', 'cursors', 'retired', 'snapshots',
                           'snapshot_items'))
SCHEMA_INDEXES = frozenset(('entries_live',))
# Tables holding what a caller stored. Any row here without an identity means the file is
# somebody's data, whether the metadata table is empty or missing altogether.
DATA_TABLES = ('entries', 'snapshot_items', 'snapshots', 'cursors', 'retired', 'idem')
FTS_TABLE = 'search'
FTS_TABLE_SQL = 'CREATE VIRTUAL TABLE IF NOT EXISTS search USING fts5(body, content="")'
# The shadow tables FTS5 creates for a contentless `search`, verified against a real one
# rather than assumed from a prefix. `search_content` is absent because the table is
# contentless. A prefix cannot prove a table is a shadow: `search_history` is not one.
FTS_SHADOWS = frozenset(('search_config', 'search_data', 'search_docsize', 'search_idx'))


def normalised_sql(text):
    """Compare definitions without being defeated by whitespace, case or SQLite's rewriting.

    SQLite stores a definition with `IF NOT EXISTS` removed, so the authored statement and
    the stored one never match literally. Comparing them raw reported every table in a
    perfectly good store as differently defined.
    """
    return ' '.join((text or '').split()).casefold().replace('if not exists ', '')

TYPES = ('decision', 'finding', 'gotcha', 'handoff', 'status', 'directive')
SCOPES = ('repo', 'task', 'session')

# Capacity. Both budgets are enforced: the logical one bounds what callers store,
# the physical one bounds what the file costs on disk including indexes and the
# search table. Reservations hold back slots *and* bytes, because an entry slot
# without room for its body still cannot record a withdrawal.
MAX_BODY = 8192
MAX_ENTRIES = 5000
MAX_LOGICAL_BYTES = 32 * 1024 * 1024
MAX_PHYSICAL_BYTES = 128 * 1024 * 1024
RESERVED_ENTRIES = 64
RESERVED_BYTES = RESERVED_ENTRIES * MAX_BODY
ENTRY_OVERHEAD = 512

# The durable ceiling, and the log's derived worst case.
#
# The log is never measured for admission. It is reset before every write transaction and
# its peak follows from the page ceiling, so admission cannot depend on checkpoint timing
# or on how far a particular SQLite build shrinks the file during recovery. An earlier
# design compared the total file size including the log and added a fixed margin for it;
# that made the same write succeed or fail according to when the last checkpoint ran.
#
# One transaction appends one frame per page it dirties, because cache_spill=OFF leaves
# the commit dirty list as the only path to the log, and then repeats the final frame to
# the next sector boundary. sqlite3SectorSize is clamped to MAX_SECTOR_SIZE, so that
# padding is finite. docs/STORAGE-BOUND-DERIVATION.md carries the argument and the
# source references; the numbers here are derived from it and must not be tuned alone.
PAGE_SIZE = 4096
FRAME_BYTES = 24 + PAGE_SIZE
MAX_SECTOR_SIZE = 0x10000
PAD_FRAMES = -(-(MAX_SECTOR_SIZE - 1) // FRAME_BYTES)
# The largest page ceiling for which the data pages and the log's worst case both fit.
MAX_PAGES = (MAX_PHYSICAL_BYTES - 32 - PAD_FRAMES * FRAME_BYTES) // (PAGE_SIZE + FRAME_BYTES)
WAL_BUDGET_BYTES = 32 + (MAX_PAGES + PAD_FRAMES) * FRAME_BYTES
# Pages ordinary appends may not consume, so that progress and control transitions still
# commit at a full store. It is sized from those transitions, not from an index ratio:
# expiring a full store transiently adds index tombstones before the vacuum returns the
# pages, which measured about 0.28 pages for each entry removed, and a withdrawal, an
# acknowledgement and a retirement record cost a few pages each.
RESERVE_PAGES = 2048
ORDINARY_MAX_PAGES = MAX_PAGES - RESERVE_PAGES
# A commit allocates pages the in-transaction count does not yet report: with incremental
# auto-vacuum one pointer-map page carries back pointers for usable/5 pages, and one is
# allocated as a growing transaction crosses that boundary, after the point where the count
# can be read. Enforcement leaves this much room so the COMMITTED store honours its
# threshold rather than the state part way through.
#
# It is a guard, NOT a bound. The gap was one page for an ordinary append on the runtime
# where it was measured and as many as eight on another supported build, so the committed
# count can sit slightly above the ordinary threshold. That is harmless: the reserve is
# three orders of magnitude larger, the engine holds MAX_PAGES underneath regardless, and
# the next append is refused at admission. What must not be claimed is that the threshold
# is exact.
PTRMAP_COVERAGE = PAGE_SIZE // 5
COMMIT_SLACK = 2
# Room admission leaves for the growth one append causes, so that a store resting just
# under the threshold does not admit every append and then roll every one of them back.
# It is sized from a leaf, two overflow pages for a body at MAX_BODY, and a pointer-map
# page. It is NOT a bound on what an append can allocate: the stored row carries more than
# the body, and FTS5 maintenance can allocate more than this. A rolled-back capacity
# refusal remains a correct outcome; this only stops it being the usual one.
APPEND_ALLOWANCE = 8
# Expiry removes rows in batches so that reclamation never needs room for index
# maintenance over the whole store at once. A whole-store tombstone reserve is not
# something this design can size, because the index cost of arbitrary legal content is not
# bounded by the record format.
EXPIRY_BATCH = 200
# Headroom a rebuild must see before it starts. It is a guard against beginning work that
# is obviously unaffordable, NOT a proof that the work fits: the rollback is what makes an
# unaffordable rebuild safe, and the index cost of arbitrary content has no derived bound.
REBUILD_HEADROOM = RESERVE_PAGES // 2

# Lifetimes. Every retained record has one, and expiry returns a defined recovery
# result rather than silently changing a caller's meaning.
SNAPSHOT_TTL = 3600
ACK_RETENTION = 86400
IDEM_TTL = 86400
CONSUMER_TTL = 30 * 86400
MAX_SNAPSHOTS_PER_CONSUMER = 4
MAX_CONSUMERS = 256
MAX_IDEM_ROWS = 20000
RETIRED_TTL = 90 * 86400
MAX_RETIRED = 1024
EXPIRY_INTERVAL = 30

# Response framing. A page is bounded by encoded bytes, not by a row count, because
# 200 rows of maximum body cannot fit one frame.
FRAME_BUDGET = LIMIT - 8192
# Rows fetched per read before byte bounding. A window that silently truncated would
# report `more` as false while results remained.
ROW_WINDOW = 500

# Test fixture only, read once at import and consulted on no other path. When set, the
# service exits abruptly after a write has committed and before its response is sent.
# That window is precisely what a durability claim must survive, and a test that kills
# the process after a successful response has not entered it.
CRASH_AFTER_COMMIT = os.environ.get('MEMORY_TEST_CRASH_AFTER_COMMIT') == '1'
# Test fixture only: hold a computed reply for this many seconds before sending it, so a
# shutdown can be driven while a request is genuinely in flight.
REPLY_DELAY = float(os.environ.get('MEMORY_TEST_REPLY_DELAY') or 0)

SNAPSHOT_ORDER = {'directive': 0, 'decision': 1, 'gotcha': 2, 'handoff': 3, 'finding': 4, 'status': 5}
SNAPSHOT_TAIL = {'finding': 25, 'handoff': 25, 'status': 10}


class MemoryError_(ValueError):
    """A request the service refuses. `code` names the recovery path."""

    def __init__(self, code, detail):
        super().__init__(f'{code}: {detail}')
        self.code, self.detail = code, detail


def repo_identity(start=None):
    """Canonical repository key: the Git common directory, absolute, hashed.

    The bare `--git-common-dir` prints a path relative to the working directory, so
    two conventional checkouts both report `.git` and would collide. The absolute
    form is required, and it is what makes every worktree of one repository share a
    single memory service.
    """
    try:
        out = subprocess.run(['git', 'rev-parse', '--path-format=absolute', '--git-common-dir'],
                             cwd=str(start or Path.cwd()), capture_output=True, text=True, timeout=10)
    except (OSError, subprocess.SubprocessError) as exc:
        raise MemoryError_('repo_unresolved', 'cannot resolve the repository') from exc
    path = out.stdout.strip()
    if out.returncode or not path or not Path(path).is_absolute():
        raise MemoryError_('repo_unresolved',
                           'not inside a Git repository, or Git is too old for --path-format')
    return hashlib.sha256(str(Path(path).resolve()).encode()).hexdigest()[:16]


def state_dir(root, repo):
    return Path(root) / 'memory' / repo


def storage_exhausted(exc):
    """Did the engine itself refuse the allocation, rather than this code refusing it?

    `max_page_count` is the durable ceiling and the engine enforces it whatever admission
    decided, so the error it raises must be reported as capacity with the transaction's
    real outcome, not surfaced as an unexplained database error.
    """
    return (isinstance(exc, sqlite3.OperationalError)
            and 'full' in str(exc).lower())


def owned_elsewhere(exc):
    """Is this failure another owner holding the store, rather than a broken store?

    Exclusive locking makes this an ordinary outcome rather than a corruption signal, so
    it must not be reported as an unreadable file. SQLite distinguishes it only by
    message, so the test is on the message and is kept in one place.
    """
    return (isinstance(exc, sqlite3.OperationalError)
            and 'locked' in str(exc).lower())


def fingerprint(payload):
    return hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=True).encode()).hexdigest()


def measure(text):
    """UTF-8 byte width. `length()` in SQLite counts characters, not bytes."""
    return len(text.encode())


class Store:
    FIELDS = ('seq', 'ts', 'type', 'scope', 'scope_target', 'path', 'body', 'author', 'author_pid',
              'consumer', 'revision', 'supersedes', 'revokes', 'superseded_by', 'revoked_by',
              'conflicts_with', 'expires')
    SELECT = 'SELECT ' + ','.join(FIELDS) + ' FROM entries'

    def __init__(self, path, repo, fts=None):
        self.repo, self.path = repo, path
        self.expired_at = 0.0
        # Autocommit, with every transaction opened explicitly below. The driver starts an
        # implicit transaction only for INSERT, UPDATE, DELETE and REPLACE, so DDL ran in
        # autocommit however it was wrapped, and the schema stayed several transactions.
        self.db = sqlite3.connect(path, isolation_level=None)
        self.blocked = None
        try:
            # Exclusive locking is set first because it writes nothing itself and because a
            # read must not create a shared-memory file beside a database this runtime may
            # be about to refuse. Then the file is classified, reading only. `configure`
            # comes after, because setting the journal mode writes a database header and no
            # file may be modified before it has been judged.
            self.db.execute('PRAGMA locking_mode=EXCLUSIVE').fetchall()
            state = self.classify(repo)
            if state == 'initialised':
                self.inspect(repo)
            self.configure()
            # The schema and the metadata that identifies it are one transaction. Split
            # across two, an interruption between them left a store with tables and no
            # identity, which could only be read as belonging to another repository.
            if state != 'initialised':
                with self.transaction():
                    for statement in SCHEMA_STATEMENTS:
                        self.db.execute(statement)
                    for key, value in (('repo', repo), ('protocol', PROTOCOL),
                                       ('schema', SCHEMA), ('head', 0), ('floor', 0)):
                        self.set_meta(key, value)
            self.fts = False if fts is False else self._open_fts()
            self._reconcile_index()
        except sqlite3.Error as exc:
            self.db.close()
            if owned_elsewhere(exc):
                raise MemoryError_(
                    'store_busy',
                    'another owner holds this store. The service keeps one exclusive '
                    'connection so that its storage bound holds, so a second owner is '
                    'refused rather than admitted; reach it through the control socket. '
                    'Nothing was written') from exc
            raise
        except BaseException:
            # A constructor that raises must not leave the handle open; the suite
            # otherwise reports unclosed connections that hide real ones.
            self.db.close()
            raise

    # Applied at open, in this order, and each one read back and compared. A pragma that
    # is silently ignored is the failure mode this project has already met once: setting
    # the journal mode writes the database header, after which auto_vacuum and page_size
    # can no longer be chosen, and locking_mode must be exclusive before the log exists
    # for the wal-index to be held in memory rather than in a file.
    @staticmethod
    def pragmas():
        """Built on each call so the module constants stay authoritative.

        Holding this as a class attribute captured MAX_PAGES once, at import, so the
        ceiling the engine was given could silently differ from the constant the rest of
        the code compared against. That is the same drift this file guards against
        everywhere else, and it hid a real gap in the ceiling test.
        """
        return (('locking_mode', 'EXCLUSIVE', 'exclusive'),
                ('auto_vacuum', 'INCREMENTAL', 2),
                ('page_size', PAGE_SIZE, PAGE_SIZE),
                ('journal_mode', 'WAL', 'wal'),
                ('cache_spill', 'OFF', 0),
                ('temp_store', 'MEMORY', 2),
                ('max_page_count', MAX_PAGES, MAX_PAGES))

    def configure(self):
        """Apply the settings the storage bound depends on, then prove they took effect.

        Every setting here is load-bearing. Exclusive locking makes single ownership an
        engine property and removes the shared-memory file; no cache spill makes the
        commit list the only path to the log; memory temporaries keep the sub-journal off
        disk; and max_page_count is the durable ceiling, enforced by the engine rather
        than by a comparison this code makes.
        """
        self.require_temp_in_memory()
        settings = self.pragmas()
        for name, value, _ in settings:
            self.db.execute(f'PRAGMA {name}={value}').fetchall()
        for name, _, expected in settings:
            got = self.db.execute(f'PRAGMA {name}').fetchone()[0]
            if got == expected:
                continue
            if name == 'max_page_count':
                # The limit cannot shrink an existing database; asked to, it returns the
                # current count. An oversized store is refused with its data intact,
                # because deleting a caller's memory to fit a new ceiling is not recovery.
                raise MemoryError_(
                    'store_too_large',
                    f'this store holds {got} pages and the supported ceiling is '
                    f'{expected}; it was left untouched. Export what is needed from it '
                    'with an older runtime rather than truncating it')
            raise MemoryError_(
                'unsupported_runtime',
                f'PRAGMA {name} reads back as {got!r} after being set to {value!r}; the '
                f'storage bound requires {expected!r}. Nothing was written')

    def require_temp_in_memory(self):
        """Refuse a build on which the sub-journal cannot be kept out of the filesystem.

        SQLite decides this through `sqlite3TempInMemory`, which `sqlite3BtreeBeginTrans`
        passes to `sqlite3PagerBegin` as its `subjInMemory` argument; `openSubJournal`
        then opens the journal with a negative spill threshold, which keeps it in memory
        and off disk. That predicate honours `temp_store` only while SQLITE_TEMP_STORE is
        1, 2 or 3. Outside that range it returns false whatever the pragma says, and the
        sub-journal becomes an unbounded term in a directory this store does not account
        for. The build reports the value, so an unsupported one is refused rather than
        carried as an unmeasured cost.
        """
        setting = 1                     # sqliteInt.h defines 1 when nothing overrides it
        for (option,) in self.db.execute('PRAGMA compile_options'):
            if option.startswith('TEMP_STORE='):
                try:
                    setting = int(option.split('=', 1)[1])
                except ValueError:
                    setting = -1
        if setting not in (1, 2, 3):
            raise MemoryError_(
                'unsupported_runtime',
                f'this SQLite build reports TEMP_STORE={setting}, on which temp_store '
                'cannot keep sub-journals in memory, so their size is unbounded and '
                'unaccounted. Nothing was written')

    def pages(self):
        """Durable pages allocated. This is what admission compares, and nothing else."""
        return self.db.execute('PRAGMA page_count').fetchone()[0]

    @contextlib.contextmanager
    def transaction(self, control=True, blocking=True):
        """The single write boundary. Every durable change in this file goes through it.

        The bound is derived for ONE transaction beginning with an empty log, so each
        transaction must begin that way. Resetting once after several have accumulated
        would bound their sum, which is a different and larger quantity, so maintenance
        and initialisation are routed here too rather than committing on their own.

        The page check runs INSIDE the transaction. Running it after the context manager
        committed produced an error that said the change was rolled back when it had
        already been applied, which for a cursor or a snapshot is a false report about
        durable progress.
        """
        self.require_writable()
        self.reset_log()
        self.db.execute('BEGIN IMMEDIATE')
        try:
            yield
            self.enforce_pages(control)
            self.db.execute('COMMIT')
        except BaseException as exc:
            # The rollback comes first and unconditionally, so no path can leave a
            # transaction open. A failed COMMIT is rolled back here too.
            try:
                self.db.execute('ROLLBACK')
            except sqlite3.Error:
                pass
            if isinstance(exc, sqlite3.OperationalError) and storage_exhausted(exc):
                if control and blocking:
                    self.block(f'the engine refused a page at the {MAX_PAGES} page ceiling '
                               'during a transition the reserve exists to protect')
                    raise MemoryError_('storage_blocked', self.blocked) from exc
                raise MemoryError_(
                    'capacity',
                    f'the engine refused a page at the {MAX_PAGES} page ceiling; the '
                    'transaction was rolled back and stored data is intact') from exc
            raise

    def block(self, reason):
        """Record that writes cannot proceed until recovery is asked for explicitly.

        Without a recorded state the runtime told a caller that recovery was required and
        then accepted the next write as though nothing had happened. Reads, status and
        stop stay available; only writes are held.
        """
        self.blocked = (f'{reason}. Stored data is intact. Reads, status and stop remain '
                        'available; ask for recovery explicitly with the recover '
                        'operation before writes resume')

    def require_writable(self):
        if self.blocked:
            raise MemoryError_('storage_blocked', self.blocked)

    def recover(self):
        """The explicit path out of a blocked store. Reads never depended on it.

        Recovery is itself a write, so it obeys the same precondition it exists to restore:
        the log is reset and the result verified BEFORE anything is written. Reclaiming
        pages first would have written through an unreset log, which is the very state
        being recovered from, and swallowing the checkpoint's errors would have hidden the
        reason. The block is kept unless every postcondition holds.
        """
        try:
            busy, log_pages, residual = self.log_state()
        except (sqlite3.Error, OSError) as exc:
            self.block(f'recovery could not prove the log reset '
                       f'({type(exc).__name__}: {exc}); nothing was written')
            raise MemoryError_('storage_blocked', self.blocked) from exc
        if busy or log_pages or residual:
            self.block(f'recovery could not reset the write-ahead log (busy={busy}, '
                       f'log_pages={log_pages}, {residual} bytes remain). Nothing was '
                       'written')
            raise MemoryError_('storage_blocked', self.blocked)
        # Only now is a write permissible.
        try:
            self.db.execute('PRAGMA incremental_vacuum').fetchall()
            busy, log_pages, residual = self.log_state()
        except (sqlite3.Error, OSError) as exc:
            self.block(f'recovery could not reclaim pages ({type(exc).__name__}: {exc})')
            raise MemoryError_('storage_blocked', self.blocked) from exc
        if busy or log_pages or residual:
            self.block('recovery reclaimed pages but could not reset the log afterwards')
            raise MemoryError_('storage_blocked', self.blocked)
        self.blocked = None
        return dict(recovered=True, pages=self.pages(), blocked=None)

    @staticmethod
    def page_cap(control):
        """The one effective page limit, so admission and enforcement cannot disagree.

        Both must subtract the commit-time allocation. When only enforcement did, a store
        resting between the two figures admitted every append and rolled every one back.
        """
        return (MAX_PAGES if control else ORDINARY_MAX_PAGES) - COMMIT_SLACK

    def reset_log(self):
        """Return the log to zero before a write, and prove it rather than assume it.

        No single result establishes a reset. A passive checkpoint reports a clear busy
        flag while moving nothing, and `(0, 0, 0)` is equally what a store returns when no
        log has ever existed and when the log was already empty. The proof is therefore a
        conjunction: a clear busy flag, zero log pages, and a log file that is absent or
        empty. Under exclusive locking no other process can hold a snapshot, so a busy
        result is a genuine recovery condition rather than ordinary contention.
        """
        try:
            busy, log_pages, residual = self.log_state()
        except (sqlite3.Error, OSError) as exc:
            # A checkpoint or a stat that raises has not proved anything. Letting it
            # propagate left `blocked` unset, so once the condition cleared the next write
            # proceeded without anyone having asked for recovery.
            self.block(f'the write-ahead log could not be proved reset '
                       f'({type(exc).__name__}: {exc}). No write was attempted')
            raise MemoryError_('storage_blocked', self.blocked) from exc
        if busy or log_pages or residual:
            # The block is recorded here rather than by the caller. `transaction` resets
            # before its own try block, so a failure raised from here bypassed the handler
            # and the state the message promised was never retained: the next write was
            # then accepted as though the reset had succeeded.
            self.block(f'the write-ahead log could not be reset (busy={busy}, '
                       f'log_pages={log_pages}, {residual} bytes remain), so the bound on '
                       'a write cannot be held. No write was attempted')
            raise MemoryError_('storage_blocked', self.blocked)
        return True

    def log_state(self):
        """The checkpoint's three results, and the log file that no result describes."""
        busy, log_pages, _moved = self.db.execute(
            'PRAGMA wal_checkpoint(TRUNCATE)').fetchall()[0]
        try:
            residual = os.stat(str(self.path) + '-wal').st_size
        except FileNotFoundError:
            # Absence is proof that nothing is retained. Any other error -- a permission
            # or I/O failure -- is a failure to obtain the proof, not evidence of an empty
            # log, and it must not be read as one. It propagates to `reset_log`.
            residual = 0
        return busy, log_pages, residual

    def classify(self, repo):
        """Decide what this file is, reading only, before anything can write to it.

        This runs before `configure`, because the pragmas there write a database header and a
        file must not be modified before it has been judged. It answers one of three things,
        and refuses everything else:

        `empty`       nothing has been created yet, including the nonempty-but-tableless file
                      an initialisation that rolled back leaves behind.
        `unfinished`  exactly this schema, with no application data and no identity: this
                      runtime's own interrupted start, which may be completed.
        `initialised` an identity is recorded, so `inspect` judges whether it is ours.

        Names are not evidence. A definition that differs is a different table wearing a
        familiar name, and a prefix proves nothing at all -- `search_history` is not an FTS5
        shadow table. So shapes are compared, the shadow set is exact, and it is admitted only
        when the virtual table it belongs to is present and matches.
        """
        try:
            objects = self.db.execute(
                "SELECT type, name, sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"
            ).fetchall()
        except sqlite3.Error as exc:
            if owned_elsewhere(exc):
                raise MemoryError_(
                    'store_busy',
                    'another owner holds this store. The service keeps one exclusive '
                    'connection so that its storage bound holds, so a second reader is '
                    'refused rather than admitted; reach it through the control socket. '
                    'Nothing was written') from exc
            # A schema that cannot be read is an error, not an empty file. Treating it as
            # empty would initialise over whatever is actually there.
            raise MemoryError_(
                'incompatible_store',
                f'this file\'s schema could not be read ({type(exc).__name__}: {exc}), so it '
                'cannot be judged; it was left untouched') from exc
        if not objects:
            return 'empty'
        # The catalog is the only thing that proves a table absent. A failed read of a table
        # that exists proves nothing at all, and must never be reported as an empty identity
        # or as no data.
        present = {name for type_, name, _sql in objects if type_ == 'table'}
        if self.identity(present):
            return 'initialised'

        # No identity, so this may only be adopted if it is exactly an unfinished start.
        expected = {}
        for statement in SCHEMA_STATEMENTS:
            name = statement.split('EXISTS', 1)[1].split('(', 1)[0].split()[0]
            expected[name] = normalised_sql(statement)
        fts_present = any(name == FTS_TABLE and normalised_sql(sql) == normalised_sql(FTS_TABLE_SQL)
                          for _type, name, sql in objects)
        unknown, malformed = [], []
        for _type, name, sql in objects:
            if name == FTS_TABLE:
                if not fts_present:
                    malformed.append(name)
                continue
            if name in FTS_SHADOWS:
                # Admitted only alongside the virtual table that owns them.
                if not fts_present:
                    unknown.append(name)
                continue
            if name not in expected:
                unknown.append(name)
                continue
            if normalised_sql(sql) != expected[name]:
                malformed.append(name)
        if unknown:
            raise MemoryError_(
                'incompatible_store',
                f'this file holds objects this store does not own '
                f'({", ".join(sorted(unknown)[:4])}), so it belongs to another application; '
                'it was left untouched')
        if malformed:
            raise MemoryError_(
                'incompatible_store',
                f'this file defines {", ".join(sorted(malformed)[:4])} differently from this '
                'schema, so it is not an unfinished store of ours; it was left untouched')
        held = self.application_rows(present)
        if held:
            raise MemoryError_(
                'incompatible_store',
                f'this file holds data ({held}) but records no repository identity, so it '
                'cannot be adopted; it was left untouched')
        return 'unfinished'

    def identity(self, present):
        """The recorded identity rows, or an empty mapping only when none can exist.

        A missing metadata table and an empty one mean the same thing: nothing has been
        recorded. A metadata table that exists but cannot be read means something quite
        different, and returning an empty mapping for it let a store belonging to another
        repository be re-identified as this one, because an unreadable identity looked
        exactly like an absent one.
        """
        if 'meta' not in present:
            return {}
        try:
            return dict(self.db.execute(
                "SELECT key,value FROM meta WHERE key IN ('repo','schema','protocol')"
            ).fetchall())
        except sqlite3.Error as exc:
            raise MemoryError_(
                'incompatible_store',
                f'this file has a metadata table that could not be read '
                f'({type(exc).__name__}: {exc}), so its identity is unknown and it cannot be '
                'adopted; it was left untouched') from exc

    def application_rows(self, present):
        """A description of any stored data, or None only when there provably is none.

        Absence is established from the catalog, never from a failed read. Treating a read
        failure as an empty table let a file holding a saved entry be adopted, because the
        check that refuses identity-free data could not see the data.
        """
        for table in DATA_TABLES:
            if table not in present:
                continue
            try:
                count = self.db.execute(f'SELECT count(*) FROM {table}').fetchone()[0]
            except sqlite3.Error as exc:
                raise MemoryError_(
                    'incompatible_store',
                    f'this file has a {table} table that could not be read '
                    f'({type(exc).__name__}: {exc}), so whether it holds data is unknown and '
                    'it cannot be adopted; it was left untouched') from exc
            if count:
                return f'{count} row(s) in {table}'
        return None


    def inspect(self, repo):
        """Read-only compatibility check on an existing store. Writes nothing."""
        try:
            rows = dict(self.db.execute(
                "SELECT key,value FROM meta WHERE key IN ('repo','schema','protocol')").fetchall())
        except sqlite3.Error as exc:
            if owned_elsewhere(exc):
                raise MemoryError_(
                    'store_busy',
                    'another owner holds this store. The service keeps one exclusive '
                    'connection so that its storage bound holds, so a second reader is '
                    'refused rather than admitted; reach it through the control socket. '
                    'Nothing was written') from exc
            raise MemoryError_('incompatible_store',
                               f'this file is not a memory store ({type(exc).__name__}); it '
                               'was left untouched') from exc
        if rows.get('repo') != repo:
            raise MemoryError_('wrong_repository',
                               'state directory belongs to another repository; it was left '
                               'untouched')
        for key, current in (('schema', SCHEMA), ('protocol', PROTOCOL)):
            found = int(rows.get(key) or 0)
            if found > current:
                raise MemoryError_('schema_too_new',
                                   f'this store declares {key} {found}; this runtime supports '
                                   f'{current}. Upgrade the runtime rather than downgrading the '
                                   'store. Nothing was written')
            if found < current:
                raise MemoryError_('schema_too_old',
                                   f'this store declares {key} {found}; this runtime expects '
                                   f'{current} and has no migration for it. Nothing was written')

    # --- schema helpers -------------------------------------------------------

    def _open_fts(self):
        """Create the search table if this build has FTS5 and there is room for it.

        The index is optional, so neither a build without FTS5 nor a store too full to hold
        the table may stop the service starting. `transaction` translates an engine refusal
        into MemoryError_, so catching sqlite3.Error alone would have let that escape and
        fail the open.
        """
        try:
            with self.transaction(blocking=False):
                self.db.execute(
                    'CREATE VIRTUAL TABLE IF NOT EXISTS search USING fts5(body, content="")')
            return True
        except sqlite3.Error:
            return False
        except MemoryError_ as exc:
            if exc.code != 'capacity':
                raise
            return False

    def _reconcile_index(self):
        """Make the index either complete or explicitly unusable, never quietly wrong.

        Emptiness is the wrong test. A store can be written with the index, reopened by a
        runtime without it, written again, and reopened with it once more; the index is then
        nonempty and wrong, and a search silently loses results. Completeness is tracked
        explicitly, and anything short of it sends search to the complete scan.

        A rebuild that cannot fit must not stop the store opening. The index is marked
        invalid **before** the rebuild starts, so an interrupted or rolled-back rebuild
        leaves it invalid rather than apparently complete, and the service then serves
        scans. Refusing to open would be the worse failure: the fallback that exists for
        exactly this case could never be reached, because there would be no service.
        """
        if not self.fts:
            # Writes made now cannot be indexed, so the index stays unusable until a
            # runtime that has FTS rebuilds it.
            with self.transaction():
                self.set_meta('indexed_through', -1)
            return
        if int(self.meta('indexed_through') or 0) == self.head():
            return
        with self.transaction(blocking=False):
            self.set_meta('indexed_through', -1)
        if self.pages() > self.page_cap(control=True) - REBUILD_HEADROOM:
            # Refused before it starts rather than part way through. Search stays on the
            # scan until there is room, which reclamation can create.
            return
        try:
            with self.transaction(blocking=False):
                self.db.execute("INSERT INTO search(search) VALUES('delete-all')")
                for seq, body in self.db.execute(
                        'SELECT seq,body FROM entries ORDER BY seq').fetchall():
                    self.db.execute('INSERT INTO search(rowid,body) VALUES(?,?)', (seq, body))
                self.set_meta('indexed_through', self.head())
        except MemoryError_ as exc:
            if exc.code != 'capacity':
                raise
            # Rolled back, so the index is still marked invalid and search uses the scan.
            # This is a degraded service, not a broken one, and it is reachable.

    def _unindex(self, rows):
        """A contentless FTS5 table needs an explicit delete; dropping the row is not enough.

        Usability, not mere presence, is the test. While the store is serving scans the
        index does not hold rows written since it was invalidated, and issuing a delete for
        one of those is an instruction to remove a posting that was never added. The index
        is already known wrong, so there is nothing to maintain and nothing to gain.
        """
        if not self.index_usable():
            return
        for seq, body in rows:
            self.db.execute("INSERT INTO search(search,rowid,body) VALUES('delete',?,?)", (seq, body))

    def meta(self, key):
        row = self.db.execute('SELECT value FROM meta WHERE key=?', (key,)).fetchone()
        return row[0] if row else None

    def set_meta(self, key, value):
        self.db.execute('INSERT INTO meta(key,value) VALUES(?,?) '
                        'ON CONFLICT(key) DO UPDATE SET value=?', (key, str(value), str(value)))

    def head(self):
        """Durable monotonic event head. Reclamation never moves it backwards."""
        return int(self.meta('head') or 0)

    def floor(self):
        return int(self.meta('floor') or 0)

    def index_usable(self):
        """Is the search index present AND known to cover every live entry?

        Emptiness and error are the wrong tests. An index that exists but lags the head
        silently loses results, which is worse than being slow, so completeness is
        tracked explicitly and anything short of it sends search to the scan instead.
        """
        return bool(self.fts) and int(self.meta('indexed_through') or 0) == self.head()

    def healthy(self):
        try:
            self.db.execute('SELECT count(*) FROM meta').fetchone()
            self.db.execute('SELECT seq FROM entries ORDER BY seq DESC LIMIT 1').fetchone()
            return True
        except sqlite3.Error:
            return False

    def close(self):
        self.db.close()

    def row(self, r):
        return dict(zip(self.FIELDS, r))

    # --- capacity -------------------------------------------------------------

    def usage(self):
        """Account for what is actually stored, not for bodies alone.

        Variable fields are measured rather than approximated by a constant, and the
        physical figure includes the write-ahead log, which `page_count` excludes and
        which can be a large share of the file on disk.
        """
        count, body = self.db.execute(
            'SELECT count(*), coalesce(sum(length(cast(body AS BLOB))'
            '+length(cast(coalesce(path,"") AS BLOB))'
            '+length(cast(coalesce(author,"") AS BLOB))'
            '+length(cast(coalesce(scope_target,"") AS BLOB))'
            '+length(cast(coalesce(consumer,"") AS BLOB))),0) FROM entries').fetchone()
        frozen = self.db.execute('SELECT coalesce(sum(bytes),0) FROM snapshot_items').fetchone()[0]
        idem, idem_bytes = self.db.execute(
            'SELECT count(*), coalesce(sum(length(cast(key AS BLOB))+64),0) FROM idem').fetchone()
        readers = self.db.execute(
            'SELECT coalesce(sum(length(cast(consumer AS BLOB))+64),0) FROM cursors').fetchone()[0]
        retired = self.db.execute(
            'SELECT count(*), coalesce(sum(length(cast(consumer AS BLOB))+64),0) FROM retired'
        ).fetchone()
        shots = self.db.execute(
            'SELECT count(*), coalesce(sum(length(cast(id AS BLOB))'
            '+length(cast(consumer AS BLOB))+96),0) FROM snapshots').fetchone()
        logical = (body + count * ENTRY_OVERHEAD + frozen + idem_bytes + readers
                   + retired[1] + shots[1])
        page_size = self.db.execute('PRAGMA page_size').fetchone()[0]
        pages = self.db.execute('PRAGMA page_count').fetchone()[0]
        physical = page_size * pages
        for suffix in ('-wal', '-shm'):
            try:
                physical += os.stat(str(self.path) + suffix).st_size
            except OSError:
                pass
        return dict(entries=count, logical=logical, physical=physical, idem=idem,
                    retired=retired[0], snapshots=shots[0])

    def physical(self):
        """Bytes actually allocated, including the write-ahead log."""
        page_size = self.db.execute('PRAGMA page_size').fetchone()[0]
        total = page_size * self.db.execute('PRAGMA page_count').fetchone()[0]
        for suffix in ('-wal', '-shm'):
            try:
                total += os.stat(str(self.path) + suffix).st_size
            except OSError:
                pass
        return total

    def charge(self, **parts):
        """The single cost calculation for a transition, in the units usage() reports."""
        total = 0
        for kind, values in parts.items():
            if kind == 'entry':
                total += sum(measure(v or '') for v in values) + ENTRY_OVERHEAD
            elif kind == 'idem':
                # Matches what usage() attributes to an idempotency row: the scoped key
                # plus its metadata. A charge that differs from the measurement is not
                # accounting, it is two opinions.
                total += sum(measure(v or '') for v in values) + 64
            elif kind == 'reader':
                total += sum(measure(v or '') for v in values) + 64
            elif kind == 'tombstone':
                total += sum(measure(v or '') for v in values) + 64
            elif kind == 'snapshot':
                total += sum(measure(v or '') for v in values) + 96
            elif kind == 'frozen':
                # The same figure usage() sums from the stored `bytes` column. A charge
                # that differs from the measurement is not accounting, it is two opinions.
                total += sum(values)
        return total

    @contextlib.contextmanager
    def mutation(self, need=0, slots=0, control=False):
        """Run a durable change under admission and end-of-transaction enforcement.

        Appends, frozen snapshots and consumer registration pass through here, because
        a caller controls how much they grow the store. Progress transitions and index
        maintenance use `progress()` instead, and removal is outside admission entirely;
        both are documented in the contract rather than skipped silently.
        """
        self.admit(need, slots, control)
        with self.transaction(control):
            yield
            if need:
                self.enforce_logical(control)

    @contextlib.contextmanager
    def progress(self):
        """A transition that records progress and may never be refused for space.

        Acknowledgements, page issuance and activity refreshes cannot be rejected on
        capacity: a reader that cannot acknowledge can never advance, and the store would
        become unreadable-forward precisely when it most needs draining. They draw on the
        reserve, which ordinary appends may not consume, rather than on a margin.
        """
        with self.transaction(control=True):
            yield

    def enforce_logical(self, control):
        """The logical budget, checked inside the transaction like the page ceiling.

        Logical usage is what callers control, so it is measured after the change rather
        than projected from the request alone.
        """
        cap = MAX_LOGICAL_BYTES if control else MAX_LOGICAL_BYTES - RESERVED_BYTES
        logical = self.usage()['logical']
        if logical > cap:
            raise MemoryError_('capacity',
                               f'this mutation would leave {logical} logical bytes stored, '
                               f'above the {cap} byte limit; it was rolled back and stored '
                               'data is intact')

    def enforce_pages(self, control):
        """Check pages actually allocated, inside the transaction, so a breach rolls back.

        Projecting growth from payload bytes is not enforcement: SQLite allocates pages
        and maintains indexes by amounts the payload does not predict, and the cost of one
        row is a step function of its size rather than a curve. Measuring what was really
        allocated and raising is what makes the ceiling a limit rather than an estimate.

        Only the durable page count is compared. The log is excluded because it is reset
        before every write and bounded by the page ceiling, so including it would make the
        outcome depend on checkpoint timing and on how far a given SQLite build shrinks
        the file during recovery.

        A control or progress transition may draw on the reserve; an ordinary append may
        not, which is what keeps a withdrawal possible at a full store.
        """
        cap = self.page_cap(control)
        actual = self.pages()
        if actual > cap:
            raise MemoryError_('capacity',
                               f'this mutation would leave {actual} pages allocated, above the '
                               f'{cap} page limit; it was rolled back and stored data is intact')

    def admit(self, need, slots=0, control=False):
        """The single admission point for every durable mutation.

        Entries, frozen snapshot copies, consumer registrations and retirement
        tombstones all pass through here. A mutation that wrote around it would grow
        the store while the advertised budget said otherwise, which is what made the
        earlier budget a claim rather than a limit.

        A control mutation, meaning a revocation or a supersession, draws on reserved
        slots *and* reserved bytes. Reserving slots alone would let a full store admit
        the row and refuse the body, pinning a withdrawn directive permanently.
        """
        for attempt in (0, 1):
            use = self.usage()
            entry_cap = MAX_ENTRIES if control else MAX_ENTRIES - RESERVED_ENTRIES
            byte_cap = MAX_LOGICAL_BYTES if control else MAX_LOGICAL_BYTES - RESERVED_BYTES
            # The same effective limit enforcement uses, less the room one append can
            # need. The log is not in this comparison at all; the end-of-transaction check
            # is what catches growth a projection cannot predict.
            cap = self.page_cap(control) - (0 if control else APPEND_ALLOWANCE)
            pages = self.pages()
            if (use['entries'] + slots <= entry_cap and use['logical'] + need <= byte_cap
                    and pages <= cap):
                return use
            if attempt == 0:
                self.reclaim()
        raise MemoryError_('capacity', f"{use['entries']} entries, {use['logical']} logical bytes "
                           f'and {pages} pages are stored, and this mutation needs {need} more '
                           'bytes; nothing was written and stored data is intact')

    def expire(self):
        """Remove only what is past its stated lifetime, in bounded batches.

        This runs at request boundaries, not merely after a capacity failure. A horizon
        enforced only when the store fills is not a lifetime; it is a side effect of
        pressure, and a caller cannot reason about it.

        Batching is what keeps reclamation possible on a full store. A contentless FTS5
        delete writes a tombstone before the vacuum returns any pages, so removing
        everything at once would need room for index maintenance over the whole store, and
        the index cost of arbitrary legal content is not something this design can bound.
        If a batch's index maintenance will not fit, the index is invalidated and the rows
        are removed without it; search continues on the complete scan and the index is
        rebuilt when there is room.
        """
        now = time.time()
        removed = 0
        while True:
            batch = self.db.execute(
                'SELECT seq,body FROM entries WHERE expires IS NOT NULL AND expires < ? '
                'ORDER BY seq LIMIT ?', (now, EXPIRY_BATCH)).fetchall()
            if not batch:
                break
            try:
                # Non-blocking: index maintenance that meets the engine ceiling must reach
                # the fallback below, not block the store. Blocking here skipped the
                # invalidate-and-delete path at exactly the moment it was needed, because
                # the handler caught capacity while the engine raised a blocked store.
                with self.transaction(blocking=False):
                    self._unindex(batch)
                    self._forget(batch)
            except MemoryError_ as exc:
                if exc.code != 'capacity':
                    raise
                # The speculative attempt rolled back. Invalidate durably first, so a
                # failure between here and the delete leaves the index known-wrong rather
                # than silently short, then remove the rows without maintaining it.
                with self.transaction():
                    self.set_meta('indexed_through', -1)
                with self.transaction():
                    self._forget(batch)
            removed += len(batch)
        with self.transaction():
            self.db.execute('DELETE FROM snapshots WHERE (acked IS NULL AND created < ?) '
                            'OR (acked IS NOT NULL AND acked_at < ?)',
                            (now - SNAPSHOT_TTL, now - ACK_RETENTION))
            self.db.execute('DELETE FROM snapshot_items WHERE id NOT IN (SELECT id FROM snapshots)')
            # Retention follows the deadline the caller fixed, not a blanket age.
            self.db.execute('DELETE FROM idem WHERE deadline < ?', (now,))
            self.db.execute('DELETE FROM retired WHERE at < ?', (now - RETIRED_TTL,))
            # A retired consumer leaves a tombstone. A later request from it gets an
            # explicit consumer_retired result instead of silently becoming a new consumer
            # that re-reads the whole store as if it had never synced.
            for consumer, seq in self.db.execute(
                    'SELECT consumer,seq FROM cursors WHERE updated < ?',
                    (now - CONSUMER_TTL,)).fetchall():
                self.db.execute('INSERT OR REPLACE INTO retired(consumer,seq,at) VALUES(?,?,?)',
                                (consumer, seq, now))
                self.db.execute('DELETE FROM cursors WHERE consumer=?', (consumer,))
        self.expired_at = now
        return removed

    def _forget(self, batch):
        """Drop a batch of entries and move the recovery boundary past them.

        The boundary comes from what was removed. A boundary taken from the lowest
        surviving row cannot describe an interior or a tail gap, and a reader above it is
        then told there is more while receiving nothing, forever.
        """
        self.db.executemany('DELETE FROM entries WHERE seq=?', [(s,) for s, _ in batch])
        highest = max(s for s, _ in batch)
        if highest > self.floor():
            self.set_meta('floor', highest)
        if not self.fts:
            # Rows were removed without maintaining the index, so it is no longer
            # trustworthy and must be rebuilt when FTS returns.
            self.set_meta('indexed_through', -1)

    def maybe_expire(self):
        """Enforce lifetimes at the request boundary, at a bounded rate."""
        if time.time() - self.expired_at >= EXPIRY_INTERVAL:
            self.expire()

    def reclaim(self):
        """Expiry, then count pruning that respects every retention window.

        Count pruning may only remove records already outside their lifetime. Evicting a
        record still promised for recovery, such as an acknowledgement inside its retention,
        would turn a documented replay into a failure in order to make room.
        """
        removed = self.expire()
        now = time.time()
        with self.transaction():
            prunable = self.db.execute(
                'SELECT key FROM idem WHERE deadline < ? ORDER BY deadline LIMIT -1 OFFSET ?',
                (now, MAX_IDEM_ROWS)).fetchall()
            self.db.executemany('DELETE FROM idem WHERE key=?', prunable)
        # Deleting rows frees pages inside the file; without this the file never shrinks and
        # a store that reached its ceiling could never recover from it. The pragma must be
        # driven to completion: preparing it without stepping through its results leaves the
        # pages exactly where they were. It runs its own transaction, so the log is reset on
        # both sides of it.
        self.reset_log()
        self.db.execute('PRAGMA incremental_vacuum').fetchall()
        self.reset_log()
        return removed

    def note(self, consumer, kind, body, scope='repo', scope_target=None, path=None,
             supersedes=None, revokes=None, expires=None, key=None, deadline=None,
             author=None, pid=None):
        if kind not in TYPES:
            raise MemoryError_('invalid_request', f'unknown entry type: {kind}')
        if scope not in SCOPES:
            raise MemoryError_('invalid_request', f'unknown scope: {scope}')
        if scope != 'repo' and not scope_target:
            raise MemoryError_('invalid_request', f'scope {scope} requires a scope target')
        if not isinstance(body, str) or not body.strip():
            raise MemoryError_('invalid_request', 'body must be nonempty text')
        if measure(body) > MAX_BODY:
            raise MemoryError_('entry_too_large', f'body exceeds {MAX_BODY} bytes')
        # Refuse at admission what could never be delivered. A stored entry larger than
        # a page would block the snapshot page containing it, permanently.
        probe = len(encode(dict(zip(self.FIELDS, (0, 0.0, kind, scope, scope_target, path, body,
                                                  author, pid, consumer, 1, supersedes, revokes,
                                                  None, None, None, expires)))))
        if probe > FRAME_BUDGET:
            raise MemoryError_('entry_too_large',
                               f'this entry encodes to {probe} bytes, above the {FRAME_BUDGET} '
                               'byte page budget, so it could never be delivered')
        if supersedes and revokes:
            raise MemoryError_('invalid_request', 'an entry supersedes or revokes, never both')
        # The reported source is part of the stored content, so it belongs in the
        # fingerprint. The transport pid is not: the same write relayed by a different
        # process is the same write.
        payload = dict(type=kind, body=body, scope=scope, scope_target=scope_target, path=path,
                       supersedes=supersedes, revokes=revokes, expires=expires, author=author)
        mark = fingerprint(payload)
        scoped = f'{self.repo}\x00{consumer}\x00{key}' if key is not None else None
        if scoped:
            # The caller fixes this deadline before its first send and repeats it on every
            # retry. That is what lets a late retry be refused rather than appended;
            # reporting a horizon in a successful response cannot reach the caller that
            # never received one.
            if deadline is None:
                raise MemoryError_('invalid_request',
                                   'an idempotent write requires a retry deadline in absolute '
                                   'epoch seconds, chosen before the first send')
            now = time.time()
            if not isinstance(deadline, (int, float)) or not math.isfinite(deadline):
                raise MemoryError_('invalid_request',
                                   'a retry deadline must be a finite number of epoch seconds')
            if deadline > now + IDEM_TTL:
                raise MemoryError_('invalid_request',
                                   f'a retry deadline may not exceed {IDEM_TTL} seconds ahead, '
                                   'because deduplication state is not retained beyond that')
            # The clock decides expiry, not whether cleanup has run. Retained state can
            # outlive its deadline by up to one expiry interval, and a retry deduplicated
            # in that window would succeed past the boundary the caller was given.
            if deadline <= now:
                raise MemoryError_('retry_deadline_expired',
                                   'this retry deadline has passed, so deduplication can no longer '
                                   'be guaranteed; establish whether the earlier attempt landed, '
                                   'then resend with a new key and deadline')
            row = self.db.execute(
                'SELECT fingerprint,seq,deadline FROM idem WHERE key=?', (scoped,)).fetchone()
            if row:
                if row[0] != mark:
                    raise MemoryError_('idempotency_conflict',
                                       'this key is already used with different content')
                if row[2] != deadline:
                    # The deadline is part of the request's identity. Accepting a changed
                    # one would let a caller extend deduplication indefinitely while the
                    # service reported a horizon it never agreed to.
                    raise MemoryError_('idempotency_conflict',
                                       'this key was accepted with a different retry deadline; '
                                       'repeat the original deadline or use a new key')
                return dict(seq=row[1], duplicate=True, deadline=row[2])
        need = self.charge(entry=(body, path, author, scope_target, consumer),
                           **(dict(idem=(scoped,)) if scoped else {}))
        # One transaction under the shared chokepoint. A failure anywhere inside rolls the
        # whole write back, so a caller told the write failed never finds it committed by
        # a later request.
        try:
            with self.mutation(need, slots=1, control=bool(supersedes or revokes)):
                revision, target, conflict = 1, supersedes or revokes, None
                if target:
                    found = self.db.execute(
                        'SELECT revision,superseded_by,revoked_by FROM entries WHERE seq=?',
                        (target,)).fetchone()
                    if not found:
                        raise MemoryError_('no_such_entry', 'the replaced entry does not exist')
                    # A second replacement of the same target is a genuine conflict between
                    # two reporters. Both are retained and the conflict is reported. Refusing
                    # the later one would be first-writer-wins with the loser discarded, which
                    # is the silent loss the contract forbids.
                    conflict = found[1] or found[2]
                    revision = found[0] + 1
                seq = self.head() + 1
                self.set_meta('head', seq)
                self.db.execute(
                    'INSERT INTO entries(seq,ts,type,scope,scope_target,path,body,author,'
                    'author_pid,consumer,revision,supersedes,revokes,superseded_by,revoked_by,'
                    'conflicts_with,expires) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,NULL,?,?)',
                    (seq, time.time(), kind, scope, scope_target, path, body, author, pid,
                     consumer, revision, supersedes, revokes, conflict, expires))
                # The link lives on the replaced row, so reclaiming this event later
                # cannot resurrect what it replaced.
                # The first replacement owns the link. A competing one is live in its own
                # right and points at the replacement it competes with.
                if supersedes and not conflict:
                    self.db.execute('UPDATE entries SET superseded_by=? WHERE seq=?', (seq, supersedes))
                if revokes and not conflict:
                    self.db.execute('UPDATE entries SET revoked_by=? WHERE seq=?', (seq, revokes))
                if self.fts and int(self.meta('indexed_through') or 0) >= 0:
                    self.db.execute('INSERT INTO search(rowid,body) VALUES(?,?)', (seq, body))
                    self.set_meta('indexed_through', seq)
                if scoped:
                    held = self.db.execute('SELECT count(*) FROM idem').fetchone()[0]
                    if held >= MAX_IDEM_ROWS:
                        # Refuse rather than evict. Dropping an in-window key to make
                        # room would turn a safe retry into a silent duplicate.
                        raise MemoryError_('idem_capacity',
                                           f'{held} idempotency keys are retained and none is past '
                                           f'its {IDEM_TTL} second horizon')
                    self.db.execute(
                        'INSERT INTO idem(key,fingerprint,seq,ts,deadline) VALUES(?,?,?,?,?)',
                        (scoped, mark, seq, time.time(), deadline))
        except sqlite3.Error as exc:
            raise MemoryError_('write_failed', f'{type(exc).__name__}; nothing was written') from exc
        # State the retry horizon rather than leaving it implicit. A retry after this
        # many seconds is a new write, not a deduplicated one, and a caller that bounds
        # its own retries by this figure cannot append twice by accident.
        return dict(seq=seq, duplicate=False, conflicts_with=conflict,
                    deadline=deadline, idempotency_horizon=IDEM_TTL)

    # --- reads ----------------------------------------------------------------

    def live_clause(self, at=None):
        """Live as of an event horizon: not replaced at or below it, and not expired."""
        if at is None:
            return ('(superseded_by IS NULL AND revoked_by IS NULL '
                    'AND (expires IS NULL OR expires > ?))', [time.time()])
        return ('(seq<=? AND (superseded_by IS NULL OR superseded_by>?) '
                'AND (revoked_by IS NULL OR revoked_by>?) AND (expires IS NULL OR expires > ?))',
                [at, at, at, time.time()])

    def live(self, at=None):
        clause, args = self.live_clause(at)
        return [self.row(r) for r in
                self.db.execute(f'{self.SELECT} WHERE {clause} ORDER BY seq', args).fetchall()]

    def cursor(self, consumer):
        row = self.db.execute(
            'SELECT seq,issued,snapshot,bootstrapped,resnapshot FROM cursors WHERE consumer=?',
            (consumer,)).fetchone()
        return row if row else None


def bounded(items, budget=None):
    """Fill a page by encoded byte size. A single oversize item is an explicit error."""
    budget = FRAME_BUDGET if budget is None else budget
    out, used = [], 0
    for size, item in items:
        if not out and size > budget:
            raise MemoryError_('entry_too_large',
                               f'one entry encodes to {size} bytes, above the {budget} byte page '
                               'budget; it cannot be delivered')
        if used + size > budget:
            return out, True
        out.append(item)
        used += size
    return out, False


def freeze(store, consumer):
    """Copy a snapshot's members as immutable payloads against a fixed head.

    Copying rather than referencing is what makes pagination stable: a revocation
    or a reclamation after this point changes neither what a reader receives nor
    whether the reader can reach the end.
    """
    head = store.head()
    clause, args = store.live_clause(at=head)
    members = [store.row(r) for r in
               store.db.execute(f'{store.SELECT} WHERE {clause}', args).fetchall()]
    tails, ordered = {}, []
    for entry in sorted(members, key=lambda e: (SNAPSHOT_ORDER.get(e['type'], 9), -e['seq'])):
        cap = SNAPSHOT_TAIL.get(entry['type'])
        if cap is not None:
            tails[entry['type']] = tails.get(entry['type'], 0) + 1
            if tails[entry['type']] > cap:
                continue
        ordered.append(entry)
    held = store.db.execute(
        'SELECT count(*) FROM snapshots WHERE consumer=? AND ((acked IS NULL AND created >= ?) '
        'OR (acked IS NOT NULL AND acked_at >= ?))',
        (consumer, time.time() - SNAPSHOT_TTL, time.time() - ACK_RETENTION)).fetchone()[0]
    if held >= MAX_SNAPSHOTS_PER_CONSUMER:
        # Refuse rather than evict. Every snapshot still inside its window is a promise:
        # an unacknowledged one can still be paged, an acknowledged one can still replay.
        raise MemoryError_('snapshot_capacity',
                           f'this consumer already holds {held} snapshots within their retention; '
                           'acknowledge or abandon one before opening another')
    sid = uuid.uuid4().hex
    payloads = [(i, e, json.dumps(e, ensure_ascii=True)) for i, e in enumerate(ordered)]
    # A snapshot copies every member, so it is a durable mutation and is charged for.
    # Writing around admission is what let the store grow while the budget said
    # otherwise.
    sizes = {i: len(encode(e)) for i, e, _ in payloads}
    need = store.charge(frozen=list(sizes.values()), snapshot=(sid, consumer))
    with store.mutation(need):
        store.db.execute(
            'INSERT INTO snapshots(id,consumer,head,created,items,issued,acked,acked_at) '
            'VALUES(?,?,?,?,?,0,NULL,NULL)', (sid, consumer, head, time.time(), len(ordered)))
        store.db.executemany(
            'INSERT INTO snapshot_items(id,position,seq,payload,bytes) VALUES(?,?,?,?,?)',
            [(sid, i, e['seq'], p, sizes[i]) for i, e, p in payloads])
        store.db.execute('UPDATE cursors SET snapshot=?,updated=? WHERE consumer=?',
                         (sid, time.time(), consumer))
        # Bound retained snapshots per consumer, but only prune ones already outside
        # their retention. Removing an acknowledged snapshot still inside ACK_RETENTION
        # would turn a documented replay into a failure in order to save space.
        now = time.time()
        old = store.db.execute(
            'SELECT id FROM snapshots WHERE consumer=? AND ((acked IS NULL AND created < ?) '
            'OR (acked IS NOT NULL AND acked_at < ?)) ORDER BY created DESC LIMIT -1 OFFSET ?',
            (consumer, now - SNAPSHOT_TTL, now - ACK_RETENTION, MAX_SNAPSHOTS_PER_CONSUMER)
        ).fetchall()
        store.db.executemany('DELETE FROM snapshots WHERE id=?', old)
        store.db.executemany('DELETE FROM snapshot_items WHERE id=?', old)
    return sid


class Service:
    def __init__(self, root, repo, store):
        self.root, self.repo, self.store = Path(root), repo, store
        self.stop = asyncio.Event()
        self.generation = uuid.uuid4().hex
        self.active = 0
        self.tasks = set()
        self.idle = asyncio.Event()
        self.idle.set()

    # --- connection handling --------------------------------------------------

    async def handle(self, reader, writer):
        if self.active >= 16:
            writer.close()
            return
        self.active += 1
        self.idle.clear()
        task = asyncio.current_task()
        self.tasks.add(task)
        try:
            pid = credentials(writer.get_extra_info('socket'))
            async with asyncio.timeout(10):
                request = json.loads(await reader.readline())
                reply = dict(ok=True, result=self.command(request, pid))
                if CRASH_AFTER_COMMIT and request.get('op') == 'note':
                    os._exit(70)
                if REPLY_DELAY and request.get('op') == 'note':
                    await asyncio.sleep(REPLY_DELAY)
        except MemoryError_ as exc:
            reply = dict(ok=False, code=exc.code, error=str(exc))
        except (ValueError, KeyError, TypeError, OSError, TimeoutError, sqlite3.Error) as exc:
            reply = dict(ok=False, code='rejected', error=type(exc).__name__)
        try:
            writer.write(encode(reply))
            await asyncio.wait_for(writer.drain(), 10)
        except (OSError, ValueError, TimeoutError, asyncio.TimeoutError):
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass
            self.tasks.discard(task)
            self.active -= 1
            if not self.active:
                self.idle.set()

    def consumer(self, r):
        """Stable consumer identity, supplied by the caller, never trusted as authority.

        A PID cannot serve: a CLI invocation has a fresh one per command, so a
        PID-keyed cursor would restart on every call. The key is recorded as
        reported data, exactly like any other provenance field.
        """
        key = r.get('consumer')
        if not isinstance(key, str) or not key.strip() or len(key) > 128:
            raise MemoryError_('invalid_request', 'a stable consumer key is required')
        return key.strip()

    # Operations that must stay reachable when writes cannot proceed. Running cleanup
    # before them made a failed cleanup block the very reads, status and stop that the
    # blocked-store error promises remain available.
    READ_ONLY = ('hello', 'status', 'recall', 'stop', 'recover')

    def command(self, r, pid):
        op = r.get('op')
        if op not in self.READ_ONLY:
            self.store.maybe_expire()
        if op == 'recover':
            return self.store.recover()
        if op == 'hello':
            return dict(service='codex-peer-memory', repo=self.repo, protocol=PROTOCOL,
                        schema=SCHEMA, generation=self.generation, pid=os.getpid(),
                        healthy=self.store.healthy(), fts=self.store.fts,
                        indexed=self.store.index_usable(), blocked=self.store.blocked)
        if op == 'note':
            return self.store.note(self.consumer(r), r.get('type'), r.get('body'),
                                   scope=r.get('scope', 'repo'), scope_target=r.get('scope_target'),
                                   path=r.get('path'), supersedes=r.get('supersedes'),
                                   revokes=r.get('revokes'), expires=r.get('expires'),
                                   key=r.get('key'), deadline=r.get('deadline'),
                                   author=r.get('author'), pid=pid)
        if op == 'sync':
            return self.sync(r)
        if op == 'ack':
            return self.ack(r)
        if op == 'recall':
            return self.recall(r)
        if op == 'status':
            return self.status(r)
        if op == 'stop':
            # Validate the target here, at the point of action. Deciding which instance
            # to stop from a record read beforehand is a race: a successor can replace
            # the endpoint between that read and this connection, and an unqualified
            # request would then stop the wrong service.
            if r.get('repo') != self.repo:
                raise MemoryError_('wrong_repository',
                                   'this service serves another repository')
            if r.get('generation') != self.generation:
                raise MemoryError_('not_this_instance',
                                   'this endpoint is served by a different instance than the one '
                                   'you asked to stop; it is still running')
            self.stop.set()
            return dict(stopping=True, generation=self.generation)
        raise MemoryError_('invalid_request', f'unknown operation: {op}')

    # --- protocol state -------------------------------------------------------

    def register(self, consumer):
        """Return this consumer's row, refusing a retired one with a recovery result."""
        row = self.store.cursor(consumer)
        if row:
            return row
        retired = self.store.db.execute('SELECT seq,at FROM retired WHERE consumer=?',
                                        (consumer,)).fetchone()
        if retired:
            raise MemoryError_('consumer_retired',
                               f'this consumer was retired after {CONSUMER_TTL} seconds idle at '
                               f'cursor {retired[0]}; re-register under a new consumer key, which '
                               'will resync from a snapshot')
        live = self.store.db.execute('SELECT count(*) FROM cursors').fetchone()[0]
        graves = self.store.db.execute('SELECT count(*) FROM retired').fetchone()[0]
        if live >= MAX_CONSUMERS or live + graves >= MAX_CONSUMERS + MAX_RETIRED:
            # Registration is bounded by live consumers *and* by the tombstones they will
            # become. Bounding the tombstones by eviction instead would silently turn a
            # returning retired consumer into a new one, contradicting the retention this
            # service promises and the explicit result it owes that caller.
            raise MemoryError_('capacity',
                               f'{live} consumers and {graves} retirement records are held, at the '
                               f'limit of {MAX_CONSUMERS} and {MAX_RETIRED}')
        # The tombstone charge is an admission buffer, not stored usage: it holds room
        # for the record this consumer becomes when it is retired, so retirement can never
        # be refused later. usage() reports a tombstone only once one exists.
        need = self.store.charge(reader=(consumer,), tombstone=(consumer,))
        with self.store.mutation(need):
            self.store.db.execute(
                'INSERT INTO cursors(consumer,seq,issued,snapshot,bootstrapped,resnapshot,'
                'updated) VALUES(?,0,0,NULL,0,0,?)', (consumer, time.time()))
        return (0, 0, None, 0, 0)

    def snapshot_state(self, sid):
        return self.store.db.execute(
            'SELECT head,items,issued,acked,acked_at,created,consumer FROM snapshots WHERE id=?',
            (sid,)).fetchone()

    def touch(self, consumer):
        """Record activity on every request, so an active poller is never retired."""
        with self.store.progress():
            self.store.db.execute('UPDATE cursors SET updated=? WHERE consumer=?',
                                  (time.time(), consumer))

    def sync(self, r):
        """Return work to do. This never advances the cursor; `ack` does that."""
        consumer = self.consumer(r)
        seq, issued, snapshot, bootstrapped, resnapshot = self.register(consumer)
        self.touch(consumer)
        if snapshot:
            state = self.snapshot_state(snapshot)
            unusable = (not state or state[6] != consumer
                        or (not state[3] and time.time() - state[5] > SNAPSHOT_TTL))
            if unusable:
                # Clearing an unusable snapshot must not also clear the obligation that
                # put this consumer into snapshot mode. Without the flag, a bootstrapped
                # reader whose cursor happens to sit above the floor would silently fall
                # through to deltas and skip everything the snapshot would have carried.
                with self.store.progress():
                    self.store.db.execute(
                        'UPDATE cursors SET snapshot=NULL,resnapshot=1 WHERE consumer=?',
                        (consumer,))
                snapshot, resnapshot = None, 1
        if snapshot or resnapshot or not bootstrapped or seq < self.store.floor():
            sid = snapshot or freeze(self.store, consumer)
            return self.snapshot_page(consumer, sid, r)
        rows = self.store.db.execute(f'{self.store.SELECT} WHERE seq>? ORDER BY seq LIMIT 500',
                                     (seq,)).fetchall()
        entries, more = bounded((len(encode(self.store.row(x))), self.store.row(x)) for x in rows)
        end = entries[-1]['seq'] if entries else seq
        head = self.store.head()
        if end > issued:
            with self.store.progress():
                self.store.db.execute('UPDATE cursors SET issued=? WHERE consumer=?',
                                      (end, consumer))
        return dict(kind='delta', entries=entries, cursor=seq, next_cursor=end,
                    head=head, more=more or end < head)

    def snapshot_page(self, consumer, sid, r):
        state = self.snapshot_state(sid)
        if not state:
            raise MemoryError_('snapshot_expired', 'restart sync without a page token')
        head, items, issued, acked, _, created, owner = state
        if owner != consumer:
            raise MemoryError_('foreign_snapshot', 'that snapshot belongs to another consumer')
        if not acked and time.time() - created > SNAPSHOT_TTL:
            raise MemoryError_('snapshot_expired', 'restart sync without a page token')
        token = int(r.get('page_token', 0))
        claimed = r.get('snapshot_id')
        # Continuation must name the snapshot it continues. A bare offset could be
        # applied to whatever snapshot happens to be open, which is the caller
        # supplying protocol state again.
        if token and claimed is None:
            raise MemoryError_('stale_page_token',
                               'continuing a snapshot requires its snapshot_id')
        if claimed is not None and claimed != sid:
            # Name the real reason. A token for another consumer's snapshot is an
            # ownership error, not merely a stale one, and the caller needs to know
            # which before it retries.
            other = self.snapshot_state(claimed)
            if other and other[6] != consumer:
                raise MemoryError_('foreign_snapshot', 'that snapshot belongs to another consumer')
            raise MemoryError_('stale_page_token',
                               'this page token belongs to a different snapshot; restart sync')
        if token > issued:
            raise MemoryError_('stale_page_token',
                               f'pages are issued in order; {issued} were issued')
        rows = self.store.db.execute(
            'SELECT position,payload,bytes FROM snapshot_items WHERE id=? AND position>=? '
            'ORDER BY position', (sid, token)).fetchall()
        page, more = bounded((size, json.loads(payload)) for _, payload, size in rows)
        nxt = token + len(page)
        if nxt > issued:
            with self.store.progress():
                self.store.db.execute('UPDATE snapshots SET issued=? WHERE id=?', (nxt, sid))
        return dict(kind='snapshot', snapshot_id=sid, head=head, entries=page, page_token=nxt,
                    total=items, more=more or nxt < items)

    def ack(self, r):
        """Advance a cursor. Completion is recorded here, never inferred from the caller."""
        consumer = self.consumer(r)
        seq, issued, snapshot, bootstrapped, resnapshot = self.register(consumer)
        self.touch(consumer)
        sid = r.get('snapshot_id')
        if sid:
            state = self.snapshot_state(sid)
            if not state:
                raise MemoryError_('snapshot_expired', 'restart sync without a page token')
            head, items, given, acked, acked_at, created, owner = state
            if owner != consumer:
                raise MemoryError_('foreign_snapshot', 'that snapshot belongs to another consumer')
            if acked:
                # A retained acknowledgement replays for its own consumer. A lost
                # response must not turn a success into a failure on retry.
                return dict(cursor=head, snapshot=sid, complete=True, replayed=True)
            if sid != snapshot:
                raise MemoryError_('stale_snapshot',
                                   'that snapshot is not this consumer\'s open snapshot')
            if time.time() - created > SNAPSHOT_TTL:
                raise MemoryError_('snapshot_expired', 'restart sync without a page token')
            if given < items:
                raise MemoryError_('snapshot_incomplete',
                                   f'{given} of {items} pages were issued; page to the end before '
                                   'acknowledging')
            with self.store.progress():
                self.store.db.execute('UPDATE snapshots SET acked=1,acked_at=? WHERE id=?',
                                      (time.time(), sid))
                self.store.db.execute(
                    'UPDATE cursors SET seq=?,issued=?,snapshot=NULL,bootstrapped=1,resnapshot=0 '
                    'WHERE consumer=?', (head, max(head, issued), consumer))
            return dict(cursor=head, snapshot=sid, complete=True, replayed=False)
        # A numeric acknowledgement can neither bootstrap a consumer nor slip past an
        # open snapshot; both would skip everything the snapshot was carrying.
        if snapshot or resnapshot:
            raise MemoryError_('snapshot_open',
                               'acknowledge the open snapshot by its snapshot_id first')
        if not bootstrapped:
            raise MemoryError_('not_bootstrapped',
                               'complete a first snapshot before acknowledging a sequence')
        through = int(r.get('through', 0))
        if through < seq:
            return dict(cursor=seq, ignored='not monotonic')
        if through > issued:
            raise MemoryError_('not_issued', f'cannot acknowledge {through}; {issued} was issued')
        with self.store.progress():
            self.store.db.execute('UPDATE cursors SET seq=? WHERE consumer=?', (through, consumer))
        return dict(cursor=through)

    def recall(self, r):
        """Search live entries only, with real continuation and a truthful `more`."""
        term = r.get('query')
        if not isinstance(term, str) or not term.strip():
            raise MemoryError_('invalid_request', 'query must be nonempty text')
        before = int(r.get('before') or 0)
        clause, args = self.store.live_clause()
        window = [before or (1 << 62), ROW_WINDOW + 1]
        # One path is chosen, on whether the index can be trusted, and never on whether
        # a query happened to match nothing. Falling back when the index returns no rows
        # conflates "no such entry" with "index unusable" and answers the two differently
        # for the same store, so a caller cannot tell which it received.
        indexed = self.store.index_usable()
        if indexed:
            try:
                rows = self.store.db.execute(
                    f'{self.store.SELECT} WHERE seq IN (SELECT rowid FROM search WHERE search MATCH ?)'
                    f' AND {clause} AND seq < ? ORDER BY seq DESC LIMIT ?',
                    [term] + args + window).fetchall()
            except sqlite3.Error:
                # The index answered with an error, so it cannot be trusted for this
                # reply either. Scan rather than report a short result as complete.
                indexed, rows = False, []
        if not indexed:
            # The complete fallback. It is slower and it matches substrings rather than
            # tokens, so the reply says which path produced it.
            pattern = '%' + term.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_') + '%'
            rows = self.store.db.execute(
                f"{self.store.SELECT} WHERE body LIKE ? ESCAPE '\\' AND {clause} AND seq < ? "
                'ORDER BY seq DESC LIMIT ?', [pattern] + args + window).fetchall()
        beyond = len(rows) > ROW_WINDOW
        rows = rows[:ROW_WINDOW]
        entries, truncated = bounded((len(encode(self.store.row(x))), self.store.row(x))
                                     for x in rows)
        # `more` accounts for both limits: the byte budget and the row window. Reporting
        # only the first would hide results behind a false ending.
        more = truncated or (beyond and len(entries) == len(rows))
        return dict(entries=entries, more=more, indexed=indexed,
                    next_before=entries[-1]['seq'] if entries and more else None)

    def status(self, r=None):
        r = r or {}
        use = self.store.usage()
        head = self.store.head()
        after = r.get('after') or ''
        rows = self.store.db.execute(
            'SELECT consumer,seq,snapshot,bootstrapped FROM cursors WHERE consumer > ? '
            'ORDER BY consumer LIMIT ?', (after, ROW_WINDOW + 1)).fetchall()
        beyond = len(rows) > ROW_WINDOW
        rows = rows[:ROW_WINDOW]
        listed = [dict(consumer=c, cursor=s, lag=head - s, snapshot=snap, bootstrapped=bool(b))
                  for c, s, snap, b in rows]
        # Measure each record rather than assuming a size; an assumed figure is how a
        # response grows past its frame while the count still looks safe.
        consumers, truncated = bounded((len(encode(x)), x) for x in listed)
        more = truncated or (beyond and len(consumers) == len(listed))
        return dict(repo=self.repo, protocol=PROTOCOL, schema=SCHEMA, generation=self.generation,
                    head=head, floor=self.store.floor(), healthy=self.store.healthy(),
                    fts=self.store.fts, usage=use,
                    # A blocked store still answers status; that is the point of blocking
                    # writes rather than failing the service, and a caller needs to see it.
                    blocked=self.store.blocked, indexed=self.store.index_usable(),
                    pages=self.store.pages(),
                    limits=dict(entries=MAX_ENTRIES, logical_bytes=MAX_LOGICAL_BYTES,
                                physical_bytes=MAX_PHYSICAL_BYTES, body=MAX_BODY,
                                max_pages=MAX_PAGES, ordinary_max_pages=ORDINARY_MAX_PAGES,
                                reserve_pages=RESERVE_PAGES, wal_budget_bytes=WAL_BUDGET_BYTES,
                                reserved_entries=RESERVED_ENTRIES, reserved_bytes=RESERVED_BYTES,
                                page_bytes=FRAME_BUDGET, consumers=MAX_CONSUMERS),
                    lifetimes=dict(snapshot=SNAPSHOT_TTL, acknowledgement=ACK_RETENTION,
                                   idempotency=IDEM_TTL, consumer=CONSUMER_TTL,
                                   retired=RETIRED_TTL, expiry_interval=EXPIRY_INTERVAL),
                    consumers=consumers, more=more,
                    next_after=consumers[-1]['consumer'] if consumers and more else None)

    async def run(self, sock):
        server = await asyncio.start_unix_server(self.handle, sock=sock, limit=LIMIT)
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self.stop.set)
            except (NotImplementedError, ValueError):
                pass
        print(json.dumps(self.status()), flush=True)
        try:
            await self.stop.wait()
        finally:
            server.close()
            await server.wait_closed()
            # Drain in flight work before the database goes away, so a request that was
            # accepted is never answered from a closed store. A timeout alone is not
            # enough: a handler still running when the store closed would fail on a
            # dead connection, so anything left is cancelled and joined first.
            try:
                await asyncio.wait_for(self.idle.wait(), 10)
            except (asyncio.TimeoutError, TimeoutError):
                for task in list(self.tasks):
                    task.cancel()
                await asyncio.gather(*list(self.tasks), return_exceptions=True)
            self.store.close()


def write_owner(home, sock_path, generation, repo):
    """Durable ownership record, so a stale socket can be recovered safely.

    Without it, a socket left by an unclean exit can never be distinguished from a
    live listener whose accept queue is full, and nothing may remove it.
    """
    pid = os.getpid()
    record = dict(pid=pid, proc_start=platform_support.proc_start(pid), generation=generation,
                  repo=repo, protocol=PROTOCOL, socket=str(sock_path))
    temp = Path(home) / f'owner.json.tmp.{uuid.uuid4().hex}'
    try:
        with temp.open('x') as f:
            json.dump(record, f)
            f.flush()
            os.fsync(f.fileno())
        temp.replace(Path(home) / 'owner.json')
    finally:
        temp.unlink(missing_ok=True)
    return record


def read_owner(home):
    try:
        value = json.loads((Path(home) / 'owner.json').read_text())
        return value if isinstance(value, dict) else None
    except (OSError, ValueError):
        return None


def owner_is_dead(owner):
    """True only when the recorded owner is provably gone.

    A missing record, an unreadable start marker, or a live matching process all
    return False. Removing a socket on anything less would race a healthy service.
    """
    if not owner or not isinstance(owner.get('pid'), int):
        return False
    pid = owner['pid']
    if not platform_support.process_alive(pid):
        return True
    try:
        return not platform_support.same_process(owner.get('proc_start'),
                                                 platform_support.proc_start(pid))
    except (ProcessLookupError, OSError, subprocess.SubprocessError):
        return False


async def request(root, payload, timeout=10):
    try:
        reply, _pid = await control_exchange(Path(root), payload, timeout)
        return reply
    except UnsafeServiceEndpoint as exc:
        raise MemoryError_('unsafe_service_endpoint', str(exc)) from None
    except NoControlReply:
        raise MemoryError_('no_reply', 'the service closed the connection without replying') from None


async def verify_running(root, repo):
    """Confirm the listener is this repository's healthy service before reusing it.

    A bind conflict plus any answer proves only that something listens. Reuse
    requires the service name, repository key, protocol, health, and agreement
    between the answer and the durable ownership record.
    """
    try:
        reply, connected_pid = await control_exchange(Path(root), dict(op='hello'), timeout=5)
    except UnsafeServiceEndpoint as exc:
        raise MemoryError_('unsafe_service_endpoint', str(exc)) from None
    except (ConnectionRefusedError, FileNotFoundError, OSError, ValueError, TimeoutError):
        return None
    result = reply.get('result') if reply.get('ok') else None
    if not isinstance(result, dict) or result.get('service') != 'codex-peer-memory':
        return None
    if (result.get('repo') != repo or type(result.get('protocol')) is not int or
            result['protocol'] != PROTOCOL):
        raise MemoryError_('foreign_service', 'another service holds this socket; refusing to reuse it')
    if not result.get('healthy'):
        raise MemoryError_('unhealthy_service', 'the running service reports an unhealthy store')
    # A listener with no ownership record is not evidence of a healthy service; it is
    # evidence that something is listening. Absence must refuse, not accept.
    owner = read_owner(root)
    control = platform_support.control_socket_path(Path(root))
    if not owner:
        raise MemoryError_('unknown_owner',
                           'a service is listening with no ownership record; stop it explicitly '
                           'before reusing this state directory')
    if (type(owner.get('pid')) is not int or type(result.get('pid')) is not int or
            owner['pid'] != connected_pid or result['pid'] != connected_pid):
        raise MemoryError_('ownership_mismatch', 'the connected process does not match the recorded service')
    if type(owner.get('protocol')) is not int:
        raise MemoryError_('ownership_mismatch', 'the recorded protocol is invalid')
    generation = owner.get('generation')
    if (not isinstance(generation, str) or len(generation) != 32 or
            any(c not in '0123456789abcdef' for c in generation)):
        raise MemoryError_('ownership_mismatch', 'the service has no valid recorded generation')
    marker = owner.get('proc_start')
    if not isinstance(marker, str) or not marker.strip():
        raise MemoryError_('ownership_mismatch', 'the service has no recorded process-start marker')
    checks = (('repo', owner.get('repo'), repo),
              ('protocol', owner.get('protocol'), PROTOCOL),
              ('socket', owner.get('socket'), str(control)),
              ('pid', owner.get('pid'), result.get('pid')),
              ('generation', owner.get('generation'), result.get('generation')))
    for field, recorded, expected in checks:
        if recorded != expected:
            raise MemoryError_('ownership_mismatch',
                               f'the recorded owner disagrees with the running service on {field}; '
                               'stop it explicitly before reusing this state directory')
    try:
        live = platform_support.proc_start(owner['pid'])
    except (ProcessLookupError, OSError, subprocess.SubprocessError):
        raise MemoryError_('ownership_mismatch', 'the recorded owner is no longer readable') from None
    if not platform_support.same_process(owner.get('proc_start'), live):
        raise MemoryError_('ownership_mismatch',
                           'the recorded owner pid belongs to a different process now')
    return result


def bind_exclusive(home, repo, generation):
    """Bind the control socket, recovering only from a provably dead owner."""
    control = platform_support.control_socket_path(Path(home))
    private_dir(control.parent)
    for attempt in (0, 1):
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            sock.bind(str(control))
        except OSError as exc:
            sock.close()
            owner = read_owner(home)
            recoverable = (owner and owner.get('socket') == str(control)
                           and owner.get('repo') == repo
                           and owner.get('protocol') == PROTOCOL and owner_is_dead(owner))
            if attempt == 0 and recoverable:
                # The recorded owner is provably gone, so this socket is a leftover.
                # Nothing here removes a socket on a failed probe alone.
                control.unlink(missing_ok=True)
                continue
            raise MemoryError_('socket_in_use',
                               f'cannot bind {control}: {exc}. Its recorded owner is not proven '
                               'dead, so it was left in place') from exc
        sock.listen(16)
        sock.setblocking(False)
        os.chmod(control, 0o600)
        write_owner(home, control, generation, repo)
        return sock, control
    raise MemoryError_('socket_in_use', f'cannot bind {control}')


def start(home, repo, store_factory):
    """Serialized start. Check and bind happen under one lock, never as a race."""
    private_dir(Path(home))
    with (Path(home) / 'start.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        existing = asyncio.run(verify_running(home, repo))
        if existing:
            return None, existing
        store = store_factory()
        service = Service(home, repo, store)
        try:
            sock, control = bind_exclusive(home, repo, service.generation)
        except BaseException:
            store.close()
            raise
        return (service, sock, control), None


def serve(home, repo, store_factory):
    started, existing = start(home, repo, store_factory)
    if existing:
        return dict(status='already_running', **existing)
    service, sock, control = started
    try:
        asyncio.run(service.run(sock))
    finally:
        sock.close()
        release(home, control, service.generation)
    return dict(status='stopped')


def release(home, control, generation):
    """Remove the endpoint and the record as one serialized ownership operation.

    Checking the generation and then unlinking two files separately is a race: a
    successor can bind and publish its own record between the two unlinks, and the
    predecessor's second unlink then deletes it. Publication and removal therefore share
    `start.lock`, and this critical section performs no waiting at all, so it cannot
    participate in the wait cycle that invariant 4 forbids.

    A missing record is not permission to delete. Absence means another process has
    already taken over the bookkeeping, so cleanup declines.
    """
    with (Path(home) / 'start.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        owner = read_owner(home)
        if not owner or owner.get('generation') != generation:
            return False
        control.unlink(missing_ok=True)
        (Path(home) / 'owner.json').unlink(missing_ok=True)
        return True


def stop_service(home, repo, timeout=20):
    """Stop one specific service instance and report only when it has gone.

    Completion is bound to the generation that was asked to stop. A refused connection
    is not evidence of exit: a service that has closed its listener and is still
    draining refuses connections while very much alive, so waiting on a failed handshake
    would report success while work was still in flight. The wait therefore watches the
    ownership record and the endpoint, and it holds no lock, because the exit path takes
    `start.lock` to remove them and a caller holding it would be waiting on a process
    waiting on the caller.
    """
    control = platform_support.control_socket_path(Path(home))
    target = read_owner(home)
    if not target:
        return dict(status='not_running', residue=control.exists())
    if target.get('repo') != repo:
        raise MemoryError_('wrong_repository',
                           'the recorded owner serves another repository; refusing to stop it')
    generation = target.get('generation')
    try:
        reply = asyncio.run(request(home, dict(op='stop', repo=repo, generation=generation),
                                    timeout=5))
        if not reply.get('ok') and reply.get('code') == 'not_this_instance':
            # A successor already owns the endpoint, so the instance we meant to stop is
            # gone and the one now running must be left alone.
            return dict(status='stopped', generation=generation, superseded=True)
    except (ConnectionRefusedError, FileNotFoundError, OSError, ValueError, TimeoutError):
        # It may already be draining or gone. The wait below decides, not this call.
        pass
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        owner = read_owner(home)
        if owner is None:
            return dict(status='stopped', generation=generation, residue=control.exists())
        if owner.get('generation') != generation:
            # A successor already owns this endpoint, so the instance we asked to stop
            # has gone and must not be confused with the one now running.
            return dict(status='stopped', generation=generation, superseded=True)
        if owner_is_dead(owner):
            break
        time.sleep(.05)
    with (Path(home) / 'start.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        owner = read_owner(home)
        if not owner or owner.get('generation') != generation:
            return dict(status='stopped', generation=generation)
        if owner_is_dead(owner) and owner.get('socket') == str(control):
            control.unlink(missing_ok=True)
            (Path(home) / 'owner.json').unlink(missing_ok=True)
            return dict(status='stopped', generation=generation, residue=True, removed=True)
        return dict(status='stop_requested', generation=generation, residue=True, removed=False,
                    detail='the service has not exited within the timeout and is not proven dead')


def main():
    os.umask(0o077)
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--state-dir', default=str(Path(os.environ.get(
        'XDG_STATE_HOME', str(Path.home() / '.local/state'))) / 'codex-peer-bridge'))
    p.add_argument('--repo-path', default=os.getcwd())
    p.add_argument('--consumer', help='stable consumer key; required for note, sync and ack')
    sub = p.add_subparsers(dest='op', required=True)
    for op in ('serve', 'stop', 'recover'):
        sub.add_parser(op)
    n = sub.add_parser('note')
    n.add_argument('body')
    n.add_argument('--type', choices=TYPES, default='finding')
    n.add_argument('--scope', choices=SCOPES, default='repo')
    n.add_argument('--scope-target', help='task or session this entry applies to')
    n.add_argument('--path')
    n.add_argument('--author', help='reported source; recorded as provenance, never as authority')
    n.add_argument('--expires', type=float, help='absolute epoch seconds after which this lapses')
    n.add_argument('--key', help='idempotency key, scoped to repository and consumer')
    n.add_argument('--deadline', type=float,
                   help='absolute epoch seconds until which a retry of this key deduplicates; '
                        'choose it before the first send and repeat it on every retry')
    n.add_argument('--supersedes', type=int)
    n.add_argument('--revokes', type=int)
    s = sub.add_parser('sync')
    s.add_argument('--snapshot-id', help='required when continuing with --page-token')
    s.add_argument('--page-token', type=int, default=0)
    a = sub.add_parser('ack')
    a.add_argument('--through', type=int, default=0,
                   help='next_cursor from a delta sync')
    a.add_argument('--snapshot-id', help='snapshot_id from a snapshot sync, once fully paged')
    q = sub.add_parser('recall')
    q.add_argument('query')
    q.add_argument('--before', type=int, help='continue from next_before of a previous page')
    st = sub.add_parser('status')
    st.add_argument('--after', help='continue from next_after of a previous page')
    args = vars(p.parse_args())
    root = Path(args.pop('state_dir')).absolute()
    repo = repo_identity(args.pop('repo_path'))
    private_dir(root)
    home = state_dir(root, repo)
    private_dir(home)
    op = args.pop('op')
    if op == 'serve':
        print(json.dumps(serve(home, repo, lambda: Store(home / 'memory.sqlite3', repo))))
        return
    if op == 'stop':
        print(json.dumps(stop_service(home, repo), indent=2))
        return
    if op in ('note', 'sync', 'ack') and not args.get('consumer'):
        raise SystemExit(f'{op} requires --consumer, a stable key that outlives one command')
    payload = dict(op=op, **{k: v for k, v in args.items() if v is not None})
    try:
        reply = asyncio.run(request(home, payload))
    except (ConnectionRefusedError, FileNotFoundError) as exc:
        raise SystemExit(f'no memory service is running for this repository ({home})') from exc
    print(json.dumps(reply, indent=2))
    raise SystemExit(0 if reply.get('ok') else 1)


if __name__ == '__main__':
    main()
