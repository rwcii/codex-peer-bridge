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
import fcntl
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import sqlite3
import subprocess
import time
import uuid

from bridge import LIMIT, credentials, encode, private_dir
import platform_support

PROTOCOL = 1
SCHEMA = 2
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

# Lifetimes. Every retained record has one, and expiry returns a defined recovery
# result rather than silently changing a caller's meaning.
SNAPSHOT_TTL = 3600
ACK_RETENTION = 86400
IDEM_TTL = 86400
CONSUMER_TTL = 30 * 86400
MAX_SNAPSHOTS_PER_CONSUMER = 4
MAX_CONSUMERS = 256
MAX_IDEM_ROWS = 20000

# Response framing. A page is bounded by encoded bytes, not by a row count, because
# 200 rows of maximum body cannot fit one frame.
FRAME_BUDGET = LIMIT - 8192

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
        self.repo = repo
        self.db = sqlite3.connect(path)
        self.db.execute('PRAGMA journal_mode=WAL')
        self.db.executescript('''
        CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT);
        CREATE TABLE IF NOT EXISTS entries(
            seq INTEGER PRIMARY KEY, ts REAL, type TEXT, scope TEXT, scope_target TEXT, path TEXT,
            body TEXT, author TEXT, author_pid INTEGER, consumer TEXT, revision INTEGER,
            supersedes INTEGER, revokes INTEGER, superseded_by INTEGER, revoked_by INTEGER,
            conflicts_with INTEGER, expires REAL);
        CREATE TABLE IF NOT EXISTS idem(key TEXT PRIMARY KEY, fingerprint TEXT, seq INTEGER, ts REAL);
        CREATE TABLE IF NOT EXISTS cursors(
            consumer TEXT PRIMARY KEY, seq INTEGER, issued INTEGER, snapshot TEXT,
            bootstrapped INTEGER, resnapshot INTEGER, updated REAL);
        CREATE TABLE IF NOT EXISTS retired(consumer TEXT PRIMARY KEY, seq INTEGER, at REAL);
        CREATE TABLE IF NOT EXISTS snapshots(
            id TEXT PRIMARY KEY, consumer TEXT, head INTEGER, created REAL, items INTEGER,
            issued INTEGER, acked INTEGER, acked_at REAL);
        CREATE TABLE IF NOT EXISTS snapshot_items(
            id TEXT, position INTEGER, seq INTEGER, payload TEXT, bytes INTEGER,
            PRIMARY KEY(id, position));
        CREATE INDEX IF NOT EXISTS entries_live ON entries(superseded_by, revoked_by, expires);
        ''')
        stored = self.meta('repo')
        if stored is None:
            with self.db:
                for key, value in (('repo', repo), ('protocol', str(PROTOCOL)),
                                   ('schema', str(SCHEMA)), ('head', '0'), ('floor', '0')):
                    self.set_meta(key, value)
        elif stored != repo:
            raise MemoryError_('wrong_repository', 'state directory belongs to another repository')
        else:
            # Validate what the file declares before writing to it. A store written by a
            # newer schema or protocol must not be opened and silently half-understood.
            for key, current in (('schema', SCHEMA), ('protocol', PROTOCOL)):
                found = int(self.meta(key) or 0)
                if found > current:
                    raise MemoryError_('schema_too_new',
                                       f'this store declares {key} {found}; this runtime supports '
                                       f'{current}. Upgrade the runtime rather than downgrading '
                                       'the store')
                if found < current:
                    raise MemoryError_('schema_too_old',
                                       f'this store declares {key} {found}; this runtime expects '
                                       f'{current} and has no migration for it')
        self.fts = False if fts is False else self._open_fts()
        self._reconcile_index()

    # --- schema helpers -------------------------------------------------------

    def _open_fts(self):
        try:
            self.db.execute('CREATE VIRTUAL TABLE IF NOT EXISTS search USING fts5(body, content="")')
            return True
        except sqlite3.Error:
            return False

    def _reconcile_index(self):
        """Rebuild the index whenever it is not known to be complete.

        Emptiness is the wrong test. A store can be written with the index, reopened by
        a runtime without it, written again, and reopened with it once more; the index
        is then nonempty and wrong, and a search silently loses results. Completeness is
        tracked explicitly and a mismatch rebuilds in one transaction.
        """
        if not self.fts:
            # Writes made now cannot be indexed, so mark the index unusable until a
            # runtime that has FTS rebuilds it.
            self.set_meta('indexed_through', -1)
            self.db.commit()
            return
        through = int(self.meta('indexed_through') or 0)
        if through == self.head():
            return
        with self.db:
            self.db.execute("INSERT INTO search(search) VALUES('delete-all')")
            for seq, body in self.db.execute('SELECT seq,body FROM entries ORDER BY seq'):
                self.db.execute('INSERT INTO search(rowid,body) VALUES(?,?)', (seq, body))
            self.set_meta('indexed_through', self.head())

    def _unindex(self, rows):
        """A contentless FTS5 table needs an explicit delete; dropping the row is not enough."""
        if not self.fts:
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
        count, body = self.db.execute(
            'SELECT count(*), coalesce(sum(length(cast(body AS BLOB))),0) FROM entries').fetchone()
        frozen = self.db.execute('SELECT coalesce(sum(bytes),0) FROM snapshot_items').fetchone()[0]
        idem = self.db.execute('SELECT count(*) FROM idem').fetchone()[0]
        logical = body + count * ENTRY_OVERHEAD + frozen + idem * 128
        page_size = self.db.execute('PRAGMA page_size').fetchone()[0]
        pages = self.db.execute('PRAGMA page_count').fetchone()[0]
        return dict(entries=count, logical=logical, physical=page_size * pages, idem=idem)

    def check_capacity(self, body, control):
        """Refuse an over-budget write explicitly, leaving stored data untouched.

        A control write, meaning a revocation or supersession, draws on reserved
        slots and reserved bytes. Without the byte reservation a full store could
        accept the slot and still refuse the body, pinning a withdrawn directive.
        """
        need = measure(body) + ENTRY_OVERHEAD
        for attempt in (0, 1):
            use = self.usage()
            entry_cap = MAX_ENTRIES if control else MAX_ENTRIES - RESERVED_ENTRIES
            byte_cap = MAX_LOGICAL_BYTES if control else MAX_LOGICAL_BYTES - RESERVED_BYTES
            if use['entries'] < entry_cap and use['logical'] + need <= byte_cap \
                    and use['physical'] < MAX_PHYSICAL_BYTES:
                return
            if attempt == 0:
                self.reclaim()
        raise MemoryError_('capacity', f"{use['entries']} entries, {use['logical']} logical bytes and "
                           f"{use['physical']} physical bytes are stored; nothing was written and "
                           'stored data is intact')

    def reclaim(self):
        """Bounded cleanup. Every removal here has a defined recovery result.

        Reclaiming an event can leave a reader below the retained range, so the
        floor moves with it and a reader below the floor is sent to a fresh
        snapshot rather than handed a silent gap.
        """
        now = time.time()
        with self.db:
            expired = self.db.execute(
                'SELECT seq,body FROM entries WHERE expires IS NOT NULL AND expires < ?',
                (now,)).fetchall()
            if expired:
                self._unindex(expired)
                self.db.executemany('DELETE FROM entries WHERE seq=?', [(s,) for s, _ in expired])
                # The recovery boundary comes from what was removed. A boundary taken
                # from the lowest surviving row cannot describe an interior or a tail
                # gap, and a reader above it is then told there is more while receiving
                # nothing, forever.
                highest = max(s for s, _ in expired)
                if highest > self.floor():
                    self.set_meta('floor', highest)
                if not self.fts:
                    # Rows were removed without maintaining the index, so it is no
                    # longer trustworthy and must be rebuilt when FTS returns.
                    self.set_meta('indexed_through', -1)
            # Open snapshots expire; acknowledged ones are retained longer so a lost
            # response can be recovered by replaying the same acknowledgement.
            self.db.execute('DELETE FROM snapshots WHERE (acked IS NULL AND created < ?) '
                            'OR (acked IS NOT NULL AND acked_at < ?)',
                            (now - SNAPSHOT_TTL, now - ACK_RETENTION))
            self.db.execute('DELETE FROM snapshot_items WHERE id NOT IN (SELECT id FROM snapshots)')
            self.db.execute('DELETE FROM idem WHERE ts < ?', (now - IDEM_TTL,))
            excess = self.db.execute('SELECT key FROM idem ORDER BY ts DESC LIMIT -1 OFFSET ?',
                                     (MAX_IDEM_ROWS,)).fetchall()
            self.db.executemany('DELETE FROM idem WHERE key=?', excess)
            # A retired consumer leaves a tombstone. A later request from it gets an
            # explicit consumer_retired result instead of silently becoming a new
            # consumer that re-reads the whole store as if it had never synced.
            stale = self.db.execute('SELECT consumer,seq FROM cursors WHERE updated < ?',
                                    (now - CONSUMER_TTL,)).fetchall()
            for consumer, seq in stale:
                self.db.execute('INSERT OR REPLACE INTO retired(consumer,seq,at) VALUES(?,?,?)',
                                (consumer, seq, now))
                self.db.execute('DELETE FROM cursors WHERE consumer=?', (consumer,))
        return len(expired)

    # --- writes ---------------------------------------------------------------

    def note(self, consumer, kind, body, scope='repo', scope_target=None, path=None,
             supersedes=None, revokes=None, expires=None, key=None, author=None, pid=None):
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
        if supersedes and revokes:
            raise MemoryError_('invalid_request', 'an entry supersedes or revokes, never both')
        payload = dict(type=kind, body=body, scope=scope, scope_target=scope_target, path=path,
                       supersedes=supersedes, revokes=revokes, expires=expires)
        mark = fingerprint(payload)
        scoped = f'{self.repo}\x00{consumer}\x00{key}' if key is not None else None
        if scoped:
            row = self.db.execute('SELECT fingerprint,seq FROM idem WHERE key=?', (scoped,)).fetchone()
            if row:
                if row[0] != mark:
                    raise MemoryError_('idempotency_conflict',
                                       'this key is already used with different content')
                return dict(seq=row[1], duplicate=True)
        self.check_capacity(body, control=bool(supersedes or revokes))
        # One transaction. A failure anywhere inside rolls the whole write back, so a
        # caller told the write failed never finds it committed by a later request.
        try:
            with self.db:
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
                    self.db.execute('INSERT INTO idem(key,fingerprint,seq,ts) VALUES(?,?,?,?)',
                                    (scoped, mark, seq, time.time()))
        except sqlite3.Error as exc:
            raise MemoryError_('write_failed', f'{type(exc).__name__}; nothing was written') from exc
        return dict(seq=seq, duplicate=False, conflicts_with=conflict)

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
    sid = uuid.uuid4().hex
    with store.db:
        store.db.execute(
            'INSERT INTO snapshots(id,consumer,head,created,items,issued,acked,acked_at) '
            'VALUES(?,?,?,?,?,0,NULL,NULL)', (sid, consumer, head, time.time(), len(ordered)))
        store.db.executemany(
            'INSERT INTO snapshot_items(id,position,seq,payload,bytes) VALUES(?,?,?,?,?)',
            [(sid, i, e['seq'], json.dumps(e, ensure_ascii=True), len(encode(e)))
             for i, e in enumerate(ordered)])
        store.db.execute('UPDATE cursors SET snapshot=?,updated=? WHERE consumer=?',
                         (sid, time.time(), consumer))
        # Bound retained snapshots per consumer so an abandoning reader cannot grow
        # the store without limit.
        old = store.db.execute(
            'SELECT id FROM snapshots WHERE consumer=? ORDER BY created DESC LIMIT -1 OFFSET ?',
            (consumer, MAX_SNAPSHOTS_PER_CONSUMER)).fetchall()
        store.db.executemany('DELETE FROM snapshots WHERE id=?', old)
        store.db.executemany('DELETE FROM snapshot_items WHERE id=?', old)
    return sid


class Service:
    def __init__(self, root, repo, store):
        self.root, self.repo, self.store = Path(root), repo, store
        self.stop = asyncio.Event()
        self.generation = uuid.uuid4().hex
        self.active = 0
        self.idle = asyncio.Event()
        self.idle.set()

    # --- connection handling --------------------------------------------------

    async def handle(self, reader, writer):
        if self.active >= 16:
            writer.close()
            return
        self.active += 1
        self.idle.clear()
        try:
            pid = credentials(writer.get_extra_info('socket'))
            async with asyncio.timeout(10):
                request = json.loads(await reader.readline())
                reply = dict(ok=True, result=self.command(request, pid))
        except MemoryError_ as exc:
            reply = dict(ok=False, code=exc.code, error=str(exc))
        except (ValueError, KeyError, TypeError, OSError, TimeoutError, sqlite3.Error) as exc:
            reply = dict(ok=False, code='rejected', error=type(exc).__name__)
        try:
            writer.write(encode(reply))
            await writer.drain()
        except (OSError, ValueError):
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass
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

    def command(self, r, pid):
        op = r.get('op')
        if op == 'hello':
            return dict(service='codex-peer-memory', repo=self.repo, protocol=PROTOCOL,
                        schema=SCHEMA, generation=self.generation, pid=os.getpid(),
                        healthy=self.store.healthy(), fts=self.store.fts)
        if op == 'note':
            return self.store.note(self.consumer(r), r.get('type'), r.get('body'),
                                   scope=r.get('scope', 'repo'), scope_target=r.get('scope_target'),
                                   path=r.get('path'), supersedes=r.get('supersedes'),
                                   revokes=r.get('revokes'), expires=r.get('expires'),
                                   key=r.get('key'), author=r.get('author'), pid=pid)
        if op == 'sync':
            return self.sync(r)
        if op == 'ack':
            return self.ack(r)
        if op == 'recall':
            return self.recall(r)
        if op == 'status':
            return self.status()
        if op == 'stop':
            self.stop.set()
            return 'stopping'
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
        with self.store.db:
            if self.store.db.execute('SELECT count(*) FROM cursors').fetchone()[0] >= MAX_CONSUMERS:
                raise MemoryError_('capacity', f'{MAX_CONSUMERS} consumers are registered')
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
        with self.store.db:
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
                with self.store.db:
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
            with self.store.db:
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
            with self.store.db:
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
            with self.store.db:
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
        with self.store.db:
            self.store.db.execute('UPDATE cursors SET seq=? WHERE consumer=?', (through, consumer))
        return dict(cursor=through)

    def recall(self, r):
        """Search live entries only, using the same liveness rule as sync."""
        term = r.get('query')
        if not isinstance(term, str) or not term.strip():
            raise MemoryError_('invalid_request', 'query must be nonempty text')
        clause, args = self.store.live_clause()
        rows = []
        if self.store.fts:
            try:
                rows = self.store.db.execute(
                    f'{self.store.SELECT} WHERE seq IN (SELECT rowid FROM search WHERE search MATCH ?)'
                    f' AND {clause} ORDER BY seq DESC LIMIT 500', [term] + args).fetchall()
            except sqlite3.Error:
                rows = []
        if not rows:
            pattern = '%' + term.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_') + '%'
            rows = self.store.db.execute(
                f"{self.store.SELECT} WHERE body LIKE ? ESCAPE '\\' AND {clause} "
                'ORDER BY seq DESC LIMIT 500', [pattern] + args).fetchall()
        entries, more = bounded((len(encode(self.store.row(x))), self.store.row(x)) for x in rows)
        return dict(entries=entries, more=more)

    def status(self):
        use = self.store.usage()
        head = self.store.head()
        consumers, _ = bounded(
            (256, dict(consumer=c, cursor=s, lag=head - s, snapshot=snap, bootstrapped=bool(b)))
            for c, s, snap, b in self.store.db.execute(
                'SELECT consumer,seq,snapshot,bootstrapped FROM cursors ORDER BY consumer LIMIT 200'))
        return dict(repo=self.repo, protocol=PROTOCOL, schema=SCHEMA, generation=self.generation,
                    head=head, floor=self.store.floor(), healthy=self.store.healthy(),
                    fts=self.store.fts, usage=use,
                    limits=dict(entries=MAX_ENTRIES, logical_bytes=MAX_LOGICAL_BYTES,
                                physical_bytes=MAX_PHYSICAL_BYTES, body=MAX_BODY,
                                reserved_entries=RESERVED_ENTRIES, reserved_bytes=RESERVED_BYTES,
                                page_bytes=FRAME_BUDGET, consumers=MAX_CONSUMERS),
                    lifetimes=dict(snapshot=SNAPSHOT_TTL, acknowledgement=ACK_RETENTION,
                                   idempotency=IDEM_TTL, consumer=CONSUMER_TTL),
                    consumers=consumers)

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
            # Drain in flight work before the database goes away, so a request that
            # was accepted is never answered from a closed store.
            try:
                await asyncio.wait_for(self.idle.wait(), 10)
            except asyncio.TimeoutError:
                pass
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
        return json.loads((Path(home) / 'owner.json').read_text())
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
    control = platform_support.control_socket_path(Path(root))
    r, w = await asyncio.open_unix_connection(str(control), limit=LIMIT)
    try:
        credentials(w.get_extra_info('socket'))
        w.write(encode(payload))
        await w.drain()
        line = await asyncio.wait_for(r.readline(), timeout)
        if not line:
            raise MemoryError_('no_reply', 'the service closed the connection without replying')
        return json.loads(line)
    finally:
        w.close()
        try:
            await w.wait_closed()
        except OSError:
            pass


async def verify_running(root, repo):
    """Confirm the listener is this repository's healthy service before reusing it.

    A bind conflict plus any answer proves only that something listens. Reuse
    requires the service name, repository key, protocol, health, and agreement
    between the answer and the durable ownership record.
    """
    try:
        reply = await request(root, dict(op='hello'), timeout=5)
    except (ConnectionRefusedError, FileNotFoundError, OSError, ValueError, TimeoutError):
        return None
    result = reply.get('result') if reply.get('ok') else None
    if not isinstance(result, dict) or result.get('service') != 'codex-peer-memory':
        return None
    if result.get('repo') != repo or result.get('protocol') != PROTOCOL:
        raise MemoryError_('foreign_service', 'another service holds this socket; refusing to reuse it')
    if not result.get('healthy'):
        raise MemoryError_('unhealthy_service', 'the running service reports an unhealthy store')
    owner = read_owner(root)
    if owner and (owner.get('pid') != result.get('pid')
                  or owner.get('generation') != result.get('generation')):
        raise MemoryError_('ownership_mismatch',
                           'the running service does not match the recorded owner; stop it '
                           'explicitly before reusing this state directory')
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
            if attempt == 0 and owner_is_dead(owner) and owner.get('socket') == str(control):
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
        control.unlink(missing_ok=True)
        (Path(home) / 'owner.json').unlink(missing_ok=True)
    return dict(status='stopped')


def main():
    os.umask(0o077)
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--state-dir', default=str(Path(os.environ.get(
        'XDG_STATE_HOME', str(Path.home() / '.local/state'))) / 'codex-peer-bridge'))
    p.add_argument('--repo-path', default=os.getcwd())
    p.add_argument('--consumer', help='stable consumer key; required for note, sync and ack')
    sub = p.add_subparsers(dest='op', required=True)
    for op in ('serve', 'status', 'stop'):
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
    n.add_argument('--supersedes', type=int)
    n.add_argument('--revokes', type=int)
    s = sub.add_parser('sync')
    s.add_argument('--snapshot-id')
    s.add_argument('--page-token', type=int, default=0)
    a = sub.add_parser('ack')
    a.add_argument('--through', type=int, default=0)
    a.add_argument('--snapshot-id')
    q = sub.add_parser('recall')
    q.add_argument('query')
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
