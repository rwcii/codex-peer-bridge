"""Single-owner bounded notification accounting, separate from the source inbox.

Provider calls are outside this module. A caller may send only after reserve()
commits. A cancelled caller does not prove that its accepted mutation rolled back.
Migration markers and bridge activation gate opening this store for delivery.
"""
from contextlib import contextmanager
import json
import os
import stat
from pathlib import Path
import sqlite3

from inbox_schema import MAX_SEQUENCE, hex_value

SCHEMA = 1
PAGE_SIZE = 4096
MAX_PAGES = 1024
MAX_WORK = 2048
MAX_MEMBERS = 10
MAX_ATTEMPTS = 3
MAX_DATABASE_BYTES = PAGE_SIZE * MAX_PAGES
MAX_WAL_BYTES = 32 + (MAX_PAGES + 16) * (PAGE_SIZE + 24)
COUNTERS = ('delivered', 'acknowledged', 'obsolete', 'failed', 'unknown', 'missing', 'attempts', 'ignored')
META_KEYS = ('schema', 'provider', 'namespace', 'target_digest', 'nonce', 'imported_through',
             'scan_through', 'enumerated_through', 'pointers_seeded', 'activation_confirmed', 'history_lost',
             'history_acknowledged', 'counters')
STATES = ('pending', 'reserved', 'delivered', 'failed', 'unknown', 'acknowledged', 'obsolete')
DDL = (
    "CREATE TABLE meta(key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL CHECK(length(CAST(value AS BLOB))<=512))",
    "CREATE TABLE work(seq INTEGER PRIMARY KEY CHECK(seq>0), kind TEXT NOT NULL CHECK(kind IN ('peer','memory-pointer')), binding TEXT, binding_instance TEXT, disposition TEXT NOT NULL CHECK(disposition IN ('pending','reserved','delivered','failed','unknown','acknowledged','obsolete')), attempts INTEGER NOT NULL CHECK(attempts BETWEEN 0 AND 3), retry_at INTEGER NOT NULL CHECK(retry_at>=0), uncertain INTEGER NOT NULL CHECK(uncertain IN (0,1)))",
    "CREATE TABLE attempt(id INTEGER PRIMARY KEY CHECK(id=1), started INTEGER NOT NULL CHECK(started>=0))",
    "CREATE TABLE attempt_member(attempt_id INTEGER NOT NULL REFERENCES attempt(id) ON DELETE CASCADE, seq INTEGER PRIMARY KEY REFERENCES work(seq) ON DELETE CASCADE)",
)


# Recovery is a control/health policy, not an inferred SQLite failure. Only actual
# storage conditions and internal API defects latch the database worker fault.
ERROR_POLICY = {
    'journal_activation_mismatch': ('operator_action', None),
    'journal_attempt_in_progress': ('retry', None),
    'journal_capacity': ('capacity', None),
    'journal_checkpoint_failed': ('retry', 'storage_error'),
    'journal_identity_invalid': ('internal_error', 'internal_error'),
    'journal_identity_mismatch': ('operator_action', None),
    'journal_invalid_activation': ('internal_error', 'internal_error'),
    'journal_invalid_capability': ('internal_error', 'internal_error'),
    'journal_invalid_counter': ('internal_error', 'internal_error'),
    'journal_invalid_outcome': ('internal_error', 'internal_error'),
    'journal_invalid_reservation': ('internal_error', 'internal_error'),
    # Explicit retry is an operator request. Its invalid row selection is not a bug.
    'journal_invalid_retry': ('invalid_request', None),
    'journal_no_attempt': ('internal_error', 'internal_error'),
    'journal_not_ready': ('internal_error', 'internal_error'),
    'journal_preserve_existing_files': ('operator_action', None),
    'journal_rebuild_refused': ('operator_action', None),
    'journal_recovery_required': ('operator_action', None),
    'journal_source_changed': ('operator_action', None),
    'journal_source_missing': ('operator_action', None),
    'journal_unsafe_state': ('operator_action', None),
    'journal_unsupported_runtime': ('operator_action', None),
    'journal_upgrade_required': ('operator_action', None),
}


class JournalError(ValueError):
    def __init__(self, code):
        self.recovery, self.database_fault = ERROR_POLICY[code]
        self.code = code
        super().__init__(code)


def integer(value, minimum=0):
    if type(value) is not int or not minimum <= value <= MAX_SEQUENCE:
        raise JournalError('journal_invalid_counter')
    return value


def identity(provider, target_digest, nonce, through, history_lost=False):
    if (provider not in ('codex', 'deepseek') or not hex_value(target_digest, 64)
            or not hex_value(nonce, 32) or type(history_lost) is not bool):
        raise JournalError('journal_identity_invalid')
    integer(through)
    return dict(schema=SCHEMA, provider=provider, namespace='account-local',
                target_digest=target_digest, nonce=nonce, imported_through=through,
                scan_through=through, enumerated_through=through, pointers_seeded=False, activation_confirmed=False,
                history_lost=history_lost, history_acknowledged=False,
                counters=dict.fromkeys(COUNTERS, 0))


class Journal:
    def __init__(self, path, expected, *, create=False, bootstrap=False):
        self.path = Path(path)
        self.db = None
        wanted = identity(expected['provider'], expected['target_digest'], expected['nonce'],
                          expected['imported_through'], expected['history_lost'])
        try:
            self.check_file_sizes()
            if create:
                try:
                    fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_CLOEXEC, 0o600)
                except FileExistsError:
                    pass
                else:
                    os.close(fd)
            self.db = sqlite3.connect(self.path.absolute().as_uri() + '?mode=rw',
                                     uri=True, isolation_level=None, timeout=.1)
            # Establish exclusive WAL access before the first schema read. A crash
            # can leave a WAL; reading it first in NORMAL mode creates a disk wal-index.
            self.db.execute('PRAGMA locking_mode=EXCLUSIVE').fetchall()
            catalog = self.db.execute(
                "SELECT sql FROM sqlite_master WHERE name NOT LIKE 'sqlite_%' ORDER BY name"
            ).fetchall()
            if catalog:
                if sorted(row[0] for row in catalog) != sorted(DDL):
                    raise JournalError('journal_recovery_required')
                state = self.validate()
                for key in ('schema', 'provider', 'namespace', 'target_digest', 'nonce',
                            'imported_through', 'history_lost'):
                    if state[key] != wanted[key]:
                        raise JournalError('journal_identity_mismatch')
                if bootstrap and (self.rows() or self.db.execute('SELECT count(*) FROM attempt').fetchone()[0]
                                  or any(state['counters'].values()) or state['pointers_seeded']
                                  or state['activation_confirmed'] or state['history_acknowledged']
                                  or state['scan_through'] != state['imported_through']
                                  or state['enumerated_through'] != state['imported_through']):
                    raise JournalError('journal_recovery_required')
            elif not create:
                raise JournalError('journal_recovery_required')
            self.configure()
            if not catalog:
                with self.transaction(validate=False):
                    for statement in DDL:
                        self.db.execute(statement)
                    for key, value in wanted.items():
                        self.db.execute('INSERT INTO meta VALUES(?,?)', (key, json.dumps(value)))
                self.validate()
            self.reset_log()
        except BaseException:
            if self.db is not None:
                self.db.close()
            raise

    def close(self):
        self.db.close()

    def check_file_sizes(self):
        directory = self.path.parent.lstat()
        if (not stat.S_ISDIR(directory.st_mode) or directory.st_uid != os.geteuid()
                or directory.st_mode & 0o077):
            raise JournalError('journal_unsafe_state')
        for suffix in ('-shm', '-journal'):
            try:
                Path(str(self.path) + suffix).lstat()
            except FileNotFoundError:
                continue
            # This store never adopts files from a different locking/journal policy.
            raise JournalError('journal_recovery_required')
        for suffix, limit in (('', MAX_DATABASE_BYTES), ('-wal', MAX_WAL_BYTES)):
            try:
                info = Path(str(self.path) + suffix).lstat()
                if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid()
                        or info.st_mode & 0o077 or info.st_nlink != 1):
                    raise JournalError('journal_unsafe_state')
                if info.st_size > limit:
                    raise JournalError('journal_capacity')
            except FileNotFoundError:
                pass

    def configure(self):
        temp = 1
        for (option,) in self.db.execute('PRAGMA compile_options'):
            if option.startswith('TEMP_STORE='):
                try:
                    temp = int(option.partition('=')[2])
                except ValueError:
                    temp = -1
        if temp not in (1, 2, 3):
            raise JournalError('journal_unsupported_runtime')
        if (self.db.execute('PRAGMA page_count').fetchone()[0] > MAX_PAGES
                or self.db.execute('PRAGMA page_size').fetchone()[0] != PAGE_SIZE
                or self.db.execute('PRAGMA auto_vacuum').fetchone()[0] != 0
                or self.db.execute('PRAGMA encoding').fetchone()[0] != 'UTF-8'):
            raise JournalError('journal_unsupported_runtime')
        settings = (('locking_mode', 'EXCLUSIVE', 'exclusive'), ('journal_mode', 'WAL', 'wal'),
                    ('synchronous', 'FULL', 2), ('fullfsync', 'ON', 1),
                    ('checkpoint_fullfsync', 'ON', 1), ('wal_autocheckpoint', 0, 0),
                    ('cache_spill', 'OFF', 0), ('temp_store', 'MEMORY', 2),
                    ('max_page_count', MAX_PAGES, MAX_PAGES), ('foreign_keys', 'ON', 1))
        for name, value, _ in settings:
            self.db.execute(f'PRAGMA {name}={value}').fetchall()
        for name, _, expected in settings:
            if self.db.execute(f'PRAGMA {name}').fetchone()[0] != expected:
                raise JournalError('journal_unsupported_runtime')

    def reset_log(self):
        busy, pages, _ = self.db.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()
        try:
            remaining = Path(str(self.path) + '-wal').stat().st_size
        except FileNotFoundError:
            remaining = 0
        if busy or pages or remaining:
            raise JournalError('journal_checkpoint_failed')

    @contextmanager
    def transaction(self, *, validate=True):
        self.reset_log()
        self.db.execute('BEGIN IMMEDIATE')
        try:
            yield
            if validate:
                self.validate()
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def meta(self):
        rows = self.db.execute('SELECT key,value FROM meta LIMIT ?', (len(META_KEYS) + 1,)).fetchall()
        if len(rows) != len(META_KEYS) or {row[0] for row in rows} != set(META_KEYS):
            raise JournalError('journal_recovery_required')
        try:
            return {key: json.loads(value) for key, value in rows}
        except (TypeError, ValueError) as exc:
            raise JournalError('journal_recovery_required') from exc

    def put(self, key, value):
        if not self.db.in_transaction or key not in META_KEYS:
            raise RuntimeError('metadata mutation outside a journal transaction')
        self.db.execute('UPDATE meta SET value=? WHERE key=?', (json.dumps(value), key))

    def rows(self):
        fields = ('seq', 'kind', 'binding', 'binding_instance', 'disposition', 'attempts', 'retry_at', 'uncertain')
        return [dict(zip(fields, row)) for row in self.db.execute(
            'SELECT seq,kind,binding,binding_instance,disposition,attempts,retry_at,uncertain FROM work ORDER BY seq LIMIT 2049')]

    def validate(self):
        try:
            return self._validate()
        except JournalError as exc:
            if exc.code in ('journal_invalid_counter', 'journal_identity_invalid'):
                # These values came from persisted state, not an internal API call.
                raise JournalError('journal_recovery_required') from exc
            raise

    def _validate(self):
        state = self.meta()
        baseline = identity(state['provider'], state['target_digest'], state['nonce'],
                            state['imported_through'], state['history_lost'])
        if state['schema'] != SCHEMA or type(state['schema']) is not int or state['namespace'] != baseline['namespace']:
            raise JournalError('journal_recovery_required')
        integer(state['scan_through'], state['imported_through'])
        integer(state['enumerated_through'], state['scan_through'])
        if any(type(state[key]) is not bool for key in ('pointers_seeded', 'activation_confirmed', 'history_acknowledged')):
            raise JournalError('journal_recovery_required')
        if state['history_acknowledged'] and not state['history_lost']:
            raise JournalError('journal_recovery_required')
        if not isinstance(state['counters'], dict) or set(state['counters']) != set(COUNTERS):
            raise JournalError('journal_recovery_required')
        for value in state['counters'].values():
            integer(value)
        rows = self.rows()
        if len(rows) > MAX_WORK:
            raise JournalError('journal_capacity')
        for row in rows:
            integer(row['seq'], 1)
            integer(row['retry_at'])
            if row['kind'] == 'peer':
                if row['binding'] is not None or row['binding_instance'] is not None:
                    raise JournalError('journal_recovery_required')
            elif row['kind'] == 'memory-pointer':
                if (not hex_value(row['binding'], 64) or not hex_value(row['binding_instance'], 32)
                        or row['binding_instance'] == '0' * 32):
                    raise JournalError('journal_recovery_required')
            else:
                raise JournalError('journal_recovery_required')
            if (row['disposition'] not in STATES or type(row['attempts']) is not int
                    or not 0 <= row['attempts'] <= MAX_ATTEMPTS
                    or type(row['uncertain']) is not int or row['uncertain'] not in (0, 1)):
                raise JournalError('journal_recovery_required')
            disposition, attempts_used = row['disposition'], row['attempts']
            if ((disposition == 'pending' and attempts_used == MAX_ATTEMPTS)
                    or (disposition in ('reserved', 'delivered') and attempts_used == 0)
                    or (disposition == 'failed' and (attempts_used != MAX_ATTEMPTS or row['uncertain']))
                    or (disposition == 'unknown' and not row['uncertain'])
                    or (disposition != 'pending' and row['retry_at'] != 0)):
                raise JournalError('journal_recovery_required')
        attempts = self.db.execute('SELECT id,started FROM attempt LIMIT 2').fetchall()
        members = self.db.execute('SELECT attempt_id,seq FROM attempt_member LIMIT 11').fetchall()
        if len(attempts) > 1 or len(members) > MAX_MEMBERS or bool(attempts) != bool(members):
            raise JournalError('journal_recovery_required')
        if attempts:
            reserved = [row for row in rows if row['disposition'] == 'reserved']
            if (not state['activation_confirmed'] or not state['pointers_seeded']
                    or len({row['kind'] for row in reserved}) != 1
                    or (reserved[0]['kind'] == 'memory-pointer' and len(reserved) != 1)):
                raise JournalError('journal_recovery_required')
            if attempts[0][0] != 1:
                raise JournalError('journal_recovery_required')
            integer(attempts[0][1])
        if {seq for attempt, seq in members if attempt == 1} != {
                row['seq'] for row in rows if row['disposition'] == 'reserved'}:
            raise JournalError('journal_recovery_required')
        if self.db.execute('PRAGMA foreign_key_check').fetchone() is not None:
            raise JournalError('journal_recovery_required')
        return state

    def count(self, key, amount=1):
        state = self.meta()['counters']
        state[key] = min(MAX_SEQUENCE, state[key] + amount)
        self.put('counters', state)

    def add(self, records, *, historical=False):
        if not self.db.in_transaction:
            raise RuntimeError('work admission outside a journal transaction')
        through = self.meta()['scan_through']
        for item in records:
            seq = integer(item['seq'], 1)
            if seq <= through and not historical:
                continue
            values = (seq, item['kind'], item['binding'], item['binding_instance'])
            old = self.db.execute('SELECT seq,kind,binding,binding_instance FROM work WHERE seq=?', (seq,)).fetchone()
            if old is not None:
                if old != values:
                    raise JournalError('journal_source_changed')
                continue
            if self.db.execute('SELECT count(*) FROM work').fetchone()[0] >= MAX_WORK:
                raise JournalError('journal_capacity')
            self.db.execute('INSERT INTO work VALUES(?,?,?,?,?,?,?,?)', (*values, 'pending', 0, 0, 0))

    def advance(self, through=None):
        rows = self.rows()
        state = self.meta()
        old = state['scan_through']
        # A seeded pointer can be ahead of the enumerated inbox range. Its result
        # is not evidence that intervening ordinary messages have been scanned.
        high = state['enumerated_through']
        if through is not None:
            high = max(high, integer(through, old))
            self.put('enumerated_through', high)
        waiting = [row['seq'] for row in rows if row['disposition'] in ('pending', 'reserved') and row['seq'] > old]
        eligible = min(high, min(waiting) - 1) if waiting else high
        self.put('scan_through', max(old, eligible))
        for row in rows:
            if row['seq'] <= max(old, eligible) and row['disposition'] in ('delivered', 'acknowledged', 'obsolete'):
                self.count(row['disposition'])
                self.db.execute('DELETE FROM work WHERE seq=?', (row['seq'],))

    def ingest(self, snapshot):
        with self.transaction():
            through = integer(snapshot['through'], self.meta()['scan_through'])
            if len(snapshot['records']) > 128 or any(item['seq'] > through for item in snapshot['records']):
                raise JournalError('journal_source_changed')
            ignored = snapshot.get('ignored', [])
            if (not isinstance(ignored, (list, tuple)) or len(ignored) + len(snapshot['records']) > 128
                    or any(type(seq) is not int or not 1 <= seq <= through for seq in ignored)
                    or len(set(ignored)) != len(ignored)
                    or set(ignored) & {item['seq'] for item in snapshot['records']}):
                raise JournalError('journal_source_changed')
            enumerated = self.meta()['enumerated_through']
            self.add(snapshot['records'])
            self.count('ignored', sum(seq > enumerated for seq in ignored))
            self.advance(through)
        return self.meta()['scan_through']

    def seed_pointers(self, snapshot):
        with self.transaction():
            if self.meta()['pointers_seeded']:
                return False
            records = snapshot['records']
            if len(records) > 16 or any(item['kind'] != 'memory-pointer' for item in records):
                raise JournalError('journal_source_changed')
            self.add(records, historical=True)
            self.put('pointers_seeded', True)
        return True

    def due(self, now, *, memory_available=True):
        integer(now)
        ready = [row for row in self.rows() if row['disposition'] == 'pending'
                 and (memory_available or row['kind'] == 'peer')]
        if not ready or ready[0]['retry_at'] > now:
            return []
        first = ready[0]
        if first['kind'] == 'memory-pointer':
            return [first['seq']]
        selected = []
        for row in ready:
            if row['kind'] != 'peer' or row['retry_at'] > now or len(selected) == MAX_MEMBERS:
                break
            selected.append(row['seq'])
        return selected

    def reserve(self, sequences, now):
        integer(now)
        if (not isinstance(sequences, (list, tuple)) or not 1 <= len(sequences) <= MAX_MEMBERS
                or any(type(value) is not int or value <= 0 for value in sequences)
                or len(set(sequences)) != len(sequences)):
            raise JournalError('journal_invalid_reservation')
        with self.transaction():
            state = self.meta()
            if not state['activation_confirmed'] or not state['pointers_seeded']:
                raise JournalError('journal_not_ready')
            if self.db.execute('SELECT count(*) FROM attempt').fetchone()[0]:
                raise JournalError('journal_attempt_in_progress')
            rows = {row['seq']: row for row in self.rows()}
            selected = [rows.get(seq) for seq in sequences]
            if any(row is None or row['disposition'] != 'pending' or row['retry_at'] > now
                   or row['attempts'] >= MAX_ATTEMPTS for row in selected):
                raise JournalError('journal_invalid_reservation')
            if len({row['kind'] for row in selected}) != 1 or (selected[0]['kind'] == 'memory-pointer' and len(selected) != 1):
                raise JournalError('journal_invalid_reservation')
            self.db.execute('INSERT INTO attempt VALUES(1,?)', (now,))
            for seq in sequences:
                self.db.execute("UPDATE work SET disposition='reserved',attempts=attempts+1,retry_at=0 WHERE seq=?", (seq,))
                self.db.execute('INSERT INTO attempt_member VALUES(1,?)', (seq,))
            self.count('attempts', len(sequences))
        return [row for row in self.rows() if row['seq'] in sequences]

    def resolve(self, outcome, now):
        integer(now)
        if outcome not in ('delivered', 'failed', 'unknown'):
            raise JournalError('journal_invalid_outcome')
        with self.transaction():
            members = {row[0] for row in self.db.execute('SELECT seq FROM attempt_member')}
            if not members:
                raise JournalError('journal_no_attempt')
            for row in self.rows():
                if row['seq'] not in members:
                    continue
                uncertain = int(bool(row['uncertain']) or outcome == 'unknown')
                if outcome == 'delivered':
                    disposition, retry_at = 'delivered', 0
                elif row['attempts'] >= MAX_ATTEMPTS:
                    disposition, retry_at = ('unknown' if uncertain else 'failed'), 0
                    self.count(disposition)
                else:
                    disposition = 'pending'
                    retry_at = min(MAX_SEQUENCE, now + (30 if row['attempts'] == 1 else 60))
                self.db.execute('UPDATE work SET disposition=?,retry_at=?,uncertain=? WHERE seq=?',
                                (disposition, retry_at, uncertain, row['seq']))
            self.db.execute('DELETE FROM attempt_member')
            self.db.execute('DELETE FROM attempt')
            self.advance()

    def recover_attempt(self, now):
        if not self.db.execute('SELECT count(*) FROM attempt').fetchone()[0]:
            return False
        self.resolve('unknown', now)
        return True

    def retry(self, sequences):
        if (not isinstance(sequences, (list, tuple)) or not 1 <= len(sequences) <= MAX_MEMBERS
                or any(type(value) is not int or value <= 0 for value in sequences)
                or len(set(sequences)) != len(sequences)):
            raise JournalError('journal_invalid_retry')
        with self.transaction():
            rows = {row['seq']: row for row in self.rows()}
            if any(seq not in rows or rows[seq]['disposition'] not in ('failed', 'unknown') for seq in sequences):
                raise JournalError('journal_invalid_retry')
            for seq in sequences:
                self.db.execute("UPDATE work SET disposition='pending',attempts=0,retry_at=0 WHERE seq=?", (seq,))

    def reconcile(self, snapshot):
        with self.transaction():
            if self.db.execute('SELECT count(*) FROM attempt').fetchone()[0]:
                raise JournalError('journal_attempt_in_progress')
            ack = snapshot['ack_through']
            if ack is not None:
                integer(ack)
            retained = {item['seq']: item for item in snapshot['records']}
            compatibility_missing = []
            for row in self.rows():
                if row['disposition'] in ('delivered', 'acknowledged', 'obsolete'):
                    continue
                if ack is not None and row['seq'] <= ack:
                    disposition = 'acknowledged'
                elif row['seq'] in retained:
                    item = retained[row['seq']]
                    if any(item[key] != row[key] for key in ('kind', 'binding', 'binding_instance')):
                        raise JournalError('journal_source_changed')
                    continue
                elif row['kind'] == 'memory-pointer':
                    if snapshot['bindings'] is None or snapshot['pointers'] is None:
                        # An older bridge has no evidence of binding removal.
                        continue
                    current = snapshot['pointers'].get(row['binding'])
                    if (snapshot['bindings'].get(row['binding']) != row['binding_instance']
                            or (current is not None and current['binding_instance'] == row['binding_instance']
                                and current['seq'] > row['seq'])):
                        disposition = 'obsolete'
                    else:
                        raise JournalError('journal_source_missing')
                elif ack is None:
                    # Old bridges cannot attribute a disappearing row to ack.
                    disposition = 'unknown'
                    if row['disposition'] != 'unknown':
                        self.count('unknown')
                    self.db.execute('UPDATE work SET uncertain=1 WHERE seq=?', (row['seq'],))
                    compatibility_missing.append(row['seq'])
                else:
                    raise JournalError('journal_source_missing')
                self.db.execute('UPDATE work SET disposition=?,retry_at=0 WHERE seq=?', (disposition, row['seq']))
            self.advance()
            through = self.meta()['scan_through']
            for seq in compatibility_missing:
                if seq <= through:
                    self.db.execute('DELETE FROM work WHERE seq=?', (seq,))


    def confirm_activation(self, evidence):
        with self.transaction():
            state = self.meta()
            if evidence != dict(target_digest=state['target_digest'], nonce=state['nonce']):
                raise JournalError('journal_activation_mismatch')
            self.put('activation_confirmed', True)

    def acknowledge_health(self):
        with self.transaction():
            state = self.meta()
            cleared = state['counters']
            self.put('counters', dict.fromkeys(COUNTERS, 0))
            if state['history_lost']:
                self.put('history_acknowledged', True)
        return cleared

    def status(self):
        state, rows = self.meta(), self.rows()
        return dict(imported_through=state['imported_through'],
                    scan_through=state['scan_through'], enumerated_through=state['enumerated_through'],
                    counters=state['counters'],
                    pending=sum(row['disposition'] in ('pending', 'reserved') for row in rows),
                    exhausted=sum(row['disposition'] in ('failed', 'unknown') for row in rows),
                    uncertain=sum(bool(row['uncertain']) for row in rows),
                    history_lost=state['history_lost'] and not state['history_acknowledged'],
                    activation_confirmed=state['activation_confirmed'],
                    pointers_seeded=state['pointers_seeded'])
