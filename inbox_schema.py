"""Transactional inbox metadata, owned exclusively by the bridge database worker."""
from contextlib import contextmanager
import json
import re
import sqlite3
import uuid
import delivery_ledger

SCHEMA = 4
MAX_SEQUENCE = (1 << 63) - 1
CAPABILITIES = ('inbox_ack_watermark', 'notification_journal_activation', 'delivery_ledger')
KEYS = {'schema', 'ack_through', 'journal_activation'}
LEGACY_COLUMNS = ('seq', 'received', 'pid', 'frame')
COLUMNS = LEGACY_COLUMNS + ('kind', 'binding', 'binding_instance')


class InboxSchemaError(sqlite3.DatabaseError):
    """The inbox schema or metadata is incompatible or inconsistent."""


@contextmanager
def transaction(db, *, write=True):
    db.execute('BEGIN IMMEDIATE' if write else 'BEGIN')
    try:
        yield
        db.commit()
    except BaseException:
        db.rollback()
        raise


def allocated_head(db):
    row = db.execute("SELECT seq FROM sqlite_sequence WHERE name='inbox'").fetchone()
    if row is None:
        return 0
    value = row[0]
    if type(value) is not int or not 0 <= value <= MAX_SEQUENCE:
        raise InboxSchemaError('invalid allocated inbox sequence')
    return value


def hex_value(value, length):
    return isinstance(value, str) and re.fullmatch('[0-9a-f]{'+str(length)+'}', value) is not None


def metadata(db, *, versions=(SCHEMA,)):
    rows = db.execute('SELECT key,value FROM inbox_meta LIMIT 4').fetchall()
    if len(rows) != len(KEYS) or {row[0] for row in rows} != KEYS:
        raise InboxSchemaError('incomplete inbox metadata')
    try:
        values = {key: json.loads(value) for key, value in rows}
    except (TypeError, ValueError) as exc:
        raise InboxSchemaError('invalid inbox metadata encoding') from exc
    if type(values['schema']) is not int or values['schema'] not in versions:
        raise InboxSchemaError('unsupported inbox schema')
    ack = values['ack_through']
    if type(ack) is not int or not 0 <= ack <= allocated_head(db):
        raise InboxSchemaError('invalid inbox acknowledgement watermark')
    activation = values['journal_activation']
    if activation is not None:
        if (not isinstance(activation, dict) or set(activation) != {'target_digest', 'nonce'}
                or not hex_value(activation['target_digest'], 64)
                or not hex_value(activation['nonce'], 32)):
            raise InboxSchemaError('invalid journal activation evidence')
    return values


def initialize(db):
    # Explicit DDL transaction: neither an interrupted migration nor a refusal
    # may leave a partially added column/table or an invented acknowledgement.
    with transaction(db):
        if db.execute('PRAGMA encoding').fetchone()[0] != 'UTF-8':
            raise InboxSchemaError('inbox requires UTF-8 encoding')
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if 'inbox_meta' in tables:
            if not {'inbox', 'memory_binding', 'sqlite_sequence'} <= tables:
                raise InboxSchemaError('incomplete inbox schema')
            state = metadata(db, versions=(2, 3, SCHEMA))
            if state['schema'] == SCHEMA and not {'delivery_identity', 'delivery_record'} <= tables:
                raise InboxSchemaError('incomplete delivery ledger schema')
            columns = tuple(row[1] for row in db.execute('PRAGMA table_info(inbox)'))
            expected = COLUMNS[:-1] if state['schema'] == 2 else COLUMNS
            if columns != expected:
                raise InboxSchemaError('incompatible inbox columns')
            if state['schema'] == 2:
                add_binding_observations(db)
            if state['schema'] < 4:
                delivery_ledger.initialize(db)
                db.execute("UPDATE inbox_meta SET value=? WHERE key='schema'", (str(SCHEMA),))
            delivery_ledger.identity(db)
            validate_bindings(db)
            return
        if tables - {'inbox', 'sqlite_sequence'}:
            raise InboxSchemaError('unrecognized legacy inbox schema')
        if 'inbox' in tables:
            columns = tuple(row[1] for row in db.execute('PRAGMA table_info(inbox)'))
            if columns != LEGACY_COLUMNS or 'sqlite_sequence' not in tables:
                raise InboxSchemaError('incompatible legacy inbox')
        else:
            db.execute('CREATE TABLE inbox (seq INTEGER PRIMARY KEY AUTOINCREMENT, received REAL, pid INTEGER, frame TEXT)')
        db.execute("ALTER TABLE inbox ADD COLUMN kind TEXT NOT NULL DEFAULT 'peer'")
        db.execute('ALTER TABLE inbox ADD COLUMN binding TEXT')
        db.execute("CREATE TABLE inbox_meta (key TEXT NOT NULL PRIMARY KEY CHECK(key IN ('schema','ack_through','journal_activation')), value TEXT NOT NULL CHECK(length(CAST(value AS BLOB))<=512))")
        db.execute("""CREATE TABLE memory_binding (
            binding TEXT NOT NULL PRIMARY KEY CHECK(length(binding)=64 AND binding NOT GLOB '*[^0-9a-f]*'),
            repo_path TEXT NOT NULL CHECK(substr(repo_path,1,1)='/' AND length(CAST(repo_path AS BLOB))<=4096),
            repo_key TEXT NOT NULL CHECK(length(repo_key)=16 AND repo_key NOT GLOB '*[^0-9a-f]*'),
            memory_state_dir TEXT NOT NULL CHECK(substr(memory_state_dir,1,1)='/' AND length(CAST(memory_state_dir AS BLOB))<=4096)
        )""")
        db.executemany('INSERT INTO inbox_meta(key,value) VALUES(?,?)',
                       [('schema', str(SCHEMA)), ('ack_through', '0'), ('journal_activation', 'null')])
        add_binding_observations(db)
        validate_bindings(db)
        delivery_ledger.initialize(db)
        metadata(db)


def add_binding_observations(db):
    # A recreated deterministic binding key must not reuse its prior lifetime.
    db.execute("ALTER TABLE inbox ADD COLUMN binding_instance TEXT")
    db.execute("ALTER TABLE memory_binding ADD COLUMN binding_instance TEXT NOT NULL DEFAULT '00000000000000000000000000000000' CHECK(length(binding_instance)=32 AND binding_instance NOT GLOB '*[^0-9a-f]*')")
    existing = db.execute('SELECT binding FROM memory_binding LIMIT 17').fetchall()
    if len(existing) > 16:
        raise InboxSchemaError('too many memory bindings')
    for (key,) in existing:
        db.execute('UPDATE memory_binding SET binding_instance=? WHERE binding=?', (uuid.uuid4().hex, key))
    # These describe issued pointers, never memory consumer acknowledgements.
    db.execute("ALTER TABLE memory_binding ADD COLUMN observed_store TEXT CHECK(observed_store IS NULL OR (length(observed_store)=32 AND observed_store NOT GLOB '*[^0-9a-f]*'))")
    db.execute("ALTER TABLE memory_binding ADD COLUMN observed_head INTEGER CHECK(observed_head IS NULL OR (typeof(observed_head)='integer' AND observed_head>=0))")
    db.execute("ALTER TABLE memory_binding ADD COLUMN anomalies INTEGER NOT NULL DEFAULT 0 CHECK(typeof(anomalies)='integer' AND anomalies BETWEEN 0 AND 3)")


def validate_bindings(db):
    columns = tuple(row[1] for row in db.execute('PRAGMA table_info(memory_binding)'))
    if columns != ('binding', 'repo_path', 'repo_key', 'memory_state_dir',
                   'binding_instance', 'observed_store', 'observed_head', 'anomalies'):
        raise InboxSchemaError('incompatible binding columns')
    rows = db.execute('SELECT binding_instance,observed_store,observed_head,anomalies FROM memory_binding LIMIT 17').fetchall()
    if len(rows) > 16:
        raise InboxSchemaError('too many memory bindings')
    for instance, store_id, head, anomalies in rows:
        if (not hex_value(instance, 32) or instance == '0'*32 or (store_id is None) != (head is None)
                or (store_id is not None and (not hex_value(store_id, 32) or type(head) is not int or not 0 <= head <= MAX_SEQUENCE))
                or type(anomalies) is not int or not 0 <= anomalies <= 3):
            raise InboxSchemaError('invalid binding observation')


def acknowledge(db, through):
    if type(through) is not int or through < 0:
        raise ValueError('through must be a nonnegative integer')
    with transaction(db):
        state = metadata(db)
        through = min(through, allocated_head(db))
        db.execute('DELETE FROM inbox WHERE seq<=?', (through,))
        watermark = max(through, state['ack_through'])
        db.execute("UPDATE inbox_meta SET value=? WHERE key='ack_through'", (str(watermark),))
    return 'acknowledged locally'


def activate(db, request):
    target, nonce = request.get('target_digest'), request.get('nonce')
    if not hex_value(target, 64) or not hex_value(nonce, 32):
        raise ValueError('target_digest and nonce must be lowercase hexadecimal digests')
    allowed = {'op', 'target_digest', 'nonce'}
    rebuilding = request['op'] == 'rebuild-notification-journal-activation'
    if rebuilding:
        allowed |= {'expected_previous_nonce', 'accept_history_loss'}
        if request.get('accept_history_loss') is not True or not hex_value(request.get('expected_previous_nonce'), 32):
            raise ValueError('rebuild requires accepted history loss and the previous nonce')
    if set(request) - allowed:
        raise ValueError('unknown journal activation field')
    value = dict(target_digest=target, nonce=nonce)
    with transaction(db):
        previous = metadata(db)['journal_activation']
        if previous is not None and previous['target_digest'] != target:
            raise ValueError('journal target cannot be replaced')
        if previous != value:
            if rebuilding:
                if previous is None or previous['nonce'] != request['expected_previous_nonce']:
                    raise ValueError('journal activation changed; rebuild refused')
            elif previous is not None:
                raise ValueError('journal nonce requires explicit rebuild')
            db.execute("UPDATE inbox_meta SET value=? WHERE key='journal_activation'",
                       (json.dumps(value, sort_keys=True, separators=(',', ':')),))
    return value
