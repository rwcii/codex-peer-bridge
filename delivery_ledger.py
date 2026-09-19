"""Bounded local delivery evidence, owned by the inbox database worker.

No method sends a peer frame. Identity is attribution within the same-user boundary.
Native sender identities cover an observed process lifetime, not an agent restart.
"""
import hashlib
import json
import time
import uuid

RETENTION = 86400
MAX_RECORDS = 10000  # Per direction; outgoing traffic cannot exhaust inbound slots.
MAX_RECORD_BYTES = 4096


class DeliveryError(ValueError):
    def __init__(self, code):
        self.code = code
        self.database_fault = 'storage_error' if code == 'delivery_identity_invalid' else None
        super().__init__(code)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def initialize(db):
    db.execute("CREATE TABLE delivery_identity (id INTEGER PRIMARY KEY CHECK(id=1), identity TEXT NOT NULL CHECK(length(identity)=32))")
    db.execute('INSERT INTO delivery_identity VALUES(1,?)', (uuid.uuid4().hex,))
    db.execute("CREATE TABLE delivery_record (key TEXT PRIMARY KEY, seq INTEGER UNIQUE, expires REAL NOT NULL, fingerprint TEXT NOT NULL, data TEXT NOT NULL CHECK(length(CAST(data AS BLOB))<=4096))")
    db.execute('CREATE INDEX delivery_expiry ON delivery_record(expires)')
    # Existing rows prove persistence only. The process-start identity was not
    # recorded, so migration deliberately does not invent historical dedup keys.
    rows = db.execute("SELECT seq,received,frame FROM inbox WHERE kind='peer' ORDER BY seq").fetchall()
    if len(rows) > MAX_RECORDS:
        raise DeliveryError('delivery_capacity')
    now = time.time()
    for seq, received, frame in rows:
        data = dict(direction='incoming', seq=seq, sender=None, msg_id=None,
                    deduplication='unavailable_pre_migration',
                    stages=dict(stored=dict(at=received, source='pre_migration')))
        insert(db, 'legacy:'+str(seq), seq, digest(json.loads(frame)), data, now)


def identity(db):
    row = db.execute('SELECT identity FROM delivery_identity WHERE id=1').fetchone()
    if row is None or not isinstance(row[0], str) or len(row[0]) != 32 or any(c not in '0123456789abcdef' for c in row[0]):
        raise DeliveryError('delivery_identity_invalid')
    return row[0]


def get(db, key=None, seq=None):
    row = db.execute('SELECT key,fingerprint,data,expires FROM delivery_record WHERE '+
                     ('key=?' if key is not None else 'seq=?'),
                     (key if key is not None else seq,)).fetchone()
    if row is None:
        return None
    return dict(key=row[0], fingerprint=row[1], data=json.loads(row[2]), expires=row[3])


def capacity(db, now, incoming=True):
    # Never evict unexpired evidence or evidence for a retained inbox entry.
    db.execute('DELETE FROM delivery_record WHERE expires<? AND (seq IS NULL OR seq NOT IN (SELECT seq FROM inbox))', (now,))
    direction = 'seq IS NOT NULL' if incoming else 'seq IS NULL'
    if db.execute('SELECT count(*) FROM delivery_record WHERE ' + direction).fetchone()[0] >= MAX_RECORDS:
        raise DeliveryError('delivery_capacity')


def insert(db, key, seq, fingerprint, data, now):
    capacity(db, now, incoming=seq is not None)
    encoded = canonical(data)
    if len(encoded.encode()) > MAX_RECORD_BYTES:
        raise DeliveryError('delivery_record_too_large')
    db.execute('INSERT INTO delivery_record VALUES(?,?,?,?,?)',
               (key, seq, now + RETENTION, fingerprint, encoded))


def update(db, record, now):
    encoded = canonical(record['data'])
    if len(encoded.encode()) > MAX_RECORD_BYTES:
        raise DeliveryError('delivery_record_too_large')
    db.execute('UPDATE delivery_record SET data=?,expires=? WHERE key=?',
               (encoded, max(record['expires'], now + RETENTION), record['key']))


def incoming_key(db, frame, sender):
    msg_id = frame.get('msg_id')
    if sender is None or not isinstance(msg_id, str) or not 1 <= len(msg_id.encode()) <= 128:
        return None
    return 'in:'+digest([sender, identity(db), msg_id])


def incoming(db, frame, sender, now):
    key = incoming_key(db, frame, sender)
    fingerprint = digest(frame)
    record = get(db, key=key) if key else None
    if record is not None:
        retained = db.execute('SELECT 1 FROM inbox WHERE seq=?', (record['data']['seq'],)).fetchone()
        if record['expires'] >= now or retained:
            if record['fingerprint'] != fingerprint:
                raise DeliveryError('delivery_payload_conflict')
            return key, fingerprint, record['data']['seq']
    capacity(db, now)
    return key, fingerprint, None


def stored(db, key, fingerprint, seq, frame, sender, now):
    msg_id = frame.get('msg_id') if key else None
    data = dict(direction='incoming', seq=seq, sender=sender, msg_id=msg_id,
                deduplication='process_lifetime' if key else 'unavailable',
                stages=dict(stored=dict(at=now, source='inbox_commit')))
    insert(db, key or 'seq:'+str(seq), seq, fingerprint, data, now)


def stage(db, seq, name, evidence, now):
    record = get(db, seq=seq)
    if record is None:
        return dict(seq=seq, recorded=False, reason='unavailable_or_expired')
    stages = record['data']['stages']
    previous = stages.get(name)
    if previous is not None:
        if name == 'handled' and previous['outcome'] != evidence['outcome']:
            raise DeliveryError('delivery_outcome_conflict')
        # A confirmed provider acceptance upgrades earlier uncertainty only.
        if name != 'notified' or previous['outcome'] == 'delivered' or evidence['outcome'] != 'delivered':
            return dict(seq=seq, recorded=True)
    stages[name] = evidence
    update(db, record, now)
    return dict(seq=seq, recorded=True)


def outbound(db, address, message, priority, msg_id, deadline, now):
    if not isinstance(msg_id, str) or not 1 <= len(msg_id.encode()) <= 128:
        raise DeliveryError('delivery_invalid_id')
    if type(deadline) not in (int, float) or not now < deadline <= now + RETENTION:
        raise DeliveryError('delivery_deadline_expired_or_invalid')
    key = 'out:'+digest([identity(db), address, msg_id])
    fingerprint = digest([address, message, priority, deadline])
    record = get(db, key=key)
    if record:
        if record['fingerprint'] != fingerprint:
            raise DeliveryError('delivery_payload_conflict')
        return dict(new=False, key=key, **record['data'])
    data = dict(direction='outgoing', msg_id=msg_id, deadline=deadline,
                status='indeterminate', stages={}, attempts=1, automatic_replay=False)
    insert(db, key, None, fingerprint, data, now)
    return dict(new=True, key=key, **data)


def transported(db, key, pid, now):
    record = get(db, key=key)
    if record is None:
        raise DeliveryError('delivery_record_missing')
    record['data'].update(status='transport_complete', peer_pid=pid)
    record['data']['stages']['transport'] = dict(at=now, source='socket_write_and_close')
    update(db, record, now)
    return record['data']


def conflict(db, key, now):
    record = get(db, key=key)
    if record is not None:
        diagnostic = record['data'].setdefault('payload_conflicts', dict(count=0, first_at=now))
        diagnostic['count'] = min(1000000000, diagnostic['count'] + 1)
        diagnostic['last_at'] = now
        update(db, record, now)


def failed_before_connect(db, key, now):
    record = get(db, key=key)
    if record is None:
        raise DeliveryError('delivery_record_missing')
    record['data'].update(status='failed_before_connect', retry='use_new_msg_id')
    update(db, record, now)
    return record['data']
