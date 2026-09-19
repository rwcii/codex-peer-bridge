"""Bounded, read-only inbox snapshots for notification journal reconciliation.

The caller supplies capabilities verified from the running bridge. This reader
never upgrades an inbox or treats a failed read as an empty source.
"""
from contextlib import contextmanager
import json
from pathlib import Path
import sqlite3

import inbox_schema

SCAN_LIMIT = 128
RECONCILE_LIMIT = 2048
POINTER_LIMIT = 16


class SourceError(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def sequence(value, *, positive=False):
    if type(value) is not int or not int(positive) <= value <= inbox_schema.MAX_SEQUENCE:
        raise SourceError('invalid_source_sequence')
    return value


class InboxSource:
    def __init__(self, path, *, schema=None, capabilities=()):
        self.schema = schema
        self.acknowledgements = 'inbox_ack_watermark' in capabilities
        self.pointers = 'memory_binding' in capabilities
        if self.acknowledgements and (type(schema) is not int or schema not in (2, 3, 4)):
            raise SourceError('source_version_mismatch')
        if self.pointers and (schema not in (3, 4) or not self.acknowledgements):
            raise SourceError('source_version_mismatch')
        self.db = sqlite3.connect(Path(path).absolute().as_uri() + '?mode=ro',
                                  uri=True, isolation_level=None, timeout=.1)
        try:
            self.db.execute('PRAGMA query_only=ON')
            if self.db.execute('PRAGMA query_only').fetchone()[0] != 1:
                raise SourceError('source_read_only_unavailable')
        except BaseException:
            self.db.close()
            raise

    def close(self):
        self.db.close()

    @contextmanager
    def snapshot(self):
        self.db.execute('BEGIN')
        try:
            if self.acknowledgements:
                state = inbox_schema.metadata(self.db, versions=(self.schema,))
                acknowledgement = state['ack_through']
                activation = state['journal_activation']
            else:
                acknowledgement = activation = None
            bindings = None
            pointers = None
            if self.pointers:
                pointers = {}
                inbox_schema.validate_bindings(self.db)
                bindings = dict(self.db.execute(
                    'SELECT binding,binding_instance FROM memory_binding LIMIT 17'))
                if len(bindings) > POINTER_LIMIT:
                    raise SourceError('source_binding_capacity')
                rows = self.db.execute(
                    "SELECT seq,kind,binding,binding_instance FROM inbox WHERE kind='memory-pointer' ORDER BY seq LIMIT 17"
                ).fetchall()
                if len(rows) > POINTER_LIMIT:
                    raise SourceError('source_pointer_capacity')
                for row in rows:
                    record = self.record(row)
                    key = record['binding']
                    if key in pointers or bindings.get(key) != record['binding_instance']:
                        raise SourceError('source_pointer_invalid')
                    pointers[key] = record
            yield dict(ack_through=acknowledgement, activation=activation, bindings=bindings, pointers=pointers)
            self.db.commit()
        except BaseException:
            self.db.rollback()
            raise

    def projection(self):
        if self.pointers:
            return 'seq,kind,binding,binding_instance'
        return "seq,'peer',NULL,NULL"

    @staticmethod
    def record(row):
        seq, kind, binding, instance = row
        sequence(seq, positive=True)
        if kind == 'memory-pointer':
            if (not inbox_schema.hex_value(binding, 64)
                    or not inbox_schema.hex_value(instance, 32) or instance == '0' * 32):
                raise SourceError('source_pointer_invalid')
        elif kind != 'peer' or binding is not None or instance is not None:
            raise SourceError('source_record_invalid')
        return dict(seq=seq, kind=kind, binding=binding, binding_instance=instance)

    def scan(self, after):
        sequence(after)
        with self.snapshot() as result:
            rows = self.db.execute(
                f'SELECT {self.projection()},frame FROM inbox WHERE seq>? ORDER BY seq LIMIT ?',
                (after, SCAN_LIMIT)).fetchall()
            records, ignored = [], []
            through = after
            for row in rows:
                record = self.record(row[:4])
                through = record['seq']
                if record['kind'] == 'peer':
                    try:
                        frame = json.loads(row[4])
                    except (TypeError, ValueError) as exc:
                        raise SourceError('source_frame_invalid') from exc
                    if not isinstance(frame, dict):
                        raise SourceError('source_frame_invalid')
                    if frame.get('type') != 'user':
                        ignored.append(record['seq'])
                        continue
                records.append(record)
            result.update(through=through, records=records, ignored=ignored)
            return result

    def seed_pointers(self):
        """Retained pointers, independent of an imported ordinary checkpoint."""
        if not self.pointers:
            raise SourceError('source_version_mismatch')
        with self.snapshot() as result:
            result['records'] = list(result['pointers'].values())
            return result

    def retained(self, sequences):
        """Read selected source identities and acknowledgement in one snapshot."""
        if (not isinstance(sequences, (list, tuple)) or len(sequences) > RECONCILE_LIMIT
                or any(type(value) is not int for value in sequences)):
            raise SourceError('invalid_source_sequence')
        for value in sequences:
            sequence(value, positive=True)
        if len(set(sequences)) != len(sequences):
            raise SourceError('invalid_source_sequence')
        with self.snapshot() as result:
            records = []
            for offset in range(0, len(sequences), SCAN_LIMIT):
                batch = sequences[offset:offset + SCAN_LIMIT]
                placeholders = ','.join('?' for _ in batch)
                rows = self.db.execute(
                    f'SELECT {self.projection()} FROM inbox WHERE seq IN ({placeholders}) ORDER BY seq',
                    batch).fetchall()
                records.extend(self.record(row) for row in rows)
            result['records'] = sorted(records, key=lambda item: item['seq'])
            return result
