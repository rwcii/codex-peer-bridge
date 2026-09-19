"""Bounded delivery health, independent of journal capacity and lifecycle readiness."""
from pathlib import Path
import time
import subprocess

import platform_support
from session_observation import lock_held

import durable_state
from inbox_schema import MAX_SEQUENCE, hex_value
from notification_journal import COUNTERS, MAX_WORK

FRESHNESS_MS = 15_000
PUBLISH_INTERVAL = 2
REASONS = frozenset((
    'notified_outbox_full', 'pending_receipt_evidence', 'receipt_evidence_unrecorded',
    'storage_wait', 'storage_error', 'version_compatibility', 'journal_capacity',
    'journal_recovery_required', 'journal_upgrade_required', 'journal_unsafe_state',
    'journal_unsupported_runtime', 'source_fault', 'pending_delivery',
    'exhausted_delivery', 'uncertain_delivery', 'history_lost', 'internal_error',
    'health_publication_failed', 'memory_unavailable', 'memory_operator_action',
    'bridge_unavailable', 'starting',
))
IDENTITY_FIELDS = ('owner', 'bridge_pid', 'notifier_pid', 'proc_start')
JOURNAL_FIELDS = frozenset((
    'imported_through', 'scan_through', 'enumerated_through', 'counters', 'pending',
    'exhausted', 'uncertain', 'history_lost', 'activation_confirmed', 'pointers_seeded',
))


def uint(value):
    return type(value) is int and 0 <= value <= MAX_SEQUENCE


def valid_identity(value):
    return (isinstance(value, dict) and hex_value(value.get('owner'), 32)
            and all(uint(value.get(key)) and value[key] > 0 for key in ('bridge_pid', 'notifier_pid'))
            and isinstance(value.get('proc_start'), str) and 0 < len(value['proc_start']) <= 128
            and not any(ord(char) < 32 or ord(char) == 127 for char in value['proc_start']))


def valid_journal(value):
    if not isinstance(value, dict) or set(value) not in (JOURNAL_FIELDS, JOURNAL_FIELDS | {'receipt_pending'}):
        return False
    flags = ('history_lost', 'activation_confirmed', 'pointers_seeded')
    counts = ('imported_through', 'scan_through', 'enumerated_through', 'pending', 'exhausted', 'uncertain')
    counters = value['counters']
    return (('receipt_pending' not in value or (uint(value['receipt_pending']) and value['receipt_pending'] <= MAX_WORK))
            and all(type(value[key]) is bool for key in flags)
            and all(uint(value[key]) for key in counts)
            and value['pending'] + value['exhausted'] <= MAX_WORK
            and value['uncertain'] <= MAX_WORK
            and value['imported_through'] <= value['scan_through'] <= value['enumerated_through']
            and isinstance(counters, dict) and set(counters) == set(COUNTERS)
            and all(uint(item) for item in counters.values()))


def journal_reasons(journal):
    flags = {reason for field, reason in (
        ('pending', 'pending_delivery'), ('exhausted', 'exhausted_delivery'),
        ('uncertain', 'uncertain_delivery'), ('history_lost', 'history_lost')) if journal[field]}
    if not journal['activation_confirmed'] or not journal['pointers_seeded']:
        flags.add('starting')
    if journal['counters'].get('receipt_unrecorded'):
        flags.add('receipt_evidence_unrecorded')
    if journal.get('receipt_pending'):
        flags.add('pending_receipt_evidence')
    return flags


def validate(value):
    fields = {'version', *IDENTITY_FIELDS, 'updated_at_ms', 'state', 'reasons', 'journal'}
    if (not isinstance(value, dict) or set(value) != fields or value['version'] != 1
            or type(value['version']) is not int or not valid_identity(value)
            or not uint(value['updated_at_ms']) or value['state'] not in ('healthy', 'degraded', 'unknown')
            or not isinstance(value['reasons'], list) or len(value['reasons']) > len(REASONS)
            or any(not isinstance(item, str) or item not in REASONS for item in value['reasons'])
            or len(set(value['reasons'])) != len(value['reasons'])
            or (value['journal'] is not None and not valid_journal(value['journal']))):
        raise ValueError('invalid notifier health snapshot')
    if value['journal'] is not None and not journal_reasons(value['journal']) <= set(value['reasons']):
        raise ValueError('notifier snapshot conceals journal state')
    if value['state'] == 'healthy' and (value['reasons'] or value['journal'] is None):
        raise ValueError('invalid healthy notifier snapshot')
    if value['state'] == 'degraded' and not value['reasons']:
        raise ValueError('degraded notifier snapshot requires reasons')
    return value


def snapshot(owner, status=None, reasons=(), *, now=None):
    if not valid_identity(owner):
        raise ValueError('invalid notifier health owner')
    flags = set(reasons)
    if not flags <= REASONS:
        raise ValueError('unclassified notifier health reason')
    journal = status.get('journal') if status is not None else None
    if journal is not None:
        if not valid_journal(journal):
            raise ValueError('invalid journal health')
        flags.update(journal_reasons(journal))
    if status is not None and status.get('compatibility'):
        flags.add('version_compatibility')
    state = 'degraded' if flags else ('healthy' if journal is not None else 'unknown')
    now = int(time.time() * 1000) if now is None else now
    value = dict(version=1, **{key: owner[key] for key in IDENTITY_FIELDS},
                 updated_at_ms=now, state=state, reasons=sorted(flags), journal=journal)
    return validate(value)


def verify_owner(root):
    """Read-only proof of the readiness process and held notifier state lock."""
    try:
        owner = durable_state.read(Path(root) / 'notify-ready.json')
        if not valid_identity(owner):
            return None
        live = platform_support.proc_start(owner['notifier_pid'])
        if not platform_support.same_process(owner['proc_start'], live):
            return None
        if not lock_held(Path(root) / 'notifier.lock'):
            return None
        return owner
    except (OSError, ValueError, subprocess.SubprocessError):
        return None


def read(root, verified_owner, *, now=None):
    """Caller must first verify readiness owner liveness and its held state lock.

    A stale or mismatched file never changes lifecycle or triggers a restart.
    Missing facts remain unknown. Returned diagnostics omit private identity fields.
    """
    def unknown(reason):
        return dict(state='unknown', reasons=[reason], journal=None)
    if not valid_identity(verified_owner):
        return unknown('health_owner_unverified')
    try:
        value = durable_state.read(Path(root) / 'notify-health.json')
        if value is None:
            return unknown('health_missing')
        validate(value)
    except (OSError, ValueError):
        return unknown('health_unreadable')
    if any(value[key] != verified_owner[key] for key in IDENTITY_FIELDS):
        return unknown('health_owner_mismatch')
    now = int(time.time() * 1000) if now is None else now
    if not uint(now) or not 0 <= now - value['updated_at_ms'] <= FRESHNESS_MS:
        return unknown('health_stale')
    return dict(state=value['state'], reasons=value['reasons'], journal=value['journal'])


class HealthFile:
    """Own on a separate bounded worker so a journal stall cannot block publication.

    The event-loop caller owns the live fault flag and retries at the publication
    interval. A raised error is not proof that replacement did not occur. Readers
    expire the prior file instead of assuming that a final failure write succeeded.
    """
    def __init__(self, root):
        self.path = Path(root) / 'notify-health.json'

    def publish(self, value):
        durable_state.publish(self.path, validate(value))

    def close(self):
        pass
