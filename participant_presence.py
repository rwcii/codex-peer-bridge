"""Evidence-based presence and explicitly limited priority capabilities."""
import time

FRESHNESS_MS = 15000


def unknown(reason='no_verified_activity_source'):
    return dict(state='unknown', source=None, observed_at_ms=None,
                freshness_ms=FRESHNESS_MS, reason=reason)


def registry_activity(record, now=None):
    # Caller must first verify that the registry process and start marker are live.
    now = int(time.time() * 1000) if now is None else now
    stamp = record.get('statusUpdatedAt')
    raw = record.get('status')
    state = {'busy': 'busy', 'shell': 'busy', 'idle': 'idle', 'waiting': 'waiting'}.get(raw) if isinstance(raw, str) else None
    if record.get('entrypoint') != 'cli':
        return unknown('registry_activity_source_unverified')
    if state is None or type(stamp) is not int:
        return unknown('registry_activity_missing')
    if not 0 <= stamp <= now:
        return unknown('registry_activity_timestamp_invalid')
    return dict(state=state, source='claude_registry', observed_at_ms=now, since_ms=stamp,
                freshness_ms=FRESHNESS_MS, reason=None)


def service(source, state='reachable', now=None):
    return dict(state=state, source=source,
                observed_at_ms=int(time.time() * 1000) if now is None else now,
                freshness_ms=FRESHNESS_MS)


def priority(provider):
    return dict(provider=provider, stored_values=['now', 'next', 'later'],
                notification_mapping='unsupported',
                source='codex_queue_cli' if provider == 'codex' else 'dsh_queue_adapter',
                schema_support=False, endpoint_reachability='not_observed',
                live_verification=False,
                reason='content_free_queue_adapter_has_no_priority_mapping')
