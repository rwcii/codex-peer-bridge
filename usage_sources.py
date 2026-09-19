"""Read explicitly selected usage sources; never read conversation bodies."""
import datetime
import hashlib
import json
import os
from pathlib import Path

from usage_selection import native_role

COMPONENTS = ('tokens_in', 'tokens_out', 'cache_write', 'cache_read', 'reasoning')
FIELDS = COMPONENTS + ('total',)
MAX_LINE = 4 * 1024 * 1024
MAX_BYTES = 1024 * 1024 * 1024
MAX_RECORDS = 250000
ADAPTER_VERSION = 1


def timestamp(value):
    if not isinstance(value, str):
        raise ValueError('timestamp must be an ISO 8601 string with timezone')
    parsed = datetime.datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        raise ValueError('timestamp requires a timezone')
    return parsed.timestamp()


def text(value, name):
    if not isinstance(value, str) or not value or len(value) > 4096:
        raise ValueError(f'{name} must be a nonempty bounded string')
    return value


def normalize(provider, native):
    """Return disjoint counters and diagnostics, preserving unknown values."""
    issues = []
    def count(name):
        value = native.get(name)
        if value is None:
            issues.append('missing:' + name)
            return None
        if type(value) is not int or not 0 <= value <= 2**63 - 1:
            issues.append('invalid:' + name)
            return None
        return value
    def add(*values):
        return None if None in values else sum(values)
    def subtract(value, *parts):
        if value is None or None in parts:
            return None
        result = value - sum(parts)
        if result < 0:
            issues.append('inconsistent:overlapping_counters')
            return None
        return result
    if provider == 'normalized':
        result = {key: count(key) for key in FIELDS}
    else:
        input_count, output = count('input_tokens'), count('output_tokens')
        if provider == 'codex':
            write, read = count('cache_write_input_tokens'), count('cached_input_tokens')
            reasoning = count('reasoning_output_tokens')
            total = count('total_tokens')
            ordinary = subtract(input_count, read, write)
        elif provider == 'claude':
            write, read = count('cache_creation_input_tokens'), count('cache_read_input_tokens')
            reasoning = count('thinking_tokens')
            ordinary = input_count
            total = add(input_count, output, write, read)
        else:
            raise ValueError('unsupported source provider')
        result = dict(tokens_in=ordinary, tokens_out=subtract(output, reasoning),
                      cache_write=write, cache_read=read, reasoning=reasoning, total=total)
    if all(result[key] is not None for key in FIELDS):
        if result['total'] != sum(result[key] for key in COMPONENTS):
            issues.append('inconsistent:total_mismatch')
    return result, issues


def extract(provider, row, selection, model):
    """Return only usage metadata. Selection never authorizes executing record text."""
    if provider == 'codex':
        if row.get('type') != 'token_usage_record':
            return None
        data = row['payload']
        if data.get('thread_id') != selection['session_id']:
            raise ValueError('usage record does not match selected session')
        native = {key: data.get('usage', {}).get(key) for key in
                  ('input_tokens', 'output_tokens', 'cache_write_input_tokens',
                   'cached_input_tokens', 'reasoning_output_tokens', 'total_tokens')}
        response = data.get('response_id')
        complete = True  # This record type describes a completed response.
    elif provider == 'claude':
        if row.get('type') != 'assistant':
            return None
        if row.get('sessionId') != selection['session_id']:
            raise ValueError('usage record does not match selected session')
        data = row.get('message', {})
        usage = data.get('usage', {})
        native = {key: usage.get(key) for key in
                  ('input_tokens', 'output_tokens', 'cache_creation_input_tokens', 'cache_read_input_tokens')}
        native['thinking_tokens'] = (usage.get('output_tokens_details') or {}).get('thinking_tokens')
        response, model = data.get('id'), data.get('model')
        complete = data.get('stop_reason') in ('end_turn', 'tool_use', 'stop_sequence',
                                              'max_tokens', 'refusal', 'pause_turn')
    else:
        raise ValueError('unsupported native usage provider')
    response = text(response, 'response_id')
    at = timestamp(row.get('timestamp'))
    counters, issues = normalize(provider, native)
    if model is None:
        issues.append('missing:model')
    else:
        text(model, 'model')
    if not complete:
        issues.append('incomplete:response')
    return dict(response_id=response, model=model, timestamp=at, first_timestamp=at,
                complete=complete, native=native, counters=counters, issues=issues)


def collect(selection, until=None):
    path = Path(selection['path'])
    records, issues, model = {}, [], None
    metadata = {}
    excluded_api_error_rows = 0
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        stat = os.fstat(stream.fileno())
        size = stat.st_size
        if size > MAX_BYTES:
            raise ValueError('source exceeds byte limit')
        remaining = size
        line_no = 0
        while remaining:
            line = stream.readline(min(MAX_LINE + 1, remaining))
            if not line:
                raise ValueError('source truncated during collection')
            remaining -= len(line)
            digest.update(line)
            line_no += 1
            if len(line) > MAX_LINE:
                while not line.endswith(b'\n') and remaining:
                    line = stream.readline(min(MAX_LINE + 1, remaining))
                    if not line:
                        raise ValueError('source truncated during collection')
                    remaining -= len(line)
                    digest.update(line)
                issues.append(f'incomplete:record:{line_no}:oversized_line')
                if len(issues) > 1000:
                    raise ValueError('source exceeds diagnostic limit')
                continue
            if not line.endswith(b'\n'):
                issues.append('incomplete:trailing_line')
                break
            try:
                row = json.loads(line)
                if not isinstance(row, dict):
                    raise ValueError('record must be an object')
                if (selection['provider'] == 'claude' and row.get('type') == 'assistant'
                        and (row.get('isApiErrorMessage') is True
                             or row.get('message', {}).get('model') == '<synthetic>')):
                    excluded_api_error_rows += 1
                    continue
                if selection['provider'] == 'codex' and row.get('type') == 'session_meta':
                    metadata = row.get('payload', {})
                    if metadata.get('id') != selection['session_id']:
                        raise ValueError('source lineage does not match selected session')
                if selection['provider'] == 'codex' and row.get('type') == 'turn_context':
                    model = row.get('payload', {}).get('model')
                record = extract(selection['provider'], row, selection, model)
                if record is None or (until is not None and record['timestamp'] > until):
                    continue
                prior = records.get(record['response_id'])
                if prior:
                    record['first_timestamp'] = min(record['first_timestamp'], prior['first_timestamp'])
                    if record['model'] != prior['model']:
                        record['issues'].append('inconsistent:response_model_changed')
                    if selection['provider'] != 'claude' and record['native'] != prior['native']:
                        record['issues'].append('inconsistent:immutable_response_changed')
                    record['issues'].extend(i for i in prior['issues'] if i.startswith('inconsistent:'))
                records[record['response_id']] = record
                if len(records) > MAX_RECORDS:
                    raise ValueError('source exceeds response limit')
            except (ValueError, TypeError, KeyError, AttributeError) as exc:
                if str(exc) == 'source exceeds response limit':
                    raise
                # Do not echo raw input or message contents in diagnostics.
                issues.append(f'invalid:record:{line_no}:{type(exc).__name__}')
                if len(issues) > 1000:
                    raise ValueError('source exceeds diagnostic limit')
        after = path.stat()
        if (after.st_dev, after.st_ino) != (stat.st_dev, stat.st_ino) or after.st_size < size:
            raise ValueError('source replaced or truncated during collection')
    role, role_source = native_role(selection['provider'], path, selection['session_id'], metadata)
    if role is None:
        issues.append('missing:role_lineage')
    elif role != selection['role']:
        issues.append('inconsistent:selected_role_disagrees_with_lineage')
    return dict(records=list(records.values()), issues=issues, role=role, role_source=role_source,
                excluded_api_error_rows=excluded_api_error_rows,
                boundary=dict(bytes=size, sha256=digest.hexdigest(), device=stat.st_dev, inode=stat.st_ino))
