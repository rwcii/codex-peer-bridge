#!/usr/bin/env python3
"""Explicit local per-agent usage reports. No hooks, messaging or cost calculation."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import sys

from usage_selection import self_manifest
from usage_sources import ADAPTER_VERSION, COMPONENTS, FIELDS, collect, normalize, text, timestamp


def read_json(path):
    with Path(path).open('rb') as stream:
        data = stream.read(4 * 1024 * 1024 + 1)
    if len(data) > 4 * 1024 * 1024:
        raise ValueError('JSON document exceeds size limit')
    return json.loads(data)


def selections(manifest):
    if manifest.get('schema_version') != 1:
        raise ValueError('unsupported manifest schema')
    agents = manifest.get('agents')
    if not isinstance(agents, list) or not 1 <= len(agents) <= 256:
        raise ValueError('select between 1 and 256 agent sessions')
    seen, sources, agent_ids, result = set(), set(), set(), []
    for item in agents:
        selected = {key: text(item.get(key), key) for key in
                    ('agent_id', 'session_id', 'provider', 'path', 'role')}
        if selected['provider'] not in ('codex', 'claude'):
            raise ValueError('provider must be codex or claude; DeepSeek usage is deferred for this release')
        if selected['role'] not in ('main', 'subagent'):
            raise ValueError('role must be main or subagent')
        path = Path(selected['path'])
        if not path.is_absolute():
            raise ValueError('source path must be absolute')
        selected['path'] = str(path.resolve())
        child = None
        if selected['provider'] == 'claude':
            from usage_selection import native_role
            if native_role('claude', path, selected['session_id'], {})[0] == 'subagent':
                child = path.stem
        identity = (selected['provider'], selected['session_id'], child)
        if selected['agent_id'] in agent_ids:
            raise ValueError('each selected agent identity must be unique')
        agent_ids.add(selected['agent_id'])
        if identity in seen or selected['path'] in sources:
            raise ValueError('duplicate source/session selection would double count usage')
        seen.add(identity)
        sources.add(selected['path'])
        result.append(selected)
    return result


def inventory(state_root):
    """List registered participants only in the explicitly supplied installation."""
    root = Path(state_root).resolve()
    participants = []
    for path in sorted((root/'sessions').glob('*/session.json')):
        if len(participants) >= 256:
            raise ValueError('participant limit exceeded')
        data = read_json(path)
        participants.append({key: data.get(key) for key in ('thread', 'agent', 'model', 'name', 'repo')})
    return dict(schema_version=1, state_root=str(root), participants=participants,
                source_selection='Select transcript paths explicitly; inventory reads no transcripts.')


def capture(manifest, until=None):
    agents = selections(manifest)
    captured, seen_files = [], set()
    for agent in agents:
        source = collect(agent, until)
        identity = (source['boundary']['device'], source['boundary']['inode'])
        if identity in seen_files:
            raise ValueError('source aliases would double count usage')
        seen_files.add(identity)
        captured.append((agent, source))
    return captured


def begin(manifest, work_block):
    sources = []
    for agent, source in capture(manifest):
        if source['issues']:
            raise ValueError('cannot mark a source with invalid or incomplete lines')
        sources.append(dict(selection=agent, boundary=source['boundary'],
                            response_ids=[r['response_id'] for r in source['records']]))
    return dict(schema_version=1, work_block=text(work_block, 'work_block'), sources=sources,
                created_at=datetime.datetime.now(datetime.timezone.utc).isoformat())


def verify_boundary(agent, saved):
    boundary = saved['boundary']
    path = Path(agent['path'])
    stat = path.stat()
    if (stat.st_dev, stat.st_ino) != (boundary['device'], boundary['inode']):
        raise ValueError('marked source replaced; boundary cannot be recovered')
    remaining = boundary['bytes']
    if type(remaining) is not int or not 0 <= remaining <= stat.st_size:
        raise ValueError('marked source truncated; boundary cannot be recovered')
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        while remaining:
            chunk = stream.read(min(1024 * 1024, remaining))
            if not chunk:
                raise ValueError('marked source truncated')
            digest.update(chunk)
            remaining -= len(chunk)
    if digest.hexdigest() != boundary['sha256']:
        raise ValueError('marked source history changed; boundary cannot be recovered')


def report(manifest, work_block=None, marker=None, since=None, until=None):
    if marker is not None:
        if since is not None or marker.get('schema_version') != 1:
            raise ValueError('marker and retrospective start are mutually exclusive')
        work_block = text(marker.get('work_block'), 'work_block')
    else:
        text(work_block, 'work_block')
        if since is None or until is None:
            raise ValueError('retrospective report requires since and until')
    start = timestamp(since) if since is not None else None
    end = timestamp(until) if until is not None else datetime.datetime.now(datetime.timezone.utc).timestamp()
    if start is not None and start >= end:
        raise ValueError('since must precede until')
    captured = capture(manifest, end)
    if marker is not None and [a for a, _ in captured] != [s['selection'] for s in marker['sources']]:
        raise ValueError('marker selection differs from manifest')
    rows, evidence, all_issues = [], [], []
    for index, (agent, source) in enumerate(captured):
        excluded = set()
        if marker is not None:
            saved = marker['sources'][index]
            verify_boundary(agent, saved)
            excluded = set(saved['response_ids'])
        selected = []
        issues = list(source['issues'])
        for record in source['records']:
            at = record['timestamp']
            if at > end:
                continue
            if record['response_id'] in excluded:
                if at > timestamp(marker['created_at']):
                    issues.append('incomplete:response_crosses_start_boundary')
                continue
            if start is not None and at <= start:
                continue
            if start is not None and record['first_timestamp'] <= start:
                issues.append('incomplete:response_crosses_start_boundary')
                continue
            selected.append(record)
        groups = {}
        for record in selected:
            groups.setdefault(record['model'], []).append(record)
        for model, records in groups.items():
            row_issues = sorted(set(i for r in records for i in r['issues']))
            counters = {key: (None if any(r['counters'][key] is None for r in records)
                              else sum(r['counters'][key] for r in records)) for key in FIELDS}
            status = ('inconsistent' if any(i.startswith(('invalid:', 'inconsistent:')) for i in row_issues)
                      else 'incomplete' if row_issues else 'complete')
            rows.append(dict(agent_id=agent['agent_id'], session_id=agent['session_id'], model=model,
                             role=source['role'], role_source=source['role_source'], **counters,
                             status=status, issues=row_issues, responses=len(records),
                             native_totals={key: (None if any(r['native'].get(key) is None for r in records)
                                                 else sum(r['native'][key] for r in records))
                                            for key in records[0]['native']
                                            if all(r['native'].get(key) is None or type(r['native'][key]) is int
                                                   for r in records)}))
            issues.extend(row_issues)
        if not selected:
            issues.append('incomplete:no_observed_responses')
        evidence.append(dict(selection=agent, adapter_version=ADAPTER_VERSION,
                             boundary=source['boundary'], selected_responses=len(selected),
                             issues=sorted(set(issues))))
        all_issues.extend(issues)
    status = ('inconsistent' if any(i.startswith(('invalid:', 'inconsistent:')) for i in all_issues)
              else 'incomplete' if all_issues else 'complete')
    return dict(schema_version=1, work_block=work_block, status=status, rows=rows, evidence=evidence,
                boundary=dict(kind='response_ids_after_marker' if marker else 'response_timestamp_window',
                              since=since, until=datetime.datetime.fromtimestamp(end, datetime.timezone.utc).isoformat()),
                coverage='Selected observed responses only; time windows do not establish causal task attribution.')


def combine(reports):
    if not reports or len(reports) > 256:
        raise ValueError('combine requires between 1 and 256 reports')
    identities, rows, evidence = set(), [], []
    block = reports[0].get('work_block')
    text(block, 'work_block')
    statuses = []
    for value in reports:
        if value.get('schema_version') != 1 or value.get('work_block') != block:
            raise ValueError('combined reports must share schema and work block')
        if value.get('status') not in ('complete', 'incomplete', 'inconsistent'):
            raise ValueError('invalid report status')
        current = {item['selection']['agent_id'] for item in value['evidence']}
        if identities & current:
            raise ValueError('duplicate agent identity across reports')
        identities.update(current)
        for row in value['rows']:
            if row['agent_id'] not in current:
                raise ValueError('report row has no source identity')
            _, issues = normalize('normalized', row)
            if any(i.startswith(('invalid:', 'inconsistent:')) for i in issues):
                statuses.append('inconsistent')
            elif issues or row.get('status') == 'incomplete':
                statuses.append('incomplete')
            elif row.get('status') == 'inconsistent':
                statuses.append('inconsistent')
        rows.extend(value['rows'])
        evidence.extend(value['evidence'])
        statuses.append(value['status'])
    status = 'inconsistent' if 'inconsistent' in statuses else 'incomplete' if 'incomplete' in statuses else 'complete'
    return dict(schema_version=1, work_block=block, status=status, rows=rows, evidence=evidence,
                boundaries=[value.get('boundary') for value in reports],
                coverage='Combined selected reports; sources were not reread.')


def table(value):
    headings = ('Model', 'Role', 'Tokens In', 'Tokens Out', 'Cache Write', 'Cache Read', 'Reasoning', 'Total')
    lines = []
    def cell(value):
        return str(value if value is not None else 'unavailable').replace('|', '\\|').replace('\n', ' ').replace('\r', ' ')
    for row in value['rows']:
        lines.extend(['Agent: ' + cell(row['agent_id']) + ' (' + row['status'] + ')', '',
                      '| ' + ' | '.join(headings) + ' |',
                      '| ' + ' | '.join(['---'] * 8) + ' |',
                      '| ' + ' | '.join(cell(row.get(key)) for key in ('model', 'role') + FIELDS) + ' |', ''])
    return '\n'.join(lines) or 'No observed responses; counters are unavailable.'


def write_new(path, value):
    # Never overwrite a marker or an existing report by accident.
    encoded = json.dumps(value, indent=2) + '\n'
    if len(encoded.encode()) > 4 * 1024 * 1024:
        raise ValueError('output document exceeds size limit')
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'w') as stream:
        stream.write(encoded)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    self_parser = commands.add_parser('self')
    self_parser.add_argument('--provider', choices=['codex', 'claude', 'deepseek'], required=True)
    self_parser.add_argument('--home')
    self_parser.add_argument('--project')
    self_parser.add_argument('--include-subagents', action='store_true')
    combining = commands.add_parser('combine')
    combining.add_argument('reports', nargs='+')
    combining.add_argument('--output')
    rendering = commands.add_parser('table')
    rendering.add_argument('report')
    listing = commands.add_parser('participants')
    listing.add_argument('--state-root', required=True)
    for name in ('begin', 'report'):
        sub = commands.add_parser(name)
        sub.add_argument('--manifest', required=True)
        sub.add_argument('--work-block', required=name == 'begin')
        sub.add_argument('--output', required=name == 'begin')
        if name == 'report':
            sub.add_argument('--marker')
            sub.add_argument('--since')
            sub.add_argument('--until')
    args = parser.parse_args()
    try:
        if args.command == 'self':
            value = self_manifest(args.provider, args.home, args.project, args.include_subagents)
        elif args.command == 'combine':
            value = combine([read_json(path) for path in args.reports])
        elif args.command == 'table':
            print(table(read_json(args.report)))
            return 0
        elif args.command == 'participants':
            value = inventory(args.state_root)
        elif args.command == 'begin':
            value = begin(read_json(args.manifest), args.work_block)
        else:
            value = report(read_json(args.manifest), args.work_block,
                           read_json(args.marker) if args.marker else None, args.since, args.until)
        if getattr(args, 'output', None):
            if args.command == 'begin':
                parent = Path(args.output).resolve().parent
                info = parent.stat()
                if info.st_uid != os.getuid() or info.st_mode & 0o077:
                    raise ValueError('marker directory must be owned by this user and mode 0700')
                if any((ancestor/'.git').exists() for ancestor in (parent, *parent.parents)):
                    raise ValueError('markers belong in private runtime state outside repositories')
            write_new(args.output, value)
        else:
            print(json.dumps(value, indent=2))
    except (OSError, ValueError, TypeError, KeyError, AttributeError) as exc:
        print(json.dumps(dict(ok=False, error=type(exc).__name__, detail=str(exc))), file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
