"""Explicit self-selection and source lineage for local usage collection."""
import os
import json
from pathlib import Path


def native_role(provider, path, session_id, metadata):
    if provider == 'codex':
        source = metadata.get('source')
        if source in ('cli', 'vscode', 'exec', 'appServer'):
            return 'main', 'session_meta.source'
        if isinstance(source, dict) and 'subagent' in source:
            return 'subagent', 'session_meta.source.subagent'
    elif provider == 'claude':
        path = Path(path)
        if path.stem == session_id:
            return 'main', 'selected_session_transcript'
        if any(parent.name == 'subagents' and parent.parent.name == session_id for parent in path.parents):
            return 'subagent', 'selected_session_subagents_directory'
    return None, 'unavailable_lineage'


def self_manifest(provider, home=None, project=None, include_subagents=False):
    variable = {'codex': 'CODEX_THREAD_ID', 'claude': 'CLAUDE_CODE_SESSION_ID',
                'deepseek': 'DSH_SESSION_ID'}[provider]
    session_id = os.environ.get(variable)
    if not session_id or any(c in session_id for c in '/\\\x00') or session_id in ('.', '..'):
        raise ValueError(f'{variable} must identify this session explicitly')
    if provider == 'deepseek':
        raise ValueError('DeepSeek usage reporting is deferred for this release; no transcript was read')
    if provider == 'codex':
        root = Path(home or os.environ.get('CODEX_HOME') or Path.home()/'.codex').expanduser()
        paths = list((root/'sessions').rglob('*' + session_id + '*.jsonl'))
        if include_subagents:
            raise ValueError('Codex subagents require explicit selected source paths; no host-wide lineage scan')
    else:
        root = Path(home or os.environ.get('CLAUDE_CONFIG_DIR') or Path.home()/'.claude').expanduser()
        project = str(Path(project or os.getcwd()).resolve())
        # Native project directory encoding replaces non-alphanumeric characters.
        import re
        slug = re.sub(r'[^a-zA-Z0-9]', '-', project)
        directory = root/'projects'/slug
        paths = [directory/(session_id + '.jsonl')]
    if len(paths) != 1 or not paths[0].is_file():
        raise ValueError('selected session transcript is absent or ambiguous; provide an explicit manifest')
    main = paths[0].resolve()
    main_role = 'main'
    if provider == 'codex':
        with main.open() as stream:
            first = json.loads(stream.readline(4 * 1024 * 1024))
        if first.get('type') != 'session_meta' or first.get('payload', {}).get('id') != session_id:
            raise ValueError('selected Codex transcript identity is not verified')
        main_role, _ = native_role(provider, main, session_id, first['payload'])
        if main_role is None:
            raise ValueError('selected Codex session lineage is unavailable')
    paths = [main]
    if provider == 'claude' and include_subagents:
        paths.extend(sorted((main.parent/session_id/'subagents').rglob('*.jsonl')))
    if len(paths) > 256:
        raise ValueError('selected session exceeds agent limit')
    return dict(schema_version=1, agents=[dict(agent_id=session_id if path == main else session_id + '/' + path.stem,
                session_id=session_id, provider=provider, path=str(path.resolve()),
                role=main_role if path == main else 'subagent') for path in paths])
