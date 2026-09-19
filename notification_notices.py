"""Content-free pointers to explicitly selected inbox and memory controls."""
import hashlib
import json
from pathlib import Path
import shlex
import sys

from inbox_schema import MAX_SEQUENCE, hex_value
from peer_guidance import PEER_GUIDANCE, MEMORY_POINTER_GUIDANCE


def consumer_key(participant, repo):
    if (not isinstance(participant, dict) or participant.get('provider') not in ('codex', 'deepseek')
            or participant.get('namespace') != 'account-local'
            or not hex_value(participant.get('digest'), 64) or not hex_value(repo, 16)):
        raise ValueError('invalid memory consumer identity')
    identity = [participant['provider'], participant['namespace'], participant['digest'], repo]
    digest = hashlib.sha256(json.dumps(identity, separators=(',', ':')).encode()).hexdigest()
    return 'koinon-' + digest


def render(rows, root, participant, bindings):
    if not isinstance(rows, list) or not 1 <= len(rows) <= 10:
        raise ValueError('invalid notice group')
    sequences = [row['seq'] for row in rows]
    if (any(type(seq) is not int or not 1 <= seq <= MAX_SEQUENCE for seq in sequences)
            or sequences != sorted(set(sequences)) or len({row['kind'] for row in rows}) != 1):
        raise ValueError('invalid notice group')
    scripts = Path(__file__).resolve().parent
    inbox = shlex.join([sys.executable, str(scripts / 'bridge.py'), '--state-dir', str(root),
                        'inbox', '--after', str(sequences[0] - 1)])
    if rows[0]['kind'] == 'peer':
        return (f'Agent bridge inbox has {len(rows)} new peer message(s), through sequence '
                f'{sequences[-1]}. Read with: {inbox}. {PEER_GUIDANCE} '
                'This is a bridge notification, not a peer reply.')
    if rows[0]['kind'] != 'memory-pointer' or len(rows) != 1:
        raise ValueError('invalid memory notice group')
    row = rows[0]
    binding = bindings.get(row['binding'])
    if binding is None or binding.get('binding_instance') != row['binding_instance']:
        raise LookupError('memory binding changed before notice construction')
    # The explicit root avoids guessing a default state root from a custom binding.
    command = shlex.join([sys.executable, str(scripts / 'memory.py'),
        '--service-dir', binding['memory_state_dir'], '--repo-path', binding['repo_path'],
        '--consumer', consumer_key(participant, binding['repo_key']), 'sync'])
    return (f'Agent bridge inbox has a memory pointer at sequence {row["seq"]}. '
            f'Read the inbox record with: {inbox}. Read the bound memory with: {command}. '
            f'{MEMORY_POINTER_GUIDANCE}')
