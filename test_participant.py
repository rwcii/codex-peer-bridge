import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import dsh_delivery
import session


PROJECT = Path(__file__).resolve().parent


SETTINGS = '''ui-onboarding:
  welcomeNoticeVersion: 2026-08-13.1
llm-deepseek:
  models:
    - id: deepseek-flash
      name: DeepSeek-V41-Flash
    - id: deepseek-v4-pro
      name: DeepSeek-V4-Pro
agent-default-model:
  provider: deepseek-official
  model: deepseek-flash
  reasoningEffort: high
'''


class DefaultModelTests(unittest.TestCase):
    def settings(self, text=SETTINGS, name='settings.yaml'):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        path = Path(directory.name) / name
        path.write_text(text)
        return path

    def test_reads_the_configured_default_not_a_catalog_entry(self):
        # The first `models:` entry is deepseek-flash too, so this also proves the
        # reader stays inside the agent-default-model block.
        self.assertEqual(dsh_delivery.default_model(self.settings()), 'deepseek-flash')

    def test_absent_file_and_block_return_none(self):
        self.assertIsNone(dsh_delivery.default_model(Path('/nonexistent/settings.yaml')))
        self.assertIsNone(dsh_delivery.default_model(self.settings('llm-deepseek:\n  models: []\n')))

    def test_quoted_value_is_unwrapped(self):
        text = 'agent-default-model:\n  model: "deepseek-v4-pro"\n'
        self.assertEqual(dsh_delivery.default_model(self.settings(text)), 'deepseek-v4-pro')

    def test_settings_path_follows_the_harness_home(self):
        self.assertEqual(dsh_delivery.settings_path('/home/example'),
                         Path('/home/example') / 'settings.yaml')
        self.assertIsNone(dsh_delivery.settings_path(''))

    def test_a_broken_settings_file_never_raises(self):
        with tempfile.TemporaryDirectory() as temp:
            link = Path(temp) / 'settings.yaml'
            link.symlink_to(Path(temp) / 'absent.yaml')
            self.assertIsNone(dsh_delivery.default_model(link))


class PeerNameTests(unittest.TestCase):
    def test_codex_names_are_unchanged_without_a_model(self):
        self.assertEqual(session.peer_base('codex', None, 'fingerprint'), 'codex-fingerprint')

    def test_deepseek_model_replaces_rather_than_repeats_the_agent(self):
        self.assertEqual(session.peer_base('deepseek', 'deepseek-v4-pro', 'fingerprint'),
                         'deepseek-v4-pro-fingerprint')
        self.assertEqual(session.peer_base('deepseek', 'deepseek-v4-flash', 'fingerprint'),
                         'deepseek-v4-flash-fingerprint')

    def test_deepseek_without_a_known_model_still_names_the_agent(self):
        self.assertEqual(session.peer_base('deepseek', None, 'fingerprint'), 'deepseek-fingerprint')

    def test_unrelated_model_keeps_both_segments(self):
        self.assertEqual(session.peer_base('codex', 'gpt-5-codex', 'repo'), 'codex-gpt-5-codex-repo')

    def test_model_ids_are_slugged_and_bounded(self):
        base = session.peer_base('deepseek', 'DeepSeek/V4 Pro!', 'repo')
        self.assertEqual(base, 'deepseek-v4-pro-repo')
        self.assertLessEqual(len(session.peer_base('codex', 'x' * 200, 'repo')), 32 + len('codex-repo-') + 1)

    def test_details_uses_agent_and_model(self):
        config = {'state_root': '/state'}
        state, name, key = session.details(Path('/app'), config, 'thread-a', '/repo/fingerprint',
                                           'deepseek', 'deepseek-v4-pro')
        self.assertEqual(name, f'deepseek-v4-pro-fingerprint-{key[:2]}')
        self.assertTrue(str(state).startswith('/state/sessions/'))

    def test_details_without_a_model_keeps_the_codex_form(self):
        config = {'state_root': '/state'}
        _, name, key = session.details(Path('/app'), config, 'thread-a', '/repo/fingerprint')
        self.assertEqual(name, f'codex-fingerprint-{key[:2]}')


class RegistrationIdentityTests(unittest.TestCase):
    def test_registration_records_agent_and_model(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            state = root / 'sessions' / session.identity('session-abc')
            state.mkdir(parents=True)
            with patch('session.peers', return_value=[]):
                saved = session.save_registration(state, root, 'session-abc', '/repo',
                                                  agent='deepseek', model='deepseek-v4-pro')
            self.assertEqual(saved['agent'], 'deepseek')
            self.assertEqual(saved['model'], 'deepseek-v4-pro')
            self.assertRegex(saved['name'], r'^deepseek-v4-pro-repo-[a-f0-9]{2}$')
            self.assertEqual(json.loads((state / 'session.json').read_text()), saved)


class GuidanceTests(unittest.TestCase):
    """Managed guidance is how a participant actually joins, so each kind must get
    its own marked section and they must be independently removable."""

    def test_deepseek_section_is_distinct_and_names_the_agent(self):
        import codex_instructions as guidance
        deepseek = guidance.section(Path('/app'), 'deepseek')
        codex = guidance.section(Path('/app'), 'codex')
        self.assertIn(guidance.MARKERS['deepseek'][0], deepseek)
        self.assertIn('--agent deepseek', deepseek)
        self.assertIn('DSH_SESSION_ID', deepseek)
        self.assertIn('permission laundering', deepseek)
        self.assertNotIn('CODEX_THREAD_ID', deepseek)
        self.assertNotIn(guidance.MARKERS['deepseek'][0], codex)
        self.assertIn('CODEX_THREAD_ID', codex)

    def test_both_sections_coexist_and_remove_independently(self):
        import codex_instructions as guidance
        with tempfile.TemporaryDirectory() as temp:
            home = Path(temp)
            home.joinpath('AGENTS.md').write_text('Personal guidance.\n')
            guidance.update(home, Path('/app'))
            guidance.update(home, Path('/app'), agent='deepseek')
            text = home.joinpath('AGENTS.md').read_text()
            self.assertIn(guidance.MARKERS['codex'][0], text)
            self.assertIn(guidance.MARKERS['deepseek'][0], text)
            self.assertEqual(text.count('## Local peer messaging'), 2)

            guidance.update(home, Path('/app'), remove=True, agent='deepseek')
            text = home.joinpath('AGENTS.md').read_text()
            self.assertNotIn(guidance.MARKERS['deepseek'][0], text)
            self.assertIn(guidance.MARKERS['codex'][0], text,
                          'removing one participant must not remove the other')

    def test_repeated_deepseek_update_is_idempotent(self):
        import codex_instructions as guidance
        with tempfile.TemporaryDirectory() as temp:
            home = Path(temp)
            guidance.update(home, Path('/one'), agent='deepseek')
            first = home.joinpath('AGENTS.md').read_text()
            guidance.update(home, Path('/one'), agent='deepseek')
            self.assertEqual(home.joinpath('AGENTS.md').read_text(), first)

    def test_unknown_participant_is_refused(self):
        import codex_instructions as guidance
        with tempfile.TemporaryDirectory() as temp:
            with self.assertRaises(ValueError):
                guidance.update(Path(temp), Path('/app'), agent='not-an-agent')


class InstallRecordTests(unittest.TestCase):
    """`uninstall` removes exactly the managed sections this record names, so the
    record has to say which participants were configured."""

    def install(self, root, *flags):
        env = dict(os.environ, CLAUDE_CONFIG_DIR=str(root / 'claude'))
        for name in ('DSH_SESSION_ID', 'CODEX_THREAD_ID'):
            env.pop(name, None)
        for home in ('codex', 'dsh'):
            (root / home).mkdir(parents=True, exist_ok=True)
            if not (root / home / 'AGENTS.md').exists():
                (root / home / 'AGENTS.md').write_text(f'Personal {home} guidance.\n')
        subprocess.run([sys.executable, str(PROJECT / 'scripts/install.py'), *flags, '--no-start',
                        '--codex', sys.executable, '--prefix', str(root / 'app'),
                        '--state-dir', str(root / 'state'), '--unit-dir', str(root / 'units'),
                        '--codex-home', str(root / 'codex'), '--dsh-home', str(root / 'dsh')],
                       check=True, capture_output=True, env=env)
        return json.loads((root / 'app' / 'install.json').read_text())

    def test_records_only_the_participants_that_were_configured(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.assertEqual(self.install(root, '--configure-codex')['participants'], ['codex'])
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.assertEqual(self.install(root, '--configure-deepseek')['participants'], ['deepseek'])
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            record = self.install(root, '--configure-codex', '--configure-deepseek')
            self.assertEqual(record['participants'], ['codex', 'deepseek'])
            self.assertIn('dsh_home', record)

    def test_sequential_installs_preserve_participants_and_uninstall_all_guidance(self):
        for first, second in (('codex', 'deepseek'), ('deepseek', 'codex')):
            with self.subTest(first=first), tempfile.TemporaryDirectory() as temp:
                root = Path(temp)
                self.install(root, '--configure-' + first)
                record = self.install(root, '--configure-' + second)
                self.assertEqual(record['participants'], ['codex', 'deepseek'])
                subprocess.run([sys.executable, str(root/'app/scripts/uninstall.py'),
                                '--prefix', str(root/'app')], check=True, capture_output=True)
                for home in ('codex', 'dsh'):
                    self.assertEqual((root/home/'AGENTS.md').read_text(),
                                     f'Personal {home} guidance.\n')

    def test_legacy_record_and_omitted_homes_survive_reconfiguration(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            record = self.install(root, '--configure-codex')
            del record['participants']
            (root/'app/install.json').write_text(json.dumps(record))
            subprocess.run([sys.executable, str(PROJECT/'scripts/install.py'),
                            '--configure-deepseek', '--no-start', '--codex', sys.executable,
                            '--prefix', str(root/'app'), '--state-dir', str(root/'state'),
                            '--unit-dir', str(root/'units')], check=True, capture_output=True)
            updated = json.loads((root/'app/install.json').read_text())
            self.assertEqual(updated['participants'], ['codex', 'deepseek'])
            self.assertEqual(updated['codex_home'], record['codex_home'])
            self.assertEqual(updated['dsh_home'], record['dsh_home'])
            self.assertIn('BEGIN DEEPSEEK PEER BRIDGE', (root/'dsh/AGENTS.md').read_text())

    def test_each_flag_writes_only_its_own_section(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            self.install(root, '--configure-deepseek')
            self.assertIn('BEGIN DEEPSEEK PEER BRIDGE', (root / 'dsh' / 'AGENTS.md').read_text())
            self.assertNotIn('PEER BRIDGE', (root / 'codex' / 'AGENTS.md').read_text(),
                             'a DeepSeek-only install must not touch the Codex guidance')


class RenameIdentityTests(unittest.TestCase):
    """`rename --repo` is documented as the way to change the project label. It must
    not silently change the participant kind or the advertised model, because the
    next `ensure` derives the delivery mechanism from them."""

    def session_command(self, app, *args, env):
        # A restricted PATH keeps the probe for a user systemd manager failing, so
        # `ensure` takes its manual_required path on every runner. Otherwise a host
        # where `systemctl --user` answers but where starting a unit is impossible
        # makes the outcome depend on the host rather than on the code under test.
        env = dict(env, PATH='/nonexistent')
        return subprocess.run([sys.executable, str(app / 'session.py'), *args],
                              capture_output=True, text=True, check=True, env=env)

    def test_bare_rename_preserves_the_registered_identity(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            env = dict(os.environ, CLAUDE_CONFIG_DIR=str(root / 'claude'))
            # Keep the environment from deciding the participant, so this exercises
            # only the "flag omitted" path.
            for name in ('DSH_SESSION_ID', 'CODEX_THREAD_ID', 'DSH_HOME'):
                env.pop(name, None)
            subprocess.run([sys.executable, str(PROJECT / 'scripts/install.py'), '--configure-codex',
                            '--no-start', '--codex', sys.executable, '--prefix', str(root / 'app'),
                            '--state-dir', str(root / 'state'), '--unit-dir', str(root / 'units'),
                            '--codex-home', str(root / 'codex')],
                           check=True, capture_output=True, env=env)
            app = root / 'app'
            self.session_command(app, 'ensure', '--agent', 'deepseek', '--thread', 't-two',
                                 '--repo', '/repo/fingerprint', env=env)
            state = root / 'state' / 'sessions' / session.identity('t-two')
            before = json.loads((state / 'session.json').read_text())
            self.assertEqual(before['agent'], 'deepseek')
            self.assertRegex(before['name'], r'^deepseek-fingerprint-[a-f0-9]{2}$')

            self.session_command(app, 'rename', '--thread', 't-two', '--repo', '/repo/renamed', env=env)
            after = json.loads((state / 'session.json').read_text())
            self.assertEqual(after['agent'], 'deepseek',
                             'a bare rename must not turn a DeepSeek peer into a Codex one')
            self.assertEqual(after['model'], before['model'])
            self.assertEqual(after['repo'], '/repo/renamed')
            self.assertEqual(after['thread'], before['thread'])

    def test_explicit_model_on_rename_does_correct_the_advertised_model(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            env = dict(os.environ, CLAUDE_CONFIG_DIR=str(root / 'claude'))
            for name in ('DSH_SESSION_ID', 'CODEX_THREAD_ID', 'DSH_HOME'):
                env.pop(name, None)
            subprocess.run([sys.executable, str(PROJECT / 'scripts/install.py'), '--configure-codex',
                            '--no-start', '--codex', sys.executable, '--prefix', str(root / 'app'),
                            '--state-dir', str(root / 'state'), '--unit-dir', str(root / 'units'),
                            '--codex-home', str(root / 'codex')],
                           check=True, capture_output=True, env=env)
            app = root / 'app'
            self.session_command(app, 'ensure', '--agent', 'deepseek', '--thread', 't-three',
                                 '--repo', '/repo/fingerprint', env=env)
            self.session_command(app, 'rename', '--thread', 't-three', '--repo', '/repo/fingerprint',
                                 '--agent', 'deepseek', '--model', 'deepseek-v4-pro', env=env)
            state = root / 'state' / 'sessions' / session.identity('t-three')
            after = json.loads((state / 'session.json').read_text())
            self.assertEqual(after['model'], 'deepseek-v4-pro')
            self.assertRegex(after['name'], r'^deepseek-v4-pro-fingerprint-[a-f0-9]{2}$')


class NotifyCommandTests(unittest.TestCase):
    config = {'codex': '/usr/bin/codex', 'dsh_url': 'http://127.0.0.1:51992',
              'dsh_credentials': '/home/example/.credentials.yaml'}

    def notify(self, agent, model=None):
        return session.notify_command(Path('/app'), self.config, Path('/state'), 'target',
                                      '/repo', 'name', agent, model)

    def test_codex_uses_the_cli(self):
        command = self.notify('codex')
        self.assertIn('--codex', command)
        self.assertNotIn('--dsh-url', command)

    def test_deepseek_passes_the_harness_endpoint(self):
        command = self.notify('deepseek')
        self.assertIn('--agent', command)
        self.assertEqual(command[command.index('--agent') + 1], 'deepseek')
        self.assertIn('--dsh-url', command)
        self.assertIn('--dsh-credentials', command)
        self.assertNotIn('--codex', command)

    def test_absent_harness_config_is_omitted_rather_than_passed_as_none(self):
        config = {'codex': '/usr/bin/codex'}
        command = session.notify_command(Path('/app'), config, Path('/state'), 'target',
                                        '/repo', 'name', 'deepseek')
        self.assertNotIn('--dsh-url', command)
        self.assertNotIn('--dsh-credentials', command)


class ServiceUnitTests(unittest.TestCase):
    def test_deepseek_notify_unit_carries_the_harness_endpoint(self):
        from scripts.install import units
        rendered = units(Path('/app'), Path('/state'), 'session-abc', 'deepseek-v4-pro-repo-1a',
                         '/repo', '/usr/bin/python3', '/usr/bin/codex',
                         agent='deepseek', model='deepseek-v4-pro',
                         dsh_url='http://127.0.0.1:51992', dsh_credentials='/home/u/.credentials.yaml')
        content = rendered['codex-peer-notify.service']
        self.assertIn('--agent', content)
        self.assertIn('deepseek', content)
        self.assertIn('--dsh-url', content)
        self.assertIn('http://127.0.0.1:51992', content)
        self.assertNotIn('--codex', content)

    def test_deepseek_session_unit_carries_the_participant_and_model(self):
        from scripts.install import units
        rendered = units(Path('/app'), Path('/state'), 'session-abc', 'deepseek-v4-pro-repo-1a',
                         '/repo', '/usr/bin/python3', '/usr/bin/codex', instance='a' * 16,
                         agent='deepseek', model='deepseek-v4-pro')
        content = rendered['codex-peer-session-' + 'a' * 16 + '.service']
        self.assertIn('--agent', content)
        self.assertIn('--model', content)
        self.assertIn('deepseek-v4-pro', content)

    def test_codex_unit_is_unchanged(self):
        from scripts.install import units
        rendered = units(Path('/app'), Path('/state'), 'thread-a', 'codex-repo-1a',
                         '/repo', '/usr/bin/python3', '/usr/bin/codex')
        self.assertIn('--codex', rendered['codex-peer-notify.service'])
        self.assertNotIn('--agent', rendered['codex-peer-notify.service'])


if __name__ == '__main__':
    unittest.main()
