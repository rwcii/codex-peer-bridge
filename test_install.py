import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('installer',Path(__file__).parent/'scripts/install.py')
installer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(installer)

class InstallTests(unittest.TestCase):
    def test_unit_arguments(self):
        quoted = installer.unit_arg('/path with spaces/%user/$value')
        self.assertIn('%%user',quoted)
        self.assertIn('$$value',quoted)
        self.assertTrue(quoted.startswith('"'))
        with self.assertRaises(ValueError):
            installer.unit_arg('bad\nExecStart=bad')

    def test_ownership_refusals_do_not_restart_bridge_or_notifier_services(self):
        args = (Path('/app'), Path('/state'), 'target', 'peer', '/repo',
                '/usr/bin/python3', '/bin/codex')
        legacy = installer.units(*args)
        supervised = next(iter(installer.units(*args, instance='a'*16).values()))
        for name, unit in (*legacy.items(), ('supervisor', supervised)):
            excluded = next(line.split('=',1)[1].split() for line in unit.splitlines()
                            if line.startswith('RestartPreventExitStatus='))
            self.assertEqual(set(excluded), {'70','78'})
            self.assertNotIn('75', excluded)
            self.assertIn('\nRestart=on-failure\n', unit)
            self.assertNotIn('SuccessExitStatus=', unit)

    def test_unrelated_unit_refused(self):
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp)/'codex-peer-bridge.service'
            path.write_text('[Service]\nExecStart=/unrelated\n')
            with self.assertRaises(ValueError):
                installer.check_owned_unit(path)
            path.write_text(installer.MARKER+'[Service]\n')
            installer.check_owned_unit(path)
            link=Path(temp)/'symlink.service'
            link.symlink_to(path)
            with self.assertRaises(ValueError):
                installer.check_owned_unit(link)

    def test_isolated_install_without_services(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)
            result=subprocess.run([sys.executable,'scripts/install.py','--thread','test-thread',
                '--codex',sys.executable,'--prefix',str(root/'app'),'--state-dir',str(root/'state'),
                '--unit-dir',str(root/'units'),'--no-start'],capture_output=True,text=True)
            self.assertEqual(result.returncode,0,result.stderr)
            self.assertTrue((root/'app/docs/INSTALL.md').exists())
            self.assertTrue((root/'app/docs/NOTIFIER.md').exists())
            self.assertTrue((root/'app/LICENSE').exists())
            for script in ('bridge.py', 'notify.py', 'session.py', 'memory.py'):
                check = subprocess.run([sys.executable, str(root/'app'/script), '--help'],
                                       cwd=root, capture_output=True, text=True)
                self.assertEqual(check.returncode, 0, check.stderr)
            unit=(root/'units/koinon-notify.service').read_text()
            self.assertIn('test-thread',unit)
            self.assertIn('--codex',unit)
            self.assertFalse((root/'state').exists())

    def isolated_command(self, root, *mode):
        return [sys.executable, 'scripts/install.py', *mode, '--no-start',
                '--prefix', str(root/'app'), '--state-dir', str(root/'state'),
                '--unit-dir', str(root/'units'), '--codex-home', str(root/'codex'),
                '--dsh-home', str(root/'dsh')]

    def test_deepseek_only_install_and_repeat_without_codex(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            env = dict(os.environ, PATH=str(root/'empty-path'))
            command = self.isolated_command(root, '--configure-deepseek')
            for _ in range(2):
                result = subprocess.run(command, env=env, capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stderr)
                config = json.loads((root/'app/install.json').read_text())
                self.assertEqual(config['participants'], ['deepseek'])
                self.assertIsNone(config['codex'])
                self.assertEqual(config['state_root'], str(root/'state'))
                self.assertTrue((root/'dsh/AGENTS.md').exists())
                self.assertFalse((root/'codex').exists())
                self.assertFalse((root/'units').exists())

    def test_codex_modes_refuse_missing_executable_before_writing(self):
        modes = [('--configure-codex',), ('--configure-codex', '--configure-deepseek'),
                 ('--thread', 'synthetic-thread'),
                 ('--configure-deepseek', '--thread', 'synthetic-thread')]
        for mode in modes:
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as temp:
                root = Path(temp).resolve()
                result = subprocess.run(self.isolated_command(root, *mode),
                    env=dict(os.environ, PATH=str(root/'empty-path')),
                    capture_output=True, text=True)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn('executable absolute --codex', result.stderr)
                self.assertEqual(list(root.iterdir()), [])

    def test_deepseek_install_refuses_invalid_codex_and_codex_session(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            env = dict(os.environ, PATH=str(root/'empty-path'))
            command = self.isolated_command(root, '--configure-deepseek')
            failed = subprocess.run(command + ['--codex', 'relative'], env=env,
                                    capture_output=True, text=True)
            self.assertEqual(failed.returncode, 2)
            self.assertEqual(list(root.iterdir()), [])
            installed = subprocess.run(command, env=env, capture_output=True, text=True)
            self.assertEqual(installed.returncode, 0, installed.stderr)
            before = (root/'app/install.json').read_bytes()
            attempted = subprocess.run(self.isolated_command(root, '--configure-codex'),
                                       env=env, capture_output=True, text=True)
            self.assertEqual(attempted.returncode, 2)
            self.assertEqual((root/'app/install.json').read_bytes(), before)
            session_command = [sys.executable, str(root/'app/session.py'), 'ensure',
                               '--agent', 'codex', '--thread', 'synthetic-thread']
            refused = subprocess.run(session_command, env=env, capture_output=True, text=True)
            self.assertNotEqual(refused.returncode, 0)
            self.assertIn('Codex participant requires', refused.stdout + refused.stderr)
            self.assertFalse((root/'state').exists())
            import session
            state = root/'state/sessions'/session.identity('synthetic-thread')
            state.mkdir(parents=True, mode=0o700)
            registration = state/'session.json'
            registration.write_text(json.dumps(dict(thread='synthetic-thread', agent='codex',
                                                    name='synthetic-peer', repo='/synthetic')))
            before = registration.read_bytes()
            refused = subprocess.run(session_command, env=env, capture_output=True, text=True)
            self.assertNotEqual(refused.returncode, 0)
            self.assertIn('Codex participant requires', refused.stdout + refused.stderr)
            self.assertEqual(registration.read_bytes(), before)
            self.assertFalse((root/'units').exists())
            enabled = subprocess.run(self.isolated_command(root, '--configure-codex',
                                    '--codex', sys.executable), env=env,
                                    capture_output=True, text=True)
            self.assertEqual(enabled.returncode, 0, enabled.stderr)

    def test_stale_saved_codex_falls_back_to_path(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            old = root/'old-codex'
            old.write_text('#!/bin/sh\nexit 0\n')
            old.chmod(0o700)
            command = self.isolated_command(root, '--configure-codex')
            installed = subprocess.run(command + ['--codex', str(old)],
                                       capture_output=True, text=True)
            self.assertEqual(installed.returncode, 0, installed.stderr)
            old.unlink()
            binary_dir = root/'bin'
            binary_dir.mkdir()
            replacement = binary_dir/'codex'
            replacement.write_text('#!/bin/sh\nexit 0\n')
            replacement.chmod(0o700)
            env = dict(os.environ, PATH=str(binary_dir))
            upgraded = subprocess.run(command, env=env, capture_output=True, text=True)
            self.assertEqual(upgraded.returncode, 0, upgraded.stderr)
            config = json.loads((root/'app/install.json').read_text())
            self.assertEqual(config['codex'], str(replacement))
            refused = subprocess.run(command + ['--codex', str(old)], env=env,
                                     capture_output=True, text=True)
            self.assertEqual(refused.returncode, 2)

    def test_saved_codex_preserved_and_explicit_invalid_override_refused(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            env = dict(os.environ, PATH=str(root/'empty-path'))
            command = self.isolated_command(root, '--configure-codex', '--codex', sys.executable)
            first = subprocess.run(command, env=env, capture_output=True, text=True)
            self.assertEqual(first.returncode, 0, first.stderr)
            command = [sys.executable, 'scripts/install.py', '--configure-deepseek',
                       '--prefix', str(root/'app'), '--no-start']
            repeated = subprocess.run(command, env=env, capture_output=True, text=True)
            self.assertEqual(repeated.returncode, 0, repeated.stderr)
            config_path = root/'app/install.json'
            config = json.loads(config_path.read_text())
            self.assertEqual(config['participants'], ['codex', 'deepseek'])
            self.assertEqual(config['codex'], sys.executable)
            self.assertEqual(config['state_root'], str(root/'state'))
            self.assertEqual(config['unit_dir'], str(root/'units'))
            self.assertEqual(config['dsh_home'], str(root/'dsh'))
            before = config_path.read_bytes()
            for invalid in (str(root/'missing'), str(root)):
                failed = subprocess.run(command + ['--codex', invalid], env=env,
                                        capture_output=True, text=True)
                self.assertNotEqual(failed.returncode, 0)
                self.assertEqual(config_path.read_bytes(), before)

    def test_uninstall_preserves_shared_locks_under_an_ancestor_state_root(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp).resolve()
            account = root/'account'
            app = root/'app'
            lock_dir = account/'.local/state/koinon-locks'
            lock_dir.mkdir(parents=True, mode=0o700)
            lock = lock_dir/('0'*64+'.lock')
            lock.touch(mode=0o600)
            inode = lock.stat().st_ino
            install = subprocess.run([sys.executable, 'scripts/install.py',
                '--configure-codex', '--no-start', '--codex', sys.executable,
                '--prefix', str(app), '--state-dir', str(account),
                '--unit-dir', str(root/'units'), '--codex-home', str(root/'codex')],
                capture_output=True, text=True)
            self.assertEqual(install.returncode, 0, install.stderr)
            uninstall = subprocess.run([sys.executable, str(app/'scripts/uninstall.py'),
                '--prefix', str(app)], capture_output=True, text=True)
            self.assertEqual(uninstall.returncode, 0, uninstall.stderr)
            self.assertEqual(lock.stat().st_ino, inode)
            self.assertEqual(lock.stat().st_size, 0)

if __name__ == '__main__':
    unittest.main()
