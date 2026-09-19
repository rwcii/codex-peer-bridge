import importlib.util
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
            unit=(root/'units/codex-peer-notify.service').read_text()
            self.assertIn('test-thread',unit)
            self.assertIn('--codex',unit)
            self.assertFalse((root/'state').exists())

    def test_uninstall_preserves_shared_locks_under_an_ancestor_state_root(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
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
