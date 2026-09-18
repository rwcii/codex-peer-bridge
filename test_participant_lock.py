import argparse
from contextlib import ExitStack
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import notify
import participant_lock as locks
import platform_support


class OwnershipTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        previous_umask = os.umask(0o077)
        self.addCleanup(os.umask, previous_umask)
        self.root = Path(temporary.name)
        self.home = self.root / 'account home'
        self.home.mkdir(mode=0o700)
        self.patch = patch('platform_support.account_home', return_value=self.home)
        self.patch.start()
        self.addCleanup(self.patch.stop)
        self.state = self.root / 'one'
        self.other = self.root / 'two'

    def test_same_target_is_excluded_across_state_roots_and_provider_homes(self):
        with locks.notifier_ownership(self.state, 'codex', 'session') as first:
            with patch.dict(os.environ, HOME='/different', CODEX_HOME='/other',
                            DSH_HOME='/harness', XDG_STATE_HOME='/state'):
                with self.assertRaises(locks.OwnershipError) as caught:
                    with locks.notifier_ownership(self.other, 'codex', 'session'):
                        self.fail('duplicate owner admitted')
            self.assertEqual(caught.exception.code, 'participant_in_use')
            self.assertEqual(caught.exception.identity, first)
            self.assertNotIn('session', json.dumps(caught.exception.result()))
        with locks.notifier_ownership(self.other, 'codex', 'session') as replacement:
            self.assertEqual(replacement, first)

    def test_different_targets_and_providers_coexist(self):
        with ExitStack() as stack:
            identities = []
            for n, provider, target in [(1, 'codex', 'a'), (2, 'codex', 'b'),
                                         (3, 'deepseek', 'a')]:
                identities.append(stack.enter_context(locks.notifier_ownership(
                    self.root/str(n), provider, target)))
            self.assertEqual(len({i['digest'] for i in identities}), 3)

    def test_state_lock_precedes_target_lock_and_failure_releases_it(self):
        with locks.notifier_ownership(self.state, 'codex', 'a'):
            with self.assertRaises(locks.OwnershipError) as caught:
                with locks.notifier_ownership(self.state, 'codex', 'b'):
                    self.fail('state lock bypassed')
            self.assertEqual(caught.exception.code, 'notifier_in_use')
            with self.assertRaises(locks.OwnershipError):
                with locks.notifier_ownership(self.other, 'codex', 'a'):
                    self.fail('target lock bypassed')
            with locks.notifier_ownership(self.other, 'codex', 'b'):
                pass

    def test_caller_failure_propagates_and_releases_both_locks(self):
        failure = OSError('synthetic storage failure')
        with self.assertRaises(OSError) as caught:
            with locks.notifier_ownership(self.state, 'codex', 'a'):
                raise failure
        self.assertIs(caught.exception, failure)
        with locks.notifier_ownership(self.state, 'codex', 'a'):
            pass

    def test_overlap_is_rejected_including_symlink_alias(self):
        namespace = platform_support.participant_lock_dir()
        namespace.mkdir(parents=True, mode=0o700)
        alias = self.root / 'alias'
        alias.symlink_to(namespace, target_is_directory=True)
        for state in (namespace, namespace/'nested', alias, alias/'nested'):
            with self.subTest(state=state):
                with self.assertRaises(locks.OwnershipError) as caught:
                    with locks.notifier_ownership(state, 'codex', 'a'):
                        self.fail('overlapping state admitted')
                self.assertEqual(caught.exception.code, 'state_overlaps_lock_namespace')
        # An ancestor is valid: stopping must preserve the nested common namespace.
        with locks.notifier_ownership(self.home, 'codex', 'a') as owner:
            path = namespace/(owner['digest']+'.lock')
            inode = path.stat().st_ino
        self.assertEqual(path.stat().st_ino, inode)
        self.assertEqual(path.stat().st_size, 0)

    def test_lock_files_are_not_unlinked_or_replaced(self):
        with locks.notifier_ownership(self.state, 'codex', 'a') as owner:
            path = platform_support.participant_lock_dir()/(owner['digest']+'.lock')
            inode = path.stat().st_ino
        with locks.notifier_ownership(self.state, 'codex', 'a'):
            self.assertEqual(path.stat().st_ino, inode)
        self.assertEqual(path.stat().st_ino, inode)

    def test_unsafe_lock_file_is_refused_without_repair(self):
        self.state.mkdir(mode=0o700)
        destination = self.root/'untouched'
        destination.write_text('preserve')
        (self.state/'notifier.lock').symlink_to(destination)
        with self.assertRaises(locks.OwnershipError):
            with locks.notifier_ownership(self.state, 'codex', 'a'):
                self.fail('symlink admitted')
        self.assertEqual(destination.read_text(), 'preserve')
        (self.state/'notifier.lock').unlink()
        path = self.state/'notifier.lock'
        path.write_text('')
        path.chmod(0o644)
        with self.assertRaises(locks.OwnershipError) as caught:
            with locks.notifier_ownership(self.state, 'codex', 'a'):
                self.fail('public lock admitted')
        self.assertEqual(caught.exception.code, 'unsafe_lock_file')
        self.assertEqual(path.stat().st_mode & 0o777, 0o644)

    def test_notifier_refuses_before_bridge_query_and_releases_on_startup_failure(self):
        opts = argparse.Namespace(state_dir=self.other, agent='codex', thread='a')
        with locks.notifier_ownership(self.state, 'codex', 'a'):
            with patch('notify.run_owned') as run:
                with self.assertRaises(locks.OwnershipError):
                    notify.run(opts)
                run.assert_not_called()
        with patch('notify.run_owned', side_effect=OSError('startup failed')):
            with self.assertRaises(OSError):
                notify.run(opts)
        with locks.notifier_ownership(self.other, 'codex', 'a'):
            pass

    def test_missing_account_home_has_explicit_failure(self):
        with patch('platform_support.account_home', side_effect=platform_support.AccountHomeUnavailable):
            with self.assertRaises(locks.OwnershipError) as caught:
                with locks.notifier_ownership(self.state, 'codex', 'a'):
                    pass
        self.assertEqual(caught.exception.code, 'account_home_unavailable')
        self.assertFalse(self.state.exists())

    def test_cli_reports_account_home_failure_without_traceback(self):
        script = """
import runpy, sys
import platform_support
platform_support.account_home = lambda: (_ for _ in ()).throw(
    platform_support.AccountHomeUnavailable())
sys.argv = ['notify.py', '--thread', 'synthetic-session', '--state-dir', sys.argv[1]]
runpy.run_module('notify', run_name='__main__')
"""
        result = subprocess.run([sys.executable, '-c', script, str(self.state)],
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 1)
        self.assertEqual(result.stderr, '')
        self.assertEqual(json.loads(result.stdout)['code'], 'account_home_unavailable')
        self.assertNotIn('synthetic-session', result.stdout)
        self.assertFalse(self.state.exists())

    def test_invalid_identity_is_rejected_without_creating_state(self):
        for value in ('', ' a', 'a\n', 'x'*513, '\ud800', None):
            with self.subTest(value=repr(value)), self.assertRaises(locks.OwnershipError):
                with locks.notifier_ownership(self.state, 'codex', value):
                    pass
        self.assertFalse(self.state.exists())

    def test_real_process_exclusion_and_child_does_not_inherit_lock(self):
        script = '''
import json, subprocess, sys
from pathlib import Path
import platform_support
from participant_lock import notifier_ownership
platform_support.account_home = lambda: Path(sys.argv[1])
with notifier_ownership(Path(sys.argv[2]), 'codex', 'a') as owner:
    child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'],
                             close_fds=False, stdin=subprocess.DEVNULL,
                             stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print(json.dumps({'child': child.pid, 'owner': owner}), flush=True)
    sys.stdin.readline()
'''
        p = subprocess.Popen([sys.executable, '-c', script, str(self.home), str(self.state)],
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             text=True)
        child = None
        try:
            import select
            ready, _, _ = select.select([p.stdout], [], [], 10)
            self.assertTrue(ready, 'owner did not start')
            data = json.loads(p.stdout.readline())
            child = data['child']
            with self.assertRaises(locks.OwnershipError) as caught:
                with locks.notifier_ownership(self.other, 'codex', 'a'):
                    pass
            self.assertEqual(caught.exception.code, 'participant_in_use')
            p.kill()
            p.wait(timeout=10)
            os.kill(child, 0)  # The child survives its parent.
            with locks.notifier_ownership(self.other, 'codex', 'a') as replacement:
                self.assertEqual(replacement, data['owner'])
        finally:
            if p.poll() is None:
                p.kill()
                p.wait(timeout=10)
            if child:
                try:
                    os.kill(child, signal.SIGTERM)
                except ProcessLookupError:
                    pass
            for stream in (p.stdin, p.stdout, p.stderr):
                stream.close()


class AccountPathTests(unittest.TestCase):
    def test_exact_platform_paths_and_ignored_environment(self):
        with tempfile.TemporaryDirectory() as temp:
            home = Path(temp)
            from types import SimpleNamespace
            with patch('pwd.getpwuid', return_value=SimpleNamespace(pw_dir=temp)) as account:
                with patch.dict(os.environ, HOME='/wrong', XDG_STATE_HOME='/wrong',
                                CODEX_HOME='/wrong', DSH_HOME='/wrong'):
                    for darwin, suffix in [(False, '.local/state/koinon-locks'),
                                           (True, 'Library/Application Support/koinon-locks')]:
                        with patch.object(platform_support, 'DARWIN', darwin):
                            self.assertEqual(platform_support.participant_lock_dir(), home/suffix)
                account.assert_called_with(os.geteuid())

    def test_missing_or_invalid_os_account_never_falls_back(self):
        from types import SimpleNamespace
        for value in ('', 'relative', '/missing-account-home-for-test'):
            with patch('pwd.getpwuid', return_value=SimpleNamespace(pw_dir=value)):
                with self.assertRaises(platform_support.AccountHomeUnavailable):
                    platform_support.participant_lock_dir()
        with patch('pwd.getpwuid', side_effect=KeyError):
            with self.assertRaises(platform_support.AccountHomeUnavailable):
                platform_support.participant_lock_dir()
