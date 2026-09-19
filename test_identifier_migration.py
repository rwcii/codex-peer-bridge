"""Literal boundary and installation preservation tests for the product rename."""
from contextlib import redirect_stdout, redirect_stderr
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

import bridge
import memory
import notify
import platform_support
import runtime_names
from scripts import install


class IdentifierMigrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.env = dict(os.environ, HOME=str(self.root/'home'), XDG_STATE_HOME=str(self.root/'state-base'))

    def install(self, *args):
        return subprocess.run([sys.executable, 'scripts/install.py', '--no-start',
            '--codex', sys.executable, *map(str,args)], env=self.env, text=True, capture_output=True)

    def test_fresh_and_saved_configuration_take_precedence_without_moving_state(self):
        app, state, units = self.root/'app', self.root/'custom-state', self.root/'custom-units'
        args = ('--configure-codex', '--prefix', app, '--codex-home', self.root/'guidance')
        first = self.install(*args, '--state-dir', state, '--unit-dir', units)
        self.assertEqual(first.returncode, 0, first.stderr)
        state.mkdir()
        checkpoint = state/'notify-cursor.json'
        checkpoint.write_bytes(b'preserve synthetic checkpoint')
        inode = checkpoint.stat().st_ino
        for name in ('koinon', 'codex-peer-bridge'):
            (Path(self.env['XDG_STATE_HOME'])/name).mkdir(parents=True)
            (Path(self.env['HOME'])/'.local/share'/name).mkdir(parents=True)
        repeated = self.install(*args)
        self.assertEqual(repeated.returncode, 0, repeated.stderr)
        config = json.loads((app/'install.json').read_text())
        self.assertEqual(config['state_root'], str(state))
        self.assertEqual(config['unit_dir'], str(units))
        self.assertEqual(checkpoint.stat().st_ino, inode)
        self.assertEqual(checkpoint.read_bytes(), b'preserve synthetic checkpoint')
        for module in ('participant_instructions.py', 'codex_instructions.py', 'runtime_names.py'):
            self.assertTrue((app/module).is_file())
        changed = self.install(*args, '--state-dir', self.root/'explicit-state', '--unit-dir', self.root/'explicit-units')
        self.assertEqual(changed.returncode, 0, changed.stderr)
        config = json.loads((app/'install.json').read_text())
        self.assertEqual(config['state_root'], str(self.root/'explicit-state'))
        self.assertEqual(config['unit_dir'], str(self.root/'explicit-units'))

    def test_legacy_only_defaults_are_reused_and_reported(self):
        app = Path(self.env['HOME'])/'.local/share/codex-peer-bridge'
        state = Path(self.env['XDG_STATE_HOME'])/'codex-peer-bridge'
        app.mkdir(parents=True)
        state.mkdir(parents=True)
        marker = state/'inbox-evidence'
        marker.write_text('preserved')
        inode = marker.stat().st_ino
        result = self.install('--configure-codex', '--codex-home', self.root/'guidance')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('legacy path reused', result.stderr)
        self.assertIn(str(app), result.stdout)
        self.assertIn(str(state), result.stdout)
        self.assertEqual(marker.stat().st_ino, inode)
        self.assertFalse((app.parent/'koinon').exists())
        self.assertFalse((state.parent/'koinon').exists())

    def test_conflicting_defaults_refuse_before_install_and_explicit_paths_bypass(self):
        for base in (Path(self.env['HOME'])/'.local/share', Path(self.env['XDG_STATE_HOME'])):
            for name in ('koinon', 'codex-peer-bridge'):
                (base/name).mkdir(parents=True)
        result = self.install('--configure-codex', '--codex-home', self.root/'guidance')
        self.assertEqual(result.returncode, 78, result.stderr)
        self.assertEqual(json.loads(result.stdout)['code'], 'ambiguous_default_paths')
        self.assertFalse((self.root/'guidance').exists())
        result = self.install('--thread', 'synthetic-thread', '--prefix', self.root/'explicit',
                              '--state-dir', self.root/'explicit-state', '--unit-dir', self.root/'units')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue((self.root/'units/koinon-bridge.service').exists())
        self.assertTrue((self.root/'units/koinon-notify.service').exists())

    def test_malformed_saved_configuration_never_falls_back(self):
        app = self.root/'app'
        app.mkdir()
        for contents in ('{', '[]', '{}', '{"state_root":"relative","unit_dir":"/units"}'):
            with self.subTest(contents=contents):
                (app/'install.json').write_text(contents)
                result = self.install('--configure-codex', '--prefix', app, '--codex-home', self.root/'guidance')
                self.assertEqual(result.returncode, 78, result.stderr)
                self.assertEqual(json.loads(result.stdout)['code'], 'invalid_install_configuration')
                self.assertEqual(list(app.iterdir()), [app/'install.json'])
                self.assertFalse((self.root/'guidance').exists())

    def test_old_units_remain_at_old_names_with_new_marker(self):
        app, units = self.root/'app', self.root/'units'
        units.mkdir()
        rendered = install.units(app, self.root/'state', 'synthetic-thread', 'synthetic-peer',
                                 '/synthetic-repo', sys.executable, sys.executable, legacy=True)
        for name, content in rendered.items():
            (units/name).write_text(content.replace('# Managed by koinon\n', '# Managed by codex-peer-bridge\n'))
        result = self.install('--thread', 'synthetic-thread', '--prefix', app,
                              '--state-dir', self.root/'state', '--unit-dir', units)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual({p.name for p in units.iterdir()}, {'codex-peer-notify.service', 'codex-peer-bridge.service'})
        for path in units.iterdir():
            self.assertTrue(path.read_text().startswith('# Managed by koinon\n'))
        self.assertIn('Requires=codex-peer-bridge.service', (units/'codex-peer-notify.service').read_text())
        self.assertEqual(install.saved_unit_option(units/'codex-peer-notify.service', '--name'), 'synthetic-peer')
        self.assertEqual(install.saved_unit_option(units/'codex-peer-notify.service', '--repo'), '/synthetic-repo')

    def test_other_prefix_is_not_owned_even_with_either_valid_marker(self):
        units = self.root/'units'
        units.mkdir()
        for marker in ('# Managed by koinon\n', '# Managed by codex-peer-bridge\n'):
            path = units/'codex-peer-bridge.service'
            original = marker + '[Service]\nExecStart="/python" "/other/bridge.py"\n'
            path.write_text(original)
            result = self.install('--thread', 'synthetic-thread', '--prefix', self.root/'app',
                                  '--state-dir', self.root/'state', '--unit-dir', units)
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse((self.root/'app').exists())
            self.assertEqual(path.read_text(), original)

    def test_conflicting_unit_families_preserve_all_files(self):
        units = self.root/'units'
        units.mkdir()
        for name in ('koinon-bridge.service', 'codex-peer-bridge.service'):
            (units/name).write_text('preserved')
        result = self.install('--thread', 'synthetic-thread', '--prefix', self.root/'app',
                              '--state-dir', self.root/'state', '--unit-dir', units)
        self.assertEqual(result.returncode, 78, result.stderr)
        self.assertEqual(json.loads(result.stdout)['code'], 'ambiguous_service_units')
        self.assertFalse((self.root/'app').exists())
        self.assertEqual([p.read_text() for p in units.iterdir()], ['preserved', 'preserved'])

    def test_every_runtime_cli_refuses_ambiguous_default_before_creating_control(self):
        base = Path(self.env['XDG_STATE_HOME'])
        for name in ('koinon', 'codex-peer-bridge'):
            (base/name).mkdir(parents=True)
        repo = self.root/'synthetic-repository'
        subprocess.run(['git', 'init', '-q', str(repo)], check=True)
        for script in ('bridge.py', 'notify.py', 'memory.py'):
            arguments = ['--repo-path', str(repo)] if script == 'memory.py' else []
            result = subprocess.run([sys.executable, script, *arguments, 'status'], env=self.env,
                                    capture_output=True, text=True)
            self.assertEqual(result.returncode, 78, (script, result.stdout, result.stderr))
            self.assertEqual(json.loads(result.stdout)['code'], 'ambiguous_default_paths')
        self.assertEqual(list((base/'koinon').iterdir()), [])
        self.assertEqual(list((base/'codex-peer-bridge').iterdir()), [])

    def test_exact_memory_root_does_not_probe_default(self):
        output = io.StringIO()
        with mock.patch.object(sys, 'argv', ['memory.py', '--service-dir', str(self.root/'exact'), 'status']), \
             mock.patch.object(runtime_names, 'default_state_root', side_effect=AssertionError('must not probe')), \
             mock.patch.object(memory, 'repo_identity', return_value='synthetic-repo'), \
             mock.patch.object(memory, 'private_state_dir'), \
             mock.patch.object(memory, 'request_bound', new=mock.AsyncMock(return_value={'ok': True})), \
             redirect_stdout(output):
            with self.assertRaises(SystemExit) as result:
                memory.cli_main()
            self.assertEqual(result.exception.code, 0)

    def test_account_lock_namespace_literal(self):
        for darwin, suffix in ((False, '.local/state/koinon-locks'),
                               (True, 'Library/Application Support/koinon-locks')):
            with mock.patch.object(platform_support, 'account_home', return_value=self.root), \
                 mock.patch.object(platform_support, 'DARWIN', darwin):
                self.assertEqual(platform_support.participant_lock_dir(), self.root/suffix)

    def test_uninstall_removes_both_owned_unit_families_and_preserves_other_installations(self):
        import importlib.util
        with mock.patch.object(sys, 'path', [str(Path('scripts').resolve()), *sys.path]):
            spec = importlib.util.spec_from_file_location('migration_uninstall', 'scripts/uninstall.py')
            uninstaller = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(uninstaller)
        app, units, state = self.root/'app', self.root/'units', self.root/'state'
        for path in (app, units, state):
            path.mkdir()
        evidence = state/'retained.lock'
        evidence.write_bytes(b'')
        inode = evidence.stat().st_ino
        (app/'install.json').write_text(json.dumps(dict(state_root=str(state), unit_dir=str(units), participants=[])))
        owned = []
        for old in (False, True):
            for instance in (None, 'a'*16):
                rendered = install.units(app, state, 'synthetic', 'peer', '/repo', sys.executable,
                                         sys.executable, instance=instance, legacy=old)
                for name, content in rendered.items():
                    if old:
                        content = content.replace('# Managed by koinon\n', '# Managed by codex-peer-bridge\n')
                    (units/name).write_text(content)
                    owned.append(name)
        foreign = units/('koinon-session-' + 'b'*16 + '.service')
        foreign.write_text('# Managed by koinon\n[Service]\nExecStart="/python" "/other/session.py"\n')
        foreign_before = foreign.read_bytes()
        with mock.patch.object(sys, 'argv', ['uninstall.py', '--prefix', str(app)]), \
             mock.patch.object(uninstaller.subprocess, 'run') as run, redirect_stdout(io.StringIO()):
            uninstaller.main()
        disabled = run.call_args_list[0].args[0]
        self.assertEqual(disabled[:4], ['systemctl', '--user', 'disable', '--now'])
        self.assertEqual(set(disabled[4:]), set(owned))
        self.assertEqual(list(units.iterdir()), [foreign])
        self.assertEqual(foreign.read_bytes(), foreign_before)
        self.assertEqual(evidence.stat().st_ino, inode)

    def test_existing_unit_target_cannot_change_before_checkpoint_exists(self):
        app, units = self.root/'app', self.root/'units'
        units.mkdir()
        rendered = install.units(app, self.root/'state', 'original-thread', 'original-peer',
                                 '/original-repo', sys.executable, sys.executable, legacy=True)
        for name, content in rendered.items():
            (units/name).write_text(content)
        result = self.install('--thread', 'different-thread', '--prefix', app,
                              '--state-dir', self.root/'state', '--unit-dir', units)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('different thread', result.stderr)
        self.assertFalse(app.exists())
        self.assertEqual({p.name:p.read_text() for p in units.iterdir()}, rendered)

    def test_uninstall_refuses_damaged_unit_for_selected_runtime_before_deleting_files(self):
        import importlib.util
        with mock.patch.object(sys, 'path', [str(Path('scripts').resolve()), *sys.path]):
            spec = importlib.util.spec_from_file_location('damaged_uninstall', 'scripts/uninstall.py')
            uninstaller = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(uninstaller)
        app, units, state = self.root/'app', self.root/'units', self.root/'state'
        for path in (app, units, state):
            path.mkdir()
        config = app/'install.json'
        config.write_text(json.dumps(dict(state_root=str(state), unit_dir=str(units), participants=[])))
        runtime = app/'bridge.py'
        runtime.write_text('preserved runtime')
        unit = units/'koinon-bridge.service'
        unit.write_text('[Service]\nExecStart="/python" ' + install.unit_arg(str(runtime)) + '\n')
        before = unit.read_bytes()
        with mock.patch.object(sys, 'argv', ['uninstall.py', '--prefix', str(app)]), \
             mock.patch.object(uninstaller.subprocess, 'run') as run:
            with self.assertRaises(ValueError):
                uninstaller.main()
            run.assert_not_called()
        self.assertEqual(runtime.read_text(), 'preserved runtime')
        self.assertEqual(unit.read_bytes(), before)
        self.assertTrue(config.exists())

    def test_installed_fixed_service_installer_can_repeat_without_copying_onto_itself(self):
        app, units, state = self.root/'app', self.root/'units', self.root/'state'
        arguments = ['--thread', 'synthetic-thread', '--prefix', str(app),
                     '--state-dir', str(state), '--unit-dir', str(units)]
        first = self.install(*arguments)
        self.assertEqual(first.returncode, 0, first.stderr)
        state.mkdir()
        retained = state/'retained.lock'
        retained.write_bytes(b'')
        inode = retained.stat().st_ino
        before = {p.name:p.read_bytes() for p in units.iterdir()}
        repeated = subprocess.run([sys.executable, str(app/'scripts/install.py'),
            '--no-start', '--codex', sys.executable, *arguments], env=self.env,
            capture_output=True, text=True)
        self.assertEqual(repeated.returncode, 0, repeated.stderr)
        self.assertEqual({p.name:p.read_bytes() for p in units.iterdir()}, before)
        self.assertEqual(retained.stat().st_ino, inode)
