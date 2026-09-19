import json
import io
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
import session
from contextlib import redirect_stdout
from unittest.mock import patch
from scripts.install import units


def isolate_account_home(app, home):
    home.mkdir(parents=True, exist_ok=True)
    # Test-owned installed copy only; production has no environment escape hatch.
    with (app/'platform_support.py').open('a') as stream:
        stream.write('\ndef account_home():\n    return Path(' + repr(str(home)) + ')\n')


class SessionTests(unittest.TestCase):
    def test_isolation_and_units(self):
        config={'state_root':'/state'}
        a=session.details(Path('/app'),config,'thread-a','/repo')
        b=session.details(Path('/app'),config,'thread-b','/repo')
        self.assertNotEqual(a[0],b[0])
        self.assertNotEqual(a[1],b[1])
        ua=units(Path('/app'),a[0],'thread-a',a[1],'/repo',sys.executable,sys.executable,a[2])
        ub=units(Path('/app'),b[0],'thread-b',b[1],'/repo',sys.executable,sys.executable,b[2])
        self.assertFalse(set(ua)&set(ub))
        self.assertEqual(list(ua),[f'codex-peer-session-{a[2]}.service'])
        self.assertIn('session.py',next(iter(ua.values())))
        for invalid in ('','../../bad','thread\nvalue'):
            with self.assertRaises(ValueError): session.identity(invalid)

    def test_short_name_collision_keeps_identity(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d)
            a=root/'sessions'/session.identity('thread-a')
            b=root/'sessions'/session.identity('thread-b')
            a.mkdir(parents=True)
            b.mkdir(parents=True)
            with patch('session.peers',return_value=[]):
                first=session.save_registration(a,root,'thread-a','/unify-ui')
            suffix=session.identity('thread-b')[:2]
            taken=f'codex-unify-ui-{suffix}'
            with patch('session.peers',return_value=[{'name':taken}]):
                second=session.save_registration(b,root,'thread-b','/unify-ui')
            self.assertRegex(first['name'],r'^codex-unify-ui-[a-f0-9]{2}$')
            self.assertRegex(second['name'],r'^codex-unify-ui-[a-f0-9]{2}$')
            self.assertNotEqual(second['name'],taken)
            self.assertNotEqual(first['name'],second['name'])

    def test_rename_preserves_state_on_failure(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d)
            state=root/'sessions'/session.identity('thread-a')
            state.mkdir(parents=True)
            inbox=state/'inbox.sqlite3'
            inbox.write_bytes(b'preserved inbox fixture')
            with patch('session.peers',return_value=[]):
                original=session.save_registration(state,root,'thread-a','/old-repo')
            all_names=[{'name':f'codex-new-repo-{i:02x}'} for i in range(256)]
            with patch('session.peers',return_value=all_names):
                with self.assertRaises(ValueError):
                    session.save_registration(state,root,'thread-a','/new-repo',rename=True)
            self.assertEqual(json.loads((state/'session.json').read_text()),original)
            with patch('session.peers',return_value=[]):
                renamed=session.save_registration(state,root,'thread-a','/new-repo',rename=True)
            self.assertEqual(renamed['thread'],original['thread'])
            self.assertEqual(renamed['repo'],'/new-repo')
            self.assertEqual(inbox.read_bytes(),b'preserved inbox fixture')

    def test_two_live_sessions_and_idempotence(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d)
            env=dict(os.environ,CLAUDE_CONFIG_DIR=str(root/'claude'))
            app=root/'app'
            subprocess.run([sys.executable,'scripts/install.py','--configure-codex','--no-start',
                '--codex',sys.executable,'--prefix',str(app),'--state-dir',str(root/'state'),
                '--unit-dir',str(root/'units'),'--codex-home',str(root/'codex')],check=True,capture_output=True,env=env)
            isolate_account_home(app, root/'account')
            config=session.read_config(app)
            processes=[]
            try:
                for thread in ('thread-one','thread-two'):
                    processes.append(subprocess.Popen([sys.executable,str(app/'session.py'),'run','--thread',thread,
                                      '--repo','/test-project'],stdout=subprocess.DEVNULL,stderr=subprocess.PIPE,env=env))
                statuses=[]
                for thread in ('thread-one','thread-two'):
                    state,_,_=session.details(app,config,thread,'/test-project')
                    deadline=time.monotonic()+10
                    while time.monotonic()<deadline:
                        status=session.bridge_status(app,state)
                        if status and session.notifier_ready(state,status): break
                        time.sleep(.05)
                    else: self.fail('session failed to start')
                    statuses.append(status)
                    self.assertTrue(session.notifier_ready(state,status))
                    result=subprocess.run([sys.executable,str(app/'session.py'),'ensure','--thread',thread],
                                           env=env,capture_output=True,text=True,check=True)
                    reported = json.loads(result.stdout)
                    self.assertEqual(reported['bridge']['pid'],status['pid'])
                    from participant_lock import identity
                    self.assertEqual(reported['participant_lock'], identity('codex', thread))
                    self.assertEqual(json.loads((state/'notify-ready.json').read_text())[
                        'participant_lock'], reported['participant_lock'])
                # Provider failure must not turn ensure into a second owner launch.
                # These installed fixtures use Python as the unavailable queue CLI.
                import sqlite3
                failed_state,_,_=session.details(app,config,'thread-one','/test-project')
                ready_before=json.loads((failed_state/'notify-ready.json').read_text())
                from contextlib import closing
                with closing(sqlite3.connect(failed_state/'inbox.sqlite3')) as db:
                    db.execute('INSERT INTO inbox(pid,frame) VALUES(?,?)',
                        (123, json.dumps(dict(type='user', message=dict(content='synthetic test')))))
                    db.commit()
                deadline=time.monotonic()+10
                while time.monotonic()<deadline:
                    observed=subprocess.run([sys.executable,str(app/'session.py'),'ensure','--thread','thread-one'],
                        env=env,capture_output=True,text=True,check=True)
                    result=json.loads(observed.stdout)
                    if 'uncertain_delivery' in result['delivery_health']['reasons']:
                        break
                    time.sleep(.05)
                else:
                    self.fail('provider failure did not reach delivery health')
                self.assertEqual(result['status'],'running')
                self.assertEqual(result['bridge']['pid'],statuses[0]['pid'])
                self.assertEqual(json.loads((failed_state/'notify-ready.json').read_text()),ready_before)
                self.assertNotEqual(statuses[0]['pid'],statuses[1]['pid'])
                self.assertNotEqual(statuses[0]['address'],statuses[1]['address'])
                # A unit file alone must not prevent shutting down a manual instance.
                state,_,key=session.details(app,config,'thread-one','/test-project')
                unit_dir=root/'units'
                unit_dir.mkdir(exist_ok=True)
                from scripts.install import MARKER
                (unit_dir/f'codex-peer-session-{key}.service').write_text(MARKER+'[Service]\n')
                # Force no systemctl resolution for this subprocess without changing children.
                stop_env=dict(env,PATH='/nonexistent')
                stopped=subprocess.run([sys.executable,str(app/'session.py'),'stop','--thread','thread-one'],
                                       env=stop_env,capture_output=True,text=True,timeout=20)
                self.assertEqual(stopped.returncode,0,stopped.stderr)
                self.assertIsNone(session.bridge_status(app,state))
                other_state,_,_=session.details(app,config,'thread-two','/test-project')
                self.assertTrue(session.notifier_ready(other_state,session.bridge_status(app,other_state)))
            finally:
                for process in processes:
                    process.terminate()
                for process in processes:
                    process.communicate(timeout=25)
            self.assertEqual(list((root/'claude/sessions').glob('*.json')),[])


class SystemdStartupTests(unittest.TestCase):
    """Use a fake service manager and real, isolated session processes."""

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        # Reach the root through a symlink on every platform. macOS puts temporary
        # directories under /var, which is a symlink to /private/var, so a path the
        # runtime resolves differs there from the one the test passed in. Reproducing
        # that on Linux too keeps the resolution expectations below honest rather than
        # platform-dependent.
        real = Path(temporary.name)/'real'
        real.mkdir()
        link = Path(temporary.name)/'link'
        link.symlink_to(real, target_is_directory=True)
        self.root = link
        self.app = self.root/'app'
        self.env = dict(os.environ, CLAUDE_CONFIG_DIR=str(self.root/'claude'))
        subprocess.run([sys.executable, 'scripts/install.py', '--configure-codex', '--no-start',
                        '--codex', sys.executable, '--prefix', str(self.app),
                        '--state-dir', str(self.root/'state'),
                        '--unit-dir', str(self.root/'units'),
                        '--codex-home', str(self.root/'codex')],
                       check=True, capture_output=True, env=self.env)
        isolate_account_home(self.app, self.root/'account')
        # A competing command must never reach the host's service manager.
        binaries = self.root/'bin'
        binaries.mkdir()
        stub = binaries/'systemctl'
        stub.write_text(f'#!{sys.executable}\nraise SystemExit(1)\n')
        stub.chmod(0o700)
        self.env['PATH'] = str(binaries)+os.pathsep+os.environ.get('PATH', '')
        self.config = session.read_config(self.app)
        self.thread = 'startup-test-thread'
        # `session.py` resolves --repo before recording it, so the test compares the
        # same form. On macOS the temporary root is under /var, a symlink to
        # /private/var, and an unresolved expectation fails there and only there.
        self.repo = str((self.root/'project').resolve())
        self.state, _, _ = session.details(self.app, self.config, self.thread, self.repo)
        self.processes = []
        self.starts = 0
        self.addCleanup(self.stop_processes)

    def stop_processes(self):
        for process in reversed(self.processes):
            if process.poll() is None:
                process.terminate()
        for process in reversed(self.processes):
            try:
                process.communicate(timeout=25)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate(timeout=5)

    def spawn(self, command):
        process = subprocess.Popen(command, env=self.env, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, text=True)
        self.processes.append(process)
        return process

    def test_bridge_refusal_propagates_during_notifier_startup_and_running(self):
        from unittest.mock import Mock
        self.state.mkdir(parents=True, exist_ok=True)
        for phase, exit_code in ((phase, code) for phase in ('notifier_startup', 'running') for code in (70, 78)):
            with self.subTest(phase=phase, exit_code=exit_code):
                bridge_child, notifier_child = Mock(), Mock()
                bridge_child.poll.return_value = None
                notifier_child.poll.return_value = None
                children = []
                def spawn(*args, **kwargs):
                    child = bridge_child if not children else notifier_child
                    children.append(child)
                    if child is notifier_child and phase == 'notifier_startup':
                        bridge_child.poll.return_value = exit_code
                    return child
                def readiness(*args):
                    bridge_child.poll.return_value = exit_code
                    return True
                with patch.object(session.subprocess, 'Popen', side_effect=spawn), \
                     patch.object(session, 'bridge_status', side_effect=lambda *a: {'pid':123} if children else None), \
                     patch.object(session, 'notifier_ready', side_effect=readiness), \
                     patch.object(session, 'notify_command', return_value=['synthetic-notifier']), \
                     patch.object(session, 'result', return_value={'status':'running'}), \
                     patch.object(session.signal, 'signal'), redirect_stdout(io.StringIO()):
                    with self.assertRaises(SystemExit) as caught:
                        session.supervisor(self.app, self.config, self.state, self.thread, self.repo, 'synthetic')
                self.assertEqual(caught.exception.code, exit_code)
                notifier_child.terminate.assert_called_once()
                notifier_child.wait.assert_called_once_with(timeout=20)
                bridge_child.terminate.assert_not_called()

    def test_supervisor_preserves_bridge_startup_refusal(self):
        script = self.app/'bridge.py'
        source = script.read_text().split("if __name__ == '__main__':", 1)[0]
        for exit_code in (70, 78):
            script.write_text(source + f"if __name__ == '__main__':\n    import sys\n    if sys.argv[-1] == 'serve':\n        raise SystemExit({exit_code})\n    main()\n")
            process = self.spawn([sys.executable, str(self.app/'session.py'), 'run',
                                  '--thread', self.thread, '--repo', self.repo])
            stdout, stderr = process.communicate(timeout=20)
            self.assertEqual(process.returncode, exit_code, stderr)
            self.assertNotIn('Traceback', stderr)
            self.assertFalse((self.state/'inbox.sqlite3').exists())

    def test_supervisor_preserves_configuration_refusal_and_stops_bridge(self):
        notifier = self.app/'notify.py'
        source = notifier.read_text().split("if __name__ == '__main__':", 1)[0]
        notifier.write_text(source + "if __name__ == '__main__':\n    raise SystemExit(78)\n")
        process = self.spawn([sys.executable, str(self.app/'session.py'), 'run',
                              '--thread', self.thread, '--repo', self.repo])
        stdout, stderr = process.communicate(timeout=20)
        self.assertEqual(process.returncode, 78, stderr)
        self.assertIn('address', stdout, 'the bridge must start before the child refusal')
        self.assertNotIn('Traceback', stderr)
        self.assertIsNone(session.bridge_status(self.app, self.state))

    def test_supervisor_preserves_refusal_after_notifier_readiness(self):
        # This test-owned child takes the real readiness lock, then exits with the
        # permanent-refusal code after the supervisor has entered its running loop.
        notifier = self.app/'notify.py'
        source = notifier.read_text().split("if __name__ == '__main__':", 1)[0]
        import textwrap
        notifier.write_text(source + "if __name__ == '__main__':\n" + textwrap.indent("""
import fcntl, json, os, sys, time
from pathlib import Path
import platform_support
root = Path(sys.argv[sys.argv.index('--state-dir')+1])
lock = (root/'notifier.lock').open('a')
fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
# The bridge PID comes from the installed bridge CLI, not from a test assumption.
import subprocess
bridge = json.loads(subprocess.check_output([sys.executable,
    str(Path(__file__).with_name('bridge.py')), '--state-dir', str(root), 'status']))['result']
(root/'notify-ready.json').write_text(json.dumps(dict(owner='a'*32,
    bridge_pid=bridge['pid'], notifier_pid=os.getpid(),
    proc_start=platform_support.proc_start(os.getpid()))))
while not (root/'exit-now').exists():
    time.sleep(.02)
raise SystemExit(78)
""", '    '))
        process = self.spawn([sys.executable, str(self.app/'session.py'), 'run',
                              '--thread', self.thread, '--repo', self.repo])
        # Read actual supervisor output until it reports the running state.
        import select
        deadline = time.monotonic()+15
        running = False
        buffered = b''
        while time.monotonic() < deadline:
            readable, _, _ = select.select([process.stdout], [], [], .2)
            if not readable:
                continue
            chunk = os.read(process.stdout.fileno(), 4096)
            if not chunk:
                break
            buffered += chunk
            while b'\n' in buffered:
                line, buffered = buffered.split(b'\n', 1)
                if json.loads(line).get('status') == 'running':
                    running = True
            if running:
                break
        self.assertTrue(running, 'supervisor did not report running')
        (self.state/'exit-now').touch()
        stdout, stderr = process.communicate(timeout=20)
        self.assertEqual(process.returncode, 78, stderr)
        self.assertNotIn('Traceback', stderr)
        self.assertIsNone(session.bridge_status(self.app, self.state))

    def start_session(self, before_start=None):
        real_run = subprocess.run

        def service_manager(command, *args, **kwargs):
            if command[0] != 'systemctl':
                return real_run(command, *args, **kwargs)
            self.assertEqual(command[:2], ['systemctl', '--user'])
            operation = command[2]
            self.assertIn(operation, ('show-environment', 'daemon-reload', 'start'))
            if operation == 'start':
                self.starts += 1
                if before_start:
                    before_start()
                # Read the actual rendered unit, as a service manager would.
                import shlex
                unit = (self.root/'units'/command[3]).read_text()
                execution = next(line.removeprefix('ExecStart=') for line in unit.splitlines()
                                 if line.startswith('ExecStart='))
                self.spawn(shlex.split(execution))
            return subprocess.CompletedProcess(command, 0)

        output = io.StringIO()
        arguments = [str(self.app/'session.py'), 'ensure', '--agent', 'codex',
                     '--thread', self.thread, '--repo', self.repo]
        with patch('session.__file__', str(self.app/'session.py')), \
                patch.object(sys, 'argv', arguments), \
                patch.dict(os.environ, self.env), \
                patch('session.subprocess.run', side_effect=service_manager), \
                redirect_stdout(output):
            session.main()
        result = json.loads(output.getvalue())
        self.assertEqual(result['status'], 'running')
        self.assertEqual(self.starts, 1)
        return result

    def competing_command(self, action):
        attempted = self.root/'lock-attempted'
        # Signal the real lock attempt, so the race test does not depend on
        # guessing how long a second Python interpreter takes to start.
        command = '''
import fcntl
from pathlib import Path
import sys
sys.path.insert(0, sys.argv[1])
import session
attempted = Path(sys.argv[2])
original = fcntl.flock
def observed(lock, operation):
    if operation == fcntl.LOCK_EX and Path(lock.name).name in ('lifecycle.lock', 'registration.lock'):
        attempted.touch()
    return original(lock, operation)
fcntl.flock = observed
sys.argv = ['session.py', *sys.argv[3:]]
session.main()
'''
        process = self.spawn([sys.executable, '-c', command, str(self.app), str(attempted),
                              action, '--agent', 'codex', '--thread', self.thread,
                              '--repo', str(self.root/'renamed-project')])
        deadline = time.monotonic()+5
        while not attempted.exists():
            if process.poll() is not None:
                self.fail(f'competing command exited before its lock attempt: {process.communicate()}')
            if time.monotonic() >= deadline:
                self.fail('competing command did not attempt a session lock')
            time.sleep(.01)
        # There is no supervisor yet. Stop/rename must not act in this gap,
        # and another ensure must wait rather than report manual_required.
        with self.assertRaises(subprocess.TimeoutExpired):
            process.communicate(timeout=.2)
        return process

    def test_initial_systemd_ensure_starts_both_children(self):
        self.start_session()
        bridge = session.bridge_status(self.app, self.state)
        self.assertIsNotNone(bridge)
        self.assertTrue(session.notifier_ready(self.state, bridge))

    def test_same_thread_ensure_waits_and_reuses_the_started_session(self):
        competing = []
        self.start_session(lambda: competing.append(self.competing_command('ensure')))
        stdout, stderr = competing[0].communicate(timeout=15)
        self.assertEqual(competing[0].returncode, 0, stderr)
        result = json.loads(stdout)
        self.assertEqual(result['status'], 'running')
        bridge = session.bridge_status(self.app, self.state)
        self.assertEqual(result['bridge']['pid'], bridge['pid'])
        self.assertEqual(json.loads((self.state/'session.json').read_text())['repo'], self.repo)

    def test_stop_waits_for_start_then_stops_the_session(self):
        competing = []
        self.start_session(lambda: competing.append(self.competing_command('stop')))
        _, stderr = competing[0].communicate(timeout=15)
        self.assertEqual(competing[0].returncode, 0, stderr)
        self.assertIsNone(session.bridge_status(self.app, self.state))

    def test_rename_waits_for_start_then_refuses_a_live_session(self):
        competing = []
        self.start_session(lambda: competing.append(self.competing_command('rename')))
        _, stderr = competing[0].communicate(timeout=15)
        self.assertNotEqual(competing[0].returncode, 0)
        self.assertIn('stop this thread before explicitly renaming it', stderr)
        self.assertEqual(json.loads((self.state/'session.json').read_text())['repo'], self.repo)
        bridge = session.bridge_status(self.app, self.state)
        self.assertTrue(session.notifier_ready(self.state, bridge))

    def test_other_thread_registration_does_not_wait_for_this_start(self):
        def register_other_thread():
            process = self.spawn([sys.executable, str(self.app/'session.py'), 'ensure',
                                  '--agent', 'codex', '--thread', 'other-startup-thread',
                                  '--repo', self.repo])
            stdout, stderr = process.communicate(timeout=5)
            self.assertEqual(process.returncode, 0, stderr)
            result = json.loads(stdout)
            self.assertEqual(result['status'], 'manual_required')
            self.assertNotEqual(result['state_dir'], str(self.state))

        self.start_session(register_other_thread)
