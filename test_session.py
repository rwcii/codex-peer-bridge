import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
import session
from scripts.install import units

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

    def test_two_live_sessions_and_idempotence(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d)
            env=dict(os.environ,CLAUDE_CONFIG_DIR=str(root/'claude'))
            app=root/'app'
            subprocess.run([sys.executable,'scripts/install.py','--configure-codex','--no-start',
                '--codex',sys.executable,'--prefix',str(app),'--state-dir',str(root/'state'),
                '--unit-dir',str(root/'units'),'--codex-home',str(root/'codex')],check=True,capture_output=True,env=env)
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
                    self.assertEqual(json.loads(result.stdout)['bridge']['pid'],status['pid'])
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
