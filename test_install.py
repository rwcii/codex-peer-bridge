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
            self.assertTrue((root/'app/LICENSE').exists())
            unit=(root/'units/codex-peer-notify.service').read_text()
            self.assertIn('test-thread',unit)
            self.assertIn('--codex',unit)
            self.assertFalse((root/'state').exists())

if __name__ == '__main__':
    unittest.main()
